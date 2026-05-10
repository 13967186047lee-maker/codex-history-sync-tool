from __future__ import annotations

import json
import subprocess
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from datetime import datetime
from unittest.mock import patch
import py_compile
from pathlib import Path

from sync_backend import (
    get_status,
    make_backup,
    parse_current_model,
    parse_current_provider,
    rebuild_session_index,
    resolve_paths,
    restore_backup,
    restore_metadata,
    session_meta_backup_path,
    sync_session_records,
    sync_to_current_provider,
    update_provider_assignments,
)


STREAM_EVENT_KEYS = {"event", "stage", "message", "done", "total", "elapsed_ms", "extra"}


def write_config(codex_home, provider: str = "new_provider", model: str = "gpt-new") -> None:
    (codex_home / "config.toml").write_text(
        f'model_provider = "{provider}"\nmodel = "{model}"\n',
        encoding="utf-8",
    )


def create_threads_db(codex_home, *, with_model: bool = True) -> None:
    conn = sqlite3.connect(codex_home / "state_5.sqlite")
    if with_model:
        conn.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT NOT NULL, model TEXT)")
        conn.executemany(
            "INSERT INTO threads (id, model_provider, model) VALUES (?, ?, ?)",
            [
                ("old-provider-old-model", "old_provider", "gpt-old"),
                ("new-provider-old-model", "new_provider", "gpt-old"),
                ("already-current", "new_provider", "gpt-new"),
            ],
        )
    else:
        conn.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, model_provider TEXT NOT NULL)")
        conn.executemany(
            "INSERT INTO threads (id, model_provider) VALUES (?, ?)",
            [
                ("old-provider", "old_provider"),
                ("already-current", "new_provider"),
            ],
        )
    conn.commit()
    conn.close()


def create_thread_db_with_index_columns(codex_home) -> None:
    conn = sqlite3.connect(codex_home / "state_5.sqlite")
    conn.execute(
        """
        CREATE TABLE threads (
            id TEXT PRIMARY KEY,
            model_provider TEXT NOT NULL,
            model TEXT,
            title TEXT,
            updated_at INTEGER,
            updated_at_ms INTEGER,
            archived INTEGER NOT NULL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO threads (id, model_provider, model, title, updated_at, updated_at_ms, archived)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("visible-existing", "new_provider", "gpt-new", "Visible", 1700000000, 1700000000123, 0),
            ("visible-missing", "new_provider", "gpt-new", "Missing", 1700000001, 1700000001456, 0),
            ("archived-existing", "new_provider", "gpt-new", "Archived", 1700000002, 1700000002789, 1),
        ],
    )
    conn.commit()
    conn.close()


def write_session_file(codex_home: Path, thread_id: str, provider: str, model: str | None = None) -> Path:
    session_dir = codex_home / "sessions" / "2026" / "01" / "01"
    session_dir.mkdir(parents=True)
    path = session_dir / f"rollout-2026-01-01T00-00-00-000Z-{thread_id}.jsonl"
    payload = {"id": thread_id, "model_provider": provider}
    if model is not None:
        payload["model"] = model
    first_line = json.dumps({"type": "session_meta", "payload": payload}, separators=(",", ":"))
    path.write_text(first_line + "\n" + json.dumps({"type": "event"}) + "\n", encoding="utf-8")
    return path


class SyncBackendTests(unittest.TestCase):
    def test_python_entrypoints_compile(self) -> None:
        py_compile.compile("sync_backend.py", doraise=True)
        py_compile.compile("launch_ui.py", doraise=True)

    def test_config_parser_accepts_single_quoted_toml_values(self) -> None:
        config = "model_provider = 'provider-a'\nmodel = 'gpt-a'\n"

        self.assertEqual(parse_current_provider(config), "provider-a")
        self.assertEqual(parse_current_model(config), "gpt-a")

    def test_sync_updates_provider_and_model_for_newer_codex_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            paths = resolve_paths(str(codex_home))

            status = get_status(paths)

            self.assertEqual(status["provider_movable_threads"], 1)
            self.assertEqual(status["model_movable_threads"], 2)
            self.assertEqual(status["movable_threads"], 2)

            result = sync_to_current_provider(paths)

            self.assertEqual(result["synced_fields"], ["model_provider", "model"])
            self.assertEqual(result["updated_rows"], 2)

            with closing(sqlite3.connect(codex_home / "state_5.sqlite")) as conn:
                rows = conn.execute(
                    "SELECT model_provider, model, COUNT(*) FROM threads GROUP BY model_provider, model"
                ).fetchall()

            self.assertEqual(rows, [("new_provider", "gpt-new", 3)])

    def test_sync_still_supports_legacy_schema_without_model_column(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=False)
            paths = resolve_paths(str(codex_home))

            status = get_status(paths)

            self.assertEqual(status["provider_movable_threads"], 1)
            self.assertIsNone(status["model_movable_threads"])
            self.assertEqual(status["movable_threads"], 1)

            result = sync_to_current_provider(paths)

            self.assertEqual(result["synced_fields"], ["model_provider"])
            self.assertEqual(result["updated_rows"], 1)

            with closing(sqlite3.connect(codex_home / "state_5.sqlite")) as conn:
                rows = conn.execute("SELECT model_provider, COUNT(*) FROM threads GROUP BY model_provider").fetchall()

            self.assertEqual(rows, [("new_provider", 2)])

    def test_sync_skips_corrupt_session_files_and_updates_valid_session_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            good_thread_id = "11111111-1111-1111-1111-111111111111"
            bad_thread_id = "22222222-2222-2222-2222-222222222222"
            good_path = write_session_file(codex_home, good_thread_id, "old_provider", "gpt-old")
            bad_path = codex_home / "sessions" / "2026" / "01" / "01" / (
                f"rollout-2026-01-01T00-00-00-000Z-{bad_thread_id}.jsonl"
            )
            bad_path.write_text("{not-json}\n", encoding="utf-8")
            paths = resolve_paths(str(codex_home))

            status = get_status(paths)
            self.assertEqual(status["movable_session_threads"], 1)

            result = sync_to_current_provider(paths)

            self.assertEqual(result["updated_session_files"], 1)
            self.assertEqual(result["skipped_session_files"], 0)
            first_line = good_path.read_text(encoding="utf-8").splitlines()[0]
            payload = json.loads(first_line)["payload"]
            self.assertEqual(payload["model_provider"], "new_provider")
            self.assertEqual(payload["model"], "gpt-new")
            self.assertEqual(bad_path.read_text(encoding="utf-8"), "{not-json}\n")

    def test_sync_session_records_skips_files_that_change_to_invalid_json_mid_sync(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            write_session_file(
                codex_home,
                "44444444-4444-4444-4444-444444444444",
                "old_provider",
                "gpt-old",
            )
            paths = resolve_paths(str(codex_home))

            with patch("sync_backend.read_text_exact", return_value="{not-json}\n"):
                result = sync_session_records(paths, "new_provider", "gpt-new")

            self.assertEqual(result["updated_session_files"], 0)
            self.assertEqual(result["skipped_session_files"], 1)

    def test_jsonl_sync_emits_progress_events_before_result(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            write_session_file(
                codex_home,
                "55555555-5555-5555-5555-555555555555",
                "old_provider",
                "gpt-old",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    "sync_backend.py",
                    "--codex-home",
                    str(codex_home),
                    "--jsonl",
                    "sync",
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
            self.assertGreater(len(events), 1)
            self.assertTrue(all(set(event) == STREAM_EVENT_KEYS for event in events))
            self.assertEqual(events[-1]["event"], "result")
            self.assertTrue(events[-1]["extra"]["ok"])
            self.assertEqual(events[-1]["extra"]["action"], "sync")
            progress_stages = {event["stage"] for event in events[:-1] if event["event"] == "progress"}
            self.assertTrue({"scan", "backup", "database", "sessions", "index"}.issubset(progress_stages))

    def test_database_lock_retry_emits_waiting_progress_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            paths = resolve_paths(str(codex_home))
            events = []
            locker = sqlite3.connect(codex_home / "state_5.sqlite")
            try:
                locker.execute("BEGIN IMMEDIATE")
                with patch("sync_backend.WRITE_LOCK_RETRY_LIMIT", 2), patch(
                    "sync_backend.WRITE_LOCK_RETRY_DELAY_SECONDS", 0
                ), patch("sync_backend.WRITE_OPERATION_TIMEOUT_SECONDS", 0.01):
                    with self.assertRaises(RuntimeError):
                        update_provider_assignments(paths, "new_provider", "gpt-new", events.append)
            finally:
                locker.rollback()
                locker.close()

            waiting_events = [
                event
                for event in events
                if event["stage"] == "database" and event["extra"].get("action") == "waiting_for_lock"
            ]
            self.assertGreaterEqual(len(waiting_events), 1)
            self.assertTrue(all(set(event) == STREAM_EVENT_KEYS for event in events))

    def test_rebuild_session_index_preserves_unknown_fields_and_drops_archived_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_thread_db_with_index_columns(codex_home)
            paths = resolve_paths(str(codex_home))
            index_entries = [
                {
                    "id": "visible-existing",
                    "thread_name": "Custom name",
                    "updated_at": "not-a-date",
                    "extra": {"keep": True},
                },
                {
                    "id": "archived-existing",
                    "thread_name": "Should disappear",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
                {
                    "id": "index-only",
                    "thread_name": "External",
                    "updated_at": "not-a-date",
                    "custom": "preserved",
                },
            ]
            paths.session_index_path.write_text(
                "".join(json.dumps(entry) + "\n" for entry in index_entries),
                encoding="utf-8",
            )

            with closing(sqlite3.connect(codex_home / "state_5.sqlite")) as conn:
                conn.row_factory = sqlite3.Row
                summary = rebuild_session_index(paths, conn)

            rebuilt = [
                json.loads(line)
                for line in paths.session_index_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            by_id = {entry["id"]: entry for entry in rebuilt}

            self.assertEqual(summary["missing_session_index_entries_before"], 1)
            self.assertEqual(summary["preserved_index_only_entries"], 1)
            self.assertEqual(summary["removed_archived_index_entries"], 1)
            self.assertIn("visible-existing", by_id)
            self.assertIn("visible-missing", by_id)
            self.assertIn("index-only", by_id)
            self.assertNotIn("archived-existing", by_id)
            self.assertEqual(by_id["visible-existing"]["thread_name"], "Custom name")
            self.assertEqual(by_id["visible-existing"]["extra"], {"keep": True})
            self.assertEqual(by_id["visible-existing"]["updated_at"], "2023-11-14T22:13:20.123000Z")
            self.assertEqual(by_id["index-only"]["custom"], "preserved")

    def test_make_backup_does_not_overwrite_when_timestamp_collides(self) -> None:
        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 1, 1, 0, 0, 0, 123456, tzinfo=tz)

        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            paths = resolve_paths(str(codex_home))

            with patch("sync_backend.datetime", FixedDateTime):
                first = make_backup(paths, "manual")
                second = make_backup(paths, "manual")

            self.assertNotEqual(first, second)
            self.assertTrue(first.exists())
            self.assertTrue(second.exists())

    def test_restore_metadata_refuses_paths_outside_sessions_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            paths = resolve_paths(str(codex_home))
            session_path = write_session_file(
                codex_home,
                "33333333-3333-3333-3333-333333333333",
                "old_provider",
                "gpt-old",
            )
            outside_path = codex_home / "outside.txt"
            outside_path.write_text("ORIGINAL\nbody\n", encoding="utf-8")
            non_session_path = paths.sessions_dir / "not-a-rollout.txt"
            non_session_path.parent.mkdir(parents=True, exist_ok=True)
            non_session_path.write_text("ORIGINAL\n", encoding="utf-8")
            backup_path = paths.backup_dir / "state_5.sqlite.manual.20260101-000000.bak"
            paths.backup_dir.mkdir()
            backup_path.write_bytes(b"placeholder")
            meta_path = session_meta_backup_path(backup_path)
            meta_path.write_text(
                json.dumps(
                    [
                        {
                            "path": str(session_path.relative_to(codex_home)),
                            "first_line": '{"type":"session_meta","payload":{"id":"33333333-3333-3333-3333-333333333333","model_provider":"new_provider"}}',
                        },
                        {"path": "../outside.txt", "first_line": "MALICIOUS"},
                        {"path": str(non_session_path.relative_to(codex_home)), "first_line": "MALICIOUS"},
                        {"path": str(session_path.relative_to(codex_home)), "first_line": "BAD\nLINE"},
                    ]
                ),
                encoding="utf-8",
            )

            summary = restore_metadata(paths, backup_path)

            self.assertEqual(summary["session_files_restored"], 1)
            self.assertEqual(summary["unsafe_paths_skipped"], 2)
            self.assertEqual(summary["session_files_skipped"], 1)
            self.assertEqual(outside_path.read_text(encoding="utf-8"), "ORIGINAL\nbody\n")
            self.assertEqual(non_session_path.read_text(encoding="utf-8"), "ORIGINAL\n")
            restored_first_line = session_path.read_text(encoding="utf-8").splitlines()[0]
            self.assertIn('"model_provider":"new_provider"', restored_first_line)

    def test_restore_backup_restores_previous_database_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            codex_home = Path(temp_dir)
            write_config(codex_home)
            create_threads_db(codex_home, with_model=True)
            paths = resolve_paths(str(codex_home))
            backup_path = make_backup(paths, "manual")

            sync_to_current_provider(paths)
            result = restore_backup(paths, str(backup_path))

            self.assertEqual(result["restored_from"], str(backup_path))
            with closing(sqlite3.connect(codex_home / "state_5.sqlite")) as conn:
                rows = conn.execute(
                    "SELECT model_provider, model, COUNT(*) FROM threads GROUP BY model_provider, model ORDER BY model_provider, model"
                ).fetchall()

            self.assertEqual(
                rows,
                [
                    ("new_provider", "gpt-new", 1),
                    ("new_provider", "gpt-old", 1),
                    ("old_provider", "gpt-old", 1),
                ],
            )


if __name__ == "__main__":
    unittest.main()
