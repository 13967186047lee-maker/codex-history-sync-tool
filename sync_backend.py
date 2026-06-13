from __future__ import annotations

import argparse
import json
import re
import sqlite3
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised only on Python 3.10
    tomllib = None

SESSION_FILENAME_PATTERN = re.compile(
    r"rollout-.*-(?P<id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.jsonl$",
    re.IGNORECASE,
)
UTC = timezone.utc
DEFAULT_DB_TIMEOUT_SECONDS = 30.0
WRITE_OPERATION_TIMEOUT_SECONDS = 0.5
WRITE_LOCK_RETRY_LIMIT = 40
WRITE_LOCK_RETRY_DELAY_SECONDS = 0.25
FILE_REPLACE_RETRY_LIMIT = 20
FILE_REPLACE_RETRY_DELAY_SECONDS = 0.1
SYNC_CHECKPOINT_MODE = "PASSIVE"


def default_codex_home() -> Path:
    return Path.home() / ".codex"


@dataclass
class Paths:
    codex_home: Path
    config_path: Path
    db_path: Path
    backup_dir: Path
    session_index_path: Path
    sessions_dir: Path


@dataclass
class SessionRecord:
    thread_id: str
    path: Path
    model_provider: str
    model: str | None


ProgressCallback = Callable[[dict[str, object]], None]


def resolve_paths(codex_home: str | None) -> Paths:
    home = Path(codex_home).expanduser() if codex_home else default_codex_home()
    return Paths(
        codex_home=home,
        config_path=home / "config.toml",
        db_path=home / "state_5.sqlite",
        backup_dir=home / "history_sync_backups",
        session_index_path=home / "session_index.jsonl",
        sessions_dir=home / "sessions",
    )


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_text_exact(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return handle.read()


def replace_file_with_retry(source_path: Path, target_path: Path) -> None:
    last_error: OSError | None = None
    for attempt in range(FILE_REPLACE_RETRY_LIMIT):
        try:
            # 用原子替换避免写到一半被 Codex 读到半成品文件。
            source_path.replace(target_path)
            return
        except PermissionError as exc:
            last_error = exc

        if attempt < FILE_REPLACE_RETRY_LIMIT - 1:
            time.sleep(FILE_REPLACE_RETRY_DELAY_SECONDS)

    raise RuntimeError(f"File is busy and could not be replaced: {target_path}") from last_error


def write_text_exact(path: Path, text: str) -> None:
    temp_path = path.with_name(f".{path.name}.codex-sync-{time.time_ns()}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        replace_file_with_retry(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def parse_toml_string_value(config_text: str, key: str) -> str | None:
    if tomllib is not None:
        data = tomllib.loads(config_text)
        value = data.get(key)
        return value if isinstance(value, str) else None

    match = re.search(rf"(?m)^\s*{re.escape(key)}\s*=\s*(['\"])(.*?)\1", config_text)
    return match.group(2) if match else None


def parse_current_provider(config_text: str) -> str | None:
    value = parse_toml_string_value(config_text, "model_provider")
    return value


def parse_current_model(config_text: str) -> str | None:
    return parse_toml_string_value(config_text, "model")


def unique_backup_path(paths: Paths, label: str) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    base = paths.backup_dir / f"state_5.sqlite.{label}.{timestamp}.bak"
    if not base.exists():
        return base

    for suffix in range(1, 1000):
        candidate = paths.backup_dir / f"state_5.sqlite.{label}.{timestamp}.{suffix}.bak"
        if not candidate.exists():
            return candidate
    raise RuntimeError("Could not allocate a unique backup filename.")


@contextmanager
def connect_db(
    path: Path,
    readonly: bool = False,
    timeout_seconds: float = DEFAULT_DB_TIMEOUT_SECONDS,
    busy_timeout_ms: int | None = None,
) -> Iterator[sqlite3.Connection]:
    if busy_timeout_ms is None:
        busy_timeout_ms = max(1, int(timeout_seconds * 1000))

    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=timeout_seconds)
    else:
        conn = sqlite3.connect(str(path), timeout=timeout_seconds)

    try:
        conn.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        conn.row_factory = sqlite3.Row
        yield conn
    finally:
        conn.close()


def ensure_environment(paths: Paths) -> None:
    if not paths.config_path.exists():
        raise RuntimeError(f"Missing config file: {paths.config_path}")
    if not paths.db_path.exists():
        raise RuntimeError(f"Missing database file: {paths.db_path}")


def get_thread_columns(conn: sqlite3.Connection) -> set[str]:
    return {str(row["name"]) for row in conn.execute("PRAGMA table_info(threads)")}


def ensure_thread_schema(conn: sqlite3.Connection) -> set[str]:
    columns = get_thread_columns(conn)
    missing = sorted({"id", "model_provider"} - columns)
    if missing:
        raise RuntimeError(
            "Codex history database does not look like a supported schema; "
            f"missing threads column(s): {', '.join(missing)}"
        )
    return columns


def counts_to_rows(counts: OrderedDict[str, int]) -> list[dict[str, object]]:
    return [{"provider": key, "count": value} for key, value in counts.items()]


def model_counts_to_rows(counts: OrderedDict[str, int]) -> list[dict[str, object]]:
    return [{"model": key, "count": value} for key, value in counts.items()]


def ordered_counts(values: list[str]) -> OrderedDict[str, int]:
    raw_counts: dict[str, int] = {}
    for value in values:
        key = value or "(empty)"
        raw_counts[key] = raw_counts.get(key, 0) + 1

    counts = OrderedDict()
    for key, value in sorted(raw_counts.items(), key=lambda item: (-item[1], item[0])):
        counts[key] = value
    return counts


def elapsed_ms(started_at: float) -> int:
    return int((time.monotonic() - started_at) * 1000)


def build_progress_event(
    event: str,
    stage: str,
    message: str,
    *,
    started_at: float | None = None,
    done: int | None = None,
    total: int | None = None,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "event": event,
        "stage": stage,
        "message": message,
        "done": done,
        "total": total,
        "elapsed_ms": elapsed_ms(started_at) if started_at is not None else 0,
        "extra": extra or {},
    }


def emit_progress(
    progress: ProgressCallback | None,
    event: str,
    stage: str,
    message: str,
    *,
    started_at: float | None = None,
    done: int | None = None,
    total: int | None = None,
    extra: dict[str, object] | None = None,
) -> None:
    if progress is None:
        return
    progress(
        build_progress_event(
            event,
            stage,
            message,
            started_at=started_at,
            done=done,
            total=total,
            extra=extra,
        )
    )


def should_emit_count_progress(done: int, total: int) -> bool:
    if total <= 0:
        return done <= 1
    if done == total or done <= 3:
        return True
    return done % 50 == 0


def query_provider_counts(conn: sqlite3.Connection) -> OrderedDict[str, int]:
    counts = OrderedDict()
    for provider, count in conn.execute(
        """
        SELECT model_provider, COUNT(*)
        FROM threads
        GROUP BY model_provider
        ORDER BY COUNT(*) DESC, model_provider ASC
        """
    ):
        counts[str(provider or "(empty)")] = int(count)
    return counts


def query_model_counts(conn: sqlite3.Connection) -> OrderedDict[str, int]:
    counts = OrderedDict()
    for model, count in conn.execute(
        """
        SELECT model, COUNT(*)
        FROM threads
        GROUP BY model
        ORDER BY COUNT(*) DESC, model ASC
        """
    ):
        counts[str(model or "(empty)")] = int(count)
    return counts


def query_provider_model_counts(conn: sqlite3.Connection) -> list[dict[str, object]]:
    rows = []
    for provider, model, count in conn.execute(
        """
        SELECT model_provider, model, COUNT(*)
        FROM threads
        GROUP BY model_provider, model
        ORDER BY COUNT(*) DESC, model_provider ASC, model ASC
        """
    ):
        rows.append(
            {
                "provider": str(provider or "(empty)"),
                "model": str(model or "(empty)"),
                "count": int(count),
            }
        )
    return rows


def query_cwd_counts(conn: sqlite3.Connection, limit: int = 20) -> list[dict[str, object]]:
    rows = []
    for cwd, count in conn.execute(
        """
        SELECT cwd, COUNT(*)
        FROM threads
        GROUP BY cwd
        ORDER BY COUNT(*) DESC, cwd ASC
        LIMIT ?
        """,
        (limit,),
    ):
        rows.append({"cwd": str(cwd or "(empty)"), "count": int(count)})
    return rows


def infer_current_provider(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT model_provider
        FROM threads
        WHERE archived = 0
          AND model_provider IS NOT NULL
          AND model_provider <> ''
        ORDER BY COALESCE(updated_at_ms, updated_at * 1000, 0) DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if row and row["model_provider"]:
        return str(row["model_provider"])

    row = conn.execute(
        """
        SELECT model_provider
        FROM threads
        WHERE model_provider IS NOT NULL
          AND model_provider <> ''
        ORDER BY COALESCE(updated_at_ms, updated_at * 1000, 0) DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    if row and row["model_provider"]:
        return str(row["model_provider"])

    raise RuntimeError(
        "Could not determine current model_provider from config.toml or local history database."
    )


def count_mismatched(conn: sqlite3.Connection, column: str, expected: str | None) -> int | None:
    if expected is None:
        return None
    return int(
        conn.execute(
            f"SELECT COUNT(*) FROM threads WHERE {column} IS NULL OR {column} <> ?",
            (expected,),
        ).fetchone()[0]
    )


def list_backups(paths: Paths, limit: int = 20) -> list[dict[str, str]]:
    if not paths.backup_dir.exists():
        return []
    files = sorted(
        paths.backup_dir.glob("state_5.sqlite.*.bak"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    output = []
    for item in files[:limit]:
        output.append(
            {
                "name": item.name,
                "path": str(item),
                "modified_at": datetime.fromtimestamp(item.stat().st_mtime).isoformat(timespec="seconds"),
            }
        )
    return output


def split_first_line(text: str) -> tuple[str, str, str]:
    for ending in ("\r\n", "\n", "\r"):
        index = text.find(ending)
        if index >= 0:
            return text[:index], ending, text[index + len(ending) :]
    return text, "", ""


def replace_first_line(path: Path, first_line: str) -> None:
    text = read_text_exact(path)
    _, ending, remainder = split_first_line(text)
    if ending:
        new_text = first_line + ending + remainder
    elif text:
        new_text = first_line
    else:
        new_text = first_line + "\n"
    write_text_exact(path, new_text)


def session_index_backup_path(backup_path: Path) -> Path:
    return backup_path.with_name(f"{backup_path.name}.session_index.jsonl")


def session_meta_backup_path(backup_path: Path) -> Path:
    return backup_path.with_name(f"{backup_path.name}.session_meta.json")


def iter_session_paths(paths: Paths) -> list[Path]:
    if not paths.sessions_dir.exists():
        return []
    return sorted(paths.sessions_dir.rglob("rollout-*.jsonl"))


def parse_session_record(path: Path) -> SessionRecord | None:
    if not SESSION_FILENAME_PATTERN.search(path.name):
        return None

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            first_line = handle.readline()
    except (OSError, UnicodeDecodeError):
        return None

    if not first_line:
        return None

    try:
        item = json.loads(first_line.rstrip("\r\n"))
    except json.JSONDecodeError:
        return None
    if not isinstance(item, dict):
        return None
    if item.get("type") != "session_meta":
        return None

    payload = item.get("payload")
    if not isinstance(payload, dict):
        return None

    thread_id = str(payload.get("id") or "").strip()
    if not thread_id:
        return None

    model_provider = str(payload.get("model_provider") or "")
    raw_model = payload.get("model")
    model = str(raw_model) if raw_model else None
    return SessionRecord(thread_id=thread_id, path=path, model_provider=model_provider, model=model)


def scan_session_records(paths: Paths) -> list[SessionRecord]:
    records: list[SessionRecord] = []
    for path in iter_session_paths(paths):
        record = parse_session_record(path)
        if record:
            records.append(record)
    return records


def read_session_index(paths: Paths) -> OrderedDict[str, dict[str, Any]]:
    entries: OrderedDict[str, dict[str, Any]] = OrderedDict()
    if not paths.session_index_path.exists():
        return entries

    for line in read_text(paths.session_index_path).splitlines():
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict):
            continue
        thread_id = str(entry.get("id") or "").strip()
        if not thread_id:
            continue
        normalized = dict(entry)
        normalized["id"] = thread_id
        normalized["thread_name"] = str(normalized.get("thread_name") or thread_id)
        normalized["updated_at"] = str(normalized.get("updated_at") or "")
        entries[thread_id] = normalized
    return entries


def write_session_index(paths: Paths, entries: list[dict[str, Any]]) -> None:
    lines = [json.dumps(entry, ensure_ascii=False, separators=(",", ":")) for entry in entries]
    content = "\n".join(lines)
    if content:
        content += "\n"
    write_text_exact(paths.session_index_path, content)


def iso_utc_from_unix(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=UTC).isoformat().replace("+00:00", "Z")


def iso_utc_from_unix_ms(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).isoformat().replace("+00:00", "Z")


def parse_index_timestamp(value: str) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=UTC)
    try:
        normalized = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return datetime.fromtimestamp(0, tz=UTC)


def snapshot_metadata(paths: Paths, backup_path: Path) -> None:
    if paths.session_index_path.exists():
        write_text_exact(session_index_backup_path(backup_path), read_text_exact(paths.session_index_path))

    items: list[dict[str, str]] = []
    for path in iter_session_paths(paths):
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                first_line = handle.readline().rstrip("\r\n")
        except FileNotFoundError:
            continue
        if not first_line:
            continue

        try:
            relative_path = path.relative_to(paths.codex_home)
        except ValueError:
            relative_path = path

        items.append({"path": str(relative_path), "first_line": first_line})

    write_text_exact(
        session_meta_backup_path(backup_path),
        json.dumps(items, ensure_ascii=False, indent=2) + "\n",
    )


def restore_metadata(
    paths: Paths,
    backup_path: Path,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    session_index_restored = False
    session_files_restored = 0
    session_files_missing = 0
    session_files_skipped = 0
    unsafe_paths_skipped = 0
    metadata_error = ""
    emit_progress(progress, "progress", "metadata", "正在恢复侧边栏索引和会话元数据...", started_at=started_at)

    index_backup = session_index_backup_path(backup_path)
    if index_backup.exists():
        write_text_exact(paths.session_index_path, read_text_exact(index_backup))
        session_index_restored = True

    meta_backup = session_meta_backup_path(backup_path)
    if meta_backup.exists():
        try:
            metadata_items = json.loads(read_text(meta_backup))
        except json.JSONDecodeError as exc:
            metadata_items = []
            metadata_error = f"Could not parse session metadata backup: {exc}"

        if not isinstance(metadata_items, list):
            metadata_items = []
            metadata_error = "Session metadata backup is not a JSON array."

        total_items = len(metadata_items)
        emit_progress(
            progress,
            "progress",
            "metadata",
            "正在恢复会话文件首行元数据...",
            started_at=started_at,
            done=0,
            total=total_items,
            extra={"restored": 0, "skipped": 0, "missing": 0},
        )

        def emit_metadata_progress(processed: int) -> None:
            if not should_emit_count_progress(processed, total_items):
                return
            emit_progress(
                progress,
                "progress",
                "metadata",
                "正在恢复会话文件首行元数据...",
                started_at=started_at,
                done=processed,
                total=total_items,
                extra={
                    "restored": session_files_restored,
                    "skipped": session_files_skipped + unsafe_paths_skipped,
                    "missing": session_files_missing,
                },
            )

        for processed, item in enumerate(metadata_items, start=1):
            if not isinstance(item, dict) or not isinstance(item.get("first_line"), str):
                session_files_skipped += 1
                emit_metadata_progress(processed)
                continue
            if "\n" in item["first_line"] or "\r" in item["first_line"]:
                session_files_skipped += 1
                emit_metadata_progress(processed)
                continue
            path = safe_session_metadata_path(paths, str(item.get("path") or ""))
            if path is None:
                unsafe_paths_skipped += 1
                emit_metadata_progress(processed)
                continue
            if not path.exists():
                session_files_missing += 1
                emit_metadata_progress(processed)
                continue
            # 只恢复首行 session_meta，后面的对话内容保持原文件不动。
            replace_first_line(path, str(item["first_line"]))
            session_files_restored += 1
            emit_metadata_progress(processed)

    emit_progress(
        progress,
        "progress",
        "metadata",
        "侧边栏索引和会话元数据已恢复。",
        started_at=started_at,
        extra={
            "restored": session_files_restored,
            "skipped": session_files_skipped + unsafe_paths_skipped,
            "missing": session_files_missing,
        },
    )

    return {
        "session_index_restored": session_index_restored,
        "session_files_restored": session_files_restored,
        "session_files_missing": session_files_missing,
        "session_files_skipped": session_files_skipped,
        "unsafe_paths_skipped": unsafe_paths_skipped,
        "metadata_error": metadata_error,
        "duration_ms": elapsed_ms(started_at),
    }


def path_is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def safe_session_metadata_path(paths: Paths, raw_path: str) -> Path | None:
    if not raw_path:
        return None
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = paths.codex_home / candidate

    if not SESSION_FILENAME_PATTERN.search(candidate.name):
        return None

    sessions_dir = paths.sessions_dir.resolve(strict=False)
    resolved = candidate.resolve(strict=False)
    if not path_is_relative_to(resolved, sessions_dir):
        return None
    return resolved


def rebuild_session_index(
    paths: Paths,
    conn: sqlite3.Connection,
    progress: ProgressCallback | None = None,
) -> dict[str, int]:
    started_at = time.monotonic()
    emit_progress(progress, "progress", "index", "正在读取侧边栏索引...", started_at=started_at)
    existing_entries = read_session_index(paths)
    columns = ensure_thread_schema(conn)
    select_parts = ["id"]
    if "title" in columns:
        select_parts.append("title")
    if "updated_at_ms" in columns:
        select_parts.append("updated_at_ms")
    if "updated_at" in columns:
        select_parts.append("updated_at")
    where_sql = "WHERE archived = 0" if "archived" in columns else ""
    db_rows = conn.execute(
        f"""
        SELECT {", ".join(select_parts)}
        FROM threads
        {where_sql}
        ORDER BY id ASC
        """
    ).fetchall()
    emit_progress(
        progress,
        "progress",
        "index",
        "正在重建侧边栏索引...",
        started_at=started_at,
        done=0,
        total=len(db_rows),
        extra={"existing_entries": len(existing_entries)},
    )
    visible_db_ids = {str(row["id"]) for row in db_rows}
    all_db_ids = {str(row["id"]) for row in conn.execute("SELECT id FROM threads")}
    existing_ids = set(existing_entries)

    merged: list[dict[str, Any]] = []
    for row in db_rows:
        thread_id = str(row["id"])
        existing_entry = existing_entries.get(thread_id)
        title = str(row["title"]) if "title" in columns and row["title"] else thread_id
        if "updated_at_ms" in columns and row["updated_at_ms"]:
            updated_at = iso_utc_from_unix_ms(int(row["updated_at_ms"]))
        elif "updated_at" in columns and row["updated_at"]:
            updated_at = iso_utc_from_unix(int(row["updated_at"]))
        else:
            updated_at = iso_utc_from_unix(0)

        entry = dict(existing_entry or {})
        entry["id"] = thread_id
        entry["thread_name"] = str(entry.get("thread_name") or title)
        entry["updated_at"] = updated_at
        merged.append(entry)

    for thread_id, entry in existing_entries.items():
        if thread_id not in all_db_ids:
            merged.append(entry)

    merged.sort(key=lambda item: (parse_index_timestamp(item["updated_at"]), item["id"]))
    write_session_index(paths, merged)
    emit_progress(
        progress,
        "progress",
        "index",
        "侧边栏索引已重建。",
        started_at=started_at,
        done=len(db_rows),
        total=len(db_rows),
        extra={"rewritten_entries": len(merged)},
    )

    return {
        "rewritten_index_entries": len(merged),
        "missing_session_index_entries_before": len(visible_db_ids - existing_ids),
        "preserved_index_only_entries": len(existing_ids - all_db_ids),
        "removed_archived_index_entries": len((existing_ids & all_db_ids) - visible_db_ids),
        "duration_ms": elapsed_ms(started_at),
    }


def sync_session_records(
    paths: Paths,
    current_provider: str,
    current_model: str | None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    emit_progress(progress, "progress", "sessions", "正在扫描会话文件...", started_at=started_at)
    before_records = scan_session_records(paths)
    updated_session_files = 0
    skipped_session_files = 0
    total_records = len(before_records)
    emit_progress(
        progress,
        "progress",
        "sessions",
        "正在同步会话文件元数据...",
        started_at=started_at,
        done=0,
        total=total_records,
        extra={"updated": 0, "skipped": 0},
    )

    def emit_session_progress(processed: int) -> None:
        if not should_emit_count_progress(processed, total_records):
            return
        emit_progress(
            progress,
            "progress",
            "sessions",
            "正在同步会话文件元数据...",
            started_at=started_at,
            done=processed,
            total=total_records,
            extra={"updated": updated_session_files, "skipped": skipped_session_files},
        )

    for processed, record in enumerate(before_records, start=1):
        model_matches = current_model is None or record.model == current_model
        if record.model_provider == current_provider and model_matches:
            emit_session_progress(processed)
            continue

        try:
            text = read_text_exact(record.path)
        except (OSError, UnicodeDecodeError):
            skipped_session_files += 1
            emit_session_progress(processed)
            continue
        first_line, ending, remainder = split_first_line(text)
        try:
            item = json.loads(first_line)
        except json.JSONDecodeError:
            skipped_session_files += 1
            emit_session_progress(processed)
            continue
        if not isinstance(item, dict):
            skipped_session_files += 1
            emit_session_progress(processed)
            continue
        payload = item.get("payload")
        if not isinstance(payload, dict):
            skipped_session_files += 1
            emit_session_progress(processed)
            continue

        payload["model_provider"] = current_provider
        if current_model:
            payload["model"] = current_model
        new_first_line = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        if ending:
            new_text = new_first_line + ending + remainder
        else:
            new_text = new_first_line
        write_text_exact(record.path, new_text)
        updated_session_files += 1
        emit_session_progress(processed)

    after_records = scan_session_records(paths)
    emit_progress(
        progress,
        "progress",
        "sessions",
        "会话文件元数据已同步。",
        started_at=started_at,
        done=total_records,
        total=total_records,
        extra={"updated": updated_session_files, "skipped": skipped_session_files},
    )
    return {
        "updated_session_files": updated_session_files,
        "skipped_session_files": skipped_session_files,
        "session_before_counts": counts_to_rows(
            ordered_counts([record.model_provider for record in before_records])
        ),
        "session_after_counts": counts_to_rows(
            ordered_counts([record.model_provider for record in after_records])
        ),
        "session_before_model_counts": model_counts_to_rows(
            ordered_counts([record.model or "(empty)" for record in before_records])
        ),
        "session_after_model_counts": model_counts_to_rows(
            ordered_counts([record.model or "(empty)" for record in after_records])
        ),
        "duration_ms": elapsed_ms(started_at),
    }


def is_locked_error(exc: sqlite3.OperationalError) -> bool:
    message = str(exc).lower()
    return (
        "database is locked" in message
        or "database table is locked" in message
        or "database is busy" in message
        or "destination database is in use" in message
    )


def checkpoint(conn: sqlite3.Connection, mode: str = SYNC_CHECKPOINT_MODE) -> tuple[int, int, int]:
    row = conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
    return int(row[0]), int(row[1]), int(row[2])


def update_provider_assignments(
    paths: Paths,
    current_provider: str,
    current_model: str | None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    last_error: sqlite3.OperationalError | None = None
    emit_progress(
        progress,
        "progress",
        "database",
        "正在等待数据库空闲并更新历史归属...",
        started_at=started_at,
        done=0,
        total=WRITE_LOCK_RETRY_LIMIT,
        extra={"attempt": 0},
    )

    for attempt in range(1, WRITE_LOCK_RETRY_LIMIT + 1):
        try:
            with connect_db(
                paths.db_path,
                readonly=False,
                timeout_seconds=WRITE_OPERATION_TIMEOUT_SECONDS,
            ) as conn:
                # 显式拿写锁，把等待控制在我们自己的重试节奏里。
                conn.execute("BEGIN IMMEDIATE")
                columns = ensure_thread_schema(conn)
                before_counts = query_provider_counts(conn)
                before_model_counts = query_model_counts(conn) if "model" in columns else OrderedDict()
                set_parts = ["model_provider = ?"]
                set_params = [current_provider]
                where_parts = ["model_provider IS NULL OR model_provider <> ?"]
                where_params = [current_provider]
                synced_fields = ["model_provider"]

                if "model" in columns and current_model:
                    set_parts.append("model = ?")
                    set_params.append(current_model)
                    where_parts.append("model IS NULL OR model <> ?")
                    where_params.append(current_model)
                    synced_fields.append("model")

                set_sql = ", ".join(set_parts)
                where_sql = " OR ".join(f"({part})" for part in where_parts)
                updated_rows = conn.execute(
                    f"UPDATE threads SET {set_sql} WHERE {where_sql}",
                    (*set_params, *where_params),
                ).rowcount
                conn.commit()
                after_counts = query_provider_counts(conn)
                after_model_counts = query_model_counts(conn) if "model" in columns else OrderedDict()
                checkpoint_result = checkpoint(conn)

            emit_progress(
                progress,
                "progress",
                "database",
                "数据库历史归属已更新。",
                started_at=started_at,
                done=attempt,
                total=WRITE_LOCK_RETRY_LIMIT,
                extra={"updated_rows": updated_rows, "attempt": attempt},
            )
            return {
                "attempts": attempt,
                "lock_wait_ms": elapsed_ms(started_at),
                "synced_fields": synced_fields,
                "updated_rows": updated_rows,
                "before_counts": counts_to_rows(before_counts),
                "after_counts": counts_to_rows(after_counts),
                "before_model_counts": model_counts_to_rows(before_model_counts),
                "after_model_counts": model_counts_to_rows(after_model_counts),
                "checkpoint": {
                    "mode": SYNC_CHECKPOINT_MODE,
                    "busy": checkpoint_result[0],
                    "log_frames": checkpoint_result[1],
                    "checkpointed_frames": checkpoint_result[2],
                },
            }
        except sqlite3.OperationalError as exc:
            if not is_locked_error(exc):
                raise
            last_error = exc
            emit_progress(
                progress,
                "progress",
                "database",
                "数据库正在忙，继续等待空闲...",
                started_at=started_at,
                done=attempt,
                total=WRITE_LOCK_RETRY_LIMIT,
                extra={"attempt": attempt, "action": "waiting_for_lock"},
            )
            if attempt >= WRITE_LOCK_RETRY_LIMIT:
                waited_seconds = (time.monotonic() - started_at)
                raise RuntimeError(
                    "Codex 当前正在写入本地历史数据库，"
                    f"已等待 {waited_seconds:.1f} 秒仍未拿到写锁。"
                    "保持 Codex 开着也可以同步，但请等当前回复、工具调用或自动保存结束后再试一次。"
                ) from exc
            time.sleep(WRITE_LOCK_RETRY_DELAY_SECONDS)

    raise RuntimeError("Database write lock retry loop ended unexpectedly.") from last_error


def restore_database_with_retry(
    paths: Paths,
    chosen_backup: Path,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    started_at = time.monotonic()
    last_error: sqlite3.OperationalError | None = None
    emit_progress(
        progress,
        "progress",
        "database",
        "正在等待数据库空闲并恢复备份...",
        started_at=started_at,
        done=0,
        total=WRITE_LOCK_RETRY_LIMIT,
        extra={"attempt": 0},
    )

    for attempt in range(1, WRITE_LOCK_RETRY_LIMIT + 1):
        try:
            with connect_db(chosen_backup, readonly=True) as source, connect_db(
                paths.db_path,
                readonly=False,
                timeout_seconds=WRITE_OPERATION_TIMEOUT_SECONDS,
            ) as target:
                # SQLite 在整库 backup 到目标库时会自己申请所需锁；
                # 这里直接尝试 restore，失败后统一按“数据库正忙”重试即可。
                source.backup(target)
                checkpoint_result = checkpoint(target)

            emit_progress(
                progress,
                "progress",
                "database",
                "数据库备份已恢复。",
                started_at=started_at,
                done=attempt,
                total=WRITE_LOCK_RETRY_LIMIT,
                extra={"attempt": attempt},
            )
            return {
                "attempts": attempt,
                "lock_wait_ms": elapsed_ms(started_at),
                "checkpoint": {
                    "mode": SYNC_CHECKPOINT_MODE,
                    "busy": checkpoint_result[0],
                    "log_frames": checkpoint_result[1],
                    "checkpointed_frames": checkpoint_result[2],
                },
            }
        except sqlite3.OperationalError as exc:
            if not is_locked_error(exc):
                raise
            last_error = exc
            emit_progress(
                progress,
                "progress",
                "database",
                "数据库正在忙，继续等待恢复...",
                started_at=started_at,
                done=attempt,
                total=WRITE_LOCK_RETRY_LIMIT,
                extra={"attempt": attempt, "action": "waiting_for_lock"},
            )
            if attempt >= WRITE_LOCK_RETRY_LIMIT:
                waited_seconds = (time.monotonic() - started_at)
                raise RuntimeError(
                    "Codex 当前正在写入本地历史数据库，"
                    f"已等待 {waited_seconds:.1f} 秒仍无法完成还原。"
                    "请等当前回复、工具调用或自动保存结束后再试一次。"
                ) from exc
            time.sleep(WRITE_LOCK_RETRY_DELAY_SECONDS)

    raise RuntimeError("Database restore retry loop ended unexpectedly.") from last_error


def get_status(paths: Paths) -> dict[str, object]:
    ensure_environment(paths)
    config_text = read_text(paths.config_path)
    configured_provider = parse_current_provider(config_text)
    current_model = parse_current_model(config_text)
    session_records = scan_session_records(paths)
    session_provider_counts = ordered_counts([record.model_provider for record in session_records])
    session_model_counts = ordered_counts([record.model or "(empty)" for record in session_records])
    should_check_index = paths.session_index_path.exists() or paths.sessions_dir.exists()
    index_entries = read_session_index(paths)

    with connect_db(paths.db_path, readonly=True) as conn:
        columns = ensure_thread_schema(conn)
        current_provider = configured_provider or infer_current_provider(conn)
        counts = query_provider_counts(conn)
        model_counts = query_model_counts(conn) if "model" in columns else OrderedDict()
        provider_model_counts = query_provider_model_counts(conn) if "model" in columns else []
        cwd_counts = query_cwd_counts(conn) if "cwd" in columns else []
        total_threads = int(conn.execute("SELECT COUNT(*) FROM threads").fetchone()[0])
        provider_movable = count_mismatched(conn, "model_provider", current_provider)
        model_movable = count_mismatched(conn, "model", current_model) if "model" in columns else None
        where_parts = ["model_provider IS NULL OR model_provider <> ?"]
        params: list[str] = [current_provider]
        if "model" in columns and current_model:
            where_parts.append("model IS NULL OR model <> ?")
            params.append(current_model)
        where_sql = " OR ".join(f"({part})" for part in where_parts)
        db_movable_ids = {str(row["id"]) for row in conn.execute(f"SELECT id FROM threads WHERE {where_sql}", params)}
        db_thread_query = "SELECT id FROM threads WHERE archived = 0" if "archived" in columns else "SELECT id FROM threads"
        db_thread_ids = {str(row["id"]) for row in conn.execute(db_thread_query)}
        missing_index_ids = db_thread_ids - set(index_entries) if should_check_index else set()
    session_movable_ids = {
        record.thread_id
        for record in session_records
        if record.model_provider != current_provider
        or (current_model is not None and record.model != current_model)
    }
    sync_candidate_ids = db_movable_ids | session_movable_ids | missing_index_ids

    return {
        "codex_home": str(paths.codex_home),
        "config_path": str(paths.config_path),
        "db_path": str(paths.db_path),
        "session_index_path": str(paths.session_index_path),
        "sessions_dir": str(paths.sessions_dir),
        "backup_dir": str(paths.backup_dir),
        "current_provider": current_provider,
        "current_model": current_model,
        "total_threads": total_threads,
        "movable_threads": len(sync_candidate_ids),
        "provider_movable_threads": provider_movable,
        "model_movable_threads": model_movable,
        "movable_database_threads": len(db_movable_ids),
        "movable_session_threads": len(session_movable_ids),
        "missing_session_index_entries": len(missing_index_ids),
        "indexed_threads": len(index_entries),
        "session_file_count": len(session_records),
        "provider_counts": counts_to_rows(counts),
        "model_counts": model_counts_to_rows(model_counts),
        "provider_model_counts": provider_model_counts,
        "cwd_counts": cwd_counts,
        "session_provider_counts": counts_to_rows(session_provider_counts),
        "session_model_counts": model_counts_to_rows(session_model_counts),
        "backups": list_backups(paths),
    }


def make_backup(
    paths: Paths,
    label: str,
    progress: ProgressCallback | None = None,
) -> Path:
    started_at = time.monotonic()
    emit_progress(progress, "progress", "backup", "正在检查备份环境...", started_at=started_at)
    ensure_environment(paths)
    paths.backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = unique_backup_path(paths, label)
    emit_progress(progress, "progress", "backup", "正在复制数据库备份...", started_at=started_at)

    def report_backup_progress(status: int, remaining: int, total: int) -> None:
        if total <= 0:
            return
        done = max(0, total - remaining)
        if not should_emit_count_progress(done, total):
            return
        emit_progress(
            progress,
            "progress",
            "backup",
            "正在复制数据库备份...",
            started_at=started_at,
            done=done,
            total=total,
            extra={"sqlite_status": status},
        )

    with connect_db(paths.db_path, readonly=True) as source, connect_db(backup_path, readonly=False) as target:
        source.backup(target, pages=128, progress=report_backup_progress)
    emit_progress(progress, "progress", "backup", "正在保存侧边栏索引和会话元数据...", started_at=started_at)
    snapshot_metadata(paths, backup_path)
    backup_path.touch()
    emit_progress(progress, "progress", "backup", "备份已创建。", started_at=started_at, extra={"label": label})
    return backup_path


def sync_to_current_provider(
    paths: Paths,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    total_started_at = time.monotonic()
    emit_progress(progress, "progress", "scan", "正在扫描当前 Codex 历史状态...", started_at=total_started_at)
    status_before = get_status(paths)
    emit_progress(
        progress,
        "progress",
        "scan",
        "当前状态扫描完成。",
        started_at=total_started_at,
        done=status_before["total_threads"],
        total=status_before["total_threads"],
        extra={
            "movable_threads": status_before["movable_threads"],
            "session_file_count": status_before["session_file_count"],
        },
    )
    current_provider = str(status_before["current_provider"])
    raw_current_model = status_before.get("current_model")
    current_model = str(raw_current_model) if raw_current_model else None

    backup_started_at = time.monotonic()
    backup_path = make_backup(paths, "pre-sync", progress)
    backup_duration_ms = elapsed_ms(backup_started_at)

    db_summary = update_provider_assignments(paths, current_provider, current_model, progress)
    session_summary = sync_session_records(paths, current_provider, current_model, progress)

    with connect_db(paths.db_path, readonly=True) as conn:
        index_summary = rebuild_session_index(paths, conn, progress)

    emit_progress(progress, "progress", "status", "正在刷新同步后的状态...", started_at=total_started_at)
    status_after = get_status(paths)
    emit_progress(progress, "progress", "status", "同步后的状态已刷新。", started_at=total_started_at)
    return {
        "action": "sync",
        "current_provider": current_provider,
        "current_model": current_model,
        "synced_fields": db_summary["synced_fields"],
        "updated_rows": db_summary["updated_rows"],
        "updated_session_files": session_summary["updated_session_files"],
        "skipped_session_files": session_summary["skipped_session_files"],
        "provider_movable_threads": status_before["provider_movable_threads"],
        "model_movable_threads": status_before["model_movable_threads"],
        "backup_path": str(backup_path),
        "before_counts": db_summary["before_counts"],
        "after_counts": db_summary["after_counts"],
        "before_model_counts": db_summary["before_model_counts"],
        "after_model_counts": db_summary["after_model_counts"],
        "session_before_counts": session_summary["session_before_counts"],
        "session_after_counts": session_summary["session_after_counts"],
        "session_before_model_counts": session_summary["session_before_model_counts"],
        "session_after_model_counts": session_summary["session_after_model_counts"],
        "checkpoint": db_summary["checkpoint"],
        "lock_wait_ms": db_summary["lock_wait_ms"],
        "lock_attempts": db_summary["attempts"],
        "rewritten_index_entries": index_summary["rewritten_index_entries"],
        "missing_session_index_entries_before": index_summary["missing_session_index_entries_before"],
        "preserved_index_only_entries": index_summary["preserved_index_only_entries"],
        "timing": {
            "backup_ms": backup_duration_ms,
            "database_ms": db_summary["lock_wait_ms"],
            "session_ms": session_summary["duration_ms"],
            "index_ms": index_summary["duration_ms"],
            "total_ms": elapsed_ms(total_started_at),
        },
        "status": status_after,
    }


def resolve_backup(paths: Paths, requested_path: str | None) -> Path:
    if requested_path:
        backup = Path(requested_path).expanduser()
    else:
        backups = list_backups(paths, limit=1)
        if not backups:
            raise RuntimeError("No backup files were found.")
        backup = Path(backups[0]["path"])
    if not backup.exists():
        raise RuntimeError(f"Backup file does not exist: {backup}")
    return backup


def create_manual_backup(
    paths: Paths,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    total_started_at = time.monotonic()
    backup_path = make_backup(paths, "manual", progress)
    return {
        "action": "backup",
        "backup_path": str(backup_path),
        "timing": {"total_ms": elapsed_ms(total_started_at)},
    }


def restore_backup(
    paths: Paths,
    backup_path: str | None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    total_started_at = time.monotonic()
    emit_progress(progress, "progress", "restore", "正在检查恢复环境...", started_at=total_started_at)
    ensure_environment(paths)
    chosen_backup = resolve_backup(paths, backup_path)

    backup_started_at = time.monotonic()
    restore_snapshot = make_backup(paths, "pre-restore", progress)
    backup_duration_ms = elapsed_ms(backup_started_at)

    restore_db_started_at = time.monotonic()
    restore_db_summary = restore_database_with_retry(paths, chosen_backup, progress)
    restore_db_duration_ms = elapsed_ms(restore_db_started_at)

    restore_summary = restore_metadata(paths, chosen_backup, progress)
    # 恢复后统一重建索引，让数据库与侧边栏索引重新对齐。
    with connect_db(paths.db_path, readonly=True) as conn:
        index_summary = rebuild_session_index(paths, conn, progress)

    emit_progress(progress, "progress", "status", "正在刷新恢复后的状态...", started_at=total_started_at)
    status_after = get_status(paths)
    emit_progress(progress, "progress", "status", "恢复后的状态已刷新。", started_at=total_started_at)
    return {
        "action": "restore",
        "restored_from": str(chosen_backup),
        "safety_backup": str(restore_snapshot),
        "metadata_restore": restore_summary,
        "checkpoint": restore_db_summary["checkpoint"],
        "lock_wait_ms": restore_db_summary["lock_wait_ms"],
        "lock_attempts": restore_db_summary["attempts"],
        "rewritten_index_entries": index_summary["rewritten_index_entries"],
        "timing": {
            "backup_ms": backup_duration_ms,
            "database_ms": restore_db_duration_ms,
            "metadata_ms": restore_summary["duration_ms"],
            "index_ms": index_summary["duration_ms"],
            "total_ms": elapsed_ms(total_started_at),
        },
        "status": status_after,
    }


def to_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2)


def to_jsonl(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def run_command(args: argparse.Namespace, paths: Paths, progress: ProgressCallback | None = None) -> dict[str, object]:
    if args.command == "status":
        return get_status(paths)
    if args.command == "sync":
        return sync_to_current_provider(paths, progress)
    if args.command == "restore":
        return restore_backup(paths, args.backup, progress)
    if args.command == "backup":
        return create_manual_backup(paths, progress)
    raise RuntimeError(f"Unsupported command: {args.command}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Codex history sync helper")
    parser.add_argument("--codex-home", help="Override Codex home directory")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true", help="Emit JSON output")
    output_group.add_argument("--jsonl", action="store_true", help="Emit newline-delimited progress JSON")

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="Show current provider/thread status")
    subparsers.add_parser("sync", help="Move all thread providers to the current provider")
    restore_parser = subparsers.add_parser("restore", help="Restore from a backup")
    restore_parser.add_argument("--backup", help="Backup file path; newest backup is used when omitted")
    subparsers.add_parser("backup", help="Create a manual backup")

    args = parser.parse_args()
    paths = resolve_paths(args.codex_home)

    def emit_jsonl(payload: dict[str, object]) -> None:
        print(to_jsonl(payload), flush=True)

    progress = emit_jsonl if args.jsonl else None

    try:
        payload = run_command(args, paths, progress)
    except Exception as exc:
        error_payload = {"ok": False, "error": str(exc)}
        if args.jsonl:
            emit_jsonl(
                build_progress_event(
                    "error",
                    args.command or "unknown",
                    str(exc),
                    extra={"error": str(exc)},
                )
            )
        elif args.json:
            print(to_json(error_payload))
        else:
            print(error_payload["error"])
        return 1

    if isinstance(payload, dict):
        payload["ok"] = True

    if args.jsonl:
        emit_jsonl(
            build_progress_event(
                "result",
                args.command,
                "操作完成。",
                done=1,
                total=1,
                extra=payload,
            )
        )
    elif args.json:
        print(to_json(payload))
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
