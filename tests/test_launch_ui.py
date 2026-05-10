from __future__ import annotations

import unittest

from launch_ui import diagnostic_stream_event, format_stream_event_message, parse_backend_stream_line


STREAM_EVENT = {
    "event": "progress",
    "stage": "sessions",
    "message": "正在同步会话文件元数据...",
    "done": 2,
    "total": 5,
    "elapsed_ms": 1200,
    "extra": {"updated": 1, "skipped": 1},
}


class LaunchUiStreamTests(unittest.TestCase):
    def test_parse_backend_stream_line_accepts_progress_result_and_error(self) -> None:
        for event_name in ("progress", "result", "error"):
            raw = dict(STREAM_EVENT)
            raw["event"] = event_name
            event, diagnostic = parse_backend_stream_line(
                '{"event":"%s","stage":"sessions","message":"ok","done":1,"total":2,"elapsed_ms":3,"extra":{}}'
                % event_name
            )

            self.assertIsNone(diagnostic)
            self.assertEqual(event["event"], raw["event"])

    def test_parse_backend_stream_line_reports_non_json_without_crashing(self) -> None:
        event, diagnostic = parse_backend_stream_line("plain stderr line\n")

        self.assertIsNone(event)
        self.assertIn("非 JSON", diagnostic)
        fallback = diagnostic_stream_event(diagnostic)
        self.assertEqual(fallback["event"], "diagnostic")
        self.assertEqual(fallback["stage"], "stream")

    def test_format_stream_event_message_includes_stage_counts(self) -> None:
        message = format_stream_event_message(STREAM_EVENT)

        self.assertIn("2/5", message)
        self.assertIn("已更新 1", message)
        self.assertIn("已跳过 1", message)
        self.assertIn("已用 1.2 秒", message)


if __name__ == "__main__":
    unittest.main()
