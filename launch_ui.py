#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import threading
import tkinter as tk
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk

TOOL_ROOT = Path(__file__).parent
BACKEND_PATH = TOOL_ROOT / "sync_backend.py"
STREAM_EVENT_KEYS = {"event", "stage", "message", "done", "total", "elapsed_ms", "extra"}


def invoke_backend(*args: str) -> dict:
    cmd = [sys.executable, str(BACKEND_PATH), "--json", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8")
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if not stdout:
        if stderr:
            raise RuntimeError(stderr)
        raise RuntimeError("后端没有返回任何内容。")
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        detail = stdout
        if stderr:
            detail = f"{detail}\n\nstderr:\n{stderr}"
        raise RuntimeError(f"后端 JSON 解析失败。\n原始错误: {exc}\n返回内容:\n{detail}") from exc
    if result.returncode != 0 or not data.get("ok"):
        error = data.get("error") or f"后端执行失败。\n{stdout}"
        if stderr:
            error = f"{error}\n\nstderr:\n{stderr}"
        raise RuntimeError(error)
    return data


def parse_backend_stream_line(line: str) -> tuple[dict | None, str | None]:
    raw = line.strip()
    if not raw:
        return None, None
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None, f"后端输出非 JSON 行: {raw}"
    if not isinstance(event, dict):
        return None, f"后端输出不是 JSON 对象: {raw}"
    missing = sorted(STREAM_EVENT_KEYS - set(event))
    if missing:
        return None, f"后端进度事件缺少字段: {', '.join(missing)}"
    return event, None


def diagnostic_stream_event(message: str) -> dict:
    return {
        "event": "diagnostic",
        "stage": "stream",
        "message": message,
        "done": None,
        "total": None,
        "elapsed_ms": 0,
        "extra": {},
    }


def invoke_backend_stream(*args: str, on_event: Callable[[dict], None] | None = None) -> dict:
    cmd = [sys.executable, str(BACKEND_PATH), "--jsonl", *args]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    result: dict | None = None
    stream_error = ""
    diagnostics: list[str] = []

    if process.stdout is None:
        raise RuntimeError("无法读取后端进度输出。")

    for line in process.stdout:
        event, diagnostic = parse_backend_stream_line(line)
        if diagnostic:
            diagnostics.append(diagnostic)
            if on_event:
                on_event(diagnostic_stream_event(diagnostic))
            continue
        if event is None:
            continue
        if on_event:
            on_event(event)
        event_type = str(event.get("event") or "")
        if event_type == "result":
            extra = event.get("extra")
            if isinstance(extra, dict):
                result = extra
        elif event_type == "error":
            extra = event.get("extra")
            if isinstance(extra, dict):
                stream_error = str(extra.get("error") or event.get("message") or "后端执行失败。")
            else:
                stream_error = str(event.get("message") or "后端执行失败。")

    stderr = process.stderr.read().strip() if process.stderr is not None else ""
    returncode = process.wait()
    if stream_error:
        if stderr:
            stream_error = f"{stream_error}\n\nstderr:\n{stderr}"
        raise RuntimeError(stream_error)
    if returncode != 0:
        detail = stderr or "\n".join(diagnostics) or "后端没有返回错误详情。"
        raise RuntimeError(f"后端执行失败，退出码 {returncode}。\n{detail}")
    if result is None:
        detail = stderr or "\n".join(diagnostics)
        if detail:
            raise RuntimeError(f"后端没有返回最终结果。\n{detail}")
        raise RuntimeError("后端没有返回最终结果。")
    if not result.get("ok"):
        raise RuntimeError(str(result.get("error") or "后端执行失败。"))
    return result


def format_counts(counts) -> str:
    if not counts:
        return "无"
    return ", ".join(f"{r['provider']}={r['count']}" for r in counts)


def format_model_counts(counts) -> str:
    if not counts:
        return "无"
    return ", ".join(f"{r['model']}={r['count']}" for r in counts)


def format_duration(ms) -> str:
    if ms is None:
        return "0 秒"
    return f"{round(float(ms) / 1000, 1)} 秒"


def format_stream_event_message(event: dict) -> str:
    message = str(event.get("message") or "正在处理...")
    done = event.get("done")
    total = event.get("total")
    elapsed_ms = event.get("elapsed_ms")
    details = []
    extra = event.get("extra")
    if isinstance(done, int) and isinstance(total, int) and total > 0:
        details.append(f"{done}/{total}")
    if isinstance(extra, dict):
        labeled_counts = [
            ("updated", "已更新"),
            ("updated_rows", "数据库更新"),
            ("skipped", "已跳过"),
            ("missing", "缺失"),
            ("rewritten_entries", "索引条目"),
        ]
        for key, label in labeled_counts:
            value = extra.get(key)
            if isinstance(value, int):
                details.append(f"{label} {value}")
    if isinstance(elapsed_ms, int) and elapsed_ms > 0:
        details.append(f"已用 {format_duration(elapsed_ms)}")
    if details:
        return f"{message}（{'，'.join(details)}）"
    return message


def get_friendly_status(status: dict) -> str:
    movable = int(status.get("movable_threads", 0))
    if movable <= 0:
        return "一切正常：历史记录已经挂到当前账号/Provider。"
    parts = []
    db_movable = int(status.get("movable_database_threads", 0))
    if db_movable > 0:
        parts.append(f"{db_movable} 条数据库记录待迁移")
    model_movable = status.get("model_movable_threads")
    if model_movable is not None and int(model_movable) > 0:
        parts.append(f"{int(model_movable)} 条模型归属待修正")
    session_movable = int(status.get("movable_session_threads", 0))
    if session_movable > 0:
        parts.append(f"{session_movable} 个会话文件待修正")
    missing_index = int(status.get("missing_session_index_entries", 0))
    if missing_index > 0:
        parts.append(f"{missing_index} 条侧边栏索引待补回")
    return "需要同步：" + "，".join(parts) + "。"


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Codex 历史找回助手")
        self.resizable(True, True)
        self.minsize(900, 700)

        self._latest_state: dict | None = None
        self._backup_map: dict[str, str] = {}

        self._build_ui()
        self.after(100, self._initial_refresh)

    def _build_ui(self) -> None:
        main = ttk.Frame(self, padding="18 18 18 12")
        main.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        main.columnconfigure(0, weight=1)

        row = 0

        title_lbl = ttk.Label(main, text="Codex 历史找回助手", font=("Helvetica Neue", 20, "bold"))
        title_lbl.grid(row=row, column=0, sticky="w", pady=(0, 4))
        row += 1

        intro_lbl = ttk.Label(
            main,
            text='用于把"换了 API / Provider / 登录方式后看不见的本地历史"重新挂回当前 Codex。Codex 开着也可以试，工具会等待数据库空闲。',
            wraplength=840,
            foreground="#4D5969",
        )
        intro_lbl.grid(row=row, column=0, sticky="w", pady=(0, 8))
        row += 1

        self.status_label = ttk.Label(
            main,
            text="正在读取状态...",
            font=("Helvetica Neue", 11, "bold"),
            foreground="#1C54A0",
        )
        self.status_label.grid(row=row, column=0, sticky="w", pady=(0, 4))
        row += 1

        self.progress = ttk.Progressbar(main, mode="indeterminate", length=840)
        self.progress.grid(row=row, column=0, sticky="ew", pady=(0, 8))
        self.progress.grid_remove()
        row += 1

        self.provider_label = ttk.Label(main, text="当前账号/Provider:")
        self.provider_label.grid(row=row, column=0, sticky="w")
        row += 1

        self.model_label = ttk.Label(main, text="当前模型:")
        self.model_label.grid(row=row, column=0, sticky="w")
        row += 1

        self.summary_label = ttk.Label(main, text="历史线程:")
        self.summary_label.grid(row=row, column=0, sticky="w")
        row += 1

        self.repair_label = ttk.Label(main, text="待修复:")
        self.repair_label.grid(row=row, column=0, sticky="w")
        row += 1

        self.path_label = ttk.Label(main, text="数据位置:", wraplength=840)
        self.path_label.grid(row=row, column=0, sticky="w", pady=(0, 10))
        row += 1

        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=row, column=0, sticky="w", pady=(0, 12))

        self.refresh_btn = ttk.Button(btn_frame, text="重新检查", command=self._on_refresh, width=12)
        self.refresh_btn.pack(side="left", padx=(0, 6))

        self.sync_btn = ttk.Button(btn_frame, text="开始找回历史", command=self._on_sync, width=16)
        self.sync_btn.pack(side="left", padx=(0, 6))

        self.backup_btn = ttk.Button(btn_frame, text="先做备份", command=self._on_backup, width=12)
        self.backup_btn.pack(side="left", padx=(0, 6))

        self.open_backups_btn = ttk.Button(
            btn_frame, text="打开备份目录", command=self._on_open_backups, width=14
        )
        self.open_backups_btn.pack(side="left")
        row += 1

        middle = ttk.Frame(main)
        middle.grid(row=row, column=0, sticky="nsew", pady=(0, 10))
        middle.columnconfigure(0, weight=1)
        middle.columnconfigure(1, weight=1)
        main.rowconfigure(row, weight=1)
        row += 1

        providers_frame = ttk.LabelFrame(middle, text="历史归属", padding="8 8 8 8")
        providers_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        providers_frame.rowconfigure(0, weight=1)
        providers_frame.columnconfigure(0, weight=1)

        self.providers_tree = ttk.Treeview(
            providers_frame,
            columns=("provider", "count", "location", "current"),
            show="headings",
            height=7,
        )
        self.providers_tree.heading("provider", text="账号/Provider")
        self.providers_tree.heading("count", text="数量")
        self.providers_tree.heading("location", text="位置")
        self.providers_tree.heading("current", text="状态")
        self.providers_tree.column("provider", width=150)
        self.providers_tree.column("count", width=60, anchor="center")
        self.providers_tree.column("location", width=80, anchor="center")
        self.providers_tree.column("current", width=50, anchor="center")
        prov_scroll = ttk.Scrollbar(providers_frame, orient="vertical", command=self.providers_tree.yview)
        self.providers_tree.configure(yscrollcommand=prov_scroll.set)
        self.providers_tree.grid(row=0, column=0, sticky="nsew")
        prov_scroll.grid(row=0, column=1, sticky="ns")

        backups_frame = ttk.LabelFrame(middle, text="安全备份", padding="8 8 8 8")
        backups_frame.grid(row=0, column=1, sticky="nsew")
        backups_frame.rowconfigure(0, weight=1)
        backups_frame.columnconfigure(0, weight=1)

        self.backup_listbox = tk.Listbox(backups_frame, height=6, font=("Menlo", 9))
        backup_scroll = ttk.Scrollbar(backups_frame, orient="vertical", command=self.backup_listbox.yview)
        self.backup_listbox.configure(yscrollcommand=backup_scroll.set)
        self.backup_listbox.grid(row=0, column=0, sticky="nsew")
        backup_scroll.grid(row=0, column=1, sticky="ns")

        restore_btn_frame = ttk.Frame(backups_frame)
        restore_btn_frame.grid(row=1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self.restore_btn = ttk.Button(
            restore_btn_frame, text="恢复选中备份", command=self._on_restore_selected, width=14
        )
        self.restore_btn.pack(side="left", padx=(0, 6))
        self.restore_latest_btn = ttk.Button(
            restore_btn_frame, text="恢复最新备份", command=self._on_restore_latest, width=14
        )
        self.restore_latest_btn.pack(side="left")

        log_frame = ttk.LabelFrame(main, text="操作日志", padding="4 4 4 4")
        log_frame.grid(row=row, column=0, sticky="ew")
        log_frame.columnconfigure(0, weight=1)
        row += 1

        self.log_text = tk.Text(log_frame, height=7, state="disabled", font=("Menlo", 9), wrap="word")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        self.log_text.grid(row=0, column=0, sticky="ew")
        log_scroll.grid(row=0, column=1, sticky="ns")

    def _all_buttons(self) -> list:
        return [
            self.refresh_btn,
            self.sync_btn,
            self.backup_btn,
            self.open_backups_btn,
            self.restore_btn,
            self.restore_latest_btn,
        ]

    def _show_indeterminate_progress(self) -> None:
        self.progress.stop()
        self.progress.configure(mode="indeterminate", maximum=100, value=0)
        self.progress.grid()
        self.progress.start(10)

    def _show_determinate_progress(self, done: int, total: int) -> None:
        self.progress.stop()
        safe_total = max(total, 1)
        safe_done = min(max(done, 0), safe_total)
        self.progress.configure(mode="determinate", maximum=safe_total, value=safe_done)
        self.progress.grid()

    def _set_busy(self, busy: bool, message: str = "") -> None:
        state = "disabled" if busy else "normal"
        for btn in self._all_buttons():
            btn.configure(state=state)
        if busy:
            self.status_label.configure(text=message)
            self._show_indeterminate_progress()
        else:
            self.progress.stop()
            self.progress.configure(mode="indeterminate", maximum=100, value=0)
            self.progress.grid_remove()
            if self._latest_state:
                self.status_label.configure(text=get_friendly_status(self._latest_state))
            else:
                self.status_label.configure(text="准备就绪")

    def _append_log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{timestamp}] {message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _handle_stream_event(self, event: dict) -> None:
        event_type = str(event.get("event") or "")
        message = format_stream_event_message(event)
        if event_type == "diagnostic":
            self._append_log(message)
            return
        if event_type == "error":
            self.status_label.configure(text=message)
            self._append_log(f"后端错误: {message}")
            return
        if event_type != "progress":
            return

        self.status_label.configure(text=message)
        done = event.get("done")
        total = event.get("total")
        if isinstance(done, int) and isinstance(total, int) and total > 0:
            self._show_determinate_progress(done, total)
        else:
            self._show_indeterminate_progress()
        self._append_log(message)

    def _stream_event_to_ui(self, event: dict) -> None:
        self.after(0, lambda event=event: self._handle_stream_event(event))

    def _apply_state(self, status: dict) -> None:
        self._latest_state = status

        self.provider_label.configure(text=f"当前账号/Provider: {status.get('current_provider', '')}")

        current_model = status.get("current_model")
        model_movable = status.get("model_movable_threads")
        if current_model:
            self.model_label.configure(text=f"当前模型: {current_model}    待修正: {model_movable}")
        else:
            self.model_label.configure(text="当前模型: 未读取到")

        self.summary_label.configure(
            text=(
                f"历史线程: {status.get('total_threads', 0)}"
                f"    会话文件: {status.get('session_file_count', 0)}"
                f"    侧边栏索引: {status.get('indexed_threads', 0)}"
            )
        )
        self.repair_label.configure(
            text=(
                f"待修复: {status.get('movable_threads', 0)}"
                f"    数据库: {status.get('movable_database_threads', 0)}"
                f"    模型: {status.get('model_movable_threads', 0)}"
                f"    会话文件: {status.get('movable_session_threads', 0)}"
                f"    索引: {status.get('missing_session_index_entries', 0)}"
            )
        )
        self.path_label.configure(text=f"数据位置: {status.get('codex_home', '')}")
        self.status_label.configure(text=get_friendly_status(status))

        for item in self.providers_tree.get_children():
            self.providers_tree.delete(item)
        current_provider = status.get("current_provider", "")
        for r in status.get("provider_counts", []):
            is_current = "当前" if r["provider"] == current_provider else ""
            self.providers_tree.insert("", "end", values=(r["provider"], r["count"], "数据库", is_current))
        for r in status.get("session_provider_counts", []):
            is_current = "当前" if r["provider"] == current_provider else ""
            self.providers_tree.insert("", "end", values=(r["provider"], r["count"], "会话文件", is_current))

        self.backup_listbox.delete(0, "end")
        self._backup_map = {}
        for backup in status.get("backups", []):
            label = f"{backup['modified_at']}    {backup['name']}"
            self._backup_map[label] = backup["path"]
            self.backup_listbox.insert("end", label)

    def _refresh_state(self) -> None:
        status = invoke_backend("status")
        self.after(0, lambda: self._apply_state(status))
        self.after(0, lambda: self._append_log(f"状态已刷新：{get_friendly_status(status)}"))

    def _initial_refresh(self) -> None:
        def run() -> None:
            try:
                self._refresh_state()
            except Exception as exc:
                msg = str(exc)
                self.after(0, lambda: messagebox.showerror("启动失败", msg))
                self.after(0, lambda: self._append_log(f"初始化状态失败: {msg}"))
            finally:
                self.after(0, lambda: self._set_busy(False))

        self._set_busy(True, "正在读取状态...")
        threading.Thread(target=run, daemon=True).start()

    def _on_refresh(self) -> None:
        def run() -> None:
            try:
                self._refresh_state()
            except Exception as exc:
                msg = str(exc)
                self.after(0, lambda: messagebox.showerror("刷新失败", msg))
                self.after(0, lambda: self._append_log(f"刷新失败: {msg}"))
            finally:
                self.after(0, lambda: self._set_busy(False))

        self._set_busy(True, "正在刷新...")
        threading.Thread(target=run, daemon=True).start()

    def _on_sync(self) -> None:
        if not self._latest_state:
            return
        movable = int(self._latest_state.get("movable_threads", 0))
        if movable <= 0:
            messagebox.showinfo("无需同步", "当前已经整理好了，不需要再同步。")
            self._append_log("同步跳过：当前已经没有需要修复的历史。")
            return
        provider = self._latest_state.get("current_provider", "")
        model = self._latest_state.get("current_model", "")
        msg = (
            f"将把旧账号/Provider/模型下的本地历史挂回当前设置：\n"
            f"Provider: {provider}\n模型: {model}\n\n"
            f"本次预计处理：{movable} 项\n"
            f"包含数据库记录、会话文件和侧边栏索引。\n\n"
            f"工具会先自动备份。Codex 正在运行也可以，但如果它正在写入历史，可能会等待几秒。"
        )
        if not messagebox.askokcancel("开始找回历史？", msg):
            self._append_log("用户取消了同步。")
            return

        def run() -> None:
            try:
                result = invoke_backend_stream("sync", on_event=self._stream_event_to_ui)

                def update() -> None:
                    self._append_log(
                        f"同步完成。数据库更新 {result.get('updated_rows', 0)} 条，"
                        f"会话文件更新 {result.get('updated_session_files', 0)} 个，"
                        f"跳过 {result.get('skipped_session_files', 0)} 个。"
                    )
                    timing = result.get("timing") or {}
                    self._append_log(
                        f"等待数据库空闲: {format_duration(result.get('lock_wait_ms'))}，"
                        f"总耗时: {format_duration(timing.get('total_ms'))}。"
                    )
                    self._append_log(f"数据库同步前: {format_counts(result.get('before_counts'))}")
                    self._append_log(f"数据库同步后: {format_counts(result.get('after_counts'))}")
                    self._append_log(f"模型同步前: {format_model_counts(result.get('before_model_counts'))}")
                    self._append_log(f"模型同步后: {format_model_counts(result.get('after_model_counts'))}")
                    self._append_log(
                        f"会话文件同步前: {format_counts(result.get('session_before_counts'))}"
                    )
                    self._append_log(
                        f"会话文件同步后: {format_counts(result.get('session_after_counts'))}"
                    )
                    self._append_log(
                        f"侧边栏索引已重建: {result.get('rewritten_index_entries', 0)} 条，"
                        f"补回 {result.get('missing_session_index_entries_before', 0)} 条。"
                    )
                    self._append_log(f"备份文件: {result.get('backup_path')}")
                    if result.get("status"):
                        self._apply_state(result["status"])
                    messagebox.showinfo("同步完成", "同步完成。如果侧边栏没有马上刷新，重新打开 Codex 即可。")

                self.after(0, update)
            except Exception as exc:
                msg = str(exc)
                self.after(0, lambda: messagebox.showerror("同步失败", msg))
                self.after(0, lambda: self._append_log(f"同步失败: {msg}"))
            finally:
                self.after(0, lambda: self._set_busy(False))

        self._set_busy(True, "正在同步历史，Codex 忙的时候会自动等一会儿...")
        threading.Thread(target=run, daemon=True).start()

    def _on_backup(self) -> None:
        def run() -> None:
            try:
                result = invoke_backend_stream("backup", on_event=self._stream_event_to_ui)
                self.after(0, lambda: self.status_label.configure(text="正在刷新备份列表..."))
                status = invoke_backend("status")

                def update() -> None:
                    self._append_log(f"手动备份完成: {result.get('backup_path')}")
                    timing = result.get("timing") or {}
                    self._append_log(f"备份耗时: {format_duration(timing.get('total_ms'))}")
                    self._apply_state(status)
                    self._append_log(f"状态已刷新：{get_friendly_status(status)}")

                self.after(0, update)
            except Exception as exc:
                msg = str(exc)
                self.after(0, lambda: messagebox.showerror("备份失败", msg))
                self.after(0, lambda: self._append_log(f"备份失败: {msg}"))
            finally:
                self.after(0, lambda: self._set_busy(False))

        self._set_busy(True, "正在创建安全备份...")
        threading.Thread(target=run, daemon=True).start()

    def _on_open_backups(self) -> None:
        try:
            folder = self._latest_state.get("backup_dir", "") if self._latest_state else ""
            if not folder:
                messagebox.showwarning("提示", "请先刷新状态后再打开备份目录。")
                return
            Path(folder).mkdir(parents=True, exist_ok=True)
            subprocess.Popen(["open", folder])
            self._append_log(f"已打开备份目录: {folder}")
        except Exception as exc:
            messagebox.showerror("打开目录失败", str(exc))
            self._append_log(f"打开备份目录失败: {exc}")

    def _on_restore_selected(self) -> None:
        selection = self.backup_listbox.curselection()
        if not selection:
            messagebox.showwarning("未选择备份", "请先在右侧选一个备份。")
            return
        label = self.backup_listbox.get(selection[0])
        backup_path = self._backup_map.get(label)
        if not backup_path:
            messagebox.showerror("错误", "无法解析选中的备份路径。")
            return
        msg = f"将恢复这个备份：\n{backup_path}\n\n恢复前会再自动做一份当前状态备份，方便反悔。"
        if not messagebox.askokcancel("确认恢复？", msg):
            self._append_log("用户取消了恢复。")
            return
        self._run_restore("restore", "--backup", backup_path)

    def _on_restore_latest(self) -> None:
        if not messagebox.askokcancel(
            "确认恢复最新备份？", "将恢复最新备份，并在恢复前再做一次当前状态备份。"
        ):
            self._append_log("用户取消了恢复最新备份。")
            return
        self._run_restore("restore")

    def _run_restore(self, *args: str) -> None:
        def run() -> None:
            try:
                result = invoke_backend_stream(*args, on_event=self._stream_event_to_ui)

                def update() -> None:
                    self._append_log(f"恢复完成。来源备份: {result.get('restored_from')}")
                    self._append_log(f"恢复前安全备份: {result.get('safety_backup')}")
                    timing = result.get("timing") or {}
                    self._append_log(f"恢复耗时: {format_duration(timing.get('total_ms'))}")
                    if result.get("status"):
                        self._apply_state(result["status"])
                    messagebox.showinfo("恢复完成", "恢复完成。建议重新打开 Codex 再看历史列表。")

                self.after(0, update)
            except Exception as exc:
                msg = str(exc)
                self.after(0, lambda: messagebox.showerror("恢复失败", msg))
                self.after(0, lambda: self._append_log(f"恢复失败: {msg}"))
            finally:
                self.after(0, lambda: self._set_busy(False))

        self._set_busy(True, "正在恢复备份...")
        threading.Thread(target=run, daemon=True).start()


if __name__ == "__main__":
    app = App()
    app.mainloop()
