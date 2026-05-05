#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if ! command -v python3 &>/dev/null; then
  echo "错误：未找到 python3，请先安装 Python 3.10 或更高版本。" >&2
  exit 1
fi

python3 "$SCRIPT_DIR/launch_ui.py"
