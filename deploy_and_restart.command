#!/bin/bash
cd "$(dirname "$0")"

echo "=== ロックファイルを削除 ==="
rm -f .git/HEAD.lock

echo "=== コミット ==="
git add app.py
git commit -m "CSVインポートを全上書き方式に変更（マージ→完全置換）"

echo "=== GitHub へプッシュ ==="
git push origin main --force

echo "=== ローカルアプリを再起動 ==="
pkill -f "python3 app.py" 2>/dev/null; sleep 1
echo "アプリを起動中... http://localhost:5001"
open "http://localhost:5001" &
sleep 1
python3 app.py

read -p "Enterで閉じる..."
