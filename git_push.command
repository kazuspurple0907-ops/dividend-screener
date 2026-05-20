#!/bin/bash
cd "$(dirname "$0")"
echo "=== Git Force Push ==="
git push origin main --force
echo ""
echo "=== 完了 ==="
read -p "Enterで閉じる..."
