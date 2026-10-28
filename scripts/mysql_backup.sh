#!/bin/bash
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin
K3S=/usr/local/bin/k3s
DIR=/data/backup/mysql
KEEP=7
TS=$(date +%Y%m%d_%H%M%S)
for i in $(seq 1 10); do
  if $K3S kubectl get nodes >/dev/null 2>&1; then break; fi
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 等待 k3s 就绪 (${i}/10)..."
  sleep 15
  [ "$i" -eq 10 ] && { echo "[$(date +'%Y-%m-%d %H:%M:%S')] k3s 未就绪，本次备份跳过" >&2; exit 1; }
done

POD=$($K3S kubectl get pods -n schedule-system -l app=mysql \
  -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}' | awk '{print $1}')
[ -n "$POD" ] || { echo "[$TS] 找不到运行中的 mysql Pod" >&2; exit 1; }

mkdir -p "$DIR"
TMP="$DIR/mysql_full_$TS.sql"

echo "[$TS] 开始备份..."
# 核心修改：只备份 schedule_db，避免污染系统库
$K3S kubectl exec -n schedule-system "$POD" -- \
  env MYSQL_PWD='Schedule@123456' mysqldump -uroot --single-transaction --databases schedule_db \
  --routines --triggers > "$TMP"

SIZE=$(stat -c%s "$TMP")
if [ "$SIZE" -lt 1024 ]; then
  echo "[$TS] 备份失败：文件过小 (${SIZE}B)，已丢弃。" >&2
  rm -f "$TMP"
  exit 1
fi

gzip "$TMP" && gzip -t "$TMP.gz"
echo "[$TS] 成功：$TMP.gz ($(stat -c%s "$TMP.gz")B)"
find "$DIR" -name 'mysql_full_*.sql.gz' -mtime +$KEEP -delete
