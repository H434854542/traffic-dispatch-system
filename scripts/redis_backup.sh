#!/bin/bash
# Redis RDB 备份脚本 —— 修正版 2026-10-28（含 k3s 就绪等待 + 三副本循环 + 大小校验）
set -euo pipefail
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin
K3S=/usr/local/bin/k3s
BACKUP_DIR="/data/backup/redis"
NAMESPACE="schedule-system"
REDIS_PASS="Redis@123456"
TS=$(date +%Y%m%d_%H%M%S)
# --- kubectl 就绪等待 ---
for i in $(seq 1 10); do
  if $K3S kubectl get nodes >/dev/null 2>&1; then break; fi
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] 等待 k3s 就绪 (${i}/10)..."
  sleep 15
  [ "$i" -eq 10 ] && { echo "[$(date +'%Y-%m-%d %H:%M:%S')] k3s 未就绪，本次备份跳过" >&2; exit 1; }
done
mkdir -p "$BACKUP_DIR"
# 遍历 redis 全部副本（0/1/2），主从都 BGSAVE 并拉取 dump.rdb，文件名带 Pod 名
for POD in $($K3S kubectl get pods -n $NAMESPACE -l app=redis \
  -o jsonpath='{.items[?(@.status.phase=="Running")].metadata.name}'); do
  echo "[$TS] $POD 开始 BGSAVE..."
  $K3S kubectl exec -n $NAMESPACE "$POD" -- \
    redis-cli -a "$REDIS_PASS" --no-auth-warning BGSAVE >/dev/null
  sleep 3
  TARGET="$BACKUP_DIR/redis_dump_${TS}_${POD}.rdb"
  $K3S kubectl cp -n $NAMESPACE "$POD":/data/dump.rdb "$TARGET"
  SIZE=$(stat -c%s "$TARGET")
  # RDB文件头部至少>50字节，小于50判定为损坏空文件
  if [ "$SIZE" -lt 50 ]; then
    echo "[$TS] $POD 备份失败：文件过小 (${SIZE}B)，已丢弃。" >&2
    rm -f "$TARGET"
    continue
  fi
  echo "[$TS] $POD 备份成功：$TARGET (${SIZE}B)"
done
# 清理 2 天前的旧备份
find "$BACKUP_DIR" -name 'redis_dump_*.rdb' -mtime +2 -delete
echo "[$TS] Redis 备份完成"
