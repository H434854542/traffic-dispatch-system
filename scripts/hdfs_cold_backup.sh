#!/bin/bash
# 注意：HDFS 冷备为规划项，集群尚未部署 HDFS，本脚本未挂入 crontab，仅作后续扩展占位。
DATE=$(date +%Y%m%d)
BACKUP_ROOT="/backup/cold"
LOCAL_BACKUP_DIR="/data/backup"
HDFS_NAMENODE="hdfs://k3s-node1:9000"

echo "[$(date +'%Y-%m-%d %H:%M:%S')] 开始每日冷备份"

hdfs dfs -fs $HDFS_NAMENODE -mkdir -p $BACKUP_ROOT/$DATE

hdfs dfs -fs $HDFS_NAMENODE -put -f $LOCAL_BACKUP_DIR/mysql/* $BACKUP_ROOT/$DATE/mysql/ 2>/dev/null
hdfs dfs -fs $HDFS_NAMENODE -put -f $LOCAL_BACKUP_DIR/redis/* $BACKUP_ROOT/$DATE/redis/ 2>/dev/null

OLD_BACKUPS=$(hdfs dfs -fs $HDFS_NAMENODE -ls $BACKUP_ROOT/ | awk '{print $8}' | grep -v '^$' | sort | head -n -30)
if [ -n "$OLD_BACKUPS" ]; then
    echo "$OLD_BACKUPS" | xargs -I {} hdfs dfs -fs $HDFS_NAMENODE -rm -r {}
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] 已清理30天前的冷备份"
fi

echo "[$(date +'%Y-%m-%d %H:%M:%S')] 每日冷备份完成！"
