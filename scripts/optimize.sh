#!/bin/bash
# 关闭 SELinux
sed -i 's/^SELINUX=.*/SELINUX=disabled/' /etc/selinux/config
setenforce 0

# 关闭 Swap
swapoff -a
sed -i 's/.*swap.*/#&/' /etc/fstab

# 关闭防火墙
systemctl stop firewalld
systemctl disable firewalld
iptables -F && iptables -X && iptables -F -t nat && iptables -X -t nat
iptables -P FORWARD ACCEPT

# 加载内核模块
modprobe overlay
modprobe br_netfilter

cat > /etc/sysctl.d/k3s.conf << EOF
net.bridge.bridge-nf-call-iptables=1
net.bridge.bridge-nf-call-ip6tables=1
net.ipv4.ip_forward=1
vm.swappiness=0
vm.overcommit_memory=1
EOF
sysctl -p /etc/sysctl.d/k3s.conf

echo "系统优化完成，请重启服务器"