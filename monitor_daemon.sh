#!/bin/bash

pid=0
server_list=$1      # server_list存放路径，例如：~/server_list.conf
log_dir=$2          # log存放路径，例如：~/memcached_log/
mkdir -p $log_dir
monitor_dir=$(cd `dirname $0`; pwd)
proc_name="python3 $monitor_dir/monitor.py --servers $server_list"     # 进程名
restart_file=$log_dir/monitor_restart.log     # 日志文件
log_file=$log_dir/monitor_debug.log     # 日志文件

proc_num()          # 计算进程数
{
    num=`ps -ef | grep "$proc_name" | grep -v grep | wc -l`
    return $num
}

proc_id()           # 进程号
{
    pid=`ps -ef | grep "$proc_name" | grep -v grep | awk '{print $2}'`
}

proc_num
number=$?
if [ $number -eq 0 ]    # 判断进程是否存在
then
    eval nohup $proc_name --log_dir=$log_dir >> $log_file 2>&1 &
    sleep 2
    proc_id             # 获取新进程号
    echo memcached monitor restart log: pid = $pid, restart_time: `date` >> $restart_file  # 将新进程号和重启时间记录
fi

