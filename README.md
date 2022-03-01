# Memcached监控工具

因为当时的使用环境无root权限，所以写了`monitor_daemon.sh`来实现守护进程的目的。

如果有root权限的话，使用`supervisord`或者`systemd`当然更方便~

## 配置说明

格式示例： * * * * * ~/memcached-monitor/monitor_daemon.sh <server_list_path> <log_dir_path>

添加crontab：

```bash
* * * * * bash ~/memcached-monitor/monitor_daemon.sh ~/server_list.conf ~/memcached_log
```

## 可选配置项

目前monitor_daemon.sh中只标记了最基础的配置项<server_list_path>和<log_dir_path>，监控工具中的其余配置项没有调用，详情可执行`python3 monitor.py --help`查看

```
usage: monitor.py [-h] [--log_dir LOG_DIR] [--servers SERVERS] [--cycles CYCLES] [--interval INTERVAL] [--timeout TIMEOUT] [--debug]

optional arguments:
  -h, --help           show this help message and exit
  --log_dir LOG_DIR    log file storage dir. default: ~/memcached_log/
  --servers SERVERS    server list file path. default: ~/server_list.conf
  --cycles CYCLES      after period_cycle*interval seconds, will calculate
                       period stats cycles. default: 1
  --interval INTERVAL  monitoring statistics interval. default: 60(seconds)
  --timeout TIMEOUT    connection timeout. default: 2.0(seconds)
  --debug              print debug log
```
