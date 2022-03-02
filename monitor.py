#!/usr/bin/env python3
"""
memcached metrics for monitoring. 
"""
from copy import deepcopy
import json
import logging
import os
import re
from statistics import mean
import subprocess
import telnetlib
import time
import typing

DELTA_TIME = 3    # 启动延后03秒，保证crontab脚本在00秒启动时，当前这轮能运行
MAX_SLABS = 47
logger = logging.getLogger()
logger.setLevel(logging.INFO)
hdl = logging.StreamHandler()
log_fmt = '[%(levelname)s] %(asctime)s PID:%(process)d %(name)s %(filename)s:%(lineno)d %(message)s'
hdl.setFormatter(fmt=logging.Formatter(fmt=log_fmt))
logger.addHandler(hdl)
log_type = ['origin', 'period', 'everyday']


class StatsCache:
    current = None
    last = None


period_stats_cache = StatsCache()
everyday_stats_cache = StatsCache()


class MemcachedStats:
    bin_name = 'memcached'

    _client = None
    _key_regex = re.compile('ITEM (.*) \[(.*); (.*)\]')
    _slab_id_regex = re.compile('STAT items:(.*):number')
    _slabs_regex = re.compile('STAT (\d*):(.*) (\d*)')
    _slabs_total_regex = re.compile('STAT (\w*) (\d*)')
    _stat_regex = re.compile("STAT (.*) ([0-9]+\.?[0-9]*)\r")

    def __init__(self, host='localhost', port=11211, timeout=2.0):
        self._host = host
        self._port = int(port)
        self._timeout = timeout

    @staticmethod
    def get_instances() -> set:
        result = set()
        ls_procs = ''' ps -ef |grep {bin_name}|grep -v grep '''.format(
            bin_name=MemcachedStats.bin_name,
        )
        lines = subprocess.getoutput(ls_procs).split("\n")
        for line in lines:
            line = line.strip()
            m = re.search(pattern="\-p\s+(?P<port>\d+)", string=line, flags=re.IGNORECASE)
            if m:
                port = m.group("port")
                result.add(int(port))
        return result

    @property
    def client(self) -> telnetlib.Telnet:
        if self._client is None:
            msg = "create connection %s:%s" % (self._host, self._port)
            # logging.debug(msg)
            self._client = telnetlib.Telnet(
                host=self._host,
                port=self._port,
                timeout=self._timeout,
            )
        return self._client

    def command(self, cmd: str) -> str:
        """Write a command to telnet and return the response. """
        msg = "issue command: %s" % cmd
        # logging.debug(msg)
        self._write(cmd)
        return self._read_until("END")

    def close(self) -> str:
        """close telnet connection. """
        return self._write("quit")

    def _write(self, cmd: str) -> typing.Optional[str]:
        if not cmd.endswith("\n"):
            cmd = cmd + "\n"
        resp = self.client.write(cmd.encode("utf8"))
        if resp:
            return resp.decode('utf8')

    def _read_until(self, token: str) -> str:
        return self.client.read_until(token.encode("utf8")).decode("utf8")

    def key_details(self, sort=True, limit=100) -> list:
        """Return a list of tuples containing keys and details. """
        cmd = 'stats cachedump %s %s'
        keys = [key for id in self.slab_ids()
                for key in self._key_regex.findall(self.command(cmd % (id, limit)))]
        if sort:
            return sorted(keys)
        else:
            return keys

    def keys(self, sort=True, limit=100) -> list:
        """Return a list of keys in use. """
        return [key[0] for key in self.key_details(sort=sort, limit=limit)]

    def slab_ids(self) -> typing.List[typing.Any]:
        """Return a list of slab ids in use. """
        return self._slab_id_regex.findall(self.command('stats items'))

    def slabs(self) -> dict:
        """Return a dict containing memcached slabs stats. """
        slabs_cmd = self.command('stats slabs')
        slab_stats = self._slabs_regex.findall(slabs_cmd)
        slab_total_stats = self._slabs_total_regex.findall(slabs_cmd)
        stats = {}
        for key, value in slab_total_stats:
            stats[key] = value
        for slab_id, key, value in slab_stats:
            if str(slab_id) not in stats:
                stats[str(slab_id)] = {}
            stats[str(slab_id)][key] = value
        return stats

    def stats(self) -> dict:
        """Return a dict containing memcached stats. """
        return dict(self._stat_regex.findall(self.command('stats')))


# FIXME: better to use a new thread? 
def delete_old_files(log_dir: str, old_time):
    """Delete old log files to release disk space. """
    logging.debug(f"Delete old files. log_dir={log_dir}, old_time={old_time}")
    dirs = os.listdir(log_dir)
    for dir in dirs:
        try:
            if time.mktime(time.strptime(dir, "%Y-%m-%d_%H:%M")) < old_time:
                files = os.listdir(os.path.join(log_dir, dir))
                for f in files:
                    try:
                        os.unlink(os.path.join(log_dir, dir, f))
                    except:
                        pass
                os.rmdir(os.path.join(log_dir, dir))
        except:
            pass


def str_of_size(value):
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = 1024.0
    for i in range(len(units)):
        if (value / size) < 1:
            return "%.2f %s" % (value, units[i])
        value = value / size
    return "%.2f %s" % (value, "PB")


def division_cache_error(fmt: str, value1: float, value2: float):
    try:
        ret = fmt % (value1 / value2)
    except ZeroDivisionError:
        ret = '0.0'
    finally:
        return ret


def ratio_compute(value1: float, value2: float):
    return float(division_cache_error('%.4g', 100 * float(value1), float(value2)))


# TODO[DONE]: 异常处理
def check_uptime(stats: dict, now: int, cluster_server_restart_path) -> dict:
    """
    检查服务器宕机情况
    """
    with open(cluster_server_restart_path, 'r') as f:
        restarts = json.load(f)

    for server in stats:
        if server == 'Cluster': continue
        uptime = now - stats[server]['Uptime']
        if server in restarts:
            last_uptime = int(restarts[server][-1].split()[1])
            # NOTE: 为啥是十分钟（600s）我也不知道，照搬原来的shell监控工具的
            if uptime - last_uptime > 600:
                uptime_str = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime(uptime))
                restarts[server].append(f"{uptime_str}  {uptime}")
        else:
            uptime_str = time.strftime("%Y-%m-%d_%H:%M:%S", time.localtime(uptime))
            restarts[server] = [f"{uptime_str}  {uptime}"]

    with open(cluster_server_restart_path, 'w') as f:
        json.dump(restarts, f, indent=2)


def wrap_stats(stats) -> dict:
    """
    从Telnet得到的原始数据中筛选整理有用的stats数据
    """
    ret = {}
    try:
        ret['Uptime'] = int(stats['uptime'])
        ret['Get_hits'] = int(stats['get_hits'])
        ret['Get_misses'] = int(stats['get_misses'])
        ret['Cmd_get'] = int(stats['cmd_get'])
        ret['Cmd_set'] = int(stats['cmd_set'])
        ret['Usage(%)'] = ratio_compute(stats['bytes'], stats['limit_maxbytes'])
        ret['Hit_ratio(%)'] = ratio_compute(stats['get_hits'], stats['cmd_get'])
        ret['Bytes'] = int(stats['bytes'])
        ret['Bytes(Human_readable)'] = str_of_size(int(stats['bytes']))
        ret['Evictions'] = int(stats['evictions'])
        ret['Eviction_item_size'] = division_cache_error('%.1f', float(stats['bytes']), float(stats['evictions']))
        ret['Eviction_item_size'] += '  ' + str_of_size(float(ret['Eviction_item_size']))
    except KeyError:
        msg = "access key not exists"
        logging.exception(msg)
    finally:
        return ret


def wrap_slabs(stats, Server_Total_pages, Server_Get_hits, Server_Cmd_set) -> dict:
    """
    从Telnet得到的原始数据中筛选整理有用的slabs数据
    """
    ret = {}
    try:
        ret['chunk_size'] = int(stats['chunk_size'])
        ret['chunk_size(Human_readable)'] = str_of_size(int(stats['chunk_size']))
        ret['Total_pages'] = int(stats['total_pages'])
        ret['Total_pages_load_ratio(%)'] = ratio_compute(stats['total_pages'], Server_Total_pages)
        ret['Total_chunks'] = int(stats['total_chunks'])
        ret['Used_chunks'] = int(stats['used_chunks'])
        ret['Free_chunks'] = int(stats['free_chunks'])
        ret['Used_chunks_ratio(%)'] = ratio_compute(stats['used_chunks'], stats['total_chunks'])
        ret['Get_hits'] = int(stats['get_hits'])
        ret['Cmd_set'] = int(stats['cmd_set'])
        ret['Get_hits_load_ratio(%)'] = ratio_compute(stats['get_hits'], Server_Get_hits)
        ret['Cmd_set_load_ratio(%)'] = ratio_compute(stats['cmd_set'], Server_Cmd_set)
    except KeyError:
        msg = "access key not exists"
        logging.exception(msg)
    finally:
        return ret


def calc_cluster_stats(stats) -> dict:
    """
    计算整个集群的stats数据
    """
    ret = {}
    ret['Cluster_Get_hits'] = 0
    ret['Cluster_Cmd_get'] = 0
    ret['Cluster_Cmd_set'] = 0
    ret['Cluster_Hit_ratio(%)'] = 0.0
    ret['Cluster_Bytes'] = 0
    ret['Cluster_Bytes(Human_readable)'] = '0 B'
    ret['Cluster_Evictions'] = 0
    try:
        for server in stats:
            if server == 'Cluster': continue
            ret['Cluster_Get_hits'] += stats[server]['Get_hits']
            ret['Cluster_Cmd_get'] += stats[server]['Cmd_get']
            ret['Cluster_Cmd_set'] += stats[server]['Cmd_set']
            ret['Cluster_Bytes'] += stats[server]['Bytes']
            ret['Cluster_Evictions'] += stats[server]['Evictions']

        ret['Cluster_Hit_ratio(%)'] = ratio_compute(ret['Cluster_Get_hits'], ret['Cluster_Cmd_get'])
        ret['Cluster_Bytes(Human_readable)'] = str_of_size(int(ret['Cluster_Bytes']))
    except KeyError:
        msg = "access key not exists, endpoint=%s" % server
        logging.exception(msg)
    finally:
        return ret


def calc_cluster_slabs(stats, Cluster_Total_pages, Cluster_Get_hits, Cluster_Cmd_set) -> dict:
    """
    计算整个集群的slabs数据
    """
    ret = {}
    for slab_id in range(MAX_SLABS):
        ret[str(slab_id)] = {}
        ret[str(slab_id)]['chunk_size'] = 0
        ret[str(slab_id)]['chunk_size(Human_readable)'] = '0 B'
        ret[str(slab_id)]['Total_pages'] = 0
        ret[str(slab_id)]['Total_pages_load_ratio(%)'] = 0.0
        ret[str(slab_id)]['Total_chunks'] = 0
        ret[str(slab_id)]['Used_chunks'] = 0
        ret[str(slab_id)]['Free_chunks'] = 0
        ret[str(slab_id)]['Used_chunks_ratio(%)'] = 0.0
        ret[str(slab_id)]['Get_hits'] = 0
        ret[str(slab_id)]['Cmd_set'] = 0
        ret[str(slab_id)]['Get_hits_load_ratio(%)'] = 0.0
        ret[str(slab_id)]['Cmd_set_load_ratio(%)'] = 0.0
    ret['Summary'] = {}
    ret['Summary']["Used_bytes"] = 0
    # ret['Summary']["Used_bytes(Human_readable)"] = '0 B'
    ret['Summary']["Total_bytes"] = 0
    # ret['Summary']["Total_bytes(Human_readable)"] = '0 B'
    ret['Summary']["Used_bytes_ratio(%)"] = 0.0

    try:
        for slab_id in range(MAX_SLABS):
            for server in stats:
                if server == 'Cluster': continue
                if str(slab_id) in stats[server]:
                    ret[str(slab_id)]['chunk_size'] = stats[server][str(slab_id)]['chunk_size']
                    ret[str(slab_id)]['chunk_size(Human_readable)'] = stats[server][str(slab_id)]['chunk_size(Human_readable)']
                    ret[str(slab_id)]['Total_pages'] += stats[server][str(slab_id)]['Total_pages']
                    ret[str(slab_id)]['Total_chunks'] += stats[server][str(slab_id)]['Total_chunks']
                    ret[str(slab_id)]['Used_chunks'] += stats[server][str(slab_id)]['Used_chunks']
                    ret[str(slab_id)]['Free_chunks'] += stats[server][str(slab_id)]['Free_chunks']
                    ret[str(slab_id)]['Get_hits'] += stats[server][str(slab_id)]['Get_hits']
                    ret[str(slab_id)]['Cmd_set'] += stats[server][str(slab_id)]['Cmd_set']
        # 删除本次统计里面不存在的slab
        for slab_id in range(MAX_SLABS):
            if ret[str(slab_id)]['chunk_size'] == 0:
                del ret[str(slab_id)]

        for slab_id in range(MAX_SLABS):
            if str(slab_id) in ret:
                ret[str(slab_id)]['Total_pages_load_ratio(%)'] = ratio_compute(ret[str(slab_id)]['Total_pages'], Cluster_Total_pages)
                ret[str(slab_id)]['Used_chunks_ratio(%)'] = ratio_compute(ret[str(slab_id)]['Used_chunks'], ret[str(slab_id)]['Total_chunks'])
                ret[str(slab_id)]['Get_hits_load_ratio(%)'] = ratio_compute(ret[str(slab_id)]['Get_hits'], Cluster_Get_hits)
                ret[str(slab_id)]['Cmd_set_load_ratio(%)'] = ratio_compute(ret[str(slab_id)]['Cmd_set'], Cluster_Cmd_set)
                ret['Summary']["Used_bytes"] += ret[str(slab_id)]['chunk_size'] * ret[str(slab_id)]['Used_chunks']
                ret['Summary']["Total_bytes"] += ret[str(slab_id)]['chunk_size'] * ret[str(slab_id)]['Total_chunks']
                # logging.debug(f"{slab_id}  {ret[str(slab_id)]['chunk_size']}  {ret[str(slab_id)]['Used_chunks']}  {ret['Used_bytes']}")
        ret['Summary']["Used_bytes_ratio(%)"] = ratio_compute(ret['Summary']["Used_bytes"], ret['Summary']["Total_bytes"])
        ret['Summary']["Used_bytes"] = str(ret['Summary']["Used_bytes"]) + '  ' + str_of_size(ret['Summary']["Used_bytes"])
        ret['Summary']["Total_bytes"] = str(ret['Summary']["Total_bytes"]) + '  ' + str_of_size(ret['Summary']["Total_bytes"])

    except KeyError:
        msg = f"access key not exists. ret['Total_pages'] += stats[{server}][{str(slab_id)}]['Total_pages']. {stats[server][str(slab_id)]}"
        logging.exception(msg)
    finally:
        return ret


def calc_period_cluster_stats(curr_period: dict, last_period: dict, period_time: int):
    """
    计算一段时间的统计数据。\n
    在period统计和everyday统计中都会用到这个函数。\n
    在period统计中，会指定now_timestamp的值，进行每时段qps的汇总。
    """
    period_cluster_stats = deepcopy(curr_period)
    ret_cluster = period_cluster_stats['Cluster']
    ret_cluster['Cluster_Get_hits'] = 0
    ret_cluster['Cluster_Cmd_get'] = 0
    ret_cluster['Cluster_Cmd_set'] = 0
    ret_cluster['Cluster_Evictions'] = 0
    # NOTE: period_cluster_stats有几项是直接用curr_period里面的值，比如Uptime/Bytes，就没有在这里面写出来

    # 计算每个server的统计数据
    for server in curr_period:
        if server == 'Cluster': continue
        ret = period_cluster_stats[server]
        ret['Get_hits'] -= last_period[server]['Get_hits']
        ret['Get_misses'] -= last_period[server]['Get_misses']
        ret['Cmd_get'] -= last_period[server]['Cmd_get']
        ret['Cmd_set'] -= last_period[server]['Cmd_set']
        ret['Hit_ratio(%)'] = ratio_compute(ret['Get_hits'], ret['Cmd_get'])
        ret['Bytes_load_ratio(%)'] = ratio_compute(ret['Bytes'], ret_cluster['Cluster_Bytes'])
        # FIXME: Miss_Bytes计算有bug，逻辑不通，用没命中率乘以总bytes没有任何科学道理
        ret['Miss_Bytes'] = int(ret['Bytes'] * (1 - ret['Hit_ratio(%)'] / 100))
        ret['Miss_Bytes'] = str(ret['Miss_Bytes']) + '  ' + str_of_size(ret['Miss_Bytes'])
        # if ret['Evictions'] > last_period[server]['Evictions']:     # else 保持原值不变
        ret['Evictions'] -= last_period[server]['Evictions']
        # 检查是否有server重启，如果有重启的话，curr_period值会非常小，减完是负数，那么置零
        if ret['Get_hits'] < 0: ret['Get_hits'] = 0
        if ret['Get_misses'] < 0: ret['Get_misses'] = 0
        if ret['Cmd_get'] < 0: ret['Cmd_get'] = 0
        if ret['Cmd_set'] < 0: ret['Cmd_set'] = 0
        if ret['Evictions'] < 0: ret['Evictions'] = 0
        # FIXME: Eviction_item_size计算有bug
        ret['Eviction_item_size'] = division_cache_error("%.2f", ret['Bytes'], ret['Evictions'])
        ret['Eviction_item_size'] += '  ' + str_of_size(float(ret['Eviction_item_size']))
        # 合并 Bytes 和 Bytes(Human_readable) 两项
        ret['Bytes'] = str(ret['Bytes']) + '  ' + ret['Bytes(Human_readable)']
        del ret['Bytes(Human_readable)']
        # 计算整个集群的统计数据
        ret_cluster['Cluster_Get_hits'] += ret['Get_hits']
        ret_cluster['Cluster_Cmd_get'] += ret['Cmd_get']
        ret_cluster['Cluster_Cmd_set'] += ret['Cmd_set']
        ret_cluster['Cluster_Evictions'] += ret['Evictions']

    # 计算整个集群的统计数据
    ret_cluster['Cluster_Hit_ratio(%)'] = ratio_compute(ret_cluster['Cluster_Get_hits'], ret_cluster['Cluster_Cmd_get'])
    # FIXME: 同上，同Miss_Bytes
    ret_cluster['Cluster_Miss_Bytes'] = int(ret_cluster['Cluster_Bytes'] * (1 - ret_cluster['Cluster_Hit_ratio(%)'] / 100))
    ret_cluster['Cluster_Miss_Bytes'] = str(ret_cluster['Cluster_Miss_Bytes']) + '  ' + str_of_size(ret_cluster['Cluster_Miss_Bytes'])
    # 合并 Cluster_Bytes 和 Cluster_Bytes(Human_readable) 两项
    ret_cluster['Cluster_Bytes'] = str(ret_cluster['Cluster_Bytes']) + '  ' + ret_cluster['Cluster_Bytes(Human_readable)']
    del ret_cluster['Cluster_Bytes(Human_readable)']
    ret_cluster['Cluster_get_qps'] = division_cache_error("%.2f", ret_cluster['Cluster_Cmd_get'], period_time)
    ret_cluster['Cluster_set_qps'] = division_cache_error("%.2f", ret_cluster['Cluster_Cmd_set'], period_time)

    return period_cluster_stats


def calc_period_final(curr_period: dict, period_cluster_stats: dict, last_time, curr_time):
    period_final = {}
    period_final['Cluster Stats'] = deepcopy(period_cluster_stats['Cluster'])
    period_final['Cluster Stats'].update(curr_period['Cluster']['Summary'])
    period_final['Cluster Stats']['Memory_utilization(%)'] = ratio_compute(
        float(period_cluster_stats['Cluster']['Cluster_Bytes'].split()[0]),
        float(curr_period['Cluster']['Summary']['Total_bytes'].split()[0])
    )
    period_final['Cluster Stats']['Server_restart_num'] = 0
    # 改变 Cluster_Evictions 的输出顺序，把它放到后面
    Cluster_Evictions = period_final['Cluster Stats']['Cluster_Evictions']
    del period_final['Cluster Stats']['Cluster_Evictions']
    period_final['Cluster Stats']['Cluster_Evictions'] = Cluster_Evictions

    period_final['Cluster Stats']['ClusterEviction'] = []
    for server in period_cluster_stats:
        if server == 'Cluster': continue
        if period_cluster_stats[server]['Evictions'] > 0:
            period_final['Cluster Stats']['ClusterEviction'].append(
                f"{server:<20}  Evictions:{period_cluster_stats[server]['Evictions']:<4}  "
                f"Eviction_item_size:{period_cluster_stats[server]['Eviction_item_size'].split('  ')[1]}"
            )

    period_final['Cluster Slabs'] = deepcopy(curr_period['Cluster'])
    del period_final['Cluster Slabs']['Summary']

    period_final['Server Restart Log'] = []
    with open(os.path.join(log_dir, 'cluster_server_restart.log'), 'r') as f:
        restarts = json.load(f)
    for server in restarts:
        for restart_time in restarts[server]:
            if last_time < int(restart_time.split()[1]) < curr_time:
                period_final['Server Restart Log'].append(f"{server}  {restart_time.split()[0]}")
                period_final['Cluster Stats']['Server_restart_num'] += 1
    return period_final


def calc_avg_qps(qps: list):
    """
    计算每天的平均qps。

    qps_without_zero指的是除去异常小值之后的平均值和最小值，
    比如，某个时段网络故障导致没有获取到数据，那么此时qps是0，就不应该统计到最终结果中；
    再比如，某个时刻几乎没有写操作，qps是0.05，那把它统计进去意义也不大。
    这个异常值取多少合适，可以之后再评估，现在取的是1（if x > 1 那里）
    """
    get_qps = [float(x.split("  ")[1]) for x in qps]
    set_qps = [float(x.split("  ")[2]) for x in qps]
    get_qps_without_zero = [x for x in get_qps if x > 0.1]
    set_qps_without_zero = [x for x in set_qps if x > 0.1]
    if (len(get_qps_without_zero) == 0):
        get_qps_without_zero = [0]
    if (len(set_qps_without_zero) == 0):
        set_qps_without_zero = [0]
    get_min = min(get_qps_without_zero)
    set_min = min(set_qps_without_zero)
    get_max = max(get_qps_without_zero)
    set_max = max(set_qps_without_zero)
    get_qps = mean(get_qps)
    set_qps = mean(set_qps)
    get_qps_without_zero = mean(get_qps_without_zero)
    set_qps_without_zero = mean(set_qps_without_zero)
    return [f"read_qps : {get_qps:<7.6g}  {get_qps_without_zero:<8.6g}  {get_min:<7.6g}  {get_max:<7.6g}\n",
            f"write_qps: {set_qps:<7.6g}  {set_qps_without_zero:<8.6g}  {set_min:<7.6g}  {set_max:<7.6g}\n"]


def calc_max_usage(curr_period: dict, time: str):
    sort_list = []
    Total_chunks = 0
    ret = ""

    try:
        for slab_id in range(MAX_SLABS):
            if str(slab_id) in curr_period:
                sort_list.append((
                    curr_period[str(slab_id)]['Used_chunks'],
                    curr_period[str(slab_id)]['chunk_size(Human_readable)']
                ))
                Total_chunks += curr_period[str(slab_id)]['Used_chunks']
        sort_list.sort(reverse = True)
        for i in range(len(sort_list)):
            if i > 10: break
            usage_ratio = ratio_compute(sort_list[i][0], Total_chunks)
            if i == 0:
                ret += f"{time}  {sort_list[i][1]:<10}  {sort_list[i][0]:<10}  {usage_ratio}% \n"
            else:
                ret += f"{' ' * len(time)}  {sort_list[i][1]:<10}  {sort_list[i][0]:<10}  {usage_ratio}% \n"
    except KeyError:
        msg = f"access key not exists."
        logging.exception(msg)
    finally:
        return ret + '\n'


def period_stats(curr_period: dict, last_period: dict):
    """
    计算一段时间的统计结果并保存。
    curr_period里面的ip >= last_period里面的ip
    """
    now = time.strftime("%Y-%m-%d_%H:%M", time.localtime(curr_period['time']))
    last = time.strftime("%Y-%m-%d_%H:%M", time.localtime(last_period['time']))

    # 如果curr_period里面有新增的ip，那么last_period里面使用curr_period的数据代替
    for server in curr_period['stats']:
        if server not in last_period['stats']:
            last_period['stats'][server] = deepcopy(curr_period['stats'][server])

    if not os.path.exists(os.path.join(log_dir, "period", now)):
        os.makedirs(os.path.join(log_dir, "period", now))
    # calc & save period_cluster_stats
    period_cluster_stats = calc_period_cluster_stats(curr_period['stats'], last_period['stats'], curr_period['time'] - last_period['time'])
    with open(os.path.join(log_dir, "period", now, f"period_cluster_stats_{last}_{now}.log"), 'w') as f:
        json.dump(period_cluster_stats, f, indent=2)

    # 计算集群每时段qps
    today = time.strftime("%Y-%m-%d", time.localtime(last_period['time']))
    get_qps = period_cluster_stats['Cluster']['Cluster_get_qps']
    set_qps = period_cluster_stats['Cluster']['Cluster_set_qps']
    if not os.path.exists(os.path.join(log_dir, "everyday", today)):
        os.makedirs(os.path.join(log_dir, "everyday", today))
    if not os.path.exists(os.path.join(log_dir, "everyday", today, "cluster_qps.log")):
        with open(os.path.join(log_dir, "everyday", today, "cluster_qps.log"), 'w') as f:
            f.write("begin_time         end_time          get_qps  set_qps \n")
    with open(os.path.join(log_dir, "everyday", today, "cluster_qps.log"), 'a') as f:
        f.write(f"{last} - {now}  {get_qps}  {set_qps} \n")

    # calc & save period_final
    try:
        period_final = calc_period_final(curr_period['slabs'], period_cluster_stats, last_period['time'], curr_period['time'])
        with open(os.path.join(log_dir, "period", now, f"period_final_{last}_{now}.log"), 'w') as f:
            json.dump(period_final, f, indent=2)
    except:
        logging.exception("@@@@@@@@@@@@@@  calc_period_final  ERROR!!!!!!!!!!!!!")


def everyday_stats(curr_period: dict, last_period: dict):
    """
    计算每天的统计结果并保存。
    curr_period里面的ip >= last_period里面的ip
    --TODO--[暂不考虑]: 待重构，与 period_stats 合并
    """
    last = time.strftime("%Y-%m-%d", time.localtime(last_period['time']))

    # 如果curr_period里面有新增的ip，那么last_period里面使用curr_period的数据代替
    for server in curr_period['stats']:
        if server not in last_period['stats']:
            last_period['stats'][server] = deepcopy(curr_period['stats'][server])

    if not os.path.exists(os.path.join(log_dir, "everyday", last)):
        os.makedirs(os.path.join(log_dir, "everyday", last))
    # calc & save period_cluster_stats
    period_cluster_stats = calc_period_cluster_stats(curr_period['stats'], last_period['stats'], curr_period['time'] - last_period['time'])
    with open(os.path.join(log_dir, "everyday", last, "period_cluster_stats.log"), 'w') as f:
        json.dump(period_cluster_stats, f, indent=2)
    # calc & save period_final
    period_final = calc_period_final(curr_period['slabs'], period_cluster_stats, last_period['time'], curr_period['time'])
    period_final['Qps'] = "请参见同目录下的 cluster_qps.log 文件"
    with open(os.path.join(log_dir, "everyday", last, "period_final.log"), 'w') as f:
        json.dump(period_final, f, indent=2)
    # calc & save max_usage
    max_usage = calc_max_usage(curr_period['slabs']['Cluster'], last)
    with open(os.path.join(log_dir, "max_usage.log"), 'a') as f:
        f.writelines(max_usage)


def run_monitor(log_dir: str, server_list: str, period_cycle: int, interval: int, timeout: float) -> typing.Optional[list]:
    stats = {}
    slabs = {}

    while True:
        trash = False
        logging.debug("waiting for a new minute to get new data. ")
        time.sleep((interval + 0.1 - time.time() % interval + DELTA_TIME) % interval)
        now = int(time.time())
        origin_file_now = time.strftime("%Y-%m-%d_%H:%M", time.localtime(now))
        Cluster_Total_pages = 0
        Cluster_Get_hits = 0
        Cluster_Cmd_set = 0

        servers = []
        with open(os.path.expanduser(server_list), 'r') as f:
            for server in f.read().splitlines():
                if ':' in server:
                    servers.append(server.split(':'))
                else:
                    servers.append([server, 11211])

        for host, port in servers:
            port = int(port)
            endpoint = f"{host}:{port}"
            # TODO[DONE]: 这一点好像在period_stats和everyday_stats的server检查那里都实现了。可以每次清空了。
            # 每次不清空数据是好还是不好？好：如果有哪个server短暂失联，就不用做异常处理了，直接使用上一时刻的数据。
            # 不好：时间长了，脏数据可能越攒越多，误差变大。所以：每隔一段时间清空一下？比如，一周，一个月。
            # stats = {}
            # slabs = {}
            stats[endpoint] = {}
            slabs[endpoint] = {}

            try:
                conn = MemcachedStats(host=host, port=port, timeout=timeout)
                server_stats = conn.stats()
                server_slabs = conn.slabs()
                conn.close()
                # 统计整理stats数据
                server_stats = wrap_stats(server_stats)

                # 计算每个server里面所有slabs的数据和
                Server_Total_pages = 0
                Server_Get_hits = 0
                Server_Cmd_set = 0
                for slab_id in range(MAX_SLABS):
                    if str(slab_id) in server_slabs:
                        Server_Total_pages += float(server_slabs[str(slab_id)]['total_pages'])
                        Server_Get_hits += float(server_slabs[str(slab_id)]['get_hits'])
                        Server_Cmd_set += float(server_slabs[str(slab_id)]['cmd_set'])
                Cluster_Total_pages += Server_Total_pages
                Cluster_Get_hits += Server_Get_hits
                Cluster_Cmd_set += Server_Cmd_set

                # 统计整理slabs数据
                for slab_id in range(MAX_SLABS):
                    if str(slab_id) in server_slabs:
                        server_slabs[str(slab_id)] = wrap_slabs(server_slabs[str(slab_id)], Server_Total_pages, Server_Get_hits, Server_Cmd_set)

                stats[endpoint] = server_stats
                slabs[endpoint] = server_slabs
            except Exception as e:
                msg = "query memcached instance stats failed, endpoint=%s" % endpoint
                logging.exception(msg)
                # 连接失败的主要原因：server挂了；server压力太大阻塞了
                with open(os.path.join(log_dir, "connect_failed.log"), 'a') as f:
                    f.writelines(f"{origin_file_now}   {endpoint}   {e} \n")
                # 如果socket阻塞太长时间（大于10秒），那么这轮数据的qps误差就太大了，干脆这轮数据就丢弃了
                if int(time.time()) - now > 10:
                    trash = True
                    break
                del stats[endpoint]
                del slabs[endpoint]
                continue

        if trash:
            logging.warning("This round data is trashed. Time: %s" % origin_file_now)
            continue

        # 计算整个集群的stats数据和slabs数据
        stats['Cluster'] = calc_cluster_stats(stats)
        slabs['Cluster'] = calc_cluster_slabs(slabs, Cluster_Total_pages, Cluster_Get_hits, Cluster_Cmd_set)

        # 检查服务器宕机情况
        cluster_server_restart_path = os.path.join(log_dir, 'cluster_server_restart.log')
        check_uptime(stats, now, cluster_server_restart_path)

        # 保存整理后的原始数据
        if not os.path.exists(os.path.join(log_dir, "origin", origin_file_now)):
            os.makedirs(os.path.join(log_dir, "origin", origin_file_now))
        with open(os.path.join(log_dir, "origin", origin_file_now, 'cluster_stats.log'), 'w') as f:
            json.dump(stats, f, indent=2)
        with open(os.path.join(log_dir, "origin", origin_file_now, 'cluster_slabs.log'), 'w') as f:
            json.dump(slabs, f, indent=2)
        end = int(time.time())
        logging.debug(f'wrap use time: {end-now}')

        # 计算每段时间的统计结果
        if now % (period_cycle * interval) - DELTA_TIME == 0:
            period_stats_cache.current = deepcopy({'stats': stats, 'slabs': slabs, 'time': now})
            if period_stats_cache.last is None:
                try:
                    origin_file_last = time.strftime("%Y-%m-%d_%H:%M", time.localtime(now - period_cycle * interval))
                    if os.path.exists(os.path.join(log_dir, "origin", origin_file_last)):
                        with open(os.path.join(log_dir, "origin", origin_file_last, 'cluster_stats.log'), 'r') as f:
                            last_stats = json.load(f)
                        with open(os.path.join(log_dir, "origin", origin_file_last, 'cluster_slabs.log'), 'r') as f:
                            last_slabs = json.load(f)
                        period_stats_cache.last = {'stats': last_stats, 'slabs': last_slabs, 'time': now - period_cycle * interval}
                        logging.debug(f"Load period_stats_cache.last[{origin_file_last}] from file. ")
                except:
                    pass
            if period_stats_cache.last is None:
                logging.warning(f"Load period_stats_cache.last[{origin_file_last}] failed, skip this round of calculations. ")
            else:
                period_stats(period_stats_cache.current, period_stats_cache.last)
            period_stats_cache.last = period_stats_cache.current


        # 计算每天的统计结果
        # 28800: GMT+8时差, 86400: 一天的秒数
        # TODO: 待重构，与上一段合并，写成函数
        if (now + 28800) % 86400 - DELTA_TIME == 0:
            everyday_stats_cache.current = deepcopy({'stats': stats, 'slabs': slabs, 'time': now})
            if everyday_stats_cache.last is None:
                try:
                    origin_file_last = time.strftime("%Y-%m-%d_%H:%M", time.localtime(now - 86400))
                    if os.path.exists(os.path.join(log_dir, "origin", origin_file_last)):
                        with open(os.path.join(log_dir, "origin", origin_file_last, 'cluster_stats.log'), 'r') as f:
                            last_stats = json.load(f)
                        with open(os.path.join(log_dir, "origin", origin_file_last, 'cluster_slabs.log'), 'r') as f:
                            last_slabs = json.load(f)
                        everyday_stats_cache.last = {'stats': last_stats, 'slabs': last_slabs, 'time': now - 86400}
                except:
                    pass
            if everyday_stats_cache.last is None:
                logging.warning(f"Load everyday_stats_cache.last[{origin_file_last}] failed, skip this round of calculations. ")
            else:
                everyday_stats(everyday_stats_cache.current, everyday_stats_cache.last)
            # 计算avg_qps
            last = time.strftime("%Y-%m-%d", time.localtime(now - DELTA_TIME - 10))     # now-DELTA_TIME是当天的00:00:00，再减10秒就是前一天啦
            if os.path.exists(os.path.join(log_dir, "everyday", last, "cluster_qps.log")):
                with open(os.path.join(log_dir, "everyday", last, "cluster_qps.log")) as f:
                    qps = f.readlines()[1:]
                avg_qps = calc_avg_qps(qps)
                avg_qps[0] = last + ' ' + avg_qps[0]
                avg_qps[1] = last + ' ' + avg_qps[1]
                with open(os.path.join(log_dir, "avg_qps.log"), 'a') as f:
                    f.writelines(avg_qps)
            everyday_stats_cache.last = everyday_stats_cache.current

        end = int(time.time())
        logging.debug(f'+stats use time: {end-now}')

        # 删除旧的数据
        try:
            if (now + 28800) % 86400 - DELTA_TIME == 0:
                delete_old_files(os.path.join(log_dir, "origin"), now - 86400)
                delete_old_files(os.path.join(log_dir, "period"), now - 86400)
                end = int(time.time())
                logging.debug(f'+delete use time: {end-now}')
        except:
            pass


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log_dir",
        help="log file storage dir. default: ~/memcached_log/",
        default="~/memcached_log/",
    )
    parser.add_argument(
        "--servers",
        help="server list file path. default: ~/server_list.conf",
        default='~/server_list.conf',
    )
    parser.add_argument(
        "--cycles",
        help="after period_cycle*interval seconds, will calculate period stats cycles. default: 1",
        default=1,
        type=int,
    )
    parser.add_argument(
        "--interval",
        help="monitoring statistics interval. default: 60(seconds)",
        default=60,
        type=int,
    )
    parser.add_argument(
        "--timeout",
        help="connection timeout. default: 2.0(seconds)",
        default=2.0,
        type=float,
    )
    parser.add_argument(
        "--debug",
        help="print debug log",
        action='store_true',
    )

    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    log_dir = os.path.expanduser(args.log_dir)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    for path in log_type:
        if not os.path.exists(os.path.join(log_dir, path)):
            os.makedirs(os.path.join(log_dir, path))
    # 创建 cluster_server_restart.log  avg_qps.log  max_usage.log
    if not os.path.exists(os.path.join(log_dir, 'cluster_server_restart.log')):
        with open(os.path.join(log_dir, 'cluster_server_restart.log'), 'w') as f:
            json.dump({}, f)
    if not os.path.exists(os.path.join(log_dir, 'avg_qps.log')):
        with open(os.path.join(log_dir, "avg_qps.log"), 'w') as f:
            f.write("date                  avg   avg(除去异常值)  min    max\n")
    if not os.path.exists(os.path.join(log_dir, 'max_usage.log')):
        with open(os.path.join(log_dir, "max_usage.log"), 'w') as f:
            f.write("\n")

    run_monitor(log_dir=log_dir, server_list=args.servers, period_cycle=args.cycles, interval=args.interval, timeout=args.timeout)

