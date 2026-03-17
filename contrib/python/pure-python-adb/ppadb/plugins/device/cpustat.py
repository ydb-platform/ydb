import re
import time

from ppadb.plugins import Plugin
from ppadb.utils.logger import AdbLogging

logger = AdbLogging.get_logger(__name__)

class TotalCPUStat:
    def __init__(self, user, nice, system, idle, iowait, irq, softirq, stealstolen, guest, guest_nice):
        self.user = user
        self.nice = nice
        self.system = system
        self.idle = idle
        self.iowait = iowait
        self.irq = irq
        self.softirq = softirq
        self.stealstolen = stealstolen
        self.guest = guest
        self.guest_nice = guest_nice

    def total(self):
        return self.user + self.nice + self.system + self.idle + self.iowait + self.irq + self.softirq + self.stealstolen + self.guest + self.guest_nice

    def __add__(self, other):
        summary = TotalCPUStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        summary.user = self.user + other.user
        summary.nice = self.nice + other.nice
        summary.system = self.system + other.system
        summary.idle = self.idle + other.idle
        summary.iowait = self.iowait + other.iowait
        summary.irq = self.irq + other.irq
        summary.softirq = self.softirq + other.softirq
        summary.stealstolen = self.stealstolen + other.stealstolen
        summary.guest = self.guest + other.guest
        summary.guest_nice = self.guest_nice + other.guest_nice

        return summary

    def __sub__(self, other):
        result = TotalCPUStat(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        result.user = self.user - other.user
        result.nice = self.nice - other.nice
        result.system = self.system - other.system
        result.idle = self.idle - other.idle
        result.iowait = self.iowait - other.iowait
        result.irq = self.irq - other.irq
        result.softirq = self.softirq - other.softirq
        result.stealstolen = self.stealstolen - other.stealstolen
        result.guest = self.guest - other.guest
        result.guest_nice = self.guest_nice - other.guest_nice

        return result

    def __str__(self):
        attrs = vars(self)
        return ', '.join("%s: %s" % item for item in attrs.items())


class ProcessCPUStat:
    def __init__(self, name, utime, stime):
        self.name = name
        self.utime = utime
        self.stime = stime

    def __add__(self, other):
        summary = ProcessCPUStat(self.name, 0, 0)
        summary.utime = self.utime + other.utime
        summary.stime = self.stime + other.stime
        return summary

    def __sub__(self, other):
        result = ProcessCPUStat(self.name, 0, 0)
        result.utime = self.utime - other.utime
        result.stime = self.stime - other.stime
        return result

    def __str__(self):
        attrs = vars(self)
        return ', '.join("%s: %s" % item for item in attrs.items())

    def total(self):
        return self.utime + self.stime


class CPUStat(Plugin):
    total_cpu_pattern = re.compile(
        "cpu\s+([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s([\d]+)\s")

    def cpu_times(self):
        return self.get_total_cpu()

    def cpu_percent(self, interval=1):
        cpu_times_start = self.cpu_times()
        time.sleep(interval)
        cpu_times_end = self.cpu_times()

        diff = cpu_times_end - cpu_times_start
        return round(((diff.user + diff.system) / diff.total()) * 100, 2)

    def cpu_count(self):
        result = self.shell('ls /sys/devices/system/cpu')
        match = re.findall(r'cpu[0-9+]', result)
        return len(match)

    def get_total_cpu(self):
        result = self.shell('cat /proc/stat')
        match = self.total_cpu_pattern.search(result)
        if not match and len(match.groups()) != 10:
            logger.error("Can't get the total cpu usage from /proc/stat")
            return None

        return TotalCPUStat(*map(lambda x: int(x), match.groups()))

    def get_pid_cpu(self, pid):
        result = self.shell('cat /proc/{}/stat'.format(pid)).strip()

        if "No such file or directory" in result:
            return ProcessCPUStat("", 0, 0)
        else:
            items = result.split()
            return ProcessCPUStat(items[1], int(items[13]), int(items[14]))

    def get_all_thread_cpu(self, pid):
        result = self.shell("ls /proc/{}/task".format(pid))
        tids = list(map(lambda line: line.strip(), result.split("\n")))

        thread_result = {}
        for tid in tids:
            result = self.shell("cat /proc/{}/task/{}/stat".format(pid, tid))

            if "No such file or directory" not in result:
                items = result.split()
                thread_result[tid] = ProcessCPUStat(items[1], int(items[13]), int(items[14]))

        return thread_result
