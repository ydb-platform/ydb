import re
from ppadb.plugins import Plugin


class Activity:
    def __init__(self, package, activity, pid):
        self.package = package
        self.activity = activity
        self.pid = pid

    def __str__(self):
        return "{}/{} - {}".format(self.package, self.activity, self.pid)


class MemInfo:
    def __init__(self, pss, private_dirty, private_clean, swapped_dirty, heap_size, heap_alloc, heap_free):
        self.pss = int(pss)
        self.private_dirty = int(private_dirty)
        self.private_clean = int(private_clean)
        self.swapped_dirty = int(swapped_dirty)
        self.heap_size = int(heap_size)
        self.heap_alloc = int(heap_alloc)
        self.heap_free = int(heap_free)


class Utils(Plugin):
    def get_top_activity(self):
        activities = self.get_top_activities()
        if activities:
            return activities[0]
        else:
            return None

    def get_top_activities(self):
        pattern = "ACTIVITY\s([\w\.]+)/([\w\.]+)\s[\w\d]+\spid=([\d]+)"
        cmd = "dumpsys activity top | grep ACTIVITY"
        result = self.shell(cmd)

        activities = []
        for line in result.split('\n'):
            match = re.search(pattern, line)
            if match:
                activities.append(Activity(match.group(1), match.group(2), int(match.group(3))))

        return activities

    def get_meminfo(self, package_name):
        total_meminfo_re = re.compile('\s*TOTAL\s*(?P<pss>\d+)'
                                      '\s*(?P<private_dirty>\d+)'
                                      '\s*(?P<private_clean>\d+)'
                                      '\s*(?P<swapped_dirty>\d+)'
                                      '\s*(?P<heap_size>\d+)'
                                      '\s*(?P<heap_alloc>\d+)'
                                      '\s*(?P<heap_free>\d+)')

        cmd = 'dumpsys meminfo {}'.format(package_name)
        result = self.shell(cmd)
        match = total_meminfo_re.search(result, 0)

        if match:
            return MemInfo(**match.groupdict())
        else:
            return MemInfo(0, 0, 0, 0, 0, 0, 0)

    def get_pid(self, package_name, toybox=False):
        # Because the version of `ps` is too much,
        # For example, the `ps` of toybox needs `-A` to list all process, but the `ps` of emulator doesn't.
        # So we use 'ps' and 'ps -A' to get all process information.

        cmds = ["ps | grep {}", "ps -A | grep {}"]
        for cmd in cmds:
            result = self.shell(cmd.format(package_name))
            if result:
                break

        if result:
            return result.split()[1]
        else:
            return None

    def get_uid(self, package_name):
        cmd = 'dumpsys package {} | grep userId'.format(package_name)
        result = self.shell(cmd).strip()

        pattern = "userId=([\d]+)"

        if result:
            match = re.search(pattern, result)
            uid = match.group(1)
            return uid
        else:
            return None

    def get_tids(self, pid):
        result = self.shell("ls /proc/{}/task".format(pid))
        return list(map(lambda line: line.strip(), result.split("\n")))

    def get_package_version_name(self, package_name):
        cmd = 'dumpsys package {} | grep versionName'.format(package_name)
        result = self.shell(cmd).strip()

        pattern = "versionName=([\d\.]+)"

        if result:
            match = re.search(pattern, result)
            version = match.group(1)
            return version
        else:
            return None
