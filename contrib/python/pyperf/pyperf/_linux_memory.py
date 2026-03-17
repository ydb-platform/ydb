import threading
import time

from pyperf._utils import proc_path


# Code to parse Linux /proc/%d/smaps files.
#
# See
# https://web.archive.org/web/20180907232758/https://bmaurer.blogspot.com/2006/03/memory-usage-with-smaps.html
# for a quick introduction to smaps.
#
# Need Linux 2.6.16 or newer.
def read_smap_file():
    total = 0
    fp = open(proc_path("self/smaps"), "rb")
    with fp:
        for line in fp:
            # Include both Private_Clean and Private_Dirty sections.
            line = line.rstrip()
            if line.startswith(b"Private_") and line.endswith(b'kB'):
                parts = line.split()
                total += int(parts[1]) * 1024
    return total


class PeakMemoryUsageThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.peak_usage = 0
        self._done = threading.Event()
        self.sleep = 0.010   # 10 ms
        self._quit = False

    def get(self):
        usage = read_smap_file()
        self.peak_usage = max(self.peak_usage, usage)

    def run(self):
        try:
            while not self._quit:
                self.get()
                time.sleep(self.sleep)
        finally:
            self._done.set()

    def stop(self):
        self._quit = True
        self._done.wait()
        return self.peak_usage


def check_tracking_memory():
    mem_thread = PeakMemoryUsageThread()
    try:
        mem_thread.get()
    except OSError as exc:
        path = proc_path("self/smaps")
        return "unable to read %s: %s" % (path, exc)

    if not mem_thread.peak_usage:
        return "memory usage is zero"

    # it seems to work
    return None
