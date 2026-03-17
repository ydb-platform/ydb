import os
try:
    from pyperf._utils import USE_PSUTIL, BSD
    if not USE_PSUTIL:
        raise ImportError
    else:
        import psutil
except ImportError:
    raise ImportError('psutil is not installed')
import threading
import time


class PeakMemoryUsageThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.process = psutil.Process(os.getpid())
        self.peak_usage = 0
        self._done = threading.Event()
        self.sleep = 0.010   # 10 ms
        self._quit = False

    def get(self):
        if BSD:
            # USS (Unique Set Size) is not supported on BSD,
            # use RSS (Resident Set Size) instead.
            usage = self.process.memory_info().rss
        else:
            usage = self.process.memory_full_info().uss
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
    mem_thread.get()

    if not mem_thread.peak_usage:
        return "memory usage is zero"

    return None
