import time
import threading


class Reservoir:
    """
    Keeps track of the number of sampled segments within
    a single second. This class is implemented to be
    thread-safe to achieve accurate sampling.
    """
    def __init__(self, traces_per_sec=0):
        """
        :param int traces_per_sec: number of guranteed
            sampled segments.
        """
        self._lock = threading.Lock()
        self.traces_per_sec = traces_per_sec
        self.used_this_sec = 0
        self.this_sec = int(time.time())

    def take(self):
        """
        Returns True if there are segments left within the
        current second, otherwise return False.
        """
        with self._lock:
            now = int(time.time())

            if now != self.this_sec:
                self.used_this_sec = 0
                self.this_sec = now

            if self.used_this_sec >= self.traces_per_sec:
                return False

            self.used_this_sec = self.used_this_sec + 1
            return True
