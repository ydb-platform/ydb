import threading


class AtomicCounter:
    """
    A helper class that implements a thread-safe counter.
    """
    def __init__(self, initial=0):

        self.value = initial
        self._lock = threading.Lock()
        self._initial = initial

    def increment(self, num=1):

        with self._lock:
            self.value += num
            return self.value

    def decrement(self, num=1):

        with self._lock:
            self.value -= num
            return self.value

    def get_current(self):

        with self._lock:
            return self.value

    def reset(self):

        with self._lock:
            self.value = self._initial
            return self.value
