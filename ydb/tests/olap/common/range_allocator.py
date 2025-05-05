import threading


class RangeAllocator:

    class Range:
        def __init__(self, left, right):  # [left, right)
            self.left = left
            self.right = right

    def __init__(self, value=0):
        self._value = int(value)
        self._lock = threading.Lock()

    def allocate_range(self, length):
        with self._lock:
            left = self._value
            self._value += int(length)
            return RangeAllocator.Range(left, self._value)

    @property
    def get_border(self):
        with self._lock:
            return self._value
