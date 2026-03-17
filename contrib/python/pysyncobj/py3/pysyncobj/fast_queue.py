try:
    import Queue
except ImportError:
    import queue as Queue
from collections import deque
import threading

# According to benchmarks, standard Queue is slow.
# Using FastQueue improves overall performance by ~15%
class FastQueue(object):
    def __init__(self, maxSize):
        self.__queue = deque()
        self.__lock = threading.Lock()
        self.__maxSize = maxSize

    def put_nowait(self, value):
        with self.__lock:
            if len(self.__queue) > self.__maxSize:
                raise Queue.Full()
            self.__queue.append(value)

    def get_nowait(self):
        with self.__lock:
            if len(self.__queue) == 0:
                raise Queue.Empty()
            return self.__queue.popleft()
