import threading


class TestThread(threading.Thread):

    __test__ = False

    def run(self) -> None:
        self.exc = None
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc:
            raise self.exc
        return self.ret


class TestThreads:

    __test__ = False

    def __init__(self):
        self.threads: list[TestThread] = list()

    def append(self, thread: TestThread) -> int:
        self.threads.append(thread)
        return len(self.threads) - 1

    def start_thread(self, thread_index: int):
        self.threads[thread_index].start()

    def start_all(self) -> None:
        for thread in self.threads:
            thread.start()

    def join_thread(self, thread_index: int):
        self.threads[thread_index].join()

    def join_all(self, timeout=None) -> None:
        for thread in self.threads:
            thread.join(timeout=timeout)

    def start_and_wait_all(self):
        self.start_all()
        self.join_all()
