import abc


class Worker:
    @property
    @abc.abstractmethod
    def id(self) -> str:
        ...


class NoWorker(Worker):
    @property
    def id(self):
        return None
