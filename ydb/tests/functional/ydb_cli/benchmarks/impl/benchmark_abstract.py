import pexpect

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import List, Any, Optional


class DatabaseAccessor(ABC):
    @abstractmethod
    def run_interactive(self, model: str, timeout: int) -> pexpect.spawn:
        raise NotImplementedError

    @abstractmethod
    def execute_query(self, query: str, parameters: Optional[dict] = None):
        raise NotImplementedError


class BenchmarkSample(ABC):
    @abstractmethod
    def run(self, database: DatabaseAccessor) -> Any:
        raise NotImplementedError


class BenchmarkAbstract(ABC):
    @abstractmethod
    def __len__(self):
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterator[BenchmarkSample]:
        raise NotImplementedError

    @abstractmethod
    def collect_statistics(self, samples: List[Any], statistics_path: str):
        raise NotImplementedError
