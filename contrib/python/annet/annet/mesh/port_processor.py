from abc import abstractmethod
from typing import Protocol, Sequence


PortPair = tuple[str, str]


class PortProcessor(Protocol):
    @abstractmethod
    def __call__(self, pairs: Sequence[PortPair]) -> Sequence[list[PortPair]]:
        raise NotImplementedError


def united_ports(pairs: Sequence[PortPair]) -> Sequence[list[PortPair]]:
    return [list(pairs)]


def separate_ports(pairs: Sequence[PortPair]) -> Sequence[list[PortPair]]:
    return [[pair] for pair in pairs]
