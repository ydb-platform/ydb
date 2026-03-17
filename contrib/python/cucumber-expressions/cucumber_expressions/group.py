from __future__ import annotations


class Group:
    def __init__(self, value: str, start: int, end: int, children: list[Group]):
        self._children = children
        self._value = value
        self._start = start
        self._end = end

    @property
    def value(self):
        return self._value

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    @property
    def children(self):
        return self._children

    @property
    def values(self):
        return (
            [self.value] if self.children is None else [v.value for v in self.children]
        )
