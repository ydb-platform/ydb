from __future__ import annotations

from cucumber_expressions.group import Group


class GroupBuilder:
    def __init__(self):
        self._group_builders: list[GroupBuilder] = []
        self._capturing = True
        self._source: str = ""
        self._end_index = None
        self._children: list[GroupBuilder] = []

    def add(self, group_builder: GroupBuilder):
        self._group_builders.append(group_builder)

    def build(self, match, group_indices) -> Group:
        group_index = next(group_indices)
        children: list[Group] = [
            gb.build(match, group_indices) for gb in self._group_builders
        ]
        return Group(
            value=match.group(group_index),
            start=match.regs[group_index][0],
            end=match.regs[group_index][1],
            children=children if len(children) else None,
        )

    def move_children_to(self, group_builder: GroupBuilder) -> None:
        for child in self._group_builders:
            group_builder.add(child)

    @property
    def capturing(self):
        return self._capturing

    @capturing.setter
    def capturing(self, value: bool):
        self._capturing = value

    @property
    def children(self) -> list[GroupBuilder]:
        return self._group_builders

    @property
    def source(self) -> str:
        return self._source

    @source.setter
    def source(self, source: str):
        self._source = source
