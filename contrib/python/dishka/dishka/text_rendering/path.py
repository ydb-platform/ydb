from collections.abc import Sequence
from typing import NamedTuple

from dishka.entities.component import Component
from dishka.entities.factory_type import FactoryData
from dishka.entities.key import DependencyKey
from dishka.entities.scope import BaseScope
from dishka.text_rendering.name import get_key_name, get_name, get_source_name


class PathRenderer:
    def __init__(self, *, cycle: bool):
        self.cycle = cycle

    def _arrow_cycle(self, index: int, length: int) -> str:
        if length == 1:
            return "⥁ "
        elif index == 0:
            return "╭─>─╮ "
        elif index + 1 < length:
            return "│   ▼ "
        else:
            return "╰─<─╯ "

    def _arrow_line(self, index: int, length: int) -> str:
        if index + 1 < length:
            return "▼   "
        else:
            return "╰─> "

    def _arrow_failed_variant(self) -> str:
        return "  ╰─× "  # noqa: RUF001

    def _arrow(self, index: int, length: int) -> str:
        if self.cycle:
            return self._arrow_cycle(index, length)
        return self._arrow_line(index, length)

    def _switch_arrow(self, index: int, length: int) -> str:
        if self.cycle:
            if index > 0:
                return "│   │ "
            return "      "
        return "│   "

    def _switch_filler(self) -> str:
        return " "

    def _switch(
            self, scope: BaseScope | None, component: Component | None,
    ) -> str:
        return f"◈ {scope}, component={component!r} ◈"

    def render(
            self,
            path: Sequence[FactoryData],
            last: DependencyKey | None = None,
            variants: Sequence[FactoryData] = (),
    ) -> str:
        row_count = len(path) + bool(last)
        rows = [
            Row(
                row_num,
                self._arrow(row_num, row_count),
                [get_key_name(factory.provides), get_source_name(factory)],
                (factory.scope, factory.provides.component),
            )
            for row_num, factory in enumerate(path)
        ]
        if last:
            rows.append(
                Row(
                    row_count - 1,
                    self._arrow(row_count-1, row_count),
                    [get_key_name(last), "???"],
                    (rows[-1].dest[0], last.component),
                ),
            )
        prev_dest: tuple[BaseScope | None, Component | None] = (None, "")
        space_left = "   "
        space_between = "   "
        res = ""

        if not rows:
            columns_count = 0
        else:
            columns_count = len(rows[0].columns)

        columns_width = [
            max(len(row.columns[col_num]) for row in rows)
            for col_num in range(columns_count)
        ]
        switch_len = (
            sum(columns_width) +
            len(space_between) * (columns_count - 1)
        )
        for row in rows:
            if row.dest != prev_dest:
                res += (
                    space_left +
                    self._switch_arrow(row.num, row_count) +
                    self._switch(*row.dest).center(
                        switch_len, self._switch_filler(),
                    ) +
                    "\n"
                )
                prev_dest = row.dest
            res += (
                space_left +
                row.border_left +
                space_between.join(
                    c.ljust(cw)
                    for c, cw in zip(row.columns, columns_width, strict=False)
                ) +
                "\n"
            )
        border_failed = self._arrow_failed_variant()
        for variant in variants:
            res += (
                space_left +
                border_failed +
                get_name(variant.source, include_module=False) +
                ": " +
                repr(variant.when_override) +
                "\n"
            )
        return res


class Row(NamedTuple):
    num: int
    border_left: str
    columns: Sequence[str]
    dest: tuple[BaseScope | None, Component | None]
