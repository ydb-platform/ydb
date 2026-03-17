from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Callable, Generic, TypeVar


@dataclass
class TreeRendererConfig:
    init: str = "  "

    node_start_root: str = "× "
    node_start_leaf: str = "▷ "
    node_space: str = "  "
    node_line: str = "│ "

    child_start: str = "├──"
    child_continue: str = "│  "
    child_start_last: str = "╰──"
    child_continue_last: str = "   "


T = TypeVar("T")
N = TypeVar("N")


class TreeRenderer(Generic[N]):
    def __init__(
        self,
        config: TreeRendererConfig,
        node_renderer: Callable[[N], str],
        children_getter: Callable[[N], Sequence[N]],
    ):
        self._config = config
        self._node_renderer = node_renderer
        self._children_getter = children_getter

    def generate_text(self, node: N):
        lines = self._generate_lines(node, self._config.node_start_root)
        return "\n".join(
            self._config.init + line
            for line in lines
        )

    def _generate_lines(self, node: N, node_start: str) -> Iterable[str]:
        node_lines = self._node_renderer(node).split("\n")
        children = self._children_getter(node)

        yield from self._with_hetero_prefix(
            node_lines,
            node_start,
            self._config.node_line if children else self._config.node_space,
        )
        for child, has_next in self._with_has_next(children):
            child_lines = self._generate_lines(child, self._config.node_start_leaf)

            if has_next:
                yield from self._with_hetero_prefix(
                    child_lines,
                    self._config.child_start,
                    self._config.child_continue,
                )
            else:
                yield from self._with_hetero_prefix(
                    child_lines,
                    self._config.child_start_last,
                    self._config.child_continue_last,
                )

    def _with_hetero_prefix(self, lines: Iterable[str], first_prefix: str, other_prefix: str) -> Iterable[str]:
        lines_iter = iter(lines)
        yield first_prefix + next(lines_iter)

        for line in lines_iter:
            yield other_prefix + line

    def _with_has_next(self, seq: Sequence[T]) -> Iterable[tuple[T, bool]]:
        seq_len = len(seq)
        for i, item in enumerate(seq, start=1):
            yield item, i != seq_len
