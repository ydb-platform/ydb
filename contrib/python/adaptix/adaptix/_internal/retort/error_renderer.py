from abc import ABC, abstractmethod
from collections.abc import Sequence

from ..provider.essential import AggregateCannotProvide, CannotProvide
from ..tree_renderer import TreeRenderer, TreeRendererConfig
from ..utils import get_notes


class ErrorRenderer(ABC):
    @abstractmethod
    def render(self, error: CannotProvide) -> str:
        ...


class BuiltinErrorRenderer(ErrorRenderer):
    def __init__(self, tree_renderer_config: TreeRendererConfig):
        self._tree_renderer = TreeRenderer(
            config=tree_renderer_config,
            children_getter=self._get_error_children,
            node_renderer=self._render_error_node,
        )

    def _get_error_children(self, error: CannotProvide) -> Sequence[CannotProvide]:
        if isinstance(error, AggregateCannotProvide):
            return error.exceptions
        return ()

    def _render_error_node(self, error: CannotProvide) -> str:
        notes = get_notes(error)
        if notes:
            return error.message + "\n" + "\n".join(notes)
        return error.message

    def render(self, error: CannotProvide) -> str:
        return self._tree_renderer.generate_text(error)
