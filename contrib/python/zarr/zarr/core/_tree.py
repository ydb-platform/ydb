import io
import os
from collections.abc import Sequence
from typing import Any

from zarr.core.group import AsyncGroup

try:
    import rich
    import rich.console
    import rich.tree
except ImportError as e:
    raise ImportError("'rich' is required for Group.tree") from e


class TreeRepr:
    """
    A simple object with a tree-like repr for the Zarr Group.

    Note that this object and it's implementation isn't considered part
    of Zarr's public API.
    """

    def __init__(self, tree: rich.tree.Tree) -> None:
        self._tree = tree

    def __repr__(self) -> str:
        color_system = os.environ.get("OVERRIDE_COLOR_SYSTEM", rich.get_console().color_system)
        console = rich.console.Console(file=io.StringIO(), color_system=color_system)
        console.print(self._tree)
        return str(console.file.getvalue())

    def _repr_mimebundle_(
        self,
        include: Sequence[str],
        exclude: Sequence[str],
        **kwargs: Any,
    ) -> dict[str, str]:
        # For jupyter support.
        # Unsure why mypy infers the return type to by Any
        return self._tree._repr_mimebundle_(include=include, exclude=exclude, **kwargs)  # type: ignore[no-any-return]


async def group_tree_async(group: AsyncGroup, max_depth: int | None = None) -> TreeRepr:
    tree = rich.tree.Tree(label=f"[bold]{group.name}[/bold]")
    nodes = {"": tree}
    members = sorted([x async for x in group.members(max_depth=max_depth)])

    for key, node in members:
        if key.count("/") == 0:
            parent_key = ""
        else:
            parent_key = key.rsplit("/", 1)[0]
        parent = nodes[parent_key]

        # We want what the spec calls the node "name", the part excluding all leading
        # /'s and path segments. But node.name includes all that, so we build it here.
        name = key.rsplit("/")[-1]
        if isinstance(node, AsyncGroup):
            label = f"[bold]{name}[/bold]"
        else:
            label = f"[bold]{name}[/bold] {node.shape} {node.dtype}"
        nodes[key] = parent.add(label)

    return TreeRepr(tree)
