from __future__ import annotations

from typing import TYPE_CHECKING

from zarr.core.group import Group, GroupMetadata, _parse_async_node
from zarr.core.group import create_hierarchy as create_hierarchy_async
from zarr.core.group import create_nodes as create_nodes_async
from zarr.core.group import create_rooted_hierarchy as create_rooted_hierarchy_async
from zarr.core.group import get_node as get_node_async
from zarr.core.sync import _collect_aiterator, sync

if TYPE_CHECKING:
    from collections.abc import Iterator

    from zarr.abc.store import Store
    from zarr.core.common import ZarrFormat
    from zarr.core.metadata import ArrayV2Metadata, ArrayV3Metadata
    from zarr.types import AnyArray


def create_nodes(
    *, store: Store, nodes: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata]
) -> Iterator[tuple[str, Group | AnyArray]]:
    """Create a collection of arrays and / or groups concurrently.

    Note: no attempt is made to validate that these arrays and / or groups collectively form a
    valid Zarr hierarchy. It is the responsibility of the caller of this function to ensure that
    the ``nodes`` parameter satisfies any correctness constraints.

    Parameters
    ----------
    store : Store
        The storage backend to use.
    nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
        A dictionary defining the hierarchy. The keys are the paths of the nodes
        in the hierarchy, and the values are the metadata of the nodes. The
        metadata must be either an instance of GroupMetadata, ArrayV3Metadata
        or ArrayV2Metadata.

    Yields
    ------
    Group | Array
        The created nodes.
    """
    coro = create_nodes_async(store=store, nodes=nodes)

    for key, value in sync(_collect_aiterator(coro)):
        yield key, _parse_async_node(value)


def create_hierarchy(
    *,
    store: Store,
    nodes: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
    overwrite: bool = False,
) -> Iterator[tuple[str, Group | AnyArray]]:
    """
    Create a complete zarr hierarchy from a collection of metadata objects.

    This function will parse its input to ensure that the hierarchy is complete. Any implicit groups
    will be inserted as needed. For example, an input like
    ```{'a/b': GroupMetadata}``` will be parsed to
    ```{'': GroupMetadata, 'a': GroupMetadata, 'b': Groupmetadata}```

    After input parsing, this function then creates all the nodes in the hierarchy concurrently.

    Arrays and Groups are yielded in the order they are created. This order is not stable and
    should not be relied on.

    Parameters
    ----------
    store : Store
        The storage backend to use.
    nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
        A dictionary defining the hierarchy. The keys are the paths of the nodes in the hierarchy,
        relative to the root of the ``Store``. The root of the store can be specified with the empty
        string ``''``. The values are instances of ``GroupMetadata`` or ``ArrayMetadata``. Note that
        all values must have the same ``zarr_format`` -- it is an error to mix zarr versions in the
        same hierarchy.

        Leading "/" characters from keys will be removed.
    overwrite : bool
        Whether to overwrite existing nodes. Defaults to ``False``, in which case an error is
        raised instead of overwriting an existing array or group.

        This function will not erase an existing group unless that group is explicitly named in
        ``nodes``. If ``nodes`` defines implicit groups, e.g. ``{`'a/b/c': GroupMetadata}``, and a
        group already exists at path ``a``, then this function will leave the group at ``a`` as-is.

    Yields
    ------
    tuple[str, Group | Array]
        This function yields (path, node) pairs, in the order the nodes were created.

    Examples
    --------
    ```python
    from zarr import create_hierarchy
    from zarr.storage import MemoryStore
    from zarr.core.group import GroupMetadata

    store = MemoryStore()
    nodes = {'a': GroupMetadata(attributes={'name': 'leaf'})}
    nodes_created = dict(create_hierarchy(store=store, nodes=nodes))
    print(nodes)
    # {'a': GroupMetadata(attributes={'name': 'leaf'}, zarr_format=3, consolidated_metadata=None, node_type='group')}
    ```
    """
    coro = create_hierarchy_async(store=store, nodes=nodes, overwrite=overwrite)

    for key, value in sync(_collect_aiterator(coro)):
        yield key, _parse_async_node(value)


def create_rooted_hierarchy(
    *,
    store: Store,
    nodes: dict[str, GroupMetadata | ArrayV2Metadata | ArrayV3Metadata],
    overwrite: bool = False,
) -> Group | AnyArray:
    """
    Create a Zarr hierarchy with a root, and return the root node, which could be a ``Group``
    or ``Array`` instance.

    Parameters
    ----------
    store : Store
        The storage backend to use.
    nodes : dict[str, GroupMetadata | ArrayV3Metadata | ArrayV2Metadata]
        A dictionary defining the hierarchy. The keys are the paths of the nodes
        in the hierarchy, and the values are the metadata of the nodes. The
        metadata must be either an instance of GroupMetadata, ArrayV3Metadata
        or ArrayV2Metadata.
    overwrite : bool
        Whether to overwrite existing nodes. Default is ``False``.

    Returns
    -------
    Group | Array
    """
    async_node = sync(create_rooted_hierarchy_async(store=store, nodes=nodes, overwrite=overwrite))
    return _parse_async_node(async_node)


def get_node(store: Store, path: str, zarr_format: ZarrFormat) -> AnyArray | Group:
    """
    Get an Array or Group from a path in a Store.

    Parameters
    ----------
    store : Store
        The store-like object to read from.
    path : str
        The path to the node to read.
    zarr_format : {2, 3}
        The zarr format of the node to read.

    Returns
    -------
    Array | Group
    """

    return _parse_async_node(sync(get_node_async(store=store, path=path, zarr_format=zarr_format)))
