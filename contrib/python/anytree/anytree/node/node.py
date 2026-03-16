from .nodemixin import NodeMixin
from .util import _repr


class Node(NodeMixin):
    """
    A simple tree node with a `name` and any `kwargs`.

    Args:
        name: A name or any other object this node can reference to as identifier.

    Keyword Args:
        parent: Reference to parent node.
        children: Iterable with child nodes.
        *: Any other given attribute is just stored as object attribute.

    Other than :any:`AnyNode` this class has at least the `name` attribute,
    to distinguish between different instances.

    The `parent` attribute refers the parent node:

    >>> from anytree import Node, RenderTree
    >>> root = Node("root")
    >>> s0 = Node("sub0", parent=root)
    >>> s0b = Node("sub0B", parent=s0, foo=4, bar=109)
    >>> s0a = Node("sub0A", parent=s0)
    >>> s1 = Node("sub1", parent=root)
    >>> s1a = Node("sub1A", parent=s1)
    >>> s1b = Node("sub1B", parent=s1, bar=8)
    >>> s1c = Node("sub1C", parent=s1)
    >>> s1ca = Node("sub1Ca", parent=s1c)

    >>> print(RenderTree(root))
    Node('/root')
    ├── Node('/root/sub0')
    │   ├── Node('/root/sub0/sub0B', bar=109, foo=4)
    │   └── Node('/root/sub0/sub0A')
    └── Node('/root/sub1')
        ├── Node('/root/sub1/sub1A')
        ├── Node('/root/sub1/sub1B', bar=8)
        └── Node('/root/sub1/sub1C')
            └── Node('/root/sub1/sub1C/sub1Ca')

    The same tree can be constructed by using the `children` attribute:

    >>> root = Node("root", children=[
    ...     Node("sub0", children=[
    ...         Node("sub0B", bar=109, foo=4),
    ...         Node("sub0A", children=None),
    ...     ]),
    ...     Node("sub1", children=[
    ...         Node("sub1A"),
    ...         Node("sub1B", bar=8, children=[]),
    ...         Node("sub1C", children=[
    ...             Node("sub1Ca"),
    ...         ]),
    ...     ]),
    ... ])

    >>> print(RenderTree(root))
    Node('/root')
    ├── Node('/root/sub0')
    │   ├── Node('/root/sub0/sub0B', bar=109, foo=4)
    │   └── Node('/root/sub0/sub0A')
    └── Node('/root/sub1')
        ├── Node('/root/sub1/sub1A')
        ├── Node('/root/sub1/sub1B', bar=8)
        └── Node('/root/sub1/sub1C')
            └── Node('/root/sub1/sub1C/sub1Ca')
    """

    def __init__(self, name, parent=None, children=None, **kwargs):
        self.__dict__.update(kwargs)
        self.name = name
        self.parent = parent
        if children:
            self.children = children

    def __repr__(self):
        args = ["{!r}".format(self.separator.join([""] + [str(node.name) for node in self.path]))]
        return _repr(self, args=args, nameblacklist=["name"])
