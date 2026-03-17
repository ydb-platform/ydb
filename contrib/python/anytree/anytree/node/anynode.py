from .nodemixin import NodeMixin
from .util import _repr


class AnyNode(NodeMixin):
    """
    A generic tree node with any `kwargs`.

    Keyword Args:
        parent: Reference to parent node.
        children: Iterable with child nodes.
        *: Any other given attribute is just stored as object attribute.

    Other than :any:`Node` this class has no default identifier.
    It is up to the user to use other attributes for identification.

    The `parent` attribute refers the parent node:

    >>> from anytree import AnyNode, RenderTree
    >>> root = AnyNode(id="root")
    >>> s0 = AnyNode(id="sub0", parent=root)
    >>> s0b = AnyNode(id="sub0B", parent=s0, foo=4, bar=109)
    >>> s0a = AnyNode(id="sub0A", parent=s0)
    >>> s1 = AnyNode(id="sub1", parent=root)
    >>> s1a = AnyNode(id="sub1A", parent=s1)
    >>> s1b = AnyNode(id="sub1B", parent=s1, bar=8)
    >>> s1c = AnyNode(id="sub1C", parent=s1)
    >>> s1ca = AnyNode(id="sub1Ca", parent=s1c)

    >>> root
    AnyNode(id='root')
    >>> s0
    AnyNode(id='sub0')
    >>> print(RenderTree(root))
    AnyNode(id='root')
    ├── AnyNode(id='sub0')
    │   ├── AnyNode(bar=109, foo=4, id='sub0B')
    │   └── AnyNode(id='sub0A')
    └── AnyNode(id='sub1')
        ├── AnyNode(id='sub1A')
        ├── AnyNode(bar=8, id='sub1B')
        └── AnyNode(id='sub1C')
            └── AnyNode(id='sub1Ca')

    >>> print(RenderTree(root))
    AnyNode(id='root')
    ├── AnyNode(id='sub0')
    │   ├── AnyNode(bar=109, foo=4, id='sub0B')
    │   └── AnyNode(id='sub0A')
    └── AnyNode(id='sub1')
        ├── AnyNode(id='sub1A')
        ├── AnyNode(bar=8, id='sub1B')
        └── AnyNode(id='sub1C')
            └── AnyNode(id='sub1Ca')

    Node attributes can be added, modified and deleted the pythonic way:

    >>> root.new = 'a new attribute'
    >>> s0b
    AnyNode(bar=109, foo=4, id='sub0B')
    >>> s0b.bar = 110  # modified
    >>> s0b
    AnyNode(bar=110, foo=4, id='sub0B')
    >>> del s1b.bar
    >>> print(RenderTree(root))
    AnyNode(id='root', new='a new attribute')
    ├── AnyNode(id='sub0')
    │   ├── AnyNode(bar=110, foo=4, id='sub0B')
    │   └── AnyNode(id='sub0A')
    └── AnyNode(id='sub1')
        ├── AnyNode(id='sub1A')
        ├── AnyNode(id='sub1B')
        └── AnyNode(id='sub1C')
            └── AnyNode(id='sub1Ca')

    The same tree can be constructed by using the `children` attribute:

    >>> root = AnyNode(id="root", children=[
    ...     AnyNode(id="sub0", children=[
    ...         AnyNode(id="sub0B", foo=4, bar=109),
    ...         AnyNode(id="sub0A"),
    ...     ]),
    ...     AnyNode(id="sub1", children=[
    ...         AnyNode(id="sub1A"),
    ...         AnyNode(id="sub1B", bar=8),
    ...         AnyNode(id="sub1C", children=[
    ...             AnyNode(id="sub1Ca"),
    ...         ]),
    ...     ]),
    ... ])
    """

    def __init__(self, parent=None, children=None, **kwargs):
        self.__dict__.update(kwargs)
        self.parent = parent
        if children:
            self.children = children

    def __repr__(self):
        return _repr(self)
