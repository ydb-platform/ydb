from .symlinknodemixin import SymlinkNodeMixin
from .util import _repr


class SymlinkNode(SymlinkNodeMixin):
    """
    Tree node which references to another tree node.

    Args:
        target: Symbolic Link Target. Another tree node, which is referred to.

    Keyword Args:
        parent: Reference to parent node.
        children: Iterable with child nodes.
        *: Any other given attribute is just stored as attribute **in** `target`.

    The :any:`SymlinkNode` has its own parent and its own child nodes.
    All other attribute accesses are just forwarded to the target node.

    >>> from anytree import SymlinkNode, Node, RenderTree
    >>> root = Node("root")
    >>> s1 = Node("sub1", parent=root, bar=17)
    >>> l = SymlinkNode(s1, parent=root, baz=18)
    >>> l0 = Node("l0", parent=l)
    >>> print(RenderTree(root))
    Node('/root')
    ├── Node('/root/sub1', bar=17, baz=18)
    └── SymlinkNode(Node('/root/sub1', bar=17, baz=18))
        └── Node('/root/sub1/l0')

    Any modifications on the target node are also available on the linked node and vice-versa:

    >>> s1.foo = 4
    >>> s1.foo
    4
    >>> l.foo
    4
    >>> l.foo = 9
    >>> s1.foo
    9
    >>> l.foo
    9
    """

    def __init__(self, target, parent=None, children=None, **kwargs):
        self.target = target
        self.target.__dict__.update(kwargs)
        self.parent = parent
        if children:
            self.children = children

    def __repr__(self):
        return _repr(self, [repr(self.target)], nameblacklist=("target",))
