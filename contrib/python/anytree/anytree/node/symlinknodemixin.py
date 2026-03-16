from .nodemixin import NodeMixin


class SymlinkNodeMixin(NodeMixin):
    """
    The :any:`SymlinkNodeMixin` class extends any Python class to a symbolic link to a tree node.

    The class **MUST** have a `target` attribute referring to another tree node.
    The :any:`SymlinkNodeMixin` class has its own parent and its own child nodes.
    All other attribute accesses are just forwarded to the target node.
    A minimal implementation looks like (see :any:`SymlinkNode` for a full implementation):

    >>> from anytree import SymlinkNodeMixin, Node, RenderTree
    >>> class SymlinkNode(SymlinkNodeMixin):
    ...     def __init__(self, target, parent=None, children=None):
    ...         self.target = target
    ...         self.parent = parent
    ...         if children:
    ...             self.children = children
    ...     def __repr__(self):
    ...         return "SymlinkNode(%r)" % (self.target)

    >>> root = Node("root")
    >>> s1 = Node("sub1", parent=root)
    >>> l = SymlinkNode(s1, parent=root)
    >>> l0 = Node("l0", parent=l)
    >>> print(RenderTree(root))
    Node('/root')
    ├── Node('/root/sub1')
    └── SymlinkNode(Node('/root/sub1'))
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

    def __getattr__(self, name):
        if name in ("_NodeMixin__parent", "_NodeMixin__children"):
            return super().__getattr__(name)
        if name == "__setstate__":
            raise AttributeError(name)
        return getattr(self.target, name)

    def __setattr__(self, name, value):
        if name in ("_NodeMixin__parent", "_NodeMixin__children", "parent", "children", "target"):
            super().__setattr__(name, value)
        else:
            setattr(self.target, name, value)
