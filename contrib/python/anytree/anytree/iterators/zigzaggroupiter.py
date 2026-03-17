from anytree.config import ASSERTIONS

from .abstractiter import AbstractIter
from .levelordergroupiter import LevelOrderGroupIter


class ZigZagGroupIter(AbstractIter):
    """
    Iterate over tree applying Zig-Zag strategy with grouping starting at `node`.

    Return a tuple of nodes for each level. The first tuple contains the
    nodes at level 0 (always `node`). The second tuple contains the nodes at level 1
    (children of `node`) in reversed order.
    The next level contains the children of the children in forward order, and so on.

    >>> from anytree import Node, RenderTree, AsciiStyle, ZigZagGroupIter
    >>> f = Node("f")
    >>> b = Node("b", parent=f)
    >>> a = Node("a", parent=b)
    >>> d = Node("d", parent=b)
    >>> c = Node("c", parent=d)
    >>> e = Node("e", parent=d)
    >>> g = Node("g", parent=f)
    >>> i = Node("i", parent=g)
    >>> h = Node("h", parent=i)
    >>> print(RenderTree(f, style=AsciiStyle()).by_attr())
    f
    |-- b
    |   |-- a
    |   +-- d
    |       |-- c
    |       +-- e
    +-- g
        +-- i
            +-- h
    >>> [[node.name for node in children] for children in ZigZagGroupIter(f)]
    [['f'], ['g', 'b'], ['a', 'd', 'i'], ['h', 'e', 'c']]
    >>> [[node.name for node in children] for children in ZigZagGroupIter(f, maxlevel=3)]
    [['f'], ['g', 'b'], ['a', 'd', 'i']]
    >>> [[node.name for node in children]
    ...  for children in ZigZagGroupIter(f, filter_=lambda n: n.name not in ('e', 'g'))]
    [['f'], ['b'], ['a', 'd', 'i'], ['h', 'c']]
    >>> [[node.name for node in children]
    ...  for children in ZigZagGroupIter(f, stop=lambda n: n.name == 'd')]
    [['f'], ['g', 'b'], ['a', 'i'], ['h']]
    """

    @staticmethod
    def _iter(children, filter_, stop, maxlevel):
        if children:
            if ASSERTIONS:  # pragma: no branch
                assert len(children) == 1
            _iter = LevelOrderGroupIter(children[0], filter_, stop, maxlevel)
            while True:
                try:
                    yield next(_iter)
                    yield tuple(reversed(next(_iter)))
                except StopIteration:
                    break
