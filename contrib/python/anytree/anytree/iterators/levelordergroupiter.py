from .abstractiter import AbstractIter


class LevelOrderGroupIter(AbstractIter):
    """
    Iterate over tree applying level-order strategy with grouping starting at `node`.

    Return a tuple of nodes for each level. The first tuple contains the
    nodes at level 0 (always `node`). The second tuple contains the nodes at level 1
    (children of `node`). The next level contains the children of the children, and so on.

    >>> from anytree import Node, RenderTree, AsciiStyle, LevelOrderGroupIter
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
    >>> [[node.name for node in children] for children in LevelOrderGroupIter(f)]
    [['f'], ['b', 'g'], ['a', 'd', 'i'], ['c', 'e', 'h']]
    >>> [[node.name for node in children] for children in LevelOrderGroupIter(f, maxlevel=3)]
    [['f'], ['b', 'g'], ['a', 'd', 'i']]
    >>> [[node.name for node in children]
    ...  for children in LevelOrderGroupIter(f, filter_=lambda n: n.name not in ('e', 'g'))]
    [['f'], ['b'], ['a', 'd', 'i'], ['c', 'h']]
    >>> [[node.name for node in children]
    ...  for children in LevelOrderGroupIter(f, stop=lambda n: n.name == 'd')]
    [['f'], ['b', 'g'], ['a', 'i'], ['h']]
    """

    @staticmethod
    def _iter(children, filter_, stop, maxlevel):
        level = 1
        while children:
            yield tuple(child for child in children if filter_(child))
            level += 1
            if AbstractIter._abort_at_level(level, maxlevel):
                break
            children = LevelOrderGroupIter._get_grandchildren(children, stop)

    @staticmethod
    def _get_grandchildren(children, stop):
        next_children = []
        for child in children:
            next_children = next_children + AbstractIter._get_children(child.children, stop)
        return next_children
