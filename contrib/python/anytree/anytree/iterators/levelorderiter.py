from .abstractiter import AbstractIter


class LevelOrderIter(AbstractIter):
    """
    Iterate over tree applying level-order strategy starting at `node`.

    >>> from anytree import Node, RenderTree, AsciiStyle, LevelOrderIter
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
    >>> [node.name for node in LevelOrderIter(f)]
    ['f', 'b', 'g', 'a', 'd', 'i', 'c', 'e', 'h']
    >>> [node.name for node in LevelOrderIter(f, maxlevel=3)]
    ['f', 'b', 'g', 'a', 'd', 'i']
    >>> [node.name for node in LevelOrderIter(f, filter_=lambda n: n.name not in ('e', 'g'))]
    ['f', 'b', 'a', 'd', 'i', 'c', 'h']
    >>> [node.name for node in LevelOrderIter(f, stop=lambda n: n.name == 'd')]
    ['f', 'b', 'g', 'a', 'i', 'h']
    """

    @staticmethod
    def _iter(children, filter_, stop, maxlevel):
        level = 1
        while children:
            next_children = []
            level += 1
            if AbstractIter._abort_at_level(level, maxlevel):
                for child in children:
                    if filter_(child):
                        yield child
            else:
                for child in children:
                    if filter_(child):
                        yield child
                    next_children += AbstractIter._get_children(child.children, stop)
            children = next_children
