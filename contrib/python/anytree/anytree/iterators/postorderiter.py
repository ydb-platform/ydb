from .abstractiter import AbstractIter


class PostOrderIter(AbstractIter):
    """
    Iterate over tree applying post-order strategy starting at `node`.

    >>> from anytree import Node, RenderTree, AsciiStyle, PostOrderIter
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
    >>> [node.name for node in PostOrderIter(f)]
    ['a', 'c', 'e', 'd', 'b', 'h', 'i', 'g', 'f']
    >>> [node.name for node in PostOrderIter(f, maxlevel=3)]
    ['a', 'd', 'b', 'i', 'g', 'f']
    >>> [node.name for node in PostOrderIter(f, filter_=lambda n: n.name not in ('e', 'g'))]
    ['a', 'c', 'd', 'b', 'h', 'i', 'f']
    >>> [node.name for node in PostOrderIter(f, stop=lambda n: n.name == 'd')]
    ['a', 'b', 'h', 'i', 'g', 'f']
    """

    @staticmethod
    def _iter(children, filter_, stop, maxlevel):
        return PostOrderIter.__next(children, 1, filter_, stop, maxlevel)

    @staticmethod
    def __next(children, level, filter_, stop, maxlevel):
        if not AbstractIter._abort_at_level(level, maxlevel):
            for child in children:
                grandchildren = AbstractIter._get_children(child.children, stop)
                yield from PostOrderIter.__next(grandchildren, level + 1, filter_, stop, maxlevel)
                if filter_(child):
                    yield child
