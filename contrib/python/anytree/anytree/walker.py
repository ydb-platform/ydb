from .config import ASSERTIONS


class Walker:
    """Walk from one node to another."""

    @staticmethod
    def walk(start, end):
        """
        Walk from `start` node to `end` node.

        Returns:
            (upwards, common, downwards): `upwards` is a list of nodes to go upward to.
            `common` top node. `downwards` is a list of nodes to go downward to.

        Raises:
            WalkError: on no common root node.

        Example:

        >>> from anytree import Node, RenderTree, AsciiStyle
        >>> f = Node("f")
        >>> b = Node("b", parent=f)
        >>> a = Node("a", parent=b)
        >>> d = Node("d", parent=b)
        >>> c = Node("c", parent=d)
        >>> e = Node("e", parent=d)
        >>> g = Node("g", parent=f)
        >>> i = Node("i", parent=g)
        >>> h = Node("h", parent=i)
        >>> print(RenderTree(f, style=AsciiStyle()))
        Node('/f')
        |-- Node('/f/b')
        |   |-- Node('/f/b/a')
        |   +-- Node('/f/b/d')
        |       |-- Node('/f/b/d/c')
        |       +-- Node('/f/b/d/e')
        +-- Node('/f/g')
            +-- Node('/f/g/i')
                +-- Node('/f/g/i/h')

        Create a walker:

        >>> w = Walker()

        This class is made for walking:

        >>> w.walk(f, f)
        ((), Node('/f'), ())
        >>> w.walk(f, b)
        ((), Node('/f'), (Node('/f/b'),))
        >>> w.walk(b, f)
        ((Node('/f/b'),), Node('/f'), ())
        >>> w.walk(h, e)
        ((Node('/f/g/i/h'), Node('/f/g/i'), Node('/f/g')), Node('/f'), (Node('/f/b'), Node('/f/b/d'), Node('/f/b/d/e')))
        >>> w.walk(d, e)
        ((), Node('/f/b/d'), (Node('/f/b/d/e'),))

        For a proper walking the nodes need to be part of the same tree:

        >>> w.walk(Node("a"), Node("b"))
        Traceback (most recent call last):
          ...
        anytree.walker.WalkError: Node('/a') and Node('/b') are not part of the same tree.
        """
        startpath = start.path
        endpath = end.path
        if start.root is not end.root:
            msg = f"{start!r} and {end!r} are not part of the same tree."
            raise WalkError(msg)
        # common
        common = Walker.__calc_common(startpath, endpath)
        if ASSERTIONS:  # pragma: no branch
            assert common[0] is start.root
        len_common = len(common)
        # upwards
        if start is common[-1]:
            upwards = ()
        else:
            upwards = tuple(reversed(startpath[len_common:]))
        # down
        if end is common[-1]:
            down = ()
        else:
            down = endpath[len_common:]
        return upwards, common[-1], down

    @staticmethod
    def __calc_common(start, end):
        return tuple(si for si, ei in zip(start, end) if si is ei)


class WalkError(RuntimeError):
    """Walk Error."""
