"""
Node Searching.

.. note:: You can speed-up node searching, by installing https://pypi.org/project/fastcache/ and
          using :any:`cachedsearch`.
"""

from anytree.iterators import PreOrderIter


def findall(node, filter_=None, stop=None, maxlevel=None, mincount=None, maxcount=None):
    """
    Search nodes matching `filter_` but stop at `maxlevel` or `stop`.

    Return tuple with matching nodes.

    Args:
        node: top node, start searching.

    Keyword Args:
        filter_: function called with every `node` as argument, `node` is returned if `True`.
        stop: stop iteration at `node` if `stop` function returns `True` for `node`.
        maxlevel (int): maximum descending in the node hierarchy.
        mincount (int): minimum number of nodes.
        maxcount (int): maximum number of nodes.

    Example tree:

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

    >>> findall(f, filter_=lambda node: node.name in ("a", "b"))
    (Node('/f/b'), Node('/f/b/a'))
    >>> findall(f, filter_=lambda node: d in node.path)
    (Node('/f/b/d'), Node('/f/b/d/c'), Node('/f/b/d/e'))

    The number of matches can be limited:

    >>> findall(f, filter_=lambda node: d in node.path, mincount=4)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
      ...
    anytree.search.CountError: Expecting at least 4 elements, but found 3. ... Node('/f/b/d/e'))
    >>> findall(f, filter_=lambda node: d in node.path, maxcount=2)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
      ...
    anytree.search.CountError: Expecting 2 elements at maximum, but found 3. ... Node('/f/b/d/e'))
    """
    return _findall(node, filter_=filter_, stop=stop, maxlevel=maxlevel, mincount=mincount, maxcount=maxcount)


def findall_by_attr(node, value, name="name", maxlevel=None, mincount=None, maxcount=None):
    """
    Search nodes with attribute `name` having `value` but stop at `maxlevel`.

    Return tuple with matching nodes.

    Args:
        node: top node, start searching.
        value: value which need to match

    Keyword Args:
        name (str): attribute name need to match
        maxlevel (int): maximum descending in the node hierarchy.
        mincount (int): minimum number of nodes.
        maxcount (int): maximum number of nodes.

    Example tree:

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

    >>> findall_by_attr(f, "d")
    (Node('/f/b/d'),)
    """
    return _findall(
        node,
        filter_=lambda n: _filter_by_name(n, name, value),
        maxlevel=maxlevel,
        mincount=mincount,
        maxcount=maxcount,
    )


def find(node, filter_=None, stop=None, maxlevel=None):
    """
    Search for *single* node matching `filter_` but stop at `maxlevel` or `stop`.

    Return matching node.

    Args:
        node: top node, start searching.

    Keyword Args:
        filter_: function called with every `node` as argument, `node` is returned if `True`.
        stop: stop iteration at `node` if `stop` function returns `True` for `node`.
        maxlevel (int): maximum descending in the node hierarchy.

    Example tree:

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

    >>> find(f, lambda node: node.name == "d")
    Node('/f/b/d')
    >>> find(f, lambda node: node.name == "z")
    >>> find(f, lambda node: b in node.path)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    anytree.search.CountError: Expecting 1 elements at maximum, but found 5. (Node('/f/b')... Node('/f/b/d/e'))
    """
    return _find(node, filter_=filter_, stop=stop, maxlevel=maxlevel)


def find_by_attr(node, value, name="name", maxlevel=None):
    """
    Search for *single* node with attribute `name` having `value` but stop at `maxlevel`.

    Return matching node.

    Args:
        node: top node, start searching.
        value: value which need to match


    Keyword Args:
        name (str): attribute name need to match
        maxlevel (int): maximum descending in the node hierarchy.

    Example tree:

    >>> from anytree import Node, RenderTree, AsciiStyle
    >>> f = Node("f")
    >>> b = Node("b", parent=f)
    >>> a = Node("a", parent=b)
    >>> d = Node("d", parent=b)
    >>> c = Node("c", parent=d, foo=4)
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

    >>> find_by_attr(f, "d")
    Node('/f/b/d')
    >>> find_by_attr(f, name="foo", value=4)
    Node('/f/b/d/c', foo=4)
    >>> find_by_attr(f, name="foo", value=8)
    """
    return _find(node, filter_=lambda n: _filter_by_name(n, name, value), maxlevel=maxlevel)


def _find(node, filter_, stop=None, maxlevel=None):
    items = _findall(node, filter_, stop=stop, maxlevel=maxlevel, maxcount=1)
    return items[0] if items else None


def _findall(node, filter_, stop=None, maxlevel=None, mincount=None, maxcount=None):
    result = tuple(PreOrderIter(node, filter_, stop, maxlevel))
    resultlen = len(result)
    if mincount is not None and resultlen < mincount:
        msg = "Expecting at least %d elements, but found %d."
        raise CountError(msg % (mincount, resultlen), result)
    if maxcount is not None and resultlen > maxcount:
        msg = "Expecting %d elements at maximum, but found %d."
        raise CountError(msg % (maxcount, resultlen), result)
    return result


def _filter_by_name(node, name, value):
    try:
        return getattr(node, name) == value
    except AttributeError:
        return False


class CountError(RuntimeError):
    def __init__(self, msg, result):
        """Error raised on `mincount` or `maxcount` mismatch."""
        if result:
            msg += " " + repr(result)
        super().__init__(msg)
