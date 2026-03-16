"""Utilities."""


def commonancestors(*nodes):
    """
    Determine common ancestors of `nodes`.

    >>> from anytree import Node, util
    >>> udo = Node("Udo")
    >>> marc = Node("Marc", parent=udo)
    >>> lian = Node("Lian", parent=marc)
    >>> dan = Node("Dan", parent=udo)
    >>> jet = Node("Jet", parent=dan)
    >>> jan = Node("Jan", parent=dan)
    >>> joe = Node("Joe", parent=dan)

    >>> util.commonancestors(jet, joe)
    (Node('/Udo'), Node('/Udo/Dan'))
    >>> util.commonancestors(jet, marc)
    (Node('/Udo'),)
    >>> util.commonancestors(jet)
    (Node('/Udo'), Node('/Udo/Dan'))
    >>> util.commonancestors()
    ()
    """
    ancestors = [node.ancestors for node in nodes]
    common = []
    for parentnodes in zip(*ancestors):
        parentnode = parentnodes[0]
        if all(parentnode is p for p in parentnodes[1:]):
            common.append(parentnode)
        else:
            break
    return tuple(common)


def leftsibling(node):
    """
    Return Left Sibling of `node`.

    >>> from anytree import Node, util
    >>> dan = Node("Dan")
    >>> jet = Node("Jet", parent=dan)
    >>> jan = Node("Jan", parent=dan)
    >>> joe = Node("Joe", parent=dan)
    >>> print(util.leftsibling(dan))
    None
    >>> print(util.leftsibling(jet))
    None
    >>> print(util.leftsibling(jan))
    Node('/Dan/Jet')
    >>> print(util.leftsibling(joe))
    Node('/Dan/Jan')
    """
    if node.parent:
        pchildren = node.parent.children
        idx = pchildren.index(node)
        if idx:
            return pchildren[idx - 1]
    return None


def rightsibling(node):
    """
    Return Right Sibling of `node`.

    >>> from anytree import Node, util
    >>> dan = Node("Dan")
    >>> jet = Node("Jet", parent=dan)
    >>> jan = Node("Jan", parent=dan)
    >>> joe = Node("Joe", parent=dan)
    >>> print(util.rightsibling(dan))
    None
    >>> print(util.rightsibling(jet))
    Node('/Dan/Jan')
    >>> print(util.rightsibling(jan))
    Node('/Dan/Joe')
    >>> print(util.rightsibling(joe))
    None
    """
    if node.parent:
        pchildren = node.parent.children
        idx = pchildren.index(node)
        try:
            return pchildren[idx + 1]
        except IndexError:
            return None
    else:
        return None
