from __future__ import generator_stop

from fissix import fixer_util
from fissix.pgen2 import token
from fissix.pygram import python_symbols as syms
from fissix.pytree import Leaf, Node

__version__ = "0.8.0"


def check_future_import(node):
    """If this is a future import, return set of symbols that are imported,
    else return None."""
    # node should be the import statement here
    if not (node.type == syms.simple_stmt and node.children):
        return set()
    node = node.children[0]
    # now node is the import_from node
    if not (
        node.type == syms.import_from
        and node.children[1].type == token.NAME
        and node.children[1].value == "__future__"
    ):
        return set()

    if node.children[3].type == token.LPAR:
        # from __future__ import (..
        node = node.children[4]
    else:
        # from __future__ import ...
        node = node.children[3]
    # now node is the import_as_name[s]

    # print(python_grammar.number2symbol[node.type])
    if node.type == syms.import_as_names:
        result = set()
        for n in node.children:
            if n.type == token.NAME:
                result.add(n.value)
            elif n.type == syms.import_as_name:
                n = n.children[0]
                assert n.type == token.NAME
                result.add(n.value)
        return result
    elif node.type == syms.import_as_name:
        node = node.children[0]
        assert node.type == token.NAME
        return {node.value}
    elif node.type == token.NAME:
        return {node.value}
    else:  # pragma: no cover
        assert 0, "strange import"


def add_future(node, symbol):

    root = fixer_util.find_root(node)

    for idx, node in enumerate(root.children):
        if (
            node.type == syms.simple_stmt
            and len(node.children) > 0
            and node.children[0].type == token.STRING
        ):
            # skip over docstring
            continue
        names = check_future_import(node)
        if not names:
            # not a future statement; need to insert before this
            break
        if symbol in names:
            # already imported
            return

    import_ = fixer_util.FromImport(
        "__future__", [Leaf(token.NAME, symbol, prefix=" ")]
    )

    # Place after any comments or whitespace. (copyright, shebang etc.)
    import_.prefix = node.prefix
    node.prefix = ""

    children = [import_, fixer_util.Newline()]
    root.insert_child(idx, Node(syms.simple_stmt, children))


def touch_import(package, name, node):
    fixer_util.touch_import(package, name, node)


def is_listcomp(node):
    def _is_listcomp(node):
        return (
            isinstance(node, Node)
            and node.type == syms.atom
            and len(node.children) >= 2
            and isinstance(node.children[0], Leaf)
            and node.children[0].value == "["
            and isinstance(node.children[-1], Leaf)
            and node.children[-1].value == "]"
        )

    def _is_noop_power_node(node):
        """https://github.com/python/cpython/pull/2235 changed the node
        structure for fix_map / fix_filter to contain a top-level `power` node
        """
        return (
            isinstance(node, Node)
            and node.type == syms.power
            and len(node.children) == 1
        )

    return (
        _is_listcomp(node)
        or _is_noop_power_node(node)
        and _is_listcomp(node.children[0])
    )
