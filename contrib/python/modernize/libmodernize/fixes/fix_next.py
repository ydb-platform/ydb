"""Fixer for it.next() -> next(it)"""

from __future__ import generator_stop

# Local imports
from fissix import fixer_base
from fissix.fixer_util import Call, Name

bind_warning = "Calls to builtin next() possibly shadowed by global binding"


class FixNext(fixer_base.BaseFix):
    BM_compatible = True
    PATTERN = """
    power< base=any+ trailer< '.' attr='next' > trailer< '(' ')' > >
    """

    order = "pre"  # Pre-order tree traversal

    def transform(self, node, results):
        base = results["base"]
        base = [n.clone() for n in base]
        base[0].prefix = ""
        node.replace(Call(Name("next", prefix=node.prefix), base))
