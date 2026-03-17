"""Fixer for 'raise E, V, T'

raise E, V, T -> six.reraise(E, V, T)

"""
# Author : Markus Unterwaditzer
from __future__ import generator_stop

# Local imports
from fissix import fixer_base
from fissix.fixer_util import Call, Comma, Name

from libmodernize import touch_import


class FixRaiseSix(fixer_base.BaseFix):

    BM_compatible = True
    PATTERN = """
    raise_stmt< 'raise' exc=any ',' val=any ',' tb=any >
    """

    def transform(self, node, results):
        exc = results["exc"].clone()
        val = results["val"].clone()
        tb = results["tb"].clone()

        exc.prefix = ""
        val.prefix = tb.prefix = " "

        touch_import(None, "six", node)
        return Call(
            Name("six.reraise"), [exc, Comma(), val, Comma(), tb], prefix=node.prefix
        )
