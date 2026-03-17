from __future__ import generator_stop

from fissix import fixer_base
from fissix.fixer_util import is_probably_builtin

import libmodernize


class FixUnichr(fixer_base.ConditionalFix):
    BM_compatible = True

    skip_on = "six.moves.unichr"
    PATTERN = """'unichr'"""

    def transform(self, node, results):
        if self.should_skip(node):
            return
        if is_probably_builtin(node):
            libmodernize.touch_import("six", "unichr", node)
