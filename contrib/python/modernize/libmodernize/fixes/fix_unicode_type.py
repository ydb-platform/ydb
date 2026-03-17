from __future__ import generator_stop

from fissix import fixer_base, fixer_util

import libmodernize


class FixUnicodeType(fixer_base.BaseFix):
    BM_compatible = True
    PATTERN = """'unicode'"""

    def transform(self, node, results):
        libmodernize.touch_import(None, "six", node)
        return fixer_util.Name("six.text_type", prefix=node.prefix)
