from __future__ import generator_stop

import re

from fissix import fixer_base
from fissix.fixer_util import Call, Name

from libmodernize import touch_import

_mapping = {"unichr": "chr", "unicode": "str"}
_literal_re = re.compile("[uU][rR]?[\\'\\\"]")


class FixUnicode(fixer_base.BaseFix):
    BM_compatible = True
    PATTERN = """STRING"""

    def transform(self, node, results):
        if _literal_re.match(node.value):
            touch_import(None, "six", node)
            new = node.clone()
            new.value = new.value[1:]
            new.prefix = ""
            node.replace(Call(Name("six.u", prefix=node.prefix), [new]))
