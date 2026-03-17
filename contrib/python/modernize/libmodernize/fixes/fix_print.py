from __future__ import generator_stop

from fissix.fixes import fix_print

import libmodernize


class FixPrint(fix_print.FixPrint):
    def transform(self, node, results):
        result = super().transform(node, results)
        libmodernize.add_future(node, "print_function")
        return result
