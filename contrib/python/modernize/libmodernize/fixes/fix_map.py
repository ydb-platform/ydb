# Copyright 2008 Armin Ronacher.
# Licensed to PSF under a Contributor Agreement.

from __future__ import generator_stop

from fissix.fixes import fix_map

import libmodernize


class FixMap(fix_map.FixMap):

    skip_on = "six.moves.map"

    def transform(self, node, results):
        result = super().transform(node, results)
        if not libmodernize.is_listcomp(result):
            # Always use the import even if no change is required so as to have
            # improved performance in iterator contexts even on Python 2.7.
            libmodernize.touch_import("six.moves", "map", node)
        return result
