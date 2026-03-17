# Copyright 2008 Armin Ronacher.
# Licensed to PSF under a Contributor Agreement.

from __future__ import generator_stop

from fissix.fixes import fix_filter

import libmodernize


class FixFilter(fix_filter.FixFilter):

    skip_on = "six.moves.filter"

    def transform(self, node, results):
        result = super().transform(node, results)
        if not libmodernize.is_listcomp(result):
            # Keep performance improvement from six.moves.filter in iterator
            # contexts on Python 2.7.
            libmodernize.touch_import("six.moves", "filter", node)
        return result
