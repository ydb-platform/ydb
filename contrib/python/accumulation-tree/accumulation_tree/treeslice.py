#!/usr/bin/env python
#coding:utf-8
# Author:  mozman -- <mozman@gmx.at>
# Purpose: TreeSlice
# Created: 11.04.2011
# Copyright (c) 2010-2013 by Manfred Moitzi
# License: MIT License

# Copyright (c) 2012, Manfred Moitzi
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

class TreeSlice(object):
    __slots__ = ['_tree', '_start', '_stop']

    def __init__(self, tree, start, stop):
        self._tree = tree
        self._start = start
        self._stop = stop

    def __repr__(self):
        tpl = "%s({%s})" % (self._tree.__class__.__name__, '%s')
        return tpl % ", ".join( ("%r: %r" % item for item in self.items()) )

    def __contains__(self, key):
        if self._is_in_range(key):
            return key in self._tree
        else:
            return False

    def _is_in_range(self, key):
        if self._start is not None and key < self._start:
                return False
        if self._stop is not None and key >= self._stop:
                return False
        return True

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self._sub_slice(key.start, key.stop)
        if self._is_in_range(key):
            return self._tree[key]
        else:
            raise KeyError(key)

    def _sub_slice(self, start, stop):
        def newstart():
            if start is None:
                return self._start
            elif self._start is None:
                return start
            else:
                return max(start, self._start)

        def newstop():
            if stop is None:
                return self._stop
            elif self._stop is None:
                return stop
            else:
                return min(stop, self._stop)

        return TreeSlice(self._tree, newstart(), newstop())

    def keys(self):
        return self._tree.key_slice(self._start, self._stop)
    __iter__ = keys

    def values(self):
        return self._tree.value_slice(self._start, self._stop)

    def items(self):
        return self._tree.iter_items(self._start, self._stop)
