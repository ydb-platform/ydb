#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2010 Søren Roug, European Environment Agency
#
# This is free software.  You may redistribute it under the terms
# of the Apache license and the GNU General Public License Version
# 2 or at your option any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public
# License along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Contributor(s):
#
import unittest

""" Really simplistic parser of an element with attributes """

class ElementParser:
    def __init__(self, s, elmttoparse):
        qelements = s.split('<')
        for i in range(len(qelements)):
            q = qelements[i]
            if q[:len(elmttoparse)] == elmttoparse:
                s = '<'.join([''] + qelements[i:])
        self.attributes = {}
        self.element = None
        currattr = None

        buf = []
        START = 1
        INELEM = 2
        SPACE = 3
        INATTR = 4
        INVALUE=5
        BEFOREVALUE = 6
        NOMORE = 7

        state=START
        ls = list(s)
        for c in ls:
            if state == NOMORE:
                continue
            if state == INVALUE: # We're in the value of the attribute. Only look for the terminator
                if c == '"':
                    state = SPACE
                    c = ''.join(buf)
                    self.attributes[currattr] = c
                    buf = []
                else:
                    buf.append(c)
            else:
                if c == '<':
                    state = INELEM
                elif c == ' ':
                    if state == INELEM:
                       self.element = ''.join(buf)
                       buf = []
                    state = SPACE
                elif c == '=':
                    if state == INATTR:
                        state = BEFOREVALUE
                        currattr = ''.join(buf)
                        buf = []
                elif c == '"':
                     state = INVALUE
                elif c == '>' or c == '/':
                    state = NOMORE
                elif c > '"' and c <= 'z' and state == SPACE: # Start of attribute
                    state = INATTR
                    buf = []
                    buf.append(c)
                else:
                    buf.append(c)

    def has_value(self, attribute, value):
        v = self.attributes.get(attribute, None)
        if v and v == value: return True
        return False

class TestParser(unittest.TestCase):
    def test1(self):
        s='<draw:object xlink:href="./Object 1"/><style:style style:name="Standard" style:display-name="Standard" style:family="paragraph"><style:property/>'
        e = ElementParser(s,'style:style')
        self.assertEqual(e.element,'style:style')
        assert e.has_value("style:display-name","Standard")

        e = ElementParser(s,'draw:object')
        self.assertEqual(e.element,'draw:object')
        assert e.has_value("xlink:href","./Object 1")

if __name__ == '__main__':
    unittest.main()
