# -*- coding: utf-8 -*-
#
#   Test the easyliststyle.py module
#   Copyright (C) 2008 J. David Eisenberg

#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.

#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.

#   You should have received a copy of the GNU General Public License along
#   with this program; if not, write to the Free Software Foundation, Inc.,
#   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Contributor(s):
# Søren Roug

import unittest, sys
from odf import easyliststyle
from odf.opendocument import OpenDocumentText
from odf.style import Style, TextProperties
from odf.text import P, List, ListItem

"""
This program tests the easyliststyle.py module.
It creates a file named "easylist_odfpy.odt"
with a bulleted list, a numbered list, and a
mixed list.
"""

bulletListSpec = '*,>,#,%'
mixedListSpec = u'1.!\u273f!a)'
numberListSpecArray = ('I', '1:', 'a')

itemList = (
    "Cats",
    ">Domestic Shorthair",
    ">Domestic Longhair",
    ">Purebred",
    ">>Russian Blue",
    ">>Siamese",
    ">>>Seal Point",
     ">>>Flame Point",
    "Dogs",
    ">Retrievers",
    ">>Golden Retriever",
    ">>Labrador Retriever",
    ">Poodles",
    ">>Toy Poodle",
    ">>Standard Poodle"
)

class TestEasyListStype(unittest.TestCase):

    def createList(self, itemList, indentDelim, styleName):
        listArray = []
        listItem = ListItem()
        level = 0
        lastLevel = 0

        for levCount in range(0,10):
            listArray.append(None)
        listArray[0] = List()

        for item in itemList:
            level = 0;
            while (level < len(item) and item[level] == indentDelim):
                level +=1
            item = item[level:]

            if (level > lastLevel):    # open the sub-levels
                for levCount in range(lastLevel+1, level+1):
                    listArray[levCount] = List()
            elif (level < lastLevel):    # close off the intervening lists
                for levCount in range(lastLevel, level, -1):
                    listArray[levCount-1].childNodes[-1].addElement(listArray[levCount])

            # now that we are at the proper level, add the item.
            listArray[level].setAttribute( 'stylename', styleName );
            listItem = ListItem()
            para = P(text=item);
            listItem.addElement(para);
            listArray[level].addElement(listItem);
            lastLevel = level;

        # close off any remaining open lists
        for levCount in range(lastLevel, 0, -1):
            listArray[levCount-1].childNodes[-1].addElement(listArray[levCount])
        return listArray[0]

    def test_list(self):
        textdoc = OpenDocumentText()

        s = textdoc.styles
        listStyle = easyliststyle.styleFromString(u'bullet1', bulletListSpec,
            u',', u'0.6cm', easyliststyle.SHOW_ONE_LEVEL)
        s.addElement(listStyle)
        result = textdoc.stylesxml()
        self.assertNotEqual(-1, result.find(u'''style:name="bullet1"'''))
        self.assertNotEqual(-1, result.find(u'''text:bullet-char="*"'''))
        self.assertNotEqual(-1, result.find(u'''text:level="1"'''))
        self.assertNotEqual(-1, result.find(u'''style:list-level-properties'''))
        #<text:list-style style:name="bullet1" style:display-name="bullet1">
        #<text:list-level-style-bullet text:bullet-char="*" text:level="1">
        #<style:list-level-properties text:min-label-width="0.6cm" text:space-before="0.6cm"/>
        #</text:list-level-style-bullet>
        #<text:list-level-style-bullet text:bullet-char="&gt;" text:level="2">
        #<style:list-level-properties text:min-label-width="0.6cm" text:space-before="1.2cm"/>
        #</text:list-level-style-bullet>
        #<text:list-level-style-bullet text:bullet-char="#" text:level="3">
        #<style:list-level-properties text:min-label-width="0.6cm" text:space-before="1.8cm"/>
        #</text:list-level-style-bullet>
        #<text:list-level-style-bullet text:bullet-char="%" text:level="4">
        #<style:list-level-properties text:min-label-width="0.6cm" text:space-before="2.4cm"/>

        listElement = self.createList(itemList, u'>', u'bullet1')
        textdoc.text.addElement(listElement)

        para = P(text="-----------------------");
        textdoc.text.addElement(para)

        listStyle = easyliststyle.styleFromList('num1', numberListSpecArray,
            '0.25in', easyliststyle.SHOW_ALL_LEVELS)
        s.addElement(listStyle)

        listElement = self.createList(itemList, '>', 'num1')
        textdoc.text.addElement(listElement)

        para = P(text="-----------------------");
        textdoc.text.addElement(para)

        listStyle = easyliststyle.styleFromString('mix1', mixedListSpec,
            '!', '0.8cm', easyliststyle.SHOW_ONE_LEVEL)
        s.addElement(listStyle)

        listElement = self.createList(itemList, '>', 'mix1')
        textdoc.text.addElement(listElement)


if __name__ == '__main__':
    unittest.main()

# vim: set expandtab sw=4 :
