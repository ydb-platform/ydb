#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  Scour
#
#  Copyright 2010 Jeff Schiller
#  Copyright 2010 Louis Simard
#  Copyright 2013-2014 Tavendo GmbH
#
#  This file is part of Scour, http://www.codedread.com/scour/
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# Notes:

# rubys' path-crunching ideas here: http://intertwingly.net/code/svgtidy/spec.rb
# (and implemented here: http://intertwingly.net/code/svgtidy/svgtidy.rb )

# Yet more ideas here: http://wiki.inkscape.org/wiki/index.php/Save_Cleaned_SVG
#
# * Process Transformations
#  * Collapse all group based transformations

# Even more ideas here: http://esw.w3.org/topic/SvgTidy
#  * analysis of path elements to see if rect can be used instead?
#    (must also need to look at rounded corners)

# Next Up:
# - why are marker-start, -end not removed from the style attribute?
# - why are only overflow style properties considered and not attributes?
# - only remove unreferenced elements if they are not children of a referenced element
# - add an option to remove ids if they match the Inkscape-style of IDs
# - investigate point-reducing algorithms
# - parse transform attribute
# - if a <g> has only one element in it, collapse the <g> (ensure transform, etc are carried down)


from __future__ import division         # use "true" division instead of integer division in Python 2 (see PEP 238)
from __future__ import print_function   # use print() as a function in Python 2 (see PEP 3105)
from __future__ import absolute_import  # use absolute imports by default in Python 2 (see PEP 328)

import math
import optparse
import os
import re
import sys
import time
import xml.dom.minidom
from xml.dom import Node, NotFoundErr
from collections import namedtuple, defaultdict
from decimal import Context, Decimal, InvalidOperation, getcontext

import six
from six.moves import range, urllib

from scour.svg_regex import svg_parser
from scour.svg_transform import svg_transform_parser
from scour.yocto_css import parseCssString
from scour import __version__


APP = u'scour'
VER = __version__
COPYRIGHT = u'Copyright Jeff Schiller, Louis Simard, 2010'


XML_ENTS_NO_QUOTES = {'<': '&lt;', '>': '&gt;', '&': '&amp;'}
XML_ENTS_ESCAPE_APOS = XML_ENTS_NO_QUOTES.copy()
XML_ENTS_ESCAPE_APOS["'"] = '&apos;'
XML_ENTS_ESCAPE_QUOT = XML_ENTS_NO_QUOTES.copy()
XML_ENTS_ESCAPE_QUOT['"'] = '&quot;'

# Used to split values where "x y" or "x,y" or a mix of the two is allowed
RE_COMMA_WSP = re.compile(r"\s*[\s,]\s*")

NS = {'SVG':      'http://www.w3.org/2000/svg',
      'XLINK':    'http://www.w3.org/1999/xlink',
      'SODIPODI': 'http://sodipodi.sourceforge.net/DTD/sodipodi-0.dtd',
      'INKSCAPE': 'http://www.inkscape.org/namespaces/inkscape',
      'ADOBE_ILLUSTRATOR': 'http://ns.adobe.com/AdobeIllustrator/10.0/',
      'ADOBE_GRAPHS': 'http://ns.adobe.com/Graphs/1.0/',
      'ADOBE_SVG_VIEWER': 'http://ns.adobe.com/AdobeSVGViewerExtensions/3.0/',
      'ADOBE_VARIABLES': 'http://ns.adobe.com/Variables/1.0/',
      'ADOBE_SFW': 'http://ns.adobe.com/SaveForWeb/1.0/',
      'ADOBE_EXTENSIBILITY': 'http://ns.adobe.com/Extensibility/1.0/',
      'ADOBE_FLOWS': 'http://ns.adobe.com/Flows/1.0/',
      'ADOBE_IMAGE_REPLACEMENT': 'http://ns.adobe.com/ImageReplacement/1.0/',
      'ADOBE_CUSTOM': 'http://ns.adobe.com/GenericCustomNamespace/1.0/',
      'ADOBE_XPATH': 'http://ns.adobe.com/XPath/1.0/',
      'SKETCH': 'http://www.bohemiancoding.com/sketch/ns'
      }

unwanted_ns = [NS['SODIPODI'], NS['INKSCAPE'], NS['ADOBE_ILLUSTRATOR'],
               NS['ADOBE_GRAPHS'], NS['ADOBE_SVG_VIEWER'], NS['ADOBE_VARIABLES'],
               NS['ADOBE_SFW'], NS['ADOBE_EXTENSIBILITY'], NS['ADOBE_FLOWS'],
               NS['ADOBE_IMAGE_REPLACEMENT'], NS['ADOBE_CUSTOM'],
               NS['ADOBE_XPATH'], NS['SKETCH']]

# A list of all SVG presentation properties
#
# Sources for this list:
#     https://www.w3.org/TR/SVG/propidx.html              (implemented)
#     https://www.w3.org/TR/SVGTiny12/attributeTable.html (implemented)
#     https://www.w3.org/TR/SVG2/propidx.html             (not yet implemented)
#
svgAttributes = [
    # SVG 1.1
    'alignment-baseline',
    'baseline-shift',
    'clip',
    'clip-path',
    'clip-rule',
    'color',
    'color-interpolation',
    'color-interpolation-filters',
    'color-profile',
    'color-rendering',
    'cursor',
    'direction',
    'display',
    'dominant-baseline',
    'enable-background',
    'fill',
    'fill-opacity',
    'fill-rule',
    'filter',
    'flood-color',
    'flood-opacity',
    'font',
    'font-family',
    'font-size',
    'font-size-adjust',
    'font-stretch',
    'font-style',
    'font-variant',
    'font-weight',
    'glyph-orientation-horizontal',
    'glyph-orientation-vertical',
    'image-rendering',
    'kerning',
    'letter-spacing',
    'lighting-color',
    'marker',
    'marker-end',
    'marker-mid',
    'marker-start',
    'mask',
    'opacity',
    'overflow',
    'pointer-events',
    'shape-rendering',
    'stop-color',
    'stop-opacity',
    'stroke',
    'stroke-dasharray',
    'stroke-dashoffset',
    'stroke-linecap',
    'stroke-linejoin',
    'stroke-miterlimit',
    'stroke-opacity',
    'stroke-width',
    'text-anchor',
    'text-decoration',
    'text-rendering',
    'unicode-bidi',
    'visibility',
    'word-spacing',
    'writing-mode',
    # SVG 1.2 Tiny
    'audio-level',
    'buffered-rendering',
    'display-align',
    'line-increment',
    'solid-color',
    'solid-opacity',
    'text-align',
    'vector-effect',
    'viewport-fill',
    'viewport-fill-opacity',
]

colors = {
    'aliceblue': 'rgb(240, 248, 255)',
    'antiquewhite': 'rgb(250, 235, 215)',
    'aqua': 'rgb( 0, 255, 255)',
    'aquamarine': 'rgb(127, 255, 212)',
    'azure': 'rgb(240, 255, 255)',
    'beige': 'rgb(245, 245, 220)',
    'bisque': 'rgb(255, 228, 196)',
    'black': 'rgb( 0, 0, 0)',
    'blanchedalmond': 'rgb(255, 235, 205)',
    'blue': 'rgb( 0, 0, 255)',
    'blueviolet': 'rgb(138, 43, 226)',
    'brown': 'rgb(165, 42, 42)',
    'burlywood': 'rgb(222, 184, 135)',
    'cadetblue': 'rgb( 95, 158, 160)',
    'chartreuse': 'rgb(127, 255, 0)',
    'chocolate': 'rgb(210, 105, 30)',
    'coral': 'rgb(255, 127, 80)',
    'cornflowerblue': 'rgb(100, 149, 237)',
    'cornsilk': 'rgb(255, 248, 220)',
    'crimson': 'rgb(220, 20, 60)',
    'cyan': 'rgb( 0, 255, 255)',
    'darkblue': 'rgb( 0, 0, 139)',
    'darkcyan': 'rgb( 0, 139, 139)',
    'darkgoldenrod': 'rgb(184, 134, 11)',
    'darkgray': 'rgb(169, 169, 169)',
    'darkgreen': 'rgb( 0, 100, 0)',
    'darkgrey': 'rgb(169, 169, 169)',
    'darkkhaki': 'rgb(189, 183, 107)',
    'darkmagenta': 'rgb(139, 0, 139)',
    'darkolivegreen': 'rgb( 85, 107, 47)',
    'darkorange': 'rgb(255, 140, 0)',
    'darkorchid': 'rgb(153, 50, 204)',
    'darkred': 'rgb(139, 0, 0)',
    'darksalmon': 'rgb(233, 150, 122)',
    'darkseagreen': 'rgb(143, 188, 143)',
    'darkslateblue': 'rgb( 72, 61, 139)',
    'darkslategray': 'rgb( 47, 79, 79)',
    'darkslategrey': 'rgb( 47, 79, 79)',
    'darkturquoise': 'rgb( 0, 206, 209)',
    'darkviolet': 'rgb(148, 0, 211)',
    'deeppink': 'rgb(255, 20, 147)',
    'deepskyblue': 'rgb( 0, 191, 255)',
    'dimgray': 'rgb(105, 105, 105)',
    'dimgrey': 'rgb(105, 105, 105)',
    'dodgerblue': 'rgb( 30, 144, 255)',
    'firebrick': 'rgb(178, 34, 34)',
    'floralwhite': 'rgb(255, 250, 240)',
    'forestgreen': 'rgb( 34, 139, 34)',
    'fuchsia': 'rgb(255, 0, 255)',
    'gainsboro': 'rgb(220, 220, 220)',
    'ghostwhite': 'rgb(248, 248, 255)',
    'gold': 'rgb(255, 215, 0)',
    'goldenrod': 'rgb(218, 165, 32)',
    'gray': 'rgb(128, 128, 128)',
    'grey': 'rgb(128, 128, 128)',
    'green': 'rgb( 0, 128, 0)',
    'greenyellow': 'rgb(173, 255, 47)',
    'honeydew': 'rgb(240, 255, 240)',
    'hotpink': 'rgb(255, 105, 180)',
    'indianred': 'rgb(205, 92, 92)',
    'indigo': 'rgb( 75, 0, 130)',
    'ivory': 'rgb(255, 255, 240)',
    'khaki': 'rgb(240, 230, 140)',
    'lavender': 'rgb(230, 230, 250)',
    'lavenderblush': 'rgb(255, 240, 245)',
    'lawngreen': 'rgb(124, 252, 0)',
    'lemonchiffon': 'rgb(255, 250, 205)',
    'lightblue': 'rgb(173, 216, 230)',
    'lightcoral': 'rgb(240, 128, 128)',
    'lightcyan': 'rgb(224, 255, 255)',
    'lightgoldenrodyellow': 'rgb(250, 250, 210)',
    'lightgray': 'rgb(211, 211, 211)',
    'lightgreen': 'rgb(144, 238, 144)',
    'lightgrey': 'rgb(211, 211, 211)',
    'lightpink': 'rgb(255, 182, 193)',
    'lightsalmon': 'rgb(255, 160, 122)',
    'lightseagreen': 'rgb( 32, 178, 170)',
    'lightskyblue': 'rgb(135, 206, 250)',
    'lightslategray': 'rgb(119, 136, 153)',
    'lightslategrey': 'rgb(119, 136, 153)',
    'lightsteelblue': 'rgb(176, 196, 222)',
    'lightyellow': 'rgb(255, 255, 224)',
    'lime': 'rgb( 0, 255, 0)',
    'limegreen': 'rgb( 50, 205, 50)',
    'linen': 'rgb(250, 240, 230)',
    'magenta': 'rgb(255, 0, 255)',
    'maroon': 'rgb(128, 0, 0)',
    'mediumaquamarine': 'rgb(102, 205, 170)',
    'mediumblue': 'rgb( 0, 0, 205)',
    'mediumorchid': 'rgb(186, 85, 211)',
    'mediumpurple': 'rgb(147, 112, 219)',
    'mediumseagreen': 'rgb( 60, 179, 113)',
    'mediumslateblue': 'rgb(123, 104, 238)',
    'mediumspringgreen': 'rgb( 0, 250, 154)',
    'mediumturquoise': 'rgb( 72, 209, 204)',
    'mediumvioletred': 'rgb(199, 21, 133)',
    'midnightblue': 'rgb( 25, 25, 112)',
    'mintcream': 'rgb(245, 255, 250)',
    'mistyrose': 'rgb(255, 228, 225)',
    'moccasin': 'rgb(255, 228, 181)',
    'navajowhite': 'rgb(255, 222, 173)',
    'navy': 'rgb( 0, 0, 128)',
    'oldlace': 'rgb(253, 245, 230)',
    'olive': 'rgb(128, 128, 0)',
    'olivedrab': 'rgb(107, 142, 35)',
    'orange': 'rgb(255, 165, 0)',
    'orangered': 'rgb(255, 69, 0)',
    'orchid': 'rgb(218, 112, 214)',
    'palegoldenrod': 'rgb(238, 232, 170)',
    'palegreen': 'rgb(152, 251, 152)',
    'paleturquoise': 'rgb(175, 238, 238)',
    'palevioletred': 'rgb(219, 112, 147)',
    'papayawhip': 'rgb(255, 239, 213)',
    'peachpuff': 'rgb(255, 218, 185)',
    'peru': 'rgb(205, 133, 63)',
    'pink': 'rgb(255, 192, 203)',
    'plum': 'rgb(221, 160, 221)',
    'powderblue': 'rgb(176, 224, 230)',
    'purple': 'rgb(128, 0, 128)',
    'red': 'rgb(255, 0, 0)',
    'rosybrown': 'rgb(188, 143, 143)',
    'royalblue': 'rgb( 65, 105, 225)',
    'saddlebrown': 'rgb(139, 69, 19)',
    'salmon': 'rgb(250, 128, 114)',
    'sandybrown': 'rgb(244, 164, 96)',
    'seagreen': 'rgb( 46, 139, 87)',
    'seashell': 'rgb(255, 245, 238)',
    'sienna': 'rgb(160, 82, 45)',
    'silver': 'rgb(192, 192, 192)',
    'skyblue': 'rgb(135, 206, 235)',
    'slateblue': 'rgb(106, 90, 205)',
    'slategray': 'rgb(112, 128, 144)',
    'slategrey': 'rgb(112, 128, 144)',
    'snow': 'rgb(255, 250, 250)',
    'springgreen': 'rgb( 0, 255, 127)',
    'steelblue': 'rgb( 70, 130, 180)',
    'tan': 'rgb(210, 180, 140)',
    'teal': 'rgb( 0, 128, 128)',
    'thistle': 'rgb(216, 191, 216)',
    'tomato': 'rgb(255, 99, 71)',
    'turquoise': 'rgb( 64, 224, 208)',
    'violet': 'rgb(238, 130, 238)',
    'wheat': 'rgb(245, 222, 179)',
    'white': 'rgb(255, 255, 255)',
    'whitesmoke': 'rgb(245, 245, 245)',
    'yellow': 'rgb(255, 255, 0)',
    'yellowgreen': 'rgb(154, 205, 50)',
}

# A list of default poperties that are safe to remove
#
# Sources for this list:
#     https://www.w3.org/TR/SVG/propidx.html              (implemented)
#     https://www.w3.org/TR/SVGTiny12/attributeTable.html (implemented)
#     https://www.w3.org/TR/SVG2/propidx.html             (not yet implemented)
#
default_properties = {  # excluded all properties with 'auto' as default
    # SVG 1.1 presentation attributes
    'baseline-shift': 'baseline',
    'clip-path': 'none',
    'clip-rule': 'nonzero',
    'color': '#000',
    'color-interpolation-filters': 'linearRGB',
    'color-interpolation': 'sRGB',
    'direction': 'ltr',
    'display': 'inline',
    'enable-background': 'accumulate',
    'fill': '#000',
    'fill-opacity': '1',
    'fill-rule': 'nonzero',
    'filter': 'none',
    'flood-color': '#000',
    'flood-opacity': '1',
    'font-size-adjust': 'none',
    'font-size': 'medium',
    'font-stretch': 'normal',
    'font-style': 'normal',
    'font-variant': 'normal',
    'font-weight': 'normal',
    'glyph-orientation-horizontal': '0deg',
    'letter-spacing': 'normal',
    'lighting-color': '#fff',
    'marker': 'none',
    'marker-start': 'none',
    'marker-mid': 'none',
    'marker-end': 'none',
    'mask': 'none',
    'opacity': '1',
    'pointer-events': 'visiblePainted',
    'stop-color': '#000',
    'stop-opacity': '1',
    'stroke': 'none',
    'stroke-dasharray': 'none',
    'stroke-dashoffset': '0',
    'stroke-linecap': 'butt',
    'stroke-linejoin': 'miter',
    'stroke-miterlimit': '4',
    'stroke-opacity': '1',
    'stroke-width': '1',
    'text-anchor': 'start',
    'text-decoration': 'none',
    'unicode-bidi': 'normal',
    'visibility': 'visible',
    'word-spacing': 'normal',
    'writing-mode': 'lr-tb',
    # SVG 1.2 tiny properties
    'audio-level': '1',
    'solid-color': '#000',
    'solid-opacity': '1',
    'text-align': 'start',
    'vector-effect': 'none',
    'viewport-fill': 'none',
    'viewport-fill-opacity': '1',
}


def is_same_sign(a, b):
    return (a <= 0 and b <= 0) or (a >= 0 and b >= 0)


def is_same_direction(x1, y1, x2, y2):
    if is_same_sign(x1, x2) and is_same_sign(y1, y2):
        diff = y1/x1 - y2/x2
        return scouringContext.plus(1 + diff) == 1
    else:
        return False


scinumber = re.compile(r"[-+]?(\d*\.?)?\d+[eE][-+]?\d+")
number = re.compile(r"[-+]?(\d*\.?)?\d+")
sciExponent = re.compile(r"[eE]([-+]?\d+)")
unit = re.compile("(em|ex|px|pt|pc|cm|mm|in|%){1,1}$")


class Unit(object):
    # Integer constants for units.
    INVALID = -1
    NONE = 0
    PCT = 1
    PX = 2
    PT = 3
    PC = 4
    EM = 5
    EX = 6
    CM = 7
    MM = 8
    IN = 9

    # String to Unit. Basically, converts unit strings to their integer constants.
    s2u = {
        '': NONE,
        '%': PCT,
        'px': PX,
        'pt': PT,
        'pc': PC,
        'em': EM,
        'ex': EX,
        'cm': CM,
        'mm': MM,
        'in': IN,
    }

    # Unit to String. Basically, converts unit integer constants to their corresponding strings.
    u2s = {
        NONE: '',
        PCT: '%',
        PX: 'px',
        PT: 'pt',
        PC: 'pc',
        EM: 'em',
        EX: 'ex',
        CM: 'cm',
        MM: 'mm',
        IN: 'in',
    }

#  @staticmethod
    def get(unitstr):
        if unitstr is None:
            return Unit.NONE
        try:
            return Unit.s2u[unitstr]
        except KeyError:
            return Unit.INVALID

#  @staticmethod
    def str(unitint):
        try:
            return Unit.u2s[unitint]
        except KeyError:
            return 'INVALID'

    get = staticmethod(get)
    str = staticmethod(str)


class SVGLength(object):

    def __init__(self, str):
        try:  # simple unitless and no scientific notation
            self.value = float(str)
            if int(self.value) == self.value:
                self.value = int(self.value)
            self.units = Unit.NONE
        except ValueError:
            # we know that the length string has an exponent, a unit, both or is invalid

            # parse out number, exponent and unit
            self.value = 0
            unitBegin = 0
            scinum = scinumber.match(str)
            if scinum is not None:
                # this will always match, no need to check it
                numMatch = number.match(str)
                expMatch = sciExponent.search(str, numMatch.start(0))
                self.value = (float(numMatch.group(0)) *
                              10 ** float(expMatch.group(1)))
                unitBegin = expMatch.end(1)
            else:
                # unit or invalid
                numMatch = number.match(str)
                if numMatch is not None:
                    self.value = float(numMatch.group(0))
                    unitBegin = numMatch.end(0)

            if int(self.value) == self.value:
                self.value = int(self.value)

            if unitBegin != 0:
                unitMatch = unit.search(str, unitBegin)
                if unitMatch is not None:
                    self.units = Unit.get(unitMatch.group(0))

            # invalid
            else:
                # TODO: this needs to set the default for the given attribute (how?)
                self.value = 0
                self.units = Unit.INVALID


def findElementsWithId(node, elems=None):
    """
    Returns all elements with id attributes
    """
    if elems is None:
        elems = {}
    id = node.getAttribute('id')
    if id != '':
        elems[id] = node
    if node.hasChildNodes():
        for child in node.childNodes:
            # from http://www.w3.org/TR/DOM-Level-2-Core/idl-definitions.html
            # we are only really interested in nodes of type Element (1)
            if child.nodeType == Node.ELEMENT_NODE:
                findElementsWithId(child, elems)
    return elems


referencingProps = ['fill', 'stroke', 'filter', 'clip-path', 'mask',  'marker-start', 'marker-end', 'marker-mid']


def findReferencedElements(node, ids=None):
    """
    Returns IDs of all referenced elements
    - node is the node at which to start the search.
    - returns a map which has the id as key and
      each value is is a set of nodes

    Currently looks at 'xlink:href' and all attributes in 'referencingProps'
    """
    global referencingProps
    if ids is None:
        ids = {}
    # TODO: input argument ids is clunky here (see below how it is called)
    # GZ: alternative to passing dict, use **kwargs

    # if this node is a style element, parse its text into CSS
    if node.nodeName == 'style' and node.namespaceURI == NS['SVG']:
        # one stretch of text, please! (we could use node.normalize(), but
        # this actually modifies the node, and we don't want to keep
        # whitespace around if there's any)
        stylesheet = "".join(child.nodeValue for child in node.childNodes)
        if stylesheet != '':
            cssRules = parseCssString(stylesheet)
            for rule in cssRules:
                for propname in rule['properties']:
                    propval = rule['properties'][propname]
                    findReferencingProperty(node, propname, propval, ids)
        return ids

    # else if xlink:href is set, then grab the id
    href = node.getAttributeNS(NS['XLINK'], 'href')
    if href != '' and len(href) > 1 and href[0] == '#':
        # we remove the hash mark from the beginning of the id
        id = href[1:]
        if id in ids:
            ids[id].add(node)
        else:
            ids[id] = {node}

    # now get all style properties and the fill, stroke, filter attributes
    styles = node.getAttribute('style').split(';')

    for style in styles:
        propval = style.split(':')
        if len(propval) == 2:
            prop = propval[0].strip()
            val = propval[1].strip()
            findReferencingProperty(node, prop, val, ids)

    for attr in referencingProps:
        val = node.getAttribute(attr).strip()
        if not val:
            continue
        findReferencingProperty(node, attr, val, ids)

    if node.hasChildNodes():
        for child in node.childNodes:
            if child.nodeType == Node.ELEMENT_NODE:
                findReferencedElements(child, ids)
    return ids


def findReferencingProperty(node, prop, val, ids):
    global referencingProps
    if prop in referencingProps and val != '':
        if len(val) >= 7 and val[0:5] == 'url(#':
            id = val[5:val.find(')')]
            if id in ids:
                ids[id].add(node)
            else:
                ids[id] = {node}
        # if the url has a quote in it, we need to compensate
        elif len(val) >= 8:
            id = None
            # double-quote
            if val[0:6] == 'url("#':
                id = val[6:val.find('")')]
            # single-quote
            elif val[0:6] == "url('#":
                id = val[6:val.find("')")]
            if id is not None:
                if id in ids:
                    ids[id].add(node)
                else:
                    ids[id] = {node}


def removeUnusedDefs(doc, defElem, elemsToRemove=None, referencedIDs=None):
    if elemsToRemove is None:
        elemsToRemove = []

    # removeUnusedDefs do not change the XML itself; therefore there is no point in
    # recomputing findReferencedElements when we recurse into child nodes.
    if referencedIDs is None:
        referencedIDs = findReferencedElements(doc.documentElement)

    keepTags = ['font', 'style', 'metadata', 'script', 'title', 'desc']
    for elem in defElem.childNodes:
        # only look at it if an element and not referenced anywhere else
        if elem.nodeType != Node.ELEMENT_NODE:
            continue

        elem_id = elem.getAttribute('id')

        if elem_id == '' or elem_id not in referencedIDs:
            # we only inspect the children of a group in a defs if the group
            # is not referenced anywhere else
            if elem.nodeName == 'g' and elem.namespaceURI == NS['SVG']:
                elemsToRemove = removeUnusedDefs(doc, elem, elemsToRemove, referencedIDs=referencedIDs)
            # we only remove if it is not one of our tags we always keep (see above)
            elif elem.nodeName not in keepTags:
                elemsToRemove.append(elem)
    return elemsToRemove


def removeUnreferencedElements(doc, keepDefs):
    """
    Removes all unreferenced elements except for <svg>, <font>, <metadata>, <title>, and <desc>.
    Also vacuums the defs of any non-referenced renderable elements.

    Returns the number of unreferenced elements removed from the document.
    """
    global _num_elements_removed
    num = 0

    # Remove certain unreferenced elements outside of defs
    removeTags = ['linearGradient', 'radialGradient', 'pattern']
    identifiedElements = findElementsWithId(doc.documentElement)
    referencedIDs = findReferencedElements(doc.documentElement)

    if not keepDefs:
        # Remove most unreferenced elements inside defs
        defs = doc.documentElement.getElementsByTagName('defs')
        for aDef in defs:
            elemsToRemove = removeUnusedDefs(doc, aDef, referencedIDs=referencedIDs)
            for elem in elemsToRemove:
                elem.parentNode.removeChild(elem)
                _num_elements_removed += 1
                num += 1

    for id in identifiedElements:
        if id not in referencedIDs:
            goner = identifiedElements[id]
            if (goner is not None and goner.nodeName in removeTags
                and goner.parentNode is not None
                    and goner.parentNode.tagName != 'defs'):
                goner.parentNode.removeChild(goner)
                num += 1
                _num_elements_removed += 1

    return num


def shortenIDs(doc, prefix, options):
    """
    Shortens ID names used in the document. ID names referenced the most often are assigned the
    shortest ID names.

    Returns the number of bytes saved by shortening ID names in the document.
    """
    num = 0

    identifiedElements = findElementsWithId(doc.documentElement)
    # This map contains maps the (original) ID to the nodes referencing it.
    # At the end of this function, it will no longer be valid and while we
    # could keep it up to date, it will complicate the code for no gain
    # (as we do not reuse the data structure beyond this function).
    referencedIDs = findReferencedElements(doc.documentElement)

    # Make idList (list of idnames) sorted by reference count
    # descending, so the highest reference count is first.
    # First check that there's actually a defining element for the current ID name.
    # (Cyn: I've seen documents with #id references but no element with that ID!)
    idList = [(len(referencedIDs[rid]), rid) for rid in referencedIDs
              if rid in identifiedElements]
    idList.sort(reverse=True)
    idList = [rid for count, rid in idList]

    # Add unreferenced IDs to end of idList in arbitrary order
    idList.extend([rid for rid in identifiedElements if rid not in idList])
    # Ensure we do not reuse a protected ID by accident
    protectedIDs = protected_ids(identifiedElements, options)
    # IDs that have been allocated and should not be remapped.
    consumedIDs = set()

    # List of IDs that need to be assigned a new ID.  The list is ordered
    # such that earlier entries will be assigned a shorter ID than those
    # later in the list.  IDs in this list *can* obtain an ID that is
    # longer than they already are.
    need_new_id = []

    id_allocations = list(compute_id_lengths(len(idList) + 1))
    # Reverse so we can use it as a stack and still work from "shortest to
    # longest" ID.
    id_allocations.reverse()

    # Here we loop over all current IDs (that we /might/ want to remap)
    # and group them into two.  1) The IDs that already have a perfect
    # length (these are added to consumedIDs) and 2) the IDs that need
    # to change length (these are appended to need_new_id).
    optimal_id_length, id_use_limit = 0, 0
    for current_id in idList:
        # If we are out of IDs of the current length, then move on
        # to the next length
        if id_use_limit < 1:
            optimal_id_length, id_use_limit = id_allocations.pop()
        # Reserve an ID from this length
        id_use_limit -= 1
        # We check for strictly equal to optimal length because our ID
        # remapping may have to assign one node a longer ID because
        # another node needs a shorter ID.
        if len(current_id) == optimal_id_length:
            # This rid is already of optimal length - lets just keep it.
            consumedIDs.add(current_id)
        else:
            # Needs a new (possibly longer) ID.
            need_new_id.append(current_id)

    curIdNum = 1

    for old_id in need_new_id:
        new_id = intToID(curIdNum, prefix)

        # Skip ahead if the new ID has already been used or is protected.
        while new_id in protectedIDs or new_id in consumedIDs:
            curIdNum += 1
            new_id = intToID(curIdNum, prefix)

        # Now that we have found the first available ID, do the remap.
        num += renameID(old_id, new_id, identifiedElements, referencedIDs.get(old_id))
        curIdNum += 1

    return num


def compute_id_lengths(highest):
    """Compute how many IDs are available of a given size

    Example:
        >>> lengths = list(compute_id_lengths(512))
        >>> lengths
        [(1, 26), (2, 676)]
        >>> total_limit = sum(x[1] for x in lengths)
        >>> total_limit
        702
        >>> intToID(total_limit, '')
        'zz'

    Which tells us that we got 26 IDs of length 1 and up to 676 IDs of length two
    if we need to allocate 512 IDs.

    :param highest: Highest ID that need to be allocated
    :return: An iterator that returns tuples of (id-length, use-limit).  The
     use-limit applies only to the given id-length (i.e. it is excluding IDs
     of shorter length).  Note that the sum of the use-limit values is always
     equal to or greater than the highest param.
    """
    step = 26
    id_length = 0
    use_limit = 1
    while highest:
        id_length += 1
        use_limit *= step
        yield (id_length, use_limit)
        highest = int((highest - 1) / step)


def intToID(idnum, prefix):
    """
    Returns the ID name for the given ID number, spreadsheet-style, i.e. from a to z,
    then from aa to az, ba to bz, etc., until zz.
    """
    rid = ''

    while idnum > 0:
        idnum -= 1
        rid = chr((idnum % 26) + ord('a')) + rid
        idnum = int(idnum / 26)

    return prefix + rid


def renameID(idFrom, idTo, identifiedElements, referringNodes):
    """
    Changes the ID name from idFrom to idTo, on the declaring element
    as well as all nodes in referringNodes.

    Updates identifiedElements.

    Returns the number of bytes saved by this replacement.
    """

    num = 0

    definingNode = identifiedElements[idFrom]
    definingNode.setAttribute("id", idTo)
    num += len(idFrom) - len(idTo)

    # Update references to renamed node
    if referringNodes is not None:

        # Look for the idFrom ID name in each of the referencing elements,
        # exactly like findReferencedElements would.
        # Cyn: Duplicated processing!

        for node in referringNodes:
            # if this node is a style element, parse its text into CSS
            if node.nodeName == 'style' and node.namespaceURI == NS['SVG']:
                # node.firstChild will be either a CDATA or a Text node now
                if node.firstChild is not None:
                    # concatenate the value of all children, in case
                    # there's a CDATASection node surrounded by whitespace
                    # nodes
                    # (node.normalize() will NOT work here, it only acts on Text nodes)
                    oldValue = "".join(child.nodeValue for child in node.childNodes)
                    # not going to reparse the whole thing
                    newValue = oldValue.replace('url(#' + idFrom + ')', 'url(#' + idTo + ')')
                    newValue = newValue.replace("url(#'" + idFrom + "')", 'url(#' + idTo + ')')
                    newValue = newValue.replace('url(#"' + idFrom + '")', 'url(#' + idTo + ')')
                    # and now replace all the children with this new stylesheet.
                    # again, this is in case the stylesheet was a CDATASection
                    node.childNodes[:] = [node.ownerDocument.createTextNode(newValue)]
                    num += len(oldValue) - len(newValue)

            # if xlink:href is set to #idFrom, then change the id
            href = node.getAttributeNS(NS['XLINK'], 'href')
            if href == '#' + idFrom:
                node.setAttributeNS(NS['XLINK'], 'href', '#' + idTo)
                num += len(idFrom) - len(idTo)

            # if the style has url(#idFrom), then change the id
            styles = node.getAttribute('style')
            if styles != '':
                newValue = styles.replace('url(#' + idFrom + ')', 'url(#' + idTo + ')')
                newValue = newValue.replace("url('#" + idFrom + "')", 'url(#' + idTo + ')')
                newValue = newValue.replace('url("#' + idFrom + '")', 'url(#' + idTo + ')')
                node.setAttribute('style', newValue)
                num += len(styles) - len(newValue)

            # now try the fill, stroke, filter attributes
            for attr in referencingProps:
                oldValue = node.getAttribute(attr)
                if oldValue != '':
                    newValue = oldValue.replace('url(#' + idFrom + ')', 'url(#' + idTo + ')')
                    newValue = newValue.replace("url('#" + idFrom + "')", 'url(#' + idTo + ')')
                    newValue = newValue.replace('url("#' + idFrom + '")', 'url(#' + idTo + ')')
                    node.setAttribute(attr, newValue)
                    num += len(oldValue) - len(newValue)

    return num


def protected_ids(seenIDs, options):
    """Return a list of protected IDs out of the seenIDs"""
    protectedIDs = []
    if options.protect_ids_prefix or options.protect_ids_noninkscape or options.protect_ids_list:
        protect_ids_prefixes = []
        protect_ids_list = []
        if options.protect_ids_list:
            protect_ids_list = options.protect_ids_list.split(",")
        if options.protect_ids_prefix:
            protect_ids_prefixes = options.protect_ids_prefix.split(",")
        for id in seenIDs:
            protected = False
            if options.protect_ids_noninkscape and not id[-1].isdigit():
                protected = True
            elif protect_ids_list and id in protect_ids_list:
                protected = True
            elif protect_ids_prefixes:
                if any(id.startswith(prefix) for prefix in protect_ids_prefixes):
                    protected = True
            if protected:
                protectedIDs.append(id)
    return protectedIDs


def unprotected_ids(doc, options):
    u"""Returns a list of unprotected IDs within the document doc."""
    identifiedElements = findElementsWithId(doc.documentElement)
    protectedIDs = protected_ids(identifiedElements, options)
    if protectedIDs:
        for id in protectedIDs:
            del identifiedElements[id]
    return identifiedElements


def removeUnreferencedIDs(referencedIDs, identifiedElements):
    """
    Removes the unreferenced ID attributes.

    Returns the number of ID attributes removed
    """
    global _num_ids_removed
    keepTags = ['font']
    num = 0
    for id in identifiedElements:
        node = identifiedElements[id]
        if id not in referencedIDs and node.nodeName not in keepTags:
            node.removeAttribute('id')
            _num_ids_removed += 1
            num += 1
    return num


def removeNamespacedAttributes(node, namespaces):
    num = 0
    if node.nodeType == Node.ELEMENT_NODE:
        # remove all namespace'd attributes from this element
        attrList = node.attributes
        attrsToRemove = []
        for attrNum in range(attrList.length):
            attr = attrList.item(attrNum)
            if attr is not None and attr.namespaceURI in namespaces:
                attrsToRemove.append(attr.nodeName)
        for attrName in attrsToRemove:
            node.removeAttribute(attrName)
        num += len(attrsToRemove)

        # now recurse for children
        for child in node.childNodes:
            num += removeNamespacedAttributes(child, namespaces)
    return num


def removeNamespacedElements(node, namespaces):
    num = 0
    if node.nodeType == Node.ELEMENT_NODE:
        # remove all namespace'd child nodes from this element
        childList = node.childNodes
        childrenToRemove = []
        for child in childList:
            if child is not None and child.namespaceURI in namespaces:
                childrenToRemove.append(child)
        for child in childrenToRemove:
            node.removeChild(child)
        num += len(childrenToRemove)

        # now recurse for children
        for child in node.childNodes:
            num += removeNamespacedElements(child, namespaces)
    return num


def removeDescriptiveElements(doc, options):
    elementTypes = []
    if options.remove_descriptive_elements:
        elementTypes.extend(("title", "desc", "metadata"))
    else:
        if options.remove_titles:
            elementTypes.append("title")
        if options.remove_descriptions:
            elementTypes.append("desc")
        if options.remove_metadata:
            elementTypes.append("metadata")
    if not elementTypes:
        return

    global _num_elements_removed
    num = 0
    elementsToRemove = []
    for elementType in elementTypes:
        elementsToRemove.extend(doc.documentElement.getElementsByTagName(elementType))

    for element in elementsToRemove:
        element.parentNode.removeChild(element)
        num += 1
        _num_elements_removed += 1

    return num


def removeNestedGroups(node):
    """
    This walks further and further down the tree, removing groups
    which do not have any attributes or a title/desc child and
    promoting their children up one level
    """
    global _num_elements_removed
    num = 0

    groupsToRemove = []
    # Only consider <g> elements for promotion if this element isn't a <switch>.
    # (partial fix for bug 594930, required by the SVG spec however)
    if not (node.nodeType == Node.ELEMENT_NODE and node.nodeName == 'switch'):
        for child in node.childNodes:
            if child.nodeName == 'g' and child.namespaceURI == NS['SVG'] and len(child.attributes) == 0:
                # only collapse group if it does not have a title or desc as a direct descendant,
                for grandchild in child.childNodes:
                    if grandchild.nodeType == Node.ELEMENT_NODE and grandchild.namespaceURI == NS['SVG'] and \
                            grandchild.nodeName in ['title', 'desc']:
                        break
                else:
                    groupsToRemove.append(child)

    for g in groupsToRemove:
        while g.childNodes.length > 0:
            g.parentNode.insertBefore(g.firstChild, g)
        g.parentNode.removeChild(g)
        _num_elements_removed += 1
        num += 1

    # now recurse for children
    for child in node.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            num += removeNestedGroups(child)
    return num


def moveCommonAttributesToParentGroup(elem, referencedElements):
    """
    This recursively calls this function on all children of the passed in element
    and then iterates over all child elements and removes common inheritable attributes
    from the children and places them in the parent group.  But only if the parent contains
    nothing but element children and whitespace.  The attributes are only removed from the
    children if the children are not referenced by other elements in the document.
    """
    num = 0

    childElements = []
    # recurse first into the children (depth-first)
    for child in elem.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            # only add and recurse if the child is not referenced elsewhere
            if not child.getAttribute('id') in referencedElements:
                childElements.append(child)
                num += moveCommonAttributesToParentGroup(child, referencedElements)
        # else if the parent has non-whitespace text children, do not
        # try to move common attributes
        elif child.nodeType == Node.TEXT_NODE and child.nodeValue.strip():
            return num

    # only process the children if there are more than one element
    if len(childElements) <= 1:
        return num

    commonAttrs = {}
    # add all inheritable properties of the first child element
    # FIXME: Note there is a chance that the first child is a set/animate in which case
    # its fill attribute is not what we want to look at, we should look for the first
    # non-animate/set element
    attrList = childElements[0].attributes
    for index in range(attrList.length):
        attr = attrList.item(index)
        # this is most of the inheritable properties from http://www.w3.org/TR/SVG11/propidx.html
        # and http://www.w3.org/TR/SVGTiny12/attributeTable.html
        if attr.nodeName in ['clip-rule',
                             'display-align',
                             'fill', 'fill-opacity', 'fill-rule',
                             'font', 'font-family', 'font-size', 'font-size-adjust', 'font-stretch',
                             'font-style', 'font-variant', 'font-weight',
                             'letter-spacing',
                             'pointer-events', 'shape-rendering',
                             'stroke', 'stroke-dasharray', 'stroke-dashoffset', 'stroke-linecap', 'stroke-linejoin',
                             'stroke-miterlimit', 'stroke-opacity', 'stroke-width',
                             'text-anchor', 'text-decoration', 'text-rendering', 'visibility',
                             'word-spacing', 'writing-mode']:
            # we just add all the attributes from the first child
            commonAttrs[attr.nodeName] = attr.nodeValue

    # for each subsequent child element
    for childNum in range(len(childElements)):
        # skip first child
        if childNum == 0:
            continue

        child = childElements[childNum]
        # if we are on an animateXXX/set element, ignore it (due to the 'fill' attribute)
        if child.localName in ['set', 'animate', 'animateColor', 'animateTransform', 'animateMotion']:
            continue

        distinctAttrs = []
        # loop through all current 'common' attributes
        for name in commonAttrs:
            # if this child doesn't match that attribute, schedule it for removal
            if child.getAttribute(name) != commonAttrs[name]:
                distinctAttrs.append(name)
        # remove those attributes which are not common
        for name in distinctAttrs:
            del commonAttrs[name]

    # commonAttrs now has all the inheritable attributes which are common among all child elements
    for name in commonAttrs:
        for child in childElements:
            child.removeAttribute(name)
        elem.setAttribute(name, commonAttrs[name])

    # update our statistic (we remove N*M attributes and add back in M attributes)
    num += (len(childElements) - 1) * len(commonAttrs)
    return num


def mergeSiblingGroupsWithCommonAttributes(elem):
    """
    Merge two or more sibling <g> elements with the identical attributes.

    This function acts recursively on the given element.
    """

    num = 0
    i = elem.childNodes.length - 1
    while i >= 0:
        currentNode = elem.childNodes.item(i)
        if currentNode.nodeType != Node.ELEMENT_NODE or currentNode.nodeName != 'g' or \
           currentNode.namespaceURI != NS['SVG']:
            i -= 1
            continue
        attributes = {a.nodeName: a.nodeValue for a in currentNode.attributes.values()}
        if not attributes:
            i -= 1
            continue
        runStart, runEnd = i, i
        runElements = 1
        while runStart > 0:
            nextNode = elem.childNodes.item(runStart - 1)
            if nextNode.nodeType == Node.ELEMENT_NODE:
                if nextNode.nodeName != 'g' or nextNode.namespaceURI != NS['SVG']:
                    break
                nextAttributes = {a.nodeName: a.nodeValue for a in nextNode.attributes.values()}
                hasNoMergeTags = (True for n in nextNode.childNodes
                                  if n.nodeType == Node.ELEMENT_NODE
                                  and n.nodeName in ('title', 'desc')
                                  and n.namespaceURI == NS['SVG'])
                if attributes != nextAttributes or any(hasNoMergeTags):
                    break
                else:
                    runElements += 1
                    runStart -= 1
            else:
                runStart -= 1

        # Next loop will start from here
        i = runStart - 1

        if runElements < 2:
            continue

        # Find the <g> entry that starts the run (we might have run
        # past it into a text node or a comment node.
        while True:
            node = elem.childNodes.item(runStart)
            if node.nodeType == Node.ELEMENT_NODE and node.nodeName == 'g' and node.namespaceURI == NS['SVG']:
                break
            runStart += 1
        primaryGroup = elem.childNodes.item(runStart)
        runStart += 1
        nodes = elem.childNodes[runStart:runEnd+1]
        for node in nodes:
            if node.nodeType == Node.ELEMENT_NODE and node.nodeName == 'g' and node.namespaceURI == NS['SVG']:
                # Merge
                for child in node.childNodes[:]:
                    primaryGroup.appendChild(child)
                elem.removeChild(node).unlink()
            else:
                primaryGroup.appendChild(node)

    # each child gets the same treatment, recursively
    for childNode in elem.childNodes:
        if childNode.nodeType == Node.ELEMENT_NODE:
            num += mergeSiblingGroupsWithCommonAttributes(childNode)

    return num


def createGroupsForCommonAttributes(elem):
    """
    Creates <g> elements to contain runs of 3 or more
    consecutive child elements having at least one common attribute.

    Common attributes are not promoted to the <g> by this function.
    This is handled by moveCommonAttributesToParentGroup.

    If all children have a common attribute, an extra <g> is not created.

    This function acts recursively on the given element.
    """
    num = 0
    global _num_elements_removed

    # TODO perhaps all of the Presentation attributes in http://www.w3.org/TR/SVG/struct.html#GElement
    # could be added here
    # Cyn: These attributes are the same as in moveAttributesToParentGroup, and must always be
    for curAttr in ['clip-rule',
                    'display-align',
                    'fill', 'fill-opacity', 'fill-rule',
                    'font', 'font-family', 'font-size', 'font-size-adjust', 'font-stretch',
                    'font-style', 'font-variant', 'font-weight',
                    'letter-spacing',
                    'pointer-events', 'shape-rendering',
                    'stroke', 'stroke-dasharray', 'stroke-dashoffset', 'stroke-linecap', 'stroke-linejoin',
                    'stroke-miterlimit', 'stroke-opacity', 'stroke-width',
                    'text-anchor', 'text-decoration', 'text-rendering', 'visibility',
                    'word-spacing', 'writing-mode']:
        # Iterate through the children in reverse order, so item(i) for
        # items we have yet to visit still returns the correct nodes.
        curChild = elem.childNodes.length - 1
        while curChild >= 0:
            childNode = elem.childNodes.item(curChild)

            if (
                childNode.nodeType == Node.ELEMENT_NODE and
                childNode.getAttribute(curAttr) != '' and
                childNode.nodeName in [
                    # only attempt to group elements that the content model allows to be children of a <g>

                    # SVG 1.1 (see https://www.w3.org/TR/SVG/struct.html#GElement)
                    'animate', 'animateColor', 'animateMotion', 'animateTransform', 'set',  # animation elements
                    'desc', 'metadata', 'title',                                            # descriptive elements
                    'circle', 'ellipse', 'line', 'path', 'polygon', 'polyline', 'rect',     # shape elements
                    'defs', 'g', 'svg', 'symbol', 'use',                                    # structural elements
                    'linearGradient', 'radialGradient',                                     # gradient elements
                    'a', 'altGlyphDef', 'clipPath', 'color-profile', 'cursor', 'filter',
                    'font', 'font-face', 'foreignObject', 'image', 'marker', 'mask',
                    'pattern', 'script', 'style', 'switch', 'text', 'view',

                    # SVG 1.2 (see https://www.w3.org/TR/SVGTiny12/elementTable.html)
                    'animation', 'audio', 'discard', 'handler', 'listener',
                    'prefetch', 'solidColor', 'textArea', 'video'
                ]
            ):
                # We're in a possible run! Track the value and run length.
                value = childNode.getAttribute(curAttr)
                runStart, runEnd = curChild, curChild
                # Run elements includes only element tags, no whitespace/comments/etc.
                # Later, we calculate a run length which includes these.
                runElements = 1

                # Backtrack to get all the nodes having the same
                # attribute value, preserving any nodes in-between.
                while runStart > 0:
                    nextNode = elem.childNodes.item(runStart - 1)
                    if nextNode.nodeType == Node.ELEMENT_NODE:
                        if nextNode.getAttribute(curAttr) != value:
                            break
                        else:
                            runElements += 1
                            runStart -= 1
                    else:
                        runStart -= 1

                if runElements >= 3:
                    # Include whitespace/comment/etc. nodes in the run.
                    while runEnd < elem.childNodes.length - 1:
                        if elem.childNodes.item(runEnd + 1).nodeType == Node.ELEMENT_NODE:
                            break
                        else:
                            runEnd += 1

                    runLength = runEnd - runStart + 1
                    if runLength == elem.childNodes.length:  # Every child has this
                        # If the current parent is a <g> already,
                        if elem.nodeName == 'g' and elem.namespaceURI == NS['SVG']:
                            # do not act altogether on this attribute; all the
                            # children have it in common.
                            # Let moveCommonAttributesToParentGroup do it.
                            curChild = -1
                            continue
                        # otherwise, it might be an <svg> element, and
                        # even if all children have the same attribute value,
                        # it's going to be worth making the <g> since
                        # <svg> doesn't support attributes like 'stroke'.
                        # Fall through.

                    # Create a <g> element from scratch.
                    # We need the Document for this.
                    document = elem.ownerDocument
                    group = document.createElementNS(NS['SVG'], 'g')
                    # Move the run of elements to the group.
                    # a) ADD the nodes to the new group.
                    group.childNodes[:] = elem.childNodes[runStart:runEnd + 1]
                    for child in group.childNodes:
                        child.parentNode = group
                    # b) REMOVE the nodes from the element.
                    elem.childNodes[runStart:runEnd + 1] = []
                    # Include the group in elem's children.
                    elem.childNodes.insert(runStart, group)
                    group.parentNode = elem
                    num += 1
                    curChild = runStart - 1
                    _num_elements_removed -= 1
                else:
                    curChild -= 1
            else:
                curChild -= 1

    # each child gets the same treatment, recursively
    for childNode in elem.childNodes:
        if childNode.nodeType == Node.ELEMENT_NODE:
            num += createGroupsForCommonAttributes(childNode)

    return num


def removeUnusedAttributesOnParent(elem):
    """
    This recursively calls this function on all children of the element passed in,
    then removes any unused attributes on this elem if none of the children inherit it
    """
    num = 0

    childElements = []
    # recurse first into the children (depth-first)
    for child in elem.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            childElements.append(child)
            num += removeUnusedAttributesOnParent(child)

    # only process the children if there are more than one element
    if len(childElements) <= 1:
        return num

    # get all attribute values on this parent
    attrList = elem.attributes
    unusedAttrs = {}
    for index in range(attrList.length):
        attr = attrList.item(index)
        if attr.nodeName in ['clip-rule',
                             'display-align',
                             'fill', 'fill-opacity', 'fill-rule',
                             'font', 'font-family', 'font-size', 'font-size-adjust', 'font-stretch',
                             'font-style', 'font-variant', 'font-weight',
                             'letter-spacing',
                             'pointer-events', 'shape-rendering',
                             'stroke', 'stroke-dasharray', 'stroke-dashoffset', 'stroke-linecap', 'stroke-linejoin',
                             'stroke-miterlimit', 'stroke-opacity', 'stroke-width',
                             'text-anchor', 'text-decoration', 'text-rendering', 'visibility',
                             'word-spacing', 'writing-mode']:
            unusedAttrs[attr.nodeName] = attr.nodeValue

    # for each child, if at least one child inherits the parent's attribute, then remove
    for childNum in range(len(childElements)):
        child = childElements[childNum]
        inheritedAttrs = []
        for name in unusedAttrs:
            val = child.getAttribute(name)
            if val == '' or val is None or val == 'inherit':
                inheritedAttrs.append(name)
        for a in inheritedAttrs:
            del unusedAttrs[a]

    # unusedAttrs now has all the parent attributes that are unused
    for name in unusedAttrs:
        elem.removeAttribute(name)
        num += 1

    return num


def removeDuplicateGradientStops(doc):
    global _num_elements_removed
    num = 0

    for gradType in ['linearGradient', 'radialGradient']:
        for grad in doc.getElementsByTagName(gradType):
            stops = {}
            stopsToRemove = []
            for stop in grad.getElementsByTagName('stop'):
                # convert percentages into a floating point number
                offsetU = SVGLength(stop.getAttribute('offset'))
                if offsetU.units == Unit.PCT:
                    offset = offsetU.value / 100.0
                elif offsetU.units == Unit.NONE:
                    offset = offsetU.value
                else:
                    offset = 0
                # set the stop offset value to the integer or floating point equivalent
                if int(offset) == offset:
                    stop.setAttribute('offset', str(int(offset)))
                else:
                    stop.setAttribute('offset', str(offset))

                color = stop.getAttribute('stop-color')
                opacity = stop.getAttribute('stop-opacity')
                style = stop.getAttribute('style')
                if offset in stops:
                    oldStop = stops[offset]
                    if oldStop[0] == color and oldStop[1] == opacity and oldStop[2] == style:
                        stopsToRemove.append(stop)
                stops[offset] = [color, opacity, style]

            for stop in stopsToRemove:
                stop.parentNode.removeChild(stop)
                num += 1
                _num_elements_removed += 1

    # linear gradients
    return num


def collapseSinglyReferencedGradients(doc):
    global _num_elements_removed
    num = 0

    identifiedElements = findElementsWithId(doc.documentElement)

    # make sure to reset the ref'ed ids for when we are running this in testscour
    for rid, nodes in six.iteritems(findReferencedElements(doc.documentElement)):
        # Make sure that there's actually a defining element for the current ID name.
        # (Cyn: I've seen documents with #id references but no element with that ID!)
        if len(nodes) == 1 and rid in identifiedElements:
            elem = identifiedElements[rid]
            if (
                elem is not None and
                elem.nodeType == Node.ELEMENT_NODE and
                elem.nodeName in ['linearGradient', 'radialGradient'] and
                elem.namespaceURI == NS['SVG']
            ):
                # found a gradient that is referenced by only 1 other element
                refElem = nodes.pop()
                if refElem.nodeType == Node.ELEMENT_NODE and refElem.nodeName in ['linearGradient', 'radialGradient'] \
                        and refElem.namespaceURI == NS['SVG']:
                    # elem is a gradient referenced by only one other gradient (refElem)

                    # add the stops to the referencing gradient (this removes them from elem)
                    if len(refElem.getElementsByTagName('stop')) == 0:
                        stopsToAdd = elem.getElementsByTagName('stop')
                        for stop in stopsToAdd:
                            refElem.appendChild(stop)

                    # adopt the gradientUnits, spreadMethod,  gradientTransform attributes if
                    # they are unspecified on refElem
                    for attr in ['gradientUnits', 'spreadMethod', 'gradientTransform']:
                        if refElem.getAttribute(attr) == '' and not elem.getAttribute(attr) == '':
                            refElem.setAttributeNS(None, attr, elem.getAttribute(attr))

                    # if both are radialGradients, adopt elem's fx,fy,cx,cy,r attributes if
                    # they are unspecified on refElem
                    if elem.nodeName == 'radialGradient' and refElem.nodeName == 'radialGradient':
                        for attr in ['fx', 'fy', 'cx', 'cy', 'r']:
                            if refElem.getAttribute(attr) == '' and not elem.getAttribute(attr) == '':
                                refElem.setAttributeNS(None, attr, elem.getAttribute(attr))

                    # if both are linearGradients, adopt elem's x1,y1,x2,y2 attributes if
                    # they are unspecified on refElem
                    if elem.nodeName == 'linearGradient' and refElem.nodeName == 'linearGradient':
                        for attr in ['x1', 'y1', 'x2', 'y2']:
                            if refElem.getAttribute(attr) == '' and not elem.getAttribute(attr) == '':
                                refElem.setAttributeNS(None, attr, elem.getAttribute(attr))

                    target_href = elem.getAttributeNS(NS['XLINK'], 'href')
                    if target_href:
                        # If the elem node had an xlink:href, then the
                        # refElem have to point to it as well to
                        # perserve the semantics of the image.
                        refElem.setAttributeNS(NS['XLINK'], 'href', target_href)
                    else:
                        # The elem node had no xlink:href reference,
                        # so we can simply remove the attribute.
                        refElem.removeAttributeNS(NS['XLINK'], 'href')

                    # now delete elem
                    elem.parentNode.removeChild(elem)
                    _num_elements_removed += 1
                    num += 1
    return num


def computeGradientBucketKey(grad):
    # Compute a key (hashable opaque value; here a string) from each
    # gradient such that "key(grad1) == key(grad2)" is the same as
    # saying that grad1 is a duplicate of grad2.
    gradBucketAttr = ['gradientUnits', 'spreadMethod', 'gradientTransform',
                      'x1', 'y1', 'x2', 'y2', 'cx', 'cy', 'fx', 'fy', 'r']
    gradStopBucketsAttr = ['offset', 'stop-color', 'stop-opacity', 'style']

    # A linearGradient can never be a duplicate of a
    # radialGradient (and vice versa)
    subKeys = [grad.getAttribute(a) for a in gradBucketAttr]
    subKeys.append(grad.getAttributeNS(NS['XLINK'], 'href'))
    stops = grad.getElementsByTagName('stop')
    if stops.length:
        for i in range(stops.length):
            stop = stops.item(i)
            for attr in gradStopBucketsAttr:
                stopKey = stop.getAttribute(attr)
                subKeys.append(stopKey)

    # Use a raw ASCII "record separator" control character as it is
    # not likely to be used in any of these values (without having to
    # be escaped).
    return "\x1e".join(subKeys)


def detect_duplicate_gradients(*grad_lists):
    """Detects duplicate gradients from each iterable/generator given as argument

    Yields (master, master_id, duplicates_id, duplicates) tuples where:
      * master_id: The ID attribute of the master element.  This will always be non-empty
        and not None as long at least one of the gradients have a valid ID.
      * duplicates_id: List of ID attributes of the duplicate gradients elements (can be
        empty where the gradient had no ID attribute)
      * duplicates: List of elements that are duplicates of the `master` element.  Will
        never include the `master` element.  Has the same order as `duplicates_id` - i.e.
        `duplicates[X].getAttribute("id") == duplicates_id[X]`.
    """
    for grads in grad_lists:
        grad_buckets = defaultdict(list)

        for grad in grads:
            key = computeGradientBucketKey(grad)
            grad_buckets[key].append(grad)

        for bucket in six.itervalues(grad_buckets):
            if len(bucket) < 2:
                # The gradient must be unique if it is the only one in
                # this bucket.
                continue
            master = bucket[0]
            duplicates = bucket[1:]
            duplicates_ids = [d.getAttribute('id') for d in duplicates]
            master_id = master.getAttribute('id')
            if not master_id:
                # If our selected "master" copy does not have an ID,
                # then replace it with one that does (assuming any of
                # them has one).  This avoids broken images like we
                # saw in GH#203
                for i in range(len(duplicates_ids)):
                    dup_id = duplicates_ids[i]
                    if dup_id:
                        # We do not bother updating the master field
                        # as it is not used any more.
                        master_id = duplicates_ids[i]
                        duplicates[i] = master
                        # Clear the old id to avoid a redundant remapping
                        duplicates_ids[i] = ""
                        break

            yield master_id, duplicates_ids, duplicates


def dedup_gradient(master_id, duplicates_ids, duplicates, referenced_ids):
    func_iri = None
    for dup_id, dup_grad in zip(duplicates_ids, duplicates):
        # if the duplicate gradient no longer has a parent that means it was
        # already re-mapped to another master gradient
        if not dup_grad.parentNode:
            continue

        # With --keep-unreferenced-defs, we can end up with
        # unreferenced gradients.  See GH#156.
        if dup_id in referenced_ids:
            if func_iri is None:
                # matches url(#<ANY_DUP_ID>), url('#<ANY_DUP_ID>') and url("#<ANY_DUP_ID>")
                dup_id_regex = "|".join(duplicates_ids)
                func_iri = re.compile('url\\([\'"]?#(?:' + dup_id_regex + ')[\'"]?\\)')
            for elem in referenced_ids[dup_id]:
                # find out which attribute referenced the duplicate gradient
                for attr in ['fill', 'stroke']:
                    v = elem.getAttribute(attr)
                    (v_new, n) = func_iri.subn('url(#' + master_id + ')', v)
                    if n > 0:
                        elem.setAttribute(attr, v_new)
                if elem.getAttributeNS(NS['XLINK'], 'href') == '#' + dup_id:
                    elem.setAttributeNS(NS['XLINK'], 'href', '#' + master_id)
                styles = _getStyle(elem)
                for style in styles:
                    v = styles[style]
                    (v_new, n) = func_iri.subn('url(#' + master_id + ')', v)
                    if n > 0:
                        styles[style] = v_new
                _setStyle(elem, styles)

        # now that all referencing elements have been re-mapped to the master
        # it is safe to remove this gradient from the document
        dup_grad.parentNode.removeChild(dup_grad)

    # If the gradients have an ID, we update referenced_ids to match the newly remapped IDs.
    # This enable us to avoid calling findReferencedElements once per loop, which is helpful as it is
    # one of the slowest functions in scour.
    if master_id:
        try:
            master_references = referenced_ids[master_id]
        except KeyError:
            master_references = set()

        for dup_id in duplicates_ids:
            references = referenced_ids.pop(dup_id, None)
            if references is None:
                continue
            master_references.update(references)

        # Only necessary but needed if the master gradient did
        # not have any references originally
        referenced_ids[master_id] = master_references


def removeDuplicateGradients(doc):
    prev_num = -1
    num = 0

    # get a collection of all elements that are referenced and their referencing elements
    referenced_ids = findReferencedElements(doc.documentElement)

    while prev_num != num:
        prev_num = num

        linear_gradients = doc.getElementsByTagName('linearGradient')
        radial_gradients = doc.getElementsByTagName('radialGradient')

        for master_id, duplicates_ids, duplicates in detect_duplicate_gradients(linear_gradients, radial_gradients):
            dedup_gradient(master_id, duplicates_ids, duplicates, referenced_ids)
            num += len(duplicates)

    return num


def _getStyle(node):
    u"""Returns the style attribute of a node as a dictionary."""
    if node.nodeType != Node.ELEMENT_NODE:
        return {}
    style_attribute = node.getAttribute('style')
    if style_attribute:
        styleMap = {}
        rawStyles = style_attribute.split(';')
        for style in rawStyles:
            propval = style.split(':')
            if len(propval) == 2:
                styleMap[propval[0].strip()] = propval[1].strip()
        return styleMap
    else:
        return {}


def _setStyle(node, styleMap):
    u"""Sets the style attribute of a node to the dictionary ``styleMap``."""
    fixedStyle = ';'.join(prop + ':' + styleMap[prop] for prop in styleMap)
    if fixedStyle != '':
        node.setAttribute('style', fixedStyle)
    elif node.getAttribute('style'):
        node.removeAttribute('style')
    return node


def repairStyle(node, options):
    num = 0
    styleMap = _getStyle(node)
    if styleMap:

        # I've seen this enough to know that I need to correct it:
        # fill: url(#linearGradient4918) rgb(0, 0, 0);
        for prop in ['fill', 'stroke']:
            if prop in styleMap:
                chunk = styleMap[prop].split(') ')
                if (len(chunk) == 2
                        and (chunk[0][:5] == 'url(#' or chunk[0][:6] == 'url("#' or chunk[0][:6] == "url('#")
                        and chunk[1] == 'rgb(0, 0, 0)'):
                    styleMap[prop] = chunk[0] + ')'
                    num += 1

        # Here is where we can weed out unnecessary styles like:
        #  opacity:1
        if 'opacity' in styleMap:
            opacity = float(styleMap['opacity'])
            # if opacity='0' then all fill and stroke properties are useless, remove them
            if opacity == 0.0:
                for uselessStyle in ['fill', 'fill-opacity', 'fill-rule', 'stroke', 'stroke-linejoin',
                                     'stroke-opacity', 'stroke-miterlimit', 'stroke-linecap', 'stroke-dasharray',
                                     'stroke-dashoffset', 'stroke-opacity']:
                    if uselessStyle in styleMap and not styleInheritedByChild(node, uselessStyle):
                        del styleMap[uselessStyle]
                        num += 1

        #  if stroke:none, then remove all stroke-related properties (stroke-width, etc)
        #  TODO: should also detect if the computed value of this element is stroke="none"
        if 'stroke' in styleMap and styleMap['stroke'] == 'none':
            for strokestyle in ['stroke-width', 'stroke-linejoin', 'stroke-miterlimit',
                                'stroke-linecap', 'stroke-dasharray', 'stroke-dashoffset', 'stroke-opacity']:
                if strokestyle in styleMap and not styleInheritedByChild(node, strokestyle):
                    del styleMap[strokestyle]
                    num += 1
            # we need to properly calculate computed values
            if not styleInheritedByChild(node, 'stroke'):
                if styleInheritedFromParent(node, 'stroke') in [None, 'none']:
                    del styleMap['stroke']
                    num += 1

        #  if fill:none, then remove all fill-related properties (fill-rule, etc)
        if 'fill' in styleMap and styleMap['fill'] == 'none':
            for fillstyle in ['fill-rule', 'fill-opacity']:
                if fillstyle in styleMap and not styleInheritedByChild(node, fillstyle):
                    del styleMap[fillstyle]
                    num += 1

        #  fill-opacity: 0
        if 'fill-opacity' in styleMap:
            fillOpacity = float(styleMap['fill-opacity'])
            if fillOpacity == 0.0:
                for uselessFillStyle in ['fill', 'fill-rule']:
                    if uselessFillStyle in styleMap and not styleInheritedByChild(node, uselessFillStyle):
                        del styleMap[uselessFillStyle]
                        num += 1

        #  stroke-opacity: 0
        if 'stroke-opacity' in styleMap:
            strokeOpacity = float(styleMap['stroke-opacity'])
            if strokeOpacity == 0.0:
                for uselessStrokeStyle in ['stroke', 'stroke-width', 'stroke-linejoin', 'stroke-linecap',
                                           'stroke-dasharray', 'stroke-dashoffset']:
                    if uselessStrokeStyle in styleMap and not styleInheritedByChild(node, uselessStrokeStyle):
                        del styleMap[uselessStrokeStyle]
                        num += 1

        # stroke-width: 0
        if 'stroke-width' in styleMap:
            strokeWidth = SVGLength(styleMap['stroke-width'])
            if strokeWidth.value == 0.0:
                for uselessStrokeStyle in ['stroke', 'stroke-linejoin', 'stroke-linecap',
                                           'stroke-dasharray', 'stroke-dashoffset', 'stroke-opacity']:
                    if uselessStrokeStyle in styleMap and not styleInheritedByChild(node, uselessStrokeStyle):
                        del styleMap[uselessStrokeStyle]
                        num += 1

        # remove font properties for non-text elements
        # I've actually observed this in real SVG content
        if not mayContainTextNodes(node):
            for fontstyle in ['font-family', 'font-size', 'font-stretch', 'font-size-adjust',
                              'font-style', 'font-variant', 'font-weight',
                              'letter-spacing', 'line-height', 'kerning',
                              'text-align', 'text-anchor', 'text-decoration',
                              'text-rendering', 'unicode-bidi',
                              'word-spacing', 'writing-mode']:
                if fontstyle in styleMap:
                    del styleMap[fontstyle]
                    num += 1

        # remove inkscape-specific styles
        # TODO: need to get a full list of these
        for inkscapeStyle in ['-inkscape-font-specification']:
            if inkscapeStyle in styleMap:
                del styleMap[inkscapeStyle]
                num += 1

        if 'overflow' in styleMap:
            # remove overflow from elements to which it does not apply,
            # see https://www.w3.org/TR/SVG/masking.html#OverflowProperty
            if node.nodeName not in ['svg', 'symbol', 'image', 'foreignObject', 'marker', 'pattern']:
                del styleMap['overflow']
                num += 1
            # if the node is not the root <svg> element the SVG's user agent style sheet
            # overrides the initial (i.e. default) value with the value 'hidden', which can consequently be removed
            # (see last bullet point in the link above)
            elif node != node.ownerDocument.documentElement:
                if styleMap['overflow'] == 'hidden':
                    del styleMap['overflow']
                    num += 1
            # on the root <svg> element the CSS2 default overflow="visible" is the initial value and we can remove it
            elif styleMap['overflow'] == 'visible':
                del styleMap['overflow']
                num += 1

        # now if any of the properties match known SVG attributes we prefer attributes
        # over style so emit them and remove them from the style map
        if options.style_to_xml:
            for propName in list(styleMap):
                if propName in svgAttributes:
                    node.setAttribute(propName, styleMap[propName])
                    del styleMap[propName]

        _setStyle(node, styleMap)

    # recurse for our child elements
    for child in node.childNodes:
        num += repairStyle(child, options)

    return num


def styleInheritedFromParent(node, style):
    """
    Returns the value of 'style' that is inherited from the parents of the passed-in node

    Warning: This method only considers presentation attributes and inline styles,
             any style sheets are ignored!
    """
    parentNode = node.parentNode

    # return None if we reached the Document element
    if parentNode.nodeType == Node.DOCUMENT_NODE:
        return None

    # check styles first (they take precedence over presentation attributes)
    styles = _getStyle(parentNode)
    if style in styles:
        value = styles[style]
        if not value == 'inherit':
            return value

    # check attributes
    value = parentNode.getAttribute(style)
    if value not in ['', 'inherit']:
        return parentNode.getAttribute(style)

    # check the next parent recursively if we did not find a value yet
    return styleInheritedFromParent(parentNode, style)


def styleInheritedByChild(node, style, nodeIsChild=False):
    """
    Returns whether 'style' is inherited by any children of the passed-in node

    If False is returned, it is guaranteed that 'style' can safely be removed
    from the passed-in node without influencing visual output of it's children

    If True is returned, the passed-in node should not have its text-based
    attributes removed.

    Warning: This method only considers presentation attributes and inline styles,
             any style sheets are ignored!
    """
    # Comment, text and CDATA nodes don't have attributes and aren't containers so they can't inherit attributes
    if node.nodeType != Node.ELEMENT_NODE:
        return False

    if nodeIsChild:
        # if the current child node sets a new value for 'style'
        # we can stop the search in the current branch of the DOM tree

        # check attributes
        if node.getAttribute(style) not in ['', 'inherit']:
            return False
        # check styles
        styles = _getStyle(node)
        if (style in styles) and not (styles[style] == 'inherit'):
            return False
    else:
        # if the passed-in node does not have any children 'style' can obviously not be inherited
        if not node.childNodes:
            return False

    # If we have child nodes recursively check those
    if node.childNodes:
        for child in node.childNodes:
            if styleInheritedByChild(child, style, True):
                return True

    # If the current element is a container element the inherited style is meaningless
    # (since we made sure it's not inherited by any of its children)
    if node.nodeName in ['a', 'defs', 'glyph', 'g', 'marker', 'mask',
                         'missing-glyph', 'pattern', 'svg', 'switch', 'symbol']:
        return False

    # in all other cases we have to assume the inherited value of 'style' is meaningfull and has to be kept
    # (e.g nodes without children at the end of the DOM tree, text nodes, ...)
    return True


def mayContainTextNodes(node):
    """
    Returns True if the passed-in node is probably a text element, or at least
    one of its descendants is probably a text element.

    If False is returned, it is guaranteed that the passed-in node has no
    business having text-based attributes.

    If True is returned, the passed-in node should not have its text-based
    attributes removed.
    """
    # Cached result of a prior call?
    try:
        return node.mayContainTextNodes
    except AttributeError:
        pass

    result = True  # Default value
    # Comment, text and CDATA nodes don't have attributes and aren't containers
    if node.nodeType != Node.ELEMENT_NODE:
        result = False
    # Non-SVG elements? Unknown elements!
    elif node.namespaceURI != NS['SVG']:
        result = True
    # Blacklisted elements. Those are guaranteed not to be text elements.
    elif node.nodeName in ['rect', 'circle', 'ellipse', 'line', 'polygon',
                           'polyline', 'path', 'image', 'stop']:
        result = False
    # Group elements. If we're missing any here, the default of True is used.
    elif node.nodeName in ['g', 'clipPath', 'marker', 'mask', 'pattern',
                           'linearGradient', 'radialGradient', 'symbol']:
        result = False
        for child in node.childNodes:
            if mayContainTextNodes(child):
                result = True
    # Everything else should be considered a future SVG-version text element
    # at best, or an unknown element at worst. result will stay True.

    # Cache this result before returning it.
    node.mayContainTextNodes = result
    return result


# A list of default attributes that are safe to remove if all conditions are fulfilled
#
# Each default attribute is an object of type 'DefaultAttribute' with the following fields:
#     name       - name of the attribute to be matched
#     value      - default value of the attribute
#     units      - the unit(s) for which 'value' is valid (see 'Unit' class for possible specifications)
#     elements   - name(s) of SVG element(s) for which the attribute specification is valid
#     conditions - additional conditions that have to be fulfilled for removal of the specified default attribute
#                  implemented as lambda functions with one argument (an xml.dom.minidom node)
#                  evaluating to either True or False
# When not specifying a field value, it will be ignored (i.e. always matches)
#
# Sources for this list:
#     https://www.w3.org/TR/SVG/attindex.html             (mostly implemented)
#     https://www.w3.org/TR/SVGTiny12/attributeTable.html (not yet implemented)
#     https://www.w3.org/TR/SVG2/attindex.html            (not yet implemented)
#
DefaultAttribute = namedtuple('DefaultAttribute', ['name', 'value', 'units', 'elements', 'conditions'])
DefaultAttribute.__new__.__defaults__ = (None,) * len(DefaultAttribute._fields)
default_attributes = [
    # unit systems
    DefaultAttribute('clipPathUnits', 'userSpaceOnUse', elements=['clipPath']),
    DefaultAttribute('filterUnits', 'objectBoundingBox', elements=['filter']),
    DefaultAttribute('gradientUnits', 'objectBoundingBox', elements=['linearGradient', 'radialGradient']),
    DefaultAttribute('maskUnits', 'objectBoundingBox', elements=['mask']),
    DefaultAttribute('maskContentUnits', 'userSpaceOnUse', elements=['mask']),
    DefaultAttribute('patternUnits', 'objectBoundingBox', elements=['pattern']),
    DefaultAttribute('patternContentUnits', 'userSpaceOnUse', elements=['pattern']),
    DefaultAttribute('primitiveUnits', 'userSpaceOnUse', elements=['filter']),

    DefaultAttribute('externalResourcesRequired', 'false',
                     elements=['a', 'altGlyph', 'animate', 'animateColor',
                               'animateMotion', 'animateTransform', 'circle', 'clipPath', 'cursor', 'defs', 'ellipse',
                               'feImage', 'filter', 'font', 'foreignObject', 'g', 'image', 'line', 'linearGradient',
                               'marker', 'mask', 'mpath', 'path', 'pattern', 'polygon', 'polyline', 'radialGradient',
                               'rect', 'script', 'set', 'svg', 'switch', 'symbol', 'text', 'textPath', 'tref', 'tspan',
                               'use', 'view']),

    # svg elements
    DefaultAttribute('width', 100, Unit.PCT, elements=['svg']),
    DefaultAttribute('height', 100, Unit.PCT, elements=['svg']),
    DefaultAttribute('baseProfile', 'none', elements=['svg']),
    DefaultAttribute('preserveAspectRatio', 'xMidYMid meet',
                     elements=['feImage', 'image', 'marker', 'pattern', 'svg', 'symbol', 'view']),

    # common attributes / basic types
    DefaultAttribute('x',  0, elements=['cursor', 'fePointLight', 'feSpotLight', 'foreignObject',
                                        'image', 'pattern', 'rect', 'svg', 'text', 'use']),
    DefaultAttribute('y',  0, elements=['cursor', 'fePointLight', 'feSpotLight', 'foreignObject',
                                        'image', 'pattern', 'rect', 'svg', 'text', 'use']),
    DefaultAttribute('z',  0, elements=['fePointLight', 'feSpotLight']),
    DefaultAttribute('x1', 0, elements=['line']),
    DefaultAttribute('y1', 0, elements=['line']),
    DefaultAttribute('x2', 0, elements=['line']),
    DefaultAttribute('y2', 0, elements=['line']),
    DefaultAttribute('cx', 0, elements=['circle', 'ellipse']),
    DefaultAttribute('cy', 0, elements=['circle', 'ellipse']),

    # markers
    DefaultAttribute('markerUnits', 'strokeWidth', elements=['marker']),
    DefaultAttribute('refX', 0, elements=['marker']),
    DefaultAttribute('refY', 0, elements=['marker']),
    DefaultAttribute('markerHeight', 3, elements=['marker']),
    DefaultAttribute('markerWidth', 3, elements=['marker']),
    DefaultAttribute('orient', 0, elements=['marker']),

    # text / textPath / tspan / tref
    DefaultAttribute('lengthAdjust', 'spacing', elements=['text', 'textPath', 'tref', 'tspan']),
    DefaultAttribute('startOffset', 0, elements=['textPath']),
    DefaultAttribute('method', 'align', elements=['textPath']),
    DefaultAttribute('spacing', 'exact', elements=['textPath']),

    # filters and masks
    DefaultAttribute('x', -10, Unit.PCT, ['filter', 'mask']),
    DefaultAttribute('x', -0.1, Unit.NONE, ['filter', 'mask'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    DefaultAttribute('y', -10, Unit.PCT, ['filter', 'mask']),
    DefaultAttribute('y', -0.1, Unit.NONE, ['filter', 'mask'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    DefaultAttribute('width', 120, Unit.PCT, ['filter', 'mask']),
    DefaultAttribute('width', 1.2, Unit.NONE, ['filter', 'mask'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    DefaultAttribute('height', 120, Unit.PCT, ['filter', 'mask']),
    DefaultAttribute('height', 1.2, Unit.NONE, ['filter', 'mask'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),

    # gradients
    DefaultAttribute('x1', 0, elements=['linearGradient']),
    DefaultAttribute('y1', 0, elements=['linearGradient']),
    DefaultAttribute('y2', 0, elements=['linearGradient']),
    DefaultAttribute('x2', 100, Unit.PCT, elements=['linearGradient']),
    DefaultAttribute('x2', 1, Unit.NONE, elements=['linearGradient'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    # remove fx/fy before cx/cy to catch the case where fx = cx = 50% or fy = cy = 50% respectively
    DefaultAttribute('fx', elements=['radialGradient'],
                     conditions=lambda node: node.getAttribute('fx') == node.getAttribute('cx')),
    DefaultAttribute('fy', elements=['radialGradient'],
                     conditions=lambda node: node.getAttribute('fy') == node.getAttribute('cy')),
    DefaultAttribute('r',  50, Unit.PCT, elements=['radialGradient']),
    DefaultAttribute('r',  0.5, Unit.NONE, elements=['radialGradient'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    DefaultAttribute('cx', 50, Unit.PCT, elements=['radialGradient']),
    DefaultAttribute('cx', 0.5, Unit.NONE, elements=['radialGradient'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    DefaultAttribute('cy', 50, Unit.PCT, elements=['radialGradient']),
    DefaultAttribute('cy', 0.5, Unit.NONE, elements=['radialGradient'],
                     conditions=lambda node: node.getAttribute('gradientUnits') != 'userSpaceOnUse'),
    DefaultAttribute('spreadMethod', 'pad', elements=['linearGradient', 'radialGradient']),

    # filter effects
    #   TODO: Some numerical attributes allow an optional second value ("number-optional-number")
    #         and are currently handled as strings to avoid an exception in 'SVGLength', see
    #         https://github.com/scour-project/scour/pull/192
    DefaultAttribute('amplitude', 1, elements=['feFuncA', 'feFuncB', 'feFuncG', 'feFuncR']),
    DefaultAttribute('azimuth', 0, elements=['feDistantLight']),
    DefaultAttribute('baseFrequency', '0', elements=['feFuncA', 'feFuncB', 'feFuncG', 'feFuncR']),
    DefaultAttribute('bias', 1, elements=['feConvolveMatrix']),
    DefaultAttribute('diffuseConstant', 1, elements=['feDiffuseLighting']),
    DefaultAttribute('edgeMode', 'duplicate', elements=['feConvolveMatrix']),
    DefaultAttribute('elevation', 0, elements=['feDistantLight']),
    DefaultAttribute('exponent', 1, elements=['feFuncA', 'feFuncB', 'feFuncG', 'feFuncR']),
    DefaultAttribute('intercept', 0, elements=['feFuncA', 'feFuncB', 'feFuncG', 'feFuncR']),
    DefaultAttribute('k1', 0, elements=['feComposite']),
    DefaultAttribute('k2', 0, elements=['feComposite']),
    DefaultAttribute('k3', 0, elements=['feComposite']),
    DefaultAttribute('k4', 0, elements=['feComposite']),
    DefaultAttribute('mode', 'normal', elements=['feBlend']),
    DefaultAttribute('numOctaves', 1, elements=['feTurbulence']),
    DefaultAttribute('offset', 0, elements=['feFuncA', 'feFuncB', 'feFuncG', 'feFuncR']),
    DefaultAttribute('operator', 'over', elements=['feComposite']),
    DefaultAttribute('operator', 'erode', elements=['feMorphology']),
    DefaultAttribute('order', '3', elements=['feConvolveMatrix']),
    DefaultAttribute('pointsAtX', 0, elements=['feSpotLight']),
    DefaultAttribute('pointsAtY', 0, elements=['feSpotLight']),
    DefaultAttribute('pointsAtZ', 0, elements=['feSpotLight']),
    DefaultAttribute('preserveAlpha', 'false', elements=['feConvolveMatrix']),
    DefaultAttribute('radius', '0', elements=['feMorphology']),
    DefaultAttribute('scale', 0, elements=['feDisplacementMap']),
    DefaultAttribute('seed', 0, elements=['feTurbulence']),
    DefaultAttribute('specularConstant', 1, elements=['feSpecularLighting']),
    DefaultAttribute('specularExponent', 1, elements=['feSpecularLighting', 'feSpotLight']),
    DefaultAttribute('stdDeviation', '0', elements=['feGaussianBlur']),
    DefaultAttribute('stitchTiles', 'noStitch', elements=['feTurbulence']),
    DefaultAttribute('surfaceScale', 1, elements=['feDiffuseLighting', 'feSpecularLighting']),
    DefaultAttribute('type', 'matrix', elements=['feColorMatrix']),
    DefaultAttribute('type', 'turbulence', elements=['feTurbulence']),
    DefaultAttribute('xChannelSelector', 'A', elements=['feDisplacementMap']),
    DefaultAttribute('yChannelSelector', 'A', elements=['feDisplacementMap'])
]

# split to increase lookup performance
# TODO: 'default_attributes_universal' is actually empty right now - will we ever need it?
default_attributes_universal = []  # list containing attributes valid for all elements
default_attributes_per_element = defaultdict(list)  # dict containing lists of attributes valid for individual elements
for default_attribute in default_attributes:
    if default_attribute.elements is None:
        default_attributes_universal.append(default_attribute)
    else:
        for element in default_attribute.elements:
            default_attributes_per_element[element].append(default_attribute)


def taint(taintedSet, taintedAttribute):
    u"""Adds an attribute to a set of attributes.

    Related attributes are also included."""
    taintedSet.add(taintedAttribute)
    if taintedAttribute == 'marker':
        taintedSet |= set(['marker-start', 'marker-mid', 'marker-end'])
    if taintedAttribute in ['marker-start', 'marker-mid', 'marker-end']:
        taintedSet.add('marker')
    return taintedSet


def removeDefaultAttributeValue(node, attribute):
    """
    Removes the DefaultAttribute 'attribute' from 'node' if specified conditions are fulfilled

    Warning: Does NOT check if the attribute is actually valid for the passed element type for increased preformance!
    """
    if not node.hasAttribute(attribute.name):
        return 0

    # differentiate between text and numeric values
    if isinstance(attribute.value, str):
        if node.getAttribute(attribute.name) == attribute.value:
            if (attribute.conditions is None) or attribute.conditions(node):
                node.removeAttribute(attribute.name)
                return 1
    else:
        nodeValue = SVGLength(node.getAttribute(attribute.name))
        if ((attribute.value is None)
                or ((nodeValue.value == attribute.value) and not (nodeValue.units == Unit.INVALID))):
            if ((attribute.units is None)
                    or (nodeValue.units == attribute.units)
                    or (isinstance(attribute.units, list) and nodeValue.units in attribute.units)):
                if (attribute.conditions is None) or attribute.conditions(node):
                    node.removeAttribute(attribute.name)
                    return 1

    return 0


def removeDefaultAttributeValues(node, options, tainted=set()):
    u"""'tainted' keeps a set of attributes defined in parent nodes.

    For such attributes, we don't delete attributes with default values."""
    num = 0
    if node.nodeType != Node.ELEMENT_NODE:
        return 0

    # Conditionally remove all default attributes defined in 'default_attributes' (a list of 'DefaultAttribute's)
    #
    # For increased performance do not iterate the whole list for each element but run only on valid subsets
    # - 'default_attributes_universal' (attributes valid for all elements)
    # - 'default_attributes_per_element' (attributes specific to one specific element type)
    for attribute in default_attributes_universal:
        num += removeDefaultAttributeValue(node, attribute)
    if node.nodeName in default_attributes_per_element:
        for attribute in default_attributes_per_element[node.nodeName]:
            num += removeDefaultAttributeValue(node, attribute)

    # Summarily get rid of default properties
    attributes = [node.attributes.item(i).nodeName for i in range(node.attributes.length)]
    for attribute in attributes:
        if attribute not in tainted:
            if attribute in default_properties:
                if node.getAttribute(attribute) == default_properties[attribute]:
                    node.removeAttribute(attribute)
                    num += 1
                else:
                    tainted = taint(tainted, attribute)
    # Properties might also occur as styles, remove them too
    styles = _getStyle(node)
    for attribute in list(styles):
        if attribute not in tainted:
            if attribute in default_properties:
                if styles[attribute] == default_properties[attribute]:
                    del styles[attribute]
                    num += 1
                else:
                    tainted = taint(tainted, attribute)
    _setStyle(node, styles)

    # recurse for our child elements
    for child in node.childNodes:
        num += removeDefaultAttributeValues(child, options, tainted.copy())

    return num


rgb = re.compile(r"\s*rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)\s*")
rgbp = re.compile(r"\s*rgb\(\s*(\d*\.?\d+)%\s*,\s*(\d*\.?\d+)%\s*,\s*(\d*\.?\d+)%\s*\)\s*")


def convertColor(value):
    """
       Converts the input color string and returns a #RRGGBB (or #RGB if possible) string
    """
    s = value

    if s in colors:
        s = colors[s]

    rgbpMatch = rgbp.match(s)
    if rgbpMatch is not None:
        r = int(float(rgbpMatch.group(1)) * 255.0 / 100.0)
        g = int(float(rgbpMatch.group(2)) * 255.0 / 100.0)
        b = int(float(rgbpMatch.group(3)) * 255.0 / 100.0)
        s = '#%02x%02x%02x' % (r, g, b)
    else:
        rgbMatch = rgb.match(s)
        if rgbMatch is not None:
            r = int(rgbMatch.group(1))
            g = int(rgbMatch.group(2))
            b = int(rgbMatch.group(3))
            s = '#%02x%02x%02x' % (r, g, b)

    if s[0] == '#':
        s = s.lower()
        if len(s) == 7 and s[1] == s[2] and s[3] == s[4] and s[5] == s[6]:
            s = '#' + s[1] + s[3] + s[5]

    return s


def convertColors(element):
    """
       Recursively converts all color properties into #RRGGBB format if shorter
    """
    numBytes = 0

    if element.nodeType != Node.ELEMENT_NODE:
        return 0

    # set up list of color attributes for each element type
    attrsToConvert = []
    if element.nodeName in ['rect', 'circle', 'ellipse', 'polygon',
                            'line', 'polyline', 'path', 'g', 'a']:
        attrsToConvert = ['fill', 'stroke']
    elif element.nodeName in ['stop']:
        attrsToConvert = ['stop-color']
    elif element.nodeName in ['solidColor']:
        attrsToConvert = ['solid-color']

    # now convert all the color formats
    styles = _getStyle(element)
    for attr in attrsToConvert:
        oldColorValue = element.getAttribute(attr)
        if oldColorValue != '':
            newColorValue = convertColor(oldColorValue)
            oldBytes = len(oldColorValue)
            newBytes = len(newColorValue)
            if oldBytes > newBytes:
                element.setAttribute(attr, newColorValue)
                numBytes += (oldBytes - len(element.getAttribute(attr)))
        # colors might also hide in styles
        if attr in styles:
            oldColorValue = styles[attr]
            newColorValue = convertColor(oldColorValue)
            oldBytes = len(oldColorValue)
            newBytes = len(newColorValue)
            if oldBytes > newBytes:
                styles[attr] = newColorValue
                numBytes += (oldBytes - newBytes)
    _setStyle(element, styles)

    # now recurse for our child elements
    for child in element.childNodes:
        numBytes += convertColors(child)

    return numBytes

# TODO: go over what this method does and see if there is a way to optimize it
# TODO: go over the performance of this method and see if I can save memory/speed by
#       reusing data structures, etc


def cleanPath(element, options):
    """
       Cleans the path string (d attribute) of the element
    """
    global _num_bytes_saved_in_path_data
    global _num_path_segments_removed

    # this gets the parser object from svg_regex.py
    oldPathStr = element.getAttribute('d')
    path = svg_parser.parse(oldPathStr)
    style = _getStyle(element)

    # This determines whether the stroke has round or square linecaps.  If it does, we do not want to collapse empty
    # segments, as they are actually rendered (as circles or squares with diameter/dimension matching the path-width).
    has_round_or_square_linecaps = (
        element.getAttribute('stroke-linecap') in ['round', 'square']
        or 'stroke-linecap' in style and style['stroke-linecap'] in ['round', 'square']
    )

    # This determines whether the stroke has intermediate markers.  If it does, we do not want to collapse
    # straight segments running in the same direction, as markers are rendered on the intermediate nodes.
    has_intermediate_markers = (
        element.hasAttribute('marker')
        or element.hasAttribute('marker-mid')
        or 'marker' in style
        or 'marker-mid' in style
    )

    # The first command must be a moveto, and whether it's relative (m)
    # or absolute (M), the first set of coordinates *is* absolute. So
    # the first iteration of the loop below will get x,y and startx,starty.

    # convert absolute coordinates into relative ones.
    # Reuse the data structure 'path', since we're not adding or removing subcommands.
    # Also reuse the coordinate lists since we're not adding or removing any.
    x = y = 0
    for pathIndex in range(len(path)):
        cmd, data = path[pathIndex]  # Changes to cmd don't get through to the data structure
        i = 0
        # adjust abs to rel
        # only the A command has some values that we don't want to adjust (radii, rotation, flags)
        if cmd == 'A':
            for i in range(i, len(data), 7):
                data[i + 5] -= x
                data[i + 6] -= y
                x += data[i + 5]
                y += data[i + 6]
            path[pathIndex] = ('a', data)
        elif cmd == 'a':
            x += sum(data[5::7])
            y += sum(data[6::7])
        elif cmd == 'H':
            for i in range(i, len(data)):
                data[i] -= x
                x += data[i]
            path[pathIndex] = ('h', data)
        elif cmd == 'h':
            x += sum(data)
        elif cmd == 'V':
            for i in range(i, len(data)):
                data[i] -= y
                y += data[i]
            path[pathIndex] = ('v', data)
        elif cmd == 'v':
            y += sum(data)
        elif cmd == 'M':
            startx, starty = data[0], data[1]
            # If this is a path starter, don't convert its first
            # coordinate to relative; that would just make it (0, 0)
            if pathIndex != 0:
                data[0] -= x
                data[1] -= y

            x, y = startx, starty
            i = 2
            for i in range(i, len(data), 2):
                data[i] -= x
                data[i + 1] -= y
                x += data[i]
                y += data[i + 1]
            path[pathIndex] = ('m', data)
        elif cmd in ['L', 'T']:
            for i in range(i, len(data), 2):
                data[i] -= x
                data[i + 1] -= y
                x += data[i]
                y += data[i + 1]
            path[pathIndex] = (cmd.lower(), data)
        elif cmd in ['m']:
            if pathIndex == 0:
                # START OF PATH - this is an absolute moveto
                # followed by relative linetos
                startx, starty = data[0], data[1]
                x, y = startx, starty
                i = 2
            else:
                startx = x + data[0]
                starty = y + data[1]
            for i in range(i, len(data), 2):
                x += data[i]
                y += data[i + 1]
        elif cmd in ['l', 't']:
            x += sum(data[0::2])
            y += sum(data[1::2])
        elif cmd in ['S', 'Q']:
            for i in range(i, len(data), 4):
                data[i] -= x
                data[i + 1] -= y
                data[i + 2] -= x
                data[i + 3] -= y
                x += data[i + 2]
                y += data[i + 3]
            path[pathIndex] = (cmd.lower(), data)
        elif cmd in ['s', 'q']:
            x += sum(data[2::4])
            y += sum(data[3::4])
        elif cmd == 'C':
            for i in range(i, len(data), 6):
                data[i] -= x
                data[i + 1] -= y
                data[i + 2] -= x
                data[i + 3] -= y
                data[i + 4] -= x
                data[i + 5] -= y
                x += data[i + 4]
                y += data[i + 5]
            path[pathIndex] = ('c', data)
        elif cmd == 'c':
            x += sum(data[4::6])
            y += sum(data[5::6])
        elif cmd in ['z', 'Z']:
            x, y = startx, starty
            path[pathIndex] = ('z', data)

    # remove empty segments and redundant commands
    # Reuse the data structure 'path' and the coordinate lists, even if we're
    # deleting items, because these deletions are relatively cheap.
    if not has_round_or_square_linecaps:
        # remove empty path segments
        for pathIndex in range(len(path)):
            cmd, data = path[pathIndex]
            i = 0
            if cmd in ['m', 'l', 't']:
                if cmd == 'm':
                    # It might be tempting to rewrite "m0 0 ..." into
                    # "l..." here.  However, this is an unsound
                    # optimization in general as "m0 0 ... z" is
                    # different from "l...z".
                    #
                    # To do such a rewrite, we need to understand the
                    # full subpath.  This logic happens after this
                    # loop.
                    i = 2
                while i < len(data):
                    if data[i] == data[i + 1] == 0:
                        del data[i:i + 2]
                        _num_path_segments_removed += 1
                    else:
                        i += 2
            elif cmd == 'c':
                while i < len(data):
                    if data[i] == data[i + 1] == data[i + 2] == data[i + 3] == data[i + 4] == data[i + 5] == 0:
                        del data[i:i + 6]
                        _num_path_segments_removed += 1
                    else:
                        i += 6
            elif cmd == 'a':
                while i < len(data):
                    if data[i + 5] == data[i + 6] == 0:
                        del data[i:i + 7]
                        _num_path_segments_removed += 1
                    else:
                        i += 7
            elif cmd == 'q':
                while i < len(data):
                    if data[i] == data[i + 1] == data[i + 2] == data[i + 3] == 0:
                        del data[i:i + 4]
                        _num_path_segments_removed += 1
                    else:
                        i += 4
            elif cmd in ['h', 'v']:
                oldLen = len(data)
                path[pathIndex] = (cmd, [coord for coord in data if coord != 0])
                _num_path_segments_removed += len(path[pathIndex][1]) - oldLen

        # remove no-op commands
        pathIndex = len(path)
        subpath_needs_anchor = False
        # NB: We can never rewrite the first m/M command (expect if it
        # is the only command)
        while pathIndex > 1:
            pathIndex -= 1
            cmd, data = path[pathIndex]
            if cmd == 'z':
                next_cmd, next_data = path[pathIndex - 1]
                if next_cmd == 'm' and len(next_data) == 2:
                    # mX Yz -> mX Y

                    # note the len check on next_data as it is not
                    # safe to rewrite "m0 0 1 1z" in general (it is a
                    # question of where the "pen" ends - you can
                    # continue a draw on the same subpath after a
                    # "z").
                    del path[pathIndex]
                    _num_path_segments_removed += 1
                else:
                    # it is not safe to rewrite "m0 0 ..." to "l..."
                    # because of this "z" command.
                    subpath_needs_anchor = True
            elif cmd == 'm':
                if len(path) - 1 == pathIndex and len(data) == 2:
                    # Ends with an empty move (but no line/draw
                    # following it)
                    del path[pathIndex]
                    _num_path_segments_removed += 1
                    continue
                if subpath_needs_anchor:
                    subpath_needs_anchor = False
                elif data[0] == data[1] == 0:
                    # unanchored, i.e. we can replace "m0 0 ..." with
                    # "l..." as there is no "z" after it.
                    path[pathIndex] = ('l', data[2:])
                    _num_path_segments_removed += 1

    # fixup: Delete subcommands having no coordinates.
    path = [elem for elem in path if len(elem[1]) > 0 or elem[0] == 'z']

    # convert straight curves into lines
    newPath = [path[0]]
    for (cmd, data) in path[1:]:
        i = 0
        newData = data
        if cmd == 'c':
            newData = []
            while i < len(data):
                # since all commands are now relative, we can think of previous point as (0,0)
                # and new point (dx,dy) is (data[i+4],data[i+5])
                # eqn of line will be y = (dy/dx)*x or if dx=0 then eqn of line is x=0
                (p1x, p1y) = (data[i], data[i + 1])
                (p2x, p2y) = (data[i + 2], data[i + 3])
                dx = data[i + 4]
                dy = data[i + 5]

                foundStraightCurve = False

                if dx == 0:
                    if p1x == 0 and p2x == 0:
                        foundStraightCurve = True
                else:
                    m = dy / dx
                    if p1y == m * p1x and p2y == m * p2x:
                        foundStraightCurve = True

                if foundStraightCurve:
                    # flush any existing curve coords first
                    if newData:
                        newPath.append((cmd, newData))
                        newData = []
                    # now create a straight line segment
                    newPath.append(('l', [dx, dy]))
                else:
                    newData.extend(data[i:i + 6])

                i += 6
        if newData or cmd == 'z' or cmd == 'Z':
            newPath.append((cmd, newData))
    path = newPath

    # collapse all consecutive commands of the same type into one command
    prevCmd = ''
    prevData = []
    newPath = []
    for (cmd, data) in path:
        if prevCmd == '':
            # initialize with current path cmd and data
            prevCmd = cmd
            prevData = data
        else:
            # collapse if
            # - cmd is not moveto (explicit moveto commands are not drawn)
            # - the previous and current commands are the same type,
            # - the previous command is moveto and the current is lineto
            #   (subsequent moveto pairs are treated as implicit lineto commands)
            if cmd != 'm' and (cmd == prevCmd or (cmd == 'l' and prevCmd == 'm')):
                prevData.extend(data)
            # else flush the previous command if it is not the same type as the current command
            else:
                newPath.append((prevCmd, prevData))
                prevCmd = cmd
                prevData = data
    # flush last command and data
    newPath.append((prevCmd, prevData))
    path = newPath

    # convert to shorthand path segments where possible
    newPath = []
    for (cmd, data) in path:
        # convert line segments into h,v where possible
        if cmd == 'l':
            i = 0
            lineTuples = []
            while i < len(data):
                if data[i] == 0:
                    # vertical
                    if lineTuples:
                        # flush the existing line command
                        newPath.append(('l', lineTuples))
                        lineTuples = []
                    # append the v and then the remaining line coords
                    newPath.append(('v', [data[i + 1]]))
                    _num_path_segments_removed += 1
                elif data[i + 1] == 0:
                    if lineTuples:
                        # flush the line command, then append the h and then the remaining line coords
                        newPath.append(('l', lineTuples))
                        lineTuples = []
                    newPath.append(('h', [data[i]]))
                    _num_path_segments_removed += 1
                else:
                    lineTuples.extend(data[i:i + 2])
                i += 2
            if lineTuples:
                newPath.append(('l', lineTuples))
        # also handle implied relative linetos
        elif cmd == 'm':
            i = 2
            lineTuples = [data[0], data[1]]
            while i < len(data):
                if data[i] == 0:
                    # vertical
                    if lineTuples:
                        # flush the existing m/l command
                        newPath.append((cmd, lineTuples))
                        lineTuples = []
                        cmd = 'l'  # dealing with linetos now
                    # append the v and then the remaining line coords
                    newPath.append(('v', [data[i + 1]]))
                    _num_path_segments_removed += 1
                elif data[i + 1] == 0:
                    if lineTuples:
                        # flush the m/l command, then append the h and then the remaining line coords
                        newPath.append((cmd, lineTuples))
                        lineTuples = []
                        cmd = 'l'  # dealing with linetos now
                    newPath.append(('h', [data[i]]))
                    _num_path_segments_removed += 1
                else:
                    lineTuples.extend(data[i:i + 2])
                i += 2
            if lineTuples:
                newPath.append((cmd, lineTuples))
        # convert Bézier curve segments into s where possible
        elif cmd == 'c':
            # set up the assumed bezier control point as the current point,
            # i.e. (0,0) since we're using relative coords
            bez_ctl_pt = (0, 0)
            # however if the previous command was 's'
            # the assumed control point is a reflection of the previous control point at the current point
            if len(newPath):
                (prevCmd, prevData) = newPath[-1]
                if prevCmd == 's':
                    bez_ctl_pt = (prevData[-2] - prevData[-4], prevData[-1] - prevData[-3])
            i = 0
            curveTuples = []
            while i < len(data):
                # rotate by 180deg means negate both coordinates
                # if the previous control point is equal then we can substitute a
                # shorthand bezier command
                if bez_ctl_pt[0] == data[i] and bez_ctl_pt[1] == data[i + 1]:
                    if curveTuples:
                        newPath.append(('c', curveTuples))
                        curveTuples = []
                    # append the s command
                    newPath.append(('s', [data[i + 2], data[i + 3], data[i + 4], data[i + 5]]))
                    _num_path_segments_removed += 1
                else:
                    j = 0
                    while j <= 5:
                        curveTuples.append(data[i + j])
                        j += 1

                # set up control point for next curve segment
                bez_ctl_pt = (data[i + 4] - data[i + 2], data[i + 5] - data[i + 3])
                i += 6

            if curveTuples:
                newPath.append(('c', curveTuples))
        # convert quadratic curve segments into t where possible
        elif cmd == 'q':
            quad_ctl_pt = (0, 0)
            i = 0
            curveTuples = []
            while i < len(data):
                if quad_ctl_pt[0] == data[i] and quad_ctl_pt[1] == data[i + 1]:
                    if curveTuples:
                        newPath.append(('q', curveTuples))
                        curveTuples = []
                    # append the t command
                    newPath.append(('t', [data[i + 2], data[i + 3]]))
                    _num_path_segments_removed += 1
                else:
                    j = 0
                    while j <= 3:
                        curveTuples.append(data[i + j])
                        j += 1

                quad_ctl_pt = (data[i + 2] - data[i], data[i + 3] - data[i + 1])
                i += 4

            if curveTuples:
                newPath.append(('q', curveTuples))
        else:
            newPath.append((cmd, data))
    path = newPath

    # For each m, l, h or v, collapse unnecessary coordinates that run in the same direction
    # i.e. "h-100-100" becomes "h-200" but "h300-100" does not change.
    # If the path has intermediate markers we have to preserve intermediate nodes, though.
    # Reuse the data structure 'path', since we're not adding or removing subcommands.
    # Also reuse the coordinate lists, even if we're deleting items, because these
    # deletions are relatively cheap.
    if not has_intermediate_markers:
        for pathIndex in range(len(path)):
            cmd, data = path[pathIndex]

            # h / v expects only one parameter and we start drawing with the first (so we need at least 2)
            if cmd in ['h', 'v'] and len(data) >= 2:
                coordIndex = 0
                while coordIndex+1 < len(data):
                    if is_same_sign(data[coordIndex], data[coordIndex+1]):
                        data[coordIndex] += data[coordIndex+1]
                        del data[coordIndex+1]
                        _num_path_segments_removed += 1
                    else:
                        coordIndex += 1

            # l expects two parameters and we start drawing with the first (so we need at least 4)
            elif cmd == 'l' and len(data) >= 4:
                coordIndex = 0
                while coordIndex+2 < len(data):
                    if is_same_direction(*data[coordIndex:coordIndex+4]):
                        data[coordIndex] += data[coordIndex+2]
                        data[coordIndex+1] += data[coordIndex+3]
                        del data[coordIndex+2]  # delete the next two elements
                        del data[coordIndex+2]
                        _num_path_segments_removed += 1
                    else:
                        coordIndex += 2

            # m expects two parameters but we have to skip the first pair as it's not drawn (so we need at least 6)
            elif cmd == 'm' and len(data) >= 6:
                coordIndex = 2
                while coordIndex+2 < len(data):
                    if is_same_direction(*data[coordIndex:coordIndex+4]):
                        data[coordIndex] += data[coordIndex+2]
                        data[coordIndex+1] += data[coordIndex+3]
                        del data[coordIndex+2]  # delete the next two elements
                        del data[coordIndex+2]
                        _num_path_segments_removed += 1
                    else:
                        coordIndex += 2

    # it is possible that we have consecutive h, v, c, t commands now
    # so again collapse all consecutive commands of the same type into one command
    prevCmd = ''
    prevData = []
    newPath = [path[0]]
    for (cmd, data) in path[1:]:
        # flush the previous command if it is not the same type as the current command
        if prevCmd != '':
            if cmd != prevCmd or cmd == 'm':
                newPath.append((prevCmd, prevData))
                prevCmd = ''
                prevData = []

        # if the previous and current commands are the same type, collapse
        if cmd == prevCmd and cmd != 'm':
            prevData.extend(data)

        # save last command and data
        else:
            prevCmd = cmd
            prevData = data
    # flush last command and data
    if prevCmd != '':
        newPath.append((prevCmd, prevData))
    path = newPath

    newPathStr = serializePath(path, options)

    # if for whatever reason we actually made the path longer don't use it
    # TODO: maybe we could compare path lengths after each optimization step and use the shortest
    if len(newPathStr) <= len(oldPathStr):
        _num_bytes_saved_in_path_data += (len(oldPathStr) - len(newPathStr))
        element.setAttribute('d', newPathStr)


def parseListOfPoints(s):
    """
       Parse string into a list of points.

       Returns a list containing an even number of coordinate strings
    """
    i = 0

    # (wsp)? comma-or-wsp-separated coordinate pairs (wsp)?
    # coordinate-pair = coordinate comma-or-wsp coordinate
    # coordinate = sign? integer
    # comma-wsp: (wsp+ comma? wsp*) | (comma wsp*)
    ws_nums = RE_COMMA_WSP.split(s.strip())
    nums = []

    # also, if 100-100 is found, split it into two also
    #  <polygon points="100,-100,100-100,100-100-100,-100-100" />
    for i in range(len(ws_nums)):
        negcoords = ws_nums[i].split("-")

        # this string didn't have any negative coordinates
        if len(negcoords) == 1:
            nums.append(negcoords[0])
        # we got negative coords
        else:
            for j in range(len(negcoords)):
                # first number could be positive
                if j == 0:
                    if negcoords[0] != '':
                        nums.append(negcoords[0])
                # otherwise all other strings will be negative
                else:
                    # unless we accidentally split a number that was in scientific notation
                    # and had a negative exponent (500.00e-1)
                    prev = ""
                    if len(nums):
                        prev = nums[len(nums) - 1]
                    if prev and prev[len(prev) - 1] in ['e', 'E']:
                        nums[len(nums) - 1] = prev + '-' + negcoords[j]
                    else:
                        nums.append('-' + negcoords[j])

    # if we have an odd number of points, return empty
    if len(nums) % 2 != 0:
        return []

    # now resolve into Decimal values
    i = 0
    while i < len(nums):
        try:
            nums[i] = getcontext().create_decimal(nums[i])
            nums[i + 1] = getcontext().create_decimal(nums[i + 1])
        except InvalidOperation:  # one of the lengths had a unit or is an invalid number
            return []

        i += 2

    return nums


def cleanPolygon(elem, options):
    """
       Remove unnecessary closing point of polygon points attribute
    """
    global _num_points_removed_from_polygon

    pts = parseListOfPoints(elem.getAttribute('points'))
    N = len(pts) / 2
    if N >= 2:
        (startx, starty) = pts[:2]
        (endx, endy) = pts[-2:]
        if startx == endx and starty == endy:
            del pts[-2:]
            _num_points_removed_from_polygon += 1
    elem.setAttribute('points', scourCoordinates(pts, options, True))


def cleanPolyline(elem, options):
    """
       Scour the polyline points attribute
    """
    pts = parseListOfPoints(elem.getAttribute('points'))
    elem.setAttribute('points', scourCoordinates(pts, options, True))


def controlPoints(cmd, data):
    """
       Checks if there are control points in the path data

       Returns the indices of all values in the path data which are control points
    """
    cmd = cmd.lower()
    if cmd in ['c', 's', 'q']:
        indices = range(len(data))
        if cmd == 'c':  # c: (x1 y1 x2 y2 x y)+
            return [index for index in indices if (index % 6) < 4]
        elif cmd in ['s', 'q']:  # s: (x2 y2 x y)+   q: (x1 y1 x y)+
            return [index for index in indices if (index % 4) < 2]

    return []


def flags(cmd, data):
    """
       Checks if there are flags in the path data

       Returns the indices of all values in the path data which are flags
    """
    if cmd.lower() == 'a':  # a: (rx ry x-axis-rotation large-arc-flag sweep-flag x y)+
        indices = range(len(data))
        return [index for index in indices if (index % 7) in [3, 4]]

    return []


def serializePath(pathObj, options):
    """
       Reserializes the path data with some cleanups.
    """
    # elliptical arc commands must have comma/wsp separating the coordinates
    # this fixes an issue outlined in Fix https://bugs.launchpad.net/scour/+bug/412754
    return ''.join(cmd + scourCoordinates(data, options,
                                          control_points=controlPoints(cmd, data),
                                          flags=flags(cmd, data))
                   for cmd, data in pathObj)


def serializeTransform(transformObj):
    """
       Reserializes the transform data with some cleanups.
    """
    return ' '.join(command + '(' + ' '.join(scourUnitlessLength(number) for number in numbers) + ')'
                    for command, numbers in transformObj)


def scourCoordinates(data, options, force_whitespace=False, control_points=[], flags=[]):
    """
       Serializes coordinate data with some cleanups:
          - removes all trailing zeros after the decimal
          - integerize coordinates if possible
          - removes extraneous whitespace
          - adds spaces between values in a subcommand if required (or if force_whitespace is True)
    """
    if data is not None:
        newData = []
        c = 0
        previousCoord = ''
        for coord in data:
            is_control_point = c in control_points
            scouredCoord = scourUnitlessLength(coord,
                                               renderer_workaround=options.renderer_workaround,
                                               is_control_point=is_control_point)
            # don't output a space if this number starts with a dot (.) or minus sign (-); we only need a space if
            #   - this number starts with a digit
            #   - this number starts with a dot but the previous number had *no* dot or exponent
            #     i.e. '1.3 0.5' -> '1.3.5' or '1e3 0.5' -> '1e3.5' is fine but '123 0.5' -> '123.5' is obviously not
            #   - 'force_whitespace' is explicitly set to 'True'
            # we never need a space after flags (occuring in elliptical arcs), but librsvg struggles without it
            if (c > 0
                    and (force_whitespace
                         or scouredCoord[0].isdigit()
                         or (scouredCoord[0] == '.' and not ('.' in previousCoord or 'e' in previousCoord)))
                    and ((c-1 not in flags) or options.renderer_workaround)):
                newData.append(' ')

            # add the scoured coordinate to the path string
            newData.append(scouredCoord)
            previousCoord = scouredCoord
            c += 1

        # What we need to do to work around GNOME bugs 548494, 563933 and 620565, is to make sure that a dot doesn't
        # immediately follow a command  (so 'h50' and 'h0.5' are allowed, but not 'h.5').
        # Then, we need to add a space character after any coordinates  having an 'e' (scientific notation),
        # so as to have the exponent separate from the next number.
        # TODO: Check whether this is still required (bugs all marked as fixed, might be time to phase it out)
        if options.renderer_workaround:
            if len(newData) > 0:
                for i in range(1, len(newData)):
                    if newData[i][0] == '-' and 'e' in newData[i - 1]:
                        newData[i - 1] += ' '
                return ''.join(newData)
        else:
            return ''.join(newData)

    return ''


def scourLength(length):
    """
    Scours a length. Accepts units.
    """
    length = SVGLength(length)

    return scourUnitlessLength(length.value) + Unit.str(length.units)


def scourUnitlessLength(length, renderer_workaround=False, is_control_point=False):  # length is of a numeric type
    """
    Scours the numeric part of a length only. Does not accept units.

    This is faster than scourLength on elements guaranteed not to
    contain units.
    """
    if not isinstance(length, Decimal):
        length = getcontext().create_decimal(str(length))
    initial_length = length

    # reduce numeric precision
    # plus() corresponds to the unary prefix plus operator and applies context precision and rounding
    if is_control_point:
        length = scouringContextC.plus(length)
    else:
        length = scouringContext.plus(length)

    # remove trailing zeroes as we do not care for significance
    intLength = length.to_integral_value()
    if length == intLength:
        length = Decimal(intLength)
    else:
        length = length.normalize()

    # Gather the non-scientific notation version of the coordinate.
    # Re-quantize from the initial value to prevent unnecessary loss of precision
    # (e.g. 123.4 should become 123, not 120 or even 100)
    nonsci = '{0:f}'.format(length)
    nonsci = '{0:f}'.format(initial_length.quantize(Decimal(nonsci)))
    if not renderer_workaround:
        if len(nonsci) > 2 and nonsci[:2] == '0.':
            nonsci = nonsci[1:]  # remove the 0, leave the dot
        elif len(nonsci) > 3 and nonsci[:3] == '-0.':
            nonsci = '-' + nonsci[2:]  # remove the 0, leave the minus and dot
    return_value = nonsci

    # Gather the scientific notation version of the coordinate which
    # can only be shorter if the length of the number is at least 4 characters (e.g. 1000 = 1e3).
    if len(nonsci) > 3:
        # We have to implement this ourselves since both 'normalize()' and 'to_sci_string()'
        # don't handle negative exponents in a reasonable way (e.g. 0.000001 remains unchanged)
        exponent = length.adjusted()  # how far do we have to shift the dot?
        length = length.scaleb(-exponent).normalize()  # shift the dot and remove potential trailing zeroes

        sci = six.text_type(length) + 'e' + six.text_type(exponent)

        if len(sci) < len(nonsci):
            return_value = sci

    return return_value


def reducePrecision(element):
    """
    Because opacities, letter spacings, stroke widths and all that don't need
    to be preserved in SVG files with 9 digits of precision.

    Takes all of these attributes, in the given element node and its children,
    and reduces their precision to the current Decimal context's precision.
    Also checks for the attributes actually being lengths, not 'inherit', 'none'
    or anything that isn't an SVGLength.

    Returns the number of bytes saved after performing these reductions.
    """
    num = 0

    styles = _getStyle(element)
    for lengthAttr in ['opacity', 'flood-opacity', 'fill-opacity',
                       'stroke-opacity', 'stop-opacity', 'stroke-miterlimit',
                       'stroke-dashoffset', 'letter-spacing', 'word-spacing',
                       'kerning', 'font-size-adjust', 'font-size',
                       'stroke-width']:
        val = element.getAttribute(lengthAttr)
        if val != '':
            valLen = SVGLength(val)
            if valLen.units != Unit.INVALID:  # not an absolute/relative size or inherit, can be % though
                newVal = scourLength(val)
                if len(newVal) < len(val):
                    num += len(val) - len(newVal)
                    element.setAttribute(lengthAttr, newVal)
        # repeat for attributes hidden in styles
        if lengthAttr in styles:
            val = styles[lengthAttr]
            valLen = SVGLength(val)
            if valLen.units != Unit.INVALID:
                newVal = scourLength(val)
                if len(newVal) < len(val):
                    num += len(val) - len(newVal)
                    styles[lengthAttr] = newVal
    _setStyle(element, styles)

    for child in element.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            num += reducePrecision(child)

    return num


def optimizeAngle(angle):
    """
    Because any rotation can be expressed within 360 degrees
    of any given number, and since negative angles sometimes
    are one character longer than corresponding positive angle,
    we shorten the number to one in the range to [-90, 270[.
    """
    # First, we put the new angle in the range ]-360, 360[.
    # The modulo operator yields results with the sign of the
    # divisor, so for negative dividends, we preserve the sign
    # of the angle.
    if angle < 0:
        angle %= -360
    else:
        angle %= 360
    # 720 degrees is unnecessary, as 360 covers all angles.
    # As "-x" is shorter than "35x" and "-xxx" one character
    # longer than positive angles <= 260, we constrain angle
    # range to [-90, 270[ (or, equally valid: ]-100, 260]).
    if angle >= 270:
        angle -= 360
    elif angle < -90:
        angle += 360
    return angle


def optimizeTransform(transform):
    """
    Optimises a series of transformations parsed from a single
    transform="" attribute.

    The transformation list is modified in-place.
    """
    # FIXME: reordering these would optimize even more cases:
    #   first: Fold consecutive runs of the same transformation
    #   extra: Attempt to cast between types to create sameness:
    #          "matrix(0 1 -1 0 0 0) rotate(180) scale(-1)" all
    #          are rotations (90, 180, 180) -- thus "rotate(90)"
    #  second: Simplify transforms where numbers are optional.
    #   third: Attempt to simplify any single remaining matrix()
    #
    # if there's only one transformation and it's a matrix,
    # try to make it a shorter non-matrix transformation
    # NOTE: as matrix(a b c d e f) in SVG means the matrix:
    # |¯  a  c  e  ¯|   make constants   |¯  A1  A2  A3  ¯|
    # |   b  d  f   |  translating them  |   B1  B2  B3   |
    # |_  0  0  1  _|  to more readable  |_  0    0   1  _|
    if len(transform) == 1 and transform[0][0] == 'matrix':
        matrix = A1, B1, A2, B2, A3, B3 = transform[0][1]
        # |¯  1  0  0  ¯|
        # |   0  1  0   |  Identity matrix (no transformation)
        # |_  0  0  1  _|
        if matrix == [1, 0, 0, 1, 0, 0]:
            del transform[0]
        # |¯  1  0  X  ¯|
        # |   0  1  Y   |  Translation by (X, Y).
        # |_  0  0  1  _|
        elif (A1 == 1 and A2 == 0
              and B1 == 0 and B2 == 1):
            transform[0] = ('translate', [A3, B3])
        # |¯  X  0  0  ¯|
        # |   0  Y  0   |  Scaling by (X, Y).
        # |_  0  0  1  _|
        elif (A2 == 0 and A3 == 0
              and B1 == 0 and B3 == 0):
            transform[0] = ('scale', [A1, B2])
        # |¯  cos(A) -sin(A)    0    ¯|  Rotation by angle A,
        # |   sin(A)  cos(A)    0     |  clockwise, about the origin.
        # |_    0       0       1    _|  A is in degrees, [-180...180].
        elif (A1 == B2 and -1 <= A1 <= 1 and A3 == 0
              and -B1 == A2 and -1 <= B1 <= 1 and B3 == 0
              # as cos² A + sin² A == 1 and as decimal trig is approximate:
              # FIXME: the "epsilon" term here should really be some function
              #        of the precision of the (sin|cos)_A terms, not 1e-15:
              and abs((B1 ** 2) + (A1 ** 2) - 1) < Decimal("1e-15")):
            sin_A, cos_A = B1, A1
            # while asin(A) and acos(A) both only have an 180° range
            # the sign of sin(A) and cos(A) varies across quadrants,
            # letting us hone in on the angle the matrix represents:
            # -- => < -90 | -+ => -90..0 | ++ => 0..90 | +- => >= 90
            #
            # http://en.wikipedia.org/wiki/File:Sine_cosine_plot.svg
            # shows asin has the correct angle the middle quadrants:
            A = Decimal(str(math.degrees(math.asin(float(sin_A)))))
            if cos_A < 0:  # otherwise needs adjusting from the edges
                if sin_A < 0:
                    A = -180 - A
                else:
                    A = 180 - A
            transform[0] = ('rotate', [A])

    # Simplify transformations where numbers are optional.
    for type, args in transform:
        if type == 'translate':
            # Only the X coordinate is required for translations.
            # If the Y coordinate is unspecified, it's 0.
            if len(args) == 2 and args[1] == 0:
                del args[1]
        elif type == 'rotate':
            args[0] = optimizeAngle(args[0])  # angle
            # Only the angle is required for rotations.
            # If the coordinates are unspecified, it's the origin (0, 0).
            if len(args) == 3 and args[1] == args[2] == 0:
                del args[1:]
        elif type == 'scale':
            # Only the X scaling factor is required.
            # If the Y factor is unspecified, it's the same as X.
            if len(args) == 2 and args[0] == args[1]:
                del args[1]

    # Attempt to coalesce runs of the same transformation.
    # Translations followed immediately by other translations,
    # rotations followed immediately by other rotations,
    # scaling followed immediately by other scaling,
    # are safe to add.
    # Identity skewX/skewY are safe to remove, but how do they accrete?
    # |¯    1     0    0    ¯|
    # |   tan(A)  1    0     |  skews X coordinates by angle A
    # |_    0     0    1    _|
    #
    # |¯    1  tan(A)  0    ¯|
    # |     0     1    0     |  skews Y coordinates by angle A
    # |_    0     0    1    _|
    #
    # FIXME: A matrix followed immediately by another matrix
    #   would be safe to multiply together, too.
    i = 1
    while i < len(transform):
        currType, currArgs = transform[i]
        prevType, prevArgs = transform[i - 1]
        if currType == prevType == 'translate':
            prevArgs[0] += currArgs[0]  # x
            # for y, only add if the second translation has an explicit y
            if len(currArgs) == 2:
                if len(prevArgs) == 2:
                    prevArgs[1] += currArgs[1]  # y
                elif len(prevArgs) == 1:
                    prevArgs.append(currArgs[1])  # y
            del transform[i]
            if prevArgs[0] == prevArgs[1] == 0:
                # Identity translation!
                i -= 1
                del transform[i]
        elif (currType == prevType == 'rotate'
              and len(prevArgs) == len(currArgs) == 1):
            # Only coalesce if both rotations are from the origin.
            prevArgs[0] = optimizeAngle(prevArgs[0] + currArgs[0])
            del transform[i]
        elif currType == prevType == 'scale':
            prevArgs[0] *= currArgs[0]  # x
            # handle an implicit y
            if len(prevArgs) == 2 and len(currArgs) == 2:
                # y1 * y2
                prevArgs[1] *= currArgs[1]
            elif len(prevArgs) == 1 and len(currArgs) == 2:
                # create y2 = uniformscalefactor1 * y2
                prevArgs.append(prevArgs[0] * currArgs[1])
            elif len(prevArgs) == 2 and len(currArgs) == 1:
                # y1 * uniformscalefactor2
                prevArgs[1] *= currArgs[0]
            del transform[i]
            # if prevArgs is [1] or [1, 1], then it is effectively an
            # identity matrix and can be removed.
            if prevArgs[0] == 1 and (len(prevArgs) == 1 or prevArgs[1] == 1):
                # Identity scale!
                i -= 1
                del transform[i]
        else:
            i += 1

    # Some fixups are needed for single-element transformation lists, since
    # the loop above was to coalesce elements with their predecessors in the
    # list, and thus it required 2 elements.
    i = 0
    while i < len(transform):
        currType, currArgs = transform[i]
        if ((currType == 'skewX' or currType == 'skewY')
                and len(currArgs) == 1 and currArgs[0] == 0):
            # Identity skew!
            del transform[i]
        elif ((currType == 'rotate')
              and len(currArgs) == 1 and currArgs[0] == 0):
            # Identity rotation!
            del transform[i]
        else:
            i += 1


def optimizeTransforms(element, options):
    """
    Attempts to optimise transform specifications on the given node and its children.

    Returns the number of bytes saved after performing these reductions.
    """
    num = 0

    for transformAttr in ['transform', 'patternTransform', 'gradientTransform']:
        val = element.getAttribute(transformAttr)
        if val != '':
            transform = svg_transform_parser.parse(val)

            optimizeTransform(transform)

            newVal = serializeTransform(transform)

            if len(newVal) < len(val):
                if len(newVal):
                    element.setAttribute(transformAttr, newVal)
                else:
                    element.removeAttribute(transformAttr)
                num += len(val) - len(newVal)

    for child in element.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            num += optimizeTransforms(child, options)

    return num


def removeComments(element):
    """
       Removes comments from the element and its children.
    """
    global _num_bytes_saved_in_comments
    num = 0

    if isinstance(element, xml.dom.minidom.Comment):
        _num_bytes_saved_in_comments += len(element.data)
        element.parentNode.removeChild(element)
        num += 1
    else:
        for subelement in element.childNodes[:]:
            num += removeComments(subelement)

    return num


def embedRasters(element, options):
    import base64
    """
      Converts raster references to inline images.
      NOTE: there are size limits to base64-encoding handling in browsers
    """
    global _num_rasters_embedded

    href = element.getAttributeNS(NS['XLINK'], 'href')

    # if xlink:href is set, then grab the id
    if href != '' and len(href) > 1:
        ext = os.path.splitext(os.path.basename(href))[1].lower()[1:]

        # only operate on files with 'png', 'jpg', and 'gif' file extensions
        if ext in ['png', 'jpg', 'gif']:
            # fix common issues with file paths
            #     TODO: should we warn the user instead of trying to correct those invalid URIs?
            # convert backslashes to slashes
            href_fixed = href.replace('\\', '/')
            # absolute 'file:' URIs have to use three slashes (unless specifying a host which I've never seen)
            href_fixed = re.sub('file:/+', 'file:///', href_fixed)

            # parse the URI to get scheme and path
            # in principle it would make sense to work only with this ParseResult and call 'urlunparse()' in the end
            # however 'urlunparse(urlparse(file:raster.png))' -> 'file:///raster.png' which is nonsense
            parsed_href = urllib.parse.urlparse(href_fixed)

            # assume locations without protocol point to local files (and should use the 'file:' protocol)
            if parsed_href.scheme == '':
                parsed_href = parsed_href._replace(scheme='file')
                if href_fixed[0] == '/':
                    href_fixed = 'file://' + href_fixed
                else:
                    href_fixed = 'file:' + href_fixed

            # relative local paths are relative to the input file, therefore temporarily change the working dir
            working_dir_old = None
            if parsed_href.scheme == 'file' and parsed_href.path[0] != '/':
                if options.infilename:
                    working_dir_old = os.getcwd()
                    working_dir_new = os.path.abspath(os.path.dirname(options.infilename))
                    os.chdir(working_dir_new)

            # open/download the file
            try:
                file = urllib.request.urlopen(href_fixed)
                rasterdata = file.read()
                file.close()
            except Exception as e:
                print("WARNING: Could not open file '" + href + "' for embedding. "
                      "The raster image will be kept as a reference but might be invalid. "
                      "(Exception details: " + str(e) + ")", file=options.ensure_value("stdout", sys.stdout))
                rasterdata = ''
            finally:
                # always restore initial working directory if we changed it above
                if working_dir_old is not None:
                    os.chdir(working_dir_old)

            # TODO: should we remove all images which don't resolve?
            #   then we also have to consider unreachable remote locations (i.e. if there is no internet connection)
            if rasterdata != '':
                # base64-encode raster
                b64eRaster = base64.b64encode(rasterdata)

                # set href attribute to base64-encoded equivalent
                if b64eRaster != '':
                    # PNG and GIF both have MIME Type 'image/[ext]', but
                    # JPEG has MIME Type 'image/jpeg'
                    if ext == 'jpg':
                        ext = 'jpeg'

                    element.setAttributeNS(NS['XLINK'], 'href',
                                           'data:image/' + ext + ';base64,' + b64eRaster.decode())
                    _num_rasters_embedded += 1
                    del b64eRaster


def properlySizeDoc(docElement, options):
    # get doc width and height
    w = SVGLength(docElement.getAttribute('width'))
    h = SVGLength(docElement.getAttribute('height'))

    # if width/height are not unitless or px then it is not ok to rewrite them into a viewBox.
    # well, it may be OK for Web browsers and vector editors, but not for librsvg.
    if options.renderer_workaround:
        if ((w.units != Unit.NONE and w.units != Unit.PX) or
                (h.units != Unit.NONE and h.units != Unit.PX)):
            return

    # else we have a statically sized image and we should try to remedy that

    # parse viewBox attribute
    vbSep = RE_COMMA_WSP.split(docElement.getAttribute('viewBox'))
    # if we have a valid viewBox we need to check it
    vbWidth, vbHeight = 0, 0
    if len(vbSep) == 4:
        try:
            # if x or y are specified and non-zero then it is not ok to overwrite it
            vbX = float(vbSep[0])
            vbY = float(vbSep[1])
            if vbX != 0 or vbY != 0:
                return

            # if width or height are not equal to doc width/height then it is not ok to overwrite it
            vbWidth = float(vbSep[2])
            vbHeight = float(vbSep[3])
            if vbWidth != w.value or vbHeight != h.value:
                return
        # if the viewBox did not parse properly it is invalid and ok to overwrite it
        except ValueError:
            pass

    # at this point it's safe to set the viewBox and remove width/height
    docElement.setAttribute('viewBox', '0 0 %s %s' % (w.value, h.value))
    docElement.removeAttribute('width')
    docElement.removeAttribute('height')


def remapNamespacePrefix(node, oldprefix, newprefix):
    if node is None or node.nodeType != Node.ELEMENT_NODE:
        return

    if node.prefix == oldprefix:
        localName = node.localName
        namespace = node.namespaceURI
        doc = node.ownerDocument
        parent = node.parentNode

        # create a replacement node
        newNode = None
        if newprefix != '':
            newNode = doc.createElementNS(namespace, newprefix + ":" + localName)
        else:
            newNode = doc.createElement(localName)

        # add all the attributes
        attrList = node.attributes
        for i in range(attrList.length):
            attr = attrList.item(i)
            newNode.setAttributeNS(attr.namespaceURI, attr.name, attr.nodeValue)

        # clone and add all the child nodes
        for child in node.childNodes:
            newNode.appendChild(child.cloneNode(True))

        # replace old node with new node
        parent.replaceChild(newNode, node)
        # set the node to the new node in the remapped namespace prefix
        node = newNode

    # now do all child nodes
    for child in node.childNodes:
        remapNamespacePrefix(child, oldprefix, newprefix)


def make_well_formed(text, quote_dict=None):
    if quote_dict is None:
        quote_dict = XML_ENTS_NO_QUOTES
    if not any(c in text for c in quote_dict):
        # The quote-able characters are quite rare in SVG (they mostly only
        # occur in text elements in practice).  Therefore it make sense to
        # optimize for this common case
        return text
    return ''.join(quote_dict[c] if c in quote_dict else c for c in text)


def choose_quote_character(value):
    quot_count = value.count('"')
    if quot_count == 0 or quot_count <= value.count("'"):
        # Fewest "-symbols (if there are 0, we pick this to avoid spending
        # time counting the '-symbols as it won't matter)
        quote = '"'
        xml_ent = XML_ENTS_ESCAPE_QUOT
    else:
        quote = "'"
        xml_ent = XML_ENTS_ESCAPE_APOS
    return quote, xml_ent


TEXT_CONTENT_ELEMENTS = ['text', 'tspan', 'tref', 'textPath', 'altGlyph',
                         'flowDiv', 'flowPara', 'flowSpan', 'flowTref', 'flowLine']


KNOWN_ATTRS = [
        # TODO: Maybe update with full list from https://www.w3.org/TR/SVG/attindex.html
        # (but should be kept intuitively ordered)
        'id', 'xml:id', 'class',
        'transform',
        'x', 'y', 'z', 'width', 'height', 'x1', 'x2', 'y1', 'y2',
        'dx', 'dy', 'rotate', 'startOffset', 'method', 'spacing',
        'cx', 'cy', 'r', 'rx', 'ry', 'fx', 'fy',
        'd', 'points',
    ] + sorted(svgAttributes) + [
        'style',
    ]

KNOWN_ATTRS_ORDER_BY_NAME = defaultdict(lambda: len(KNOWN_ATTRS),
                                        {name: order for order, name in enumerate(KNOWN_ATTRS)})


# use custom order for known attributes and alphabetical order for the rest
def _attribute_sort_key_function(attribute):
    name = attribute.name
    order_value = KNOWN_ATTRS_ORDER_BY_NAME[name]
    return order_value, name


def attributes_ordered_for_output(element):
    if not element.hasAttributes():
        return []
    attribute = element.attributes
    # The .item(i) call is painfully slow (bpo#40689). Therefore we ensure we
    # call it at most once per attribute.
    # - it would be many times faster to use `attribute.values()` but sadly
    #   that is an "experimental" interface.
    return sorted((attribute.item(i) for i in range(attribute.length)),
                  key=_attribute_sort_key_function)


# hand-rolled serialization function that has the following benefits:
# - pretty printing
# - somewhat judicious use of whitespace
# - ensure id attributes are first
def serializeXML(element, options, indent_depth=0, preserveWhitespace=False):
    outParts = []

    indent_type = ''
    newline = ''
    if options.newlines:
        if options.indent_type == 'tab':
            indent_type = '\t'
        elif options.indent_type == 'space':
            indent_type = ' '
        indent_type *= options.indent_depth
        newline = '\n'

    outParts.extend([(indent_type * indent_depth), '<', element.nodeName])

    # now serialize the other attributes
    attrs = attributes_ordered_for_output(element)
    for attr in attrs:
        attrValue = attr.nodeValue
        quote, xml_ent = choose_quote_character(attrValue)
        attrValue = make_well_formed(attrValue, xml_ent)

        if attr.nodeName == 'style':
            # sort declarations
            attrValue = ';'.join(sorted(attrValue.split(';')))

        outParts.append(' ')
        # preserve xmlns: if it is a namespace prefix declaration
        if attr.prefix is not None:
            outParts.extend([attr.prefix, ':'])
        elif attr.namespaceURI is not None:
            if attr.namespaceURI == 'http://www.w3.org/2000/xmlns/' and attr.nodeName.find('xmlns') == -1:
                outParts.append('xmlns:')
            elif attr.namespaceURI == 'http://www.w3.org/1999/xlink':
                outParts.append('xlink:')
        outParts.extend([attr.localName, '=', quote, attrValue, quote])

        if attr.nodeName == 'xml:space':
            if attrValue == 'preserve':
                preserveWhitespace = True
            elif attrValue == 'default':
                preserveWhitespace = False

    children = element.childNodes
    if children.length == 0:
        outParts.append('/>')
    else:
        outParts.append('>')

        onNewLine = False
        for child in element.childNodes:
            # element node
            if child.nodeType == Node.ELEMENT_NODE:
                # do not indent inside text content elements as in SVG there's a difference between
                #    "text1\ntext2" and
                #    "text1\n text2"
                # see https://www.w3.org/TR/SVG/text.html#WhiteSpace
                if preserveWhitespace or element.nodeName in TEXT_CONTENT_ELEMENTS:
                    outParts.append(serializeXML(child, options, 0, preserveWhitespace))
                else:
                    outParts.extend([newline, serializeXML(child, options, indent_depth + 1, preserveWhitespace)])
                    onNewLine = True
            # text node
            elif child.nodeType == Node.TEXT_NODE:
                text_content = child.nodeValue
                if not preserveWhitespace:
                    # strip / consolidate whitespace according to spec, see
                    #    https://www.w3.org/TR/SVG/text.html#WhiteSpace
                    if element.nodeName in TEXT_CONTENT_ELEMENTS:
                        text_content = text_content.replace('\n', '')
                        text_content = text_content.replace('\t', ' ')
                        if child == element.firstChild:
                            text_content = text_content.lstrip()
                        elif child == element.lastChild:
                            text_content = text_content.rstrip()
                        while '  ' in text_content:
                            text_content = text_content.replace('  ', ' ')
                    else:
                        text_content = text_content.strip()
                outParts.append(make_well_formed(text_content))
            # CDATA node
            elif child.nodeType == Node.CDATA_SECTION_NODE:
                outParts.extend(['<![CDATA[', child.nodeValue, ']]>'])
            # Comment node
            elif child.nodeType == Node.COMMENT_NODE:
                outParts.extend([newline, indent_type * (indent_depth+1), '<!--', child.nodeValue, '-->'])
            # TODO: entities, processing instructions, what else?
            else:  # ignore the rest
                pass

        if onNewLine:
            outParts.append(newline)
            outParts.append(indent_type * indent_depth)
        outParts.extend(['</', element.nodeName, '>'])

    return "".join(outParts)


# this is the main method
# input is a string representation of the input XML
# returns a string representation of the output XML
def scourString(in_string, options=None):
    # sanitize options (take missing attributes from defaults, discard unknown attributes)
    options = sanitizeOptions(options)

    # default or invalid value
    if(options.cdigits < 0):
        options.cdigits = options.digits

    # create decimal contexts with reduced precision for scouring numbers
    # calculations should be done in the default context (precision defaults to 28 significant digits)
    # to minimize errors
    global scouringContext
    global scouringContextC  # even more reduced precision for control points
    scouringContext = Context(prec=options.digits)
    scouringContextC = Context(prec=options.cdigits)

    # globals for tracking statistics
    # TODO: get rid of these globals...
    global _num_elements_removed
    global _num_attributes_removed
    global _num_ids_removed
    global _num_comments_removed
    global _num_style_properties_fixed
    global _num_rasters_embedded
    global _num_path_segments_removed
    global _num_points_removed_from_polygon
    global _num_bytes_saved_in_path_data
    global _num_bytes_saved_in_colors
    global _num_bytes_saved_in_comments
    global _num_bytes_saved_in_ids
    global _num_bytes_saved_in_lengths
    global _num_bytes_saved_in_transforms
    _num_elements_removed = 0
    _num_attributes_removed = 0
    _num_ids_removed = 0
    _num_comments_removed = 0
    _num_style_properties_fixed = 0
    _num_rasters_embedded = 0
    _num_path_segments_removed = 0
    _num_points_removed_from_polygon = 0
    _num_bytes_saved_in_path_data = 0
    _num_bytes_saved_in_colors = 0
    _num_bytes_saved_in_comments = 0
    _num_bytes_saved_in_ids = 0
    _num_bytes_saved_in_lengths = 0
    _num_bytes_saved_in_transforms = 0

    doc = xml.dom.minidom.parseString(in_string)

    # determine number of flowRoot elements in input document
    # flowRoot elements don't render at all on current browsers (04/2016)
    cnt_flowText_el = len(doc.getElementsByTagName('flowRoot'))
    if cnt_flowText_el:
        errmsg = "SVG input document uses {} flow text elements, " \
                 "which won't render on browsers!".format(cnt_flowText_el)
        if options.error_on_flowtext:
            raise Exception(errmsg)
        else:
            print("WARNING: {}".format(errmsg), file=sys.stderr)

    # remove descriptive elements
    removeDescriptiveElements(doc, options)

    # remove unneeded namespaced elements/attributes added by common editors
    if options.keep_editor_data is False:
        _num_elements_removed += removeNamespacedElements(doc.documentElement,
                                                          unwanted_ns)
        _num_attributes_removed += removeNamespacedAttributes(doc.documentElement,
                                                              unwanted_ns)

        # remove the xmlns: declarations now
        xmlnsDeclsToRemove = []
        attrList = doc.documentElement.attributes
        for index in range(attrList.length):
            if attrList.item(index).nodeValue in unwanted_ns:
                xmlnsDeclsToRemove.append(attrList.item(index).nodeName)

        for attr in xmlnsDeclsToRemove:
            doc.documentElement.removeAttribute(attr)
            _num_attributes_removed += 1

    # ensure namespace for SVG is declared
    # TODO: what if the default namespace is something else (i.e. some valid namespace)?
    if doc.documentElement.getAttribute('xmlns') != 'http://www.w3.org/2000/svg':
        doc.documentElement.setAttribute('xmlns', 'http://www.w3.org/2000/svg')
        # TODO: throw error or warning?

    # check for redundant and unused SVG namespace declarations
    def xmlnsUnused(prefix, namespace):
        if doc.getElementsByTagNameNS(namespace, "*"):
            return False
        else:
            for element in doc.getElementsByTagName("*"):
                for attribute in element.attributes.values():
                    if attribute.name.startswith(prefix):
                        return False
        return True

    attrList = doc.documentElement.attributes
    xmlnsDeclsToRemove = []
    redundantPrefixes = []
    for i in range(attrList.length):
        attr = attrList.item(i)
        name = attr.nodeName
        val = attr.nodeValue
        if name[0:6] == 'xmlns:':
            if val == 'http://www.w3.org/2000/svg':
                redundantPrefixes.append(name[6:])
                xmlnsDeclsToRemove.append(name)
            elif xmlnsUnused(name[6:], val):
                xmlnsDeclsToRemove.append(name)

    for attrName in xmlnsDeclsToRemove:
        doc.documentElement.removeAttribute(attrName)
        _num_attributes_removed += 1

    for prefix in redundantPrefixes:
        remapNamespacePrefix(doc.documentElement, prefix, '')

    if options.strip_comments:
        _num_comments_removed = removeComments(doc)

    if options.strip_xml_space_attribute and doc.documentElement.hasAttribute('xml:space'):
        doc.documentElement.removeAttribute('xml:space')
        _num_attributes_removed += 1

    # repair style (remove unnecessary style properties and change them into XML attributes)
    _num_style_properties_fixed = repairStyle(doc.documentElement, options)

    # convert colors to #RRGGBB format
    if options.simple_colors:
        _num_bytes_saved_in_colors = convertColors(doc.documentElement)

    # remove unreferenced gradients/patterns outside of defs
    # and most unreferenced elements inside of defs
    while removeUnreferencedElements(doc, options.keep_defs) > 0:
        pass

    # remove empty defs, metadata, g
    # NOTE: these elements will be removed if they just have whitespace-only text nodes
    for tag in ['defs', 'title', 'desc', 'metadata', 'g']:
        for elem in doc.documentElement.getElementsByTagName(tag):
            removeElem = not elem.hasChildNodes()
            if removeElem is False:
                for child in elem.childNodes:
                    if child.nodeType in [Node.ELEMENT_NODE, Node.CDATA_SECTION_NODE, Node.COMMENT_NODE]:
                        break
                    elif child.nodeType == Node.TEXT_NODE and not child.nodeValue.isspace():
                        break
                else:
                    removeElem = True
            if removeElem:
                elem.parentNode.removeChild(elem)
                _num_elements_removed += 1

    if options.strip_ids:
        referencedIDs = findReferencedElements(doc.documentElement)
        identifiedElements = unprotected_ids(doc, options)
        removeUnreferencedIDs(referencedIDs, identifiedElements)

    while removeDuplicateGradientStops(doc) > 0:
        pass

    # remove gradients that are only referenced by one other gradient
    while collapseSinglyReferencedGradients(doc) > 0:
        pass

    # remove duplicate gradients
    _num_elements_removed += removeDuplicateGradients(doc)

    if options.group_collapse:
        _num_elements_removed += mergeSiblingGroupsWithCommonAttributes(doc.documentElement)
    # create <g> elements if there are runs of elements with the same attributes.
    # this MUST be before moveCommonAttributesToParentGroup.
    if options.group_create:
        createGroupsForCommonAttributes(doc.documentElement)

    # move common attributes to parent group
    # NOTE: the if the <svg> element's immediate children
    # all have the same value for an attribute, it must not
    # get moved to the <svg> element. The <svg> element
    # doesn't accept fill=, stroke= etc.!
    referencedIds = findReferencedElements(doc.documentElement)
    for child in doc.documentElement.childNodes:
        _num_attributes_removed += moveCommonAttributesToParentGroup(child, referencedIds)

    # remove unused attributes from parent
    _num_attributes_removed += removeUnusedAttributesOnParent(doc.documentElement)

    # Collapse groups LAST, because we've created groups. If done before
    # moveAttributesToParentGroup, empty <g>'s may remain.
    if options.group_collapse:
        while removeNestedGroups(doc.documentElement) > 0:
            pass

    # remove unnecessary closing point of polygons and scour points
    for polygon in doc.documentElement.getElementsByTagName('polygon'):
        cleanPolygon(polygon, options)

    # scour points of polyline
    for polyline in doc.documentElement.getElementsByTagName('polyline'):
        cleanPolyline(polyline, options)

    # clean path data
    for elem in doc.documentElement.getElementsByTagName('path'):
        if elem.getAttribute('d') == '':
            elem.parentNode.removeChild(elem)
        else:
            cleanPath(elem, options)

    # shorten ID names as much as possible
    if options.shorten_ids:
        _num_bytes_saved_in_ids += shortenIDs(doc, options.shorten_ids_prefix, options)

    # scour lengths (including coordinates)
    for type in ['svg', 'image', 'rect', 'circle', 'ellipse', 'line',
                 'linearGradient', 'radialGradient', 'stop', 'filter']:
        for elem in doc.getElementsByTagName(type):
            for attr in ['x', 'y', 'width', 'height', 'cx', 'cy', 'r', 'rx', 'ry',
                         'x1', 'y1', 'x2', 'y2', 'fx', 'fy', 'offset']:
                if elem.getAttribute(attr) != '':
                    elem.setAttribute(attr, scourLength(elem.getAttribute(attr)))
    viewBox = doc.documentElement.getAttribute('viewBox')
    if viewBox:
        lengths = RE_COMMA_WSP.split(viewBox)
        lengths = [scourUnitlessLength(length) for length in lengths]
        doc.documentElement.setAttribute('viewBox', ' '.join(lengths))

    # more length scouring in this function
    _num_bytes_saved_in_lengths = reducePrecision(doc.documentElement)

    # remove default values of attributes
    _num_attributes_removed += removeDefaultAttributeValues(doc.documentElement, options)

    # reduce the length of transformation attributes
    _num_bytes_saved_in_transforms = optimizeTransforms(doc.documentElement, options)

    # convert rasters references to base64-encoded strings
    if options.embed_rasters:
        for elem in doc.documentElement.getElementsByTagName('image'):
            embedRasters(elem, options)

    # properly size the SVG document (ideally width/height should be 100% with a viewBox)
    if options.enable_viewboxing:
        properlySizeDoc(doc.documentElement, options)

    # output the document as a pretty string with a single space for indent
    # NOTE: removed pretty printing because of this problem:
    # http://ronrothman.com/public/leftbraned/xml-dom-minidom-toprettyxml-and-silly-whitespace/
    # rolled our own serialize function here to save on space, put id first, customize indentation, etc
#  out_string = doc.documentElement.toprettyxml(' ')
    out_string = serializeXML(doc.documentElement, options) + '\n'

    # return the string with its XML prolog and surrounding comments
    if options.strip_xml_prolog is False:
        total_output = '<?xml version="1.0" encoding="UTF-8"'
        if doc.standalone:
            total_output += ' standalone="yes"'
        total_output += '?>\n'
    else:
        total_output = ""

    for child in doc.childNodes:
        if child.nodeType == Node.ELEMENT_NODE:
            total_output += out_string
        else:  # doctypes, entities, comments
            total_output += child.toxml() + '\n'

    return total_output


# used mostly by unit tests
# input is a filename
# returns the minidom doc representation of the SVG
def scourXmlFile(filename, options=None):
    # sanitize options (take missing attributes from defaults, discard unknown attributes)
    options = sanitizeOptions(options)
    # we need to make sure infilename is set correctly (otherwise relative references in the SVG won't work)
    options.ensure_value("infilename", filename)

    # open the file and scour it
    with open(filename, "rb") as f:
        in_string = f.read()
    out_string = scourString(in_string, options)

    # prepare the output xml.dom.minidom object
    doc = xml.dom.minidom.parseString(out_string.encode('utf-8'))

    # since minidom does not seem to parse DTDs properly
    # manually declare all attributes with name "id" to be of type ID
    # (otherwise things like doc.getElementById() won't work)
    all_nodes = doc.getElementsByTagName("*")
    for node in all_nodes:
        try:
            node.setIdAttribute('id')
        except NotFoundErr:
            pass

    return doc


# GZ: Seems most other commandline tools don't do this, is it really wanted?
class HeaderedFormatter(optparse.IndentedHelpFormatter):
    """
       Show application name, version number, and copyright statement
       above usage information.
    """

    def format_usage(self, usage):
        return "%s %s\n%s\n%s" % (APP, VER, COPYRIGHT,
                                  optparse.IndentedHelpFormatter.format_usage(self, usage))


# GZ: would prefer this to be in a function or class scope, but tests etc need
#     access to the defaults anyway
_options_parser = optparse.OptionParser(
    usage="%prog [INPUT.SVG [OUTPUT.SVG]] [OPTIONS]",
    description=("If the input/output files are not specified, stdin/stdout are used. "
                 "If the input/output files are specified with a svgz extension, "
                 "then compressed SVG is assumed."),
    formatter=HeaderedFormatter(max_help_position=33),
    version=VER)

# legacy options (kept around for backwards compatibility, should not be used in new code)
_options_parser.add_option("-p", action="store", type=int, dest="digits", help=optparse.SUPPRESS_HELP)

# general options
_options_parser.add_option("-q", "--quiet",
                           action="store_true", dest="quiet", default=False,
                           help="suppress non-error output")
_options_parser.add_option("-v", "--verbose",
                           action="store_true", dest="verbose", default=False,
                           help="verbose output (statistics, etc.)")
_options_parser.add_option("-i",
                           action="store", dest="infilename", metavar="INPUT.SVG",
                           help="alternative way to specify input filename")
_options_parser.add_option("-o",
                           action="store", dest="outfilename", metavar="OUTPUT.SVG",
                           help="alternative way to specify output filename")

_option_group_optimization = optparse.OptionGroup(_options_parser, "Optimization")
_option_group_optimization.add_option("--set-precision",
                                      action="store", type=int, dest="digits", default=5, metavar="NUM",
                                      help="set number of significant digits (default: %default)")
_option_group_optimization.add_option("--set-c-precision",
                                      action="store", type=int, dest="cdigits", default=-1, metavar="NUM",
                                      help="set number of significant digits for control points "
                                           "(default: same as '--set-precision')")
_option_group_optimization.add_option("--disable-simplify-colors",
                                      action="store_false", dest="simple_colors", default=True,
                                      help="won't convert colors to #RRGGBB format")
_option_group_optimization.add_option("--disable-style-to-xml",
                                      action="store_false", dest="style_to_xml", default=True,
                                      help="won't convert styles into XML attributes")
_option_group_optimization.add_option("--disable-group-collapsing",
                                      action="store_false", dest="group_collapse", default=True,
                                      help="won't collapse <g> elements")
_option_group_optimization.add_option("--create-groups",
                                      action="store_true", dest="group_create", default=False,
                                      help="create <g> elements for runs of elements with identical attributes")
_option_group_optimization.add_option("--keep-editor-data",
                                      action="store_true", dest="keep_editor_data", default=False,
                                      help="won't remove Inkscape, Sodipodi, Adobe Illustrator "
                                           "or Sketch elements and attributes")
_option_group_optimization.add_option("--keep-unreferenced-defs",
                                      action="store_true", dest="keep_defs", default=False,
                                      help="won't remove elements within the defs container that are unreferenced")
_option_group_optimization.add_option("--renderer-workaround",
                                      action="store_true", dest="renderer_workaround", default=True,
                                      help="work around various renderer bugs (currently only librsvg) (default)")
_option_group_optimization.add_option("--no-renderer-workaround",
                                      action="store_false", dest="renderer_workaround", default=True,
                                      help="do not work around various renderer bugs (currently only librsvg)")
_options_parser.add_option_group(_option_group_optimization)

_option_group_document = optparse.OptionGroup(_options_parser, "SVG document")
_option_group_document.add_option("--strip-xml-prolog",
                                  action="store_true", dest="strip_xml_prolog", default=False,
                                  help="won't output the XML prolog (<?xml ?>)")
_option_group_document.add_option("--remove-titles",
                                  action="store_true", dest="remove_titles", default=False,
                                  help="remove <title> elements")
_option_group_document.add_option("--remove-descriptions",
                                  action="store_true", dest="remove_descriptions", default=False,
                                  help="remove <desc> elements")
_option_group_document.add_option("--remove-metadata",
                                  action="store_true", dest="remove_metadata", default=False,
                                  help="remove <metadata> elements "
                                       "(which may contain license/author information etc.)")
_option_group_document.add_option("--remove-descriptive-elements",
                                  action="store_true", dest="remove_descriptive_elements", default=False,
                                  help="remove <title>, <desc> and <metadata> elements")
_option_group_document.add_option("--enable-comment-stripping",
                                  action="store_true", dest="strip_comments", default=False,
                                  help="remove all comments (<!-- -->)")
_option_group_document.add_option("--disable-embed-rasters",
                                  action="store_false", dest="embed_rasters", default=True,
                                  help="won't embed rasters as base64-encoded data")
_option_group_document.add_option("--enable-viewboxing",
                                  action="store_true", dest="enable_viewboxing", default=False,
                                  help="changes document width/height to 100%/100% and creates viewbox coordinates")
_options_parser.add_option_group(_option_group_document)

_option_group_formatting = optparse.OptionGroup(_options_parser, "Output formatting")
_option_group_formatting.add_option("--indent",
                                    action="store", type="string", dest="indent_type", default="space", metavar="TYPE",
                                    help="indentation of the output: none, space, tab (default: %default)")
_option_group_formatting.add_option("--nindent",
                                    action="store", type=int, dest="indent_depth", default=1, metavar="NUM",
                                    help="depth of the indentation, i.e. number of spaces/tabs: (default: %default)")
_option_group_formatting.add_option("--no-line-breaks",
                                    action="store_false", dest="newlines", default=True,
                                    help="do not create line breaks in output"
                                    "(also disables indentation; might be overridden by xml:space=\"preserve\")")
_option_group_formatting.add_option("--strip-xml-space",
                                    action="store_true", dest="strip_xml_space_attribute", default=False,
                                    help="strip the xml:space=\"preserve\" attribute from the root SVG element")
_options_parser.add_option_group(_option_group_formatting)

_option_group_ids = optparse.OptionGroup(_options_parser, "ID attributes")
_option_group_ids.add_option("--enable-id-stripping",
                             action="store_true", dest="strip_ids", default=False,
                             help="remove all unreferenced IDs")
_option_group_ids.add_option("--shorten-ids",
                             action="store_true", dest="shorten_ids", default=False,
                             help="shorten all IDs to the least number of letters possible")
_option_group_ids.add_option("--shorten-ids-prefix",
                             action="store", type="string", dest="shorten_ids_prefix", default="", metavar="PREFIX",
                             help="add custom prefix to shortened IDs")
_option_group_ids.add_option("--protect-ids-noninkscape",
                             action="store_true", dest="protect_ids_noninkscape", default=False,
                             help="don't remove IDs not ending with a digit")
_option_group_ids.add_option("--protect-ids-list",
                             action="store", type="string", dest="protect_ids_list", metavar="LIST",
                             help="don't remove IDs given in this comma-separated list")
_option_group_ids.add_option("--protect-ids-prefix",
                             action="store", type="string", dest="protect_ids_prefix", metavar="PREFIX",
                             help="don't remove IDs starting with the given prefix")
_options_parser.add_option_group(_option_group_ids)

_option_group_compatibility = optparse.OptionGroup(_options_parser, "SVG compatibility checks")
_option_group_compatibility.add_option("--error-on-flowtext",
                                       action="store_true", dest="error_on_flowtext", default=False,
                                       help="exit with error if the input SVG uses non-standard flowing text "
                                            "(only warn by default)")
_options_parser.add_option_group(_option_group_compatibility)


def parse_args(args=None, ignore_additional_args=False):
    options, rargs = _options_parser.parse_args(args)

    if rargs:
        if not options.infilename:
            options.infilename = rargs.pop(0)
        if not options.outfilename and rargs:
            options.outfilename = rargs.pop(0)
        if not ignore_additional_args and rargs:
            _options_parser.error("Additional arguments not handled: %r, see --help" % rargs)
    if options.digits < 1:
        _options_parser.error("Number of significant digits has to be larger than zero, see --help")
    if options.cdigits > options.digits:
        options.cdigits = -1
        print("WARNING: The value for '--set-c-precision' should be lower than the value for '--set-precision'. "
              "Number of significant digits for control points reset to default value, see --help", file=sys.stderr)
    if options.indent_type not in ['tab', 'space', 'none']:
        _options_parser.error("Invalid value for --indent, see --help")
    if options.indent_depth < 0:
        _options_parser.error("Value for --nindent should be positive (or zero), see --help")
    if options.infilename and options.outfilename and options.infilename == options.outfilename:
        _options_parser.error("Input filename is the same as output filename")

    return options


# this function was replaced by 'sanitizeOptions()' and is only kept for backwards compatibility
# TODO: delete this at some point or continue to keep it around?
def generateDefaultOptions():
    return sanitizeOptions()


# sanitizes options by updating attributes in a set of defaults options while discarding unknown attributes
def sanitizeOptions(options=None):
    optionsDict = dict((key, getattr(options, key)) for key in dir(options) if not key.startswith('__'))

    sanitizedOptions = _options_parser.get_default_values()
    sanitizedOptions._update_careful(optionsDict)

    return sanitizedOptions


def maybe_gziped_file(filename, mode="r"):
    if os.path.splitext(filename)[1].lower() in (".svgz", ".gz"):
        import gzip
        return gzip.GzipFile(filename, mode)
    return open(filename, mode)


def getInOut(options):
    if options.infilename:
        infile = maybe_gziped_file(options.infilename, "rb")
        # GZ: could catch a raised IOError here and report
    else:
        # GZ: could sniff for gzip compression here
        #
        # open the binary buffer of stdin and let XML parser handle decoding
        try:
            infile = sys.stdin.buffer
        except AttributeError:
            infile = sys.stdin
        # the user probably does not want to manually enter SVG code into the terminal...
        if sys.stdin.isatty():
            _options_parser.error("No input file specified, see --help for detailed usage information")

    if options.outfilename:
        outfile = maybe_gziped_file(options.outfilename, "wb")
    else:
        # open the binary buffer of stdout as the output is already encoded
        try:
            outfile = sys.stdout.buffer
        except AttributeError:
            outfile = sys.stdout
        # redirect informational output to stderr when SVG is output to stdout
        options.stdout = sys.stderr

    return [infile, outfile]


def getReport():
    return (
        '  Number of elements removed: ' + str(_num_elements_removed) + os.linesep +
        '  Number of attributes removed: ' + str(_num_attributes_removed) + os.linesep +
        '  Number of unreferenced IDs removed: ' + str(_num_ids_removed) + os.linesep +
        '  Number of comments removed: ' + str(_num_comments_removed) + os.linesep +
        '  Number of style properties fixed: ' + str(_num_style_properties_fixed) + os.linesep +
        '  Number of raster images embedded: ' + str(_num_rasters_embedded) + os.linesep +
        '  Number of path segments reduced/removed: ' + str(_num_path_segments_removed) + os.linesep +
        '  Number of points removed from polygons: ' + str(_num_points_removed_from_polygon) + os.linesep +
        '  Number of bytes saved in path data: ' + str(_num_bytes_saved_in_path_data) + os.linesep +
        '  Number of bytes saved in colors: ' + str(_num_bytes_saved_in_colors) + os.linesep +
        '  Number of bytes saved in comments: ' + str(_num_bytes_saved_in_comments) + os.linesep +
        '  Number of bytes saved in IDs: ' + str(_num_bytes_saved_in_ids) + os.linesep +
        '  Number of bytes saved in lengths: ' + str(_num_bytes_saved_in_lengths) + os.linesep +
        '  Number of bytes saved in transformations: ' + str(_num_bytes_saved_in_transforms)
    )


def start(options, input, output):
    # sanitize options (take missing attributes from defaults, discard unknown attributes)
    options = sanitizeOptions(options)

    start = time.time()

    # do the work
    in_string = input.read()
    out_string = scourString(in_string, options).encode("UTF-8")
    output.write(out_string)

    # Close input and output files (but do not attempt to close stdin/stdout!)
    if not ((input is sys.stdin) or (hasattr(sys.stdin, 'buffer') and input is sys.stdin.buffer)):
        input.close()
    if not ((output is sys.stdout) or (hasattr(sys.stdout, 'buffer') and output is sys.stdout.buffer)):
        output.close()

    end = time.time()

    # run-time in ms
    duration = int(round((end - start) * 1000.))

    oldsize = len(in_string)
    newsize = len(out_string)
    sizediff = (newsize / oldsize) * 100.

    if not options.quiet:
        print('Scour processed file "{}" in {} ms: {}/{} bytes new/orig -> {:.1f}%'.format(
            input.name,
            duration,
            newsize,
            oldsize,
            sizediff), file=options.ensure_value("stdout", sys.stdout))
        if options.verbose:
            print(getReport(), file=options.ensure_value("stdout", sys.stdout))


def run():
    options = parse_args()
    (input, output) = getInOut(options)
    start(options, input, output)


if __name__ == '__main__':
    run()
