#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  yocto-css, an extremely bare minimum CSS parser
#
#  Copyright 2009 Jeff Schiller
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

# In order to resolve Bug 368716 (https://bugs.launchpad.net/scour/+bug/368716)
# scour needed a bare-minimum CSS parser in order to determine if some elements
# were still referenced by CSS properties.

# I looked at css-py (a CSS parser built in Python), but that library
# is about 35k of Python and requires ply to be installed.  I just need
# something very basic to suit scour's needs.

# yocto-css takes a string of CSS and tries to spit out a list of rules
# A rule is an associative array (dictionary) with the following keys:
# - selector: contains the string of the selector (see CSS grammar)
# - properties: contains an associative array of CSS properties for this rule

# TODO: need to build up some unit tests for yocto_css

# stylesheet  : [ CDO | CDC | S | statement ]*;
# statement   : ruleset | at-rule;
# at-rule     : ATKEYWORD S* any* [ block | ';' S* ];
# block       : '{' S* [ any | block | ATKEYWORD S* | ';' S* ]* '}' S*;
# ruleset     : selector? '{' S* declaration? [ ';' S* declaration? ]* '}' S*;
# selector    : any+;
# declaration : property S* ':' S* value;
# property    : IDENT;
# value       : [ any | block | ATKEYWORD S* ]+;
# any         : [ IDENT | NUMBER | PERCENTAGE | DIMENSION | STRING
#               | DELIM | URI | HASH | UNICODE-RANGE | INCLUDES
#               | DASHMATCH | FUNCTION S* any* ')'
#               | '(' S* any* ')' | '[' S* any* ']' ] S*;


def parseCssString(str):
    rules = []
    # first, split on } to get the rule chunks
    chunks = str.split('}')
    for chunk in chunks:
        # second, split on { to get the selector and the list of properties
        bits = chunk.split('{')
        if len(bits) != 2:
            continue
        rule = {}
        rule['selector'] = bits[0].strip()
        # third, split on ; to get the property declarations
        bites = bits[1].strip().split(';')
        if len(bites) < 1:
            continue
        props = {}
        for bite in bites:
            # fourth, split on : to get the property name and value
            nibbles = bite.strip().split(':')
            if len(nibbles) != 2:
                continue
            props[nibbles[0].strip()] = nibbles[1].strip()
        rule['properties'] = props
        rules.append(rule)
    return rules
