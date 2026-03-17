#!/usr/bin/env python
# -*- coding: utf-8 -*-

#  SVG transformation list parser
#
#  Copyright 2010 Louis Simard
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

""" Small recursive descent parser for SVG transform="" data.


In [1]: from svg_transform import svg_transform_parser

In [3]: svg_transform_parser.parse('translate(50, 50)')
Out[3]: [('translate', [50.0, 50.0])]

In [4]: svg_transform_parser.parse('translate(50)')
Out[4]: [('translate', [50.0])]

In [5]: svg_transform_parser.parse('rotate(36 50,50)')
Out[5]: [('rotate', [36.0, 50.0, 50.0])]

In [6]: svg_transform_parser.parse('rotate(36)')
Out[6]: [('rotate', [36.0])]

In [7]: svg_transform_parser.parse('skewX(20)')
Out[7]: [('skewX', [20.0])]

In [8]: svg_transform_parser.parse('skewY(40)')
Out[8]: [('skewX', [20.0])]

In [9]: svg_transform_parser.parse('scale(2 .5)')
Out[9]: [('scale', [2.0, 0.5])]

In [10]: svg_transform_parser.parse('scale(.5)')
Out[10]: [('scale', [0.5])]

In [11]: svg_transform_parser.parse('matrix(1 0 50 0 1 80)')
Out[11]: [('matrix', [1.0, 0.0, 50.0, 0.0, 1.0, 80.0])]

Multiple transformations are supported:

In [12]: svg_transform_parser.parse('translate(30 -30) rotate(36)')
Out[12]: [('translate', [30.0, -30.0]), ('rotate', [36.0])]
"""
from __future__ import absolute_import

import re
from decimal import Decimal
from functools import partial

from six.moves import range


# Sentinel.
class _EOF(object):

    def __repr__(self):
        return 'EOF'


EOF = _EOF()

lexicon = [
    ('float', r'[-+]?(?:(?:[0-9]*\.[0-9]+)|(?:[0-9]+\.?))(?:[Ee][-+]?[0-9]+)?'),
    ('int', r'[-+]?[0-9]+'),
    ('command', r'(?:matrix|translate|scale|rotate|skew[XY])'),
    ('coordstart', r'\('),
    ('coordend', r'\)'),
]


class Lexer(object):
    """ Break SVG path data into tokens.

    The SVG spec requires that tokens are greedy. This lexer relies on Python's
    regexes defaulting to greediness.

    This style of implementation was inspired by this article:

        http://www.gooli.org/blog/a-simple-lexer-in-python/
    """

    def __init__(self, lexicon):
        self.lexicon = lexicon
        parts = []
        for name, regex in lexicon:
            parts.append('(?P<%s>%s)' % (name, regex))
        self.regex_string = '|'.join(parts)
        self.regex = re.compile(self.regex_string)

    def lex(self, text):
        """ Yield (token_type, str_data) tokens.

        The last token will be (EOF, None) where EOF is the singleton object
        defined in this module.
        """
        for match in self.regex.finditer(text):
            for name, _ in self.lexicon:
                m = match.group(name)
                if m is not None:
                    yield (name, m)
                    break
        yield (EOF, None)


svg_lexer = Lexer(lexicon)


class SVGTransformationParser(object):
    """ Parse SVG transform="" data into a list of commands.

    Each distinct command will take the form of a tuple (type, data). The
    `type` is the character string that defines the type of transformation in the
    transform data, so either of "translate", "rotate", "scale", "matrix",
    "skewX" and "skewY". Data is always a list of numbers contained within the
    transformation's parentheses.

    See the SVG documentation for the interpretation of the individual elements
    for each transformation.

    The main method is `parse(text)`. It can only consume actual strings, not
    filelike objects or iterators.
    """

    def __init__(self, lexer=svg_lexer):
        self.lexer = lexer

        self.command_dispatch = {
            'translate': self.rule_1or2numbers,
            'scale': self.rule_1or2numbers,
            'skewX': self.rule_1number,
            'skewY': self.rule_1number,
            'rotate': self.rule_1or3numbers,
            'matrix': self.rule_6numbers,
        }

#        self.number_tokens = set(['int', 'float'])
        self.number_tokens = list(['int', 'float'])

    def parse(self, text):
        """ Parse a string of SVG transform="" data.
        """
        gen = self.lexer.lex(text)
        next_val_fn = partial(next, *(gen,))

        commands = []
        token = next_val_fn()
        while token[0] is not EOF:
            command, token = self.rule_svg_transform(next_val_fn, token)
            commands.append(command)
        return commands

    def rule_svg_transform(self, next_val_fn, token):
        if token[0] != 'command':
            raise SyntaxError("expecting a transformation type; got %r" % (token,))
        command = token[1]
        rule = self.command_dispatch[command]
        token = next_val_fn()
        if token[0] != 'coordstart':
            raise SyntaxError("expecting '('; got %r" % (token,))
        numbers, token = rule(next_val_fn, token)
        if token[0] != 'coordend':
            raise SyntaxError("expecting ')'; got %r" % (token,))
        token = next_val_fn()
        return (command, numbers), token

    def rule_1or2numbers(self, next_val_fn, token):
        numbers = []
        # 1st number is mandatory
        token = next_val_fn()
        number, token = self.rule_number(next_val_fn, token)
        numbers.append(number)
        # 2nd number is optional
        number, token = self.rule_optional_number(next_val_fn, token)
        if number is not None:
            numbers.append(number)

        return numbers, token

    def rule_1number(self, next_val_fn, token):
        # this number is mandatory
        token = next_val_fn()
        number, token = self.rule_number(next_val_fn, token)
        numbers = [number]
        return numbers, token

    def rule_1or3numbers(self, next_val_fn, token):
        numbers = []
        # 1st number is mandatory
        token = next_val_fn()
        number, token = self.rule_number(next_val_fn, token)
        numbers.append(number)
        # 2nd number is optional
        number, token = self.rule_optional_number(next_val_fn, token)
        if number is not None:
            # but, if the 2nd number is provided, the 3rd is mandatory.
            # we can't have just 2.
            numbers.append(number)

            number, token = self.rule_number(next_val_fn, token)
            numbers.append(number)

        return numbers, token

    def rule_6numbers(self, next_val_fn, token):
        numbers = []
        token = next_val_fn()
        # all numbers are mandatory
        for i in range(6):
            number, token = self.rule_number(next_val_fn, token)
            numbers.append(number)
        return numbers, token

    def rule_number(self, next_val_fn, token):
        if token[0] not in self.number_tokens:
            raise SyntaxError("expecting a number; got %r" % (token,))
        x = Decimal(token[1]) * 1
        token = next_val_fn()
        return x, token

    def rule_optional_number(self, next_val_fn, token):
        if token[0] not in self.number_tokens:
            return None, token
        else:
            x = Decimal(token[1]) * 1
            token = next_val_fn()
            return x, token


svg_transform_parser = SVGTransformationParser()
