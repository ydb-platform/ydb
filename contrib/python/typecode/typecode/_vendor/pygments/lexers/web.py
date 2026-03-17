# -*- coding: utf-8 -*-
"""
    pygments.lexers.web
    ~~~~~~~~~~~~~~~~~~~

    Just export previously exported lexers.

    :copyright: Copyright 2006-2021 by the Pygments team, see AUTHORS.
    :license: BSD, see LICENSE for details.
"""

from typecode._vendor.pygments.lexers.html import HtmlLexer, DtdLexer, XmlLexer, XsltLexer, \
    HamlLexer, ScamlLexer, JadeLexer
from typecode._vendor.pygments.lexers.css import CssLexer, SassLexer, ScssLexer
from typecode._vendor.pygments.lexers.javascript import JavascriptLexer, LiveScriptLexer, \
    DartLexer, TypeScriptLexer, LassoLexer, ObjectiveJLexer, CoffeeScriptLexer
from typecode._vendor.pygments.lexers.actionscript import ActionScriptLexer, \
    ActionScript3Lexer, MxmlLexer
from typecode._vendor.pygments.lexers.php import PhpLexer
from typecode._vendor.pygments.lexers.webmisc import DuelLexer, XQueryLexer, SlimLexer, QmlLexer
from typecode._vendor.pygments.lexers.data import JsonLexer
JSONLexer = JsonLexer  # for backwards compatibility with Pygments 1.5

__all__ = []
