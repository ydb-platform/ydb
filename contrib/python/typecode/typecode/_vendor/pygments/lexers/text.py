# -*- coding: utf-8 -*-
"""
    pygments.lexers.text
    ~~~~~~~~~~~~~~~~~~~~

    Lexers for non-source code file types.

    :copyright: Copyright 2006-2021 by the Pygments team, see AUTHORS.
    :license: BSD, see LICENSE for details.
"""

from typecode._vendor.pygments.lexers.configs import ApacheConfLexer, NginxConfLexer, \
    SquidConfLexer, LighttpdConfLexer, IniLexer, RegeditLexer, PropertiesLexer
from typecode._vendor.pygments.lexers.console import PyPyLogLexer
from typecode._vendor.pygments.lexers.textedit import VimLexer
from typecode._vendor.pygments.lexers.markup import BBCodeLexer, MoinWikiLexer, RstLexer, \
    TexLexer, GroffLexer
from typecode._vendor.pygments.lexers.installers import DebianControlLexer, SourcesListLexer
from typecode._vendor.pygments.lexers.make import MakefileLexer, BaseMakefileLexer, CMakeLexer
from typecode._vendor.pygments.lexers.haxe import HxmlLexer
from typecode._vendor.pygments.lexers.sgf import SmartGameFormatLexer
from typecode._vendor.pygments.lexers.diff import DiffLexer, DarcsPatchLexer
from typecode._vendor.pygments.lexers.data import YamlLexer
from typecode._vendor.pygments.lexers.textfmts import IrcLogsLexer, GettextLexer, HttpLexer

__all__ = []
