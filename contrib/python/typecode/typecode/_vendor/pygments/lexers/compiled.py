# -*- coding: utf-8 -*-
"""
    pygments.lexers.compiled
    ~~~~~~~~~~~~~~~~~~~~~~~~

    Just export lexer classes previously contained in this module.

    :copyright: Copyright 2006-2021 by the Pygments team, see AUTHORS.
    :license: BSD, see LICENSE for details.
"""

from typecode._vendor.pygments.lexers.jvm import JavaLexer, ScalaLexer
from typecode._vendor.pygments.lexers.c_cpp import CLexer, CppLexer
from typecode._vendor.pygments.lexers.d import DLexer
from typecode._vendor.pygments.lexers.objective import ObjectiveCLexer, \
    ObjectiveCppLexer, LogosLexer
from typecode._vendor.pygments.lexers.go import GoLexer
from typecode._vendor.pygments.lexers.rust import RustLexer
from typecode._vendor.pygments.lexers.c_like import ECLexer, ValaLexer, CudaLexer
from typecode._vendor.pygments.lexers.pascal import DelphiLexer, Modula2Lexer, AdaLexer
from typecode._vendor.pygments.lexers.business import CobolLexer, CobolFreeformatLexer
from typecode._vendor.pygments.lexers.fortran import FortranLexer
from typecode._vendor.pygments.lexers.prolog import PrologLexer
from typecode._vendor.pygments.lexers.python import CythonLexer
from typecode._vendor.pygments.lexers.graphics import GLShaderLexer
from typecode._vendor.pygments.lexers.ml import OcamlLexer
from typecode._vendor.pygments.lexers.basic import BlitzBasicLexer, BlitzMaxLexer, MonkeyLexer
from typecode._vendor.pygments.lexers.dylan import DylanLexer, DylanLidLexer, DylanConsoleLexer
from typecode._vendor.pygments.lexers.ooc import OocLexer
from typecode._vendor.pygments.lexers.felix import FelixLexer
from typecode._vendor.pygments.lexers.nimrod import NimrodLexer
from typecode._vendor.pygments.lexers.crystal import CrystalLexer

__all__ = []
