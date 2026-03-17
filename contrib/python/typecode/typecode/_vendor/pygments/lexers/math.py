# -*- coding: utf-8 -*-
"""
    pygments.lexers.math
    ~~~~~~~~~~~~~~~~~~~~

    Just export lexers that were contained in this module.

    :copyright: Copyright 2006-2021 by the Pygments team, see AUTHORS.
    :license: BSD, see LICENSE for details.
"""

from typecode._vendor.pygments.lexers.python import NumPyLexer
from typecode._vendor.pygments.lexers.matlab import MatlabLexer, MatlabSessionLexer, \
    OctaveLexer, ScilabLexer
from typecode._vendor.pygments.lexers.julia import JuliaLexer, JuliaConsoleLexer
from typecode._vendor.pygments.lexers.r import RConsoleLexer, SLexer, RdLexer
from typecode._vendor.pygments.lexers.modeling import BugsLexer, JagsLexer, StanLexer
from typecode._vendor.pygments.lexers.idl import IDLLexer
from typecode._vendor.pygments.lexers.algebra import MuPADLexer

__all__ = []
