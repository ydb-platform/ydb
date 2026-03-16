# -*- coding: utf-8 -*-
# flake8: NOQA
from parglare.parser import Parser, Token, pos_to_line_col
from parglare.tables import LALR, SLR, SHIFT, REDUCE, ACCEPT
from parglare.glr import GLRParser
from parglare.grammar import (
    Grammar,
    NonTerminal,
    Terminal,
    RegExRecognizer,
    StringRecognizer,
    EMPTY,
    STOP,
)
from parglare.common import get_collector
from parglare.trees import Node, NodeTerm, NodeNonTerm, visitor
from parglare.exceptions import (
    ParserInitError,
    SyntaxError,
    GrammarError,
    DisambiguationError,
    LoopError,
)

try:
    from importlib.metadata import version
except ModuleNotFoundError:
    from importlib_metadata import version  # type: ignore

__version__ = version("parglare")
