"""Powerful and Lightweight Python Tree Data Structure."""

__version__ = "2.12.1"
__author__ = "c0fec0de"
__author_email__ = "c0fec0de@gmail.com"
__description__ = """Powerful and Lightweight Python Tree Data Structure."""
__url__ = "https://github.com/c0fec0de/anytree"

from . import cachedsearch, util
from .iterators import LevelOrderGroupIter, LevelOrderIter, PostOrderIter, PreOrderIter, ZigZagGroupIter
from .node import AnyNode, LightNodeMixin, LoopError, Node, NodeMixin, SymlinkNode, SymlinkNodeMixin, TreeError
from .render import AbstractStyle, AsciiStyle, ContRoundStyle, ContStyle, DoubleStyle, RenderTree
from .resolver import ChildResolverError, Resolver, ResolverError, RootResolverError
from .search import CountError, find, find_by_attr, findall, findall_by_attr
from .walker import Walker, WalkError

# legacy
LevelGroupOrderIter = LevelOrderGroupIter


__all__ = [
    "AbstractStyle",
    "AnyNode",
    "AsciiStyle",
    "ChildResolverError",
    "ContRoundStyle",
    "ContStyle",
    "CountError",
    "DoubleStyle",
    "LevelGroupOrderIter",
    "LevelOrderGroupIter",
    "LevelOrderIter",
    "LightNodeMixin",
    "LoopError",
    "Node",
    "NodeMixin",
    "PostOrderIter",
    "PreOrderIter",
    "RenderTree",
    "Resolver",
    "ResolverError",
    "RootResolverError",
    "SymlinkNode",
    "SymlinkNodeMixin",
    "TreeError",
    "WalkError",
    "Walker",
    "ZigZagGroupIter",
    "cachedsearch",
    "find",
    "find_by_attr",
    "findall",
    "findall_by_attr",
    "util",
]
