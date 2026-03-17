"""
Node Classes.

* :any:`AnyNode`: a generic tree node with any number of attributes.
* :any:`Node`: a simple tree node with at least a name attribute and any number of additional attributes.
* :any:`NodeMixin`: extends any python class to a tree node.
* :any:`SymlinkNode`: Tree node which references to another tree node.
* :any:`SymlinkNodeMixin`: extends any Python class to a symbolic link to a tree node.
* :any:`LightNodeMixin`: A :any:`NodeMixin` using slots.
"""

from .anynode import AnyNode
from .exceptions import LoopError, TreeError
from .lightnodemixin import LightNodeMixin
from .node import Node
from .nodemixin import NodeMixin
from .symlinknode import SymlinkNode
from .symlinknodemixin import SymlinkNodeMixin

__all__ = [
    "AnyNode",
    "LightNodeMixin",
    "LoopError",
    "Node",
    "NodeMixin",
    "SymlinkNode",
    "SymlinkNodeMixin",
    "TreeError",
]
