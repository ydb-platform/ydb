"""
A static analyzer for Python code.

Beniget provides a static over-approximation of the global and
local definitions inside Python Module/Class/Function.
It can also compute def-use chains from each definition.

Beniget provides three analyse:

- `Ancestors` that maps each node to the list of enclosing nodes;
- `DefUseChains` that maps each node to the list of definition points in that node;
- `UseDefChains` that maps each node to the list of possible definition of that node.
"""
from beniget.version import __version__
from beniget.beniget import (Ancestors, 
                             DefUseChains, 
                             UseDefChains,
                             Def, )

__all__ = ['Ancestors', 'DefUseChains', 'UseDefChains', 'Def', '__version__']