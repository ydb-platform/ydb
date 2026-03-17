"""
Tree Iteration.

* :any:`PreOrderIter`: iterate over tree using pre-order strategy (self, children)
* :any:`PostOrderIter`: iterate over tree using post-order strategy (children, self)
* :any:`LevelOrderIter`: iterate over tree using level-order strategy
* :any:`LevelOrderGroupIter`: iterate over tree using level-order strategy returning group for every level
* :any:`ZigZagGroupIter`: iterate over tree using level-order strategy returning group for every level
"""

from .abstractiter import AbstractIter
from .levelordergroupiter import LevelOrderGroupIter
from .levelorderiter import LevelOrderIter
from .postorderiter import PostOrderIter
from .preorderiter import PreOrderIter
from .zigzaggroupiter import ZigZagGroupIter

__all__ = [
    "AbstractIter",
    "LevelOrderGroupIter",
    "LevelOrderIter",
    "PostOrderIter",
    "PreOrderIter",
    "ZigZagGroupIter",
]
