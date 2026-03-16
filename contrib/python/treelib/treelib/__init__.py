# Copyright (C) 2011
# Brett Alistair Kromkamp - brettkromkamp@gmail.com
# Copyright (C) 2012-2025
# Xiaming Chen - chenxm35@gmail.com
# and other contributors.
# All rights reserved.
"""
treelib - Efficient Tree Data Structure for Python

`treelib` is a Python library providing a comprehensive tree data structure implementation.
It features two primary classes: Node and Tree, designed for maximum efficiency and ease of use.

Key Features:
    - O(1) node access and search operations
    - Multiple tree traversal algorithms (DFS, BFS, ZigZag)
    - Rich tree visualization with customizable display formats
    - Flexible tree modification operations (add, remove, move, copy)
    - Data export/import (JSON, dictionary, GraphViz DOT)
    - Subtree operations and advanced filtering
    - Support for custom data payloads on nodes
    - Memory-efficient design with shallow/deep copy options

Architecture:
    - **Tree**: Self-contained hierarchical structure managing nodes and relationships
    - **Node**: Elementary objects storing data and maintaining parent-child links
    - Each tree has exactly one root node (or none if empty)
    - Each non-root node has exactly one parent and zero or more children

Quick Start:
    .. code-block:: python

       from treelib import Tree, Node

       # Create and populate a tree
       tree = Tree()
       tree.create_node("Root", "root")
       tree.create_node("Child A", "a", parent="root")
       tree.create_node("Child B", "b", parent="root")
       tree.create_node("Grandchild", "a1", parent="a")

       # Display the tree
       tree.show()

       # Tree traversal
       for node_id in tree.expand_tree(mode=Tree.DEPTH):
           print(f"Visiting: {tree[node_id].tag}")

       # Export to JSON
       json_data = tree.to_json()

Common Use Cases:
    - File system representations
    - Organizational hierarchies
    - Decision trees and game trees
    - Category/taxonomy structures
    - Menu and navigation systems
    - Abstract syntax trees
    - Family trees and genealogy

Compatibility Note:
    To ensure string compatibility between Python 2.x and 3.x, treelib follows
    Python 3.x string handling conventions. All strings are handled as unicode.

    For Python 2.x users with non-ASCII characters, enable unicode literals:

    .. code-block:: python

       from __future__ import unicode_literals
"""

from .node import Node  # noqa: F401
from .tree import Tree  # noqa: F401
