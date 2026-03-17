#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (C) 2011
# Brett Alistair Kromkamp - brettkromkamp@gmail.com
# Copyright (C) 2012-2025
# Xiaming Chen - chenxm35@gmail.com
# and other contributors.
# All rights reserved.
"""
Tree structure in `treelib`.

The :class:`Tree` object defines the tree-like structure based on :class:`Node` objects.
Trees provide hierarchical data organization with efficient operations for traversal,
modification, search, and visualization.

A new tree can be created from scratch without any parameter or a shallow/deep copy of another tree.
When deep=True, a deepcopy operation is performed on feeding tree parameter and more memory
is required to create the tree.

Key Features:
    - O(1) node lookup and access
    - Multiple traversal modes (depth-first, breadth-first, zigzag)
    - Flexible tree modification (add, remove, move, paste)
    - Rich display options with customizable formatting
    - JSON/dict export and import capabilities
    - Subtree operations and filtering
    - Tree metrics and analysis tools
"""
from __future__ import print_function, unicode_literals

try:
    from builtins import str as text
except ImportError:
    from __builtin__ import str as text  # type: ignore

import codecs
import json
import sys
import uuid
from copy import deepcopy
from typing import Any, Callable, List, Optional, Union, cast

from six import iteritems, python_2_unicode_compatible

try:
    from StringIO import StringIO  # type: ignore
except ImportError:
    from io import StringIO

from .exceptions import (
    DuplicatedNodeIdError,
    InvalidLevelNumber,
    LinkPastRootNodeError,
    LoopError,
    MultipleRootError,
    NodeIDAbsentError,
)
from .node import Node

if sys.version_info >= (3, 9):
    StrList = list[str]
    StrLList = list[list[str]]
    NodeList = list[Node]
else:
    StrList = List[str]  # Python 3.8 and earlier
    StrLList = List[List[str]]
    NodeList = List[Node]


__author__ = "chenxm"


@python_2_unicode_compatible
class Tree(object):
    """
    Hierarchical tree data structure.

    A Tree is a collection of Node objects organized in a hierarchical structure
    with exactly one root node and zero or more child nodes. Each node (except root)
    has exactly one parent, but can have multiple children.

    The tree provides efficient operations for:
    - Adding, removing, and moving nodes
    - Traversing the tree in different orders
    - Searching and filtering nodes
    - Displaying tree structure
    - Exporting to various formats

    Attributes:
        root (str): Identifier of the root node, or None if tree is empty.
        nodes (dict): Dictionary mapping node identifiers to Node objects.

    Constants:
        ROOT (int): Tree traversal starting from root level.
        DEPTH (int): Depth-first tree traversal mode.
        WIDTH (int): Breadth-first tree traversal mode.
        ZIGZAG (int): Zigzag tree traversal mode.

    Example:
        Creating and manipulating a tree::

            tree = Tree()

            # Build structure
            tree.create_node("Company", "company")
            tree.create_node("Engineering", "eng", parent="company")
            tree.create_node("Sales", "sales", parent="company")

            # Add team members
            tree.create_node("Alice", "alice", parent="eng")
            tree.create_node("Bob", "bob", parent="eng")
            tree.create_node("Carol", "carol", parent="sales")

            # Display tree
            tree.show()

            # Find specific nodes
            eng_team = tree.children("eng")
            all_employees = [tree[node].tag for node in tree.expand_tree()
                           if tree.level(node) > 1]

            # Tree operations
            tree.move_node("alice", "sales")  # Alice moves to sales
            subtree = tree.subtree("eng")     # Get engineering subtree
    """

    #: ROOT, DEPTH, WIDTH, ZIGZAG constants :
    (ROOT, DEPTH, WIDTH, ZIGZAG) = list(range(4))
    node_class = Node

    def __contains__(self, identifier: str) -> bool:
        """
        Check if a node with the given identifier exists in this tree.

        Implements the 'in' operator for tree membership testing, providing
        a convenient way to check node existence without raising exceptions.

        Args:
            identifier: Node identifier to check for existence.

        Returns:
            bool: True if node exists in tree, False otherwise.

        Example:
            Checking node membership::

                if "user123" in tree:
                    print("User node exists")
                    user = tree["user123"]
                else:
                    print("User node not found")

                # More concise than try/except approach
                exists = "node_id" in tree  # True or False
        """
        return identifier in self.nodes.keys()

    def __init__(
        self,
        tree=None,
        deep: bool = False,
        node_class=None,
        identifier: Optional[str] = None,
    ) -> None:
        """
        Initialize a new tree or copy another tree.

        Creates either an empty tree or a copy of an existing tree. When copying,
        you can choose between shallow copy (references to same nodes) or deep copy
        (completely independent nodes and data).

        Args:
            tree: Existing Tree object to copy from. If None, creates empty tree.
            deep: If True, perform deep copy of all nodes and their data.
                 If False, creates shallow copy sharing node references.
            node_class: Custom Node class to use instead of default Node.
                       Must be subclass of Node.
            identifier: Unique identifier for this tree instance.
                       If None, generates UUID automatically.

        Example:
            Different ways to create trees::

                # Empty tree
                tree1 = Tree()

                # Tree with custom identifier
                tree2 = Tree(identifier="my_tree")

                # Shallow copy of existing tree
                tree3 = Tree(tree1)

                # Deep copy with independent data
                tree4 = Tree(tree1, deep=True)

                # Custom node class
                class MyNode(Node):
                    def __init__(self, tag, identifier=None):
                        super().__init__(tag, identifier)
                        self.custom_attr = "value"

                tree5 = Tree(node_class=MyNode)

        Raises:
            AssertionError: If node_class is not a subclass of Node.
        """
        self._identifier: Optional[str] = None
        self._set_identifier(identifier)

        if node_class:
            assert issubclass(node_class, Node)
            self.node_class = node_class

        #: dictionary, identifier: Node object
        self._nodes = {}

        #: Get or set the identifier of the root. This attribute can be accessed and modified
        #: with ``.`` and ``=`` operator respectively.
        self.root: Optional[str] = None

        if tree is not None:
            self.root = tree.root
            for nid, node in iteritems(tree.nodes):
                new_node = deepcopy(node) if deep else node
                self._nodes[nid] = new_node
                if tree.identifier != self._identifier:
                    new_node.clone_pointers(tree.identifier, self._identifier)

    def _clone(
        self,
        identifier: Optional[str] = None,
        with_tree: bool = False,
        deep: bool = False,
    ):
        """
        Create a clone of this tree instance with optional content copying.

        This method is designed to be overridden by subclasses to enable
        proper cloning of extended tree classes. It provides the foundation
        for subtree and remove_subtree operations while maintaining
        polymorphic behavior.

        Args:
            identifier: Unique identifier for the new tree instance.
                       If None, generates UUID automatically.
            with_tree: If True, copy all nodes from current tree.
                      If False, create empty tree with same class.
            deep: If True and with_tree=True, perform deep copy of node data.
                 If False, create shallow copy sharing node references.

        Returns:
            Tree: New tree instance of the same class as this tree.

        Example:
            Subclassing with custom clone behavior::

                class EnhancedTree(Tree):
                    def __init__(self, metadata=None, **kwargs):
                        super().__init__(**kwargs)
                        self.metadata = metadata or {}

                    def _clone(self, identifier=None, with_tree=False, deep=False):
                        return EnhancedTree(
                            metadata=self.metadata.copy(),
                            identifier=identifier,
                            tree=self if with_tree else None,
                            deep=deep
                        )

                # Custom tree operations preserve metadata
                enhanced = EnhancedTree(metadata={"version": "1.0"})
                subtree = enhanced.subtree("node_id")  # Preserves metadata
        """
        return self.__class__(identifier=identifier, tree=self if with_tree else None, deep=deep)

    @property
    def identifier(self) -> Optional[str]:
        """
        Get the unique identifier of this tree instance.

        Each tree has its own unique identifier used to distinguish it from
        other trees, especially when nodes exist in multiple trees simultaneously.
        This identifier is automatically generated if not provided during creation.

        Returns:
            str or None: The unique identifier of this tree instance.

        Example:
            Using tree identifiers::

                tree1 = Tree(identifier="main_tree")
                tree2 = Tree()  # Auto-generated identifier

                print(tree1.identifier)  # "main_tree"
                print(tree2.identifier)  # UUID string like "abc123..."

                # Used internally for node relationships
                node.predecessor(tree1.identifier)  # Parent in tree1
        """
        return self._identifier

    def _set_identifier(self, nid: Optional[str]) -> None:
        """
        Initialize tree identifier with given value or auto-generate one.

        Private method used during tree creation to set the unique identifier.
        If no identifier is provided, generates a UUID automatically to ensure
        uniqueness across tree instances.

        Args:
            nid: Desired tree identifier, or None to auto-generate.

        Note:
            This is an internal method used during tree initialization.
            The identifier should not be changed after tree creation.
        """
        if nid is None:
            self._identifier = str(uuid.uuid1())
        else:
            self._identifier = nid

    def __getitem__(self, key: str) -> Node:
        """
        Get node by identifier using bracket notation.

        Provides convenient dictionary-style access to nodes. Raises exception
        if node doesn't exist, making it clear when invalid identifiers are used.
        For safer access that returns None instead of raising, use get_node().

        Args:
            key: Node identifier to retrieve.

        Returns:
            Node: The node object with the specified identifier.

        Raises:
            NodeIDAbsentError: If no node with the given identifier exists.

        Example:
            Accessing nodes by identifier::

                # Direct access (raises exception if not found)
                user_node = tree["user123"]
                profile_node = tree["user123_profile"]

                # Use with in operator for safety
                if "user123" in tree:
                    user_node = tree["user123"]
                    print(user_node.tag)
        """
        try:
            return self._nodes[key]
        except KeyError:
            raise NodeIDAbsentError("Node '%s' is not in the tree" % key)

    def __len__(self) -> int:
        """
        Get the total number of nodes in this tree.

        Returns the count of all nodes currently in the tree, including
        the root node. Useful for tree size analysis and iteration bounds.

        Returns:
            int: Total number of nodes in the tree.

        Example:
            Getting tree size::

                tree_size = len(tree)
                print(f"Tree has {tree_size} nodes")

                # Empty tree check
                if len(tree) == 0:
                    print("Tree is empty")

                # Use in comparisons
                if len(tree1) > len(tree2):
                    print("Tree1 is larger")
        """
        return len(self._nodes)

    def __str__(self) -> str:
        """
        Get string representation of the tree structure.

        Returns a formatted tree visualization showing the hierarchical structure
        with default display settings. Useful for debugging, logging, and quick
        inspection of tree contents.

        Returns:
            str: Formatted tree structure as string.

        Example:
            Tree string representation::

                tree = Tree()
                tree.create_node("Root", "root")
                tree.create_node("Child A", "a", parent="root")
                tree.create_node("Child B", "b", parent="root")

                print(str(tree))
                # Output:
                # Root
                # ├── Child A
                # └── Child B

                # Use in logging
                logger.info(f"Current tree structure:\n{tree}")
        """
        self._reader = ""

        def write(line):
            self._reader += line.decode("utf-8") + "\n"

        self.__print_backend(func=write)
        return self._reader

    def __print_backend(
        self,
        nid: Optional[str] = None,
        level: int = ROOT,
        idhidden: bool = True,
        filter: Optional[Callable[[Node], bool]] = None,
        key: Optional[Callable[[Node], Node]] = None,
        reverse: bool = False,
        line_type="ascii-ex",
        data_property=None,
        sorting: bool = True,
        func: Callable[[bytes], None] = print,
    ):
        """
        Another implementation of printing tree using Stack
        Print tree structure in hierarchy style.

        For example:

        .. code-block:: bash

            Root
            |___ C01
            |    |___ C11
            |         |___ C111
            |         |___ C112
            |___ C02
            |___ C03
            |    |___ C31

        A more elegant way to achieve this function using Stack
        structure, for constructing the Nodes Stack push and pop nodes
        with additional level info.

        UPDATE: the @key @reverse is present to sort node at each
        level.
        """
        # Factory for proper get_label() function
        if data_property:
            if idhidden:

                def get_label(node: Node) -> str:
                    return getattr(node.data, data_property)

            else:

                def get_label(node: Node) -> str:
                    return "%s[%s]" % (
                        getattr(node.data, data_property),
                        node.identifier,
                    )

        else:
            if idhidden:

                def get_label(node: Node) -> str:
                    return node.tag

            else:

                def get_label(node: Node) -> str:
                    return "%s[%s]" % (node.tag, node.identifier)

        # legacy ordering
        if sorting and key is None:

            def key(node):
                return node

        # iter with func
        for pre, node in self.__get(nid, level, filter, key, reverse, line_type, sorting):
            label = get_label(node)
            func("{0}{1}".format(pre, label).encode("utf-8"))

    def __get(
        self,
        nid: Optional[str],
        level: int,
        filter_: Optional[Callable[[Node], bool]],
        key: Optional[Callable[[Node], Node]],
        reverse: bool,
        line_type: str,
        sorting: bool,
    ):
        # default filter
        if filter_ is None:

            def filter_(node):
                return True

        # render characters
        dt = {
            "ascii": ("|", "|-- ", "+-- "),
            "ascii-ex": ("\u2502", "\u251c\u2500\u2500 ", "\u2514\u2500\u2500 "),
            "ascii-exr": ("\u2502", "\u251c\u2500\u2500 ", "\u2570\u2500\u2500 "),
            "ascii-em": ("\u2551", "\u2560\u2550\u2550 ", "\u255a\u2550\u2550 "),
            "ascii-emv": ("\u2551", "\u255f\u2500\u2500 ", "\u2559\u2500\u2500 "),
            "ascii-emh": ("\u2502", "\u255e\u2550\u2550 ", "\u2558\u2550\u2550 "),
        }[line_type]

        return self.__get_iter(nid, level, filter_, key, reverse, dt, [], sorting)

    def __get_iter(
        self,
        nid: Optional[str],
        level: int,
        filter_: Callable[[Node], bool],
        key: Optional[Callable[[Node], Node]],
        reverse: bool,
        dt,  # tuple[str, str, str]
        is_last: list,
        sorting: bool,
    ):
        dt_vertical_line, dt_line_box, dt_line_corner = dt

        nid = cast(str, self.root if nid is None else nid)
        node = self[nid]

        if level == self.ROOT:
            yield "", node
        else:
            leading = "".join(
                map(
                    lambda x: dt_vertical_line + " " * 3 if not x else " " * 4,
                    is_last[0:-1],
                )
            )
            lasting = dt_line_corner if is_last[-1] else dt_line_box
            yield leading + lasting, node

        if filter_(node) and node.expanded:
            children = [self[i] for i in node.successors(self._identifier) if filter_(self[i])]
            idxlast = len(children) - 1
            if sorting:
                if key:
                    children.sort(key=key, reverse=reverse)
                elif reverse:
                    children = list(reversed(children))
            level += 1
            for idx, child in enumerate(children):
                is_last.append(idx == idxlast)
                for item in self.__get_iter(child.identifier, level, filter_, key, reverse, dt, is_last, sorting):
                    yield item
                is_last.pop()

    def __update_bpointer(self, nid, parent_id):
        """set self[nid].bpointer"""
        self[nid].set_predecessor(parent_id, self._identifier)

    def __update_fpointer(self, nid: str, child_id: str, mode: int):
        if nid is None:
            return
        else:
            self[nid].update_successors(child_id, mode, tree_id=self._identifier)

    def add_node(self, node: Node, parent: Optional[Union[Node, str]] = None) -> None:
        """
        Add an existing node object to the tree.

        Integrates a pre-created Node instance into the tree structure by
        establishing parent-child relationships and updating internal mappings.
        For creating and adding nodes simultaneously, use create_node() instead.

        Args:
            node: Existing Node instance to add to the tree.
                 Must be an instance of the tree's node_class.
            parent: Parent node (Node object or identifier string), or None
                   to make this node the root. If None, tree must be empty.

        Raises:
            OSError: If node is not an instance of the expected node class.
            DuplicatedNodeIdError: If node identifier already exists in tree.
            MultipleRootError: If parent is None but tree already has a root.
            NodeIDAbsentError: If parent identifier doesn't exist in tree.

        Example:
            Adding pre-created nodes::

                # Create nodes first
                root_node = Node("Company", "company")
                dept_node = Node("Engineering", "eng")

                # Add to tree
                tree.add_node(root_node)  # Root node
                tree.add_node(dept_node, parent="company")  # Child node

                # Adding with Node object as parent
                mgr_node = Node("Manager", "mgr")
                tree.add_node(mgr_node, parent=dept_node)
        """
        if not isinstance(node, self.node_class):
            raise OSError("First parameter must be object of {}".format(self.node_class))

        if node.identifier in self._nodes:
            raise DuplicatedNodeIdError("Can't create node " "with ID '%s'" % node.identifier)

        pid = parent.identifier if isinstance(parent, self.node_class) else parent

        if pid is None:
            if self.root is not None:
                raise MultipleRootError("A tree takes one root merely.")
            else:
                self.root = node.identifier
        elif not self.contains(pid):
            raise NodeIDAbsentError("Parent node '%s' " "is not in the tree" % pid)

        pid = cast(str, pid)
        self._nodes.update({node.identifier: node})
        self.__update_fpointer(pid, node.identifier, self.node_class.ADD)
        self.__update_bpointer(node.identifier, pid)
        node.set_initial_tree_id(cast(str, self._identifier))

    def all_nodes(self) -> NodeList:
        """
        Get all nodes in the tree as a list.

        Returns a list containing all Node objects currently in the tree.
        The order is not guaranteed. For ordered traversal, use expand_tree().
        Useful for operations that need to process all nodes simultaneously.

        Returns:
            list[Node]: List of all Node objects in the tree.

        Example:
            Processing all nodes::

                # Get all nodes
                all_nodes = tree.all_nodes()
                print(f"Total nodes: {len(all_nodes)}")

                # Process each node
                for node in all_nodes:
                    print(f"Node: {node.tag}")

                # Filter nodes by property
                leaf_nodes = [node for node in tree.all_nodes()
                             if node.is_leaf(tree.identifier)]
        """
        return list(self._nodes.values())

    def all_nodes_itr(self) -> Any:
        """
        Get all nodes in the tree as an iterator.

        Returns an iterator over all Node objects in the tree, providing
        memory-efficient access for large trees. Useful when you need to
        process nodes one at a time without loading all into memory.

        Returns:
            Iterator[Node]: Iterator over all Node objects in the tree.

        Example:
            Memory-efficient node processing::

                # Iterate without loading all nodes
                for node in tree.all_nodes_itr():
                    if node.tag.startswith("temp_"):
                        process_temporary_node(node)

                # Use with filter operations
                filtered = filter(lambda n: n.data is not None,
                                tree.all_nodes_itr())

        Note:
            Added by William Rusnack for memory efficiency.
        """
        return self._nodes.values()

    def ancestor(self, nid, level=None):
        """
        Get the ancestor node at a specific level above the given node.

        Traverses up the tree hierarchy from the specified node to find an
        ancestor at the desired level. If no level is specified, returns
        the immediate parent. Useful for navigating tree hierarchies.

        Args:
            nid: Identifier of the node to start from.
            level: Target level of the ancestor (0 = root). If None,
                  returns immediate parent.

        Returns:
            str or None: Identifier of the ancestor node, or None if not found.

        Raises:
            NodeIDAbsentError: If nid doesn't exist in the tree.
            InvalidLevelNumber: If level is invalid (>= descendant level).

        Example:
            Finding ancestors::

                # Get immediate parent
                parent_id = tree.ancestor("grandchild")

                # Get ancestor at specific level
                root_id = tree.ancestor("grandchild", level=0)  # Root
                grandparent_id = tree.ancestor("grandchild", level=1)

                # Use in hierarchy navigation
                if tree.ancestor("node", level=0) == "root":
                    print("Node is in main hierarchy")
        """
        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        descendant = self[nid]
        ascendant = self[nid]._predecessor[self._identifier]
        ascendant_level = self.level(ascendant)

        if level is None:
            return ascendant
        elif nid == self.root:
            return self[nid]
        elif level >= self.level(descendant.identifier):
            raise InvalidLevelNumber(
                "Descendant level (level %s) must be greater \
                                      than its ancestor's level (level %s)"
                % (str(self.level(descendant.identifier)), level)
            )

        while ascendant is not None:
            if ascendant_level == level:
                return self[ascendant]
            else:
                descendant = ascendant
                ascendant = self[descendant]._predecessor[self._identifier]
                ascendant_level = self.level(ascendant)
        return None

    def children(self, nid: str) -> NodeList:
        """
        Get all direct children of the specified node.

        Returns a list of Node objects that are immediate children of the
        given node. The order follows the insertion order unless the tree
        has been sorted. Returns empty list if node has no children.

        Args:
            nid: Identifier of the parent node.

        Returns:
            list[Node]: List of child Node objects. Empty if no children.

        Raises:
            NodeIDAbsentError: If nid doesn't exist in the tree.

        Example:
            Working with child nodes::

                # Get all children
                child_nodes = tree.children("parent_id")
                print(f"Parent has {len(child_nodes)} children")

                # Process each child
                for child in tree.children("parent_id"):
                    print(f"Child: {child.tag}")

                # Check if node has children
                if tree.children("node_id"):
                    print("Node has children")
                else:
                    print("Node is a leaf")
        """
        return [self[i] for i in self.is_branch(nid)]

    def contains(self, nid):
        """
        Check if the tree contains a node with the given identifier.

        Determines whether a node with the specified identifier exists
        in this tree. Equivalent to using the 'in' operator but provided
        as an explicit method for clarity and consistency.

        Args:
            nid: Node identifier to check for existence.

        Returns:
            bool: True if node exists in tree, False otherwise.

        Example:
            Checking node existence::

                # Explicit method call
                if tree.contains("user123"):
                    print("User node exists")

                # Equivalent using 'in' operator
                if "user123" in tree:
                    print("User node exists")

                # Use before operations
                if tree.contains("node_id"):
                    tree.move_node("node_id", "new_parent")
        """
        return True if nid in self._nodes else False

    def create_node(
        self,
        tag: Optional[str] = None,
        identifier: Optional[str] = None,
        parent: Optional[Union[Node, str]] = None,
        data: Any = None,
    ) -> Node:
        """
        Create and add a new node to the tree.

        This is the primary method for building tree structures. Creates a new node
        with the specified properties and attaches it to the given parent. If no
        parent is specified, the node becomes the root (only allowed if tree is empty).

        Args:
            tag: Human-readable label for display. If None, uses identifier.
            identifier: Unique ID for the node. If None, generates UUID automatically.
            parent: Parent node identifier, Node object, or None for root.
                   Must be None if tree is empty, must exist if tree has nodes.
            data: Optional user data to associate with this node.

        Returns:
            Node: The newly created Node object.

        Raises:
            DuplicatedNodeIdError: If identifier already exists in the tree.
            MultipleRootError: If parent is None but tree already has a root.
            NodeIDAbsentError: If parent identifier doesn't exist in the tree.

        Example:
            Building a family tree::

                tree = Tree()

                # Create root (no parent)
                tree.create_node("Grandpa", "grandpa")

                # Add children
                tree.create_node("Dad", "dad", parent="grandpa")
                tree.create_node("Uncle", "uncle", parent="grandpa")

                # Add grandchildren
                tree.create_node("Me", "me", parent="dad")
                tree.create_node("Sister", "sister", parent="dad")

                # Add node with custom data
                tree.create_node("Baby", "baby", parent="me",
                               data={"age": 1, "cute": True})
        """
        node = self.node_class(tag=tag, identifier=identifier, data=data)
        self.add_node(node, parent)
        return node

    def depth(self, node: Optional[Node] = None) -> int:
        """
        Get the maximum depth of the tree or the level of a specific node.

        When called without arguments, returns the maximum depth of the entire
        tree (distance from root to deepest leaf). When called with a node,
        returns the level of that specific node (distance from root).

        Args:
            node: Node object or identifier string. If None, returns tree depth.
                 If provided, returns the level of this specific node.

        Returns:
            int: Tree depth (max level) or node level. Root is at level 0.

        Raises:
            NodeIDAbsentError: If specified node doesn't exist in the tree.

        Example:
            Measuring tree dimensions::

                # Get overall tree depth
                max_depth = tree.depth()
                print(f"Tree depth: {max_depth}")

                # Get specific node level
                node_level = tree.depth("some_node")
                node_level2 = tree.depth(tree["some_node"])  # Same result

                # Use for tree analysis
                if tree.depth() > 5:
                    print("Tree is quite deep")
        """
        ret = 0
        if node is None:
            # Get maximum level of this tree
            leaves = self.leaves()
            for leave in leaves:
                level = self.level(leave.identifier)
                ret = level if level >= ret else ret
        else:
            # Get level of the given node
            if not isinstance(node, self.node_class):
                nid = node
            else:
                nid = node.identifier
            if not self.contains(nid):
                raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)
            ret = self.level(nid)
        return ret

    def expand_tree(
        self,
        nid: Optional[str] = None,
        mode: int = DEPTH,
        filter: Optional[Callable[[Node], bool]] = None,
        key: Optional[Callable[[Node], Node]] = None,
        reverse: bool = False,
        sorting: bool = True,
    ):
        """
        Traverse the tree and yield node identifiers in specified order.

        This is the primary method for tree iteration, providing flexible traversal
        with multiple algorithms, filtering, and sorting options. Essential for most
        tree processing operations.

        Args:
            nid: Starting node identifier. If None, starts from tree root.
                Must exist in the tree if specified.
            mode: Traversal algorithm to use:
                 Tree.DEPTH (0) - Depth-first search (default)
                 Tree.WIDTH (1) - Breadth-first search
                 Tree.ZIGZAG (2) - ZigZag traversal (alternating levels)
            filter: Optional function to filter nodes during traversal.
                   Takes Node object, returns bool. If False, node and its
                   entire subtree are skipped.
            key: Sorting function for nodes at each level. Takes Node object,
                returns comparison key. If None and sorting=True, sorts by node.
            reverse: If True, reverse the sorting order at each level.
            sorting: If True, sort nodes at each level using key function.
                    If False, preserve insertion order (key/reverse ignored).

        Yields:
            str: Node identifiers in traversal order.

        Raises:
            NodeIDAbsentError: If nid doesn't exist in the tree.
            ValueError: If mode is not a valid traversal constant.

        Example:
            Different traversal patterns::

                tree = Tree()
                tree.create_node("A", "a")
                tree.create_node("B", "b", parent="a")
                tree.create_node("C", "c", parent="a")
                tree.create_node("D", "d", parent="b")
                tree.create_node("E", "e", parent="c")

                # Depth-first (default): A, B, D, C, E
                for node_id in tree.expand_tree():
                    print(f"DFS: {tree[node_id].tag}")

                # Breadth-first: A, B, C, D, E
                for node_id in tree.expand_tree(mode=Tree.WIDTH):
                    print(f"BFS: {tree[node_id].tag}")

                # ZigZag traversal
                for node_id in tree.expand_tree(mode=Tree.ZIGZAG):
                    print(f"ZigZag: {tree[node_id].tag}")

                # Filtered traversal (only nodes with vowels)
                vowel_filter = lambda node: any(v in node.tag.lower() for v in 'aeiou')
                for node_id in tree.expand_tree(filter=vowel_filter):
                    print(f"Vowels: {tree[node_id].tag}")

                # Sorted by tag, reversed
                for node_id in tree.expand_tree(key=lambda x: x.tag, reverse=True):
                    print(f"Sorted: {tree[node_id].tag}")

                # From specific subtree
                for node_id in tree.expand_tree(nid="b"):
                    print(f"Subtree: {tree[node_id].tag}")

        Common Usage Patterns::

            # Process all nodes
            for node_id in tree.expand_tree():
                process_node(tree[node_id])

            # Get all node tags
            tags = [tree[nid].tag for nid in tree.expand_tree()]

            # Find specific nodes
            matching_nodes = [nid for nid in tree.expand_tree()
                             if tree[nid].tag.startswith("prefix")]

            # Level-order processing
            for node_id in tree.expand_tree(mode=Tree.WIDTH):
                level = tree.level(node_id)
                print(f"Level {level}: {tree[node_id].tag}")

        Performance:
            - Time complexity: O(n) where n is number of visited nodes
            - Memory: O(h) where h is tree height (for traversal stack)
            - Generator-based: memory efficient for large trees

        Note:
            This is a generator function - it yields results lazily. Perfect for
            large trees where you might not need to visit all nodes, or when
            memory efficiency is important.
        """
        nid = cast(str, self.root if nid is None else nid)
        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        filter = (lambda x: True) if (filter is None) else filter
        if filter(self[nid]):
            yield nid
            queue = [self[i] for i in self[nid].successors(self._identifier) if filter(self[i])]
            if mode in [self.DEPTH, self.WIDTH]:
                if sorting:
                    queue.sort(key=key, reverse=reverse)
                while queue:
                    yield queue[0].identifier
                    expansion = [self[i] for i in queue[0].successors(self._identifier) if filter(self[i])]
                    if sorting:
                        expansion.sort(key=key, reverse=reverse)
                    if mode is self.DEPTH:
                        queue = expansion + queue[1:]  # depth-first
                    elif mode is self.WIDTH:
                        queue = queue[1:] + expansion  # width-first

            elif mode is self.ZIGZAG:
                # Suggested by Ilya Kuprik (ilya-spy@ynadex.ru).
                stack_fw: NodeList = []
                queue.reverse()
                stack = stack_bw = queue
                direction = False
                while stack:
                    expansion = [self[i] for i in stack[0].successors(self._identifier) if filter(self[i])]
                    yield stack.pop(0).identifier
                    if direction:
                        expansion.reverse()
                        stack_bw = expansion + stack_bw
                    else:
                        stack_fw = expansion + stack_fw
                    if not stack:
                        direction = not direction
                        stack = stack_fw if direction else stack_bw

            else:
                raise ValueError("Traversal mode '{}' is not supported".format(mode))

    def filter_nodes(self, func: Callable[[Node], bool]):
        """
        Filter all nodes in the tree using a custom function.

        Applies the given function to every node and returns an iterator over
        nodes where the function returns True. Useful for finding specific
        types of nodes or nodes with certain properties.

        Args:
            func: Function that takes a Node object and returns bool.
                 True means include the node in results.

        Returns:
            Iterator[Node]: Iterator over nodes where func returns True.

        Example:
            Filtering nodes by criteria::

                # Find all leaf nodes
                leaves = list(tree.filter_nodes(lambda n: n.is_leaf(tree.identifier)))

                # Find nodes with specific data
                important_nodes = list(tree.filter_nodes(
                    lambda n: hasattr(n.data, 'priority') and n.data.priority == 'high'
                ))

                # Find nodes by tag pattern
                temp_nodes = list(tree.filter_nodes(
                    lambda n: n.tag.startswith('temp_')
                ))

                # Memory efficient processing
                for node in tree.filter_nodes(lambda n: n.data is not None):
                    process_node_with_data(node)

        Note:
            Added by William Rusnack for flexible node filtering.
        """
        return filter(func, self.all_nodes_itr())

    def get_node(self, nid: Optional[str]) -> Optional[Node]:
        """
        Safely get a node by identifier without raising exceptions.

        Provides null-safe access to nodes, returning None if the node
        doesn't exist instead of raising an exception. Preferred over
        bracket notation when node existence is uncertain.

        Args:
            nid: Node identifier to retrieve, or None.

        Returns:
            Node or None: The node object if found, None otherwise.

        Example:
            Safe node access::

                # Safe access (no exception)
                node = tree.get_node("might_not_exist")
                if node is not None:
                    print(f"Found: {node.tag}")
                else:
                    print("Node not found")

                # Compare with bracket notation
                try:
                    node = tree["might_not_exist"]  # Raises exception
                except NodeIDAbsentError:
                    node = None

                # Use in conditional logic
                user_node = tree.get_node("user123")
                if user_node and user_node.data.active:
                    process_active_user(user_node)
        """
        if nid is None or not self.contains(nid):
            return None
        return self._nodes[nid]

    def is_branch(self, nid):
        """
        Get the list of child node identifiers for the specified node.

        Returns the identifiers of all direct children of the given node.
        Despite the name suggesting a boolean, this method actually returns
        the list of child identifiers. Use children() for Node objects.

        Args:
            nid: Identifier of the parent node. Cannot be None.

        Returns:
            list[str]: List of child node identifiers. Empty if no children.

        Raises:
            OSError: If nid is None.
            NodeIDAbsentError: If nid doesn't exist in the tree.

        Example:
            Getting child identifiers::

                # Get child IDs
                child_ids = tree.is_branch("parent_id")
                for child_id in child_ids:
                    print(f"Child ID: {child_id}")

                # Check if node has children
                if tree.is_branch("node_id"):
                    print("Node has children")

                # Use with node objects
                child_nodes = [tree[child_id] for child_id in tree.is_branch("parent")]
        """
        if nid is None:
            raise OSError("First parameter can't be None")
        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        try:
            fpointer = self[nid].successors(self._identifier)
        except KeyError:
            fpointer = []
        return fpointer

    def leaves(self, nid: Optional[str] = None) -> NodeList:
        """
        Get all leaf nodes in the tree or subtree.

        Returns all nodes that have no children. If a starting node is specified,
        only considers leaves within that subtree. Leaf nodes are terminal nodes
        in the tree structure and often represent end points in hierarchies.

        Args:
            nid: Root of subtree to search. If None, searches entire tree.

        Returns:
            list[Node]: List of all leaf Node objects in the specified scope.

        Example:
            Finding leaf nodes::

                # Get all leaves in tree
                all_leaves = tree.leaves()
                print(f"Tree has {len(all_leaves)} leaf nodes")

                # Get leaves in specific subtree
                subtree_leaves = tree.leaves("department_root")

                # Process leaf nodes
                for leaf in tree.leaves():
                    print(f"Leaf: {leaf.tag}")

                # Find deepest nodes
                max_level = max(tree.level(leaf.identifier) for leaf in tree.leaves())
                deepest_leaves = [leaf for leaf in tree.leaves()
                                if tree.level(leaf.identifier) == max_level]
        """
        leaves = []
        if nid is None:
            for node in self._nodes.values():
                if node.is_leaf(self._identifier):
                    leaves.append(node)
        else:
            for node in self.expand_tree(nid):
                if self[node].is_leaf(self._identifier):
                    leaves.append(self[node])
        return leaves

    def level(self, nid, filter=None):
        """
        Get the level (depth) of a node in the tree hierarchy.

        Returns the distance from the root to the specified node, where
        the root is at level 0. Optionally filters the path calculation
        by excluding certain nodes.

        Args:
            nid: Identifier of the node to get level for.
            filter: Optional function to filter nodes during path calculation.
                   Takes Node object, returns bool. Filtered nodes are excluded.

        Returns:
            int: Level of the node (0 for root, 1 for root's children, etc.)

        Example:
            Getting node levels::

                # Basic level calculation
                root_level = tree.level("root")        # 0
                child_level = tree.level("child")      # 1
                grandchild_level = tree.level("gc")    # 2

                # Use for tree analysis
                max_depth = max(tree.level(node.identifier)
                              for node in tree.all_nodes())

                # Filtered level calculation
                level = tree.level("node", filter=lambda n: n.tag != "skip")
        """
        return len([n for n in self.rsearch(nid, filter)]) - 1

    def link_past_node(self, nid: str):
        """
        Remove a node while preserving connections between its parent and children.

        Deletes the specified node but maintains the tree structure by directly
        connecting the node's parent to all of its children. This operation
        effectively "links past" the node, removing it from the hierarchy
        without breaking the tree connections.

        Args:
            nid: Identifier of the node to link past and remove.

        Raises:
            NodeIDAbsentError: If nid doesn't exist in the tree.
            LinkPastRootNodeError: If attempting to link past the root node.

        Example:
            Linking past nodes::

                # Before: Root -> Manager -> Employee1, Employee2
                tree.link_past_node("Manager")
                # After: Root -> Employee1, Employee2

                # Useful for flattening hierarchies
                middle_managers = ["mgr1", "mgr2", "mgr3"]
                for mgr in middle_managers:
                    if tree.contains(mgr):
                        tree.link_past_node(mgr)

                # Cannot link past root
                try:
                    tree.link_past_node(tree.root)
                except LinkPastRootNodeError:
                    print("Cannot link past root node")
        """
        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)
        if self.root == nid:
            raise LinkPastRootNodeError("Cannot link past the root node, " "delete it with remove_node()")
        # Get the parent of the node we are linking past
        parent = self[self[nid].predecessor(self._identifier)]
        # Set the children of the node to the parent
        for child in self[nid].successors(self._identifier):
            self[child].set_predecessor(parent.identifier, self._identifier)
        # Link the children to the parent
        for id_ in self[nid].successors(self._identifier) or []:
            parent.update_successors(id_, tree_id=self._identifier)
        # Delete the node
        parent.update_successors(nid, mode=parent.DELETE, tree_id=self._identifier)
        del self._nodes[nid]

    def move_node(self, source, destination):
        """
        Move a node from its current parent to a new parent.

        Changes the parent-child relationship by moving the source node
        (and its entire subtree) from its current parent to the specified
        destination parent. This operation preserves the subtree structure
        while reorganizing the tree hierarchy.

        Args:
            source: Identifier of the node to move.
            destination: Identifier of the new parent node.

        Raises:
            NodeIDAbsentError: If source or destination node doesn't exist.
            LoopError: If moving would create a circular reference
                      (destination is a descendant of source).

        Example:
            Reorganizing tree structure::

                # Move employee between departments
                tree.move_node("employee123", "sales_dept")

                # Move entire department
                tree.move_node("engineering_dept", "new_division")

                # Prevent circular moves (this would raise LoopError)
                try:
                    tree.move_node("parent", "child_of_parent")
                except LoopError:
                    print("Cannot create circular reference")

                # Bulk reorganization
                employees = ["emp1", "emp2", "emp3"]
                for emp in employees:
                    tree.move_node(emp, "new_manager")
        """
        if not self.contains(source) or not self.contains(destination):
            raise NodeIDAbsentError
        elif self.is_ancestor(source, destination):
            raise LoopError

        parent = self[source].predecessor(self._identifier)
        self.__update_fpointer(parent, source, self.node_class.DELETE)
        self.__update_fpointer(destination, source, self.node_class.ADD)
        self.__update_bpointer(source, destination)

    def is_ancestor(self, ancestor, grandchild):
        """
        Check if one node is an ancestor of another node.

        Determines whether the ancestor node appears in the path from
        the grandchild node to the root. An ancestor is any node that
        appears above another node in the tree hierarchy.

        Args:
            ancestor: Identifier of the potential ancestor node.
            grandchild: Identifier of the potential descendant node.

        Returns:
            bool: True if ancestor is indeed an ancestor of grandchild,
                 False otherwise.

        Example:
            Checking ancestry relationships::

                # Direct parent-child
                is_parent = tree.is_ancestor("parent", "child")  # True

                # Multi-level ancestry
                is_grandparent = tree.is_ancestor("grandparent", "grandchild")  # True

                # Not related
                is_ancestor = tree.is_ancestor("sibling1", "sibling2")  # False

                # Prevent circular moves
                if not tree.is_ancestor("node_to_move", "new_parent"):
                    tree.move_node("node_to_move", "new_parent")
                else:
                    print("Cannot create circular reference")
        """
        parent = self[grandchild].predecessor(self._identifier)
        child = grandchild
        while parent is not None:
            if parent == ancestor:
                return True
            else:
                child = self[child].predecessor(self._identifier)
                parent = self[child].predecessor(self._identifier)
        return False

    @property
    def nodes(self):
        """
        Get the internal dictionary mapping node identifiers to Node objects.

        Returns the underlying dictionary that stores all nodes in the tree,
        mapping from node identifiers to Node instances. Useful for direct
        access to the node collection and bulk operations.

        Returns:
            dict[str, Node]: Dictionary mapping node IDs to Node objects.

        Example:
            Working with the nodes dictionary::

                # Get all node identifiers
                all_ids = list(tree.nodes.keys())

                # Get all node objects
                all_nodes = list(tree.nodes.values())

                # Direct access to nodes dict
                nodes_dict = tree.nodes
                for node_id, node in nodes_dict.items():
                    print(f"ID: {node_id}, Tag: {node.tag}")

                # Bulk operations
                total_nodes = len(tree.nodes)
                has_node = "some_id" in tree.nodes
        """
        return self._nodes

    def parent(self, nid: str) -> Optional[Node]:
        """
        Get the parent Node object of the specified node.

        Returns the immediate parent of the given node, or None if the node
        is the root or has no parent. Provides object-based access to the
        parent, unlike ancestor() which returns identifiers.

        Args:
            nid: Identifier of the node whose parent to retrieve.

        Returns:
            Node or None: Parent Node object, or None if node is root.

        Raises:
            NodeIDAbsentError: If nid doesn't exist in the tree.

        Example:
            Working with parent relationships::

                # Get parent node
                parent_node = tree.parent("child_id")
                if parent_node:
                    print(f"Parent: {parent_node.tag}")
                else:
                    print("Node is root or orphaned")

                # Navigate up the hierarchy
                current = "leaf_node"
                while tree.parent(current):
                    current = tree.parent(current).identifier
                    print(f"Ancestor: {tree[current].tag}")

                # Check parent properties
                parent = tree.parent("employee")
                if parent and hasattr(parent.data, 'department'):
                    print(f"Department: {parent.data.department}")
        """
        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        pid = self[nid].predecessor(self._identifier)
        if pid is None or not self.contains(pid):
            return None

        return self[pid]

    def merge(self, nid: str, new_tree, deep: bool = False):
        """Patch @new_tree on current tree by pasting new_tree root children on current tree @nid node.

        Consider the following tree:
        >>> current.show()
        root
        ├── A
        └── B
        >>> new_tree.show()
        root2
        ├── C
        └── D
            └── D1
        Merging new_tree on B node:
        >>>current.merge('B', new_tree)
        >>>current.show()
        root
        ├── A
        └── B
            ├── C
            └── D
                └── D1

        Note: if current tree is empty and nid is None, the new_tree root will be used as root on current tree. In all
        other cases new_tree root is not pasted.
        """
        if new_tree.root is None:
            return

        if nid is None:
            if self.root is None:
                new_tree_root = new_tree[new_tree.root]
                self.add_node(new_tree_root)
                nid = new_tree.root
            else:
                raise ValueError('Must define "nid" under which new tree is merged.')
        for child in new_tree.children(new_tree.root):
            self.paste(nid=nid, new_tree=new_tree.subtree(child.identifier), deep=deep)

    def paste(self, nid: str, new_tree, deep: bool = False):
        """
        Paste a @new_tree to the original one by linking the root
        of new tree to given node (nid).

        Update: add @deep copy of pasted tree.
        """
        assert isinstance(new_tree, Tree)

        if new_tree.root is None:
            return

        if nid is None:
            raise ValueError('Must define "nid" under which new tree is pasted.')

        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        set_joint = set(new_tree._nodes) & set(self._nodes)  # joint keys
        if set_joint:
            raise ValueError("Duplicated nodes %s exists." % list(map(text, set_joint)))

        for cid, node in iteritems(new_tree.nodes):
            if deep:
                node = deepcopy(new_tree[node])
            self._nodes.update({cid: node})
            node.clone_pointers(new_tree.identifier, self._identifier)

        self.__update_bpointer(new_tree.root, nid)
        self.__update_fpointer(nid, new_tree.root, self.node_class.ADD)

    def paths_to_leaves(self) -> StrLList:
        """
        Use this function to get the identifiers allowing to go from the root
        nodes to each leaf.

        :return: a list of list of identifiers, root being not omitted.

        For example:

        .. code-block:: python

            Harry
            |___ Bill
            |___ Jane
            |    |___ Diane
            |         |___ George
            |              |___ Jill
            |         |___ Mary
            |    |___ Mark

        Expected result:

        .. code-block:: python

            [['harry', 'jane', 'diane', 'mary'],
             ['harry', 'jane', 'mark'],
             ['harry', 'jane', 'diane', 'george', 'jill'],
             ['harry', 'bill']]

        """
        res = []

        for leaf in self.leaves():
            res.append([nid for nid in self.rsearch(leaf.identifier)][::-1])

        return res

    def remove_node(self, identifier: str) -> int:
        """Remove a node indicated by 'identifier' with all its successors.
        Return the number of removed nodes.
        """
        if not self.contains(identifier):
            raise NodeIDAbsentError("Node '%s' " "is not in the tree" % identifier)

        parent = self[identifier].predecessor(self._identifier)

        # Remove node and its children
        removed = list(self.expand_tree(identifier))

        for id_ in removed:
            if id_ == self.root:
                self.root = None
            self.__update_bpointer(id_, None)
            for cid in self[id_].successors(self._identifier) or []:
                self.__update_fpointer(id_, cid, self.node_class.DELETE)

        # Update parent info
        self.__update_fpointer(parent, identifier, self.node_class.DELETE)
        self.__update_bpointer(identifier, None)

        for id_ in removed:
            self.nodes.pop(id_)
        return len(removed)

    def remove_subtree(self, nid: str, identifier: Optional[str] = None):
        """
        Get a subtree with ``nid`` being the root. If nid is None, an
        empty tree is returned.

        For the original tree, this method is similar to
        `remove_node(self,nid)`, because given node and its children
        are removed from the original tree in both methods.
        For the returned value and performance, these two methods are
        different:

            * `remove_node` returns the number of deleted nodes;
            * `remove_subtree` returns a subtree of deleted nodes;

        You are always suggested to use `remove_node` if your only to
        delete nodes from a tree, as the other one need memory
        allocation to store the new tree.

        :return: a :class:`Tree` object.
        """
        st = self._clone(identifier)
        if nid is None:
            return st

        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)
        st.root = nid

        # in original tree, the removed nid will be unreferenced from its
        # parents children
        parent = self[nid].predecessor(self._identifier)

        removed = list(self.expand_tree(nid))
        for id_ in removed:
            if id_ == self.root:
                self.root = None
            st._nodes.update({id_: self._nodes.pop(id_)})
            st[id_].clone_pointers(cast(str, self._identifier), cast(str, st.identifier))
            st[id_].reset_pointers(self._identifier)
            if id_ == nid:
                st[id_].set_predecessor(None, st.identifier)
        self.__update_fpointer(parent, nid, self.node_class.DELETE)
        return st

    def rsearch(self, nid: str, filter: Optional[Callable[[Node], bool]] = None):
        """
        Traverse the tree branch along the branch from nid to its
        ancestors (until root).

        :param filter: the function of one variable to act on the :class:`Node` object.
        """
        if nid is None:
            return

        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        filter = (lambda x: True) if (filter is None) else filter

        current = nid
        while current is not None:
            if filter(self[current]):
                yield current
            # subtree() hasn't update the bpointer
            current = self[current].predecessor(self._identifier) if self.root != current else None

    def save2file(
        self,
        filename: str,
        nid: Optional[str] = None,
        level: int = ROOT,
        idhidden: bool = True,
        filter: Optional[Callable[[Node], bool]] = None,
        key: Optional[Callable[[Node], Node]] = None,
        reverse: bool = False,
        line_type: str = "ascii-ex",
        data_property: Optional[str] = None,
        sorting: bool = True,
    ):
        """
        Save the tree into file for offline analysis.
        """

        def _write_line(line, f):
            f.write(line + b"\n")

        def handler(x):
            return _write_line(x, open(filename, "ab"))

        self.__print_backend(
            nid,
            level,
            idhidden,
            filter,
            key,
            reverse,
            line_type,
            data_property,
            sorting,
            func=handler,
        )

    def show(
        self,
        nid: Optional[str] = None,
        level: int = ROOT,
        idhidden: bool = True,
        filter: Optional[Callable[[Node], bool]] = None,
        key: Optional[Callable[[Node], Node]] = None,
        reverse: bool = False,
        line_type: str = "ascii-ex",
        data_property: Optional[str] = None,
        stdout: bool = True,
        sorting: bool = True,
    ):
        """
        Display the tree structure in a beautiful hierarchical format.

        This method provides rich visualization options for tree structures, allowing
        customization of appearance, content filtering, and display properties. Perfect
        for debugging, documentation, and user interfaces.

        Args:
            nid: Starting node identifier. If None, starts from root.
            level: Starting level (0 for root). Mainly for internal use.
            idhidden: If True, hide node identifiers in output (cleaner display).
                     If False, show both tag and identifier as "tag[id]".
            filter: Function to selectively display nodes. Takes Node object,
                   returns bool. Filtered nodes and their subtrees are hidden.
            key: Sorting function for nodes at each level. Takes Node object,
                returns comparison key. If None and sorting=True, sorts by Node.
            reverse: If True, reverse the sorting order at each level.
            line_type: Visual style for tree connections. Options:
                      'ascii' - Simple |-- style
                      'ascii-ex' - Unicode ├── style (default)
                      'ascii-exr' - Unicode with rounded corners
                      'ascii-em' - Double-line Unicode ╠══ style
                      'ascii-emv' - Mixed vertical Unicode style
                      'ascii-emh' - Mixed horizontal Unicode style
            data_property: If specified, display this property from node.data
                          instead of node.tag. Useful for rich data objects.
            stdout: If True, print to console. If False, return as string.
            sorting: If True, sort nodes at each level. If False, preserve
                    insertion order (key and reverse are ignored).

        Returns:
            str or None: If stdout=False, returns formatted string.
                        If stdout=True, prints to console and returns None.

        Example:
            Different display styles::

                tree = Tree()
                tree.create_node("Root", "root")
                tree.create_node("Child A", "a", parent="root")
                tree.create_node("Child B", "b", parent="root")

                # Basic display
                tree.show()

                # Fancy style with IDs
                tree.show(idhidden=False, line_type="ascii-em")

                # Filtered display (only nodes starting with 'C')
                tree.show(filter=lambda x: x.tag.startswith('C'))

                # Sorted by tag, reversed
                tree.show(key=lambda x: x.tag, reverse=True)

                # Show custom data property
                tree.show(data_property="name")  # If node.data.name exists

                # Return as string instead of printing
                tree_str = tree.show(stdout=False)

        Output styles::

            ascii:        Root
                         |-- Child A
                         |-- Child B

            ascii-ex:     Root
                         ├── Child A
                         └── Child B

            ascii-em:     Root
                         ╠══ Child A
                         ╚══ Child B

        Note:
            The tree display automatically handles complex hierarchies with proper
            indentation and connection lines. Very large trees may be truncated
            for performance reasons.
        """
        self._reader = ""

        def write(line):
            self._reader += line.decode("utf-8") + "\n"

        try:
            self.__print_backend(
                nid,
                level,
                idhidden,
                filter,
                key,
                reverse,
                line_type,
                data_property,
                sorting,
                func=write,
            )
        except NodeIDAbsentError:
            print("Tree is empty")

        if stdout:
            print(self._reader)
        else:
            return self._reader

    def siblings(self, nid: str) -> NodeList:
        """
        Return the siblings of given @nid.

        If @nid is root or there are no siblings, an empty list is returned.
        """
        siblings = []

        if nid != self.root:
            pid = self[nid].predecessor(self._identifier)
            siblings = [self[i] for i in self[pid].successors(self._identifier) if i != nid]

        return siblings

    def size(self, level: Optional[int] = None) -> int:
        """
        Get the number of nodes of the whole tree if @level is not
        given. Otherwise, the total number of nodes at specific level
        is returned.

        @param level The level number in the tree. It must be between
        [0, tree.depth].

        Otherwise, InvalidLevelNumber exception will be raised.
        """
        if level is None:
            return len(self._nodes)
        else:
            try:
                level = int(level)
                return len([node for node in self.all_nodes_itr() if self.level(node.identifier) == level])
            except Exception:
                raise TypeError("level should be an integer instead of '%s'" % type(level))

    def subtree(self, nid: str, identifier: Optional[str] = None):
        """
        Return a shallow COPY of subtree with nid being the new root.
        If nid is None, return an empty tree.
        If you are looking for a deepcopy, please create a new tree
        with this shallow copy, e.g.,

        .. code-block:: python

            new_tree = Tree(t.subtree(t.root), deep=True)

        This line creates a deep copy of the entire tree.
        """
        st = self._clone(identifier)
        if nid is None:
            return st

        if not self.contains(nid):
            raise NodeIDAbsentError("Node '%s' is not in the tree" % nid)

        st.root = nid
        for node_n in self.expand_tree(nid):
            st._nodes.update({self[node_n].identifier: self[node_n]})
            # define nodes parent/children in this tree
            # all pointers are the same as copied tree, except the root
            st[node_n].clone_pointers(cast(str, self._identifier), cast(str, st.identifier))
            if node_n == nid:
                # reset root parent for the new tree
                st[node_n].set_predecessor(None, st.identifier)
        return st

    def update_node(self, nid: str, **attrs) -> None:
        """
        Update node's attributes.

        :param nid: the identifier of modified node
        :param attrs: attribute pairs recognized by Node object
        :return: None
        """
        cn = self[nid]
        for attr, val in iteritems(attrs):
            if attr == "identifier":
                # Updating node id meets following contraints:
                # * Update node identifier property
                # * Update parent's followers
                # * Update children's parents
                # * Update tree registration of var _nodes
                # * Update tree root if necessary
                cn = self._nodes.pop(nid)
                setattr(cn, "identifier", val)
                self._nodes[val] = cn

                if cn.predecessor(self._identifier) is not None:
                    self[cn.predecessor(self._identifier)].update_successors(
                        nid,
                        mode=self.node_class.REPLACE,
                        replace=val,
                        tree_id=self._identifier,
                    )

                for fp in cn.successors(self._identifier):
                    self[fp].set_predecessor(val, self._identifier)

                if self.root == nid:
                    self.root = val
            else:
                setattr(cn, attr, val)

    def to_dict(self, nid=None, key=None, sort=True, reverse=False, with_data=False):
        """Transform the whole tree into a dict."""

        nid = self.root if (nid is None) else nid
        ntag = self[nid].tag
        tree_dict = {ntag: {"children": []}}
        if with_data:
            tree_dict[ntag]["data"] = self[nid].data

        if self[nid].expanded:
            queue = [self[i] for i in self[nid].successors(self._identifier)]
            key = (lambda x: x) if (key is None) else key
            if sort:
                queue.sort(key=key, reverse=reverse)

            for elem in queue:
                tree_dict[ntag]["children"].append(
                    self.to_dict(elem.identifier, with_data=with_data, sort=sort, reverse=reverse)
                )
            if len(tree_dict[ntag]["children"]) == 0:
                tree_dict = self[nid].tag if not with_data else {ntag: {"data": self[nid].data}}
            return tree_dict

    def to_json(self, with_data: bool = False, sort: bool = True, reverse: bool = False):
        """
        Export the tree structure as a JSON string.

        Converts the entire tree into a JSON representation that can be easily
        saved, transmitted, or reconstructed. Perfect for data persistence,
        API responses, and cross-system integration.

        Args:
            with_data: If True, include node.data in the JSON output.
                      If False, only include tree structure and tags.
                      Default False for smaller output size.
            sort: If True, sort children at each level by node comparison.
                 If False, preserve insertion order.
            reverse: If True, reverse the sorting order at each level.
                    Only applies when sort=True.

        Returns:
            str: JSON string representation of the tree.

        Example:
            Basic JSON export::

                tree = Tree()
                tree.create_node("Root", "root")
                tree.create_node("Child A", "a", parent="root")
                tree.create_node("Child B", "b", parent="root")

                # Basic structure only
                json_str = tree.to_json()
                print(json_str)
                # Output: {"Root": {"children": [{"Child A": {}}, {"Child B": {}}]}}

                # Include node data
                tree.create_node("Data Node", "data", parent="root",
                               data={"type": "important", "value": 42})
                json_with_data = tree.to_json(with_data=True)
                print(json_with_data)
                # Output includes: "data": {"type": "important", "value": 42}

                # Sorted output
                sorted_json = tree.to_json(sort=True, reverse=True)

        JSON Structure::

            The output follows this hierarchical format:

            {
                "root_tag": {
                    "children": [
                        {
                            "child1_tag": {
                                "children": [...],
                                "data": {...}  // if with_data=True
                            }
                        },
                        {
                            "child2_tag": {}  // leaf node
                        }
                    ],
                    "data": {...}  // if with_data=True
                }
            }

        Usage with APIs::

            # Save to file
            with open('tree.json', 'w') as f:
                f.write(tree.to_json(with_data=True))

            # Send via HTTP
            import requests
            response = requests.post('/api/trees',
                                   json={'tree': tree.to_json()})

            # Pretty print
            import json
            formatted = json.dumps(json.loads(tree.to_json()), indent=2)
            print(formatted)

        Note:
            The JSON format preserves the complete tree structure and can be
            used to reconstruct equivalent trees. However, complex data objects
            in node.data must be JSON-serializable (no functions, custom classes
            without __dict__, etc.).
        """
        return json.dumps(self.to_dict(with_data=with_data, sort=sort, reverse=reverse))

    def to_graphviz(
        self,
        filename: Optional[str] = None,
        shape: str = "circle",
        graph: str = "digraph",
        filter=None,
        key=None,
        reverse: bool = False,
        sorting: bool = True,
    ):
        """Exports the tree in the dot format of the graphviz software"""
        nodes, connections = [], []
        if self.nodes:
            for n in self.expand_tree(
                mode=self.WIDTH,
                filter=filter,
                key=key,
                reverse=reverse,
                sorting=sorting,
            ):
                nid = self[n].identifier
                label = str(self[n].tag).translate(str.maketrans({'"': r"\""}))
                state = '"{0}" [label="{1}", shape={2}]'.format(nid, label, shape)
                nodes.append(state)

                for c in self.children(nid):
                    cid = c.identifier
                    edge = "->" if graph == "digraph" else "--"
                    connections.append(('"{0}" ' + edge + ' "{1}"').format(nid, cid))

        # write nodes and connections to dot format
        is_plain_file = filename is not None
        if is_plain_file:
            f = codecs.open(cast(str, filename), "w", "utf-8")
        else:
            f = StringIO()

        f.write(graph + " tree {\n")
        for n in nodes:
            f.write("\t" + n + "\n")

        if len(connections) > 0:
            f.write("\n")

        for cns in connections:
            f.write("\t" + cns + "\n")

        f.write("}")

        if not is_plain_file:
            print(f.getvalue())

        f.close()

    @classmethod
    def from_map(cls, child_parent_dict, id_func=None, data_func=None):
        """
        takes a dict with child:parent, and form a tree
        """
        tree = Tree()
        if tree is None or tree.size() > 0:
            raise ValueError("need to pass in an empty tree")
        id_func = id_func if id_func else lambda x: x
        data_func = data_func if data_func else lambda x: None
        parent_child_dict = {}
        root_node = None
        for k, v in child_parent_dict.items():
            if v is None and root_node is None:
                root_node = k
            elif v is None and root_node is not None:
                raise ValueError("invalid input, more than 1 child has no parent")
            else:
                if v in parent_child_dict:
                    parent_child_dict[v].append(k)
                else:
                    parent_child_dict[v] = [k]
        if root_node is None:
            raise ValueError("cannot find root")

        tree.create_node(root_node, id_func(root_node), data=data_func(root_node))
        queue = [root_node]
        while len(queue) > 0:
            parent_node = queue.pop()
            for child in parent_child_dict.get(parent_node, []):
                tree.create_node(
                    child,
                    id_func(child),
                    parent=id_func(parent_node),
                    data=data_func(child),
                )
                queue.append(child)
        return tree
