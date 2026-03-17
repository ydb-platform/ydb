#!/usr/bin/env python
# Copyright (C) 2011
# Brett Alistair Kromkamp - brettkromkamp@gmail.com
# Copyright (C) 2012-2025
# Xiaming Chen - chenxm35@gmail.com
# and other contributors.
# All rights reserved.
"""
Node structure in treelib.

A :class:`Node` object contains basic properties such as node identifier,
node tag, parent node, children nodes etc., and some operations for a node.

The Node class is the fundamental building block of a Tree. Each node can store:
- A unique identifier for tree operations
- A human-readable tag for display
- Custom data payload
- Parent-child relationships within one or more trees

Note:
    Nodes are typically created through Tree.create_node() rather than
    directly instantiated, as the Tree class manages the parent-child
    relationships automatically.
"""
from __future__ import unicode_literals

import copy
import sys
import uuid
from collections import defaultdict
from typing import Any, List, Optional, Union, cast
from warnings import warn

from .exceptions import NodePropertyError
from .misc import deprecated

if sys.version_info >= (3, 9):
    StrList = list[str]
else:
    StrList = List[str]  # Python 3.8 and earlier


class Node(object):
    """
    Elementary node object stored in Tree structures.

    A Node represents a single element in a tree hierarchy. Each node maintains
    references to its parent and children within specific tree contexts, along
    with a human-readable tag and optional data payload.

    Attributes:
        identifier (str): Unique identifier for this node within a tree.
        tag (str): Human-readable label for display purposes.
        expanded (bool): Controls visibility of children in tree displays.
        data (Any): User-defined payload associated with this node.

    Example:
        Creating nodes with different configurations::

            # Basic node
            node = Node("Root", "root")

            # Node with custom data
            node = Node("File", "file1", data={"size": 1024, "type": "txt"})

            # Node that starts collapsed
            node = Node("Folder", "folder1", expanded=False)

    Warning:
        While nodes can be created directly, it's recommended to use
        Tree.create_node() which properly manages tree relationships.
    """

    #: Mode constants for routine `update_fpointer()`.
    (ADD, DELETE, INSERT, REPLACE) = list(range(4))

    def __init__(
        self,
        tag: Optional[str] = None,
        identifier: Optional[str] = None,
        expanded: bool = True,
        data: Any = None,
    ) -> None:
        """
        Create a new Node object to be placed inside a Tree object.

        Args:
            tag: Human-readable label for the node. If None, uses identifier.
            identifier: Unique ID for the node. If None, generates a UUID.
            expanded: Whether node children are visible in tree displays.
            data: Optional user data to associate with this node.

        Example:
            Creating different types of nodes::

                # Auto-generated identifier
                node1 = Node("My Node")

                # Explicit identifier
                node2 = Node("Root", "root")

                # With custom data
                node3 = Node("User", "user1", data={"name": "Alice", "role": "admin"})

                # Initially collapsed
                node4 = Node("Large Folder", "folder", expanded=False)
        """

        #: if given as a parameter, must be unique
        self._identifier: Optional[str] = None
        self._set_identifier(identifier)

        #: None or something else
        #: if None, self._identifier will be set to the identifier's value.
        if tag is None:
            self._tag = self._identifier
        else:
            self._tag = tag

        #: boolean
        self.expanded = expanded

        #: identifier of the parent's node :
        self._predecessor: dict = {}
        #: identifier(s) of the soons' node(s) :
        self._successors: dict = defaultdict(list)

        #: User payload associated with this node.
        self.data = data

        # for retro-compatibility on bpointer/fpointer
        self._initial_tree_id: Optional[str] = None

    def __lt__(self, other) -> bool:
        """
        Compare nodes for sorting based on their tags.

        Enables sorting of Node objects in collections by comparing their tag attributes.
        This allows natural ordering when displaying or processing collections of nodes.

        Args:
            other: Another Node object to compare with.

        Returns:
            bool: True if this node's tag is lexicographically less than other's tag.

        Example:
            Sorting nodes by tag::

                nodes = [Node("Charlie", "c"), Node("Alice", "a"), Node("Bob", "b")]
                sorted_nodes = sorted(nodes)  # Alice, Bob, Charlie
        """
        return self.tag < other.tag

    def set_initial_tree_id(self, tree_id: str) -> None:
        """
        Set the initial tree identifier for backward compatibility.

        This method is used internally to maintain compatibility with older
        node pointer systems. Each node remembers the first tree it was added to
        for legacy bpointer/fpointer access.

        Args:
            tree_id: Unique identifier of the tree this node was first added to.

        Note:
            This is an internal method used for legacy compatibility.
            Modern code should use the tree-specific pointer methods instead.
        """
        if self._initial_tree_id is None:
            self._initial_tree_id = tree_id

    def _set_identifier(self, nid: Optional[str]) -> None:
        """
        Initialize node identifier with given value or auto-generate one.

        Private method used during node creation to set the unique identifier.
        If no identifier is provided, generates a UUID automatically to ensure
        uniqueness within the tree.

        Args:
            nid: Desired node identifier, or None to auto-generate.

        Note:
            This is an internal method. Use the identifier property for
            accessing or modifying the identifier after node creation.
        """
        if nid is None:
            self._identifier = str(uuid.uuid1())
        else:
            self._identifier = nid

    @property
    @deprecated(alias="node.predecessor")
    def bpointer(self):
        if self._initial_tree_id not in self._predecessor.keys():
            return None
        return self._predecessor[self._initial_tree_id]

    @bpointer.setter
    @deprecated(alias="node.set_predecessor")
    def bpointer(self, value) -> None:
        self.set_predecessor(value, self._initial_tree_id)

    @deprecated(alias="node.set_predecessor")
    def update_bpointer(self, nid) -> None:
        self.set_predecessor(nid, self._initial_tree_id)

    @property
    @deprecated(alias="node.successors")
    def fpointer(self):
        if self._initial_tree_id not in self._successors:
            return []
        return self._successors[self._initial_tree_id]

    @fpointer.setter
    @deprecated(alias="node.update_successors")
    def fpointer(self, value: Union[None, list, dict, set]) -> None:
        self.set_successors(value, tree_id=self._initial_tree_id)

    @deprecated(alias="node.update_successors")
    def update_fpointer(self, nid, mode=ADD, replace=None):
        self.update_successors(nid, mode, replace, self._initial_tree_id)

    def predecessor(self, tree_id):
        """
        Get the parent node identifier in the specified tree.

        Since nodes can exist in multiple trees simultaneously, this method
        returns the parent identifier for this node within a specific tree context.

        Args:
            tree_id: Identifier of the tree to query parent relationship in.

        Returns:
            str or None: Parent node identifier, or None if this is a root node.

        Example:
            Accessing parent in different trees::

                # Node can have different parents in different trees
                parent_id = node.predecessor("tree1")
                if parent_id:
                    print(f"Parent in tree1: {parent_id}")
        """
        return self._predecessor[tree_id]

    def set_predecessor(self, nid: Optional[str], tree_id: Optional[str]) -> None:
        """
        Set the parent node identifier for this node in a specific tree.

        Establishes or updates the parent-child relationship for this node
        within the context of a particular tree. Used internally by tree
        operations to maintain proper hierarchy.

        Args:
            nid: Parent node identifier, or None to make this a root node.
            tree_id: Identifier of the tree to set parent relationship in.

        Example:
            Setting parent relationship::

                node.set_predecessor("parent_id", "tree1")
                node.set_predecessor(None, "tree1")  # Make root
        """
        self._predecessor[tree_id] = nid

    def successors(self, tree_id: Optional[str]) -> StrList:
        """
        Get the list of child node identifiers in the specified tree.

        Returns all direct children of this node within a specific tree context.
        Children are maintained as an ordered list to preserve insertion order
        and support custom sorting.

        Args:
            tree_id: Identifier of the tree to query children in.

        Returns:
            list[str]: List of child node identifiers. Empty list if no children.

        Example:
            Accessing children in different trees::

                children = node.successors("tree1")
                for child_id in children:
                    print(f"Child: {child_id}")

                # Node can have different children in different trees
                tree1_children = node.successors("tree1")
                tree2_children = node.successors("tree2")
        """
        return self._successors[tree_id]

    def set_successors(self, value: Union[None, list, dict, set], tree_id: Optional[str] = None) -> None:
        """
        Set the complete list of child node identifiers for a specific tree.

        Replaces the entire children list with new values. Accepts multiple
        input formats for flexibility: list, set, dict (uses keys), or None.

        Args:
            value: New children collection. Can be:
                  - list: Used directly as child identifiers
                  - set: Converted to list of identifiers
                  - dict: Uses dictionary keys as identifiers
                  - None: Sets empty children list
            tree_id: Identifier of the tree to set children in.

        Raises:
            NotImplementedError: If value type is not supported.

        Example:
            Setting children from different sources::

                # From list
                node.set_successors(["child1", "child2"], "tree1")

                # From set
                node.set_successors({"child1", "child2"}, "tree1")

                # From dict (uses keys)
                node.set_successors({"child1": data1, "child2": data2}, "tree1")

                # Clear children
                node.set_successors(None, "tree1")
        """
        setter_lookup = {
            "NoneType": lambda x: list(),
            "list": lambda x: x,
            "dict": lambda x: list(x.keys()),
            "set": lambda x: list(x),
        }

        t = value.__class__.__name__
        if t in setter_lookup:
            f_setter = setter_lookup[t]
            self._successors[tree_id] = f_setter(value)
        else:
            raise NotImplementedError("Unsupported value type %s" % t)

    def update_successors(
        self,
        nid: Optional[str],
        mode: int = ADD,
        replace: Optional[str] = None,
        tree_id: Optional[str] = None,
    ) -> None:
        """
        Update the children list with different modification modes.

        Provides granular control over child node list modifications without
        replacing the entire list. Supports adding, removing, and replacing
        individual child references.

        Args:
            nid: Child node identifier to operate on.
            mode: Operation type using Node constants:
                 - Node.ADD: Append child to end of list
                 - Node.DELETE: Remove child from list
                 - Node.INSERT: Deprecated, same as ADD
                 - Node.REPLACE: Replace child with another identifier
            replace: New identifier when mode=REPLACE. Required for replace mode.
            tree_id: Identifier of the tree to modify children in.

        Raises:
            NodePropertyError: If replace is None when mode=REPLACE.
            NotImplementedError: If mode is not supported.

        Example:
            Different update operations::

                # Add a child
                node.update_successors("new_child", Node.ADD, tree_id="tree1")

                # Remove a child
                node.update_successors("old_child", Node.DELETE, tree_id="tree1")

                # Replace a child
                node.update_successors("old_child", Node.REPLACE,
                                     replace="new_child", tree_id="tree1")
        """
        if nid is None:
            return

        def _manipulator_append() -> None:
            self.successors(tree_id).append(nid)

        def _manipulator_delete() -> None:
            if nid in self.successors(tree_id):
                self.successors(tree_id).remove(nid)
            else:
                warn("Nid %s wasn't present in fpointer" % nid)

        def _manipulator_insert() -> None:
            warn("WARNING: INSERT is deprecated to ADD mode")
            self.update_successors(nid, tree_id=tree_id)

        def _manipulator_replace() -> None:
            if replace is None:
                raise NodePropertyError('Argument "replace" should be provided when mode is {}'.format(mode))
            ind = self.successors(tree_id).index(nid)
            self.successors(tree_id)[ind] = replace

        manipulator_lookup = {
            self.ADD: "_manipulator_append",
            self.DELETE: "_manipulator_delete",
            self.INSERT: "_manipulator_insert",
            self.REPLACE: "_manipulator_replace",
        }

        if mode not in manipulator_lookup:
            raise NotImplementedError("Unsupported node updating mode %s" % str(mode))

        f_name = cast(str, manipulator_lookup.get(mode))
        f = locals()[f_name]
        return f()

    @property
    def identifier(self) -> str:
        """
        Get the unique identifier of this node.

        The identifier serves as the primary key for this node within tree structures.
        It must be unique within each tree that contains this node. Used for all
        tree operations including lookup, traversal, and relationship management.

        Returns:
            str: The unique node identifier.

        Example:
            Accessing node identifier::

                node = Node("My Node", "unique_id")
                print(node.identifier)  # "unique_id"

                # Use in tree operations
                tree[node.identifier]  # Access via identifier
        """
        return cast(str, self._identifier)

    def clone_pointers(self, former_tree_id: str, new_tree_id: str) -> None:
        """
        Clone parent-child relationships from one tree context to another.

        Copies the complete relationship structure (parent and children) from
        one tree context to a new tree context. Used when moving or copying
        nodes between trees while preserving their hierarchical relationships.

        Args:
            former_tree_id: Source tree identifier to copy relationships from.
            new_tree_id: Target tree identifier to copy relationships to.

        Example:
            Cloning relationships between trees::

                # Copy node relationships from tree1 to tree2
                node.clone_pointers("tree1", "tree2")

                # Now node has same relationships in both trees
                assert node.predecessor("tree1") == node.predecessor("tree2")
        """
        former_bpointer = self.predecessor(former_tree_id)
        self.set_predecessor(former_bpointer, new_tree_id)
        former_fpointer = self.successors(former_tree_id)
        # fpointer is a list and would be copied by reference without deepcopy
        self.set_successors(copy.deepcopy(former_fpointer), tree_id=new_tree_id)

    def reset_pointers(self, tree_id) -> None:
        """
        Reset all parent-child relationships for a specific tree context.

        Clears both parent and children references for this node within the
        specified tree, effectively making it an isolated node. Used during
        tree restructuring operations.

        Args:
            tree_id: Identifier of the tree to reset relationships in.

        Example:
            Resetting node relationships::

                # Clear all relationships in tree1
                node.reset_pointers("tree1")

                # Node now has no parent or children in tree1
                assert node.predecessor("tree1") is None
                assert len(node.successors("tree1")) == 0
        """
        self.set_predecessor(None, tree_id)
        self.set_successors([], tree_id=tree_id)

    @identifier.setter  # type: ignore
    def identifier(self, value: str) -> None:
        """
        Set the unique identifier of this node.

        Updates the node's identifier while maintaining data integrity.
        The new identifier must be unique within any trees containing this node.

        Args:
            value: New unique identifier for this node. Cannot be None.

        Warning:
            Changing a node's identifier while it exists in trees can break
            tree integrity. Use Tree.update_node() for safe identifier changes.

        Example:
            Updating node identifier::

                node.identifier = "new_unique_id"

                # Better approach when node is in a tree:
                tree.update_node("old_id", identifier="new_id")
        """
        if value is None:
            print("WARNING: node ID can not be None")
        else:
            self._set_identifier(value)

    def is_leaf(self, tree_id: Optional[str] = None) -> bool:
        """
        Check if this node is a leaf (has no children) in the specified tree.

        A leaf node is one that has no child nodes. This is a fundamental
        property used in tree algorithms and can vary between different
        tree contexts if the node exists in multiple trees.

        Args:
            tree_id: Identifier of the tree to check leaf status in.
                    If None, uses the initial tree for backward compatibility.

        Returns:
            bool: True if node has no children in the specified tree, False otherwise.

        Example:
            Checking leaf status::

                if node.is_leaf("tree1"):
                    print("This is a leaf node")

                # Different trees may have different leaf status
                is_leaf_tree1 = node.is_leaf("tree1")
                is_leaf_tree2 = node.is_leaf("tree2")  # May differ
        """
        if tree_id is None:
            # for retro-compatilibity
            if self._initial_tree_id not in self._successors.keys():
                return True
            else:
                tree_id = self._initial_tree_id

        if len(self.successors(tree_id)) == 0:
            return True
        else:
            return False

    def is_root(self, tree_id: Optional[str] = None) -> bool:
        """
        Check if this node is a root (has no parent) in the specified tree.

        A root node is the topmost node in a tree hierarchy with no parent.
        Every tree has exactly one root node. This status can vary between
        different tree contexts if the node exists in multiple trees.

        Args:
            tree_id: Identifier of the tree to check root status in.
                    If None, uses the initial tree for backward compatibility.

        Returns:
            bool: True if node has no parent in the specified tree, False otherwise.

        Example:
            Checking root status::

                if node.is_root("tree1"):
                    print("This is the root node")

                # Same node might be root in one tree but not another
                is_root_tree1 = node.is_root("tree1")
                is_root_tree2 = node.is_root("tree2")  # May differ
        """
        if tree_id is None:
            # for retro-compatilibity
            if self._initial_tree_id not in self._predecessor.keys():
                return True
            else:
                tree_id = self._initial_tree_id

        return self.predecessor(tree_id) is None

    @property
    def tag(self) -> str:
        """
        Get the human-readable display name of this node.

        The tag serves as the display label for this node in tree visualizations,
        printouts, and user interfaces. Unlike the identifier, the tag doesn't
        need to be unique and is primarily for human readability.

        Returns:
            str: The display name/label for this node.

        Example:
            Using node tags for display::

                node = Node("User Profile", "user123")
                print(node.tag)        # "User Profile"
                print(node.identifier) # "user123"

                # Tags are used in tree display
                tree.show()  # Shows "User Profile" not "user123"
        """
        return cast(str, self._tag)

    @tag.setter
    def tag(self, value: Optional[str]) -> None:
        """
        Set the human-readable display name of this node.

        Updates the display label used in tree visualizations and string
        representations. The tag can be changed freely without affecting
        tree structure or node relationships.

        Args:
            value: New display name for the node. Can be None.

        Example:
            Updating node display name::

                node.tag = "Updated Profile"
                print(node.tag)  # "Updated Profile"

                # Useful for dynamic labeling
                node.tag = f"User: {user.name} (Active)"
        """
        self._tag = value if value is not None else None

    def __repr__(self) -> str:
        """
        Return detailed string representation of this node for debugging.

        Provides a comprehensive view of the node's properties including
        tag, identifier, and data. Useful for debugging, logging, and
        development work.

        Returns:
            str: Detailed string representation in format:
                 Node(tag=<tag>, identifier=<id>, data=<data>)

        Example:
            Node representation output::

                node = Node("Profile", "user123", data={"age": 30})
                print(repr(node))
                # Output: Node(tag=Profile, identifier=user123, data={'age': 30})

                # Useful in lists and debugging
                print([node1, node2])  # Shows detailed info for each node
        """
        name = self.__class__.__name__
        kwargs = [
            "tag={0}".format(self.tag),
            "identifier={0}".format(self.identifier),
            "data={0}".format(self.data),
        ]
        return "%s(%s)" % (name, ", ".join(kwargs))
