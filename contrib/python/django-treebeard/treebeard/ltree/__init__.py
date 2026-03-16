"""Postgres Ltree Trees"""

import functools
import itertools
import operator
import string
from collections.abc import Iterable
from typing import Any

from django.core import serializers
from django.db import models, transaction
from django.db.models import F, Func, OuterRef, Q, Subquery, Value
from django.db.models.functions import Concat
from django.utils.translation import gettext_noop as _

from treebeard.exceptions import InvalidMoveToDescendant, NodeAlreadySaved, PathOverflow
from treebeard.models import Node

from .fields import Ltree2Text, PathField, PathValue, Subpath, Text2LTree


class InvalidLabelConstraints(Exception): ...


def generate_label(
    skip: set[str],
    before: str | None = None,
    after: str | None = None,
):
    """
    Generate a new label value that will order the label before `before` and after `after`.

    Uses an alphabet of digits and ascii uppercase letters.
    If no `before` constraint is provided, then chooses only from letters: this allows room
    for digits to be used in future if nodes are inserted to the left of this one, without
    having to move large chunks of the tree.

    :raise InvalidLabelConstraints: when no label could be generated within the given constraints.
    """
    char_choices = string.ascii_uppercase if not before and not after else string.digits + string.ascii_uppercase

    start = after or char_choices[0]

    if before and before <= start:
        raise InvalidLabelConstraints

    # Construct sets of characters for each position, appending one at the end if we need to extend the string
    char_lists = [char_choices[idx:] for idx in [char_choices.index(char) for char in start]]
    char_lists.append(char_choices)

    # There is no point testing portions of the strings that are identical
    start_from = 0
    for ch1, ch2 in zip(before or "", after or ""):
        if ch1 != ch2:
            break
        start_from += 1

    for i in range(start_from, len(char_lists)):
        iter = itertools.product(*char_lists[: i + 1])
        for label_parts in iter:
            label = "".join(label_parts)
            if after and label <= after:
                continue
            if label in skip:
                continue
            if before and label >= before:
                break  # No point looking any further
            return label

    # We should never reach here... right?
    raise ValueError("Failed to generate label. Please report this as a bug.")


def generate_path(
    prefix: PathValue | None = None,
    before: str | None = None,
    after: str | None = None,
    skip: Iterable[PathValue] | None = None,
) -> PathValue:
    skip = {path[-1] for path in skip} if skip else set()
    label = generate_label(before=before, after=after, skip=skip)
    return PathValue((prefix or []) + [label])


class LT_NodeQuerySet(models.query.QuerySet):
    """
    Custom queryset for the tree node manager.

    Needed only for the custom delete method.
    """

    def delete(self, *args, **kwargs):
        """
        Custom delete method, will remove all descendant nodes to ensure a
        consistent tree (no orphans)

        :returns: tuple of the number of objects deleted and a dictionary
                  with the number of deletions per object type
        """
        # Construct the minimal list of nodes that need deleting along with their descendants
        paths_to_remove = set()
        for node in self.order_by("path").only("path").iterator():
            found = False
            for depth in range(1, len(node.path)):
                path = node.path[0:depth]
                if str(path) in paths_to_remove:
                    # we are already removing an ancestor of this node, so skip it
                    found = True
                    break
            if not found:
                paths_to_remove.add(str(node.path))

        model = self.model.tree_model()

        if not paths_to_remove:
            return super(LT_NodeQuerySet, model.objects.none()).delete(*args, **kwargs)

        query = functools.reduce(operator.or_, [Q(path__descendants=path) for path in paths_to_remove])
        return super(LT_NodeQuerySet, model.objects.filter(query)).delete(*args, **kwargs)

    delete.alters_data = True
    delete.queryset_only = True


class LT_NodeManager(models.Manager):
    """Custom manager for nodes in a Materialized Path tree."""

    def get_queryset(self):
        """Sets the custom queryset as the default."""
        return LT_NodeQuerySet(self.model).order_by("path")


class LT_ComplexAddMoveHandler:
    def _get_new_path(self, prefix, insert_before, insert_after, nodes_moved=False, num_iters=0):
        insert_before_path = insert_before.path[-1] if insert_before else None
        insert_after_path = insert_after.path[-1] if insert_after else None

        if insert_before_path and insert_after_path and insert_after_path > insert_before_path:
            raise ValueError("Invalid path constraints")

        siblings = self.node_cls.tree_model().objects.filter(path__descendants=prefix, path__depth=len(prefix) + 1)

        try:
            path = generate_path(
                prefix=prefix,
                before=insert_before_path,
                after=insert_after_path,
                skip=siblings.values_list("path", flat=True),
            )
        except InvalidLabelConstraints:
            # We failed to find a path within the available range. That means we need to move the insert_before
            # node and everything after it to the right
            self._move_subtree_right(insert_before)
            insert_before.refresh_from_db()
            num_iters += 1

            # We should never end up here... the move right operation should always succeed
            if num_iters > 2:
                raise PathOverflow("Failed to generate a new path for node. Please report this as a bug.")
            return self._get_new_path(prefix, insert_before, insert_after, nodes_moved=True, num_iters=num_iters)

        return path, nodes_moved

    def _move_subtree_right(self, start_node):
        """
        Move the node and everything after it in the tree to the right. This is achieved simply by
        appending an extra character (A) to the topmost label in the path.
        """
        result_class = self.node_cls.tree_model()
        node_depth = len(start_node.path)
        if node_depth > 1:
            result_class.objects.filter(path__gte=start_node.path, path__depth=node_depth).update(
                path=Concat(
                    Subpath(F("path"), 0, node_depth - 1),
                    Text2LTree(Concat(Ltree2Text(Subpath(F("path"), node_depth - 1, 1)), Value("A"))),
                )
            )
            result_class.objects.filter(path__gte=start_node.path, path__depth__gt=node_depth).update(
                path=Concat(
                    Subpath(F("path"), 0, node_depth - 1),
                    Text2LTree(Concat(Ltree2Text(Subpath(F("path"), node_depth - 1, 1)), Value("A"))),
                    Subpath(F("path"), node_depth),
                ),
            )
        else:
            # Moving root nodes
            result_class.objects.filter(path__gte=start_node.path, path__depth=1).update(
                path=Text2LTree(Concat(Ltree2Text(Subpath(F("path"), 0, 1)), Value("A")))
            )
            result_class.objects.filter(path__gte=start_node.path, path__depth__gt=1).update(
                path=Concat(Text2LTree(Concat(Ltree2Text(Subpath(F("path"), 0, 1)), Value("A"))), Subpath(F("path"), 1))
            )


class LT_AddRootHandler:
    def __init__(self, cls, **kwargs):
        super().__init__()
        self.cls = cls
        self.kwargs = kwargs

    def process(self):
        # Lock all root node rows to avoid integrity errors. We must force evaluation of the queryset
        list(self.cls.get_root_nodes().select_for_update().only("pk"))

        # do we have a root node already?
        last_root = self.cls.get_last_root_node()

        if last_root and last_root.node_order_by:
            # There are root nodes and node_order_by has been set.
            # Delegate sorted insertion to add_sibling.
            # We must pass an instance here to ensure that the right object is created for
            # models with multi-table inheritance.
            return last_root.add_sibling(
                "sorted-sibling", instance=self.kwargs.get("instance") or self.cls(**self.kwargs)
            )

        if len(self.kwargs) == 1 and "instance" in self.kwargs:
            # adding the passed (unsaved) instance to the tree
            newobj = self.kwargs["instance"]
            if not newobj._state.adding:
                raise NodeAlreadySaved("Attempted to add a tree node that is already in the database")
        else:
            # creating the new object
            newobj = self.cls(**self.kwargs)

        if last_root:
            # adding the new root node as the last one
            newobj.path = generate_path(skip=self.cls.get_root_nodes().values_list("path", flat=True))
        else:
            # adding the first root node
            newobj.path = generate_path()

        # saving the instance before returning it
        newobj.save()
        return newobj


class LT_AddChildHandler:
    def __init__(self, node, creation_kwargs: dict[str, Any]):
        super().__init__()
        self.node = node
        self.node_cls = node.__class__
        # These are deliberately not extracted in the function signature to avoid collision with model field names
        self.kwargs = creation_kwargs

    def process(self):
        # Lock parent row
        self.node_cls.tree_model().objects.filter(pk=self.node.pk).select_for_update().only("pk").get()
        if self.node_cls.node_order_by and not self.node.is_leaf():
            # there are child nodes and node_order_by has been set
            # delegate sorted insertion to add_sibling
            return self.node.get_last_child().add_sibling("sorted-sibling", **self.kwargs)

        if len(self.kwargs) == 1 and "instance" in self.kwargs:
            # adding the passed (unsaved) instance to the tree
            newobj = self.kwargs["instance"]
            if not newobj._state.adding:
                raise NodeAlreadySaved("Attempted to add a tree node that is already in the database")
        else:
            # creating a new object
            newobj = self.node_cls(**self.kwargs)

        if self.node.is_leaf():
            # the node had no children, adding the first child
            newobj.path = generate_path(prefix=self.node.path)
        else:
            # adding the new child as the last one
            newobj.path = generate_path(
                prefix=self.node.path,
                skip=self.node.get_children().values_list("path", flat=True),
                after=self.node.get_children().last().path[-1],
            )

        # saving the instance before returning it
        newobj.save()
        return newobj


class LT_AddSiblingHandler(LT_ComplexAddMoveHandler):
    def __init__(self, node, pos, creation_kwargs: dict[str, Any]):
        super().__init__()
        self.node = node
        self.node_cls = node.__class__
        self.pos = pos
        # These are deliberately not extracted in the function signature to avoid collision with model field names
        self.kwargs = creation_kwargs

    def process(self):
        self.pos = self.node._prepare_pos_var_for_add_sibling(self.pos)

        if len(self.kwargs) == 1 and "instance" in self.kwargs:
            # adding the passed (unsaved) instance to the tree
            newobj = self.kwargs["instance"]
            if not newobj._state.adding:
                raise NodeAlreadySaved("Attempted to add a tree node that is already in the database")
        else:
            # creating a new object
            newobj = self.node_cls(**self.kwargs)

        insert_before = None
        insert_after = None
        if self.pos == "sorted-sibling":
            insert_before = self.node.get_sorted_pos_queryset(self.node.get_siblings(), newobj).first()
            if insert_before:
                insert_after = insert_before.get_prev_sibling()
        elif self.pos == "first-sibling":
            insert_before = self.node.get_siblings().first()
        elif self.pos == "left":
            insert_before = self.node
            insert_after = self.node.get_prev_sibling()
        elif self.pos == "right":
            insert_after = self.node
            insert_before = self.node.get_next_sibling()
        elif self.pos == "last-sibling":
            insert_after = self.node.get_siblings().last()

        newobj.path, _ = self._get_new_path(self.node.path[:-1], insert_before, insert_after)
        newobj.save()
        return newobj


class LT_MoveHandler(LT_ComplexAddMoveHandler):
    def __init__(self, node, target, pos=None):
        super().__init__()
        self.node = node
        self.node_cls = node.__class__
        self.target = target
        self.pos = pos

    def process(self):
        self.pos = self.node._prepare_pos_var_for_move(self.pos)

        if self.pos in ("first-child", "last-child", "sorted-child"):
            if self.target == self.node:
                raise InvalidMoveToDescendant(_("Can't move node to itself."))

        if self.target.is_descendant_of(self.node):
            raise InvalidMoveToDescendant(_("Can't move node to a descendant."))

        insert_before = None
        insert_after = None
        prefix = self.target.path[:-1]  # Assume initially that target is a sibling

        if self.pos == "sorted-sibling":
            insert_before = self.node.get_sorted_pos_queryset(self.target.get_siblings(), self.node).first()
            if insert_before:
                insert_after = insert_before.get_prev_sibling()
        elif self.pos == "sorted-child":
            insert_before = self.node.get_sorted_pos_queryset(self.target.get_children(), self.node).first()
            if insert_before:
                insert_after = insert_before.get_prev_sibling()
            prefix = self.target.path
        elif self.pos == "first-sibling":
            insert_before = self.target.get_siblings().first()
        elif self.pos == "last-sibling":
            insert_after = self.target.get_siblings().last()
        elif self.pos == "left":
            insert_before = self.target
            insert_after = self.target.get_prev_sibling()
        elif self.pos == "right":
            insert_after = self.target
            insert_before = self.target.get_next_sibling()
        elif self.pos == "first-child":
            insert_before = self.target.get_children().first()
            prefix = self.target.path
        elif self.pos == "last-child":
            insert_after = self.target.get_children().last()
            prefix = self.target.path

        new_path, nodes_moved = self._get_new_path(prefix, insert_before, insert_after)
        if nodes_moved:
            self.node.refresh_from_db()

        # Update the path for all the descendants of the node
        result_class = self.node_cls.tree_model()
        result_class.objects.filter(path__descendants=self.node.path, path__depth__gt=len(self.node.path)).update(
            path=Concat(
                Value(new_path, output_field=PathField()),
                Subpath(F("path"), len(self.node.path)),
            )
        )
        # And update the path for the node itself
        self.node.path = new_path
        self.node.save()


class LT_Node(Node):
    """Abstract model to create your own Postgres LTree trees."""

    node_order_by = []
    path = PathField(unique=True)

    TREEBEARD_IDENTIFYING_FIELD = "path"
    MOVENODE_FORM_EXCLUDED_FIELDS = ("path",)

    objects = LT_NodeManager()

    _cached_attributes = (*Node._cached_attributes,)

    @classmethod
    @transaction.atomic
    def add_root(cls, **kwargs):
        """
        Adds a root node to the tree.

        This method saves the node in database. The object is populated as if via:

        ```
        obj = cls(**kwargs)
        ```
        """
        return LT_AddRootHandler(cls, **kwargs).process()

    @classmethod
    def dump_bulk(cls, parent=None, keep_ids=True):
        """Dumps a tree branch to a python data structure."""

        cls = cls.tree_model()

        # Because of fix_tree, this method assumes that the depth
        # and numchild properties in the nodes can be incorrect,
        # so no helper methods are used
        qset = cls.objects.all()
        if parent:
            qset = qset.filter(path__descendants=parent.path)
        ret, lnk = [], {}
        pk_field = cls._meta.pk.attname
        for pyobj in serializers.serialize("python", qset.iterator()):
            # django's serializer stores the attributes in 'fields'
            fields = pyobj["fields"]
            path = PathValue(fields["path"])
            depth = len(path)
            # this will be useless in load_bulk
            del fields["path"]
            if pk_field in fields:
                # this happens immediately after a load_bulk
                del fields[pk_field]

            newobj = {"data": fields}
            if keep_ids:
                newobj[pk_field] = pyobj["pk"]

            if (not parent and depth == 1) or (parent and len(path) == len(parent.path)):
                ret.append(newobj)
            else:
                parentpath = path[0 : depth - 1]
                parentobj = lnk[str(parentpath)]
                if "children" not in parentobj:
                    parentobj["children"] = []
                parentobj["children"].append(newobj)
            lnk[str(path)] = newobj
        return ret

    @classmethod
    def get_tree(cls, parent=None):
        """
        :returns:

            A *queryset* of nodes ordered as DFS, including the parent.
            If no parent is given, the entire tree is returned.
        """
        cls = cls.tree_model()

        if parent is None:
            # return the entire tree
            return cls.objects.all()

        return cls.objects.filter(path__descendants=parent.path)

    @classmethod
    def get_root_nodes(cls):
        """:returns: A queryset containing the root nodes in the tree."""
        return cls.tree_model().objects.filter(path__depth=1).order_by("path")

    @classmethod
    def get_descendants_group_count(cls, parent=None):
        """
        Helper for a very common case: get a group of siblings and the number
        of *descendants* (not only children) in every sibling.

        :param parent:

            The parent of the siblings to return. If no parent is given, the
            root nodes will be returned.

        :returns:

            A Queryset of node objects with an extra attribute: `descendants_count`.
        """
        cls = cls.tree_model()
        qs = parent.get_children() if parent else cls.get_root_nodes()
        subquery = (
            cls.objects.filter(path__descendants=OuterRef("path"))
            .order_by()
            .annotate(count=Func(F("pk"), function="Count"))
            .values("count")
        )
        return qs.annotate(
            descendants_count=Subquery(subquery, output_field=models.IntegerField()) - 1
        )  # Subtract the parent node from the count

    def get_depth(self):
        """:returns: the depth (level) of the node"""
        return len(self.path)

    def get_siblings(self):
        """
        :returns: A queryset of all the node's siblings, including the node
            itself.
        """
        return self.tree_model().objects.filter(path__descendants=self.path[:-1], path__depth=len(self.path))

    def get_children(self):
        """:returns: A queryset of all the node's children"""
        return self.tree_model().objects.filter(path__descendants=self.path, path__depth=len(self.path) + 1)

    def get_next_sibling(self):
        """
        :returns: The next node's sibling, or None if it was the rightmost
            sibling.
        """
        return self.get_siblings().filter(path__gt=self.path).first()

    def get_descendants(self, include_self=False):
        """
        :returns: A queryset of all the node's descendants as DFS, doesn't
            include the node itself if `include_self` is False
        """
        if include_self:
            return self.__class__.get_tree(self)

        return self.__class__.get_tree(self).exclude(pk=self.pk)

    def get_prev_sibling(self):
        """
        :returns: The previous node's sibling, or None if it was the leftmost
            sibling.
        """
        try:
            return self.get_siblings().filter(path__lt=self.path).reverse()[0]
        except IndexError:
            return None

    def get_children_count(self):
        """
        :returns: The number the node's children, calculated in the most
        efficient possible way.
        """
        return self.get_children().count()

    def is_sibling_of(self, node) -> bool:
        """
        :returns: ``True`` if the node is a sibling of another node given as an
            argument, else, returns ``False``
        """
        return self.path[:-1] == node.path[:-1]

    def is_child_of(self, node) -> bool:
        """
        :returns: ``True`` is the node if a child of another node given as an
            argument, else, returns ``False``
        """
        return self.path[:-1] == node.path

    def is_descendant_of(self, node) -> bool:
        """
        :returns: ``True`` if the node is a descendant of another node given
            as an argument, else, returns ``False``
        """
        return len(self.path) > len(node.path) and all([self.path[idx] == label for idx, label in enumerate(node.path)])

    @transaction.atomic
    def add_child(self, **kwargs):
        """
        Adds a child to the node.

        This method saves the node in database. The object is populated as if via:

        ```
        obj = self.__class__(**kwargs)
        ```
        """
        return LT_AddChildHandler(self, kwargs).process()

    @transaction.atomic
    def add_sibling(self, pos=None, **kwargs):
        """
        Adds a new node as a sibling to the current node object.

        This method saves the node in database. The object is populated as if via:

        ```
        obj = self.__class__(**kwargs)
        ```
        """
        return LT_AddSiblingHandler(self, pos, kwargs).process()

    def get_root(self):
        """:returns: the root node for the current node object."""
        return self.tree_model().objects.get(path=self.path[0])

    def is_root(self):
        """:returns: True if the node is a root node (else, returns False)"""
        return len(self.path) == 1

    def is_leaf(self):
        """:returns: True if the node is a leaf node (else, returns False)"""
        return not self.get_children().exists()

    def get_ancestors(self):
        """
        :returns: A queryset containing the current node object's ancestors,
            starting by the root node and descending to the parent.
        """
        return self.tree_model().objects.filter(path__ancestors=self.path).exclude(pk=self.pk)

    def get_parent(self, update=False):
        """
        :returns: the parent node of the current node object.
        """
        if len(self.path) == 1:
            return

        parentpath = self.path[:-1]
        return self.tree_model().objects.get(path=parentpath)

    @transaction.atomic
    def move(self, target, pos=None):
        """
        Moves the current node and all it's descendants to a new position
        relative to another node.
        """
        return LT_MoveHandler(self, target, pos).process()

    class Meta:
        abstract = True
