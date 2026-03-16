import re
import sys
from collections import OrderedDict
from contextlib import suppress
from typing import Optional, List, Dict, Tuple, cast
from warnings import warn

from _pytest.config import Config
from _pytest.mark import Mark
from _pytest.python import Function

from .item import Item, ItemList, ItemGroup, filter_marks, move_item, RelativeMark
from .settings import Settings, Scope

orders_map = {
    "first": 0,
    "second": 1,
    "third": 2,
    "fourth": 3,
    "fifth": 4,
    "sixth": 5,
    "seventh": 6,
    "eighth": 7,
    "last": -1,
    "second_to_last": -2,
    "third_to_last": -3,
    "fourth_to_last": -4,
    "fifth_to_last": -5,
    "sixth_to_last": -6,
    "seventh_to_last": -7,
    "eighth_to_last": -8,
}


class Sorter:
    """
    Sort all items according to the given configuration.
    """

    def __init__(self, config: Config, items: List[Function]) -> None:
        self.settings: Settings = Settings(config)
        self.items: List[Item] = [Item(item) for item in items]
        self.node_ids: Dict[str, Item] = OrderedDict()
        self.node_id_last: Dict[str, List[str]] = {}
        for item in self.items:
            self.node_ids[item.node_id] = item
            last_part = item.node_id.rpartition("::")[2]
            if "[" in last_part:
                last_part = last_part.rpartition("[")[0]
            # save last nodeid component to avoid to iterate over all
            # items for each label
            self.node_id_last.setdefault(last_part, []).append(item.node_id)
        self.rel_marks: List[RelativeMark[Item]] = []
        self.dep_marks: List[RelativeMark[Item]] = []

    def sort_items(self) -> List[Function]:
        """
        Do the actual sorting and return the sorted items.
        """
        self.collect_markers()
        if self.settings.scope == Scope.SESSION:
            if self.settings.scope_level > 0:
                dir_groups = directory_item_groups(
                    self.items, self.settings.scope_level
                )
                sorted_list = []
                for items in dir_groups.values():
                    sorter = ScopeSorter(
                        self.settings, items, self.rel_marks, self.dep_marks
                    )
                    sorted_list.extend(sorter.sort_items())
            else:
                sorter = ScopeSorter(
                    self.settings,
                    self.items,
                    self.rel_marks,
                    self.dep_marks,
                    session_scope=True,
                )
                sorted_list = sorter.sort_items()
        elif self.settings.scope == Scope.MODULE:
            module_groups = module_item_groups(self.items)
            sorted_list = []
            for module_items in module_groups.values():
                sorter = ScopeSorter(
                    self.settings, module_items, self.rel_marks, self.dep_marks
                )
                sorted_list.extend(sorter.sort_items())
        else:  # class scope
            class_groups = class_item_groups(self.items)
            sorted_list = []
            for class_items in class_groups.values():
                sorter = ScopeSorter(
                    self.settings, class_items, self.rel_marks, self.dep_marks
                )
                sorted_list.extend(sorter.sort_items())
        return [item.item for item in sorted_list]

    def mark_binning(
        self,
        item: Item,
        dep_marks: Dict[Tuple[str, Scope, str], List[Item]],
        aliases: Dict[str, List[Item]],
    ) -> None:
        """
        Collect relevant markers for the given item.
        """
        keys = item.item.keywords.keys()
        has_dependency = "dependency" in keys
        has_order = "order" in keys
        if not has_order and self.settings.marker_prefix:
            for key in keys:
                if key.startswith(self.settings.marker_prefix):
                    try:
                        index = int(key[len(self.settings.marker_prefix)])
                        item.order = index
                    except ValueError:
                        pass
        if has_dependency or self.settings.auto_mark_dep:
            self.handle_dependency_mark(item, has_order, dep_marks, aliases)
        if has_order:
            self.handle_order_marks(item)

    def handle_dependency_mark(
        self,
        item: Item,
        has_order: bool,
        dep_marks: Dict[Tuple[str, Scope, str], List[Item]],
        aliases: Dict[str, List[Item]],
    ) -> None:
        # always order dependencies if an order mark is present
        # otherwise only if order-dependencies is set
        mark = item.item.get_closest_marker("dependency")
        name_mark = None
        if mark and (self.settings.order_dependencies or has_order):
            dependent_mark = mark.kwargs.get("depends")
            if dependent_mark:
                scope = scope_from_name(mark.kwargs.get("scope", "module"))
                prefix = scoped_node_id(item.node_id, scope)
                for name in dependent_mark:
                    dep_marks.setdefault((name, scope, prefix), []).append(item)
                    item.inc_rel_marks()
            # we always collect the names of the dependent items, because
            # we need them in both cases
            name_mark = mark.kwargs.get("name")
        # the default name in pytest-dependency is the nodeid or a part
        # of the nodeid, depending on the scope
        if not name_mark:
            name_mark = item.node_id
        aliases.setdefault(name_mark, []).append(item)

    def handle_order_marks(self, item: Item) -> None:
        marks = item.item.iter_markers("order")
        for mark in marks:
            self.handle_order_mark(item, mark)

    def handle_order_mark(self, item: Item, mark: Mark) -> None:
        order = mark.args[0] if mark.args else mark.kwargs.get("index")
        if order is not None:
            if isinstance(order, int):
                order = int(order)
            elif order in orders_map:
                order = orders_map[order]
            else:
                warn(f"Unknown order attribute:'{order}'")
                order = None
        if item.order is None:
            item.order = order
        self.handle_relative_marks(item, mark)
        if order is not None:
            item.nr_rel_items = 0

    def items_from_label(self, label: str, item: Item, is_cls_mark: bool) -> List[Item]:
        """
        Return the list of matching items from the given label.
        The list contains one item for a single matching test, several items
        in the case of a matching parametrized test, or no item in case of
        an invalid label.
        """
        item_id = item.node_id
        label_len = len(label)
        last_comp = label.split("/")[-1].split("::")[-1]
        items = []
        with suppress(KeyError):
            node_ids = self.node_id_last[last_comp]
            for node_id in node_ids:
                if node_id.endswith(label):
                    id_start = node_id[:-label_len]
                elif node_id.endswith("]") and node_id.rpartition("[")[0].endswith(
                    label
                ):
                    id_start = node_id.rpartition("[")[0][:-label_len]
                else:
                    continue
                if is_cls_mark and id_start.count("::") == 2:
                    continue
                if item_id.startswith(id_start):
                    items.append(self.node_ids[node_id])

        return items

    def items_from_class_label(self, label: str, item: Item) -> List[Item]:
        items = []
        item_id = item.node_id
        label_len = len(label)
        for node_id in self.node_ids:
            if node_id.count("::") == 2:
                cls_index = node_id.rindex("::")
                if node_id[:cls_index].endswith(label):
                    id_start = node_id[: cls_index - label_len]
                    if item_id.startswith(id_start):
                        items.append(self.node_ids[node_id])
        return items

    def handle_before_or_after_mark(
        self, item: Item, mark: Mark, marker_name: str, is_after: bool
    ) -> bool:
        def is_class_mark() -> bool:
            if item.item.cls and item.item.parent:
                return item.item.parent.get_closest_marker("order") == mark
            return False

        def is_mark_for_class() -> bool:
            return "::" not in marker_name and is_class_mark()

        is_cls_mark = is_class_mark()
        items_for_label = self.items_from_label(marker_name, item, is_cls_mark)
        if items_for_label:
            for item_for_label in items_for_label:
                rel_mark = RelativeMark(item_for_label, item, move_after=is_after)
                if is_after or not is_cls_mark:
                    self.rel_marks.append(rel_mark)
                else:
                    self.rel_marks.insert(0, rel_mark)
                item.inc_rel_marks()
            return True
        else:
            if is_mark_for_class():
                items = self.items_from_class_label(marker_name, item)
                for item_for_label in items:
                    rel_mark = RelativeMark(item_for_label, item, move_after=is_after)
                    if is_after:
                        self.rel_marks.append(rel_mark)
                    else:
                        self.rel_marks.insert(0, rel_mark)
                    item.inc_rel_marks()
                return len(items) > 0
        return False

    def handle_relative_marks(self, item: Item, mark: Mark) -> bool:
        has_relative_marks = False
        before_marks = mark.kwargs.get("before", ())
        if before_marks and not isinstance(before_marks, (list, tuple)):
            before_marks = (before_marks,)
        for before_mark in before_marks:
            if self.handle_before_or_after_mark(
                item, mark, before_mark, is_after=False
            ):
                has_relative_marks = True
            else:
                self.warn_about_unknown_test(item, before_mark)
        after_marks = mark.kwargs.get("after", ())
        if after_marks and not isinstance(after_marks, (list, tuple)):
            after_marks = (after_marks,)
        for after_mark in after_marks:
            if self.handle_before_or_after_mark(item, mark, after_mark, is_after=True):
                has_relative_marks = True
            else:
                self.warn_about_unknown_test(item, after_mark)
        return has_relative_marks

    def warn_about_unknown_test(self, item: Item, rel_mark: str) -> None:
        if self.settings.error_on_failed_ordering:
            item.item.fixturenames.insert(0, "fail_after_cannot_order")
            ignore_msg = ""
        else:
            ignore_msg = " - ignoring the marker"
        sys.stdout.write(
            f"\nWARNING: cannot execute '{item.item.name}' relative to others: "
            f"'{rel_mark}'{ignore_msg}."
        )

    def collect_markers(self) -> None:
        aliases: Dict[str, List[Item]] = {}
        dep_marks: Dict[Tuple[str, Scope, str], List[Item]] = {}
        for item in self.items:
            self.mark_binning(item, dep_marks, aliases)
        self.resolve_dependency_markers(dep_marks, aliases)

    def resolve_dependency_markers(
        self,
        dep_marks: Dict[Tuple[str, Scope, str], List[Item]],
        aliases: Dict[str, List[Item]],
    ) -> None:
        for (name, _, prefix), items in dep_marks.items():
            if name in aliases:
                for item in items:
                    alias = self.matching_alias(aliases[name], item)
                    self.dep_marks.append(RelativeMark(alias, item, move_after=True))
            else:
                label = "::".join((prefix, name))
                if label in aliases:
                    for item in items:
                        alias = self.matching_alias(aliases[label], item)
                        self.dep_marks.append(
                            RelativeMark(alias, item, move_after=True)
                        )
                else:
                    sys.stdout.write(
                        f"\nWARNING: Cannot resolve the dependency marker '{name}' "
                        "- ignoring it."
                    )

    @staticmethod
    def matching_alias(aliases: List[Item], item: Item) -> Item:
        if len(aliases) == 1:
            return aliases[0]

        # handle the rare case that several tests have the same alias name
        # we use the item that best matches the node id of the dependent item
        max_matching_parts = 0
        node_id_parts = re.split("(::|/)", item.node_id)
        matching_item = aliases[0]
        for alias_item in aliases:
            alias_node_id_parts = re.split("(::|/)", alias_item.node_id)
            nr_matching_parts = 0
            for n, a in zip(node_id_parts, alias_node_id_parts):
                if n != a:
                    break
                nr_matching_parts += 1
            if nr_matching_parts > max_matching_parts:
                max_matching_parts = nr_matching_parts
                matching_item = alias_item
        return matching_item


def module_item_groups(items: List[Item]) -> Dict[str, List[Item]]:
    """
    Split items into groups per module.
    """
    module_items: OrderedDict[str, List[Item]] = OrderedDict()
    for item in items:
        module_items.setdefault(item.module_path, []).append(item)
    return module_items


def directory_item_groups(items: List[Item], level: int) -> Dict[str, List[Item]]:
    """
    Split items into groups per directory at the given level.
    The level is relative to the root directory, which is at level 0.
    """
    module_items: OrderedDict[str, List[Item]] = OrderedDict()
    for item in items:
        module_items.setdefault(item.parent_path(level), []).append(item)
    return module_items


def class_item_groups(items: List[Item]) -> Dict[str, List[Item]]:
    """
    Split items into groups per class.
    Items outside a class are sorted into a group per module.
    """
    class_items: OrderedDict[str, List[Item]] = OrderedDict()
    for item in items:
        delimiter_index = item.node_id.index("::")
        if "::" in item.node_id[delimiter_index + 2 :]:
            delimiter_index = item.node_id.index("::", delimiter_index + 2)
        class_path = item.node_id[:delimiter_index]
        class_items.setdefault(class_path, []).append(item)
    return class_items


class ScopeSorter:
    """
    Sorts the items for the defined scope.
    """

    def __init__(
        self,
        settings: Settings,
        items: List[Item],
        rel_marks: List[RelativeMark[Item]],
        dep_marks: List[RelativeMark[Item]],
        session_scope: bool = False,
    ) -> None:
        self.settings = settings
        self.items = items
        # no need to filter items in session scope
        if session_scope:
            self.rel_marks = rel_marks
            self.dep_marks = dep_marks
        else:
            self.rel_marks = filter_marks(rel_marks, self.items)
            self.dep_marks = filter_marks(dep_marks, self.items)

    def sort_items(self) -> List[Item]:
        if self.settings.group_scope.value < self.settings.scope.value:
            if self.settings.scope == Scope.SESSION:
                sorted_list = self.sort_in_session_scope()
            else:  # module scope / class group scope
                sorted_list = self.sort_in_module_scope()
        else:
            sorted_list = self.sort_items_in_scope(self.items, Scope.SESSION).items

        return sorted_list

    def sort_in_session_scope(self) -> List[Item]:
        sorted_list = []
        module_items = module_item_groups(self.items)
        if self.settings.group_scope == Scope.CLASS:
            module_groups = self.sort_class_groups(module_items)
        else:
            module_groups = [
                self.sort_items_in_scope(item, Scope.MODULE)
                for item in module_items.values()
            ]
        sorter = GroupSorter(
            Scope.MODULE, module_groups, self.rel_marks, self.dep_marks
        )
        for group in sorter.sorted_groups()[1]:
            sorted_list.extend(group.items)
        return sorted_list

    def sort_in_module_scope(self) -> List[Item]:
        sorted_list = []
        class_items = class_item_groups(self.items)
        class_groups = [
            self.sort_items_in_scope(item, Scope.CLASS) for item in class_items.values()
        ]
        sorter = GroupSorter(Scope.CLASS, class_groups, self.rel_marks, self.dep_marks)
        for group in sorter.sorted_groups()[1]:
            sorted_list.extend(group.items)
        return sorted_list

    def sort_class_groups(self, module_items: Dict[str, List[Item]]) -> List[ItemGroup]:
        module_groups = []
        for module_item in module_items.values():
            class_items = class_item_groups(module_item)
            class_groups = [
                self.sort_items_in_scope(item, Scope.CLASS)
                for item in class_items.values()
            ]
            module_group = ItemGroup()
            sorter = GroupSorter(
                Scope.CLASS, class_groups, self.rel_marks, self.dep_marks
            )
            group_order, class_groups = sorter.sorted_groups()
            module_group.extend(class_groups, group_order)
            module_groups.append(module_group)
        return module_groups

    def sort_items_in_scope(self, items: List[Item], scope: Scope) -> ItemGroup:
        item_list = ItemList(
            items, self.settings, scope, self.rel_marks, self.dep_marks
        )
        for item in items:
            item_list.collect_markers(item)

        sorted_list = item_list.sort_numbered_items()

        still_left = 0
        length = item_list.number_of_rel_groups()
        while length and still_left != length:
            still_left = length
            item_list.handle_rel_marks(sorted_list)
            item_list.handle_dep_marks(sorted_list)
            length = item_list.number_of_rel_groups()
        if length:
            item_list.print_unhandled_items()
        return ItemGroup(sorted_list, item_list.group_order())


def scope_from_name(name: str) -> Scope:
    if name == "module":
        return Scope.MODULE
    if name == "class":
        return Scope.CLASS
    return Scope.SESSION


def scoped_node_id(node_id: str, scope: Scope) -> str:
    if scope == Scope.MODULE:
        return node_id[: node_id.index("::")]
    if scope == Scope.CLASS:
        return node_id[: node_id.rindex("::")]
    return ""


class GroupSorter:
    """
    Sorts groups of items.
    """

    def __init__(
        self,
        scope: Scope,
        groups: List[ItemGroup],
        rel_marks: List[RelativeMark[Item]],
        dep_marks: List[RelativeMark[Item]],
    ) -> None:
        self.scope: Scope = scope
        self.groups: List[ItemGroup] = groups
        self.rel_marks: List[RelativeMark[ItemGroup]] = self.collect_group_marks(
            rel_marks
        )
        self.dep_marks: List[RelativeMark[ItemGroup]] = self.collect_group_marks(
            dep_marks
        )

    def collect_group_marks(
        self, marks: List[RelativeMark[Item]]
    ) -> List[RelativeMark[ItemGroup]]:
        group_marks: List[RelativeMark[ItemGroup]] = []
        for mark in marks:
            group = self.group_for_item(mark.item)
            group_to_move = self.group_for_item(mark.item_to_move)
            if group is not None and group_to_move is not None:
                group_marks.append(RelativeMark(group, group_to_move, mark.move_after))
                group_to_move.inc_rel_marks()
        return group_marks

    def group_for_item(self, item: Item) -> Optional[ItemGroup]:
        for group in self.groups:
            if item in group.items:
                return group
        return None

    def sorted_groups(self) -> Tuple[Optional[int], List[ItemGroup]]:
        group_order = self.sort_by_ordinal_markers()
        length = len(self.rel_marks) + len(self.dep_marks)
        if length == 0:
            return group_order, self.groups

        # handle relative markers the same way single items are handled
        still_left = 0
        while length and still_left != length:
            still_left = length
            self.handle_rel_marks(self.rel_marks)
            self.handle_rel_marks(self.dep_marks)
        return group_order, self.groups

    def sort_by_ordinal_markers(self) -> Optional[int]:
        start_groups = []
        middle_groups = []
        end_groups = []
        for group in self.groups:
            if group.order is None:
                middle_groups.append(group)
            elif group.order >= 0:
                start_groups.append(group)
            else:
                end_groups.append(group)
        start_groups = sorted(start_groups, key=lambda g: cast(int, g.order))
        end_groups = sorted(end_groups, key=lambda g: cast(int, g.order))
        self.groups = start_groups + middle_groups + end_groups
        if start_groups:
            group_order = start_groups[0].order
        elif end_groups:
            group_order = end_groups[-1].order
        else:
            group_order = None
        return group_order

    def handle_rel_marks(self, marks: List[RelativeMark[ItemGroup]]) -> None:
        for mark in reversed(marks):
            if move_item(mark, self.groups):
                marks.remove(mark)
