import sys
from typing import Optional, List, Dict, Tuple, Generic, TypeVar

from _pytest.python import Function

from .settings import Scope, Settings


_ItemType = TypeVar("_ItemType", "Item", "ItemGroup")


class Item:
    """Represents a single test item."""

    def __init__(self, item: Function) -> None:
        self.item: Function = item
        self.nr_rel_items: int = 0
        self.order: Optional[int] = None
        self._node_id: Optional[str] = None

    def inc_rel_marks(self) -> None:
        if self.order is None:
            self.nr_rel_items += 1

    def dec_rel_marks(self) -> None:
        if self.order is None:
            self.nr_rel_items -= 1

    @property
    def module_path(self) -> str:
        return self.item.nodeid[: self.node_id.index("::")]

    def parent_path(self, level) -> str:
        return "/".join(self.module_path.split("/")[:level])

    @property
    def node_id(self) -> str:
        if self._node_id is None:
            # in pytest < 4 the nodeid has an unwanted ::() part
            self._node_id = self.item.nodeid.replace("::()", "")
        return self._node_id


class ItemList:
    """Handles a group of items with the same scope."""

    def __init__(
        self,
        items: List[Item],
        settings: Settings,
        scope: Scope,
        rel_marks: List["RelativeMark[Item]"],
        dep_marks: List["RelativeMark[Item]"],
    ) -> None:
        self.items = items
        self.settings = settings
        self.scope = scope
        self.start_items: List[Tuple[int, List[Item]]] = []
        self.end_items: List[Tuple[int, List[Item]]] = []
        self.unordered_items: List[Item] = []
        self._start_items: Dict[int, List[Item]] = {}
        self._end_items: Dict[int, List[Item]] = {}
        self.all_rel_marks = rel_marks
        self.all_dep_marks = dep_marks
        self.rel_marks = filter_marks(rel_marks, items)
        self.dep_marks = filter_marks(dep_marks, items)

    def collect_markers(self, item: Item) -> None:
        self.handle_order_mark(item)
        if item.nr_rel_items or item.order is None:
            self.unordered_items.append(item)

    def handle_order_mark(self, item: Item) -> None:
        if item.order is not None:
            if item.order < 0:
                self._end_items.setdefault(item.order, []).append(item)
            else:
                self._start_items.setdefault(item.order, []).append(item)

    def sort_numbered_items(self) -> List[Item]:
        self.start_items = sorted(self._start_items.items())
        self.end_items = sorted(self._end_items.items())
        sorted_list = []
        index = 0
        for order, items in self.start_items:
            if self.settings.sparse_ordering:
                while order > index and self.unordered_items:
                    sorted_list.append(self.unordered_items.pop(0))
                    index += 1
            sorted_list += items
            index += len(items)
        mid_index = len(sorted_list)
        index = -1
        for order, items in reversed(self.end_items):
            if self.settings.sparse_ordering:
                while order < index and self.unordered_items:
                    sorted_list.insert(mid_index, self.unordered_items.pop())
                    index -= 1
            sorted_list[mid_index:mid_index] = items
            index -= len(items)
        sorted_list[mid_index:mid_index] = self.unordered_items
        return sorted_list

    def print_unhandled_items(self) -> None:
        failed_items = [mark.item for mark in self.rel_marks] + [
            mark.item for mark in self.dep_marks
        ]
        msg = " ".join([item.node_id for item in failed_items])
        sys.stdout.write("\nWARNING: cannot execute test relative to others: ")
        sys.stdout.write(msg)
        if self.settings.error_on_failed_ordering:
            sys.stdout.write(" - ignoring the marker.\n")
        else:
            sys.stdout.write(".\n")
        sys.stdout.flush()
        if self.settings.error_on_failed_ordering:
            for item in failed_items:
                item.item.fixturenames.insert(0, "fail_after_cannot_order")

    def number_of_rel_groups(self) -> int:
        return len(self.rel_marks) + len(self.dep_marks)

    def handle_rel_marks(self, sorted_list: List[Item]) -> None:
        self.handle_relative_marks(self.rel_marks, sorted_list, self.all_rel_marks)

    def handle_dep_marks(self, sorted_list: List[Item]) -> None:
        self.handle_relative_marks(self.dep_marks, sorted_list, self.all_dep_marks)

    @staticmethod
    def handle_relative_marks(
        marks: List["RelativeMark[Item]"],
        sorted_list: List[Item],
        all_marks: List["RelativeMark[Item]"],
    ) -> None:
        for mark in reversed(marks):
            if move_item(mark, sorted_list):
                marks.remove(mark)
                all_marks.remove(mark)

    def group_order(self) -> Optional[int]:
        if self.start_items:
            return self.start_items[0][0]
        elif self.end_items:
            return self.end_items[-1][0]
        return None


class ItemGroup:
    """
    Holds a group of sorted items with the same group order scope.
    Used for sorting groups similar to Item for sorting items.
    """

    def __init__(
        self, items: Optional[List[Item]] = None, order: Optional[int] = None
    ) -> None:
        self.items: List[Item] = items or []
        self.order = order
        self.nr_rel_items = 0

    def inc_rel_marks(self) -> None:
        if self.order is None:
            self.nr_rel_items += 1

    def dec_rel_marks(self) -> None:
        if self.order is None:
            self.nr_rel_items -= 1

    def extend(self, groups: List["ItemGroup"], order: Optional[int]) -> None:
        for group in groups:
            self.items.extend(group.items)
        self.order = order


class RelativeMark(Generic[_ItemType]):
    """
    Represents a marker for an item or an item group.
    Holds two related items or groups and their relationship.
    """

    def __init__(
        self,
        item: _ItemType,
        item_to_move: _ItemType,
        move_after: bool,
    ) -> None:
        self.item: _ItemType = item
        self.item_to_move: _ItemType = item_to_move
        self.move_after: bool = move_after


def filter_marks(
    marks: List[RelativeMark[_ItemType]], all_items: List[Item]
) -> List[RelativeMark[_ItemType]]:
    result = []
    for mark in marks:
        if mark.item in all_items and mark.item_to_move in all_items:
            result.append(mark)
        else:
            mark.item_to_move.dec_rel_marks()
    return result


def move_item(mark: RelativeMark[_ItemType], sorted_items: List[_ItemType]) -> bool:
    if (
        mark.item not in sorted_items
        or mark.item_to_move not in sorted_items
        or mark.item.nr_rel_items
    ):
        return False
    pos_item = sorted_items.index(mark.item)
    pos_item_to_move = sorted_items.index(mark.item_to_move)
    if mark.item_to_move.order is not None and mark.item.order is None:
        # if the item to be moved has already been ordered numerically,
        # and the other item is not ordered, we move that one instead
        mark.move_after = not mark.move_after
        mark.item, mark.item_to_move = mark.item_to_move, mark.item
        pos_item, pos_item_to_move = pos_item_to_move, pos_item
    mark.item_to_move.dec_rel_marks()
    if mark.move_after:
        if pos_item_to_move < pos_item + 1:
            del sorted_items[pos_item_to_move]
            sorted_items.insert(pos_item, mark.item_to_move)
    else:
        if pos_item_to_move > pos_item:
            del sorted_items[pos_item_to_move]
            pos_item -= 1
            sorted_items.insert(pos_item + 1, mark.item_to_move)
    return True
