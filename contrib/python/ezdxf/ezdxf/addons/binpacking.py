# Source package: "py3dbp" hosted on PyPI
# (c) Enzo Ruiz Pelaez
# https://github.com/enzoruiz/3dbinpacking
# License: MIT License
# Credits:
# - https://github.com/enzoruiz/3dbinpacking/blob/master/erick_dube_507-034.pdf
# - https://github.com/gedex/bp3d - implementation in Go
# - https://github.com/bom-d-van/binpacking - implementation in Go
#
# ezdxf add-on:
# License: MIT License
# (c) 2022, Manfred Moitzi:
# - refactoring
# - type annotations
# - adaptations:
#   - removing Decimal class usage
#   - utilizing ezdxf.math.BoundingBox for intersection checks
#   - removed non-distributing mode; copy packer and use different bins for each copy
# - additions:
#   - Item.get_transformation()
#   - shuffle_pack()
#   - pack_item_subset()
#   - DXF exporter for debugging
from __future__ import annotations
from typing import (
    Iterable,
    TYPE_CHECKING,
    TypeVar,
    Optional,
)
from enum import Enum, auto
import copy
import math
import random


from ezdxf.enums import TextEntityAlignment
from ezdxf.math import (
    Vec2,
    Vec3,
    UVec,
    BoundingBox,
    BoundingBox2d,
    AbstractBoundingBox,
    Matrix44,
)
from . import genetic_algorithm as ga

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

__all__ = [
    "Item",
    "FlatItem",
    "Box",  # contains Item
    "Envelope",  # contains FlatItem
    "AbstractPacker",
    "Packer",
    "FlatPacker",
    "RotationType",
    "PickStrategy",
    "shuffle_pack",
    "pack_item_subset",
    "export_dxf",
]

UNLIMITED_WEIGHT = 1e99
T = TypeVar("T")
PI_2 = math.pi / 2


class RotationType(Enum):
    """Rotation type of an item:

    - W = width
    - H = height
    - D = depth

    """

    WHD = auto()
    HWD = auto()
    HDW = auto()
    DHW = auto()
    DWH = auto()
    WDH = auto()


class Axis(Enum):
    WIDTH = auto()
    HEIGHT = auto()
    DEPTH = auto()


class PickStrategy(Enum):
    """Order of how to pick items for placement."""

    SMALLER_FIRST = auto()
    BIGGER_FIRST = auto()
    SHUFFLE = auto()


START_POSITION: tuple[float, float, float] = (0, 0, 0)


class Item:
    """3D container item."""

    def __init__(
        self,
        payload,
        width: float,
        height: float,
        depth: float,
        weight: float = 0.0,
    ):
        self.payload = payload  # arbitrary associated Python object
        self.width = float(width)
        self.height = float(height)
        self.depth = float(depth)
        self.weight = float(weight)
        self._rotation_type = RotationType.WHD
        self._position = START_POSITION
        self._bbox: Optional[AbstractBoundingBox] = None

    def __str__(self):
        return (
            f"{str(self.payload)}({self.width}x{self.height}x{self.depth}, "
            f"weight: {self.weight}) pos({str(self.position)}) "
            f"rt({self.rotation_type}) vol({self.get_volume()})"
        )

    def copy(self):
        """Returns a copy, all copies have a reference to the same payload
        object.
        """
        return copy.copy(self)  # shallow copy

    @property
    def bbox(self) -> AbstractBoundingBox:
        if self._bbox is None:
            self._update_bbox()
        return self._bbox  # type: ignore

    def _update_bbox(self) -> None:
        v1 = Vec3(self._position)
        self._bbox = BoundingBox([v1, v1 + Vec3(self.get_dimension())])

    def _taint(self):
        self._bbox = None

    @property
    def rotation_type(self) -> RotationType:
        return self._rotation_type

    @rotation_type.setter
    def rotation_type(self, value: RotationType) -> None:
        self._rotation_type = value
        self._taint()

    @property
    def position(self) -> tuple[float, float, float]:
        """Returns the position of then lower left corner of the item in the
        container, the lower left corner is the origin (0, 0, 0).
        """
        return self._position

    @position.setter
    def position(self, value: tuple[float, float, float]) -> None:
        self._position = value
        self._taint()

    def get_volume(self) -> float:
        """Returns the volume of the item."""
        return self.width * self.height * self.depth

    def get_dimension(self) -> tuple[float, float, float]:
        """Returns the item dimension according the :attr:`rotation_type`."""
        rt = self.rotation_type
        if rt == RotationType.WHD:
            return self.width, self.height, self.depth
        elif rt == RotationType.HWD:
            return self.height, self.width, self.depth
        elif rt == RotationType.HDW:
            return self.height, self.depth, self.width
        elif rt == RotationType.DHW:
            return self.depth, self.height, self.width
        elif rt == RotationType.DWH:
            return self.depth, self.width, self.height
        elif rt == RotationType.WDH:
            return self.width, self.depth, self.height
        raise ValueError(rt)

    def get_transformation(self) -> Matrix44:
        """Returns the transformation matrix to transform the source entity
        located with the minimum extension corner of its bounding box in
        (0, 0, 0) to the final location including the required rotation.
        """
        x, y, z = self.position
        rt = self.rotation_type
        if rt == RotationType.WHD:  # width, height, depth
            return Matrix44.translate(x, y, z)
        if rt == RotationType.HWD:  # height, width, depth
            return Matrix44.z_rotate(PI_2) @ Matrix44.translate(
                x + self.height, y, z
            )
        if rt == RotationType.HDW:  # height, depth, width
            return Matrix44.xyz_rotate(PI_2, 0, PI_2) @ Matrix44.translate(
                x + self.height, y + self.depth, z
            )
        if rt == RotationType.DHW:  # depth, height, width
            return Matrix44.y_rotate(-PI_2) @ Matrix44.translate(
                x + self.depth, y, z
            )
        if rt == RotationType.DWH:  # depth, width, height
            return Matrix44.xyz_rotate(0, PI_2, PI_2) @ Matrix44.translate(
                x, y, z
            )
        if rt == RotationType.WDH:  # width, depth, height
            return Matrix44.x_rotate(PI_2) @ Matrix44.translate(
                x, y + self.depth, z
            )
        raise TypeError(rt)


class FlatItem(Item):
    """2D container item, inherited from :class:`Item`. Has a default depth of
    1.0.
    """

    def __init__(
        self,
        payload,
        width: float,
        height: float,
        weight: float = 0.0,
    ):
        super().__init__(payload, width, height, 1.0, weight)

    def _update_bbox(self) -> None:
        v1 = Vec2(self._position)
        self._bbox = BoundingBox2d([v1, v1 + Vec2(self.get_dimension())])

    def __str__(self):
        return (
            f"{str(self.payload)}({self.width}x{self.height}, "
            f"weight: {self.weight}) pos({str(self.position)}) "
            f"rt({self.rotation_type}) area({self.get_volume()})"
        )


class Bin:
    def __init__(
        self,
        name,
        width: float,
        height: float,
        depth: float,
        max_weight: float = UNLIMITED_WEIGHT,
    ):
        self.name = name
        self.width = float(width)
        if self.width <= 0.0:
            raise ValueError("invalid width")
        self.height = float(height)
        if self.height <= 0.0:
            raise ValueError("invalid height")
        self.depth = float(depth)
        if self.depth <= 0.0:
            raise ValueError("invalid depth")
        self.max_weight = float(max_weight)
        self.items: list[Item] = []

    def __len__(self):
        return len(self.items)

    def __iter__(self):
        return iter(self.items)

    def copy(self):
        """Returns a copy."""
        box = copy.copy(self)  # shallow copy
        box.items = list(self.items)
        return box

    def reset(self):
        """Reset the container to empty state."""
        self.items.clear()

    @property
    def is_empty(self) -> bool:
        return not len(self.items)

    def __str__(self) -> str:
        return (
            f"{str(self.name)}({self.width:.3f}x{self.height:.3f}x{self.depth:.3f}, "
            f"max_weight:{self.max_weight}) "
            f"vol({self.get_capacity():.3f})"
        )

    def put_item(self, item: Item, pivot: tuple[float, float, float]) -> bool:
        valid_item_position = item.position
        item.position = pivot
        x, y, z = pivot

        # Try all possible rotations:
        for rotation_type in self.rotations():
            item.rotation_type = rotation_type
            w, h, d = item.get_dimension()
            if self.width < x + w or self.height < y + h or self.depth < z + d:
                continue
            # new item fits inside the box at he current location and rotation:
            item_bbox = item.bbox
            if (
                not any(item_bbox.has_intersection(i.bbox) for i in self.items)
                and self.get_total_weight() + item.weight <= self.max_weight
            ):
                self.items.append(item)
                return True

        item.position = valid_item_position
        return False

    def get_capacity(self) -> float:
        """Returns the maximum fill volume of the bin."""
        return self.width * self.height * self.depth

    def get_total_weight(self) -> float:
        """Returns the total weight of all fitted items."""
        return sum(item.weight for item in self.items)

    def get_total_volume(self) -> float:
        """Returns the total volume of all fitted items."""
        return sum(item.get_volume() for item in self.items)

    def get_fill_ratio(self) -> float:
        """Return the fill ratio."""
        try:
            return self.get_total_volume() / self.get_capacity()
        except ZeroDivisionError:
            return 0.0

    def rotations(self) -> Iterable[RotationType]:
        return RotationType


class Box(Bin):
    """3D container inherited from :class:`Bin`."""

    pass


class Envelope(Bin):
    """2D container inherited from :class:`Bin`."""

    def __init__(
        self,
        name,
        width: float,
        height: float,
        max_weight: float = UNLIMITED_WEIGHT,
    ):
        super().__init__(name, width, height, 1.0, max_weight)

    def __str__(self) -> str:
        return (
            f"{str(self.name)}({self.width:.3f}x{self.height:.3f}, "
            f"max_weight:{self.max_weight}) "
            f"area({self.get_capacity():.3f})"
        )

    def rotations(self) -> Iterable[RotationType]:
        return RotationType.WHD, RotationType.HWD


def _smaller_first(bins: list, items: list) -> None:
    # SMALLER_FIRST is often very bad! Especially for many in small
    # amounts increasing sizes.
    bins.sort(key=lambda b: b.get_capacity())
    items.sort(key=lambda i: i.get_volume())


def _bigger_first(bins: list, items: list) -> None:
    # BIGGER_FIRST is the best strategy
    bins.sort(key=lambda b: b.get_capacity(), reverse=True)
    items.sort(key=lambda i: i.get_volume(), reverse=True)


def _shuffle(bins: list, items: list) -> None:
    # Better as SMALLER_FIRST
    random.shuffle(bins)
    random.shuffle(items)


PICK_STRATEGY = {
    PickStrategy.SMALLER_FIRST: _smaller_first,
    PickStrategy.BIGGER_FIRST: _bigger_first,
    PickStrategy.SHUFFLE: _shuffle,
}


class AbstractPacker:
    def __init__(self) -> None:
        self.bins: list[Bin] = []
        self.items: list[Item] = []
        self._init_state = True

    def copy(self):
        """Copy packer in init state to apply different pack strategies."""
        if self.is_packed:
            raise TypeError("cannot copy packed state")
        if not all(box.is_empty for box in self.bins):
            raise TypeError("bins contain data in unpacked state")
        packer = self.__class__()
        packer.bins = [box.copy() for box in self.bins]
        packer.items = [item.copy() for item in self.items]
        return packer

    @property
    def is_packed(self) -> bool:
        """Returns ``True`` if packer is packed, each packer can only be used
        once.
        """
        return not self._init_state

    @property
    def unfitted_items(self) -> list[Item]:  # just an alias
        """Returns the unfitted items."""
        return self.items

    def __str__(self) -> str:
        fill = ""
        if self.is_packed:
            fill = f", fill ratio: {self.get_fill_ratio()}"
        return f"{self.__class__.__name__}, {len(self.bins)} bins{fill}"

    def append_bin(self, box: Bin) -> None:
        """Append a container."""
        if self.is_packed:
            raise TypeError("cannot append bins to packed state")
        if not box.is_empty:
            raise TypeError("cannot append bins with content")
        self.bins.append(box)

    def append_item(self, item: Item) -> None:
        """Append a item."""
        if self.is_packed:
            raise TypeError("cannot append items to packed state")
        self.items.append(item)

    def get_fill_ratio(self) -> float:
        """Return the fill ratio of all bins."""
        total_capacity = self.get_capacity()
        if total_capacity == 0.0:
            return 0.0
        return self.get_total_volume() / total_capacity

    def get_capacity(self) -> float:
        """Returns the maximum fill volume of all bins."""
        return sum(box.get_capacity() for box in self.bins)

    def get_total_weight(self) -> float:
        """Returns the total weight of all fitted items in all bins."""
        return sum(box.get_total_weight() for box in self.bins)

    def get_total_volume(self) -> float:
        """Returns the total volume of all fitted items in all bins."""
        return sum(box.get_total_volume() for box in self.bins)

    def get_unfitted_volume(self) -> float:
        """Returns the total volume of all unfitted items."""
        return sum(item.get_volume() for item in self.items)

    def pack(self, pick=PickStrategy.BIGGER_FIRST) -> None:
        """Pack items into bins. Distributes all items across all bins."""
        PICK_STRATEGY[pick](self.bins, self.items)
        # items are removed from self.items while packing!
        self._pack(self.bins, list(self.items))
        # unfitted items remain in self.items

    def _pack(self, bins: Iterable[Bin], items: Iterable[Item]) -> None:
        """Pack items into bins, removes packed items from self.items!"""
        self._init_state = False
        for box in bins:
            for item in items:
                if self.pack_to_bin(box, item):
                    self.items.remove(item)
        # unfitted items remain in self.items

    def pack_to_bin(self, box: Bin, item: Item) -> bool:
        if not box.items:
            return box.put_item(item, START_POSITION)

        for axis in self._axis():
            for placed_item in box.items:
                w, h, d = placed_item.get_dimension()
                x, y, z = placed_item.position
                if axis == Axis.WIDTH:
                    pivot = (x + w, y, z)  # new item right of the placed item
                elif axis == Axis.HEIGHT:
                    pivot = (x, y + h, z)  # new item above of the placed item
                elif axis == Axis.DEPTH:
                    pivot = (x, y, z + d)  # new item on top of the placed item
                else:
                    raise TypeError(axis)
                if box.put_item(item, pivot):
                    return True
        return False

    @staticmethod
    def _axis() -> Iterable[Axis]:
        return Axis


def shuffle_pack(packer: AbstractPacker, attempts: int) -> AbstractPacker:
    """Random shuffle packing. Returns a new packer with the best packing result,
    the input packer is unchanged.
    """
    if attempts < 1:
        raise ValueError("expected attempts >= 1")
    best_ratio = 0.0
    best_packer = packer
    for _ in range(attempts):
        new_packer = packer.copy()
        new_packer.pack(PickStrategy.SHUFFLE)
        new_ratio = new_packer.get_fill_ratio()
        if new_ratio > best_ratio:
            best_ratio = new_ratio
            best_packer = new_packer
    return best_packer


def pack_item_subset(
    packer: AbstractPacker, picker: Iterable, strategy=PickStrategy.BIGGER_FIRST
) -> None:
    """Pack a subset of `packer.items`, which are chosen by an iterable
    yielding a True or False value for each item.
    """
    assert packer.is_packed is False
    chosen, rejects = get_item_subset(packer.items, picker)
    packer.items = chosen
    packer.pack(strategy)  # unfitted items remain in packer.items
    packer.items.extend(rejects)  # append rejects as unfitted items


def get_item_subset(
    items: list[Item], picker: Iterable
) -> tuple[list[Item], list[Item]]:
    """Returns a subset of `items`, where items are chosen by an iterable
    yielding a True or False value for each item.
    """
    chosen: list[Item] = []
    rejects: list[Item] = []
    count = 0
    for item, pick in zip(items, picker):
        count += 1
        if pick:
            chosen.append(item)
        else:
            rejects.append(item)

    if count < len(items):  # too few pick values given
        rejects.extend(items[count:])
    return chosen, rejects


class SubSetEvaluator(ga.Evaluator):
    def __init__(self, packer: AbstractPacker):
        self.packer = packer

    def evaluate(self, dna: ga.DNA) -> float:
        packer = self.run_packer(dna)
        return packer.get_fill_ratio()

    def run_packer(self, dna: ga.DNA) -> AbstractPacker:
        packer = self.packer.copy()
        pack_item_subset(packer, dna)
        return packer


class Packer(AbstractPacker):
    """3D Packer inherited from :class:`AbstractPacker`."""

    def add_bin(
        self,
        name: str,
        width: float,
        height: float,
        depth: float,
        max_weight: float = UNLIMITED_WEIGHT,
    ) -> Box:
        """Add a 3D :class:`Box` container."""
        box = Box(name, width, height, depth, max_weight)
        self.append_bin(box)
        return box

    def add_item(
        self,
        payload,
        width: float,
        height: float,
        depth: float,
        weight: float = 0.0,
    ) -> Item:
        """Add a 3D :class:`Item` to pack."""
        item = Item(payload, width, height, depth, weight)
        self.append_item(item)
        return item


class FlatPacker(AbstractPacker):
    """2D Packer inherited from :class:`AbstractPacker`. All containers and
    items used by this packer must have a depth of 1."""

    def add_bin(
        self,
        name: str,
        width: float,
        height: float,
        max_weight: float = UNLIMITED_WEIGHT,
    ) -> Envelope:
        """Add a 2D :class:`Envelope` container."""
        envelope = Envelope(name, width, height, max_weight)
        self.append_bin(envelope)
        return envelope

    def add_item(
        self,
        payload,
        width: float,
        height: float,
        weight: float = 0.0,
    ) -> Item:
        """Add a 2D :class:`FlatItem` to pack."""
        item = FlatItem(payload, width, height, weight)
        self.append_item(item)
        return item

    @staticmethod
    def _axis() -> Iterable[Axis]:
        return Axis.WIDTH, Axis.HEIGHT


def export_dxf(
    layout: "GenericLayoutType", bins: list[Bin], offset: UVec = (1, 0, 0)
) -> None:
    from ezdxf import colors

    offset_vec = Vec3(offset)
    start = Vec3()
    index = 0
    rgb = (colors.RED, colors.GREEN, colors.BLUE, colors.MAGENTA, colors.CYAN)
    for box in bins:
        m = Matrix44.translate(start.x, start.y, start.z)
        _add_frame(layout, box, "FRAME", m)
        for item in box.items:
            _add_mesh(layout, item, "ITEMS", rgb[index], m)
            index += 1
            if index >= len(rgb):
                index = 0
        start += offset_vec


def _add_frame(layout: "GenericLayoutType", box: Bin, layer: str, m: Matrix44):
    def add_line(v1, v2):
        line = layout.add_line(v1, v2, dxfattribs=attribs)
        line.transform(m)

    attribs = {"layer": layer}
    x0, y0, z0 = (0.0, 0.0, 0.0)
    x1 = float(box.width)
    y1 = float(box.height)
    z1 = float(box.depth)
    corners = [
        (x0, y0),
        (x1, y0),
        (x1, y1),
        (x0, y1),
        (x0, y0),
    ]
    for (sx, sy), (ex, ey) in zip(corners, corners[1:]):
        add_line((sx, sy, z0), (ex, ey, z0))
        add_line((sx, sy, z1), (ex, ey, z1))
    for x, y in corners[:-1]:
        add_line((x, y, z0), (x, y, z1))

    text = layout.add_text(box.name, height=0.25, dxfattribs=attribs)
    text.set_placement((x0 + 0.25, y1 - 0.5, z1))
    text.transform(m)


def _add_mesh(
    layout: "GenericLayoutType", item: Item, layer: str, color: int, m: Matrix44
):
    from ezdxf.render.forms import cube

    attribs = {
        "layer": layer,
        "color": color,
    }
    mesh = cube(center=False)
    sx, sy, sz = item.get_dimension()
    mesh.scale(sx, sy, sz)
    x, y, z = item.position
    mesh.translate(x, y, z)
    mesh.render_polyface(layout, attribs, matrix=m)
    text = layout.add_text(
        str(item.payload), height=0.25, dxfattribs={"layer": "TEXT"}
    )
    if sy > sx:
        text.dxf.rotation = 90
        align = TextEntityAlignment.TOP_LEFT
    else:
        align = TextEntityAlignment.BOTTOM_LEFT
    text.set_placement((x + 0.25, y + 0.25, z + sz), align=align)
    text.transform(m)
