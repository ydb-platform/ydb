#  Copyright (c) 2023-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Iterator,
    NamedTuple,
    Optional,
    Tuple,
    List,
    TYPE_CHECKING,
    Sequence,
)
import enum
import logging

from ezdxf.math import (
    Matrix44,
    UVec,
    Vec3,
    NonUniformScalingError,
    InsertTransformationError,
)
from ezdxf.entities import (
    DXFEntity,
    DXFGraphic,
    Circle,
    LWPolyline,
    Polyline,
    Ellipse,
    is_graphic_entity,
)
from ezdxf.entities.copy import default_copy, CopyNotSupported
from ezdxf.protocols import SupportsTemporaryTransformation
from ezdxf.document import Drawing

if TYPE_CHECKING:
    from ezdxf.layouts import BlockLayout

__all__ = [
    "Logger",
    "Error",
    "inplace",
    "copies",
    "translate",
    "scale_uniform",
    "scale",
    "x_rotate",
    "y_rotate",
    "z_rotate",
    "axis_rotate",
    "transform_entity_by_blockref",
    "transform_entities_by_blockref",
]
MIN_SCALING_FACTOR = 1e-12
logger = logging.getLogger("ezdxf")


class Error(enum.Enum):
    NONE = 0
    TRANSFORMATION_NOT_SUPPORTED = enum.auto()
    COPY_NOT_SUPPORTED = enum.auto()
    NON_UNIFORM_SCALING_ERROR = enum.auto()
    INSERT_TRANSFORMATION_ERROR = enum.auto()
    VIRTUAL_ENTITY_NOT_SUPPORTED = enum.auto()


class Logger:
    class Entry(NamedTuple):
        error: Error
        message: str
        entity: DXFEntity

    def __init__(self) -> None:
        self._entries: list[Logger.Entry] = []

    def __getitem__(self, index: int) -> Entry:
        """Returns the error entry at `index`."""
        return self._entries[index]

    def __iter__(self) -> Iterator[Entry]:
        """Iterates over all error entries."""
        return iter(self._entries)

    def __len__(self) -> int:
        """Returns the count of error entries."""
        return len(self._entries)

    def add(self, error: Error, message: str, entity: DXFEntity):
        self._entries.append(Logger.Entry(error, message, entity))

    def messages(self) -> list[str]:
        """Returns all error messages as list of strings."""
        return [entry.message for entry in self._entries]

    def purge(self, entries: Iterable[Entry]) -> None:
        for entry in entries:
            try:
                self._entries.remove(entry)
            except ValueError:
                pass


def _inplace(entities: Iterable[DXFEntity], m: Matrix44) -> Logger:
    """Transforms the given `entities` inplace by the transformation matrix `m`,
    non-uniform scaling is not supported. The function logs errors and does not raise
    errors for unsupported entities or transformations that cannot be performed,
    see enum :class:`Error`.
    The :func:`inplace` function supports virtual entities as well.

    """
    log = Logger()
    for entity in entities:
        if not entity.is_alive:
            continue
        try:
            entity.transform(m)  # type: ignore
        except (AttributeError, NotImplementedError):
            log.add(
                Error.TRANSFORMATION_NOT_SUPPORTED,
                f"{str(entity)} entity does not support transformation",
                entity,
            )
        except NonUniformScalingError:
            log.add(
                Error.NON_UNIFORM_SCALING_ERROR,
                f"{str(entity)} entity does not support non-uniform scaling",
                entity,
            )
        except InsertTransformationError:
            log.add(
                Error.INSERT_TRANSFORMATION_ERROR,
                f"{str(entity)} entity can not represent a non-orthogonal target coordinate system",
                entity,
            )

    return log


def inplace(
    entities: Iterable[DXFEntity],
    m: Matrix44,
) -> Logger:
    """Transforms the given `entities` inplace by the transformation matrix `m`,
    non-uniform scaling is supported. The function converts circular arcs into ellipses
    to perform non-uniform scaling.  The function logs errors and does not raise errors
    for unsupported entities or transformation errors, see enum :class:`Error`.

    .. important::

        The :func:`inplace` function does not support type conversion for virtual
        entities e.g. non-uniform scaling for CIRCLE, ARC or POLYLINE with bulges,
        see also function :func:`copies`.

    """
    log = _inplace(entities, m)
    errors: list[Logger.Entry] = []
    for entry in log:
        if entry.error != Error.NON_UNIFORM_SCALING_ERROR:
            continue

        entity = entry.entity
        if entity.is_virtual:
            errors.append(entry)
            log.add(
                Error.VIRTUAL_ENTITY_NOT_SUPPORTED,
                f"non-uniform scaling is not supported for virtual entity {str(entity)}",
                entity,
            )
            continue

        if isinstance(entity, Circle):  # CIRCLE, ARC
            errors.append(entry)
            ellipse = entity.to_ellipse(replace=True)
            ellipse.transform(m)
        elif isinstance(entity, (LWPolyline, Polyline)):  # has bulges (circular arcs)
            errors.append(entry)
            for sub_entity in entity.explode():
                if isinstance(sub_entity, Circle):
                    sub_entity = sub_entity.to_ellipse()
                sub_entity.transform(m)  # type: ignore
        # else: NON_UNIFORM_SCALING_ERROR stays unchanged
    log.purge(errors)
    return log


def copies(
    entities: Iterable[DXFEntity], m: Optional[Matrix44] = None
) -> Tuple[Logger, List[DXFEntity]]:
    """Copy entities and transform them by matrix `m`. Does not raise any exception
    and ignores all entities that cannot be copied or transformed. Just copies the input
    entities if matrix `m` is ``None``. Returns a tuple of :class:`Logger` and a list of
    transformed virtual copies. The function supports virtual entities as input and
    converts circular arcs into ellipses to perform non-uniform scaling.
    """

    log = Logger()
    clones = _copy_entities(entities, log)
    if isinstance(m, Matrix44):
        clones = _transform_clones(clones, m, log)
    return log, clones


def _copy_entities(entities: Iterable[DXFEntity], log: Logger) -> list[DXFEntity]:
    clones: list[DXFEntity] = []
    for entity in entities:
        if not entity.is_alive:
            continue
        try:
            clone = entity.copy(copy_strategy=default_copy)
        except CopyNotSupported:
            log.add(
                Error.COPY_NOT_SUPPORTED,
                f"{str(entity)} entity does not support copy",
                entity,
            )
        else:
            clones.append(clone)
    return clones


def _transform_clones(clones: Iterable[DXFEntity], m: Matrix44, log: Logger):
    entities: List[DXFEntity] = []
    for entity in clones:
        try:
            entity.transform(m)  # type: ignore
        except (AttributeError, NotImplementedError):
            log.add(
                Error.TRANSFORMATION_NOT_SUPPORTED,
                f"{str(entity)} entity does not support transformation",
                entity,
            )
        except InsertTransformationError:
            log.add(
                Error.INSERT_TRANSFORMATION_ERROR,
                f"{str(entity)} entity can not represent a non-orthogonal target coordinate system",
                entity,
            )
        except NonUniformScalingError:
            try:
                entities.extend(_scale_non_uniform(entity, m))
            except TypeError:
                log.add(
                    Error.NON_UNIFORM_SCALING_ERROR,
                    f"{str(entity)} entity does not support non-uniform scaling",
                    entity,
                )
        else:
            entities.append(entity)

    return entities


def _scale_non_uniform(entity: DXFEntity, m: Matrix44):
    sub_entity: DXFGraphic
    if isinstance(entity, Circle):
        sub_entity = Ellipse.from_arc(entity)
        sub_entity.transform(m)
        yield sub_entity
    elif isinstance(entity, (LWPolyline, Polyline)):
        for sub_entity in entity.virtual_entities():
            if isinstance(sub_entity, Circle):
                sub_entity = Ellipse.from_arc(sub_entity)
            sub_entity.transform(m)
            yield sub_entity
    else:
        raise TypeError


def translate(entities: Iterable[DXFEntity], offset: UVec) -> Logger:
    """Translates (moves) `entities` inplace by the `offset` vector."""
    v = Vec3(offset)
    if v:
        return _inplace(entities, m=Matrix44.translate(v.x, v.y, v.z))
    return Logger()


def scale_uniform(entities: Iterable[DXFEntity], factor: float) -> Logger:
    """Scales `entities` inplace by a `factor` in all axis. Scaling factors smaller than
    :attr:`MIN_SCALING_FACTOR` are ignored.

    """
    f = float(factor)
    if abs(f) > MIN_SCALING_FACTOR:
        return _inplace(entities, m=Matrix44.scale(f, f, f))
    return Logger()


def scale(entities: Iterable[DXFEntity], sx: float, sy: float, sz: float) -> Logger:
    """Scales `entities` inplace by the factors `sx` in x-axis, `sy` in y-axis and `sz`
    in z-axis. Scaling factors smaller than :attr:`MIN_SCALING_FACTOR` are ignored.

    .. important::

        same limitations for virtual entities as the :func:`inplace` function

    """

    def safe(f: float) -> float:
        f = float(f)
        return f if abs(f) > MIN_SCALING_FACTOR else 1.0

    return inplace(entities, Matrix44.scale(safe(sx), safe(sy), safe(sz)))


def x_rotate(entities: Iterable[DXFEntity], angle: float) -> Logger:
    """Rotates `entities` inplace by `angle` in radians about the x-axis."""
    a = float(angle)
    if a:
        return _inplace(entities, m=Matrix44.x_rotate(a))
    return Logger()


def y_rotate(entities: Iterable[DXFEntity], angle: float) -> Logger:
    """Rotates `entities` inplace by `angle` in radians about the y-axis."""
    a = float(angle)
    if a:
        return _inplace(entities, m=Matrix44.y_rotate(a))
    return Logger()


def z_rotate(entities: Iterable[DXFEntity], angle: float) -> Logger:
    """Rotates `entities` inplace by `angle` in radians about the x-axis."""
    a = float(angle)
    if a:
        return _inplace(entities, m=Matrix44.z_rotate(a))
    return Logger()


def axis_rotate(entities: Iterable[DXFEntity], axis: UVec, angle: float) -> Logger:
    """Rotates `entities` inplace by `angle` in radians about the rotation axis starting
    at the origin pointing in `axis` direction.
    """
    a = float(angle)
    if not a:
        return Logger()

    v = Vec3(axis)
    if not v.is_null:
        return _inplace(entities, m=Matrix44.axis_rotate(v, a))
    return Logger()


def transform_entity_by_blockref(entity: DXFEntity, m: Matrix44) -> bool:
    """Apply a transformation by moving an entity into a block and replacing the entity
    by a block reference with the applied transformation.
    """
    return _transform_by_blockref([entity], m) is not None


def transform_entities_by_blockref(
    entities: Iterable[DXFEntity], m: Matrix44
) -> BlockLayout | None:
    """Apply a transformation by moving entities into a block and replacing the entities
    by a block reference with the applied transformation.
    """
    return _transform_by_blockref(list(entities), m)


def _transform_by_blockref(
    entities: Sequence[DXFEntity], m: Matrix44
) -> BlockLayout | None:
    if len(entities) == 0:
        return None

    first_entity = entities[0]
    if not is_graphic_entity(first_entity):
        return None

    doc = first_entity.doc
    if doc is None:
        return None

    layout = first_entity.get_layout()
    if layout is None:
        return None

    block = doc.blocks.new_anonymous_block()
    insert = layout.add_blockref(block.name, (0, 0, 0))
    try:
        insert.transform(m)
    except InsertTransformationError:
        logger.warning(f"cannot apply invalid transformation")
        layout.delete_entity(insert)
        doc.blocks.delete_block(block.name, safe=False)
        return None

    for e in entities:
        if is_graphic_entity(e):
            layout.move_to_layout(e, block)
    return block


def apply_temporary_transformations(entities: Iterable[DXFEntity]) -> None:
    for entity in entities:
        if isinstance(entity, SupportsTemporaryTransformation):
            tt = entity.temporary_transformation()
            tt.apply_transformation(entity)
