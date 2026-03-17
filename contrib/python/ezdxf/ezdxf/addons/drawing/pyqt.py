# Copyright (c) 2020-2023, Matthew Broadway
# License: MIT License
# mypy: ignore_errors=True
from __future__ import annotations
from typing import Optional, Iterable
import abc
import math

import numpy as np

from ezdxf.addons.xqt import QtCore as qc, QtGui as qg, QtWidgets as qw
from ezdxf.addons.drawing.backend import Backend, BkPath2d, BkPoints2d, ImageData
from ezdxf.addons.drawing.config import Configuration
from ezdxf.addons.drawing.type_hints import Color
from ezdxf.addons.drawing.properties import BackendProperties
from ezdxf.math import Vec2, Matrix44
from ezdxf.npshapes import to_qpainter_path


class _Point(qw.QAbstractGraphicsShapeItem):
    """A dimensionless point which is drawn 'cosmetically' (scale depends on
    view)
    """

    def __init__(self, x: float, y: float, brush: qg.QBrush):
        super().__init__()
        self.location = qc.QPointF(x, y)
        self.radius = 1.0
        self.setPen(qg.QPen(qc.Qt.NoPen))
        self.setBrush(brush)

    def paint(
        self,
        painter: qg.QPainter,
        option: qw.QStyleOptionGraphicsItem,
        widget: Optional[qw.QWidget] = None,
    ) -> None:
        view_scale = _get_x_scale(painter.transform())
        radius = self.radius / view_scale
        painter.setBrush(self.brush())
        painter.setPen(qc.Qt.NoPen)
        painter.drawEllipse(self.location, radius, radius)

    def boundingRect(self) -> qc.QRectF:
        return qc.QRectF(self.location, qc.QSizeF(1, 1))


# The key used to store the dxf entity corresponding to each graphics element
CorrespondingDXFEntity = qc.Qt.UserRole + 0  # type: ignore
CorrespondingDXFParentStack = qc.Qt.UserRole + 1  # type: ignore


class _PyQtBackend(Backend):
    """
    Abstract PyQt backend which uses the :mod:`PySide6` package to implement an
    interactive viewer. The :mod:`PyQt5` package can be used as fallback if the
    :mod:`PySide6` package is not available.
    """

    def __init__(self, scene: qw.QGraphicsScene):
        super().__init__()
        self._scene = scene
        self._color_cache: dict[Color, qg.QColor] = {}
        self._no_line = qg.QPen(qc.Qt.NoPen)
        self._no_fill = qg.QBrush(qc.Qt.NoBrush)

    def configure(self, config: Configuration) -> None:
        if config.min_lineweight is None:
            config = config.with_changes(min_lineweight=0.24)
        super().configure(config)

    def set_scene(self, scene: qw.QGraphicsScene) -> None:
        self._scene = scene

    def _add_item(self, item: qw.QGraphicsItem, entity_handle: str) -> None:
        self.set_item_data(item, entity_handle)
        self._scene.addItem(item)

    @abc.abstractmethod
    def set_item_data(self, item: qw.QGraphicsItem, entity_handle: str) -> None:
        ...

    def _get_color(self, color: Color) -> qg.QColor:
        try:
            return self._color_cache[color]
        except KeyError:
            pass
        if len(color) == 7:
            qt_color = qg.QColor(color)  # '#RRGGBB'
        elif len(color) == 9:
            rgb = color[1:7]
            alpha = color[7:9]
            qt_color = qg.QColor(f"#{alpha}{rgb}")  # '#AARRGGBB'
        else:
            raise TypeError(color)

        self._color_cache[color] = qt_color
        return qt_color

    def _get_pen(self, properties: BackendProperties) -> qg.QPen:
        """Returns a cosmetic pen with applied lineweight but without line type
        support.
        """
        px = properties.lineweight / 0.3527 * self.config.lineweight_scaling
        pen = qg.QPen(self._get_color(properties.color), px)
        # Use constant width in pixel:
        pen.setCosmetic(True)
        pen.setJoinStyle(qc.Qt.RoundJoin)
        return pen

    def _get_fill_brush(self, color: Color) -> qg.QBrush:
        return qg.QBrush(self._get_color(color), qc.Qt.SolidPattern)  # type: ignore

    def set_background(self, color: Color):
        self._scene.setBackgroundBrush(qg.QBrush(self._get_color(color)))

    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        """Draw a real dimensionless point."""
        brush = self._get_fill_brush(properties.color)
        item = _Point(pos.x, pos.y, brush)
        self._add_item(item, properties.handle)

    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        # PyQt draws a long line for a zero-length line:
        if start.isclose(end):
            self.draw_point(start, properties)
        else:
            item = qw.QGraphicsLineItem(start.x, start.y, end.x, end.y)
            item.setPen(self._get_pen(properties))
            self._add_item(item, properties.handle)

    def draw_solid_lines(
        self,
        lines: Iterable[tuple[Vec2, Vec2]],
        properties: BackendProperties,
    ):
        """Fast method to draw a bunch of solid lines with the same properties."""
        pen = self._get_pen(properties)
        add_line = self._add_item
        for s, e in lines:
            if s.isclose(e):
                self.draw_point(s, properties)
            else:
                item = qw.QGraphicsLineItem(s.x, s.y, e.x, e.y)
                item.setPen(pen)
                add_line(item, properties.handle)

    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        if len(path) == 0:
            return
        item = qw.QGraphicsPathItem(to_qpainter_path([path]))
        item.setPen(self._get_pen(properties))
        item.setBrush(self._no_fill)
        self._add_item(item, properties.handle)

    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        # Default fill rule is OddEvenFill! Detecting the path orientation is not
        # necessary!
        _paths = list(paths)
        if len(_paths) == 0:
            return
        item = _CosmeticPath(to_qpainter_path(_paths))
        item.setPen(self._get_pen(properties))
        item.setBrush(self._get_fill_brush(properties.color))
        self._add_item(item, properties.handle)

    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        brush = self._get_fill_brush(properties.color)
        polygon = qg.QPolygonF()
        for p in points.vertices():
            polygon.append(qc.QPointF(p.x, p.y))
        item = _CosmeticPolygon(polygon)
        item.setPen(self._no_line)
        item.setBrush(brush)
        self._add_item(item, properties.handle)

    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        image = image_data.image
        transform = image_data.transform
        height, width, depth = image.shape
        assert depth == 4
        bytes_per_row = width * depth
        image = np.ascontiguousarray(np.flip(image, axis=0))
        pixmap = qg.QPixmap(
            qg.QImage(
                image.data,
                width,
                height,
                bytes_per_row,
                qg.QImage.Format.Format_RGBA8888,
            )
        )
        item = qw.QGraphicsPixmapItem()
        item.setPixmap(pixmap)
        item.setTransformationMode(qc.Qt.TransformationMode.SmoothTransformation)
        item.setTransform(_matrix_to_qtransform(transform))
        self._add_item(item, properties.handle)

    def clear(self) -> None:
        self._scene.clear()

    def finalize(self) -> None:
        super().finalize()
        self._scene.setSceneRect(self._scene.itemsBoundingRect())


class PyQtBackend(_PyQtBackend):
    """
    Backend which uses the :mod:`PySide6` package to implement an interactive
    viewer. The :mod:`PyQt5` package can be used as fallback if the :mod:`PySide6`
    package is not available.

    Args:
        scene: drawing canvas of type :class:`QtWidgets.QGraphicsScene`,
            if ``None`` a new canvas will be created
    """

    def __init__(
        self,
        scene: Optional[qw.QGraphicsScene] = None,
    ):
        super().__init__(scene or qw.QGraphicsScene())

    # This implementation keeps all virtual entities alive by attaching references
    # to entities to the graphic scene items.

    def set_item_data(self, item: qw.QGraphicsItem, entity_handle: str) -> None:
        parent_stack = tuple(e for e, props in self.entity_stack[:-1])
        current_entity = self.current_entity
        item.setData(CorrespondingDXFEntity, current_entity)
        item.setData(CorrespondingDXFParentStack, parent_stack)


class PyQtPlaybackBackend(_PyQtBackend):
    """
    Backend which uses the :mod:`PySide6` package to implement an interactive
    viewer. The :mod:`PyQt5` package can be used as fallback if the :mod:`PySide6`
    package is not available.

    This backend can be used a playback backend for the :meth:`replay` method of the
    :class:`Player` class

    Args:
        scene: drawing canvas of type :class:`QtWidgets.QGraphicsScene`,
            if ``None`` a new canvas will be created
    """

    def __init__(
        self,
        scene: Optional[qw.QGraphicsScene] = None,
    ):
        super().__init__(scene or qw.QGraphicsScene())

    # The backend recorder does not record enter_entity() and exit_entity() events.
    # This implementation attaches only entity handles (str) to the graphic scene items.
    # Each item references the top level entity e.g. all items of a block reference
    # references the handle of the INSERT entity.

    def set_item_data(self, item: qw.QGraphicsItem, entity_handle: str) -> None:
        item.setData(CorrespondingDXFEntity, entity_handle)


class _CosmeticPath(qw.QGraphicsPathItem):
    def paint(
        self,
        painter: qg.QPainter,
        option: qw.QStyleOptionGraphicsItem,
        widget: Optional[qw.QWidget] = None,
    ) -> None:
        _set_cosmetic_brush(self, painter)
        super().paint(painter, option, widget)


class _CosmeticPolygon(qw.QGraphicsPolygonItem):
    def paint(
        self,
        painter: qg.QPainter,
        option: qw.QStyleOptionGraphicsItem,
        widget: Optional[qw.QWidget] = None,
    ) -> None:
        _set_cosmetic_brush(self, painter)
        super().paint(painter, option, widget)


def _set_cosmetic_brush(
    item: qw.QAbstractGraphicsShapeItem, painter: qg.QPainter
) -> None:
    """like a cosmetic pen, this sets the brush pattern to appear the same independent of the view"""
    brush = item.brush()
    # scale by -1 in y because the view is always mirrored in y and undoing the view transformation entirely would make
    # the hatch mirrored w.r.t the view
    brush.setTransform(painter.transform().inverted()[0].scale(1, -1))  # type: ignore
    item.setBrush(brush)


def _get_x_scale(t: qg.QTransform) -> float:
    return math.sqrt(t.m11() * t.m11() + t.m21() * t.m21())


def _matrix_to_qtransform(matrix: Matrix44) -> qg.QTransform:
    """Qt also uses row-vectors so the translation elements are placed in the
    bottom row.

    This is only a simple conversion which assumes that although the
    transformation is 4x4,it does not involve the z axis.

    A more correct transformation could be implemented like so:
    https://stackoverflow.com/questions/10629737/convert-3d-4x4-rotation-matrix-into-2d
    """
    return qg.QTransform(*matrix.get_2d_transformation())
