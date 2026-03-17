#!/usr/bin/env python3
# Copyright (c) 2020-2023, Matthew Broadway
# License: MIT License
# mypy: ignore_errors=True
from __future__ import annotations
from typing import Iterable, Sequence, Set, Optional
import math
import os
import time

from ezdxf.addons.xqt import QtWidgets as qw, QtCore as qc, QtGui as qg
from ezdxf.addons.xqt import Slot, QAction, Signal

import ezdxf
import ezdxf.bbox
from ezdxf import recover
from ezdxf.addons import odafc
from ezdxf.addons.drawing import Frontend, RenderContext
from ezdxf.addons.drawing.config import Configuration

from ezdxf.addons.drawing.properties import (
    is_dark_color,
    set_layers_state,
    LayerProperties,
)
from ezdxf.addons.drawing.pyqt import (
    _get_x_scale,
    PyQtBackend,
    CorrespondingDXFEntity,
    CorrespondingDXFParentStack,
)
from ezdxf.audit import Auditor
from ezdxf.document import Drawing
from ezdxf.entities import DXFGraphic, DXFEntity
from ezdxf.layouts import Layout
from ezdxf.lldxf.const import DXFStructureError


class CADGraphicsView(qw.QGraphicsView):
    closing = Signal()

    def __init__(
        self,
        *,
        view_buffer: float = 0.2,
        zoom_per_scroll_notch: float = 0.2,
        loading_overlay: bool = True,
    ):
        super().__init__()
        self._zoom = 1.0
        self._default_zoom = 1.0
        self._zoom_limits = (0.5, 100)
        self._zoom_per_scroll_notch = zoom_per_scroll_notch
        self._view_buffer = view_buffer
        self._loading_overlay = loading_overlay
        self._is_loading = False

        self.setTransformationAnchor(qw.QGraphicsView.AnchorUnderMouse)
        self.setResizeAnchor(qw.QGraphicsView.AnchorUnderMouse)
        self.setVerticalScrollBarPolicy(qc.Qt.ScrollBarAlwaysOff)
        self.setHorizontalScrollBarPolicy(qc.Qt.ScrollBarAlwaysOff)
        self.setDragMode(qw.QGraphicsView.ScrollHandDrag)
        self.setFrameShape(qw.QFrame.NoFrame)
        self.setRenderHints(
            qg.QPainter.Antialiasing
            | qg.QPainter.TextAntialiasing
            | qg.QPainter.SmoothPixmapTransform
        )

        self.setScene(qw.QGraphicsScene())
        self.scale(1, -1)  # so that +y is up

    def closeEvent(self, event: qg.QCloseEvent) -> None:
        super().closeEvent(event)
        self.closing.emit()

    def clear(self):
        pass

    def begin_loading(self):
        self._is_loading = True
        self.scene().invalidate(qc.QRectF(), qw.QGraphicsScene.AllLayers)
        qw.QApplication.processEvents()

    def end_loading(self, new_scene: qw.QGraphicsScene):
        self.setScene(new_scene)
        self._is_loading = False
        self.buffer_scene_rect()
        self.scene().invalidate(qc.QRectF(), qw.QGraphicsScene.AllLayers)

    def buffer_scene_rect(self):
        scene = self.scene()
        r = scene.sceneRect()
        bx, by = (
            r.width() * self._view_buffer / 2,
            r.height() * self._view_buffer / 2,
        )
        scene.setSceneRect(r.adjusted(-bx, -by, bx, by))

    def fit_to_scene(self):
        self.fitInView(self.sceneRect(), qc.Qt.KeepAspectRatio)
        self._default_zoom = _get_x_scale(self.transform())
        self._zoom = 1

    def _get_zoom_amount(self) -> float:
        return _get_x_scale(self.transform()) / self._default_zoom

    def wheelEvent(self, event: qg.QWheelEvent) -> None:
        # dividing by 120 gets number of notches on a typical scroll wheel.
        # See QWheelEvent documentation
        delta_notches = event.angleDelta().y() / 120
        direction = math.copysign(1, delta_notches)
        factor = (1.0 + self._zoom_per_scroll_notch * direction) ** abs(delta_notches)
        resulting_zoom = self._zoom * factor
        if resulting_zoom < self._zoom_limits[0]:
            factor = self._zoom_limits[0] / self._zoom
        elif resulting_zoom > self._zoom_limits[1]:
            factor = self._zoom_limits[1] / self._zoom
        self.scale(factor, factor)
        self._zoom *= factor

    def save_view(self) -> SavedView:
        return SavedView(
            self.transform(),
            self._default_zoom,
            self._zoom,
            self.horizontalScrollBar().value(),
            self.verticalScrollBar().value(),
        )

    def restore_view(self, view: SavedView):
        self.setTransform(view.transform)
        self._default_zoom = view.default_zoom
        self._zoom = view.zoom
        self.horizontalScrollBar().setValue(view.x)
        self.verticalScrollBar().setValue(view.y)

    def drawForeground(self, painter: qg.QPainter, rect: qc.QRectF) -> None:
        if self._is_loading and self._loading_overlay:
            painter.save()
            painter.fillRect(rect, qg.QColor("#aa000000"))
            painter.setWorldMatrixEnabled(False)
            r = self.viewport().rect()
            painter.setBrush(qc.Qt.NoBrush)
            painter.setPen(qc.Qt.white)
            painter.drawText(r.center(), "Loading...")
            painter.restore()


class SavedView:
    def __init__(
        self, transform: qg.QTransform, default_zoom: float, zoom: float, x: int, y: int
    ):
        self.transform = transform
        self.default_zoom = default_zoom
        self.zoom = zoom
        self.x = x
        self.y = y


class CADGraphicsViewWithOverlay(CADGraphicsView):
    mouse_moved = Signal(qc.QPointF)
    element_hovered = Signal(object, int)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._selected_items: list[qw.QGraphicsItem] = []
        self._selected_index = None
        self._mark_selection = True

    @property
    def current_hovered_element(self) -> Optional[DXFEntity]:
        if self._selected_items:
            graphics_item = self._selected_items[self._selected_index]
            dxf_entity = graphics_item.data(CorrespondingDXFEntity)
            return dxf_entity
        else:
            return None

    def clear(self):
        super().clear()
        self._selected_items = None
        self._selected_index = None

    def begin_loading(self):
        self.clear()
        super().begin_loading()

    def drawForeground(self, painter: qg.QPainter, rect: qc.QRectF) -> None:
        super().drawForeground(painter, rect)
        if self._selected_items and self._mark_selection:
            item = self._selected_items[self._selected_index]
            r = item.sceneTransform().mapRect(item.boundingRect())
            painter.fillRect(r, qg.QColor(0, 255, 0, 100))

    def mouseMoveEvent(self, event: qg.QMouseEvent) -> None:
        super().mouseMoveEvent(event)
        pos = self.mapToScene(event.pos())
        self.mouse_moved.emit(pos)
        selected_items = self.scene().items(pos)
        if selected_items != self._selected_items:
            self._selected_items = selected_items
            self._selected_index = 0 if self._selected_items else None
            self._emit_selected()

    def mouseReleaseEvent(self, event: qg.QMouseEvent) -> None:
        super().mouseReleaseEvent(event)
        if event.button() == qc.Qt.LeftButton and self._selected_items:
            self._selected_index = (self._selected_index + 1) % len(
                self._selected_items
            )
            self._emit_selected()

    def _emit_selected(self):
        self.element_hovered.emit(self._selected_items, self._selected_index)
        self.scene().invalidate(self.sceneRect(), qw.QGraphicsScene.ForegroundLayer)

    def toggle_selection_marker(self):
        self._mark_selection = not self._mark_selection


class CADWidget(qw.QWidget):
    def __init__(self, view: CADGraphicsView, config: Configuration = Configuration()):
        super().__init__()
        layout = qw.QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(view)
        self.setLayout(layout)
        self._view = view
        self._view.closing.connect(self.close)
        self._config = config
        self._bbox_cache = ezdxf.bbox.Cache()
        self._doc: Drawing = None  # type: ignore
        self._render_context: RenderContext = None  # type: ignore
        self._visible_layers: set[str] = set()
        self._current_layout: str = "Model"
        self._reset_backend()

    def _reset_backend(self):
        # clear caches
        self._backend = PyQtBackend()

    @property
    def doc(self) -> Drawing:
        return self._doc

    @property
    def view(self) -> CADGraphicsView:
        return self._view

    @property
    def render_context(self) -> RenderContext:
        return self._render_context

    @property
    def current_layout(self) -> str:
        return self._current_layout

    def set_document(
        self,
        document: Drawing,
        *,
        layout: str = "Model",
        draw: bool = True,
    ):
        self._doc = document
        # initialize bounding box cache for faste paperspace drawing
        self._bbox_cache = ezdxf.bbox.Cache()
        self._render_context = self._make_render_context(document)
        self._reset_backend()
        self._visible_layers = set()
        self._current_layout = None
        if draw:
            self.draw_layout(layout)

    def set_visible_layers(self, layers: Set[str]) -> None:
        self._visible_layers = layers
        self.draw_layout(self._current_layout, reset_view=False)

    def _make_render_context(self, doc: Drawing) -> RenderContext:
        def update_layers_state(layers: Sequence[LayerProperties]):
            if self._visible_layers:
                set_layers_state(layers, self._visible_layers, state=True)

        render_context = RenderContext(doc)
        render_context.set_layer_properties_override(update_layers_state)
        return render_context

    def draw_layout(
        self,
        layout_name: str,
        reset_view: bool = True,
    ):
        self._current_layout = layout_name
        self._view.begin_loading()
        new_scene = qw.QGraphicsScene()
        self._backend.set_scene(new_scene)
        layout = self._doc.layout(layout_name)
        self._update_render_context(layout)
        try:
            self._create_frontend().draw_layout(layout)
        finally:
            self._backend.finalize()
        self._view.end_loading(new_scene)
        self._view.buffer_scene_rect()
        if reset_view:
            self._view.fit_to_scene()

    def _create_frontend(self) -> Frontend:
        return Frontend(
            ctx=self._render_context,
            out=self._backend,
            config=self._config,
            bbox_cache=self._bbox_cache,
        )

    def _update_render_context(self, layout: Layout) -> None:
        assert self._render_context is not None
        self._render_context.set_current_layout(layout)


class CADViewer(qw.QMainWindow):
    def __init__(self, cad: Optional[CADWidget] = None):
        super().__init__()
        self._doc: Optional[Drawing] = None
        if cad is None:
            self._cad = CADWidget(CADGraphicsViewWithOverlay(), config=Configuration())
        else:
            self._cad = cad
        self._view = self._cad.view

        if isinstance(self._view, CADGraphicsViewWithOverlay):
            self._view.element_hovered.connect(self._on_element_hovered)
            self._view.mouse_moved.connect(self._on_mouse_moved)

        menu = self.menuBar()
        select_doc_action = QAction("Select Document", self)
        select_doc_action.triggered.connect(self._select_doc)
        menu.addAction(select_doc_action)
        self.select_layout_menu = menu.addMenu("Select Layout")

        toggle_sidebar_action = QAction("Toggle Sidebar", self)
        toggle_sidebar_action.triggered.connect(self._toggle_sidebar)
        menu.addAction(toggle_sidebar_action)

        toggle_selection_marker_action = QAction("Toggle Entity Marker", self)
        toggle_selection_marker_action.triggered.connect(self._toggle_selection_marker)
        menu.addAction(toggle_selection_marker_action)

        self.reload_menu = menu.addMenu("Reload")
        reload_action = QAction("Reload", self)
        reload_action.setShortcut(qg.QKeySequence("F5"))
        reload_action.triggered.connect(self._reload)
        self.reload_menu.addAction(reload_action)
        self.keep_view_action = QAction("Keep View", self)
        self.keep_view_action.setCheckable(True)
        self.keep_view_action.setChecked(True)
        self.reload_menu.addAction(self.keep_view_action)
        watch_action = QAction("Watch", self)
        watch_action.setCheckable(True)
        watch_action.toggled.connect(self._toggle_watch)
        self.reload_menu.addAction(watch_action)
        self._watch_timer = qc.QTimer()
        self._watch_timer.setInterval(50)
        self._watch_timer.timeout.connect(self._check_watch)
        self._watch_mtime = None

        self.sidebar = qw.QSplitter(qc.Qt.Vertical)
        self.layers = qw.QListWidget()
        self.layers.setStyleSheet(
            "QListWidget {font-size: 12pt;} "
            "QCheckBox {font-size: 12pt; padding-left: 5px;}"
        )
        self.sidebar.addWidget(self.layers)
        info_container = qw.QWidget()
        info_layout = qw.QVBoxLayout()
        info_layout.setContentsMargins(0, 0, 0, 0)
        self.selected_info = qw.QPlainTextEdit()
        self.selected_info.setReadOnly(True)
        info_layout.addWidget(self.selected_info)
        self.mouse_pos = qw.QLabel()
        info_layout.addWidget(self.mouse_pos)
        info_container.setLayout(info_layout)
        self.sidebar.addWidget(info_container)

        container = qw.QSplitter()
        self.setCentralWidget(container)
        container.addWidget(self._cad)
        container.addWidget(self.sidebar)
        container.setCollapsible(0, False)
        container.setCollapsible(1, True)
        w = container.width()
        container.setSizes([int(3 * w / 4), int(w / 4)])
        self.setWindowTitle("CAD Viewer")
        self.resize(1600, 900)
        self.show()

    @staticmethod
    def from_config(config: Configuration) -> CADViewer:
        return CADViewer(cad=CADWidget(CADGraphicsViewWithOverlay(), config=config))

    def _create_cad_widget(self):
        self._view = CADGraphicsViewWithOverlay()
        self._cad = CADWidget(self._view)

    def load_file(self, path: str, layout: str = "Model"):
        try:
            if os.path.splitext(path)[1].lower() == ".dwg":
                doc = odafc.readfile(path)
                auditor = doc.audit()
            else:
                try:
                    doc = ezdxf.readfile(path)
                except ezdxf.DXFError:
                    doc, auditor = recover.readfile(path)
                else:
                    auditor = doc.audit()
            self.set_document(doc, auditor, layout=layout)
        except IOError as e:
            qw.QMessageBox.critical(self, "Loading Error", str(e))
        except DXFStructureError as e:
            qw.QMessageBox.critical(
                self,
                "DXF Structure Error",
                f'Invalid DXF file "{path}": {str(e)}',
            )

    def _select_doc(self):
        path, _ = qw.QFileDialog.getOpenFileName(
            self,
            caption="Select CAD Document",
            filter="CAD Documents (*.dxf *.DXF *.dwg *.DWG)",
        )
        if path:
            self.load_file(path)

    def set_document(
        self,
        document: Drawing,
        auditor: Auditor,
        *,
        layout: str = "Model",
        draw: bool = True,
    ):
        error_count = len(auditor.errors)
        if error_count > 0:
            ret = qw.QMessageBox.question(
                self,
                "Found DXF Errors",
                f'Found {error_count} errors in file "{document.filename}"\n'
                f"Load file anyway? ",
            )
            if ret == qw.QMessageBox.No:
                auditor.print_error_report(auditor.errors)
                return

        if document.filename:
            try:
                self._watch_mtime = os.stat(document.filename).st_mtime
            except OSError:
                self._watch_mtime = None
        else:
            self._watch_mtime = None
        self._cad.set_document(document, layout=layout, draw=draw)
        self._doc = document
        self._populate_layouts()
        self._populate_layer_list()
        self.setWindowTitle("CAD Viewer - " + str(document.filename))

    def _populate_layer_list(self):
        self.layers.blockSignals(True)
        self.layers.clear()
        for layer in self._cad.render_context.layers.values():
            name = layer.layer
            item = qw.QListWidgetItem()
            self.layers.addItem(item)
            checkbox = qw.QCheckBox(name)
            checkbox.setCheckState(
                qc.Qt.Checked if layer.is_visible else qc.Qt.Unchecked
            )
            checkbox.stateChanged.connect(self._layers_updated)
            text_color = "#FFFFFF" if is_dark_color(layer.color, 0.4) else "#000000"
            checkbox.setStyleSheet(
                f"color: {text_color}; background-color: {layer.color}"
            )
            self.layers.setItemWidget(item, checkbox)
        self.layers.blockSignals(False)

    def _populate_layouts(self):
        def draw_layout(name: str):
            def run():
                self.draw_layout(name, reset_view=True)

            return run

        self.select_layout_menu.clear()
        for layout_name in self._cad.doc.layout_names_in_taborder():
            action = QAction(layout_name, self)
            action.triggered.connect(draw_layout(layout_name))
            self.select_layout_menu.addAction(action)

    def draw_layout(
        self,
        layout_name: str,
        reset_view: bool = True,
    ):
        print(f"drawing {layout_name}")
        try:
            start = time.perf_counter()
            self._cad.draw_layout(layout_name, reset_view=reset_view)
            duration = time.perf_counter() - start
            print(f"took {duration:.4f} seconds")
        except DXFStructureError as e:
            qw.QMessageBox.critical(
                self,
                "DXF Structure Error",
                f'Abort rendering of layout "{layout_name}": {str(e)}',
            )

    def resizeEvent(self, event: qg.QResizeEvent) -> None:
        self._view.fit_to_scene()

    def _layer_checkboxes(self) -> Iterable[tuple[int, qw.QCheckBox]]:
        for i in range(self.layers.count()):
            item = self.layers.itemWidget(self.layers.item(i))
            yield i, item  # type: ignore

    @Slot(int)  # type: ignore
    def _layers_updated(self, item_state: qc.Qt.CheckState):
        shift_held = qw.QApplication.keyboardModifiers() & qc.Qt.ShiftModifier
        if shift_held:
            for i, item in self._layer_checkboxes():
                item.blockSignals(True)
                item.setCheckState(item_state)
                item.blockSignals(False)

        visible_layers = set()
        for i, layer in self._layer_checkboxes():
            if layer.checkState() == qc.Qt.Checked:
                visible_layers.add(layer.text())
        self._cad.set_visible_layers(visible_layers)

    @Slot()
    def _toggle_sidebar(self):
        self.sidebar.setHidden(not self.sidebar.isHidden())

    @Slot()
    def _toggle_selection_marker(self):
        self._view.toggle_selection_marker()

    @Slot()
    def _reload(self):
        if self._cad.doc is not None and self._cad.doc.filename:
            keep_view = self.keep_view_action.isChecked()
            view = self._view.save_view() if keep_view else None
            self.load_file(self._cad.doc.filename, layout=self._cad.current_layout)
            if keep_view:
                self._view.restore_view(view)

    @Slot()
    def _toggle_watch(self):
        if self._watch_timer.isActive():
            self._watch_timer.stop()
        else:
            self._watch_timer.start()

    @Slot()
    def _check_watch(self):
        if self._watch_mtime is None or self._cad.doc is None:
            return
        filename = self._cad.doc.filename
        if filename:
            try:
                mtime = os.stat(filename).st_mtime
            except OSError:
                return
            if mtime != self._watch_mtime:
                self._reload()

    @Slot(qc.QPointF)
    def _on_mouse_moved(self, mouse_pos: qc.QPointF):
        self.mouse_pos.setText(
            f"mouse position: {mouse_pos.x():.4f}, {mouse_pos.y():.4f}\n"
        )

    @Slot(object, int)
    def _on_element_hovered(self, elements: list[qw.QGraphicsItem], index: int):
        if not elements:
            text = "No element selected"
        else:
            text = f"Selected: {index + 1} / {len(elements)}    (click to cycle)\n"
            element = elements[index]
            dxf_entity: DXFGraphic | str | None = element.data(CorrespondingDXFEntity)
            if isinstance(dxf_entity, str):
                dxf_entity = self.load_dxf_entity(dxf_entity)
            if dxf_entity is None:
                text += "No data"
            else:
                text += (
                    f"Selected Entity: {dxf_entity}\n"
                    f"Layer: {dxf_entity.dxf.layer}\n\nDXF Attributes:\n"
                )
                text += _entity_attribs_string(dxf_entity)

                dxf_parent_stack = element.data(CorrespondingDXFParentStack)
                if dxf_parent_stack:
                    text += "\nParents:\n"
                    for entity in reversed(dxf_parent_stack):
                        text += f"- {entity}\n"
                        text += _entity_attribs_string(entity, indent="    ")
        self.selected_info.setPlainText(text)

    def load_dxf_entity(self, entity_handle: str) -> DXFGraphic | None:
        if self._doc is not None:
            return self._doc.entitydb.get(entity_handle)
        return None


def _entity_attribs_string(dxf_entity: DXFGraphic, indent: str = "") -> str:
    text = ""
    for key, value in dxf_entity.dxf.all_existing_dxf_attribs().items():
        text += f"{indent}- {key}: {value}\n"
    return text
