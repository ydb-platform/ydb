# Copyright (c) 2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Any, TYPE_CHECKING

import math
import os
import pathlib

from ezdxf.addons import xplayer
from ezdxf.addons.xqt import QtWidgets, QtGui, QtCore, QMessageBox
from ezdxf.addons.drawing import svg, layout, pymupdf, dxf
from ezdxf.addons.drawing.qtviewer import CADGraphicsView
from ezdxf.addons.drawing.pyqt import PyQtPlaybackBackend

from . import api
from .deps import BoundingBox2d, Matrix44, colors

if TYPE_CHECKING:
    from ezdxf.document import Drawing

VIEWER_NAME = "HPGL/2 Viewer"


class HPGL2Widget(QtWidgets.QWidget):
    def __init__(self, view: CADGraphicsView) -> None:
        super().__init__()
        layout = QtWidgets.QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(view)
        self.setLayout(layout)
        self._view = view
        self._view.closing.connect(self.close)
        self._player: api.Player = api.Player([], {})
        self._reset_backend()

    def _reset_backend(self) -> None:
        self._backend = PyQtPlaybackBackend()

    @property
    def view(self) -> CADGraphicsView:
        return self._view

    @property
    def player(self) -> api.Player:
        return self._player.copy()

    def plot(self, data: bytes) -> None:
        self._reset_backend()
        self._player = api.record_plotter_output(data, api.MergeControl.AUTO)

    def replay(
        self, bg_color="#ffffff", override=None, reset_view: bool = True
    ) -> None:
        self._reset_backend()
        self._view.begin_loading()
        new_scene = QtWidgets.QGraphicsScene()
        self._backend.set_scene(new_scene)
        new_scene.addItem(self._bg_paper(bg_color))

        xplayer.hpgl2_to_drawing(
            self._player, self._backend, bg_color="", override=override
        )
        self._view.end_loading(new_scene)
        self._view.buffer_scene_rect()
        if reset_view:
            self._view.fit_to_scene()

    def _bg_paper(self, color):
        bbox = self._player.bbox()
        insert = bbox.extmin
        size = bbox.size
        rect = QtWidgets.QGraphicsRectItem(insert.x, insert.y, size.x, size.y)
        rect.setBrush(QtGui.QBrush(QtGui.QColor(color)))
        return rect


SPACING = 20
DEFAULT_DPI = 96
COLOR_SCHEMA = [
    "Default",
    "Black on White",
    "White on Black",
    "Monochrome Light",
    "Monochrome Dark",
    "Blueprint High Contrast",
    "Blueprint Low Contrast",
]


class HPGL2Viewer(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self._cad = HPGL2Widget(CADGraphicsView())
        self._view = self._cad.view
        self._player: api.Player = api.Player([], {})
        self._bbox: BoundingBox2d = BoundingBox2d()
        self._page_rotation = 0
        self._color_scheme = 0
        self._current_file = pathlib.Path()

        self.page_size_label = QtWidgets.QLabel()
        self.png_size_label = QtWidgets.QLabel()
        self.message_label = QtWidgets.QLabel()
        self.scaling_factor_line_edit = QtWidgets.QLineEdit("1")
        self.dpi_line_edit = QtWidgets.QLineEdit(str(DEFAULT_DPI))

        self.flip_x_check_box = QtWidgets.QCheckBox("Horizontal")
        self.flip_x_check_box.setCheckState(QtCore.Qt.CheckState.Unchecked)
        self.flip_x_check_box.stateChanged.connect(self.update_view)

        self.flip_y_check_box = QtWidgets.QCheckBox("Vertical")
        self.flip_y_check_box.setCheckState(QtCore.Qt.CheckState.Unchecked)
        self.flip_y_check_box.stateChanged.connect(self.update_view)

        self.rotation_combo_box = QtWidgets.QComboBox()
        self.rotation_combo_box.addItems(["0", "90", "180", "270"])
        self.rotation_combo_box.currentIndexChanged.connect(self.update_rotation)

        self.color_combo_box = QtWidgets.QComboBox()
        self.color_combo_box.addItems(COLOR_SCHEMA)
        self.color_combo_box.currentIndexChanged.connect(self.update_colors)

        self.aci_export_mode = QtWidgets.QCheckBox("ACI Export Mode")
        self.aci_export_mode.setCheckState(QtCore.Qt.CheckState.Unchecked)

        self.export_svg_button = QtWidgets.QPushButton("Export SVG")
        self.export_svg_button.clicked.connect(self.export_svg)
        self.export_png_button = QtWidgets.QPushButton("Export PNG")
        self.export_png_button.clicked.connect(self.export_png)
        self.export_pdf_button = QtWidgets.QPushButton("Export PDF")
        self.export_pdf_button.clicked.connect(self.export_pdf)
        self.export_dxf_button = QtWidgets.QPushButton("Export DXF")
        self.export_dxf_button.clicked.connect(self.export_dxf)
        self.disable_export_buttons(True)

        self.scaling_factor_line_edit.editingFinished.connect(self.update_sidebar)
        self.dpi_line_edit.editingFinished.connect(self.update_sidebar)

        layout = QtWidgets.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        container = QtWidgets.QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)

        layout.addWidget(self._cad)
        sidebar = self.make_sidebar()
        layout.addWidget(sidebar)
        self.setWindowTitle(VIEWER_NAME)
        self.resize(1600, 900)
        self.show()

    def reset_values(self):
        self.scaling_factor_line_edit.setText("1")
        self.dpi_line_edit.setText(str(DEFAULT_DPI))
        self.flip_x_check_box.setCheckState(QtCore.Qt.CheckState.Unchecked)
        self.flip_y_check_box.setCheckState(QtCore.Qt.CheckState.Unchecked)
        self.rotation_combo_box.setCurrentIndex(0)
        self._page_rotation = 0
        self.update_view()

    def make_sidebar(self) -> QtWidgets.QWidget:
        sidebar = QtWidgets.QWidget()
        v_layout = QtWidgets.QVBoxLayout()
        v_layout.setContentsMargins(SPACING // 2, 0, SPACING // 2, 0)
        sidebar.setLayout(v_layout)

        policy = QtWidgets.QSizePolicy()
        policy.setHorizontalPolicy(QtWidgets.QSizePolicy.Policy.Fixed)
        sidebar.setSizePolicy(policy)

        open_button = QtWidgets.QPushButton("Open HPGL/2 File")
        open_button.clicked.connect(self.select_plot_file)
        v_layout.addWidget(open_button)
        v_layout.addWidget(self.page_size_label)
        h_layout = QtWidgets.QHBoxLayout()
        h_layout.addWidget(QtWidgets.QLabel("Scaling Factor:"))
        h_layout.addWidget(self.scaling_factor_line_edit)
        v_layout.addLayout(h_layout)

        h_layout = QtWidgets.QHBoxLayout()
        h_layout.addWidget(QtWidgets.QLabel("Page Rotation:"))
        h_layout.addWidget(self.rotation_combo_box)
        v_layout.addLayout(h_layout)

        group = QtWidgets.QGroupBox("Mirror Page")
        h_layout = QtWidgets.QHBoxLayout()
        h_layout.addWidget(self.flip_x_check_box)
        h_layout.addWidget(self.flip_y_check_box)
        group.setLayout(h_layout)
        v_layout.addWidget(group)

        h_layout = QtWidgets.QHBoxLayout()
        h_layout.addWidget(QtWidgets.QLabel("Colors:"))
        h_layout.addWidget(self.color_combo_box)
        v_layout.addLayout(h_layout)

        v_layout.addSpacing(SPACING)

        h_layout = QtWidgets.QHBoxLayout()
        h_layout.addWidget(QtWidgets.QLabel("DPI (PNG only):"))
        h_layout.addWidget(self.dpi_line_edit)
        v_layout.addLayout(h_layout)
        v_layout.addWidget(self.png_size_label)

        v_layout.addWidget(self.export_png_button)
        v_layout.addWidget(self.export_svg_button)
        v_layout.addWidget(self.export_pdf_button)

        v_layout.addSpacing(SPACING)

        v_layout.addWidget(self.aci_export_mode)
        v_layout.addWidget(self.export_dxf_button)

        v_layout.addSpacing(SPACING)

        reset_button = QtWidgets.QPushButton("Reset")
        reset_button.clicked.connect(self.reset_values)
        v_layout.addWidget(reset_button)

        v_layout.addSpacing(SPACING)

        v_layout.addWidget(self.message_label)
        return sidebar

    def disable_export_buttons(self, disabled: bool):
        self.export_svg_button.setDisabled(disabled)
        self.export_dxf_button.setDisabled(disabled)
        if pymupdf.is_pymupdf_installed:
            self.export_png_button.setDisabled(disabled)
            self.export_pdf_button.setDisabled(disabled)
        else:
            print("PDF/PNG export requires the PyMuPdf package!")
            self.export_png_button.setDisabled(True)
            self.export_pdf_button.setDisabled(True)

    def load_plot_file(self, path: str | os.PathLike, force=False) -> None:
        try:
            with open(path, "rb") as fp:
                data = fp.read()
            if force:
                data = b"%1B" + data
            self.set_plot_data(data, path)
        except IOError as e:
            QtWidgets.QMessageBox.critical(self, "Loading Error", str(e))

    def select_plot_file(self) -> None:
        path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            dir=str(self._current_file.parent),
            caption="Select HPGL/2 Plot File",
            filter="Plot Files (*.plt)",
        )
        if path:
            self.load_plot_file(path)

    def set_plot_data(self, data: bytes, filename: str | os.PathLike) -> None:
        try:
            self._cad.plot(data)
        except api.Hpgl2Error as e:
            msg = f"Cannot plot HPGL/2 file '{filename}', {str(e)}"
            QtWidgets.QMessageBox.critical(self, "Plot Error", msg)
            return
        self._player = self._cad.player
        self._bbox = self._player.bbox()
        self._current_file = pathlib.Path(filename)
        self.update_colors(self._color_scheme)
        self.update_sidebar()
        self.setWindowTitle(f"{VIEWER_NAME} - " + str(filename))
        self.disable_export_buttons(False)

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        self._view.fit_to_scene()

    def get_scale_factor(self) -> float:
        try:
            return float(self.scaling_factor_line_edit.text())
        except ValueError:
            return 1.0

    def get_dpi(self) -> int:
        try:
            return int(self.dpi_line_edit.text())
        except ValueError:
            return DEFAULT_DPI

    def get_page_size(self) -> tuple[int, int]:
        factor = self.get_scale_factor()
        x = 0
        y = 0
        if self._bbox.has_data:
            size = self._bbox.size
            # 40 plot units = 1mm
            x = round(size.x / 40 * factor)
            y = round(size.y / 40 * factor)
        if self._page_rotation in (90, 270):
            x, y = y, x
        return x, y

    def get_pixel_size(self) -> tuple[int, int]:
        dpi = self.get_dpi()
        x, y = self.get_page_size()
        return round(x / 25.4 * dpi), round(y / 25.4 * dpi)

    def get_flip_x(self) -> bool:
        return self.flip_x_check_box.checkState() == QtCore.Qt.CheckState.Checked

    def get_flip_y(self) -> bool:
        return self.flip_y_check_box.checkState() == QtCore.Qt.CheckState.Checked

    def get_aci_export_mode(self) -> bool:
        return self.aci_export_mode.checkState() == QtCore.Qt.CheckState.Checked

    def update_sidebar(self):
        x, y = self.get_page_size()
        self.page_size_label.setText(f"Page Size: {x}x{y}mm")
        px, py = self.get_pixel_size()
        self.png_size_label.setText(f"PNG Size: {px}x{py}px")
        self.clear_message()

    def update_view(self):
        self._view.setTransform(self.view_transformation())
        self._view.fit_to_scene()
        self.update_sidebar()

    def update_rotation(self, index: int):
        rotation = index * 90
        if rotation != self._page_rotation:
            self._page_rotation = rotation
            self.update_view()

    def update_colors(self, index: int):
        self._color_scheme = index
        self._cad.replay(*replay_properties(index))
        self.update_view()

    def view_transformation(self):
        if self._page_rotation == 0:
            m = Matrix44()
        else:
            m = Matrix44.z_rotate(math.radians(self._page_rotation))
        sx = -1 if self.get_flip_x() else 1
        # inverted y-axis
        sy = 1 if self.get_flip_y() else -1
        m @= Matrix44.scale(sx, sy, 1)
        return QtGui.QTransform(*m.get_2d_transformation())

    def show_message(self, msg: str) -> None:
        self.message_label.setText(msg)

    def clear_message(self) -> None:
        self.message_label.setText("")

    def get_export_name(self, suffix: str) -> str:
        return str(self._current_file.with_suffix(suffix))

    def export_svg(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            dir=self.get_export_name(".svg"),
            caption="Save SVG File",
            filter="SVG Files (*.svg)",
        )
        if not path:
            return
        try:
            with open(path, "wt") as fp:
                fp.write(self.make_svg_string())
            self.show_message("SVG successfully exported")
        except IOError as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def get_export_matrix(self) -> Matrix44:
        scale = self.get_scale_factor()
        rotation = self._page_rotation
        sx = -scale if self.get_flip_x() else scale
        sy = -scale if self.get_flip_y() else scale
        if rotation in (90, 270):
            sx, sy = sy, sx
        m = Matrix44.scale(sx, sy, 1)
        if rotation:
            m @= Matrix44.z_rotate(math.radians(rotation))
        return m

    def make_svg_string(self) -> str:
        """Replays the HPGL/2 recordings on the SVGBackend of the drawing add-on."""
        player = self._player.copy()
        player.transform(self.get_export_matrix())
        size = player.bbox().size
        svg_backend = svg.SVGBackend()
        bg_color, override = replay_properties(self._color_scheme)
        xplayer.hpgl2_to_drawing(
            player, svg_backend, bg_color=bg_color, override=override
        )
        del player  # free memory as soon as possible
        # 40 plot units == 1mm
        page = layout.Page(width=size.x / 40, height=size.y / 40)
        return svg_backend.get_string(page)

    def export_pdf(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            dir=self.get_export_name(".pdf"),
            caption="Save PDF File",
            filter="PDF Files (*.pdf)",
        )
        if not path:
            return
        try:
            with open(path, "wb") as fp:
                fp.write(self._pymupdf_export(fmt="pdf"))
            self.show_message("PDF successfully exported")
        except IOError as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def export_png(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            dir=self.get_export_name(".png"),
            caption="Save PNG File",
            filter="PNG Files (*.png)",
        )
        if not path:
            return
        try:
            with open(path, "wb") as fp:
                fp.write(self._pymupdf_export(fmt="png"))
            self.show_message("PNG successfully exported")
        except IOError as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def _pymupdf_export(self, fmt: str) -> bytes:
        """Replays the HPGL/2 recordings on the PyMuPdfBackend of the drawing add-on."""
        player = self._player.copy()
        player.transform(self.get_export_matrix())
        size = player.bbox().size
        pdf_backend = pymupdf.PyMuPdfBackend()
        bg_color, override = replay_properties(self._color_scheme)
        xplayer.hpgl2_to_drawing(
            player, pdf_backend, bg_color=bg_color, override=override
        )
        del player  # free memory as soon as possible
        # 40 plot units == 1mm
        page = layout.Page(width=size.x / 40, height=size.y / 40)
        if fmt == "pdf":
            return pdf_backend.get_pdf_bytes(page)
        else:
            return pdf_backend.get_pixmap_bytes(page, fmt=fmt, dpi=self.get_dpi())

    def export_dxf(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            dir=self.get_export_name(".dxf"),
            caption="Save DXF File",
            filter="DXF Files (*.dxf)",
        )
        if not path:
            return
        doc = self._get_dxf_document()
        try:
            doc.saveas(path)
            self.show_message("DXF successfully exported")
        except IOError as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def _get_dxf_document(self) -> Drawing:
        import ezdxf
        from ezdxf import zoom

        color_mode = (
            dxf.ColorMode.ACI if self.get_aci_export_mode() else dxf.ColorMode.RGB
        )

        doc = ezdxf.new()
        msp = doc.modelspace()
        player = self._player.copy()
        bbox = player.bbox()

        m = self.get_export_matrix()
        corners = m.fast_2d_transform(bbox.rect_vertices())
        # move content to origin:
        tx, ty = BoundingBox2d(corners).extmin
        m @= Matrix44.translate(-tx, -ty, 0)
        player.transform(m)
        bbox = player.bbox()

        dxf_backend = dxf.DXFBackend(msp, color_mode=color_mode)
        bg_color, override = replay_properties(self._color_scheme)
        if color_mode == dxf.ColorMode.RGB:
            doc.layers.add("BACKGROUND")
            bg = dxf.add_background(msp, bbox, colors.RGB.from_hex(bg_color))
            bg.dxf.layer = "BACKGROUND"
        # exports the HPGL/2 content in plot units (plu) as modelspace:
        # 1 plu = 0.025mm or 40 plu == 1mm
        xplayer.hpgl2_to_drawing(
            player, dxf_backend, bg_color=bg_color, override=override
        )
        del player
        if bbox.has_data:  # non-empty page
            zoom.window(msp, bbox.extmin, bbox.extmax)
            dxf.update_extents(doc, bbox)
            # paperspace is set up in mm:
            dxf.setup_paperspace(doc, bbox)
        return doc


def replay_properties(index: int) -> tuple[str, Any]:
    bg_color, override = "#ffffff", None  # default
    if index == 1:  # black on white
        bg_color, override = "#ffffff", xplayer.map_color("#000000")
    elif index == 2:  # white on black
        bg_color, override = "#000000", xplayer.map_color("#ffffff")
    elif index == 3:  # monochrome light
        bg_color, override = "#ffffff", xplayer.map_monochrome(dark_mode=False)
    elif index == 4:  # monochrome dark
        bg_color, override = "#000000", xplayer.map_monochrome(dark_mode=True)
    elif index == 5:  # blueprint high contrast
        bg_color, override = "#192c64", xplayer.map_color("#e9ebf3")
    elif index == 6:  # blueprint low contrast
        bg_color, override = "#243f8f", xplayer.map_color("#bdc5dd")
    return bg_color, override
