import pathlib
import sys
from abc import ABC, abstractmethod
import subprocess
import os
import platform

from ezdxf.addons.drawing.backend import BackendInterface


class FileOutputRenderBackend(ABC):
    def __init__(self, dpi: float) -> None:
        self._dpi = dpi

    @abstractmethod
    def supported_formats(self) -> list[tuple[str, str]]:
        raise NotImplementedError

    @abstractmethod
    def default_format(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def backend(self) -> BackendInterface:
        raise NotImplementedError

    @abstractmethod
    def save(self, output: pathlib.Path) -> None:
        raise NotImplementedError


class MatplotlibFileOutput(FileOutputRenderBackend):
    def __init__(self, dpi: float) -> None:
        super().__init__(dpi)

        try:
            import matplotlib.pyplot as plt
        except ImportError:
            raise ImportError("Matplotlib not found") from None

        from ezdxf.addons.drawing.matplotlib import MatplotlibBackend

        self._plt = plt
        self._fig = plt.figure()
        self._ax = self._fig.add_axes((0, 0, 1, 1))
        self._backend = MatplotlibBackend(self._ax)

    def supported_formats(self) -> list[tuple[str, str]]:
        return list(self._fig.canvas.get_supported_filetypes().items())

    def default_format(self) -> str:
        return "png"

    def backend(self) -> BackendInterface:
        return self._backend

    def save(self, output: pathlib.Path) -> None:
        self._fig.savefig(output, dpi=self._dpi)
        self._plt.close(self._fig)


class PyQtFileOutput(FileOutputRenderBackend):
    def __init__(self, dpi: float) -> None:
        super().__init__(dpi)

        try:
            from ezdxf.addons.xqt import QtCore, QtGui, QtWidgets
            from ezdxf.addons.drawing.pyqt import PyQtBackend
        except ImportError:
            raise ImportError("PyQt not found") from None

        self._qc = QtCore
        self._qg = QtGui
        self._qw = QtWidgets
        self._app = QtWidgets.QApplication(sys.argv)
        self._scene = QtWidgets.QGraphicsScene()
        self._backend = PyQtBackend()
        self._backend.set_scene(self._scene)

    def supported_formats(self) -> list[tuple[str, str]]:
        # https://doc.qt.io/qt-6/qimage.html#reading-and-writing-image-files
        return [
            ("bmp", "Windows Bitmap"),
            ("jpg", "Joint Photographic Experts Group"),
            ("jpeg", "Joint Photographic Experts Group"),
            ("png", "Portable Network Graphics"),
            ("ppm", "Portable Pixmap"),
            ("xbm", "X11 Bitmap"),
            ("xpm", "X11 Pixmap"),
            ("svg", "Scalable Vector Graphics"),
        ]

    def default_format(self) -> str:
        return "png"

    def backend(self) -> BackendInterface:
        return self._backend

    def save(self, output: pathlib.Path) -> None:
        if output.suffix.lower() == ".svg":
            from PySide6.QtSvg import QSvgGenerator

            generator = QSvgGenerator()
            generator.setFileName(str(output))
            generator.setResolution(int(self._dpi))
            scene_rect = self._scene.sceneRect()
            output_size = self._qc.QSize(
                round(scene_rect.size().width()), round(scene_rect.size().height())
            )
            generator.setSize(output_size)
            generator.setViewBox(
                self._qc.QRect(0, 0, output_size.width(), output_size.height())
            )

            painter = self._qg.QPainter()

            transform = self._qg.QTransform()
            transform.scale(1, -1)
            transform.translate(0, -output_size.height())

            painter.begin(generator)
            painter.setWorldTransform(transform, combine=True)
            painter.setRenderHint(self._qg.QPainter.RenderHint.Antialiasing)
            self._scene.render(painter)
            painter.end()

        else:

            view = self._qw.QGraphicsView(self._scene)
            view.setRenderHint(self._qg.QPainter.RenderHint.Antialiasing)
            sizef: QRectF = self._scene.sceneRect() * self._dpi / 92  # type: ignore
            image = self._qg.QImage(
                self._qc.QSize(round(sizef.width()), round(sizef.height())),
                self._qg.QImage.Format.Format_ARGB32,
            )
            painter = self._qg.QPainter(image)
            painter.setRenderHint(self._qg.QPainter.RenderHint.Antialiasing)
            painter.fillRect(image.rect(), self._scene.backgroundBrush())
            self._scene.render(painter)
            painter.end()
            image.mirror(False, True)
            image.save(str(output))


class MuPDFFileOutput(FileOutputRenderBackend):
    def __init__(self, dpi: float) -> None:
        super().__init__(dpi)

        from ezdxf.addons.drawing.pymupdf import PyMuPdfBackend, is_pymupdf_installed

        if not is_pymupdf_installed:
            raise ImportError("PyMuPDF not found")
        self._backend = PyMuPdfBackend()

    def supported_formats(self) -> list[tuple[str, str]]:
        # https://pymupdf.readthedocs.io/en/latest/pixmap.html#pixmapoutput
        return [
            ("pdf", "Portable Document Format"),
            ("svg", "Scalable Vector Graphics"),
            ("jpg", "Joint Photographic Experts Group"),
            ("jpeg", "Joint Photographic Experts Group"),
            ("pam", "Portable Arbitrary Map"),
            ("pbm", "Portable Bitmap"),
            ("pgm", "Portable Graymap"),
            ("png", "Portable Network Graphics"),
            ("pnm", "Portable Anymap"),
            ("ppm", "Portable Pixmap (no alpha channel)"),
            ("ps", "Adobe PostScript Image"),
            ("psd", "Adobe Photoshop Document"),
        ]

    def default_format(self) -> str:
        return "pdf"

    def backend(self) -> BackendInterface:
        return self._backend

    def save(self, output: pathlib.Path) -> None:
        from ezdxf.addons.drawing import layout

        backend = self._backend.get_replay(layout.Page(0, 0))
        if output.suffix == ".pdf":
            output.write_bytes(backend.get_pdf_bytes())
        elif output.suffix == ".svg":
            output.write_text(backend.get_svg_image())
        else:
            pixmap = backend.get_pixmap(int(self._dpi), alpha=True)
            pixmap.save(str(output))


class SvgFileOutput(FileOutputRenderBackend):
    def __init__(self, dpi: float) -> None:
        super().__init__(dpi)

        from ezdxf.addons.drawing.svg import SVGBackend

        self._backend = SVGBackend()

    def supported_formats(self) -> list[tuple[str, str]]:
        return [("svg", "Scalable Vector Graphics")]

    def default_format(self) -> str:
        return "svg"

    def backend(self) -> BackendInterface:
        return self._backend

    def save(self, output: pathlib.Path) -> None:
        from ezdxf.addons.drawing import layout

        output.write_text(self._backend.get_string(layout.Page(0, 0)))


def open_file(path: pathlib.Path) -> None:
    """open the given path in the default application"""
    system = platform.system()
    if system == "Darwin":
        subprocess.call(
            ["open", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    elif system == "Windows":
        os.startfile(str(path))  # type: ignore
    else:
        subprocess.call(
            ["xdg-open", str(path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
