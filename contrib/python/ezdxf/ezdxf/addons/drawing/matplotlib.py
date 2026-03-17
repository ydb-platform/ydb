# Copyright (c) 2020-2023, Matthew Broadway
# License: MIT License
from __future__ import annotations
from typing import Iterable, Optional, Union
import math
import logging
from os import PathLike

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import LineCollection
from matplotlib.image import AxesImage
from matplotlib.lines import Line2D
from matplotlib.patches import PathPatch
from matplotlib.path import Path
from matplotlib.transforms import Affine2D

from ezdxf.npshapes import to_matplotlib_path
from ezdxf.addons.drawing.backend import Backend, BkPath2d, BkPoints2d, ImageData
from ezdxf.addons.drawing.properties import BackendProperties, LayoutProperties
from ezdxf.addons.drawing.type_hints import FilterFunc
from ezdxf.addons.drawing.type_hints import Color
from ezdxf.math import Vec2, Matrix44
from ezdxf.layouts import Layout
from .config import Configuration

logger = logging.getLogger("ezdxf")
# matplotlib docs: https://matplotlib.org/index.html

# line style:
# https://matplotlib.org/api/_as_gen/matplotlib.lines.Line2D.html#matplotlib.lines.Line2D.set_linestyle
# https://matplotlib.org/gallery/lines_bars_and_markers/linestyles.html

# line width:
# https://matplotlib.org/api/_as_gen/matplotlib.lines.Line2D.html#matplotlib.lines.Line2D.set_linewidth
# points unit (pt), 1pt = 1/72 inch, 1pt = 0.3527mm
POINTS = 1.0 / 0.3527  # mm -> points
CURVE4x3 = (Path.CURVE4, Path.CURVE4, Path.CURVE4)
SCATTER_POINT_SIZE = 0.1


def setup_axes(ax: plt.Axes):
    # like set_axis_off, except that the face_color can still be set
    ax.xaxis.set_visible(False)
    ax.yaxis.set_visible(False)
    for s in ax.spines.values():
        s.set_visible(False)

    ax.autoscale(False)
    ax.set_aspect("equal", "datalim")


class MatplotlibBackend(Backend):
    """Backend which uses the :mod:`Matplotlib` package for image export.

    Args:
        ax: drawing canvas as :class:`matplotlib.pyplot.Axes` object
        adjust_figure: automatically adjust the size of the parent
            :class:`matplotlib.pyplot.Figure` to display all content
    """

    def __init__(
        self,
        ax: plt.Axes,
        *,
        adjust_figure: bool = True,
    ):
        super().__init__()
        setup_axes(ax)
        self.ax = ax
        self._adjust_figure = adjust_figure
        self._current_z = 0

    def configure(self, config: Configuration) -> None:
        if config.min_lineweight is None:
            # If not set by user, use ~1 pixel
            figure = self.ax.get_figure()
            if figure:
                config = config.with_changes(min_lineweight=72.0 / figure.dpi)
        super().configure(config)
        # LinePolicy.ACCURATE is handled by the frontend since v0.18.1

    def _get_z(self) -> int:
        z = self._current_z
        self._current_z += 1
        return z

    def set_background(self, color: Color):
        self.ax.set_facecolor(color)

    def draw_point(self, pos: Vec2, properties: BackendProperties):
        """Draw a real dimensionless point."""
        color = properties.color
        self.ax.scatter(
            [pos.x],
            [pos.y],
            s=SCATTER_POINT_SIZE,
            c=color,
            zorder=self._get_z(),
        )

    def get_lineweight(self, properties: BackendProperties) -> float:
        """Set lineweight_scaling=0 to use a constant minimal lineweight."""
        assert self.config.min_lineweight is not None
        return max(
            properties.lineweight * self.config.lineweight_scaling,
            self.config.min_lineweight,
        )

    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties):
        """Draws a single solid line, line type rendering is done by the
        frontend since v0.18.1
        """
        if start.isclose(end):
            # matplotlib draws nothing for a zero-length line:
            self.draw_point(start, properties)
        else:
            self.ax.add_line(
                Line2D(
                    (start.x, end.x),
                    (start.y, end.y),
                    linewidth=self.get_lineweight(properties),
                    color=properties.color,
                    zorder=self._get_z(),
                )
            )

    def draw_solid_lines(
        self,
        lines: Iterable[tuple[Vec2, Vec2]],
        properties: BackendProperties,
    ):
        """Fast method to draw a bunch of solid lines with the same properties."""
        color = properties.color
        lineweight = self.get_lineweight(properties)
        _lines = []
        point_x = []
        point_y = []
        z = self._get_z()
        for s, e in lines:
            if s.isclose(e):
                point_x.append(s.x)
                point_y.append(s.y)
            else:
                _lines.append(((s.x, s.y), (e.x, e.y)))

        self.ax.scatter(point_x, point_y, s=SCATTER_POINT_SIZE, c=color, zorder=z)
        self.ax.add_collection(
            LineCollection(
                _lines,
                linewidths=lineweight,
                color=color,
                zorder=z,
                capstyle="butt",
            )
        )

    def draw_path(self, path: BkPath2d, properties: BackendProperties):
        """Draw a solid line path, line type rendering is done by the
        frontend since v0.18.1
        """
        
        mpl_path = to_matplotlib_path([path])
        try:
            patch = PathPatch(
                mpl_path,
                linewidth=self.get_lineweight(properties),
                fill=False,
                color=properties.color,
                zorder=self._get_z(),
            )
        except ValueError as e:
            logger.info(f"ignored matplotlib error: {str(e)}")
        else:
            self.ax.add_patch(patch)

    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ):
        linewidth = 0

        try:
            patch = PathPatch(
                to_matplotlib_path(paths, detect_holes=True),
                color=properties.color,
                linewidth=linewidth,
                fill=True,
                zorder=self._get_z(),
            )
        except ValueError as e:
            logger.info(f"ignored matplotlib error in draw_filled_paths(): {str(e)}")
        else:
            self.ax.add_patch(patch)

    def draw_filled_polygon(self, points: BkPoints2d, properties: BackendProperties):
        self.ax.fill(
            *zip(*((p.x, p.y) for p in points.vertices())),
            color=properties.color,
            linewidth=0,
            zorder=self._get_z(),
        )

    def draw_image(
        self, image_data: ImageData, properties: BackendProperties
    ) -> None:
        height, width, depth = image_data.image.shape
        assert depth == 4

        # using AxesImage directly avoids an issue with ax.imshow where the data limits
        # are updated to include the un-transformed image because the transform is applied
        # afterward. We can use a slight hack which is that the outlines of images are drawn
        # as well as the image itself, so we don't have to adjust the data limits at all here
        # as the outline will take care of that
        handle = AxesImage(self.ax, interpolation="antialiased")
        handle.set_data(np.flip(image_data.image, axis=0))
        handle.set_zorder(self._get_z())

        (
            m11,
            m12,
            m13,
            m14,
            m21,
            m22,
            m23,
            m24,
            m31,
            m32,
            m33,
            m34,
            m41,
            m42,
            m43,
            m44,
        ) = image_data.transform
        matplotlib_transform = Affine2D(
            matrix=np.array(
                [
                    [m11, m21, m41],
                    [m12, m22, m42],
                    [0, 0, 1],
                ]
            )
        )
        handle.set_transform(matplotlib_transform + self.ax.transData)
        self.ax.add_image(handle)

    def clear(self):
        self.ax.clear()

    def finalize(self):
        super().finalize()
        self.ax.autoscale(True)
        if self._adjust_figure:
            minx, maxx = self.ax.get_xlim()
            miny, maxy = self.ax.get_ylim()
            data_width, data_height = maxx - minx, maxy - miny
            if not math.isclose(data_width, 0):
                width, height = plt.figaspect(data_height / data_width)
                self.ax.get_figure().set_size_inches(width, height, forward=True)


def _get_aspect_ratio(ax: plt.Axes) -> float:
    minx, maxx = ax.get_xlim()
    miny, maxy = ax.get_ylim()
    data_width, data_height = maxx - minx, maxy - miny
    if abs(data_height) > 1e-9:
        return data_width / data_height
    return 1.0


def _get_width_height(ratio: float, width: float, height: float) -> tuple[float, float]:
    if width == 0.0 and height == 0.0:
        raise ValueError("invalid (width, height) values")
    if width == 0.0:
        width = height * ratio
    elif height == 0.0:
        height = width / ratio
    return width, height


def qsave(
    layout: Layout,
    filename: Union[str, PathLike],
    *,
    bg: Optional[Color] = None,
    fg: Optional[Color] = None,
    dpi: int = 300,
    backend: str = "agg",
    config: Optional[Configuration] = None,
    filter_func: Optional[FilterFunc] = None,
    size_inches: Optional[tuple[float, float]] = None,
) -> None:
    """Quick and simplified render export by matplotlib.

    Args:
        layout: modelspace or paperspace layout to export
        filename: export filename, file extension determines the format e.g.
            "image.png" to save in PNG format.
        bg: override default background color in hex format #RRGGBB or #RRGGBBAA,
            e.g. use bg="#FFFFFF00" to get a transparent background and a black
            foreground color (ACI=7), because a white background #FFFFFF gets a
            black foreground color or vice versa bg="#00000000" for a transparent
            (black) background and a white foreground color.
        fg: override default foreground color in hex format #RRGGBB or #RRGGBBAA,
            requires also `bg` argument. There is no explicit foreground color
            in DXF defined (also not a background color), but the ACI color 7
            has already a variable color value, black on a light background and
            white on a dark background, this argument overrides this (ACI=7)
            default color value.
        dpi: image resolution (dots per inches).
        size_inches: paper size in inch as `(width, height)` tuple, which also
            defines the size in pixels = (`width` * `dpi`) x (`height` * `dpi`).
            If `width` or `height` is 0.0 the value is calculated by the aspect
            ratio of the drawing.
        backend: the matplotlib rendering backend to use (agg, cairo, svg etc)
            (see documentation for `matplotlib.use() <https://matplotlib.org/3.1.1/api/matplotlib_configuration_api.html?highlight=matplotlib%20use#matplotlib.use>`_
            for a complete list of backends)
        config: drawing parameters
        filter_func: filter function which takes a DXFGraphic object as input
            and returns ``True`` if the entity should be drawn or ``False`` if
            the entity should be ignored

    """
    from .properties import RenderContext
    from .frontend import Frontend
    import matplotlib

    # Set the backend to prevent warnings about GUIs being opened from a thread
    # other than the main thread.
    old_backend = matplotlib.get_backend()
    matplotlib.use(backend)
    if config is None:
        config = Configuration()

    try:
        fig: plt.Figure = plt.figure(dpi=dpi)
        ax: plt.Axes = fig.add_axes((0, 0, 1, 1))
        ctx = RenderContext(layout.doc)
        layout_properties = LayoutProperties.from_layout(layout)
        if bg is not None:
            layout_properties.set_colors(bg, fg)
        out = MatplotlibBackend(ax)
        Frontend(ctx, out, config).draw_layout(
            layout,
            finalize=True,
            filter_func=filter_func,
            layout_properties=layout_properties,
        )
        # transparent=True sets the axes color to fully transparent
        # facecolor sets the figure color
        # (semi-)transparent axes colors do not produce transparent outputs
        # but (semi-)transparent figure colors do.
        if size_inches is not None:
            ratio = _get_aspect_ratio(ax)
            w, h = _get_width_height(ratio, size_inches[0], size_inches[1])
            fig.set_size_inches(w, h, True)
        fig.savefig(filename, dpi=dpi, facecolor=ax.get_facecolor(), transparent=True)
        plt.close(fig)
    finally:
        matplotlib.use(old_backend)
