import numpy as np
from matplotlib.collections import LineCollection, PatchCollection
from matplotlib.patches import Rectangle

from ...core.options import abbreviated_exception
from ..mixins import GeomMixin
from .element import ColorbarPlot
from .path import PathPlot, PolygonPlot


class SegmentPlot(GeomMixin, ColorbarPlot):
    """Segments are lines in 2D space where each two key dimensions specify a
    (x, y) node of the line.

    """

    style_opts = [*PathPlot.style_opts, 'cmap']

    _nonvectorized_styles = ['cmap']

    _plot_methods = dict(single='segment')

    def init_artists(self, ax, plot_args, plot_kwargs):
        if 'c' in plot_kwargs:
            plot_kwargs['array'] = plot_kwargs.pop('c')
        if 'vmin' in plot_kwargs and 'vmax' in plot_kwargs:
            plot_kwargs['clim'] = plot_kwargs.pop('vmin'), plot_kwargs.pop('vmax')
        if "array" not in plot_kwargs and 'cmap' in plot_kwargs:
            del plot_kwargs['cmap']
        line_segments = LineCollection(*plot_args, **plot_kwargs)
        ax.add_collection(line_segments)
        return {'artist': line_segments}

    def get_data(self, element, ranges, style):
        inds = (1, 0, 3, 2) if self.invert_axes else (0, 1, 2, 3)
        dims = element.dimensions()
        data = [[(x0, y0), (x1, y1)] for x0, y0, x1, y1
                in zip(*(element.dimension_values(d) for d in inds), strict=None)]
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        return (data,), style, {'dimensions': dims}



class RectanglesPlot(GeomMixin, ColorbarPlot):
    """Rectangles are polygons in 2D space where the key dimensions represent
    the bottom-left and top-right corner of the rectangle.

    """

    style_opts = PolygonPlot.style_opts

    _nonvectorized_styles = ['cmap']

    def init_artists(self, ax, plot_args, plot_kwargs):
        if 'c' in plot_kwargs:
            plot_kwargs['array'] = plot_kwargs.pop('c')
        if 'vmin' in plot_kwargs and 'vmax' in plot_kwargs:
            plot_kwargs['clim'] = plot_kwargs.pop('vmin'), plot_kwargs.pop('vmax')
        if "array" not in plot_kwargs and 'cmap' in plot_kwargs:
            del plot_kwargs['cmap']
        line_segments = PatchCollection(*plot_args, **plot_kwargs)
        ax.add_collection(line_segments)
        return {'artist': line_segments}

    def get_data(self, element, ranges, style):
        # Get [x0, y0, x1, y1]
        inds = (1, 0, 3, 2) if self.invert_axes else (0, 1, 2, 3)

        x0s, y0s, x1s, y1s = (element.dimension_values(kd) for kd in inds)
        x0s, x1s = np.min([x0s, x1s], axis=0), np.max([x0s, x1s], axis=0)
        y0s, y1s = np.min([y0s, y1s], axis=0), np.max([y0s, y1s], axis=0)

        dims = element.dimensions()
        data = [Rectangle((x0, y0), x1, y1) for (x0, y0, x1, y1)
                in zip(x0s, y0s, x1s-x0s, y1s-y0s, strict=None)]

        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        return (data,), style, {'dimensions': dims}
