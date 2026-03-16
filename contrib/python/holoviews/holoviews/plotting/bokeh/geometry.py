import numpy as np
import param

from ...core.util import dimension_sanitizer
from ..mixins import GeomMixin
from .element import ColorbarPlot, LegendPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import base_properties, fill_properties, line_properties


class SegmentPlot(GeomMixin, ColorbarPlot):
    """Segments are lines in 2D space where each two each dimensions specify a
    (x, y) node of the line.

    """

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = base_properties + line_properties + ['cmap']

    _allow_implicit_categories = False
    _nonvectorized_styles = [*base_properties, 'cmap']
    _plot_methods = dict(single='segment')

    def get_data(self, element, ranges, style):
        inds = (1, 0, 3, 2) if self.invert_axes else (0, 1, 2, 3)
        xd0, yd0, xd1, yd1 = (dimension_sanitizer(element.get_dimension(i).name)
                              for i in inds)
        x0s, y0s, x1s, y1s = (element.dimension_values(kd) for kd in inds)
        data = {xd0: x0s, xd1: x1s, yd0: y0s, yd1: y1s}
        mapping = dict(x0=xd0, x1=xd1, y0=yd0, y1=yd1)
        self._get_hover_data(data, element)
        return (data, mapping, style)


class RectanglesPlot(GeomMixin, LegendPlot, ColorbarPlot):

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = (base_properties + line_properties + fill_properties +
                  ['cmap'])

    _allow_implicit_categories = False
    _nonvectorized_styles = [*base_properties, 'cmap']
    _plot_methods = dict(single='quad')
    _batched_style_opts = line_properties + fill_properties
    _color_style = 'fill_color'

    def get_data(self, element, ranges, style):
        inds = (1, 0, 3, 2) if self.invert_axes else (0, 1, 2, 3)
        x0, y0, x1, y1 = (element.dimension_values(kd) for kd in inds)
        x0, x1 = np.min([x0, x1], axis=0), np.max([x0, x1], axis=0)
        y0, y1 = np.min([y0, y1], axis=0), np.max([y0, y1], axis=0)
        data = {
            'left': x0,
            'right': x1,
            'bottom': y0,
            'top': y1
        }
        mapping = {'left': 'left', 'right': 'right', 'bottom': 'bottom', 'top': 'top'}
        self._get_hover_data(data, element)
        return data, mapping, style
