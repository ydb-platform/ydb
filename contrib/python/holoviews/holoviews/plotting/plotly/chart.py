import numpy as np
import param

from ...element import Tiles
from ...operation import interpolate_curve
from ..mixins import AreaMixin, BarsMixin
from .element import ColorbarPlot, ElementPlot
from .selection import PlotlyOverlaySelectionDisplay
from .util import PLOTLY_SCATTERMAP


class ChartPlot(ElementPlot):

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'scatter'}

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        if is_geo:
            if self.invert_axes:
                x = element.dimension_values(1)
                y = element.dimension_values(0)
            else:
                x = element.dimension_values(0)
                y = element.dimension_values(1)

            lon, lat = Tiles.easting_northing_to_lon_lat(x, y)
            return [{"lon": lon, "lat": lat}]
        else:
            x, y = ('y', 'x') if self.invert_axes else ('x', 'y')
            return [{x: element.dimension_values(0),
                     y: element.dimension_values(1)}]


class ScatterPlot(ChartPlot, ColorbarPlot):

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
      Index of the dimension from which the color will the drawn""")

    style_opts = [
        'visible',
        'marker',
        'color',
        'cmap',
        'alpha',
        'size',
        'sizemin',
        'selectedpoints',
    ]

    _nonvectorized_styles = ['visible', 'cmap', 'alpha', 'sizemin', 'selectedpoints']

    _style_key = 'marker'

    selection_display = PlotlyOverlaySelectionDisplay()

    _supports_geo = True

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        if is_geo:
            return {'type': PLOTLY_SCATTERMAP, 'mode': 'markers'}
        else:
            return {'type': 'scatter', 'mode': 'markers'}

    def graph_options(self, element, ranges, style, **kwargs):
        opts = super().graph_options(element, ranges, style, **kwargs)
        cdim = element.get_dimension(self.color_index)
        if cdim:
            copts = self.get_color_opts(cdim, element, ranges, style)
            copts['color'] = element.dimension_values(cdim)
            opts['marker'].update(copts)

        # If cmap was present and applicable, it was processed by get_color_opts above.
        # Remove it now to avoid plotly validation error
        opts.get('marker', {}).pop('cmap', None)
        return opts


class CurvePlot(ChartPlot, ColorbarPlot):

    interpolation = param.Selector(objects=['linear', 'steps-mid',
                                                  'steps-pre', 'steps-post'],
                                         default='linear', doc="""
        Defines how the samples of the Curve are interpolated,
        default is 'linear', other options include 'steps-mid',
        'steps-pre' and 'steps-post'.""")

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    style_opts = ['visible', 'color', 'dash', 'line_width']

    _nonvectorized_styles = style_opts

    unsupported_geo_style_opts = ["dash"]

    _style_key = 'line'

    _supports_geo = True

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        if is_geo:
            return {'type': PLOTLY_SCATTERMAP, 'mode': 'lines'}
        else:
            return {'type': 'scatter', 'mode': 'lines'}

    def get_data(self, element, ranges, style, **kwargs):
        if 'steps' in self.interpolation:
            element = interpolate_curve(element, interpolation=self.interpolation)
        return super().get_data(element, ranges, style, **kwargs)


class AreaPlot(AreaMixin, ChartPlot):

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    style_opts = ['visible', 'color', 'dash', 'line_width']

    _style_key = 'line'

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'scatter', 'mode': 'lines'}

    def get_data(self, element, ranges, style, **kwargs):
        x, y = ('y', 'x') if self.invert_axes else ('x', 'y')
        if len(element.vdims) == 1:
            kwargs = super().get_data(element, ranges, style, **kwargs)[0]
            kwargs['fill'] = 'tozero'+y
            return [kwargs]
        xs = element.dimension_values(0)
        ys = element.dimension_values(1)
        bottom = element.dimension_values(2)
        return [{x: xs, y: bottom, 'fill': None},
                {x: xs, y: ys, 'fill': 'tonext'+y}]


class SpreadPlot(ChartPlot):

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    style_opts = ['visible', 'color', 'dash', 'line_width']

    _style_key = 'line'

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'scatter', 'mode': 'lines'}

    def get_data(self, element, ranges, style, **kwargs):
        x, y = ('y', 'x') if self.invert_axes else ('x', 'y')
        xs = element.dimension_values(0)
        mean = element.dimension_values(1)
        neg_error = element.dimension_values(2)
        pos_idx = 3 if len(element.dimensions()) > 3 else 2
        pos_error = element.dimension_values(pos_idx)
        lower = mean - neg_error
        upper = mean + pos_error
        return [{x: xs, y: lower, 'fill': None},
                {x: xs, y: upper, 'fill': 'tonext'+y}]


class ErrorBarsPlot(ChartPlot, ColorbarPlot):

    style_opts = ['visible', 'color', 'dash', 'line_width', 'thickness']

    _nonvectorized_styles = style_opts

    _style_key = 'error_y'

    selection_display = PlotlyOverlaySelectionDisplay()

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'scatter', 'mode': 'lines', 'line': {'width': 0}}

    def get_data(self, element, ranges, style, **kwargs):
        x, y = ('y', 'x') if self.invert_axes else ('x', 'y')
        error_k = 'error_' + x if element.horizontal else 'error_' + y
        neg_error = element.dimension_values(2)
        pos_idx = 3 if len(element.dimensions()) > 3 else 2
        pos_error = element.dimension_values(pos_idx)
        error_v = dict(type='data', array=pos_error, arrayminus=neg_error)
        return [{x: element.dimension_values(0),
                 y: element.dimension_values(1),
                 error_k: error_v}]


class BarPlot(BarsMixin, ElementPlot):

    multi_level = param.Boolean(default=True, doc="""
       Whether the Bars should be grouped into a second categorical axis level.""")

    stacked = param.Boolean(default=False, doc="""
       Whether the bars should be stacked or grouped.""")

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['visible', 'color']

    selection_display = PlotlyOverlaySelectionDisplay()

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'bar'}

    def _get_axis_dims(self, element):
        if element.ndims > 1 and not self.stacked:
            xdims = element.kdims
        else:
            xdims = element.kdims[0]
        return (xdims, element.vdims[0])

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        x0, y0, x1, y1 = BarsMixin.get_extents(self, element, ranges, range_type)
        if range_type not in ('data', 'combined'):
            return x0, y0, x1, y1
        return (None, y0, None, y1)

    def get_data(self, element, ranges, style, **kwargs):
        # Get x, y, group, stack and color dimensions
        xdim = element.kdims[0]
        vdim = element.vdims[0]
        group_dim, stack_dim = None, None
        if element.ndims == 1:
            pass
        elif self.stacked:
            stack_dim = element.get_dimension(1)
            if stack_dim.values:
                svals = stack_dim.values
            elif stack_dim in ranges and ranges[stack_dim.label].get('factors'):
                svals = ranges[stack_dim.label]['factors']
            else:
                svals = element.dimension_values(1, False)
        else:
            group_dim = element.get_dimension(1)

        if self.invert_axes:
            x, y = ('y', 'x')
            orientation = 'h'
        else:
            x, y = ('x', 'y')
            orientation = 'v'

        xvals, gvals = self._get_coords(element, ranges, as_string=False)

        bars = []
        if element.ndims == 1:
            values = []
            for v in xvals:
                sel = element[[v]]
                values.append(sel.iloc[0, 1] if len(sel) else 0)
            bars.append({
                'orientation': orientation, 'showlegend': False,
                x: xvals,
                y: np.nan_to_num(values)})
        elif stack_dim or not self.multi_level:
            group_dim = stack_dim or group_dim
            order = list(svals if stack_dim else gvals)
            els = element.groupby(group_dim)
            sorted_groups = sorted(els.items(), key=lambda x: order.index(x[0])
                                   if x[0] in order else -1)
            for k, el in sorted_groups[::-1]:
                values = []
                for v in xvals:
                    sel = el[[v]]
                    values.append(sel.iloc[0, 1] if len(sel) else 0)
                bars.append({
                    'orientation': orientation, 'name': group_dim.pprint_value(k),
                    x: xvals,
                    y: np.nan_to_num(values)})
        else:
            values = element.dimension_values(vdim)
            bars.append({
                'orientation': orientation,
                x: [[d.pprint_value(v) for v in element.dimension_values(d)]
                    for d in (xdim, group_dim)],
                y: np.nan_to_num(values)})

        return bars

    def graph_options(self, element, ranges, style, **kwargs):
        if 'color' in style:
            style['marker_color'] = style.pop('color')
        opts = super().graph_options(element, ranges, style, **kwargs)
        return opts

    def init_layout(self, key, element, ranges, **kwargs):
        layout = super().init_layout(key, element, ranges)
        stack_dim = None
        if element.ndims > 1 and self.stacked:
            stack_dim = element.get_dimension(1)
        layout['barmode'] = 'stack' if stack_dim else 'group'
        return layout


class HistogramPlot(ElementPlot):

    style_opts = [
        'visible', 'color', 'line_color', 'line_width', 'opacity', 'selectedpoints'
    ]

    _style_key = 'marker'

    selection_display = PlotlyOverlaySelectionDisplay()

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'bar'}

    def get_data(self, element, ranges, style, **kwargs):
        xdim = element.kdims[0]
        ydim = element.vdims[0]
        values = np.asarray(element.interface.coords(element, ydim))
        edges = np.asarray(element.interface.coords(element, xdim))
        if len(edges) < 2:
            binwidth = 0
        else:
            binwidth = edges[1] - edges[0]

        if self.invert_axes:
            ys = edges
            xs = values
            orientation = 'h'
        else:
            xs = edges
            ys = values
            orientation = 'v'
        return [{'x': xs, 'y': ys, 'width': binwidth, 'orientation': orientation}]

    def init_layout(self, key, element, ranges, **kwargs):
        layout = super().init_layout(key, element, ranges)
        layout['barmode'] = 'overlay'
        return layout
