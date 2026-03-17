import numpy as np
import param
from plotly import colors
from plotly.figure_factory._trisurf import trisurf as trisurface

from ...core.options import SkipRendering
from .chart import CurvePlot, ScatterPlot
from .element import ColorbarPlot, ElementPlot
from .selection import PlotlyOverlaySelectionDisplay


class Chart3DPlot(ElementPlot):

    aspect = param.Parameter(default='cube')

    camera_angle = param.NumericTuple(default=(0.2, 0.5, 0.1, 0.2))

    camera_position = param.NumericTuple(default=(0.1, 0, -0.1))

    camera_zoom = param.Integer(default=3)

    projection = param.String(default='3d')

    width = param.Integer(default=500)

    height = param.Integer(default=500)

    zticks = param.Parameter(default=None, doc="""
        Ticks along z-axis specified as an integer, explicit list of
        tick locations, list of tuples containing the locations.""")

    def get_data(self, element, ranges, style, **kwargs):
        return [dict(x=element.dimension_values(0),
                     y=element.dimension_values(1),
                     z=element.dimension_values(2))]


class SurfacePlot(Chart3DPlot, ColorbarPlot):

    style_opts = ['visible', 'alpha', 'lighting', 'lightposition', 'cmap']

    selection_display = PlotlyOverlaySelectionDisplay(supports_region=False)

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'surface'}

    def graph_options(self, element, ranges, style, **kwargs):
        opts = super().graph_options(element, ranges, style, **kwargs)
        copts = self.get_color_opts(element.vdims[0], element, ranges, style)
        return dict(opts, **copts)

    def get_data(self, element, ranges, style, **kwargs):
        return [dict(x=element.dimension_values(0, False),
                     y=element.dimension_values(1, False),
                     z=element.dimension_values(2, flat=False))]


class Scatter3DPlot(Chart3DPlot, ScatterPlot):

    style_opts = [
        'visible', 'marker', 'color', 'cmap', 'alpha', 'opacity', 'size', 'sizemin'
    ]

    _supports_geo = False

    selection_display = PlotlyOverlaySelectionDisplay(supports_region=False)

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'scatter3d', 'mode': 'markers'}


class Path3DPlot(Chart3DPlot, CurvePlot):

    _per_trace = True

    _nonvectorized_styles = []

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'scatter3d', 'mode': 'lines'}

    def graph_options(self, element, ranges, style, **kwargs):
        opts = super().graph_options(element, ranges, style, **kwargs)
        opts['line'].pop('showscale', None)
        return opts

    def get_data(self, element, ranges, style, **kwargs):
        return [dict(x=el.dimension_values(0), y=el.dimension_values(1),
                     z=el.dimension_values(2))
                for el in element.split()]


class TriSurfacePlot(Chart3DPlot, ColorbarPlot):

    style_opts = ['cmap', 'edges_color', 'facecolor']

    selection_display = PlotlyOverlaySelectionDisplay(supports_region=False)

    def get_data(self, element, ranges, style, **kwargs):
        try:
            from scipy.spatial import Delaunay
        except ImportError:
            raise SkipRendering("SciPy not available, cannot plot TriSurface") from None
        x, y, z = (element.dimension_values(i) for i in range(3))
        points2D = np.vstack([x, y]).T
        tri = Delaunay(points2D)
        simplices = tri.simplices
        return [dict(x=x, y=y, z=z, simplices=simplices)]

    def graph_options(self, element, ranges, style, **kwargs):
        opts = super().graph_options(
            element, ranges, style, **kwargs
        )
        copts = self.get_color_opts(element.dimensions()[2], element, ranges, style)
        opts['colormap'] = [tuple(v/255. for v in colors.hex_to_rgb(c))
                            for _, c in copts['colorscale']]
        opts['scale'] = [l for l, _ in copts['colorscale']]
        opts['show_colorbar'] = self.colorbar
        opts['edges_color'] = style.get('edges_color', 'black')
        opts['plot_edges'] = 'edges_color' in style
        opts['colorbar'] = copts.get('colorbar', None)

        return {k: v for k, v in opts.items() if 'legend' not in k and k != 'name'}

    def init_graph(self, datum, options, index=0, **kwargs):

        # Pop colorbar options since these aren't accepted by the trisurf
        # figure factory.
        colorbar = options.pop('colorbar', None)
        trisurface_traces = trisurface(**dict(datum, **options))

        # Find colorbar to set colorbar options. Colorbar is associated with
        # a `scatter3d` scatter trace.
        if colorbar:
            marker_traces = [trace for trace in trisurface_traces
                             if trace.type == 'scatter3d' and
                             trace.mode == 'markers']
            if marker_traces:
                marker_traces[0].marker.colorbar = colorbar

        return {'traces': [t.to_plotly_json() for t in trisurface_traces]}
