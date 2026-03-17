import numpy as np
import param

from ...core.options import SkipRendering
from ...core.util import isfinite
from ...element import Image, Raster
from ..mixins import HeatMapMixin
from .element import ColorbarPlot


class RasterPlot(ColorbarPlot):

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    style_opts = ['visible', 'cmap', 'alpha']

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        return {'type': 'heatmap'}

    def graph_options(self, element, ranges, style, **kwargs):
        opts = super().graph_options(element, ranges, style, **kwargs)
        copts = self.get_color_opts(element.vdims[0], element, ranges, style)
        cmin, cmax = copts.pop('cmin'), copts.pop('cmax')
        if isfinite(cmin):
            opts['zmin'] = cmin
        if isfinite(cmax):
            opts['zmax'] = cmax
        opts['zauto'] = copts.pop('cauto')
        return dict(opts, **copts)

    def get_data(self, element, ranges, style, **kwargs):
        if isinstance(element, Image):
            l, b, r, t = element.bounds.lbrt()
        else:
            l, b, r, t = element.extents
        array = element.dimension_values(2, flat=False)
        if type(element) is Raster:
            array=array.T[::-1,...]
        ny, nx = array.shape
        if any(not isfinite(c) for c in (l, b, r, t)) or nx == 0 or ny == 0:
            l, b, r, t, dx, dy = 0, 0, 0, 0, 0, 0
        else:
            dx, dy = float(r-l)/nx, float(t-b)/ny
        x0, y0 = l+dx/2., b+dy/2.
        if self.invert_axes:
            x0, y0, dx, dy = y0, x0, dy, dx
            array = array.T

        return [dict(x0=x0, y0=y0, dx=dx, dy=dy, z=array)]


class HeatMapPlot(HeatMapMixin, RasterPlot):

    def init_layout(self, key, element, ranges, **kwargs):
        layout = super().init_layout(key, element, ranges)
        gridded = element.gridded
        xdim, ydim = gridded.dimensions()[:2]

        if self.invert_axes:
            xaxis, yaxis = ('yaxis', 'xaxis')
        else:
            xaxis, yaxis = ('xaxis', 'yaxis')

        shape = gridded.interface.shape(gridded, gridded=True)

        xtype = gridded.interface.dtype(gridded, xdim)
        if xtype.kind in 'SUO':
            layout[xaxis]['tickvals'] = np.arange(shape[1])
            layout[xaxis]['ticktext'] = gridded.dimension_values(0, expanded=False)

        ytype = gridded.interface.dtype(gridded, ydim)
        if ytype.kind in 'SUO':
            layout[yaxis]['tickvals'] = np.arange(shape[0])
            layout[yaxis]['ticktext'] = gridded.dimension_values(1, expanded=False)
        return layout

    def get_data(self, element, ranges, style, **kwargs):
        if not element._unique:
            self.param.warning('HeatMap element index is not unique,  ensure you '
                               'aggregate the data before displaying it, e.g. '
                               'using heatmap.aggregate(function=np.mean). '
                               'Duplicate index values have been dropped.')

        gridded = element.gridded
        xdim, ydim = gridded.dimensions()[:2]
        data = gridded.dimension_values(2, flat=False)

        xtype = gridded.interface.dtype(gridded, xdim)
        if xtype.kind in 'SUO':
            xvals = np.arange(data.shape[1]+1)-0.5
        else:
            xvals = gridded.interface.coords(gridded, xdim, edges=True, ordered=True)

        ytype = gridded.interface.dtype(gridded, ydim)
        if ytype.kind in 'SUO':
            yvals = np.arange(data.shape[0]+1)-0.5
        else:
            yvals = gridded.interface.coords(gridded, ydim, edges=True, ordered=True)

        if self.invert_axes:
            xvals, yvals = yvals, xvals
            data = data.T

        return [dict(x=xvals, y=yvals, z=data)]


class QuadMeshPlot(RasterPlot):

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    def get_data(self, element, ranges, style, **kwargs):
        x, y, z = element.dimensions()[:3]
        irregular = element.interface.irregular(element, x)
        if irregular:
            raise SkipRendering("Plotly QuadMeshPlot only supports rectilinear meshes")
        xc, yc = (element.interface.coords(element, x, edges=True, ordered=True),
                  element.interface.coords(element, y, edges=True, ordered=True))
        zdata = element.dimension_values(z, flat=False)
        x, y = ('x', 'y')
        if self.invert_axes:
            y, x = 'x', 'y'
            zdata = zdata.T

        return [{x: xc, y: yc, 'z': zdata}]
