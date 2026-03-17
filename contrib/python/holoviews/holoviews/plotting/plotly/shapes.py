import numpy as np
import param

from ...element import HLine, HSpan, Tiles, VLine, VSpan
from ..mixins import GeomMixin
from .element import ElementPlot
from .util import PLOTLY_SCATTERMAP


class ShapePlot(ElementPlot):

    # The plotly shape type ("line", "rect", etc.)
    _shape_type = None
    style_opts = ['opacity', 'fillcolor', 'line_color', 'line_width', 'line_dash']

    _supports_geo = True

    def init_graph(self, datum, options, index=0, is_geo=False, **kwargs):
        if is_geo:
            trace = {
                'type': PLOTLY_SCATTERMAP,
                'mode': 'lines',
                'showlegend': False,
                'hoverinfo': 'skip',
            }
            trace.update(datum, **options)

            # Turn on self fill if a fillcolor is specified
            if options.get("fillcolor", None):
                trace["fill"] = "toself"

            return dict(traces=[trace])
        else:
            shape = dict(type=self._shape_type, **dict(datum, **options))
            return dict(shapes=[shape])

    @staticmethod
    def build_path(xs, ys, closed=True):
        line_tos = ''.join([f'L{x} {y}'
                            for x, y in zip(xs[1:], ys[1:], strict=None)])
        path = f'M{xs[0]} {ys[0]}{line_tos}'

        if closed:
            path += 'Z'

        return path


class BoxShapePlot(GeomMixin, ShapePlot):
    _shape_type = 'rect'

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        inds = (1, 0, 3, 2) if self.invert_axes else (0, 1, 2, 3)
        x0s, y0s, x1s, y1s = (element.dimension_values(kd) for kd in inds)

        if is_geo:
            if len(x0s) == 0:
                lat = []
                lon = []
            else:
                lon0s, lat0s = Tiles.easting_northing_to_lon_lat(easting=x0s, northing=y0s)
                lon1s, lat1s = Tiles.easting_northing_to_lon_lat(easting=x1s, northing=y1s)

                lon_chunks, lat_chunks = zip(*[
                    ([lon0, lon0, lon1, lon1, lon0, np.nan],
                     [lat0, lat1, lat1, lat0, lat0, np.nan])
                    for (lon0, lat0, lon1, lat1) in zip(lon0s, lat0s, lon1s, lat1s, strict=None)
                ], strict=None)

                lon = np.concatenate(lon_chunks)
                lat = np.concatenate(lat_chunks)
            return [{"lat": lat, "lon": lon}]
        else:
            return [dict(x0=x0, x1=x1, y0=y0, y1=y1, xref='x', yref='y')
                    for (x0, y0, x1, y1) in zip(x0s, y0s, x1s, y1s, strict=None)]


class SegmentShapePlot(GeomMixin, ShapePlot):
    _shape_type = 'line'

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        inds = (1, 0, 3, 2) if self.invert_axes else (0, 1, 2, 3)
        x0s, y0s, x1s, y1s = (element.dimension_values(kd) for kd in inds)
        if is_geo:
            if len(x0s) == 0:
                lat = []
                lon = []
            else:
                lon0s, lat0s = Tiles.easting_northing_to_lon_lat(easting=x0s, northing=y0s)
                lon1s, lat1s = Tiles.easting_northing_to_lon_lat(easting=x1s, northing=y1s)

                lon_chunks, lat_chunks = zip(*[
                    ([lon0, lon1, np.nan],
                     [lat0, lat1, np.nan])
                    for (lon0, lat0, lon1, lat1) in zip(lon0s, lat0s, lon1s, lat1s, strict=None)
                ], strict=None)

                lon = np.concatenate(lon_chunks)
                lat = np.concatenate(lat_chunks)
            return [{"lat": lat, "lon": lon}]
        else:
            return [dict(x0=x0, x1=x1, y0=y0, y1=y1, xref='x', yref='y')
                    for (x0, y0, x1, y1) in zip(x0s, y0s, x1s, y1s, strict=None)]


class PathShapePlot(ShapePlot):
    _shape_type = 'path'

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        if self.invert_axes:
            ys = element.dimension_values(0)
            xs = element.dimension_values(1)
        else:
            xs = element.dimension_values(0)
            ys = element.dimension_values(1)

        if is_geo:
            lon, lat = Tiles.easting_northing_to_lon_lat(easting=xs, northing=ys)
            return [{"lat": lat, "lon": lon}]
        else:
            path = ShapePlot.build_path(xs, ys)
            return [dict(path=path, xref='x', yref='y')]


class PathsPlot(ShapePlot):
    _shape_type = 'path'

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        if is_geo:
            lon_chunks = []
            lat_chunks = []
            for el in element.split():
                xdim, ydim = (1, 0) if self.invert_axes else (0, 1)
                xs = el.dimension_values(xdim)
                ys = el.dimension_values(ydim)
                el_lon, el_lat = Tiles.easting_northing_to_lon_lat(xs, ys)
                lon_chunks.extend([el_lon, [np.nan]])
                lat_chunks.extend([el_lat, [np.nan]])
            if lon_chunks:
                lon = np.concatenate(lon_chunks)
                lat = np.concatenate(lat_chunks)
            else:
                lon = []
                lat = []
            return [{"lat": lat, "lon": lon}]
        else:
            paths = []
            for el in element.split():
                xdim, ydim = (1, 0) if self.invert_axes else (0, 1)
                xs = el.dimension_values(xdim)
                ys = el.dimension_values(ydim)
                path = ShapePlot.build_path(xs, ys)
                paths.append(dict(path=path, xref='x', yref='y'))
            return paths


class HVLinePlot(ShapePlot):

    apply_ranges = param.Boolean(default=False, doc="""
        Whether to include the annotation in axis range calculations.""")

    _shape_type = 'line'
    _supports_geo = False

    def get_data(self, element, ranges, style, **kwargs):
        if ((isinstance(element, HLine) and self.invert_axes) or
            (isinstance(element, VLine) and not self.invert_axes)):
            x = element.data
            visible = x is not None
            return [dict(
                x0=x, x1=x, y0=0, y1=1, xref='x', yref="paper", visible=visible
            )]
        else:
            y = element.data
            visible = y is not None
            return [dict(
                x0=0.0, x1=1.0, y0=y, y1=y, xref="paper", yref='y', visible=visible
            )]


class HVSpanPlot(ShapePlot):

    apply_ranges = param.Boolean(default=False, doc="""
        Whether to include the annotation in axis range calculations.""")

    _shape_type = 'rect'
    _supports_geo = False

    def get_data(self, element, ranges, style, **kwargs):

        if ((isinstance(element, HSpan) and self.invert_axes) or
            (isinstance(element, VSpan) and not self.invert_axes)):
            x0, x1 = element.data
            visible = not (x0 is None and x1 is None)
            return [dict(
                x0=x0, x1=x1, y0=0, y1=1, xref='x', yref="paper", visible=visible
            )]
        else:
            y0, y1 = element.data
            visible = not (y0 is None and y1 is None)
            return [dict(
                x0=0.0, x1=1.0, y0=y0, y1=y1, xref="paper", yref='y', visible=visible
            )]
