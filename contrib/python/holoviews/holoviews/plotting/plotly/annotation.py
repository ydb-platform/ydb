import param

from ...element import Tiles
from .chart import ScatterPlot
from .util import PLOTLY_SCATTERMAP


class LabelPlot(ScatterPlot):

    xoffset = param.Number(default=None, doc="""
      Amount of offset to apply to labels along x-axis.""")

    yoffset = param.Number(default=None, doc="""
      Amount of offset to apply to labels along x-axis.""")

    style_opts = ['visible', 'color', 'family', 'size']

    _nonvectorized_styles = []

    _style_key = 'textfont'

    @classmethod
    def trace_kwargs(cls, is_geo=False, **kwargs):
        if is_geo:
            return {'type': PLOTLY_SCATTERMAP, 'mode': 'text'}
        else:
            return {'type': 'scatter', 'mode': 'text'}

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        text_dim = element.vdims[0]
        xs = element.dimension_values(0)
        if self.xoffset:
            xs = xs + self.xoffset
        ys = element.dimension_values(1)
        if self.yoffset:
            ys = ys + self.yoffset
        text = [text_dim.pprint_value(v) for v in element.dimension_values(2)]

        if is_geo:
            lon, lat = Tiles.easting_northing_to_lon_lat(xs, ys)
            return [{"lon": lon, "lat": lat, 'text': text}]
        else:
            x, y = ('y', 'x') if self.invert_axes else ('x', 'y')
            return [{x: xs, y: ys, 'text': text}]
