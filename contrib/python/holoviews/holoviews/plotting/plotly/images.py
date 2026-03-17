import numpy as np
from plotly.graph_objs.layout import Image as _Image

from ...core.util import VersionError, dtype_kind
from ...element import Tiles
from .element import ElementPlot
from .selection import PlotlyOverlaySelectionDisplay
from .util import PLOTLY_MAP, PLOTLY_SCATTERMAP


class RGBPlot(ElementPlot):

    style_opts = ['opacity']

    apply_ranges = True

    selection_display = PlotlyOverlaySelectionDisplay()

    _supports_geo = True

    def init_graph(self, datum, options, index=0, is_geo=False, **kwargs):
        if is_geo:
            layer = dict(datum, **options)
            dummy_trace = {
                'type': PLOTLY_SCATTERMAP,
                'lat': [None],
                'lon': [None],
                'mode': 'markers',
                'showlegend': False
            }
            return {PLOTLY_MAP: dict(layers=[layer]), 'traces': [dummy_trace]}
        else:
            image = dict(datum, **options)
            # Create a dummy invisible scatter trace for this image.
            # This serves two purposes
            #  1. The two points placed on the corners of the image are used by the
            #     autoscale logic to allow using the autoscale button to properly center
            #     the image.
            #  2. This trace will be given a UID, and this UID will make it possible to
            #     associate callbacks with the image element. This is needed, in particular
            #     to support datashader
            dummy_trace = {
                'type': 'scatter',
                'x': [image['x'], image['x'] + image['sizex']],
                'y': [image['y'] - image['sizey'], image['y']],
                'mode': 'markers',
                'marker': {'opacity': 0},
                "showlegend": False,
            }
            return dict(images=[image],
                        traces=[dummy_trace])

    def get_data(self, element, ranges, style, is_geo=False, **kwargs):
        try:
            import PIL.Image
        except ImportError:
            raise VersionError("""\
Rendering RGB elements with the plotly backend requires the Pillow package""") from None

        img = np.flip(
            np.dstack([element.dimension_values(d, flat=False)
                       for d in element.vdims]),
            axis=0
        )

        if dtype_kind(img) == 'f':
            img = img * 255
        if img.size and (img.min() < 0 or img.max() > 255):
            self.param.warning('Clipping input data to the valid '
                               'range for RGB data ([0..1] for '
                               'floats or [0..255] for integers).')
            img = np.clip(img, 0, 255)

        if img.dtype.name != 'uint8':
            img = img.astype(np.uint8)

        if 0 in img.shape:
            img = np.zeros((1, 1, 3), dtype=np.uint8)

        if img.ndim != 3 or img.shape[2] not in (3, 4):
            raise ValueError(f"Unsupported image array with shape: {img.shape}")

        # Ensure axis inversions are handled correctly
        l, b, r, t = element.bounds.lbrt()
        if self.invert_axes:
            img = np.rot90(img.swapaxes(0, 1), 2)
            l, b, r, t = b, l, t, r
        if self.invert_xaxis:
            l, r = r, l
            img = np.flip(img, axis=1)
        if self.invert_yaxis:
            img = np.flip(img, axis=0)
            b, t = t, b

        source = _Image(source=PIL.Image.fromarray(img)).source

        if is_geo:
            lon_left, lat_top = Tiles.easting_northing_to_lon_lat(l, t)
            lon_right, lat_bottom = Tiles.easting_northing_to_lon_lat(r, b)
            coordinates = [
                [lon_left, lat_top],
                [lon_right, lat_top],
                [lon_right, lat_bottom],
                [lon_left, lat_bottom],
            ]
            layer = {
                "sourcetype": "image",
                "source": source,
                "coordinates": coordinates,
                "below": 'traces',
            }
            return [layer]
        else:
            return [dict(source=source,
                         x=l,
                         y=t,
                         sizex=r - l,
                         sizey=t - b,
                         xref='x',
                         yref='y',
                         sizing='stretch',
                         layer='above')]
