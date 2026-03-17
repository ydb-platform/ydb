import numpy as np

from holoviews.element.tiles import _ATTRIBUTIONS
from holoviews.plotting.plotly import ElementPlot
from holoviews.plotting.plotly.util import (
    PLOTLY_GE_6_0_0,
    PLOTLY_MAP,
    PLOTLY_SCATTERMAP,
    STYLE_ALIASES,
)
from holoviews.util.warnings import warn


class TilePlot(ElementPlot):
    style_opts = [
        "min_zoom",
        "max_zoom",
        "alpha",
        "mapstyle",
        # Only needed for Plotly <6
        "accesstoken",
        "mapboxstyle",
    ]

    _supports_geo = True

    @classmethod
    def trace_kwargs(cls, **kwargs):
        return {'type': PLOTLY_SCATTERMAP}

    def get_data(self, element, ranges, style, **kwargs):
        return [{
            "type": PLOTLY_SCATTERMAP, "lat": [], "lon": [], "subplot": PLOTLY_MAP,
            "showlegend": False,
        }]

    def graph_options(self, element, ranges, style, **kwargs):
        style = dict(style)
        if PLOTLY_GE_6_0_0 and (mapboxstyle := style.pop("mapboxstyle", None)):
            warn("'mapboxstyle' no longer supported with Plotly 6.0 use 'mapstyle' instead", category=UserWarning)
            style["mapstyle"] = mapboxstyle
        if PLOTLY_GE_6_0_0 and style.pop("accesstoken", None):
            warn("'accesstoken' no longer needed with Plotly 6.0", category=UserWarning)

        opts = dict(
            style=style.pop("mapstyle", "white-bg"),
            accesstoken=style.pop("accesstoken", None),
        )
        # Extract URL and lower case wildcard characters for mapbox
        url = element.data
        if url:
            layer = {}
            opts["layers"] = [layer]

            # element.data is xyzservices.TileProvider
            if isinstance(element.data, dict):
                layer["source"] = [element.data.build_url(scale_factor="@2x")]
                layer['sourceattribution'] = element.data.html_attribution
                layer['minzoom'] = element.data.get("min_zoom", 0)
                layer['maxzoom'] = element.data.get("max_zoom", 20)
            else:
                for v in ["X", "Y", "Z"]:
                    url = url.replace(f"{{{v}}}", f"{{{v.lower()}}}")
                layer["source"] = [url]

                for key, attribution in _ATTRIBUTIONS.items():
                    if all(k in element.data for k in key):
                        layer['sourceattribution'] = attribution

            layer["below"] = 'traces'
            layer["sourcetype"] = "raster"
            # Remaining style options are layer options
            layer.update({STYLE_ALIASES.get(k, k): v for k, v in style.items()})

        return opts

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        extents = super().get_extents(element, ranges, range_type)
        if (not self.overlaid and all(e is None or not np.isfinite(e) for e in extents)
            and range_type in ('combined', 'data')):
            x0, x1 = (-20037508.342789244, 20037508.342789244)
            y0, y1 = (-20037508.342789255, 20037508.342789244)
            global_extent = (x0, y0, x1, y1)
            return global_extent
        return extents

    def init_graph(self, datum, options, index=0, **kwargs):
        return {'traces': [datum], PLOTLY_MAP: options}

    def generate_plot(self, key, ranges, element=None, is_geo=False):
        """Override to force is_geo to True

        """
        return super().generate_plot(key, ranges, element, is_geo=True)
