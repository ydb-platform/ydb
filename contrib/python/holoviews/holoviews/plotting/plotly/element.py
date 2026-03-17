import re
import uuid

import numpy as np
import param

from ... import Tiles
from ...core import util
from ...core.dimension import Dimension
from ...core.element import Element
from ...core.spaces import DynamicMap
from ...core.util import dtype_kind
from ...streams import Stream
from ...util.transform import dim
from ..plot import GenericElementPlot, GenericOverlayPlot
from ..util import dim_range_key
from .plot import PlotlyPlot
from .util import (
    PLOTLY_MAP,
    STYLE_ALIASES,
    get_colorscale,
    legend_trace_types,
    merge_figure,
    merge_layout,
)


class ElementPlot(PlotlyPlot, GenericElementPlot):

    aspect = param.Parameter(default='cube', doc="""
        The aspect ratio mode of the plot. By default, a plot may
        select its own appropriate aspect ratio but sometimes it may
        be necessary to force a square aspect ratio (e.g. to display
        the plot as an element of a grid). The modes 'auto' and
        'equal' correspond to the axis modes of the same name in
        matplotlib, a numeric value may also be passed.""")

    bgcolor = param.ClassSelector(class_=(str, tuple), default=None, doc="""
        If set bgcolor overrides the background color of the axis.""")

    invert_axes = param.Selector(default=False, doc="""
        Inverts the axes of the plot. Note that this parameter may not
        always be respected by all plots but should be respected by
        adjoined plots when appropriate.""")

    invert_xaxis = param.Boolean(default=False, doc="""
        Whether to invert the plot x-axis.""")

    invert_yaxis = param.Boolean(default=False, doc="""
        Whether to invert the plot y-axis.""")

    invert_zaxis = param.Boolean(default=False, doc="""
        Whether to invert the plot z-axis.""")

    labelled = param.List(default=['x', 'y', 'z'], doc="""
        Whether to label the 'x' and 'y' axes.""")

    logx = param.Boolean(default=False, doc="""
         Whether to apply log scaling to the x-axis of the Chart.""")

    logy  = param.Boolean(default=False, doc="""
         Whether to apply log scaling to the y-axis of the Chart.""")

    logz  = param.Boolean(default=False, doc="""
         Whether to apply log scaling to the y-axis of the Chart.""")

    margins = param.NumericTuple(default=(50, 50, 50, 50), doc="""
         Margins in pixel values specified as a tuple of the form
         (left, bottom, right, top).""")

    responsive = param.Boolean(default=False, doc="""
         Whether the plot should stretch to fill the available space.""")

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    xaxis = param.Selector(default='bottom',
                                 objects=['top', 'bottom', 'bare', 'top-bare',
                                          'bottom-bare', None], doc="""
        Whether and where to display the xaxis, bare options allow suppressing
        all axis labels including ticks and xlabel. Valid options are 'top',
        'bottom', 'bare', 'top-bare' and 'bottom-bare'.""")

    xticks = param.Parameter(default=None, doc="""
        Ticks along x-axis specified as an integer, explicit list of
        tick locations, list of tuples containing the locations.""")

    yaxis = param.Selector(default='left',
                                      objects=['left', 'right', 'bare', 'left-bare',
                                               'right-bare', None], doc="""
        Whether and where to display the yaxis, bare options allow suppressing
        all axis labels including ticks and ylabel. Valid options are 'left',
        'right', 'bare' 'left-bare' and 'right-bare'.""")

    yticks = param.Parameter(default=None, doc="""
        Ticks along y-axis specified as an integer, explicit list of
        tick locations, list of tuples containing the locations.""")

    zlabel = param.String(default=None, doc="""
        An explicit override of the z-axis label, if set takes precedence
        over the dimension label.""")

    zticks = param.Parameter(default=None, doc="""
        Ticks along z-axis specified as an integer, explicit list of
        tick locations, list of tuples containing the locations.""")

    _style_key = None

    # Whether vectorized styles are applied per trace
    _per_trace = False

    # Whether plot type can be displayed on mapbox plot
    _supports_geo = False

    # Declare which styles cannot be mapped to a non-scalar dimension
    _nonvectorized_styles = []

    def __init__(self, element, plot=None, **params):
        super().__init__(element, **params)
        self.trace_uid = str(uuid.uuid4())
        self.static = len(self.hmap) == 1 and len(self.keys) == len(self.hmap)
        self.callbacks, self.source_streams = self._construct_callbacks()

    @classmethod
    def trace_kwargs(cls, **kwargs):
        return {}

    def initialize_plot(self, ranges=None, is_geo=False):
        """Initializes a new plot object with the last available frame.

        """
        # Get element key and ranges for frame
        fig = self.generate_plot(self.keys[-1], ranges, is_geo=is_geo)
        self.drawn = True

        trigger = self._trigger
        self._trigger = []
        Stream.trigger(trigger)

        return fig


    def generate_plot(self, key, ranges, element=None, is_geo=False):
        self.prev_frame =  self.current_frame
        if element is None:
            element = self._get_frame(key)
        else:
            self.current_frame = element

        if is_geo and not self._supports_geo:
            raise ValueError(
                f"Elements of type {type(element)} cannot be overlaid "
                "with Tiles elements using the plotly backend"
            )

        if element is None:
            return self.handles['fig']

        # Set plot options
        plot_opts = self.lookup_options(element, 'plot').options
        self.param.update(**{k: v for k, v in plot_opts.items()
                                if k in self.param})

        # Get ranges
        ranges = self.compute_ranges(self.hmap, key, ranges)
        ranges = util.match_spec(element, ranges)

        # Get style
        self.style = self.lookup_options(element, 'style')
        style = self.style[self.cyclic_index]

        # Validate style properties are supported in geo mode
        if is_geo:
            unsupported_opts = [
                style_opt for style_opt in style
                if style_opt in self.unsupported_geo_style_opts
            ]
            if unsupported_opts:
                raise ValueError(
                    f"The following {type(element).__name__} style options are not supported by the Plotly "
                    f"backend when overlaid on Tiles:\n    {unsupported_opts}"
                )

        # Get data and options and merge them
        data = self.get_data(element, ranges, style, is_geo=is_geo)
        opts = self.graph_options(element, ranges, style, is_geo=is_geo)

        components = {
            'traces': [],
            'images': [],
            'annotations': [],
            'shapes': [],
        }

        for i, d in enumerate(data):
            # Initialize traces
            datum_components = self.init_graph(d, opts, index=i, is_geo=is_geo)

            # Handle traces
            traces = datum_components.get('traces', [])
            components['traces'].extend(traces)

            if i == 0 and traces:
                # Associate element with trace.uid property of the first
                # plotly trace that is used to render the element. This is
                # used to associate the element with the trace during callbacks
                traces[0]['uid'] = self.trace_uid

            # Handle images, shapes, annotations
            for k in ['images', 'shapes', 'annotations']:
                components[k].extend(datum_components.get(k, []))

            # Handle mapbox
            if PLOTLY_MAP in datum_components:
                components[PLOTLY_MAP] = datum_components[PLOTLY_MAP]

        self.handles['components'] = components

        # Initialize layout
        layout = self.init_layout(key, element, ranges, is_geo=is_geo)
        for k in ['images', 'shapes', 'annotations']:
            layout.setdefault(k, [])
            layout[k].extend(components.get(k, []))

        if PLOTLY_MAP in components:
            merge_layout(layout.setdefault(PLOTLY_MAP, {}), components[PLOTLY_MAP])

        self.handles['layout'] = layout

        # Create figure and return it
        layout['autosize'] = self.responsive
        fig = dict(data=components['traces'], layout=layout, config=dict(responsive=self.responsive))
        self.handles['fig'] = fig

        self._execute_hooks(element)
        self.drawn = True

        return fig


    def graph_options(self, element, ranges, style, is_geo=False, **kwargs):
        if self.overlay_dims:
            legend = ', '.join([d.pprint_value_string(v) for d, v in
                                self.overlay_dims.items()])
        else:
            legend = element.label

        opts = dict(
            name=legend, **self.trace_kwargs(is_geo=is_geo))

        if self.trace_kwargs(is_geo=is_geo).get('type', None) in legend_trace_types:
            opts.update(
                showlegend=self.show_legend, legendgroup=element.group+'_'+legend) # make legendgroup unique for single trace enable/disable

        if self._style_key is not None:
            styles = self._apply_transforms(element, ranges, style)

            # If style starts with '{_style_key}_', remove the prefix.  This way
            # a line_color property with self._style_key of 'line' doesn't end up
            # as `line_line_color`
            key_prefix_re = re.compile('^' + self._style_key + '_')
            styles = {key_prefix_re.sub('', k): v for k, v in styles.items()}

            opts[self._style_key] = {STYLE_ALIASES.get(k, k): v
                                     for k, v in styles.items()}

            # Move certain options from the style key back to root
            for k in ['selectedpoints', 'visible']:
                if k in opts.get(self._style_key, {}):
                    opts[k] = opts[self._style_key].pop(k)

        else:
            opts.update({STYLE_ALIASES.get(k, k): v
                         for k, v in style.items() if k != 'cmap'})
        return opts

    def init_graph(self, datum, options, index=0, **kwargs):
        """Initialize the plotly components that will represent the element

        Parameters
        ----------
        datum : dict
            An element of the data list returned by the get_data method
        options : dict
            Graph options that were returned by the graph_options method
        index : int
            Index of datum in the original list returned by the get_data method

        Returns
        -------
        dict
            Dictionary of the plotly components that represent the element. Keys may include:
                - 'traces': List of trace dicts
                - 'annotations': List of annotations dicts
                - 'images': List of image dicts
                - 'shapes': List of shape dicts
        """
        trace = dict(options)
        for k, v in datum.items():
            if k in trace and isinstance(trace[k], dict):
                trace[k].update(v)
            else:
                trace[k] = v

        if self._style_key and self._per_trace:
            vectorized = {k: v for k, v in options[self._style_key].items()
                          if isinstance(v, np.ndarray)}
            trace[self._style_key] = dict(trace[self._style_key])
            for s, val in vectorized.items():
                trace[self._style_key][s] = val[index]
        return {'traces': [trace]}


    def get_data(self, element, ranges, style, is_geo=False):
        return []


    def get_aspect(self, xspan, yspan):
        """Computes the aspect ratio of the plot

        """
        if self.aspect == 'equal' and (
            isinstance(xspan, util.datetime_types) ^ isinstance(yspan, util.datetime_types)
            or isinstance(xspan, util.timedelta_types) ^ isinstance(yspan, util.timedelta_types)
        ):
            msg = (
                "The aspect is set to 'equal', but the axes does not have the same type: "
                f"x-axis {type(xspan).__name__} and y-axis {type(yspan).__name__}. "
                "Either have the axes be the same type or or set '.opts(aspect=)' "
                "to either a number or 'square'."
            )
            raise TypeError(msg)

        return self.width/self.height


    def _get_axis_dims(self, element):
        """Returns the dimensions corresponding to each axis.

        Should return a list of dimensions or list of lists of
        dimensions, which will be formatted to label the axis
        and to link axes.

        """
        dims = element.dimensions()[:3]
        pad = [None]*max(3-len(dims), 0)
        return dims + pad


    def _apply_transforms(self, element, ranges, style):
        new_style = dict(style)
        for k, v in dict(style).items():
            if isinstance(v, (Dimension, str)):
                if k == 'marker' and v in 'xsdo':
                    continue
                elif v in element:
                    v = dim(element.get_dimension(v))
                elif any(d==v for d in self.overlay_dims):
                    v = dim(next(d for d in self.overlay_dims if d==v))

            if not isinstance(v, dim):
                continue
            elif (not v.applies(element) and v.dimension not in self.overlay_dims):
                new_style.pop(k)
                self.param.warning(f'Specified {k} dim transform {v!r} could not be applied, as not all '
                             'dimensions could be resolved.')
                continue

            if len(v.ops) == 0 and v.dimension in self.overlay_dims:
                val = self.overlay_dims[v.dimension]
            else:
                val = v.apply(element, ranges=ranges, flat=True)

            if (not util.isscalar(val) and len(util.unique_array(val)) == 1
                and 'color' not in k):
                val = val[0]

            if not util.isscalar(val):
                if k in self._nonvectorized_styles:
                    element = type(element).__name__
                    raise ValueError(f'Mapping a dimension to the "{k}" '
                                     'style option is not supported by the '
                                     f'{element} element using the {self.renderer.backend} '
                                     f'backend. To map the "{v.dimension}" dimension '
                                     f'to the {k} use a groupby operation '
                                     'to overlay your data along the dimension.')

            # If color is not valid colorspec add colormapper
            numeric = isinstance(val, np.ndarray) and dtype_kind(val) in 'uifMm'
            if ('color' in k and isinstance(val, np.ndarray) and numeric):
                copts = self.get_color_opts(v, element, ranges, style)
                new_style.pop('cmap', None)
                new_style.update(copts)
            new_style[k] = val
        return new_style

    def _format_title(self, key, separator=' '):
      """Formats the title of the plot.

      """
      title = super()._format_title(key, separator)

      if self.fontsize is not None and "title" in self.fontsize:
          title = {"text": title, "font": {"size": self.fontsize["title"]}}

      return title

    def init_layout(self, key, element, ranges, is_geo=False):
        el = element.traverse(lambda x: x, [Element])
        el = el[0] if el else element
        layout = dict(
            title=self._format_title(key, separator=' '),
            plot_bgcolor=self.bgcolor, uirevision=True
        )
        if not self.responsive:
            layout['width'] = self.width
            layout['height'] = self.height

        extent = self.get_extents(element, ranges)

        if len(extent) == 4:
            l, b, r, t = extent
        else:
            l, b, z0, r, t, z1 = extent

        dims = self._get_axis_dims(el)
        if len(dims) > 2:
            xdim, ydim, zdim = dims
        else:
            xdim, ydim = dims
            zdim = None
        xlabel, ylabel, zlabel = self._get_axis_labels(dims)

        if self.invert_axes:
            if is_geo:
                raise ValueError(
                    "The invert_axes parameter is not supported on Tiles elements "
                    "with the plotly backend"
                )
            xlabel, ylabel = ylabel, xlabel
            ydim, xdim = xdim, ydim
            l, b, r, t = b, l, t, r

        if 'x' not in self.labelled:
            xlabel = ''
        if 'y' not in self.labelled:
            ylabel = ''
        if 'z' not in self.labelled:
            zlabel = ''

        xaxis = {}
        if xdim and not is_geo:
            try:
                if any(np.isnan([r, l])):
                    r, l = 0, 1
            except TypeError:
                # r and l not numeric, don't change anything
                pass

            xrange = [r, l] if self.invert_xaxis else [l, r]
            xaxis = dict(range=xrange, title=xlabel)
            if self.logx:
                xaxis['type'] = 'log'
                xaxis['range'] = np.log10(xaxis['range'])
            self._get_ticks(xaxis, self.xticks)

            if self.projection != '3d' and self.xaxis:
                xaxis['automargin'] = False

                # Create dimension string used to compute matching axes
                if isinstance(xdim, (list, tuple)):
                    dim_str = "-".join([f"{d.name}^{d.label}^{d.unit}"
                                        for d in xdim])
                else:
                    dim_str = f"{xdim.name}^{xdim.label}^{xdim.unit}"

                xaxis['_dim'] = dim_str

                if 'bare' in self.xaxis:
                    xaxis['ticks'] = ''
                    xaxis['showticklabels'] = False
                    xaxis['title'] = ''

                if 'top' in self.xaxis:
                    xaxis['side'] = 'top'
                else:
                    xaxis['side'] = 'bottom'

        yaxis = {}
        if ydim and not is_geo:
            try:
                if any(np.isnan([b, t])):
                    b, t = 0, 1
            except TypeError:
                # b and t not numeric, don't change anything
                pass

            yrange = [t, b] if self.invert_yaxis else [b, t]
            yaxis = dict(range=yrange, title=ylabel)
            if self.logy:
                yaxis['type'] = 'log'
                yaxis['range'] = np.log10(yaxis['range'])
            self._get_ticks(yaxis, self.yticks)

            if self.projection != '3d' and self.yaxis:
                yaxis['automargin'] = False

                # Create dimension string used to compute matching axes
                if isinstance(ydim, (list, tuple)):
                    dim_str = "-".join([f"{d.name}^{d.label}^{d.unit}"
                                        for d in ydim])
                else:
                    dim_str = f"{ydim.name}^{ydim.label}^{ydim.unit}"

                yaxis['_dim'] = dim_str,
                if 'bare' in self.yaxis:
                    yaxis['ticks'] = ''
                    yaxis['showticklabels'] = False
                    yaxis['title'] = ''

                if 'right' in self.yaxis:
                    yaxis['side'] = 'right'
                else:
                    yaxis['side'] = 'left'

        if is_geo:
            mapbox = {}
            if all(np.isfinite(v) for v in (l, b, r, t)):
                x_center = (l + r) / 2.0
                y_center = (b + t) / 2.0
                lons, lats = Tiles.easting_northing_to_lon_lat([x_center], [y_center])

                mapbox["center"] = dict(lat=lats[0], lon=lons[0])

                # Compute zoom level
                margin_left, margin_bottom, margin_right, margin_top = self.margins
                viewport_width = self.width - margin_left - margin_right
                viewport_height = self.height - margin_top - margin_bottom
                mapbox_tile_size = 512

                max_delta = 2 * np.pi * 6378137
                x_delta = r - l
                y_delta = t - b

                with np.errstate(divide="ignore"):
                    max_x_zoom = (np.log2(max_delta / x_delta) -
                                np.log2(mapbox_tile_size / viewport_width))
                    max_y_zoom = (np.log2(max_delta / y_delta) -
                                np.log2(mapbox_tile_size / viewport_height))
                mapbox["zoom"] = min(max_x_zoom, max_y_zoom)
            layout[PLOTLY_MAP] = mapbox

        if isinstance(self.projection, str) and self.projection == '3d':
            scene = dict(xaxis=xaxis, yaxis=yaxis)
            if zdim:
                zrange = [z1, z0] if self.invert_zaxis else [z0, z1]
                zaxis = dict(range=zrange, title=zlabel)
                if self.logz:
                    zaxis['type'] = 'log'
                self._get_ticks(zaxis, self.zticks)
                scene['zaxis'] = zaxis
            if self.aspect == 'cube':
                scene['aspectmode'] = 'cube'
            else:
                scene['aspectmode'] = 'manual'
                scene['aspectratio'] = self.aspect
            layout['scene'] = scene
        else:
            l, b, r, t = self.margins
            layout['margin'] = dict(l=l, r=r, b=b, t=t, pad=4)
            if not is_geo:
                layout['xaxis'] = xaxis
                layout['yaxis'] = yaxis

        return layout

    def _get_ticks(self, axis, ticker):
        axis_props = {}
        if isinstance(ticker, (tuple, list)):
            if all(isinstance(t, tuple) for t in ticker):
                ticks, labels = zip(*ticker, strict=None)
                labels = [l if isinstance(l, str) else str(l)
                              for l in labels]
                axis_props['tickvals'] = ticks
                axis_props['ticktext'] = labels
            else:
                axis_props['tickvals'] = ticker
            axis.update(axis_props)

    def update_frame(self, key, ranges=None, element=None, is_geo=False):
        """Updates an existing plot with data corresponding
        to the key.

        """
        self.generate_plot(key, ranges, element, is_geo=is_geo)


class ColorbarPlot(ElementPlot):

    clim = param.NumericTuple(default=(np.nan, np.nan), length=2, doc="""
        User-specified colorbar axis range limits for the plot, as a
        tuple (low,high). If specified, takes precedence over data
        and dimension ranges.""")

    clim_percentile = param.ClassSelector(default=False, class_=(int, float, bool), doc="""
        Percentile value to compute colorscale robust to outliers. If
        True, uses 2nd and 98th percentile; otherwise uses the specified
        numerical percentile value.""")

    colorbar = param.Boolean(default=False, doc="""
        Whether to display a colorbar.""")

    color_levels = param.ClassSelector(default=None, class_=(int, list), doc="""
        Number of discrete colors to use when colormapping or a set of color
        intervals defining the range of values to map each color to.""")

    colorbar_opts = param.Dict(default={}, doc="""
        Allows setting including borderwidth, showexponent, nticks,
        outlinecolor, thickness, bgcolor, outlinewidth, bordercolor,
        ticklen, xpad, ypad, tickangle...""")

    symmetric = param.Boolean(default=False, doc="""
        Whether to make the colormap symmetric around zero.""")

    def get_color_opts(self, eldim, element, ranges, style):
        opts = {}
        dim_name = dim_range_key(eldim)
        if self.colorbar:
            opts['colorbar'] = dict(**self.colorbar_opts)
            if 'title' not in opts['colorbar']:
                if isinstance(eldim, dim):
                    title = str(eldim)
                    if eldim.ops:
                        pass
                    elif title.startswith("dim('") and title.endswith("')"):
                        title = title[5:-2]
                    else:
                        title = title[1:-1]
                else:
                    title = eldim.pprint_label
                opts['colorbar']['title']=title
            opts['showscale'] = True
        else:
            opts['showscale'] = False

        if eldim:
            auto = False
            if util.isfinite(self.clim).all():
                cmin, cmax = self.clim
            elif dim_name in ranges:
                if self.clim_percentile and 'robust' in ranges[dim_name]:
                    cmin, cmax = ranges[dim_name]['robust']
                else:
                    cmin, cmax = ranges[dim_name]['combined']
            elif isinstance(eldim, dim):
                cmin, cmax = np.nan, np.nan
                auto = True
            else:
                cmin, cmax = element.range(dim_name)
            if self.symmetric:
                cabs = np.abs([cmin, cmax])
                cmin, cmax = -cabs.max(), cabs.max()
        else:
            auto = True
            cmin, cmax = None, None

        cmap = style.pop('cmap', 'viridis')
        colorscale = get_colorscale(cmap, self.color_levels, cmin, cmax)

        # Reduce colorscale length to <= 255 to work around
        # https://github.com/plotly/plotly.js/issues/3699. Plotly.js performs
        # colorscale interpolation internally so reducing the number of colors
        # here makes very little difference to the displayed colorscale.
        #
        # Note that we need to be careful to make sure the first and last
        # colorscale pairs, colorscale[0] and colorscale[-1], are preserved
        # as the first and last in the subsampled colorscale
        if isinstance(colorscale, list) and len(colorscale) > 255:
            last_clr_pair = colorscale[-1]
            step = int(np.ceil(len(colorscale) / 255))
            colorscale = colorscale[0::step]
            colorscale[-1] = last_clr_pair

        if cmin is not None:
            opts['cmin'] = cmin
        if cmax is not None:
            opts['cmax'] = cmax
        opts['cauto'] = auto
        opts['colorscale'] = colorscale
        return opts


class OverlayPlot(GenericOverlayPlot, ElementPlot):

    _propagate_options = [
        'width', 'height', 'xaxis', 'yaxis', 'labelled', 'bgcolor',
        'invert_axes', 'show_frame', 'show_grid', 'logx', 'logy',
        'xticks', 'toolbar', 'yticks', 'xrotation', 'yrotation', 'responsive',
        'invert_xaxis', 'invert_yaxis', 'sizing_mode', 'title', 'title_format',
        'padding', 'xlabel', 'ylabel', 'zlabel', 'xlim', 'ylim', 'zlim']

    def initialize_plot(self, ranges=None, is_geo=False):
        """Initializes a new plot object with the last available frame.

        """
        # Get element key and ranges for frame
        return self.generate_plot(next(iter(self.hmap.data.keys())), ranges, is_geo=is_geo)

    def generate_plot(self, key, ranges, element=None, is_geo=False):
        if element is None:
            element = self._get_frame(key)
        items = [] if element is None else list(element.data.items())

        # Update plot options
        plot_opts = self.lookup_options(element, 'plot').options
        inherited = self._traverse_options(element, 'plot',
                                           self._propagate_options,
                                           defaults=False)
        plot_opts.update(**{k: v[0] for k, v in inherited.items() if k not in plot_opts})
        self.param.update(**plot_opts)

        ranges = self.compute_ranges(self.hmap, key, ranges)
        figure = None

        # Check if elements should be overlaid in geographic coordinates using mapbox
        #
        # Pass this through to generate_plot to build geo version of plot
        for _, el in items:
            if isinstance(el, Tiles):
                is_geo = True
                break

        for okey, subplot in self.subplots.items():
            if element is not None and subplot.drawn:
                idx, _spec, _exact = self._match_subplot(okey, subplot, items, element)
                if idx is not None:
                    _, el = items.pop(idx)
                else:
                    el = None
            else:
                el = None

            # propagate plot options to subplots
            subplot.param.update(**plot_opts)

            fig = subplot.generate_plot(key, ranges, el, is_geo=is_geo)
            if figure is None:
                figure = fig
            else:
                merge_figure(figure, fig)

        layout = self.init_layout(key, element, ranges, is_geo=is_geo)
        merge_layout(figure['layout'], layout)
        self.drawn = True

        self.handles['fig'] = figure
        return figure

    def update_frame(self, key, ranges=None, element=None, is_geo=False):
        reused = isinstance(self.hmap, DynamicMap) and self.overlaid
        self.prev_frame =  self.current_frame
        if not reused and element is None:
            element = self._get_frame(key)
        elif element is not None:
            self.current_frame = element
            self.current_key = key
        items = [] if element is None else list(element.data.items())

        for _, el in items:
            if isinstance(el, Tiles):
                is_geo = True

        # Instantiate dynamically added subplots
        for k, subplot in self.subplots.items():
            # If in Dynamic mode propagate elements to subplots
            if not (isinstance(self.hmap, DynamicMap) and element is not None):
                continue
            idx, _, _ = self._match_subplot(k, subplot, items, element)
            if idx is not None:
                items.pop(idx)
        if isinstance(self.hmap, DynamicMap) and items:
            self._create_dynamic_subplots(key, items, ranges)

        self.generate_plot(key, ranges, element, is_geo=is_geo)
