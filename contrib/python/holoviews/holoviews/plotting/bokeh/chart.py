import math
from collections import defaultdict

import numpy as np
import param
from bokeh.models import CategoricalColorMapper, CustomJS, Whisker
from bokeh.models.tools import BoxSelectTool
from bokeh.transform import jitter

from ...core.data import Dataset
from ...core.dimension import dimension_name
from ...core.util import dimension_sanitizer, dtype_kind, isdatetime, isfinite
from ...operation import interpolate_curve
from ...util.transform import dim
from ...util.warnings import warn
from ..mixins import AreaMixin, BarsMixin, SpikesMixin
from ..util import compute_sizes, get_min_distance
from .element import ColorbarPlot, ElementPlot, LegendPlot, OverlayPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import (
    base_properties,
    expand_batched_style,
    fill_properties,
    line_properties,
    mpl_to_bokeh,
    rgb2hex,
)
from .util import BOKEH_GE_3_8_0, categorize_array


class SizebarMixin(LegendPlot):

    sizebar = param.Boolean(default=False, doc="""
        Whether to display a sizebar.""")

    sizebar_location = param.Selector(
        default="below",
        objects=["above", "below", "left", "right", "center"],
        doc="""
        Location anchor for positioning scale bar, default to 'below'.

        The sizebar_location is only used if sizebar is True.""")

    sizebar_orientation = param.Selector(default="horizontal", objects=["horizontal", "vertical"], doc="""
        Orientation of the sizebar, default to 'horizontal'.

        The sizebar_orientation is only used if sizebar is True.
    """)

    sizebar_color = param.String(default="black", doc="""
        Color of the glyph in the sizebar, default to 'black'.

        The sizebar_color is only used if sizebar is True.""")

    sizebar_alpha = param.Number(default=0.6, bounds=(0, 1), doc="""
        Alpha value of the glyph in the sizebar, default to 0.6.

        The sizebar_alpha is only used if sizebar is True.""")

    sizebar_bounds = param.NumericTuple(default=None, length=2, doc="""
        Bounds of the sizebar, default to None which will automatically
        determine the bounds based on the data.

        The sizebar_bounds is only used if sizebar is True.""")

    sizebar_opts = param.Dict(
        default={}, doc="""
        Allows setting specific styling options for the sizebar.
        See https://docs.bokeh.org/en/latest/docs/reference/models/annotations.html#bokeh.models.SizeBar
        for more information.

        The sizebar_opts is only used if sizebar is True.""")

    def _init_glyph(self, plot, mapping, properties):
        renderer, glyph = super()._init_glyph(plot, mapping, properties)
        if self.sizebar:
            self._draw_sizebar(plot, renderer, glyph)
        return renderer, glyph

    def _draw_sizebar(self, plot, renderer, glyph):
        if not BOKEH_GE_3_8_0:
            raise RuntimeError("Sizebar requires Bokeh >= 3.8.0")

        from bokeh.models import SizeBar
        from bokeh.models.glyph import RadialGlyph

        if not isinstance(glyph, RadialGlyph):
            if isinstance(self, PointPlot):
                # PointPlot have both Scatter and Circle plot methods
                msg = "For sizebar to work you need to have radius set"
                warn(msg, category=RuntimeWarning)
            return

        sizebar_kwargs = dict(
            self.sizebar_opts,
            renderer=renderer,
            orientation=self.sizebar_orientation,
            glyph_fill_color=self.sizebar_color,
            glyph_fill_alpha=self.sizebar_alpha,
            bounds=self.sizebar_bounds or "auto",
        )

        if "width" not in sizebar_kwargs:  # Width is the primary axis
            match (self.sizebar_location, self.sizebar_orientation):
                case (("above" | "below"), "horizontal") | (("left" | "right"), "vertical"):
                    sizebar_kwargs["width"] = "max"

        sizebar = SizeBar(**sizebar_kwargs)
        plot.add_layout(sizebar, self.sizebar_location)
        self.handles['sizebar'] = sizebar


class PointPlot(SizebarMixin, ColorbarPlot):

    jitter = param.Number(default=None, bounds=(0, None), doc="""
      The amount of jitter to apply to offset the points along the x-axis.""")

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    # Deprecated parameters

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `color=dim('color')`""")

    size_index = param.ClassSelector(default=None, class_=(str, int),
                                     allow_None=True, doc="""
        Deprecated in favor of size style mapping, e.g. `size=dim('size')`""")

    scaling_method = param.Selector(default="area",
                                          objects=["width", "area"],
                                          doc="""
        Deprecated in favor of size style mapping, e.g.
        size=dim('size')**2.""")

    scaling_factor = param.Number(default=1, bounds=(0, None), doc="""
      Scaling factor which is applied to either the width or area
      of each point, depending on the value of `scaling_method`.""")

    size_fn = param.Callable(default=np.abs, doc="""
      Function applied to size values before applying scaling,
      to remove values lower than zero.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = [
        "cmap", "palette", "marker", "size", "angle", "hit_dilation",
        "radius", "radius_dimension",
        *base_properties, *line_properties, *fill_properties
    ]

    _plot_methods = dict(single='scatter', batched='scatter')
    _batched_style_opts = line_properties + fill_properties + ['size', 'marker', 'angle']

    def _init_glyph(self, plot, mapping, properties):
        if properties.get("radius") is not None:
            self._plot_methods = dict(single='circle', batched='circle')
            properties.pop("size", None)
        else:
            self._plot_methods = dict(single='scatter', batched='scatter')
            properties.pop("radius_dimension", None)
            properties.pop("radius", None)
        return super()._init_glyph(plot, mapping, properties)

    def _get_size_data(self, element, ranges, style):
        data, mapping = {}, {}
        sdim = element.get_dimension(self.size_index)
        ms = style.get('size', np.sqrt(6))
        if sdim and ((isinstance(ms, str) and ms in element) or isinstance(ms, dim)):
            self.param.warning(
                "Cannot declare style mapping for 'size' option and "
                "declare a size_index; ignoring the size_index.")
            sdim = None
        if not sdim or self.static_source:
            return data, mapping

        map_key = 'size_' + sdim.name
        ms = ms**2
        sizes = element.dimension_values(self.size_index)
        sizes = compute_sizes(sizes, self.size_fn,
                              self.scaling_factor,
                              self.scaling_method, ms)
        if sizes is None:
            eltype = type(element).__name__
            self.param.warning(
                f'{sdim.pprint_label} dimension is not numeric, cannot use to scale {eltype} size.')
        else:
            data[map_key] = np.sqrt(sizes)
            mapping['size'] = map_key
        return data, mapping


    def get_data(self, element, ranges, style):
        dims = element.dimensions(label=True)

        xidx, yidx = (1, 0) if self.invert_axes else (0, 1)
        mapping = dict(x=dims[xidx], y=dims[yidx])
        data = {}

        if not self.static_source or self.batched:
            xdim, ydim = dims[:2]
            data[xdim] = element.dimension_values(xdim)
            data[ydim] = element.dimension_values(ydim)
            self._categorize_data(data, dims[:2], element.dimensions())

        cdata, cmapping = self._get_color_data(element, ranges, style)
        data.update(cdata)
        mapping.update(cmapping)

        sdata, smapping = self._get_size_data(element, ranges, style)
        data.update(sdata)
        mapping.update(smapping)

        if 'angle' in style and isinstance(style['angle'], (int, float)):
            style['angle'] = np.deg2rad(style['angle'])

        if self.jitter:
            if self.invert_axes:
                mapping['y'] = jitter(dims[yidx], self.jitter,
                                      range=self.handles['y_range'])
            else:
                mapping['x'] = jitter(dims[xidx], self.jitter,
                                      range=self.handles['x_range'])

        self._get_hover_data(data, element)
        return data, mapping, style


    def get_batched_data(self, element, ranges):
        data = defaultdict(list)
        zorders = self._updated_zorders(element)

        # Angles need special handling since they are tied to the
        # marker in certain cases
        has_angles = False
        for (key, el), zorder in zip(element.data.items(), zorders, strict=None):
            el_opts = self.lookup_options(el, 'plot').options
            self.param.update(**{k: v for k, v in el_opts.items()
                                    if k not in OverlayPlot._propagate_options})
            style = self.lookup_options(element.last, 'style')
            style = style.max_cycles(len(self.ordering))[zorder]
            eldata, elmapping, style = self.get_data(el, ranges, style)
            style = mpl_to_bokeh(style)
            for k, eld in eldata.items():
                data[k].append(eld)

            # Skip if data is empty
            if not eldata:
                continue

            # Apply static styles
            nvals = len(next(iter(eldata.values())))
            sdata, smapping = expand_batched_style(style, self._batched_style_opts,
                                                   elmapping, nvals)
            if 'angle' in sdata and '__angle' not in data and 'marker' in data:
                data['__angle'] = [np.zeros(len(d)) for d in data['marker']]
                has_angles = True
            elmapping.update(smapping)
            for k, v in sorted(sdata.items()):
                if k == 'angle':
                    k = '__angle'
                    has_angles = True
                data[k].append(v)
            if has_angles and 'angle' not in sdata:
                data['__angle'].append(np.zeros(len(v)))

            if 'hover' in self.handles:
                for d, k in zip(element.dimensions(), key, strict=None):
                    sanitized = dimension_sanitizer(d.name)
                    data[sanitized].append([k]*nvals)

        data = {k: np.concatenate(v) for k, v in data.items()}
        if '__angle' in data:
            elmapping['angle'] = {'field': '__angle'}
        return data, elmapping, style



class VectorFieldPlot(ColorbarPlot):

    arrow_heads = param.Boolean(default=True, doc="""
        Whether or not to draw arrow heads.""")

    magnitude = param.ClassSelector(class_=(str, dim), doc="""
        Dimension or dimension value transform that declares the magnitude
        of each vector. Magnitude is expected to be scaled between 0-1,
        by default the magnitudes are rescaled relative to the minimum
        distance between vectors, this can be disabled with the
        rescale_lengths option.""")

    padding = param.ClassSelector(default=0.05, class_=(int, float, tuple))

    pivot = param.Selector(default='mid', objects=['mid', 'tip', 'tail'],
                                 doc="""
        The point around which the arrows should pivot valid options
        include 'mid', 'tip' and 'tail'.""")

    rescale_lengths = param.Boolean(default=True, doc="""
        Whether the lengths will be rescaled to take into account the
        smallest non-zero distance between two vectors.""")

    # Deprecated parameters

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of dimension value transform on color option,
        e.g. `color=dim('Magnitude')`.
        """)

    size_index = param.ClassSelector(default=None, class_=(str, int),
                                     allow_None=True, doc="""
        Deprecated in favor of the magnitude option, e.g.
        `magnitude=dim('Magnitude')`.
        """)

    normalize_lengths = param.Boolean(default=True, doc="""
        Deprecated in favor of rescaling length using dimension value
        transforms using the magnitude option, e.g.
        `dim('Magnitude').norm()`.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = base_properties + line_properties + ['scale', 'cmap']

    _nonvectorized_styles = [*base_properties, "scale", "cmap"]

    _plot_methods = dict(single='segment')

    def _get_lengths(self, element, ranges):
        size_dim = element.get_dimension(self.size_index)
        mag_dim = self.magnitude
        if size_dim and mag_dim:
            self.param.warning(
                "Cannot declare style mapping for 'magnitude' option "
                "and declare a size_index; ignoring the size_index.")
        elif size_dim:
            mag_dim = size_dim
        elif isinstance(mag_dim, str):
            mag_dim = element.get_dimension(mag_dim)

        if mag_dim:
            if isinstance(mag_dim, dim):
                magnitudes = mag_dim.apply(element, flat=True)
            else:
                magnitudes = element.dimension_values(mag_dim)
                _, max_magnitude = ranges[dimension_name(mag_dim)]['combined']
                if self.normalize_lengths and max_magnitude != 0:
                    magnitudes = magnitudes / max_magnitude
            if self.rescale_lengths:
                base_dist = get_min_distance(element)
                magnitudes = magnitudes * base_dist
        else:
            magnitudes = np.ones(len(element))
            if self.rescale_lengths:
                base_dist = get_min_distance(element)
                magnitudes = magnitudes * base_dist

        return magnitudes

    def _glyph_properties(self, *args):
        properties = super()._glyph_properties(*args)
        properties.pop('scale', None)
        return properties

    def get_data(self, element, ranges, style):
        input_scale = style.pop('scale', 1.0)

        # Get x, y, angle, magnitude and color data
        rads = element.dimension_values(2)
        if self.invert_axes:
            xidx, yidx = (1, 0)
            rads = np.pi/2 - rads
        else:
            xidx, yidx = (0, 1)
        lens = self._get_lengths(element, ranges)/input_scale
        cdim = element.get_dimension(self.color_index)
        cdata, cmapping = self._get_color_data(element, ranges, style,
                                               name='line_color')

        # Compute segments and arrowheads
        xs = element.dimension_values(xidx)
        ys = element.dimension_values(yidx)

        # Compute offset depending on pivot option
        xoffsets = np.cos(rads)*lens/2.
        yoffsets = np.sin(rads)*lens/2.
        if self.pivot == 'mid':
            nxoff, pxoff = xoffsets, xoffsets
            nyoff, pyoff = yoffsets, yoffsets
        elif self.pivot == 'tip':
            nxoff, pxoff = 0, xoffsets*2
            nyoff, pyoff = 0, yoffsets*2
        elif self.pivot == 'tail':
            nxoff, pxoff = xoffsets*2, 0
            nyoff, pyoff = yoffsets*2, 0
        x0s, x1s = (xs + nxoff, xs - pxoff)
        y0s, y1s = (ys + nyoff, ys - pyoff)

        color = None
        if self.arrow_heads:
            arrow_len = (lens/4.)
            xa1s = x0s - np.cos(rads+np.pi/4)*arrow_len
            ya1s = y0s - np.sin(rads+np.pi/4)*arrow_len
            xa2s = x0s - np.cos(rads-np.pi/4)*arrow_len
            ya2s = y0s - np.sin(rads-np.pi/4)*arrow_len
            x0s = np.tile(x0s, 3)
            x1s = np.concatenate([x1s, xa1s, xa2s])
            y0s = np.tile(y0s, 3)
            y1s = np.concatenate([y1s, ya1s, ya2s])
            if cdim and cdim.name in cdata:
                color = np.tile(cdata[cdim.name], 3)
        elif cdim:
            color = cdata.get(cdim.name)

        data = {
            'x0': x0s,
            'x1': x1s,
            'y0': y0s,
            'y1': y1s,
        }
        if 'hover' in self.handles:
            data.update({
                # tile to match the length of the segments
                "x": np.tile(xs, 3),
                "y": np.tile(ys, 3),
                "Angle": np.tile(rads, 3),
                "Magnitude": np.tile(lens, 3)
            })
        mapping = dict(x0='x0', x1='x1', y0='y0', y1='y1')
        if cdim and color is not None:
            data[cdim.name] = color
            mapping.update(cmapping)

        return (data, mapping, style)


class CurvePlot(ElementPlot):

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    interpolation = param.Selector(objects=['linear', 'steps-mid',
                                                  'steps-pre', 'steps-post'],
                                         default='linear', doc="""
        Defines how the samples of the Curve are interpolated,
        default is 'linear', other options include 'steps-mid',
        'steps-pre' and 'steps-post'.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = base_properties + line_properties

    _batched_style_opts = line_properties
    _nonvectorized_styles = base_properties + line_properties
    _plot_methods = dict(single='line', batched='multi_line')

    def get_data(self, element, ranges, style):
        xidx, yidx = (1, 0) if self.invert_axes else (0, 1)
        x = element.get_dimension(xidx).name
        y = element.get_dimension(yidx).name
        if self.static_source and not self.batched:
            return {}, dict(x=x, y=y), style

        if 'steps' in self.interpolation:
            element = interpolate_curve(element, interpolation=self.interpolation)
        data = {x: element.dimension_values(xidx),
                y: element.dimension_values(yidx)}
        self._get_hover_data(data, element)
        self._categorize_data(data, (x, y), element.dimensions())
        return (data, dict(x=x, y=y), style)

    def _hover_opts(self, element):
        if self.batched:
            dims = list(self.hmap.last.kdims)
            line_policy = 'prev'
        else:
            dims = list(self.overlay_dims.keys())+element.dimensions()
            line_policy = 'nearest'
        return dims, dict(line_policy=line_policy)

    def get_batched_data(self, overlay, ranges):
        data = defaultdict(list)

        zorders = self._updated_zorders(overlay)
        for (key, el), zorder in zip(overlay.data.items(), zorders, strict=None):
            el_opts = self.lookup_options(el, 'plot').options
            self.param.update(**{k: v for k, v in el_opts.items()
                                    if k not in OverlayPlot._propagate_options})
            style = self.lookup_options(el, 'style')
            style = style.max_cycles(len(self.ordering))[zorder]
            eldata, elmapping, style = self.get_data(el, ranges, style)

            # Skip if data empty
            if not eldata:
                continue

            for k, eld in eldata.items():
                data[k].append(eld)

            # Apply static styles
            sdata, smapping = expand_batched_style(style, self._batched_style_opts,
                                                   elmapping, nvals=1)
            elmapping.update(smapping)
            for k, v in sdata.items():
                data[k].append(v[0])

            for d, k in zip(overlay.kdims, key, strict=None):
                sanitized = dimension_sanitizer(d.name)
                data[sanitized].append(k)
        data = {opt: vals for opt, vals in data.items()
                if not any(v is None for v in vals)}
        mapping = {{'x': 'xs', 'y': 'ys'}.get(k, k): v
                   for k, v in elmapping.items()}
        return data, mapping, style


class HistogramPlot(ColorbarPlot):

    selection_display = BokehOverlaySelectionDisplay(color_prop=['color', 'fill_color'])

    style_opts = base_properties + fill_properties + line_properties + ['cmap']

    _nonvectorized_styles = [*base_properties, "line_dash"]
    _plot_methods = dict(single='quad')

    def initialize_plot(self, ranges=None, plot=None, plots=None, source=None):
        plot = super().initialize_plot(ranges, plot, plots, source)
        logx = self.logx and self.invert_axes
        logy = self.logy and not self.invert_axes
        if logx or logy:
            if logy:
                range_ = plot.y_range
                pos = "end" if self.invert_yaxis else "start"
            else:
                range_ = plot.x_range
                pos = "end" if self.invert_xaxis else "start"
            source = self.handles["source"]
            # Insert bottom to the lower value of the axis
            source.data['bottom'] = [getattr(range_, pos)] * len(source.data['top'])
            callback = CustomJS(
                args=dict(source=source, range=range_, pos=pos),
                code="""
                source.data['bottom'].fill(range[pos]);
                source.change.emit();
            """,
            )
            range_.js_on_change(pos, callback)

        return plot

    def get_data(self, element, ranges, style):
        if self.invert_axes:
            left = 'bottom' if self.logx else 0
            mapping = dict(top='right', bottom='left', left=left, right='top')
        else:
            bottom = 'bottom' if self.logy else 0
            mapping = dict(top='top', bottom=bottom, left='left', right='right')
        if self.static_source:
            data = dict(top=[], left=[], right=[])
        else:
            x = element.kdims[0]
            values = element.dimension_values(1)
            edges = element.interface.coords(element, x, edges=True)
            if hasattr(edges, 'compute'):
                edges = edges.compute()
            data = dict(top=values, left=edges[:-1], right=edges[1:])
            self._get_hover_data(data, element)
        return data, mapping, style

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        ydim = element.get_dimension(1)
        s0, s1 = ranges[ydim.label]['soft']
        s0 = min(s0, 0) if isfinite(s0) else 0
        s1 = max(s1, 0) if isfinite(s1) else 0
        ranges[ydim.label]['soft'] = (s0, s1)
        return super().get_extents(element, ranges, range_type)

    def _update_range(self, axis_range, low, high, factors, invert, shared, log, streaming=False):
        # We allow zero values with histogram
        if log and low == 0:
            low = 0.01 if high > 0.01 else 10**(math.log10(high)-2)
        return super()._update_range(axis_range, low, high, factors, invert, shared, log, streaming)


class SideHistogramPlot(HistogramPlot):

    style_opts = [*HistogramPlot.style_opts, "cmap"]

    height = param.Integer(default=125, doc="The height of the plot")

    width = param.Integer(default=125, doc="The width of the plot")

    show_title = param.Boolean(default=False, doc="""
        Whether to display the plot title.""")

    default_tools = param.List(default=['save', 'pan', 'wheel_zoom',
                                        'box_zoom', 'reset'],
        doc="A list of plugin tools to use on the plot.")

    _callback = """
    color_mapper.low = cb_obj['geometry']['{axis}0'];
    color_mapper.high = cb_obj['geometry']['{axis}1'];
    source.change.emit()
    main_source.change.emit()
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.invert_axes:
            self.default_tools.append('ybox_select')
        else:
            self.default_tools.append('xbox_select')


    def get_data(self, element, ranges, style):
        data, mapping, style = HistogramPlot.get_data(self, element, ranges, style)
        color_dims = [d for d in self.adjoined.traverse(lambda x: x.handles.get('color_dim'))
                      if d is not None]
        dimension = color_dims[0] if color_dims else None
        cmapper = self._get_colormapper(dimension, element, {}, {})
        if cmapper:
            cvals = None
            if isinstance(dimension, dim):
                if dimension.applies(element):
                    dim_name = dimension.dimension.name
                    cvals = [] if self.static_source else dimension.apply(element)
            elif dimension in element.dimensions():
                dim_name = dimension.name
                cvals = [] if self.static_source else element.dimension_values(dimension)
            if cvals is not None:
                data[dim_name] = cvals
                mapping['fill_color'] = {'field': dim_name,
                                         'transform': cmapper}
        return (data, mapping, style)


    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        ret = super()._init_glyph(plot, mapping, properties)
        if "field" not in mapping.get("fill_color", {}):
            return ret
        dim = mapping['fill_color']['field']
        sources = self.adjoined.traverse(lambda x: (x.handles.get('color_dim'),
                                                     x.handles.get('source')))
        sources = [src for cdim, src in sources if cdim == dim]
        tools = [t for t in self.handles['plot'].tools
                 if isinstance(t, BoxSelectTool)]
        if not tools or not sources:
            return
        main_source = sources[0]
        handles = {'color_mapper': self.handles['color_mapper'],
                   'source': self.handles['source'],
                   'cds': self.handles['source'],
                   'main_source': main_source}
        callback = self._callback.format(axis='y' if self.invert_axes else 'x')
        self.state.js_on_event("selectiongeometry", CustomJS(args=handles, code=callback))
        return ret



class ErrorPlot(ColorbarPlot):

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = ([
        p for p in line_properties if p.split('_')[0] not in
        ('hover', 'selection', 'nonselection', 'muted')
    ] + ['lower_head', 'upper_head'] + base_properties)

    _nonvectorized_styles = [*base_properties, "line_dash"]
    _mapping = dict(base="base", upper="upper", lower="lower")
    _plot_methods = dict(single=Whisker)

    def get_data(self, element, ranges, style):
        mapping = dict(self._mapping)
        if self.static_source:
            return {}, mapping, style

        x_idx, y_idx = (1, 0) if element.horizontal else (0, 1)
        base = element.dimension_values(x_idx)
        mean = element.dimension_values(y_idx)
        neg_error = element.dimension_values(2)
        pos_idx = 3 if len(element.dimensions()) > 3 else 2
        pos_error = element.dimension_values(pos_idx)
        lower = mean - neg_error
        upper = mean + pos_error

        if element.horizontal ^ self.invert_axes:
            mapping['dimension'] = 'width'
        else:
            mapping['dimension'] = 'height'

        data = dict(base=base, lower=lower, upper=upper)
        self._categorize_data(data, ('base',), element.dimensions())
        return (data, mapping, style)


    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        properties = {k: v for k, v in properties.items() if 'legend' not in k}
        for prop in ['color', 'alpha']:
            if prop not in properties:
                continue
            pval = properties.pop(prop)
            line_prop = f'line_{prop}'
            fill_prop = f'fill_{prop}'
            if line_prop not in properties:
                properties[line_prop] = pval
            if fill_prop not in properties and fill_prop in self.style_opts:
                properties[fill_prop] = pval
        properties = mpl_to_bokeh(properties)
        plot_method = self._plot_methods['single']
        glyph = plot_method(**dict(properties, **mapping))
        plot.add_layout(glyph)
        return None, glyph



class SpreadPlot(ElementPlot):

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = base_properties + fill_properties + line_properties

    _no_op_style = style_opts
    _nonvectorized_styles = style_opts
    _plot_methods = dict(single='patch')
    _stream_data = False # Plot does not support streaming data

    def _split_area(self, xs, lower, upper):
        """Splits area plots at nans and returns x- and y-coordinates for
        each area separated by nans.

        """
        xnan = np.array([np.datetime64('nat') if dtype_kind(xs) == 'M' else np.nan])
        ynan = np.array([np.datetime64('nat') if dtype_kind(lower) == 'M' else np.nan])
        split = np.where(~isfinite(xs) | ~isfinite(lower) | ~isfinite(upper))[0]
        xvals = np.split(xs, split)
        lower = np.split(lower, split)
        upper = np.split(upper, split)
        band_x, band_y = [], []
        for i, (x, l, u) in enumerate(zip(xvals, lower, upper, strict=None)):
            if i:
                x, l, u = x[1:], l[1:], u[1:]
            if not len(x):
                continue
            band_x += [np.append(x, x[::-1]), xnan]
            band_y += [np.append(l, u[::-1]), ynan]
        if len(band_x):
            xs = np.concatenate(band_x[:-1])
            ys = np.concatenate(band_y[:-1])
            return xs, ys
        return [], []

    def get_data(self, element, ranges, style):
        mapping = dict(x='x', y='y')
        xvals = element.dimension_values(0)
        mean = element.dimension_values(1)
        neg_error = element.dimension_values(2)
        pos_idx = 3 if len(element.dimensions()) > 3 else 2
        pos_error = element.dimension_values(pos_idx)
        lower = mean - neg_error
        upper = mean + pos_error

        band_x, band_y = self._split_area(xvals, lower, upper)
        if self.invert_axes:
            data = dict(x=band_y, y=band_x)
        else:
            data = dict(x=band_x, y=band_y)
        return data, mapping, style



class AreaPlot(AreaMixin, SpreadPlot):

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    selection_display = BokehOverlaySelectionDisplay()

    _stream_data = False # Plot does not support streaming data

    def get_data(self, element, ranges, style):
        mapping = dict(x='x', y='y')
        xs = element.dimension_values(0)

        if len(element.vdims) > 1:
            bottom = element.dimension_values(2)
        else:
            bottom = np.zeros(len(element))
        top = element.dimension_values(1)

        band_xs, band_ys = self._split_area(xs, bottom, top)
        if self.invert_axes:
            data = dict(x=band_ys, y=band_xs)
        else:
            data = dict(x=band_xs, y=band_ys)
        return data, mapping, style



class SpikesPlot(SpikesMixin, ColorbarPlot):

    spike_length = param.Number(default=0.5, doc="""
      The length of each spike if Spikes object is one dimensional.""")

    position = param.Number(default=0., doc="""
      The position of the lower end of each spike.""")

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    # Deprecated parameters

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `color=dim('color')`""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = base_properties + line_properties + ['cmap', 'palette']

    _nonvectorized_styles = [*base_properties, "cmap"]
    _plot_methods = dict(single='segment')

    def get_data(self, element, ranges, style):
        dims = element.dimensions()

        data = {}
        pos = self.position

        opts = self.lookup_options(element, 'plot').options
        if len(element) == 0 or self.static_source:
            data = {'x': [], 'y0': [], 'y1': []}
        else:
            data['x'] = element.dimension_values(0)
            data['y0'] = np.full(len(element), pos)
            if len(dims) > 1 and 'spike_length' not in opts:
                data['y1'] = element.dimension_values(1)+pos
            else:
                data['y1'] = data['y0']+self.spike_length

        if self.invert_axes:
            mapping = {'x0': 'y0', 'x1': 'y1', 'y0': 'x', 'y1': 'x'}
        else:
            mapping = {'x0': 'x', 'x1': 'x', 'y0': 'y0', 'y1': 'y1'}

        cdata, cmapping = self._get_color_data(element, ranges, dict(style))
        data.update(cdata)
        mapping.update(cmapping)
        self._get_hover_data(data, element)

        return data, mapping, style


class SideSpikesPlot(SpikesPlot):
    """SpikesPlot with useful defaults for plotting adjoined rug plot.

    """

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    xaxis = param.Selector(default='top-bare',
                                 objects=['top', 'bottom', 'bare', 'top-bare',
                                          'bottom-bare', None], doc="""
        Whether and where to display the xaxis, bare options allow suppressing
        all axis labels including ticks and xlabel. Valid options are 'top',
        'bottom', 'bare', 'top-bare' and 'bottom-bare'.""")

    yaxis = param.Selector(default='right-bare',
                                      objects=['left', 'right', 'bare', 'left-bare',
                                               'right-bare', None], doc="""
        Whether and where to display the yaxis, bare options allow suppressing
        all axis labels including ticks and ylabel. Valid options are 'left',
        'right', 'bare' 'left-bare' and 'right-bare'.""")

    border = param.Integer(default=5, doc="Default borders on plot")

    height = param.Integer(default=50, doc="Height of plot")

    width = param.Integer(default=50, doc="Width of plot")



class BarPlot(BarsMixin, ColorbarPlot, LegendPlot):
    """BarPlot allows generating single- or multi-category
    bar Charts, by selecting which key dimensions are
    mapped onto separate groups, categories and stacks.

    """

    multi_level = param.Boolean(default=True, doc="""
       Whether the Bars should be grouped into a second categorical axis level.""")

    stacked = param.Boolean(default=False, doc="""
       Whether the bars should be stacked or grouped.""")

    # Deprecated parameters

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `color=dim('color')`""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = (base_properties + fill_properties + line_properties +
                  ['bar_width', 'cmap'])

    _nonvectorized_styles = [*base_properties, "bar_width", "cmap"]
    _plot_methods = dict(single=('vbar', 'hbar'))

    def _axis_properties(self, axis, key, plot, dimension=None,
                         ax_mapping=None):
        if ax_mapping is None:
            ax_mapping = {"x": 0, "y": 1}
        props = super()._axis_properties(axis, key, plot, dimension, ax_mapping)
        if (not self.multi_level and not self.stacked and self.current_frame.ndims > 1 and
            ((not self.invert_axes and axis == 'x') or (self.invert_axes and axis =='y'))):
            props['separator_line_width'] = 0
            props['major_tick_line_alpha'] = 0
            # The major_label_text_* is a workaround for 0pt font size not working in Safari.
            # See: https://github.com/holoviz/holoviews/issues/5672
            props['major_label_text_font_size'] = '1px'
            props['major_label_text_alpha'] = 0
            props['major_label_text_line_height'] = 0

            props['group_text_color'] = 'black'
            props['group_text_font_style'] = "normal"
            if axis == 'x':
                props['group_text_align'] = "center"
            if 'major_label_orientation' in props:
                props['group_label_orientation'] = props.pop('major_label_orientation')
            elif axis == 'y':
                props['group_label_orientation'] = 0
                props['group_text_align'] = 'right'
                props['group_text_baseline'] = 'middle'
        return props

    def _element_transform(self, transform, element, ranges):
        if (self.multi_level or self.stacked) and len(element.kdims) > 1:
            return transform.apply(element.groupby(element.kdims[1]).collapse(), ranges=ranges, flat=True)
        return transform.apply(element, ranges=ranges, flat=True)

    def _get_factors(self, element, ranges):
        xvals, gvals = self._get_coords(element, ranges)
        if gvals is not None:
            xvals = [(x, g) for x in xvals for g in gvals]
        return ([], xvals) if self.invert_axes else (xvals, [])

    def get_stack(self, xvals, yvals, baselines, sign='positive'):
        """Iterates over a x- and y-values in a stack layer
        and appropriately offsets the layer on top of the
        previous layer.

        """
        bottoms, tops = [], []
        for x, y in zip(xvals, yvals, strict=None):
            baseline = baselines[x][sign]
            if sign == 'positive':
                bottom = baseline
                top = bottom+y
                baseline = top
            else:
                top = baseline
                bottom = top+y
                baseline = bottom
            baselines[x][sign] = baseline
            bottoms.append(bottom)
            tops.append(top)
        return bottoms, tops

    def _glyph_properties(self, *args, **kwargs):
        props = super()._glyph_properties(*args, **kwargs)
        return {k: v for k, v in props.items() if k not in ['width', 'bar_width']}

    def _add_color_data(self, ds, ranges, style, cdim, data, mapping, factors, colors):
        cdata, cmapping = self._get_color_data(ds, ranges, dict(style),
                                               factors=factors, colors=colors)
        if 'color' not in cmapping:
            return

        # Enable legend if colormapper is categorical
        cmapper = cmapping['color']['transform']
        legend_prop = 'legend_field'
        if ('color' in cmapping and self.show_legend and
            isinstance(cmapper, CategoricalColorMapper)):
            mapping[legend_prop] = cdim.name

        if not self.stacked and ds.ndims > 1 and self.multi_level:
            cmapping.pop(legend_prop, None)
            mapping.pop(legend_prop, None)

        # Merge data and mappings
        mapping.update(cmapping)
        for k, cd in cdata.items():
            if isinstance(cmapper, CategoricalColorMapper) and dtype_kind(cd) in 'uif':
                cd = categorize_array(cd, cdim)
            if k not in data or (len(data[k]) != next(len(data[key]) for key in data if key != k)):
                data[k].append(cd)
            else:
                data[k][-1] = cd

    def get_data(self, element, ranges, style):
        # Get x, y, group, stack and color dimensions
        group_dim, stack_dim, stack_order = None, None, None
        if element.ndims == 1:
            grouping = None
        elif self.stacked:
            grouping = 'stacked'
            stack_dim = element.get_dimension(1)
            if stack_dim.values:
                stack_order = stack_dim.values
            elif stack_dim in ranges and ranges[stack_dim.label].get('factors'):
                stack_order = ranges[stack_dim.label]['factors']
            else:
                stack_order = element.dimension_values(1, False)
            stack_order = list(stack_order)
            stack_data = element.dimension_values(stack_dim)
            stack_idx = stack_data == stack_data[0]
        else:
            grouping = 'grouped'
            group_dim = element.get_dimension(1)

        data = defaultdict(list)
        xdim = element.get_dimension(0)
        ydim = element.vdims[0]
        no_cidx = self.color_index is None
        color_index = (group_dim or stack_dim) if no_cidx else self.color_index
        color_dim = element.get_dimension(color_index)
        if color_dim:
            self.color_index = color_dim.label

        # Define style information
        width = style.get('bar_width', style.get('width', 1))
        if 'width' in style:
            self.param.warning("BarPlot width option is deprecated "
                               "use 'bar_width' instead.")
        cmap = style.get('cmap')
        hover = 'hover' in self.handles

        # Group by stack or group dim if necessary
        xdiff = None
        xvals = element.dimension_values(xdim)
        if group_dim is None:
            grouped = {0: element}
            is_dt = isdatetime(xvals)
            if is_dt or dtype_kind(xvals) not in 'OU':
                xslice = stack_idx if stack_order else slice(None)
                xdiff = np.abs(np.diff(xvals[xslice]))
                diff_size = len(np.unique(xdiff))
                if diff_size == 0 or (diff_size == 1 and xdiff[0] == 0):
                    xdiff = 1
                if is_dt:
                    width *= xdiff.astype('timedelta64[ms]').astype(np.int64)
                else:
                    width = width * xdiff if np.min(xdiff) < 1 else width / xdiff
                width = np.min(width)
        else:
            grouped = element.groupby(group_dim, group_type=Dataset,
                                      container_type=dict,
                                      datatype=['dataframe', 'dictionary'])

        width = abs(width)
        _y0, y1 = ranges.get(ydim.label, {'combined': (None, None)})['combined']
        if self.logy:
            bottom = (ydim.range[0] or (0.01 if y1 > 0.01 else 10**(np.log10(y1)-2)))
        else:
            bottom = 0

        # Map attributes to data
        if grouping == 'stacked':
            mapping = {'x': xdim.name, 'top': 'top',
                       'bottom': 'bottom', 'width': width}
        elif grouping == 'grouped':
            mapping = {'x': 'xoffsets', 'top': ydim.name, 'bottom': bottom, 'width': width}
        else:
            mapping = {'x': xdim.name, 'top': ydim.name, 'bottom': bottom, 'width': width}

        # Get colors
        cdim = color_dim or group_dim
        style_mapping = [v for k, v in style.items() if 'color' in k and
                         (isinstance(v, dim) or v in element)]
        if style_mapping and not no_cidx and self.color_index is not None:
            self.param.warning(f"Cannot declare style mapping for '{style_mapping[0]}' option "
                               "and declare a color_index; ignoring the color_index.")
            cdim = None

        cvals = element.dimension_values(cdim, expanded=False) if cdim else None
        if cvals is not None:
            if dtype_kind(cvals) in 'uif' and no_cidx:
                cvals = categorize_array(cvals, color_dim)

            factors = None if dtype_kind(cvals) in 'uif' else list(cvals)
            if cdim is xdim and factors:
                factors = list(categorize_array(factors, xdim))
            if cmap is None and factors:
                styles = self.style.max_cycles(len(factors))
                colors = [styles[i]['color'] for i in range(len(factors))]
                colors = [rgb2hex(c) if isinstance(c, tuple) else c for c in colors]
            else:
                colors = None
        else:
            factors, colors = None, None

        # Iterate over stacks and groups and accumulate data
        baselines = defaultdict(lambda: {'positive': bottom, 'negative': 0})
        for k, ds in grouped.items():
            k = k[0] if isinstance(k, tuple) else k
            if group_dim:
                gval = k if isinstance(k, str) else group_dim.pprint_value(k)
            # Apply stacking or grouping
            if grouping == 'stacked':
                for sign, slc in [('negative', (None, 0)), ('positive', (0, None))]:
                    slc_ds = ds.select(**{ds.vdims[0].name: slc})
                    stack_inds = [stack_order.index(v) if v in stack_order else -1
                                  for v in slc_ds[stack_dim.name]]
                    slc_ds = slc_ds.add_dimension('_stack_order', 0, stack_inds).sort('_stack_order')
                    xs = slc_ds.dimension_values(xdim)
                    ys = slc_ds.dimension_values(ydim)
                    bs, ts = self.get_stack(xs, ys, baselines, sign)
                    data['bottom'].append(bs)
                    data['top'].append(ts)
                    data[xdim.name].append(xs)
                    data[stack_dim.name].append(slc_ds.dimension_values(stack_dim))
                    if hover:
                        data[ydim.name].append(ys)
                        for vd in slc_ds.vdims[1:]:
                            data[vd.name].append(slc_ds.dimension_values(vd))
                    if not style_mapping:
                        self._add_color_data(slc_ds, ranges, style, cdim, data,
                                             mapping, factors, colors)
            elif grouping == 'grouped':
                xs = ds.dimension_values(xdim)
                ys = ds.dimension_values(ydim)
                xoffsets = [(x if dtype_kind(xs) in 'SU' else xdim.pprint_value(x), gval)
                            for x in xs]
                data['xoffsets'].append(xoffsets)
                data[ydim.name].append(ys)
                if hover: data[xdim.name].append(xs)
                if group_dim not in ds.dimensions():
                    ds = ds.add_dimension(group_dim, ds.ndims, gval)
                data[group_dim.name].append(ds.dimension_values(group_dim))
            else:
                data[xdim.name].append(xvals)
                data[ydim.name].append(ds.dimension_values(ydim))

            if hover and grouping != 'stacked':
                for vd in ds.vdims[1:]:
                    data[vd.name].append(ds.dimension_values(vd))

            if grouping != 'stacked' and not style_mapping:
                self._add_color_data(ds, ranges, style, cdim, data,
                                     mapping, factors, colors)

        # Concatenate the stacks or groups
        sanitized_data = {}
        for col, vals in data.items():
            if len(vals) == 1:
                sanitized_data[dimension_sanitizer(col)] = vals[0]
            elif vals:
                sanitized_data[dimension_sanitizer(col)] = np.concatenate(vals)

        for name, val in mapping.items():
            sanitized = None
            if isinstance(val, str):
                sanitized = dimension_sanitizer(val)
                mapping[name] = sanitized
            elif isinstance(val, dict) and 'field' in val:
                sanitized = dimension_sanitizer(val['field'])
                val['field'] = sanitized
            if sanitized is not None and sanitized not in sanitized_data:
                sanitized_data[sanitized] = []

        # Ensure x-values are categorical
        xname = dimension_sanitizer(xdim.name)
        if xname in sanitized_data and isinstance(sanitized_data[xname], np.ndarray) and dtype_kind(sanitized_data[xname]) not in 'uifM' and not isdatetime(sanitized_data[xname]):
            sanitized_data[xname] = categorize_array(sanitized_data[xname], xdim)

        # If axes inverted change mapping to match hbar signature
        if self.invert_axes:
            mapping.update({'y': mapping.pop('x'), 'left': mapping.pop('bottom'),
                            'right': mapping.pop('top'), 'height': mapping.pop('width')})

        return sanitized_data, mapping, style
