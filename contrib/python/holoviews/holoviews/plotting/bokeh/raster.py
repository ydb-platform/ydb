import sys

import bokeh.core.properties as bp
import numpy as np
import param
from bokeh.model import DataModel
from bokeh.models import CustomJS, CustomJSHover, DatetimeAxis, HoverTool
from bokeh.models.dom import Div, Span, Styles, ValueOf
from panel.io import hold

from ...core.data import XArrayInterface
from ...core.util import cartesian_product, dimension_sanitizer, dtype_kind, isfinite
from ...element import Raster
from ...util.warnings import warn
from ..util import categorical_legend
from .chart import PointPlot
from .element import ColorbarPlot, LegendPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import base_properties, fill_properties, line_properties, mpl_to_bokeh
from .util import (
    BOKEH_GE_3_3_0,
    BOKEH_GE_3_4_0,
    BOKEH_GE_3_7_0,
    BOKEH_GE_3_8_0,
    BOKEH_VERSION,
    colormesh,
)

_EPOCH = np.datetime64("1970-01-01", "ns")


class HoverModel(DataModel):
    xy = bp.Any()
    data = bp.Any()

    code_js = """
    export default ({hover_model, attr, fmt}) => {
      const templating = Bokeh.require("core/util/templating");
      const value = hover_model.data[attr];
      if (fmt == "M") {
        const formatter = templating.DEFAULT_FORMATTERS.datetime;
        return formatter(value, "%Y-%m-%d %H:%M:%S")
      } else {
        const formatter = templating.get_formatter();
        return formatter(value)
      }
    }; """


class ServerHoverMixin(param.Parameterized):

    selector_in_hovertool = param.Boolean(default=True, doc="""
        Whether to show the selector in HoverTool.""")

    def _update_hover(self, element):
        tool = self.handles['hover']
        if 'hv_created' in tool.tags and isinstance(tool.tooltips, Div):
            self._hover_data = element.data
            return
        super()._update_hover(element)

    def _init_tools(self, element, callbacks=None):
        tools = super()._init_tools(element, callbacks=callbacks)

        hover = None
        for tool in tools or ():
            if isinstance(tool, HoverTool):
                hover = tool
                break

        data = element.data
        if (
            hover is None
            or "hv_created" not in tool.tags
            or not (XArrayInterface.applies(data) and "selector_columns" in data.attrs)
        ):
            return tools

        if not BOKEH_GE_3_7_0:
            bk_str = ".".join(map(str, BOKEH_VERSION))
            msg = f"selector needs Bokeh 3.7 or greater, you are using version {bk_str}."
            warn(msg, RuntimeWarning)
            hover.tooltips = Div(children=[msg])
            return tools

        self._hover_data = data

        # Get dimensions
        coords, vars = list(data.coords), list(data.data_vars)
        ht = self.hover_tooltips or {}
        if ht:
            ht = [ht] if isinstance(ht, str) else ht
            ht = dict([t[::-1] if isinstance(t, tuple) else (t, t) for t in ht])
            coords = [c for c in coords if c in ht]
            vars = [v for v in vars if v in ht]
        elif isinstance(self, RGBPlot):
            # Remove vdims (RGBA) as they are not very useful
            for vdim in map(str, element.vdims):
                if vdim in vars:
                    vars.remove(vdim)

        hover_model = HoverModel(data={})
        dims = (*coords, *vars)
        dtypes = {**data.coords.dtypes, **data.data_vars.dtypes}
        is_datetime = [dtypes[c].kind == "M" for c in data.coords]
        def _create_row(attr):
            kwargs = {
                "format": "@{custom}",
                "formatter": CustomJS(
                    args=dict(hover_model=hover_model, attr=attr, fmt=dtypes[attr].kind),
                    code=HoverModel.code_js
                )
            }
            return (
                Span(children=[f"{ht.get(attr, attr)}:"], style={"color": "#26aae1", "text_align": "right"}),
                Span(children=[ValueOf(obj=hover_model, attr="data", **kwargs)], style={"text_align": "left"}),
            )
        children = [el for dim in dims for el in _create_row(dim) if dim != "__index__"]

        # Add a horizontal ruler and show the selector if available
        selector_columns = data.attrs["selector_columns"]
        first_selector = next((i for i, dim in enumerate(dims) if dim in selector_columns), None)
        if first_selector:  # Don't show if first
            divider = [Div(style={
                "border": "none",
                "height": "1px",
                "background-color": "#ccc",
                "margin": "4px 0",
                "grid-column": "span 2",
            })]
        else:
            divider = ()

        if first_selector is not None and data.attrs.get("selector") and self.selector_in_hovertool:
            selector_row = (
                Span(children=["Selector:"], style={"color": "#26aae1", "font-weight": "bold", "text_align": "right"}),
                Span(children=[data.attrs["selector"]], style={"font-weight": "bold", "text_align": "left"}),
             )
        else:
            selector_row = ()

        first_selector = first_selector or 0
        children = [
            *children[:first_selector * 2],
            *divider,
            *selector_row,
            *children[first_selector * 2:],
        ]

        style = Styles(display="grid", grid_template_columns="auto auto", column_gap="10px")
        grid = Div(children=children, style=style)
        hover.tooltips = grid
        hover.callback = CustomJS(
            args={"position": hover_model},
            code="export default ({position}, _, {geometry: {x, y}}) => {position.xy = [x, y]}",
        )

        if BOKEH_GE_3_8_0:
            hover.filters = {"": CustomJS(
                args=dict(hover_model=hover_model),
                code="""export default ({hover_model}) => hover_model.data["__index__"] != -1"""
            )}

        def on_change(attr, old, new):
            if np.isinf(new).all():
                return
            if is_datetime[0]:
                new[0] = _EPOCH + np.timedelta64(int(new[0] * 1e6), "ns")
            if is_datetime[1]:
                new[1] = _EPOCH + np.timedelta64(int(new[1] * 1e6), "ns")
            try:
                data_sel = self._hover_data.sel(
                    **dict(zip(self._hover_data.coords, new, strict=True)),
                    method="nearest"
                ).to_dict()
            except KeyError:
                # Can happen when a coord is empty, e.g. xlim=(0, 0)
                return
            data_coords = {dim: data_sel['coords'][dim]['data'] for dim in coords}
            data_vars = {dim: data_sel['data_vars'][dim]['data'] for dim in vars}
            with hold(self.document):
                hover_model.update(data={**data_coords, **data_vars})
                if self.comm:  # Jupyter Notebook
                    self.push()

        hover_model.on_change("xy", on_change)

        return tools


class RasterPlot(ServerHoverMixin, ColorbarPlot):

    clipping_colors = param.Dict(default={'NaN': 'transparent'})

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    style_opts = [*base_properties, 'cmap', 'alpha']

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='image')

    selection_display = BokehOverlaySelectionDisplay()

    def _hover_opts(self, element):
        xdim, ydim = element.kdims
        tooltips = [(xdim.pprint_label, '$x'), (ydim.pprint_label, '$y')]
        vdims = element.vdims
        tooltips.append((vdims[0].pprint_label, '@image'))
        for vdim in vdims[1:]:
            vname = dimension_sanitizer(vdim.name)
            tooltips.append((vdim.pprint_label, f'@{{{vname}}}'))
        return tooltips, {}

    def _postprocess_hover(self, renderer, source):
        super()._postprocess_hover(renderer, source)
        hover = self.handles.get('hover')
        if not (hover and isinstance(hover.tooltips, list)):
            return

        xaxis = self.handles['xaxis']
        yaxis = self.handles['yaxis']

        code = """
        var {ax} = special_vars.{ax};
        var date = new Date({ax});
        return date.toISOString().slice(0, 19).replace('T', ' ')
        """
        tooltips, formatters = [], dict(hover.formatters)
        for (name, formatter) in hover.tooltips:
            if isinstance(xaxis, DatetimeAxis) and formatter == '$x':
                xhover = CustomJSHover(code=code.format(ax='x'))
                formatters['$x'] = xhover
                formatter += '{custom}'
            if isinstance(yaxis, DatetimeAxis) and formatter == '$y':
                yhover = CustomJSHover(code=code.format(ax='y'))
                formatters['$y'] = yhover
                formatter += '{custom}'
            tooltips.append((name, formatter))

        if not BOKEH_GE_3_4_0:  # https://github.com/bokeh/bokeh/issues/13598
            datetime_code = """
            if (value === -9223372036854776) {
                return "NaN"
            } else {
                const date = new Date(value);
                return date.toISOString().slice(0, 19).replace('T', ' ')
            }
            """
            for key, formatter in formatters.values():
                if isinstance(formatter, str) and formatter.lower() == "datetime":
                    formatters[key] = CustomJSHover(code=datetime_code)

        hover.tooltips = tooltips
        hover.formatters = formatters

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.hmap.type == Raster:
            self.invert_yaxis = not self.invert_yaxis

    def get_data(self, element, ranges, style):
        mapping = dict(image='image', x='x', y='y', dw='dw', dh='dh')
        val_dim = element.vdims[0]
        style['color_mapper'] = self._get_colormapper(val_dim, element, ranges, style)
        if 'alpha' in style:
            style['global_alpha'] = style['alpha']

        if self.static_source:
            return {}, mapping, style

        if type(element) is Raster:
            l, b, r, t = element.extents
            if self.invert_axes:
                l, b, r, t = b, l, t, r
        else:
            l, b, r, t = element.bounds.lbrt()
            if self.invert_axes:
                l, b, r, t = b, l, t, r

        dh, dw = t-b, r-l
        data = dict(x=[l], y=[b], dw=[dw], dh=[dh])

        for i, vdim in enumerate(element.vdims, 2):
            if i > 2 and 'hover' not in self.handles:
                break
            img = element.dimension_values(i, flat=False)
            if dtype_kind(img) == 'b':
                img = img.astype(np.int8)
            if 0 in img.shape:
                img = np.array([[np.nan]])
            if self.invert_axes ^ (type(element) is Raster):
                img = img.T
            key = 'image' if i == 2 else dimension_sanitizer(vdim.name)
            data[key] = [img]

        return (data, mapping, style)


class SyntheticLegendMixin(LegendPlot):

    def _init_glyphs(self, plot, element, ranges, source):
        super()._init_glyphs(plot, element, ranges, source)
        if not ('holoviews.operation.datashader' in sys.modules and self.show_legend):
            return
        try:
            cmap = self.lookup_options(element, 'style').options.get("cmap")
            legend = categorical_legend(
                element,
                backend=self.backend,
                # Only adding if it not None to not overwrite the default
                **({"cmap": cmap} if cmap else {})
            )
        except Exception:
            return
        if legend is None:
            return
        legend_params = {k: v for k, v in self.param.values().items()
                         if k.startswith('legend')}
        self._legend_plot = PointPlot(legend, keys=[], overlaid=1, **legend_params)
        self._legend_plot.initialize_plot(plot=plot)
        self._legend_plot.handles['glyph_renderer'].tags.append('hv_legend')
        self.handles['synthetic_color_mapper'] = self._legend_plot.handles['color_color_mapper']


class RGBPlot(ServerHoverMixin, SyntheticLegendMixin):

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    style_opts = ['alpha', *base_properties]

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='image_rgba')

    selection_display = BokehOverlaySelectionDisplay()

    def __init__(self, hmap, **params):
        super().__init__(hmap, **params)
        self._legend_plot = None

    def _hover_opts(self, element):
        xdim, ydim = element.kdims
        return [(xdim.pprint_label, '$x'), (ydim.pprint_label, '$y'),
                ('RGBA', '@image')], {}

    def get_data(self, element, ranges, style):
        mapping = dict(image='image', x='x', y='y', dw='dw', dh='dh')
        if 'alpha' in style:
            style['global_alpha'] = style['alpha']

        if self.static_source:
            return {}, mapping, style

        img = np.dstack([element.dimension_values(d, flat=False)
                         for d in element.vdims])

        nan_mask = np.isnan(img)
        img[nan_mask] = 0

        if img.ndim == 3:
            img_max = img.max() if img.size else np.nan
            # Can be 0 to 255 if nodata has been used
            if dtype_kind(img) == 'f' and img_max <= 1:
                img = img*255
                # img_max * 255 <- have no effect
            if img.size and (img.min() < 0 or img_max > 255):
                self.param.warning('Clipping input data to the valid '
                                   'range for RGB data ([0..1] for '
                                   'floats or [0..255] for integers).')
                img = np.clip(img, 0, 255)

            if img.dtype.name != 'uint8':
                img = img.astype(np.uint8)
            if img.shape[2] == 3: # alpha channel not included
                alpha = np.full(img.shape[:2], 255, dtype='uint8')
                img = np.dstack([img, alpha])
            N, M, _ = img.shape
            #convert image NxM dtype=uint32
            if not img.flags['C_CONTIGUOUS']:
                img = img.copy()
            img = img.view(dtype=np.uint32).reshape((N, M))

        img[nan_mask.any(-1)] = 0

        # Ensure axis inversions are handled correctly
        l, b, r, t = element.bounds.lbrt()
        if self.invert_axes:
            img = img.T
            l, b, r, t = b, l, t, r

        dh, dw = t-b, r-l

        if 0 in img.shape:
            img = np.zeros((1, 1), dtype=np.uint32)

        data = dict(image=[img], x=[l], y=[b], dw=[dw], dh=[dh])
        return (data, mapping, style)


class ImageStackPlot(RasterPlot, SyntheticLegendMixin):

    _plot_methods = dict(single='image_stack')

    cnorm = param.Selector(default='eq_hist', objects=['linear', 'log', 'eq_hist'], doc="""
        Color normalization to be applied during colormapping.""")

    start_alpha = param.Integer(default=0, bounds=(0, 255))

    end_alpha = param.Integer(default=255, bounds=(0, 255))

    num_colors = param.Integer(default=10)

    def _get_cmapper_opts(self, low, high, factors, colors):
        from bokeh.models import WeightedStackColorMapper
        from bokeh.palettes import varying_alpha_palette

        AlphaMapper, _ = super()._get_cmapper_opts(low, high, factors, colors)
        palette = varying_alpha_palette(
            color="#000",
            n=self.num_colors,
            start_alpha=self.start_alpha,
            end_alpha=self.end_alpha,
        )
        alpha_mapper = AlphaMapper(palette=palette)
        opts = {"alpha_mapper": alpha_mapper}

        if "NaN" in colors:
            opts["nan_color"] = colors["NaN"]

        return WeightedStackColorMapper, opts

    def _get_colormapper(self, eldim, element, ranges, style, factors=None,
                         colors=None, group=None, name='color_mapper'):
        indices = None
        vdims = element.vdims
        if isinstance(style.get("cmap"), dict):
            dict_cmap = style["cmap"]
            missing = [vd.name for vd in vdims if vd.name not in dict_cmap]
            if missing:
                missing_str = "', '".join(sorted(missing))
                raise ValueError(
                    "The supplied cmap dictionary must have the same "
                    f"value dimensions as the element. Missing: '{missing_str}'"
                )
            keys, values = zip(*dict_cmap.items(), strict=None)
            style["cmap"] = list(values)
            indices = [keys.index(vd.name) for vd in vdims]

        cmapper = super()._get_colormapper(
            eldim, element, ranges, style, factors=factors,
            colors=colors, group=group, name=name
        )

        if indices is None:
            num_elements = len(vdims)
            step_size = len(cmapper.palette) // num_elements
            indices = np.arange(num_elements) * step_size

        cmapper.palette = np.array(cmapper.palette)[indices].tolist()
        return cmapper

    def get_data(self, element, ranges, style):
        mapping = dict(image="image", x="x", y="y", dw="dw", dh="dh")
        x, y, z = element.dimensions()[:3]

        mapping["color_mapper"] = self._get_colormapper(z, element, ranges, style)

        img = np.dstack([
            element.dimension_values(vd, flat=False)
            if not self.invert_axes
            else element.dimension_values(vd, flat=False).transpose()
            for vd in element.vdims
        ])
        if 0 in img.shape[:2]:  # Means we don't have any data
            img = np.array([[[np.nan]]])
        # Ensure axis inversions are handled correctly
        l, b, r, t = element.bounds.lbrt()
        if self.invert_axes:
            # transposed in dstack
            l, b, r, t = b, l, t, r

        x = [l]
        y = [b]
        dh, dw = t - b, r - l
        if self.invert_xaxis:
            l, r = r, l
            x = [r]
        if self.invert_yaxis:
            b, t = t, b
            y = [t]

        data = dict(image=[img], x=x, y=y, dw=[dw], dh=[dh])
        return (data, mapping, style)

    def _hover_opts(self, element):
        # Bokeh 3.3 has simple support for multi hover in a tuple.
        # https://github.com/bokeh/bokeh/pull/13193
        # https://github.com/bokeh/bokeh/pull/13366
        if BOKEH_GE_3_3_0:
            xdim, ydim = element.kdims
            vdim = ", ".join([d.pprint_label for d in element.vdims])
            return [(xdim.pprint_label, '$x'), (ydim.pprint_label, '$y'), (vdim, '@image')], {}
        else:
            xdim, ydim = element.kdims
            return [(xdim.pprint_label, '$x'), (ydim.pprint_label, '$y')], {}


class HSVPlot(RGBPlot):

    def get_data(self, element, ranges, style):
        return super().get_data(element.rgb, ranges, style)


class QuadMeshPlot(ColorbarPlot):

    clipping_colors = param.Dict(default={'NaN': 'transparent'})

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = ['cmap', *base_properties, *line_properties, *fill_properties]

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='quad')

    def get_data(self, element, ranges, style):
        x, y, z = element.dimensions()[:3]

        if self.invert_axes: x, y = y, x
        cmapper = self._get_colormapper(z, element, ranges, style)
        cmapper = {'field': dimension_sanitizer(z.name), 'transform': cmapper}

        irregular = (element.interface.irregular(element, x) or
                     element.interface.irregular(element, y))
        if irregular:
            mapping = dict(xs='xs', ys='ys', fill_color=cmapper)
        else:
            mapping = {'left': 'left', 'right': 'right',
                       'fill_color': cmapper,
                       'top': 'top', 'bottom': 'bottom'}

        if self.static_source:
            return {}, mapping, style

        x, y = dimension_sanitizer(x.name), dimension_sanitizer(y.name)

        zdata = element.dimension_values(z, flat=False)
        hover_data = {}

        if irregular:
            dims = element.kdims
            if self.invert_axes: dims = dims[::-1]
            X, Y = (element.interface.coords(element, d, expanded=True, edges=True)
                    for d in dims)
            X, Y = colormesh(X, Y)
            zvals = zdata.T.flatten() if self.invert_axes else zdata.flatten()
            XS, YS = [], []
            mask = []
            xc, yc = [], []
            for xs, ys, zval in zip(X, Y, zvals, strict=None):
                xs, ys = xs[:-1], ys[:-1]
                if isfinite(zval) and all(isfinite(xs)) and all(isfinite(ys)):
                    XS.append(list(xs))
                    YS.append(list(ys))
                    mask.append(True)
                    if 'hover' in self.handles:
                        xc.append(xs.mean())
                        yc.append(ys.mean())
                else:
                    mask.append(False)
            mask = np.array(mask)

            data = {'xs': XS, 'ys': YS, dimension_sanitizer(z.name): zvals[mask]}
            if 'hover' in self.handles:
                if not self.static_source:
                    hover_data = self._collect_hover_data(element, mask, irregular=True)
                hover_data[x] = np.array(xc)
                hover_data[y] = np.array(yc)
        else:
            xc, yc = (element.interface.coords(element, x, edges=True, ordered=True),
                      element.interface.coords(element, y, edges=True, ordered=True))

            x0, y0 = cartesian_product([xc[:-1], yc[:-1]], copy=True)
            x1, y1 = cartesian_product([xc[1:], yc[1:]], copy=True)
            zvals = zdata.flatten() if self.invert_axes else zdata.T.flatten()
            data = {'left': x0, 'right': x1, dimension_sanitizer(z.name): zvals,
                    'bottom': y0, 'top': y1}

            if 'hover' in self.handles and not self.static_source:
                hover_data = self._collect_hover_data(element)
                hover_data[x] = element.dimension_values(x)
                hover_data[y] = element.dimension_values(y)

        data.update(hover_data)

        return data, mapping, style

    def _collect_hover_data(self, element, mask=(), irregular=False):
        """Returns a dict mapping hover dimension names to flattened arrays.

        Note that `Quad` glyphs are used when given 1-D coords but `Patches` are
        used for "irregular" 2-D coords, and Bokeh inserts data into these glyphs
        in the opposite order such that the relationship b/w the `invert_axes`
        parameter and the need to transpose the arrays before flattening is
        reversed.

        """
        transpose = self.invert_axes if irregular else not self.invert_axes

        hover_dims = element.dimensions()[3:]
        hover_vals = [element.dimension_values(hover_dim, flat=False)
                      for hover_dim in hover_dims]
        hover_data = {}
        for hdim, hvals in zip(hover_dims, hover_vals, strict=None):
            hdat = hvals.T.flatten() if transpose else hvals.flatten()
            hover_data[dimension_sanitizer(hdim.name)] = hdat[mask]
        return hover_data

    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        properties = mpl_to_bokeh(properties)
        properties = dict(properties, **mapping)
        if 'xs' in mapping:
            renderer = plot.patches(**properties)
        else:
            renderer = plot.quad(**properties)
        if self.colorbar and 'color_mapper' in self.handles:
            self._draw_colorbar(plot, self.handles['color_mapper'])
        return renderer, renderer.glyph
