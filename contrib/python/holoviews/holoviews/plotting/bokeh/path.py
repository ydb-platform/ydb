from collections import defaultdict

import numpy as np
import param
from bokeh.models import FactorRange

from ...core import util
from ...core.dimension import Dimension
from ...core.util import arraylike_types, dtype_kind
from ...element import Contours, Polygons
from ...util.transform import dim
from .callbacks import PolyDrawCallback, PolyEditCallback
from .element import ColorbarPlot, LegendPlot, OverlayPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import (
    base_properties,
    expand_batched_style,
    fill_properties,
    line_properties,
    mpl_to_bokeh,
    validate,
)
from .util import multi_polygons_data


class PathPlot(LegendPlot, ColorbarPlot):

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    # Deprecated options

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `color=dim('color')`""")

    style_opts = base_properties + line_properties + ['cmap']

    _plot_methods = dict(single='multi_line', batched='multi_line')
    _mapping = dict(xs='xs', ys='ys')
    _nonvectorized_styles = [*base_properties, 'cmap']
    _batched_style_opts = line_properties

    def _element_transform(self, transform, element, ranges):
        if isinstance(element, Contours):
            data = super()._element_transform(transform, element, ranges)
            new_data = []
            for d in data:
                if isinstance(d, arraylike_types) and len(d) == 1:
                    new_data.append(d[0])
                else:
                    new_data.append(d)
            return np.array(new_data)

        transformed = []
        for el in element.split():
            new_el = transform.apply(el, ranges=ranges, flat=True)
            if len(new_el) == 1:
                kdim_length = len(el[el.kdims[0]])
                transformed.append(np.tile(new_el, kdim_length - 1))
            else:
                transformed.append(new_el)

        return np.concatenate(transformed)

    def _hover_opts(self, element):
        cdim = element.get_dimension(self.color_index)
        if self.batched:
            dims = list(self.hmap.last.kdims)+self.hmap.last.last.vdims
        else:
            dims = list(self.overlay_dims.keys())+self.hmap.last.vdims
        if cdim not in dims and cdim is not None:
            dims.append(cdim)
        return dims, {}


    def _get_hover_data(self, data, element):
        """Initializes hover data based on Element dimension values.

        """
        if 'hover' not in self.handles or self.static_source:
            return

        for k, v in self.overlay_dims.items():
            dim = util.dimension_sanitizer(k.name)
            if dim not in data:
                data[dim] = [v] * len(next(iter(data.values())))


    def get_data(self, element, ranges, style):
        color = style.get('color', None)
        cdim = None
        if isinstance(color, str) and not validate('color', color):
            cdim = element.get_dimension(color)
        elif isinstance(color, Dimension):
            # Handle hv.Dimension() objects directly
            cdim = element.get_dimension(color.name) if color.name in element else color
        elif self.color_index is not None:
            cdim = element.get_dimension(self.color_index)

        scalar = element.interface.isunique(element, cdim, per_geom=True) if cdim else False
        style_mapping = {
            (s, v) for s, v in style.items() if (s not in self._nonvectorized_styles) and
            (isinstance(v, str) and v in element) or isinstance(v, (dim, Dimension)) and
            not (not isinstance(v, (dim, Dimension)) and v == color and s == 'color')}
        mapping = dict(self._mapping)

        if not (cdim or style_mapping or 'hover' in self.handles):
            if self.static_source:
                data = {}
            else:
                paths = element.split(datatype='columns', dimensions=element.kdims)
                xs, ys = ([path[kd.name] for path in paths] for kd in element.kdims)
                if self.invert_axes:
                    xs, ys = ys, xs
                data = dict(xs=xs, ys=ys)
            return data, mapping, style

        hover = 'hover' in self.handles
        vals = defaultdict(list)
        if hover:
            vals.update({util.dimension_sanitizer(vd.name): [] for vd in element.vdims})
        if cdim:
            dim_name = util.dimension_sanitizer(cdim.name)
            cmapper = self._get_colormapper(cdim, element, ranges, style)
            mapping['line_color'] = {'field': dim_name, 'transform': cmapper}
            vals[dim_name] = []

        xpaths, ypaths = [], []
        for path in element.split():
            cols = path.columns(path.kdims)
            xs, ys = (cols[kd.name] for kd in element.kdims)
            alen = len(xs)

            if cdim:
                scalar = path.interface.isunique(path, cdim, per_geom=True)
                if scalar:
                    # one value per geometry; repeat per segment
                    cval = path.dimension_values(cdim, expanded=False)[0]
                    vals[dim_name].append(np.full(alen-1, cval))
                else:
                    # per-vertex values; drop last to match segments
                    cvals = path.dimension_values(cdim, expanded=True)
                    vals[dim_name].append(cvals[:-1])

            xpaths += [xs[s1:s2+1] for (s1, s2) in zip(range(alen-1), range(1, alen+1), strict=None)]
            ypaths += [ys[s1:s2+1] for (s1, s2) in zip(range(alen-1), range(1, alen+1), strict=None)]
            if not hover:
                continue
            for vd in element.vdims:
                if vd == cdim:
                    continue
                values = path.dimension_values(vd)[:-1]
                vd_name = util.dimension_sanitizer(vd.name)
                vals[vd_name].append(values)

        values = {d: np.concatenate(vs) if len(vs) else [] for d, vs in vals.items()}
        if self.invert_axes:
            xpaths, ypaths = ypaths, xpaths
        data = dict(xs=xpaths, ys=ypaths, **values)
        self._get_hover_data(data, element)
        return data, mapping, style


    def get_batched_data(self, element, ranges=None):
        data = defaultdict(list)

        zorders = self._updated_zorders(element)
        for (key, el), zorder in zip(element.data.items(), zorders, strict=None):
            el_opts = self.lookup_options(el, 'plot').options
            self.param.update(**{k: v for k, v in el_opts.items()
                                    if k not in OverlayPlot._propagate_options})
            style = self.lookup_options(el, 'style')
            style = style.max_cycles(len(self.ordering))[zorder]
            self.overlay_dims = dict(zip(element.kdims, key, strict=None))
            eldata, elmapping, style = self.get_data(el, ranges, style)
            for k, eld in eldata.items():
                data[k].extend(eld)

            # Skip if data is empty
            if not eldata:
                continue

            # Apply static styles
            nvals = len(next(iter(eldata.values())))
            sdata, smapping = expand_batched_style(style, self._batched_style_opts,
                                                   elmapping, nvals)
            elmapping.update({k: v for k, v in smapping.items() if k not in elmapping})
            for k, v in sdata.items():
                data[k].extend(list(v))

        return data, elmapping, style


class DendrogramPlot(PathPlot):

    def initialize_plot(self, ranges=None, plot=None, plots=None, source=None):
        plot = super().initialize_plot(ranges, plot, plots, source)
        if self.adjoined:
            if self.layout_num and self.shared_axes:
                msg = "Adjoined dendrogram in a Layout, does not currently support `.opts(shared_axes=True)`"
                raise NotImplementedError(msg)
            pos = ["main", "right", "top"][len(plots)]
            main = self.adjoined[0]
            if pos == "right":
                if self.width == self.param.width.default:
                    plot.width = 80
                self._update_adjoined_figure(main, plot, "y")
            elif pos == "top":
                if self.height == self.param.height.default:
                    plot.height = 80
                self._update_adjoined_figure(main, plot, "x")
        return plot

    def _update_adjoined_figure(self, main, side, dim):
        main_dim = getattr(main, f"{dim}_range").name
        side_dim = f"{dim}s"
        data = side.renderers[0].data_source.data
        if isinstance(getattr(main, f"{dim}_range"), FactorRange):
            # 0.5 is the factor used by Bokeh to convert a synthetic
            # coordinate into a categorical factor.
            # data_min.min() will for Scipy dendogram calculation be 5.
            data_adj = np.asarray(data[side_dim])
            data[side_dim] = list(0.5 / data_adj.min() * data_adj)
        else:
            main_src = main.renderers[0].data_source.data
            data_adj, data_main = np.asarray(data[side_dim]), np.asarray(main_src.get(main_dim, main_src.get(f"{main_dim}s")))
            if data_adj.size and data_main.size:
                x1, x2, y1, y2 = data_adj.min(), data_adj.max(), data_main.min(), data_main.max()
                data[side_dim] = list((y2 - y1) / (x2 - x1) * (data_adj - x1) + y1)
            else:
                data[side_dim] = data_adj

        # Update range and scale to match main plot
        setattr(side, f"{dim}_range", getattr(main, f"{dim}_range"))
        setattr(side, f"{dim}_scale", getattr(main, f"{dim}_scale"))


class ContourPlot(PathPlot):

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    # Deprecated options

    color_index = param.ClassSelector(default=0, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `color=dim('color')`""")

    _color_style = 'line_color'
    _nonvectorized_styles = [*base_properties, 'cmap']

    def __init__(self, *args, **params):
        super().__init__(*args, **params)
        self._has_holes = None

    def _hover_opts(self, element):
        if self.batched:
            dims = list(self.hmap.last.kdims)+self.hmap.last.last.vdims
        else:
            dims = list(self.overlay_dims.keys())+self.hmap.last.vdims
        return dims, {}

    def _get_hover_data(self, data, element):
        """Initializes hover data based on Element dimension values.
        If empty initializes with no data.

        """
        if 'hover' not in self.handles or self.static_source:
            return

        interface = element.interface
        scalar_kwargs = {'per_geom': True} if interface.multi else {}
        for d in element.vdims:
            dim = util.dimension_sanitizer(d.name)
            if dim not in data:
                if interface.isunique(element, d, **scalar_kwargs):
                    data[dim] = element.dimension_values(d, expanded=False)
                else:
                    data[dim] = element.split(datatype='array', dimensions=[d])

        for k, v in self.overlay_dims.items():
            dim = util.dimension_sanitizer(k.name)
            if dim not in data:
                data[dim] = [v] * len(next(iter(data.values())))

    def get_data(self, element, ranges, style):
        if self._has_holes is None:
            draw_callbacks = any(isinstance(cb, (PolyDrawCallback, PolyEditCallback))
                                 for cb in self.callbacks)
            has_holes = (isinstance(element, Polygons) and not draw_callbacks)
            self._has_holes = has_holes
        else:
            has_holes = self._has_holes

        if not element.interface.multi:
            element = element.clone([element.data], datatype=type(element).datatype)

        if self.static_source:
            data = {}
            xs = self.handles['cds'].data['xs']
        else:
            if has_holes:
                xs, ys = multi_polygons_data(element)
            else:
                xs, ys = (list(element.dimension_values(kd, expanded=False))
                          for kd in element.kdims)
            if self.invert_axes:
                xs, ys = ys, xs
            data = dict(xs=xs, ys=ys)
        mapping = dict(self._mapping)
        self._get_hover_data(data, element)

        color, fill_color = style.get('color'), style.get('fill_color')
        if (((isinstance(color, dim) and color.applies(element)) or color in element) or
            (isinstance(fill_color, dim) and fill_color.applies(element)) or fill_color in element):
            cdim = None
        else:
            cidx = self.color_index+2 if isinstance(self.color_index, int) else self.color_index
            cdim = element.get_dimension(cidx)

        if cdim is None:
            return data, mapping, style

        dim_name = util.dimension_sanitizer(cdim.name)
        values = element.dimension_values(cdim, expanded=False)
        data[dim_name] = values

        factors = None
        if cdim.label in ranges and 'factors' in ranges[cdim.label]:
            factors = ranges[cdim.label]['factors']
        elif dtype_kind(values) in 'SUO' and len(values):
            if isinstance(values[0], np.ndarray):
                values = np.concatenate(values)
            factors = util.unique_array(values)
        cmapper = self._get_colormapper(cdim, element, ranges, style, factors)
        mapping[self._color_style] = {'field': dim_name, 'transform': cmapper}
        if self.show_legend:
            mapping['legend_field'] = dim_name
        return data, mapping, style

    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        plot_method = properties.pop('plot_method', None)
        properties = mpl_to_bokeh(properties)
        data = dict(properties, **mapping)
        if self._has_holes:
            plot_method = 'multi_polygons'
        elif plot_method is None:
            plot_method = self._plot_methods.get('single')
        renderer = getattr(plot, plot_method)(**data)
        if self.colorbar:
            for k, v in list(self.handles.items()):
                if not k.endswith('color_mapper'):
                    continue
                self._draw_colorbar(plot, v, k[:-12])
        return renderer, renderer.glyph


class PolygonPlot(ContourPlot):

    style_opts = base_properties + line_properties + fill_properties + ['cmap']
    _plot_methods = dict(single='patches', batched='patches')
    _batched_style_opts = line_properties + fill_properties
    _color_style = 'fill_color'

    selection_display = BokehOverlaySelectionDisplay()
