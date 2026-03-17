from collections import defaultdict
from functools import partial

import numpy as np
import param
from bokeh.models import Circle, FactorRange, HBar, VBar

from ...core import NdOverlay
from ...core.dimension import Dimension, Dimensioned
from ...core.ndmapping import sorted_context
from ...core.util import (
    dimension_sanitizer,
    is_cupy_array,
    is_dask_array,
    isfinite,
    unique_iterator,
    wrap_tuple,
)
from ...operation.stats import univariate_kde
from ...util.transform import dim
from ..mixins import MultiDistributionMixin
from .chart import AreaPlot
from .element import ColorbarPlot, CompositeElementPlot, LegendPlot
from .path import PolygonPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import base_properties, fill_properties, line_properties
from .util import decode_bytes


class DistributionPlot(AreaPlot):
    """DistributionPlot visualizes a distribution of values as a KDE.

    """

    bandwidth = param.Number(default=None, doc="""
        The bandwidth of the kernel for the density estimate.""")

    cut = param.Number(default=3, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    filled = param.Boolean(default=True, doc="""
        Whether the bivariate contours should be filled.""")

    selection_display = BokehOverlaySelectionDisplay()


class BivariatePlot(PolygonPlot):
    """Bivariate plot visualizes two-dimensional kernel density
    estimates. Additionally, by enabling the joint option, the
    marginals distributions can be plotted alongside each axis (does
    not animate or compose).

    """

    bandwidth = param.Number(default=None, doc="""
        The bandwidth of the kernel for the density estimate.""")

    cut = param.Number(default=3, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    filled = param.Boolean(default=False, doc="""
        Whether the bivariate contours should be filled.""")

    levels = param.ClassSelector(default=10, class_=(list, int), doc="""
        A list of scalar values used to specify the contour levels.""")

    selection_display = BokehOverlaySelectionDisplay(color_prop='cmap', is_cmap=True)


class BoxWhiskerPlot(MultiDistributionMixin, CompositeElementPlot, ColorbarPlot, LegendPlot):

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    outlier_radius = param.Number(default=0.01, doc="""
        The radius of the circle marker for the outliers.""")

    # Deprecated options

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `box_color=dim('color')`""")

    # X-axis is categorical
    _x_range_type = FactorRange

    # Map each glyph to a style group
    _style_groups = {'segment': 'whisker', 'vbar': 'box', 'hbar': 'box', 'circle': 'outlier'}

    style_opts = (['whisker_'+p for p in base_properties+line_properties] +
                  ['box_'+p for p in base_properties+fill_properties+line_properties] +
                  ['outlier_'+p for p in fill_properties+line_properties] +
                  ['box_width', 'cmap', 'box_cmap'])

    _nonvectorized_styles = [*base_properties, 'box_width', 'whisker_width', 'cmap', 'box_cmap']

    _stream_data = False # Plot does not support streaming data

    selection_display = BokehOverlaySelectionDisplay(color_prop='box_color')

    def _glyph_properties(self, plot, element, source, ranges, style, group=None):
        properties = dict(style, source=source)
        if self.show_legend and not element.kdims and self.overlaid:
            legend_prop = 'legend_label'
            properties[legend_prop] = element.label
        return properties

    def _apply_transforms(self, element, data, ranges, style, group=None):
        if element.ndims > 0:
            element = element.aggregate(function=np.mean)
        else:
            agg = element.aggregate(function=np.mean)
            if isinstance(agg, Dimensioned):
                element = agg
            else:
                element = element.clone([(agg,)])
        return super()._apply_transforms(element, data, ranges, style, group)

    def _get_factors(self, element, ranges):
        """Get factors for categorical axes.

        """
        if not element.kdims:
            xfactors, yfactors = [element.label], []
        else:
            factors = [key for key in element.groupby(element.kdims).data.keys()]
            if element.ndims > 1:
                factors = sorted(factors)
            factors = [tuple(d.pprint_value(k) for d, k in zip(element.kdims, key, strict=None))
                       for key in factors]
            factors = [f[0] if len(f) == 1 else f for f in factors]
            xfactors, yfactors = factors, []
        return (yfactors, xfactors) if self.invert_axes else (xfactors, yfactors)

    def _postprocess_hover(self, renderer, source):
        if not isinstance(renderer.glyph, (Circle, VBar, HBar)):
            return
        super()._postprocess_hover(renderer, source)

    def _box_stats(self, vals):
        is_finite = isfinite
        is_dask = is_dask_array(vals)
        is_cupy = is_cupy_array(vals)
        if is_cupy:
            import cupy
            percentile = cupy.percentile
            is_finite = cupy.isfinite
        elif is_dask:
            import dask.array as da
            percentile = da.percentile
        else:
            percentile = np.percentile

        vals = vals[is_finite(vals)]

        if is_dask or len(vals):
            q1, q2, q3 = (percentile(vals, q=q) for q in range(25, 100, 25))
            iqr = q3 - q1
            upper = max(vals[vals <= q3 + 1.5*iqr].max(), q3)
            lower = min(vals[vals >= q1 - 1.5*iqr].min(), q1)
        else:
            q1, q2, q3 = 0, 0, 0
            upper, lower = 0, 0
        outliers = vals[(vals > upper) | (vals < lower)]

        if is_cupy:
            return (q1.item(), q2.item(), q3.item(), upper.item(),
                    lower.item(), cupy.asnumpy(outliers))
        elif is_dask:
            return da.compute(q1, q2, q3, upper, lower, outliers)
        else:
            return q1, q2, q3, upper, lower, outliers

    def get_data(self, element, ranges, style):
        if element.kdims:
            with sorted_context(False):
                groups = element.groupby(element.kdims).data
        else:
            groups = dict([(element.label, element)])
        vdim = dimension_sanitizer(element.vdims[0].name)

        # Define CDS data
        r1_data, r2_data = ({'index': [], 'top': [], 'bottom': []} for i in range(2))
        s1_data, s2_data = ({'x0': [], 'y0': [], 'x1': [], 'y1': []} for i in range(2))
        w1_data, w2_data = ({'x0': [], 'y0': [], 'x1': [], 'y1': []} for i in range(2))
        out_data = defaultdict(list, {'index': [], vdim: []})

        # Define glyph-data mapping
        width = style.get('box_width', 0.7)
        whisker_width = style.pop('whisker_width', 0.4)/2.
        if 'width' in style:
            self.param.warning("BoxWhisker width option is deprecated "
                               "use 'box_width' instead.")
        if self.invert_axes:
            vbar_map = {'y': 'index', 'left': 'top', 'right': 'bottom', 'height': width}
            seg_map = {'y0': 'x0', 'y1': 'x1', 'x0': 'y0', 'x1': 'y1'}
            out_map = {'y': 'index', 'x': vdim, 'radius': self.outlier_radius}
        else:
            vbar_map = {'x': 'index', 'top': 'top', 'bottom': 'bottom', 'width': width}
            seg_map = {'x0': 'x0', 'x1': 'x1', 'y0': 'y0', 'y1': 'y1'}
            out_map = {'x': 'index', 'y': vdim, 'radius': self.outlier_radius}
        vbar2_map = dict(vbar_map)

        # Get color values
        if self.color_index is not None:
            cdim = element.get_dimension(self.color_index)
            cidx = element.get_dimension_index(self.color_index)
        else:
            cdim, cidx = None, None

        factors = []
        vdim = dimension_sanitizer(element.vdims[0].name)
        for key, g in groups.items():
            # Compute group label
            if element.kdims:
                label = tuple(d.pprint_value(v) for d, v in zip(element.kdims, key, strict=None))
                if len(label) == 1:
                    label = label[0]
            else:
                label = key
            hover = 'hover' in self.handles

            # Add color factor
            if cidx is not None and cidx<element.ndims:
                factors.append(cdim.pprint_value(wrap_tuple(key)[cidx]))
            else:
                factors.append(label)

            # Compute statistics
            vals = g.interface.values(g, vdim, compute=False)
            q1, q2, q3, upper, lower, outliers = self._box_stats(vals)

            # Add to CDS data
            for data in [r1_data, r2_data]:
                data['index'].append(label)
            for data in [s1_data, s2_data]:
                data['x0'].append(label)
                data['x1'].append(label)
            for data in [w1_data, w2_data]:
                data['x0'].append((*wrap_tuple(label), -whisker_width))
                data['x1'].append((*wrap_tuple(label), whisker_width))
            r1_data['top'].append(q2)
            r2_data['top'].append(q1)
            r1_data['bottom'].append(q3)
            r2_data['bottom'].append(q2)
            s1_data['y0'].append(upper)
            s2_data['y0'].append(lower)
            s1_data['y1'].append(q3)
            s2_data['y1'].append(q1)
            w1_data['y0'].append(lower)
            w1_data['y1'].append(lower)
            w2_data['y0'].append(upper)
            w2_data['y1'].append(upper)
            if len(outliers):
                out_data['index'] += [label]*len(outliers)
                out_data[vdim] += list(outliers)
                if hover:
                    for kd, k in zip(element.kdims, wrap_tuple(key), strict=None):
                        out_data[dimension_sanitizer(kd.name)] += [k]*len(outliers)
            if hover:
                for kd, k in zip(element.kdims, wrap_tuple(key), strict=None):
                    kd_name = dimension_sanitizer(kd.name)
                    if kd_name in r1_data:
                        r1_data[kd_name].append(k)
                    else:
                        r1_data[kd_name] = [k]
                    if kd_name in r2_data:
                        r2_data[kd_name].append(k)
                    else:
                        r2_data[kd_name] = [k]
                if vdim in r1_data:
                    r1_data[vdim].append(q2)
                else:
                    r1_data[vdim] = [q2]
                if vdim in r2_data:
                    r2_data[vdim].append(q2)
                else:
                    r2_data[vdim] = [q2]

        # Define combined data and mappings
        bar_glyph = 'hbar' if self.invert_axes else 'vbar'
        data = {
            bar_glyph+'_1': r1_data, bar_glyph+'_2': r2_data, 'segment_1': s1_data,
            'segment_2': s2_data, 'segment_3': w1_data, 'segment_4': w2_data,
            'circle_1': out_data
        }
        mapping = {
            bar_glyph+'_1': vbar_map, bar_glyph+'_2': vbar2_map, 'segment_1': seg_map,
            'segment_2': seg_map, 'segment_3': seg_map, 'segment_4': seg_map,
            'circle_1': out_map
        }

        # Cast data to arrays to take advantage of base64 encoding
        for gdata in [r1_data, r2_data, s1_data, s2_data, out_data]:
            for k, values in gdata.items():
                gdata[k] = np.array(values)

        # Return if not grouped
        if not element.kdims:
            return data, mapping, style

        # Define color dimension and data
        if cidx is None or cidx>=element.ndims:
            cdim = Dimension('index')
        else:
            r1_data[dimension_sanitizer(cdim.name)] = factors
            r2_data[dimension_sanitizer(cdim.name)] = factors
            factors = list(unique_iterator(factors))

        if self.show_legend:
            vbar_map['legend_field'] = cdim.name

        return data, mapping, style



class ViolinPlot(BoxWhiskerPlot):

    bandwidth = param.Number(default=None, doc="""
        Allows supplying explicit bandwidth value rather than relying
        on scott or silverman method.""")

    clip = param.NumericTuple(default=None, length=2, doc="""
        A tuple of a lower and upper bound to clip the violin at.""")

    cut = param.Number(default=5, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    inner = param.Selector(objects=['box', 'quartiles', 'stick', None],
                                 default='box', doc="""
        Inner visual indicator for distribution values:

          * box - A small box plot
          * stick - Lines indicating each sample value
          * quartiles - Indicates first, second and third quartiles
        """)

    split = param.ClassSelector(default=None, class_=(str, dim), doc="""
       The dimension to split the Violin on.""")

    violin_width = param.Number(default=0.8, doc="""
       Relative width of the violin""")

    # Deprecated options

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `violin_color=dim('color')`""")

    # Map each glyph to a style group
    _style_groups = {'patches': 'violin', 'multi_line': 'outline',
                     'segment': 'stats', 'vbar': 'box', 'scatter': 'median',
                     'hbar': 'box'}

    _draw_order = ['patches', 'multi_line', 'segment', 'vbar', 'hbar', 'circle', 'scatter']

    style_opts = ([glyph+p for p in base_properties+fill_properties+line_properties
                   for glyph in ('violin_', 'box_')] +
                  [glyph+p for p in base_properties+line_properties
                   for glyph in ('stats_', 'outline_')] +
                  [f'{glyph}_{p}' for p in ('color', 'alpha')
                   for glyph in ('box', 'violin', 'stats', 'median')] +
                  ['cmap', 'box_cmap', 'violin_cmap'])

    _stat_fns = [partial(np.percentile, q=q) for q in [25, 50, 75]]

    selection_display = BokehOverlaySelectionDisplay(color_prop='violin_fill_color')

    def _get_axis_dims(self, element):
        split_dim = dim(self.split) if isinstance(self.split, str) else self.split
        kdims = [kd for kd in element.kdims if not split_dim or kd != split_dim.dimension]
        return kdims, element.vdims[0]

    def _get_factors(self, element, ranges):
        """Get factors for categorical axes.

        """
        split_dim = dim(self.split) if isinstance(self.split, str) else self.split
        kdims = [kd for kd in element.kdims if not split_dim or kd != split_dim.dimension]
        if not kdims:
            xfactors, yfactors = [element.label], []
        else:
            factors = [key for key in element.groupby(kdims).data.keys()]
            if element.ndims > 1:
                factors = sorted(factors)
            factors = [tuple(d.pprint_value(k) for d, k in zip(kdims, key, strict=None))
                       for key in factors]
            factors = [f[0] if len(f) == 1 else f for f in factors]
            xfactors, yfactors = factors, []
        return (yfactors, xfactors) if self.invert_axes else (xfactors, yfactors)

    def _kde_data(self, element, el, key, split_dim, split_cats, **kwargs):
        vdims = el.vdims
        vdim = vdims[0]
        if self.clip:
            vdim = vdim(range=self.clip)
            el = el.clone(vdims=[vdim])

        if split_dim is not None:
            el = el.clone(kdims=element.kdims)
            all_cats = split_dim.apply(el)
            if len(split_cats) > 2:
                raise ValueError(
                    'The number of categories for split violin plots cannot be '
                    'greater than 2. Found {} categories: {}'.format(
                        len(split_cats), ', '.join(split_cats)))
            el = el.add_dimension(repr(split_dim), len(el.kdims), all_cats)
            kdes = univariate_kde(el, dimension=vdim.name, groupby=repr(split_dim), **kwargs)
            scale = 4
        else:
            split_cats = [None, None]
            kdes = {None: univariate_kde(el, dimension=vdim.name, **kwargs)}
            scale = 2

        x_range = el.range(vdim)
        xs, fill_xs, ys, fill_ys = [], [], [], []
        for i, cat in enumerate(split_cats):
            kde = kdes.get(cat)
            if kde is None:
                _xs, _ys = np.array([]), np.array([])
            else:
                _xs, _ys = (kde.dimension_values(idim) for idim in range(2))
            mask = isfinite(_ys) & (_ys>0) # Mask out non-finite and zero values
            _xs, _ys = _xs[mask], _ys[mask]

            if i == 0:
                _ys *= -1
            else:
                _ys = _ys[::-1]
                _xs = _xs[::-1]

            if split_dim:
                if len(_xs):
                    fill_xs.append([x_range[0], *_xs, x_range[-1]])
                    fill_ys.append([0, *_ys, 0])
                else:
                    fill_xs.append([])
                    fill_ys.append([])
            x_range = x_range[::-1]

            xs += list(_xs)
            ys += list(_ys)

        xs = np.array(xs)
        ys = np.array(ys)

        # this scales the width
        if split_dim:
            fill_xs = [np.asarray(x) for x in fill_xs]
            fill_ys = [[(*key, y) for y in (fy/np.abs(ys).max())*(self.violin_width/scale)]
                       if len(fy) else [] for fy in fill_ys]
        ys = (ys/np.nanmax(np.abs(ys)))*(self.violin_width/scale) if len(ys) else []
        ys = [(*key, y) for y in ys]

        line = {'ys': xs, 'xs': ys}
        if split_dim:
            kde = {'ys': fill_xs, 'xs': fill_ys}
        else:
            kde = line

        if isinstance(kdes, NdOverlay):
            kde[repr(split_dim)] = [str(k) for k in split_cats]

        bars, segments, scatter = defaultdict(list), defaultdict(list), {}
        values = el.dimension_values(vdim)
        values = values[isfinite(values)]
        if not len(values):
            pass
        elif self.inner == 'quartiles':
            if len(xs):
                for stat_fn in self._stat_fns:
                    stat = stat_fn(values)
                    sidx = np.argmin(np.abs(xs-stat))
                    sx, sy = xs[sidx], ys[sidx]
                    segments['x'].append(sx)
                    segments['y0'].append((*key, -sy[-1]))
                    segments['y1'].append(sy)
        elif self.inner == 'stick':
            if len(xs):
                for value in values:
                    sidx = np.argmin(np.abs(xs-value))
                    sx, sy = xs[sidx], ys[sidx]
                    segments['x'].append(sx)
                    segments['y0'].append((*key, -sy[-1]))
                    segments['y1'].append(sy)
        elif self.inner == 'box':
            xpos = (*key, 0)
            q1, q2, q3, upper, lower, _ = self._box_stats(values)
            segments['x'].append(xpos)
            segments['y0'].append(lower)
            segments['y1'].append(upper)
            bars['x'].append(xpos)
            bars['bottom'].append(q1)
            bars['top'].append(q3)
            scatter['x'] = xpos
            scatter['y'] = q2

        return kde, line, segments, bars, scatter


    def get_data(self, element, ranges, style):
        split_dim = dim(self.split) if isinstance(self.split, str) else self.split
        kdims = [kd for kd in element.kdims if not split_dim or split_dim.dimension != kd]

        if kdims:
            with sorted_context(False):
                groups = element.groupby(kdims).data
        else:
            groups = dict([((element.label,), element)])

        if split_dim:
            split_name = split_dim.dimension.label
            if split_name in ranges and not split_dim.ops and 'factors' in ranges[split_name]:
                split_cats = ranges[split_name].get('factors')
            elif split_dim:
                split_cats = list(unique_iterator(split_dim.apply(element)))
        else:
            split_cats = None

        # Define glyph-data mapping
        if self.invert_axes:
            bar_map = {'y': 'x', 'left': 'bottom',
                       'right': 'top', 'height': 0.1}
            kde_map = {'xs': 'ys', 'ys': 'xs'}
            if self.inner == 'box':
                seg_map = {'x0': 'y0', 'x1': 'y1', 'y0': 'x', 'y1': 'x'}
            else:
                seg_map = {'x0': 'x', 'x1': 'x', 'y0': 'y0', 'y1': 'y1'}
            scatter_map = {'x': 'y', 'y': 'x'}
            bar_glyph = 'hbar'
        else:
            bar_map = {'x': 'x', 'bottom': 'bottom',
                       'top': 'top', 'width': 0.1}
            kde_map = {'xs': 'xs', 'ys': 'ys'}
            if self.inner == 'box':
                seg_map = {'x0': 'x', 'x1': 'x', 'y0': 'y0', 'y1': 'y1'}
            else:
                seg_map = {'y0': 'x', 'y1': 'x', 'x0': 'y0', 'x1': 'y1'}
            scatter_map = {'x': 'x', 'y': 'y'}
            bar_glyph = 'vbar'

        kwargs = {'bandwidth': self.bandwidth, 'cut': self.cut}
        mapping, data = {}, {}
        kde_data, line_data, seg_data, bar_data, scatter_data = (
            defaultdict(list) for i in range(5)
        )
        for key, g in groups.items():
            key = decode_bytes(key)
            if element.kdims:
                key = tuple(d.pprint_value(k) for d, k in zip(element.kdims, key, strict=None))
            kde, line, segs, bars, scatter = self._kde_data(
                element, g, key, split_dim, split_cats, **kwargs
            )
            for k, v in segs.items():
                seg_data[k] += v
            for k, v in bars.items():
                bar_data[k] += v
            for k, v in scatter.items():
                scatter_data[k].append(v)
            for k, v in line.items():
                line_data[k].append(v)
            for k, vals in kde.items():
                if split_dim:
                    for v in vals:
                        kde_data[k].append(v)
                else:
                    kde_data[k].append(vals)

        data['multi_line_1'] = line_data
        mapping['multi_line_1'] = kde_map
        data['patches_1'] = kde_data
        mapping['patches_1'] = kde_map
        if seg_data:
            data['segment_1'] = {k: v if isinstance(v[0], tuple) else np.array(v)
                                 for k, v in seg_data.items()}
            mapping['segment_1'] = seg_map
        if bar_data:
            data[bar_glyph+'_1'] = {k: v if isinstance(v[0], tuple) else np.array(v)
                                    for k, v in bar_data.items()}
            mapping[bar_glyph+'_1'] = bar_map
        if scatter_data:
            data['scatter_1'] = {k: v if isinstance(v[0], tuple) else np.array(v)
                                 for k, v in scatter_data.items()}
            mapping['scatter_1'] = scatter_map

        if split_dim:
            factors = [str(v) for v in split_cats]
            cmapper = self._get_colormapper(
                split_dim, element, ranges, dict(style), name='violin_color_mapper',
                group='violin', factors=factors)
            style['violin_fill_color'] = {'field': repr(split_dim), 'transform': cmapper}
            if self.show_legend:
                kde_map['legend_field'] = repr(split_dim)

        for k in style.copy():
            if k.startswith('violin_line'):
                style[k.replace('violin', 'outline')] = style.pop(k)
        style['violin_line_width'] = 0

        return data, mapping, style
