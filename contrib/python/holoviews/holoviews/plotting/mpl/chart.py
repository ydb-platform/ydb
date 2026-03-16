import matplotlib as mpl
import numpy as np
import param
from matplotlib.collections import LineCollection
from matplotlib.dates import DateFormatter, date2num

from ...core.dimension import Dimension
from ...core.options import Store, abbreviated_exception
from ...core.util import (
    dt64_to_dt,
    dt_to_int,
    dtype_kind,
    isdatetime,
    isfinite,
    isscalar,
    match_spec,
    search_indices,
    unique_array,
)
from ...element import HeatMap, Raster
from ...operation import interpolate_curve
from ...util.transform import dim
from ..mixins import AreaMixin, BarsMixin, SpikesMixin
from ..plot import PlotSelector
from ..util import compute_sizes, dim_range_key, get_min_distance, get_sideplot_ranges
from .element import ColorbarPlot, ElementPlot, LegendPlot
from .path import PathPlot
from .plot import AdjoinedPlot, mpl_rc_context
from .util import MPL_GE_3_7_0, MPL_GE_3_9_0, MPL_VERSION


class ChartPlot(ElementPlot):
    """Baseclass to plot Chart elements.

    """


class CurvePlot(ChartPlot):
    """CurvePlot can plot Curve and ViewMaps of Curve, which can be
    displayed as a single frame or animation. Axes, titles and legends
    are automatically generated from dim_info.

    If the dimension is set to cyclic in the dim_info it will rotate
    the curve so that minimum y values are at the minimum x value to
    make the plots easier to interpret.

    """

    autotick = param.Boolean(default=False, doc="""
        Whether to let matplotlib automatically compute tick marks
        or to allow the user to control tick marks.""")

    interpolation = param.Selector(objects=['linear', 'steps-mid',
                                                  'steps-pre', 'steps-post'],
                                         default='linear', doc="""
        Defines how the samples of the Curve are interpolated,
        default is 'linear', other options include 'steps-mid',
        'steps-pre' and 'steps-post'.""")

    relative_labels = param.Boolean(default=False, doc="""
        If plotted quantity is cyclic and center_cyclic is enabled,
        will compute tick labels relative to the center.""")

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    show_grid = param.Boolean(default=False, doc="""
        Enable axis grid.""")

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['alpha', 'color', 'visible', 'linewidth', 'linestyle', 'marker', 'ms']

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='plot')

    def get_data(self, element, ranges, style):
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        if 'steps' in self.interpolation:
            element = interpolate_curve(element, interpolation=self.interpolation)
        xs = element.dimension_values(0)
        ys = element.dimension_values(1)
        dims = element.dimensions()
        if isdatetime(xs):
            dimtype = element.get_dimension_type(0)
            dt_format = Dimension.type_formatters.get(dimtype, '%Y-%m-%d %H:%M:%S')
            dims[0] = dims[0].clone(value_format=DateFormatter(dt_format))
        coords = (ys, xs) if self.invert_axes else (xs, ys)
        return coords, style, {'dimensions': dims}

    def init_artists(self, ax, plot_args, plot_kwargs):
        xs, ys = plot_args
        if isdatetime(xs):
            if MPL_GE_3_9_0:
                artist = ax.plot(xs, ys, '-', **plot_kwargs)[0]
            else:
                artist = ax.plot_date(xs, ys, '-', **plot_kwargs)[0]
        else:
            artist = ax.plot(xs, ys, **plot_kwargs)[0]
        return {'artist': artist}

    def update_handles(self, key, axis, element, ranges, style):
        artist = self.handles['artist']
        (xs, ys), style, axis_kwargs = self.get_data(element, ranges, style)
        artist.set_xdata(xs)
        artist.set_ydata(ys)
        return axis_kwargs



class ErrorPlot(ColorbarPlot):
    """ErrorPlot plots the ErrorBar Element type and supporting
    both horizontal and vertical error bars via the 'horizontal'
    plot option.

    """

    style_opts = ['edgecolor', 'elinewidth', 'capsize', 'capthick',
                  'barsabove', 'lolims', 'uplims', 'xlolims',
                  'errorevery', 'xuplims', 'alpha', 'linestyle',
                  'linewidth', 'markeredgecolor', 'markeredgewidth',
                  'markerfacecolor', 'markersize', 'solid_capstyle',
                  'solid_joinstyle', 'dashes', 'color']

    _plot_methods = dict(single='errorbar')

    def init_artists(self, ax, plot_data, plot_kwargs):
        handles = ax.errorbar(*plot_data, **plot_kwargs)
        bottoms, tops = None, None
        if MPL_VERSION >= (2, 0, 0):
            _, caps, verts = handles
            if caps:
                bottoms, tops = caps
        else:
            _, (bottoms, tops), verts = handles
        return {'bottoms': bottoms, 'tops': tops, 'verts': verts[0], 'artist': verts[0]}

    def get_data(self, element, ranges, style):
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        color = style.pop('color', None)
        if isinstance(color, np.ndarray):
            style['ecolor'] = color
        if 'edgecolor' in style:
            style['ecolor'] = style.pop('edgecolor')
        if 'linewidth' in style:
            # Raise ValueError if a numpy array, so needs to be a list.
            style["elinewidth"] = np.asarray(style.pop('linewidth')).tolist()
        c = style.get('c')
        if isinstance(c, np.ndarray):
            with abbreviated_exception():
                raise ValueError('Mapping a continuous or categorical '
                                 'dimension to a color on a ErrorBarPlot '
                                 f'is not supported by the {self.renderer.backend} backend. '
                                 'To map a dimension to a color supply '
                                 'an explicit list of rgba colors.'
                )

        style['fmt'] = 'none'
        dims = element.dimensions()
        xs, ys = (element.dimension_values(i) for i in range(2))
        err = element.array(dimensions=dims[2:4])

        err_key = 'xerr' if element.horizontal ^ self.invert_axes else 'yerr'
        coords = (ys, xs) if self.invert_axes else (xs, ys)
        style[err_key] = err.T if len(dims) > 3 else err[:, 0]
        return coords, style, {}


    def update_handles(self, key, axis, element, ranges, style):
        bottoms = self.handles['bottoms']
        tops = self.handles['tops']
        verts = self.handles['verts']

        _, style, axis_kwargs = self.get_data(element, ranges, style)
        xs, ys, neg_error = (element.dimension_values(i) for i in range(3))
        pos_idx = 3 if len(element.dimensions()) > 3 else 2
        pos_error = element.dimension_values(pos_idx)
        if element.horizontal:
            bxs, bys = xs - neg_error, ys
            txs, tys = xs + pos_error, ys
        else:
            bxs, bys = xs, ys - neg_error
            txs, tys = xs, ys + pos_error
        if self.invert_axes:
            bxs, bys = bys, bxs
            txs, tys = tys, txs
        new_arrays = np.moveaxis(np.array([[bxs, bys], [txs, tys]]), 2, 0)
        verts.set_paths(new_arrays)
        if bottoms:
            bottoms.set_xdata(bxs)
            bottoms.set_ydata(bys)
        if tops:
            tops.set_xdata(txs)
            tops.set_ydata(tys)
        if 'ecolor' in style:
            verts.set_edgecolors(style['ecolor'])
        if 'elinewidth' in style:
            verts.set_linewidths(style['elinewidth'])

        return axis_kwargs



class AreaPlot(AreaMixin, ChartPlot):

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['color', 'facecolor', 'alpha', 'edgecolor', 'linewidth',
                  'hatch', 'linestyle', 'joinstyle',
                  'fill', 'capstyle', 'interpolate', 'step']

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='fill_between')

    def get_data(self, element, ranges, style):
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        xs = element.dimension_values(0)
        ys = [element.dimension_values(vdim) for vdim in element.vdims]
        return (xs, *ys), style, {}

    def init_artists(self, ax, plot_data, plot_kwargs):
        fill_fn = ax.fill_betweenx if self.invert_axes else ax.fill_between
        stack = fill_fn(*plot_data, **plot_kwargs)
        return {'artist': stack}




class SideAreaPlot(AdjoinedPlot, AreaPlot):

    bgcolor = param.Parameter(default=(1, 1, 1, 0), doc="""
        Make plot background invisible.""")

    border_size = param.Number(default=0, doc="""
        The size of the border expressed as a fraction of the main plot.""")

    xaxis = param.Selector(default='bare',
                                 objects=['top', 'bottom', 'bare', 'top-bare',
                                          'bottom-bare', None], doc="""
        Whether and where to display the xaxis, bare options allow suppressing
        all axis labels including ticks and xlabel. Valid options are 'top',
        'bottom', 'bare', 'top-bare' and 'bottom-bare'.""")

    yaxis = param.Selector(default='bare',
                                 objects=['left', 'right', 'bare', 'left-bare',
                                          'right-bare', None], doc="""
        Whether and where to display the yaxis, bare options allow suppressing
        all axis labels including ticks and ylabel. Valid options are 'left',
        'right', 'bare' 'left-bare' and 'right-bare'.""")


class SpreadPlot(AreaPlot):
    """SpreadPlot plots the Spread Element type.

    """

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    def __init__(self, element, **params):
        super().__init__(element, **params)

    def get_data(self, element, ranges, style):
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        xs = element.dimension_values(0)
        mean = element.dimension_values(1)
        neg_error = element.dimension_values(2)
        pos_idx = 3 if len(element.dimensions()) > 3 else 2
        pos_error = element.dimension_values(pos_idx)
        return (xs, mean-neg_error, mean+pos_error), style, {}

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        return ChartPlot.get_extents(self, element, ranges, range_type)


class HistogramPlot(ColorbarPlot):
    """HistogramPlot can plot DataHistograms and ViewMaps of
    DataHistograms, which can be displayed as a single frame or
    animation.

    """

    style_opts = ['alpha', 'color', 'align', 'visible', 'facecolor',
                  'edgecolor', 'log', 'capsize', 'error_kw', 'hatch',
                  'linewidth']

    _nonvectorized_styles = ['alpha', 'log', 'error_kw', 'hatch', 'visible', 'align']

    def __init__(self, histograms, **params):
        self.center = False
        self.cyclic = False

        super().__init__(histograms, **params)

        if self.invert_axes:
            self.axis_settings = ['ylabel', 'xlabel', 'yticks']
        else:
            self.axis_settings = ['xlabel', 'ylabel', 'xticks']
        val_dim = self.hmap.last.get_dimension(1)
        self.cyclic_range = val_dim.range if val_dim.cyclic else None


    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        hist = self.hmap.last
        key = self.keys[-1]

        ranges = self.compute_ranges(self.hmap, key, ranges)
        el_ranges = match_spec(hist, ranges)

        # Get plot ranges and values
        dims = hist.dimensions()[:2]
        edges, hvals, widths, lims, is_datetime = self._process_hist(hist)
        if is_datetime and not dims[0].value_format:
            dt_format = Dimension.type_formatters[np.datetime64]
            dims[0] = dims[0].clone(value_format=DateFormatter(dt_format))

        style = self.style[self.cyclic_index]
        if self.invert_axes:
            self.offset_linefn = self.handles['axis'].axvline
            self.plotfn = self.handles['axis'].barh
        else:
            self.offset_linefn = self.handles['axis'].axhline
            self.plotfn = self.handles['axis'].bar

        with abbreviated_exception():
            style = self._apply_transforms(hist, ranges, style)
            if 'vmin' in style:
                raise ValueError('Mapping a continuous dimension to a '
                                 'color on a HistogramPlot is not '
                                 f'supported by the {self.renderer.backend} backend. '
                                 'To map a dimension to a color supply '
                                 'an explicit list of rgba colors.'
                )

        # Plot bars and make any adjustments
        legend = hist.label if self.show_legend else ''
        bars = self.plotfn(edges, hvals, widths, zorder=self.zorder, label=legend, align='edge', **style)
        self.handles['artist'] = self._update_plot(self.keys[-1], hist, bars, lims, ranges) # Indexing top

        ticks = self._compute_ticks(hist, edges, widths, lims)
        ax_settings = self._process_axsettings(hist, lims, ticks)
        ax_settings['dimensions'] = dims

        return self._finalize_axis(self.keys[-1], ranges=el_ranges, element=hist, **ax_settings)


    def _process_hist(self, hist):
        """Get data from histogram, including bin_ranges and values.

        """
        self.cyclic = hist.get_dimension(0).cyclic
        x = hist.kdims[0]
        edges = hist.interface.coords(hist, x, edges=True)
        values = hist.dimension_values(1)
        hist_vals = np.array(values)
        xlim = hist.range(0)
        ylim = hist.range(1)
        is_datetime = isdatetime(edges)
        if hasattr(edges, "compute"):
            edges = edges.compute()
        if is_datetime:
            edges = np.array([dt64_to_dt(e) if isinstance(e, np.datetime64) else e for e in edges])
            edges = date2num(edges)
            xlim = tuple(dt_to_int(v, 'D') for v in xlim)
        widths = np.diff(edges)
        return edges[:-1], hist_vals, widths, xlim+ylim, is_datetime

    def _compute_ticks(self, element, edges, widths, lims):
        """Compute the ticks either as cyclic values in degrees or as roughly
        evenly spaced bin centers.

        """
        if self.xticks is None or not isinstance(self.xticks, int):
            return None
        if self.cyclic:
            x0, x1, _, _ = lims
            xvals = np.linspace(x0, x1, self.xticks)
            labels = [f"{np.rad2deg(x):.0f}\N{DEGREE SIGN}" for x in xvals]
        elif self.xticks:
            dim = element.get_dimension(0)
            inds = np.linspace(0, len(edges), self.xticks, dtype=int)
            edges = [*edges, edges[-1] + widths[-1]]
            xvals = [edges[i] for i in inds]
            labels = [dim.pprint_value(v) for v in xvals]
        return [xvals, labels]

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        ydim = element.get_dimension(1)
        s0, s1 = ranges[ydim.label]['soft']
        s0 = min(s0, 0) if isfinite(s0) else 0
        s1 = max(s1, 0) if isfinite(s1) else 0
        ranges[ydim.label]['soft'] = (s0, s1)
        return super().get_extents(element, ranges, range_type)

    def _process_axsettings(self, hist, lims, ticks):
        """Get axis settings options including ticks, x- and y-labels
        and limits.

        """
        axis_settings = dict(zip(self.axis_settings, [None, None, (None if self.overlaid else ticks)], strict=None))
        return axis_settings

    def _update_plot(self, key, hist, bars, lims, ranges):
        """Process bars can be subclassed to manually adjust bars
        after being plotted.

        """
        return bars

    def _update_artists(self, key, hist, edges, hvals, widths, lims, ranges):
        """Update all the artists in the histogram. Subclassable to
        allow updating of further artists.

        """
        plot_vals = zip(self.handles['artist'], edges, hvals, widths, strict=None)
        for bar, edge, height, width in plot_vals:
            if self.invert_axes:
                bar.set_y(edge)
                bar.set_width(height)
                bar.set_height(width)
            else:
                bar.set_x(edge)
                bar.set_height(height)
                bar.set_width(width)


    def update_handles(self, key, axis, element, ranges, style):
        # Process values, axes and style
        edges, hvals, widths, lims, _ = self._process_hist(element)

        ticks = self._compute_ticks(element, edges, widths, lims)
        ax_settings = self._process_axsettings(element, lims, ticks)
        self._update_artists(key, element, edges, hvals, widths, lims, ranges)
        return ax_settings


class SideHistogramPlot(AdjoinedPlot, HistogramPlot):

    bgcolor = param.Parameter(default=(1, 1, 1, 0), doc="""
        Make plot background invisible.""")

    offset = param.Number(default=0.2, bounds=(0,1), doc="""
        Histogram value offset for a colorbar.""")

    show_grid = param.Boolean(default=False, doc="""
        Whether to overlay a grid on the axis.""")

    def _process_hist(self, hist):
        """Subclassed to offset histogram by defined amount.

        """
        edges, hvals, widths, lims, isdatetime = super()._process_hist(hist)
        offset = self.offset * lims[3]
        hvals = hvals * (1-self.offset)
        hvals += offset
        lims = (*lims[0:3], lims[3] + offset)
        return edges, hvals, widths, lims, isdatetime

    def _update_artists(self, n, element, edges, hvals, widths, lims, ranges):
        super()._update_artists(n, element, edges, hvals, widths, lims, ranges)
        self._update_plot(n, element, self.handles['artist'], lims, ranges)

    def _update_plot(self, key, element, bars, lims, ranges):
        """Process the bars and draw the offset line as necessary. If a
        color map is set in the style of the 'main' ViewableElement object, color
        the bars appropriately, respecting the required normalization
        settings.

        """
        main = self.adjoined.main
        _, y1 = element.range(1)
        offset = self.offset * y1
        range_item, main_range, dim = get_sideplot_ranges(self, element, main, ranges)

        # Check if plot is colormapped
        plot_type = Store.registry['matplotlib'].get(type(range_item))
        if isinstance(plot_type, PlotSelector):
            plot_type = plot_type.get_plot_class(range_item)
        opts = self.lookup_options(range_item, 'plot')
        if plot_type and issubclass(plot_type, ColorbarPlot):
            cidx = opts.options.get('color_index', None)
            if cidx is None:
                opts = self.lookup_options(range_item, 'style')
                cidx = opts.kwargs.get('color', None)
                if cidx not in range_item:
                    cidx = None
            cdim = None if cidx is None else range_item.get_dimension(cidx)
        else:
            cdim = None

        # Get colormapping options
        if isinstance(range_item, (HeatMap, Raster)) or (cdim and cdim in element):
            style = self.lookup_options(range_item, 'style')[self.cyclic_index]
            if MPL_GE_3_7_0:
                # https://github.com/matplotlib/matplotlib/pull/28355
                cmap = mpl.colormaps.get_cmap(style.get('cmap'))
            else:
                from matplotlib import cm
                cmap = cm.get_cmap(style.get('cmap'))
            main_range = style.get('clims', main_range)
        else:
            cmap = None

        if offset and ('offset_line' not in self.handles):
            self.handles['offset_line'] = self.offset_linefn(offset,
                                                             linewidth=1.0,
                                                             color='k')
        elif offset:
            self._update_separator(offset)

        if cmap is not None and main_range and (None not in main_range):
            self._colorize_bars(cmap, bars, element, main_range, dim)
        return bars

    def _colorize_bars(self, cmap, bars, element, main_range, dim):
        """Use the given cmap to color the bars, applying the correct
        color ranges as necessary.

        """
        cmap_range = main_range[1] - main_range[0]
        lower_bound = main_range[0]
        colors = np.array(element.dimension_values(dim))
        colors = (colors - lower_bound) / (cmap_range)
        for c, bar in zip(colors, bars, strict=None):
            bar.set_facecolor(cmap(c))
            bar.set_clip_on(False)

    def _update_separator(self, offset):
        """Compute colorbar offset and update separator line
        if map is non-zero.

        """
        offset_line = self.handles['offset_line']
        if offset == 0:
            offset_line.set_visible(False)
        else:
            offset_line.set_visible(True)
            if self.invert_axes:
                offset_line.set_xdata(offset)
            else:
                offset_line.set_ydata(offset)


class PointPlot(ChartPlot, ColorbarPlot, LegendPlot):
    """Note that the 'cmap', 'vmin' and 'vmax' style arguments control
    how point magnitudes are rendered to different colors.

    """

    show_grid = param.Boolean(default=False, doc="""
      Whether to draw grid lines at the tick positions.""")

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

    style_opts = ['alpha', 'color', 'edgecolors', 'facecolors',
                  'linewidth', 'marker', 'size', 'visible',
                  'cmap', 'vmin', 'vmax', 'norm']

    _nonvectorized_styles = ['alpha', 'marker', 'cmap', 'vmin', 'vmax',
                      'norm', 'visible']

    _disabled_opts = ['size']
    _plot_methods = dict(single='scatter')

    def get_data(self, element, ranges, style):
        xs, ys = (element.dimension_values(i) for i in range(2))
        self._compute_styles(element, ranges, style)
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        return (ys, xs) if self.invert_axes else (xs, ys), style, {}


    def _compute_styles(self, element, ranges, style):
        cdim = element.get_dimension(self.color_index)
        color = style.pop('color', None)
        cmap = style.get('cmap', None)

        if cdim and ((isinstance(color, str) and color in element) or isinstance(color, dim)):
            self.param.warning(
                "Cannot declare style mapping for 'color' option and "
                "declare a color_index; ignoring the color_index.")
            cdim = None
        if cdim and cmap:
            cs = element.dimension_values(self.color_index)
            # Check if numeric otherwise treat as categorical
            if dtype_kind(cs) in 'uif':
                style['c'] = cs
            else:
                style['c'] = search_indices(cs, unique_array(cs))
            self._norm_kwargs(element, ranges, style, cdim)
        elif color is not None:
            style['color'] = color
        style['edgecolors'] = style.pop('edgecolors', style.pop('edgecolor', 'none'))

        ms = style.get('s', mpl.rcParams['lines.markersize'])
        sdim = element.get_dimension(self.size_index)
        if sdim and ((isinstance(ms, str) and ms in element) or isinstance(ms, dim)):
            self.param.warning(
                "Cannot declare style mapping for 's' option and "
                "declare a size_index; ignoring the size_index.")
            sdim = None
        if sdim:
            sizes = element.dimension_values(self.size_index)
            sizes = compute_sizes(sizes, self.size_fn, self.scaling_factor,
                                  self.scaling_method, ms)
            if sizes is None:
                eltype = type(element).__name__
                self.param.warning(
                    f'{sdim.pprint_label} dimension is not numeric, cannot use to '
                    f'scale {eltype} size.')
            else:
                style['s'] = sizes
        style['edgecolors'] = style.pop('edgecolors', 'none')


    def update_handles(self, key, axis, element, ranges, style):
        paths = self.handles['artist']
        (xs, ys), style, _ = self.get_data(element, ranges, style)
        xdim, ydim = element.dimensions()[:2]
        if 'factors' in ranges.get(xdim.label, {}):
            factors = list(ranges[xdim.label]['factors'])
            xs = [factors.index(x) for x in xs if x in factors]
        if 'factors' in ranges.get(ydim.label, {}):
            factors = list(ranges[ydim.label]['factors'])
            ys = [factors.index(y) for y in ys if y in factors]
        paths.set_offsets(np.column_stack([xs, ys]))
        if 's' in style:
            sizes = style['s']
            if isscalar(sizes):
                sizes = [sizes]
            paths.set_sizes(sizes)
        if 'vmin' in style:
            paths.set_clim((style['vmin'], style['vmax']))
        if 'c' in style:
            paths.set_array(style['c'])
        if 'norm' in style:
            paths.norm = style['norm']
        if 'linewidth' in style:
            paths.set_linewidths(style['linewidth'])
        if 'edgecolors' in style:
            paths.set_edgecolors(style['edgecolors'])
        if 'facecolors' in style:
            paths.set_edgecolors(style['facecolors'])



class VectorFieldPlot(ColorbarPlot):
    """Renders vector fields in sheet coordinates. The vectors are
    expressed in polar coordinates and may be displayed according to
    angle alone (with some common, arbitrary arrow length) or may be
    true polar vectors.

    The color or magnitude can be mapped onto any dimension using the
    color_index and size_index.

    The length of the arrows is controlled by the 'scale' style
    option. The scaling of the arrows may also be controlled via the
    normalize_lengths and rescale_lengths plot option, which will
    normalize the lengths to a maximum of 1 and scale them according
    to the minimum distance respectively.

    """

    arrow_heads = param.Boolean(default=True, doc="""
       Whether or not to draw arrow heads. If arrowheads are enabled,
       they may be customized with the 'headlength' and
       'headaxislength' style options.""")

    magnitude = param.ClassSelector(class_=(str, dim), doc="""
        Dimension or dimension value transform that declares the magnitude
        of each vector. Magnitude is expected to be scaled between 0-1,
        by default the magnitudes are rescaled relative to the minimum
        distance between vectors, this can be disabled with the
        rescale_lengths option.""")

    padding = param.ClassSelector(default=0.05, class_=(int, float, tuple))

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

    style_opts = ['alpha', 'color', 'edgecolors', 'facecolors',
                  'linewidth', 'marker', 'visible', 'cmap',
                  'scale', 'headlength', 'headaxislength', 'pivot',
                  'width', 'headwidth', 'norm']

    _nonvectorized_styles = ['alpha', 'marker', 'cmap', 'visible', 'norm',
                             'pivot', 'headlength', 'headaxislength',
                             'headwidth']

    _plot_methods = dict(single='quiver')

    def _get_magnitudes(self, element, style, ranges):
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
        if mag_dim is not None:
            if isinstance(mag_dim, dim):
                magnitudes = mag_dim.apply(element, flat=True)
            else:
                magnitudes = element.dimension_values(mag_dim)
                _, max_magnitude = ranges[dim_range_key(mag_dim)]['combined']
                if self.normalize_lengths and max_magnitude != 0:
                    magnitudes = magnitudes / max_magnitude
        else:
            magnitudes = np.ones(len(element))
        return magnitudes

    def get_data(self, element, ranges, style):
        # Compute coordinates
        xidx, yidx = (1, 0) if self.invert_axes else (0, 1)
        xs = element.dimension_values(xidx) if len(element.data) else []
        ys = element.dimension_values(yidx) if len(element.data) else []

        # Compute vector angle and magnitude
        radians = element.dimension_values(2) if len(element.data) else []
        if self.invert_axes: radians = radians+1.5*np.pi
        angles = list(np.rad2deg(radians))
        magnitudes = self._get_magnitudes(element, style, ranges)
        input_scale = style.pop('scale', 1.0)
        if self.rescale_lengths:
            min_dist = get_min_distance(element)
            input_scale = input_scale / min_dist

        args = (xs, ys, magnitudes,  [0.0] * len(element))

        # Compute color
        cdim = element.get_dimension(self.color_index)
        color = style.get('color', None)
        if cdim and ((isinstance(color, str) and color in element) or isinstance(color, dim)):
            self.param.warning(
                "Cannot declare style mapping for 'color' option and "
                "declare a color_index; ignoring the color_index.")
            cdim = None
        if cdim:
            colors = element.dimension_values(self.color_index)
            style['c'] = colors
            cdim = element.get_dimension(self.color_index)
            self._norm_kwargs(element, ranges, style, cdim)
            style.pop('color', None)

        # Process style
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        style.update(dict(scale=input_scale, angles=angles, units='x', scale_units='x'))
        if 'vmin' in style:
            style['clim'] = (style.pop('vmin'), style.pop('vmax'))
        if 'c' in style:
            style['array'] = style.pop('c')
        if 'pivot' not in style:
            style['pivot'] = 'mid'
        if not self.arrow_heads:
            style['headaxislength'] = 0

        return args, style, {}

    def update_handles(self, key, axis, element, ranges, style):
        args, style, axis_kwargs = self.get_data(element, ranges, style)

        # Set magnitudes, angles and colors if supplied.
        quiver = self.handles['artist']
        quiver.set_offsets(np.column_stack(args[:2]))
        quiver.U = args[2]
        quiver.angles = style['angles']
        if 'color' in style:
            quiver.set_facecolors(style['color'])
            quiver.set_edgecolors(style['color'])
        if 'array' in style:
            quiver.set_array(style['array'])
        if 'clim' in style:
            quiver.set_clim(style['clim'])
        if 'linewidth' in style:
            quiver.set_linewidths(style['linewidth'])
        return axis_kwargs



class BarPlot(BarsMixin, ColorbarPlot, LegendPlot):

    bar_padding = param.Number(default=0.2, doc="""
       Defines the padding between groups.""")

    multi_level = param.Boolean(default=True, doc="""
       Whether the Bars should be grouped into a second categorical axis level.""")

    stacked = param.Boolean(default=False, doc="""
       Whether the bars should be stacked or grouped.""")

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['alpha', 'color', 'align', 'visible', 'edgecolor',
                  'log', 'facecolor', 'capsize', 'error_kw', 'hatch']

    _nonvectorized_styles = ['visible']

    legend_specs = dict(
        LegendPlot.legend_specs,
        top=dict(
            bbox_to_anchor=(0., 1.02, 1., .102),
            ncol=3, loc=3, mode="expand", borderaxespad=0.
        ),
        bottom=dict(
            bbox_to_anchor=(0., -0.4, 1., .102),
            ncol=3, loc=2, mode="expand", borderaxespad=0.1
        )
    )

    def _get_values(self, element, ranges):
        """Get unique index value for each bar

        """
        gvals, cvals = self._get_coords(element, ranges, as_string=False)
        kdims = element.kdims
        if element.ndims == 1:
            dimensions = [*kdims, None, None]
            values = {'group': gvals, 'stack': [None]}
        elif self.stacked:
            stack_dim = kdims[1]
            dimensions = [kdims[0], None, stack_dim]
            if stack_dim.values:
                stack_order = stack_dim.values
            elif stack_dim in ranges and ranges[stack_dim.label].get('factors'):
                stack_order = ranges[stack_dim.label]['factors']
            else:
                stack_order = element.dimension_values(1, False)
            stack_order = list(stack_order)
            values = {'group': gvals, 'stack': stack_order}
        else:
            dimensions = [*kdims, None]
            values = {'group': gvals, 'category': cvals}
        return dimensions, values


    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        element = self.hmap.last
        vdim = element.vdims[0]
        axis = self.handles['axis']
        key = self.keys[-1]

        style = dict(zorder=self.zorder, **self.style[self.cyclic_index])
        ranges = self.compute_ranges(self.hmap, key, ranges)
        ranges = match_spec(element, ranges)

        self.handles['artist'], xticks, xdims = self._create_bars(axis, element, ranges, style)
        kwargs = {'yticks': xticks} if self.invert_axes else {'xticks': xticks}
        return self._finalize_axis(key, ranges=ranges, element=element,
                                   dimensions=[xdims, vdim], **kwargs)

    def _finalize_ticks(self, axis, element, xticks, yticks, zticks):
        """Apply ticks with appropriate offsets.

        """
        alignments = None
        ticks = xticks or yticks
        if ticks is not None:
            ticks, labels, alignments = zip(*sorted(ticks, key=lambda x: x[0]), strict=None)
            ticks = (list(ticks), list(labels))
        if xticks:
            xticks = ticks
        elif yticks:
            yticks = ticks
        super()._finalize_ticks(axis, element, xticks, yticks, zticks)
        if alignments:
            if xticks:
                for t, y in zip(axis.get_xticklabels(), alignments, strict=None):
                    t.set_y(y)
            elif yticks:
                for t, x in zip(axis.get_yticklabels(), alignments, strict=None):
                    t.set_x(x)

    def _create_bars(self, axis, element, ranges, style):
        # Get values dimensions, and style information
        (gdim, cdim, sdim), values = self._get_values(element, ranges)

        cats = None
        style_dim = None
        xslice = slice(None)
        if sdim:
            cats = values['stack']
            style_dim = sdim
            stack_data = element.dimension_values(sdim)
            xslice = stack_data == stack_data[0]
        elif cdim:
            cats = values['category']
            style_dim = cdim

        if style_dim:
            style_map = {style_dim.pprint_value(v): self.style[i]
                         for i, v in enumerate(cats)}
        else:
            style_map = {None: {}}

        # Compute widths
        xvals = element.dimension_values(0)
        is_dt = isdatetime(xvals)
        continuous = True
        if is_dt or dtype_kind(xvals) not in 'OU' and not (cdim or len(element.kdims) > 1):
            xvals = xvals[xslice]
            xdiff = np.abs(np.diff(xvals))
            diff_size = len(np.unique(xdiff))
            if diff_size == 0 or (diff_size == 1 and xdiff[0] == 0):
                xdiff = 1
            else:
                xdiff = np.min(xdiff)
            width = (1 - self.bar_padding) * (date2num(xdiff.astype("timedelta64[us]")) if is_dt else xdiff)
            data_width = (1 - self.bar_padding) * xdiff
        else:
            xdiff = len(values.get('category', [None]))
            width = (1 - self.bar_padding) / xdiff
            continuous = False

        if self.invert_axes:
            plot_fn = 'barh'
            x, y, w, bottom = 'y', 'width', 'height', 'left'
        else:
            plot_fn = 'bar'
            x, y, w, bottom = 'x', 'height', 'width', 'bottom'

        # Iterate over group, category and stack dimension values
        # computing xticks and drawing bars and applying styles
        xticks, labels, bar_data = [], [], {}
        categories = values.get('category', [None])
        num_categories = len(categories)
        for gidx, grp in enumerate(values.get('group', [None])):
            sel_key = {}
            label = None
            if grp is not None:
                grp_label = gdim.pprint_value(grp)
                sel_key[gdim.name] = [grp]
                yalign = -0.04 if cdim and self.multi_level else 0
                goffset = width * (num_categories / 2 - 0.5)
                if num_categories > 1:
                    # mini offset needed or else combines with non-continuous
                    goffset += width / 1000

                xpos = gidx+goffset if not continuous else xvals[gidx]
                if not continuous:
                    xticks.append(((xpos), grp_label, yalign))
            for cidx, cat in enumerate(categories):
                xpos = gidx+(cidx*width) if not continuous else xvals[gidx]
                if cat is not None:
                    label = cdim.pprint_value(cat)
                    sel_key[cdim.name] = [cat]
                    if self.multi_level and not continuous:
                        xticks.append((xpos, label, 0))
                prev = 0
                for stk in values.get('stack', [None]):
                    if stk is not None:
                        label = sdim.pprint_value(stk)
                        sel_key[sdim.name] = [stk]
                    el = element.select(**sel_key)
                    vals = el.dimension_values(element.vdims[0].name)
                    val = float(vals[0]) if len(vals) else np.nan
                    xval = xpos

                    if label in bar_data:
                        group = bar_data[label]
                        group[x].append(xval)
                        group[y].append(val)
                        group[bottom].append(prev)
                    else:
                        bar_style = dict(style, **style_map.get(label, {}))
                        with abbreviated_exception():
                            bar_style = self._apply_transforms(el, ranges, bar_style)
                        bar_data[label] = {
                            x:[xval], y: [val], w: width, bottom: [prev],
                            'label': label,
                        }
                        bar_data[label].update(bar_style)
                    prev += val if isfinite(val) else 0
                    if label is not None:
                        labels.append(label)

        # Draw bars
        bars = [getattr(axis, plot_fn)(**bar_spec) for bar_spec in bar_data.values()]

        # Generate legend and axis labels
        ax_dims = [gdim]
        title = ''
        if sdim:
            title = sdim.pprint_label
            ax_dims.append(sdim)
        elif cdim:
            title = cdim.pprint_label
            if self.multi_level:
                ax_dims.append(cdim)
        if self.show_legend and any(len(l) for l in labels) and (sdim or not self.multi_level):
            leg_spec = self.legend_specs[self.legend_position]
            if self.legend_cols: leg_spec['ncol'] = self.legend_cols
            legend_opts = self.legend_opts.copy()
            legend_opts.update(**leg_spec)
            axis.legend(title=title, **legend_opts)

        x_range = ranges[gdim.label]["data"]
        if style.get('align', 'center') == 'center':
            left_multiplier = 0.5
            right_multiplier = 0.5
        else:
            left_multiplier = 0
            right_multiplier = 1
        if continuous:
            ranges[gdim.label]["data"] = (
                x_range[0] - data_width * left_multiplier,
                x_range[1] + data_width * right_multiplier,
            )
        else:
            locs = [item[0] for item in xticks]
            xmin, xmax = min(locs), max(locs)
            ranges[gdim.label]["data"] = (
                - width * left_multiplier + xmin,
                width * right_multiplier + xmax
            )
        return bars, xticks if not continuous else None, ax_dims


class SpikesPlot(SpikesMixin, PathPlot, ColorbarPlot):

    aspect = param.Parameter(default='square', doc="""
        The aspect ratio mode of the plot. Allows setting an
        explicit aspect ratio as width/height as well as
        'square' and 'equal' options.""")

    color_index = param.ClassSelector(default=None, allow_None=True,
                                      class_=(str, int), doc="""
      Index of the dimension from which the color will the drawn""")

    spike_length = param.Number(default=0.1, doc="""
      The length of each spike if Spikes object is one dimensional.""")

    padding = param.ClassSelector(default=(0, 0.1), class_=(int, float, tuple))

    position = param.Number(default=0., doc="""
      The position of the lower end of each spike.""")

    style_opts = [*PathPlot.style_opts, 'cmap']

    def init_artists(self, ax, plot_args, plot_kwargs):
        if 'c' in plot_kwargs:
            plot_kwargs['array'] = plot_kwargs.pop('c')
        if 'vmin' in plot_kwargs and 'vmax' in plot_kwargs:
            plot_kwargs['clim'] = plot_kwargs.pop('vmin'), plot_kwargs.pop('vmax')
        if "array" not in plot_kwargs and 'cmap' in plot_kwargs:
            del plot_kwargs['cmap']
        line_segments = LineCollection(*plot_args, **plot_kwargs)
        ax.add_collection(line_segments)
        return {'artist': line_segments}

    def get_data(self, element, ranges, style):
        dimensions = element.dimensions(label=True)
        ndims = len(dimensions)
        opts = self.lookup_options(element, 'plot').options

        pos = self.position
        if ndims > 1 and 'spike_length' not in opts:
            data = element.columns([0, 1])
            xs, ys = data[dimensions[0]], data[dimensions[1]]
            data = [[(x, pos), (x, pos+y)] for x, y in zip(xs, ys, strict=None)]
        else:
            xs = element.array([0])
            height = self.spike_length
            data = [[(x[0], pos), (x[0], pos+height)] for x in xs]

        if self.invert_axes:
            data = [(line[0][::-1], line[1][::-1]) for line in data]

        dims = element.dimensions()
        clean_spikes = []
        for spike in data:
            xs, ys = zip(*spike, strict=None)
            cols = []
            for i, vs in enumerate((xs, ys)):
                vs = np.array(vs)
                if isdatetime(vs):
                    dt_format = Dimension.type_formatters.get(
                        type(vs[0]),
                        Dimension.type_formatters[np.datetime64]
                    )
                    vs = date2num(vs)
                    dims[i] = dims[i].clone(value_format=DateFormatter(dt_format))
                cols.append(vs)
            clean_spikes.append(np.column_stack(cols))

        cdim = element.get_dimension(self.color_index)
        color = style.get('color', None)
        if cdim and ((isinstance(color, str) and color in element) or isinstance(color, dim)):
            self.param.warning(
                "Cannot declare style mapping for 'color' option and "
                "declare a color_index; ignoring the color_index.")
            cdim = None
        if cdim:
            style['array'] = element.dimension_values(cdim)
            self._norm_kwargs(element, ranges, style, cdim)

        if 'spike_length' in opts:
            axis_dims =  (element.dimensions()[0], None)
        elif len(element.dimensions()) == 1:
            axis_dims =  (element.dimensions()[0], None)
        else:
            axis_dims =  (element.dimensions()[0], element.dimensions()[1])
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        return (clean_spikes,), style, {'dimensions': axis_dims}


    def update_handles(self, key, axis, element, ranges, style):
        artist = self.handles['artist']
        (data,), kwargs, axis_kwargs = self.get_data(element, ranges, style)
        artist.set_paths(data)
        artist.set_visible(style.get('visible', True))
        if 'color' in kwargs:
            artist.set_edgecolors(kwargs['color'])
        if 'array' in kwargs or 'c' in kwargs:
            artist.set_array(kwargs.get('array', kwargs.get('c')))
        if 'vmin' in kwargs:
            artist.set_clim((kwargs['vmin'], kwargs['vmax']))
        if 'norm' in kwargs:
            artist.norm = kwargs['norm']
        if 'linewidth' in kwargs:
            artist.set_linewidths(kwargs['linewidth'])
        return axis_kwargs


class SideSpikesPlot(AdjoinedPlot, SpikesPlot):

    bgcolor = param.Parameter(default=(1, 1, 1, 0), doc="""
        Make plot background invisible.""")

    border_size = param.Number(default=0, doc="""
        The size of the border expressed as a fraction of the main plot.""")

    subplot_size = param.Number(default=0.1, doc="""
        The size subplots as expressed as a fraction of the main plot.""")

    spike_length = param.Number(default=1, doc="""
      The length of each spike if Spikes object is one dimensional.""")

    xaxis = param.Selector(default='bare',
                                 objects=['top', 'bottom', 'bare', 'top-bare',
                                          'bottom-bare', None], doc="""
        Whether and where to display the xaxis, bare options allow suppressing
        all axis labels including ticks and xlabel. Valid options are 'top',
        'bottom', 'bare', 'top-bare' and 'bottom-bare'.""")

    yaxis = param.Selector(default='bare',
                                      objects=['left', 'right', 'bare', 'left-bare',
                                               'right-bare', None], doc="""
        Whether and where to display the yaxis, bare options allow suppressing
        all axis labels including ticks and ylabel. Valid options are 'left',
        'right', 'bare' 'left-bare' and 'right-bare'.""")
