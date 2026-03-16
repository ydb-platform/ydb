import sys

import numpy as np
import param

from ...core import CompositeOverlay, Element, traversal
from ...core.util import isfinite, match_spec, max_range, unique_iterator
from ...element.raster import RGB, Image, Raster
from ..util import categorical_legend
from .chart import PointPlot
from .element import ColorbarPlot, ElementPlot, LegendPlot, OverlayPlot
from .plot import GridPlot, MPLPlot, mpl_rc_context
from .util import MPL_VERSION, get_raster_array


class RasterBasePlot(ElementPlot):

    aspect = param.Parameter(default='equal', doc="""
        Raster elements respect the aspect ratio of the
        Images by default but may be set to an explicit
        aspect ratio or to 'square'.""")

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    situate_axes = param.Boolean(default=True, doc="""
        Whether to situate the image relative to other plots. """)

    _plot_methods = dict(single='imshow')

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        extents = super().get_extents(element, ranges, range_type)
        if self.situate_axes or range_type not in ('combined', 'data'):
            return extents
        elif isinstance(element, Image):
            return element.bounds.lbrt()
        else:
            return element.extents

    def _compute_ticks(self, element, ranges):
        return None, None


class RasterPlot(RasterBasePlot, ColorbarPlot):

    clipping_colors = param.Dict(default={'NaN': 'transparent'})

    style_opts = ['alpha', 'cmap', 'interpolation', 'visible',
                  'filterrad', 'clims', 'norm']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.hmap.type == Raster:
            self.invert_yaxis = not self.invert_yaxis

    def get_data(self, element, ranges, style):
        xticks, yticks = self._compute_ticks(element, ranges)

        if isinstance(element, RGB):
            style.pop('cmap', None)

        data = get_raster_array(element)
        if type(element) is Raster:
            l, b, r, t = element.extents
            if self.invert_axes:
                data = data[:, ::-1]
            else:
                data = data[::-1]
        else:
            l, b, r, t = element.bounds.lbrt()
            if self.invert_axes:
                data = data[::-1, ::-1]

        if self.invert_axes:
            data = data.transpose([1, 0, 2]) if isinstance(element, RGB) else data.T
            l, b, r, t = b, l, t, r

        vdim = element.vdims[0]
        self._norm_kwargs(element, ranges, style, vdim)
        style['extent'] = [l, r, b, t]
        style['origin'] = 'upper'

        return [data], style, {'xticks': xticks, 'yticks': yticks}

    def update_handles(self, key, axis, element, ranges, style):
        im = self.handles['artist']
        data, style, axis_kwargs = self.get_data(element, ranges, style)
        l, r, b, t = style['extent']
        im.set_data(data[0])
        im.set_extent((l, r, b, t))
        im.set_clim((style['vmin'], style['vmax']))
        if 'norm' in style:
            im.norm = style['norm']

        return axis_kwargs


class RGBPlot(RasterBasePlot, LegendPlot):

    style_opts = ['alpha', 'interpolation', 'visible', 'filterrad']

    def get_data(self, element, ranges, style):
        xticks, yticks = self._compute_ticks(element, ranges)
        data = get_raster_array(element)
        l, b, r, t = element.bounds.lbrt()
        if self.invert_axes:
            data = data[::-1, ::-1]
            data = data.transpose([1, 0, 2])
            l, b, r, t = b, l, t, r
        if all(isfinite(e) for e in (l, b, r, t)):
            style['extent'] = [l, r, b, t]
        style['origin'] = 'upper'
        if data.shape[:2] == (0, 0):
            data = np.zeros((1, 1, 4), dtype='uint8')
        return [data], style, {'xticks': xticks, 'yticks': yticks}

    def init_artists(self, ax, plot_args, plot_kwargs):
        handles = super().init_artists(ax, plot_args, plot_kwargs)
        if 'holoviews.operation.datashader' not in sys.modules or not self.show_legend:
            return handles
        try:
            legend = categorical_legend(self.current_frame, backend=self.backend)
        except Exception:
            return handles
        if legend is None:
            return handles
        legend_params = {k: v for k, v in self.param.values().items() if k.startswith('legend')}
        self._legend_plot = PointPlot(legend, axis=ax, fig=self.state,
                                      keys=self.keys, dimensions=self.dimensions,
                                      overlaid=1, **legend_params)
        self._legend_plot.initialize_plot()
        return handles

    def update_handles(self, key, axis, element, ranges, style):
        im = self.handles['artist']
        data, style, axis_kwargs = self.get_data(element, ranges, style)
        l, r, b, t = style['extent']
        im.set_data(data[0])
        im.set_extent((l, r, b, t))
        return axis_kwargs


class QuadMeshPlot(ColorbarPlot):

    clipping_colors = param.Dict(default={'NaN': 'transparent'})

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['alpha', 'cmap', 'clims', 'edgecolors', 'norm', 'shading',
                  'linestyles', 'linewidths', 'hatch', 'visible']

    _plot_methods = dict(single='pcolormesh')

    def get_data(self, element, ranges, style):
        zdata = element.dimension_values(2, flat=False)
        data = np.ma.array(zdata, mask=np.logical_not(np.isfinite(zdata)))

        expanded = element.interface.irregular(element, element.kdims[0])
        edges = style.get('shading') != 'gouraud'
        coords = [element.interface.coords(element, d, ordered=True,
                                           expanded=expanded, edges=edges)
                  for d in element.kdims]
        if self.invert_axes:
            coords = coords[::-1]
            data = data.T
        cmesh_data = [*coords, data]
        if expanded:
            style['locs'] = np.concatenate(coords)
        vdim = element.vdims[0]
        self._norm_kwargs(element, ranges, style, vdim)
        return tuple(cmesh_data), style, {}

    def init_artists(self, ax, plot_args, plot_kwargs):
        locs = plot_kwargs.pop('locs', None)
        artist = ax.pcolormesh(*plot_args, **plot_kwargs)
        colorbar = self.handles.get('cbar')
        if 'norm' in plot_kwargs: # vmin/vmax should now be exclusively in norm
            plot_kwargs.pop('vmin', None)
            plot_kwargs.pop('vmax', None)
        if colorbar and MPL_VERSION < (3, 1, 0):
            colorbar.set_norm(artist.norm)
            if hasattr(colorbar, 'set_array'):
                # Compatibility with mpl < 3
                colorbar.set_array(artist.get_array())
            colorbar.set_clim(artist.get_clim())
            colorbar.update_normal(artist)
        elif colorbar:
            colorbar.update_normal(artist)

        return {'artist': artist, 'locs': locs}


class RasterGridPlot(GridPlot, OverlayPlot):
    """RasterGridPlot evenly spaces out plots of individual projections on
    a grid, even when they differ in size. Since this class uses a single
    axis to generate all the individual plots it is much faster than the
    equivalent using subplots.

    """

    padding = param.Number(default=0.1, doc="""
        The amount of padding as a fraction of the total Grid size""")

    # Parameters inherited from OverlayPlot that are not part of the
    # GridPlot interface. Some of these may be enabled in future in
    # conjunction with GridPlot.

    apply_extents = param.Parameter(precedence=-1)
    apply_ranges = param.Parameter(precedence=-1)
    apply_ticks = param.Parameter(precedence=-1)
    batched = param.Parameter(precedence=-1)
    bgcolor = param.Parameter(precedence=-1)
    data_aspect = param.Parameter(precedence=-1)
    default_span = param.Parameter(precedence=-1)
    hooks = param.Parameter(precedence=-1)
    invert_axes = param.Parameter(precedence=-1)
    invert_xaxis = param.Parameter(precedence=-1)
    invert_yaxis = param.Parameter(precedence=-1)
    invert_zaxis = param.Parameter(precedence=-1)
    labelled = param.Parameter(precedence=-1)
    legend_cols = param.Parameter(precedence=-1)
    legend_labels = param.Parameter(precedence=-1)
    legend_position = param.Parameter(precedence=-1)
    legend_opts = param.Parameter(precedence=-1)
    legend_limit = param.Parameter(precedence=-1)
    logx = param.Parameter(precedence=-1)
    logy = param.Parameter(precedence=-1)
    logz = param.Parameter(precedence=-1)
    show_grid = param.Parameter(precedence=-1)
    style_grouping = param.Parameter(precedence=-1)
    xlim = param.Parameter(precedence=-1)
    ylim = param.Parameter(precedence=-1)
    zlim = param.Parameter(precedence=-1)
    xticks = param.Parameter(precedence=-1)
    xformatter = param.Parameter(precedence=-1)
    yticks = param.Parameter(precedence=-1)
    yformatter = param.Parameter(precedence=-1)
    zticks = param.Parameter(precedence=-1)
    zaxis = param.Parameter(precedence=-1)
    zrotation = param.Parameter(precedence=-1)
    zformatter = param.Parameter(precedence=-1)
    xlabel = param.Parameter(precedence=-1)
    ylabel = param.Parameter(precedence=-1)
    zlabel = param.Parameter(precedence=-1)


    def __init__(self, layout, keys=None, dimensions=None, create_axes=False, ranges=None,
                 layout_num=1, **params):
        self.top_level = keys is None
        if self.top_level:
            dimensions, keys = traversal.unique_dimkeys(layout)
        MPLPlot.__init__(self, dimensions=dimensions, keys=keys, **params)

        self.layout = layout
        self.cyclic_index = 0
        self.zorder = 0
        self.layout_num = layout_num
        self.overlaid = False
        self.hmap = layout
        if layout.ndims > 1:
            xkeys, ykeys = zip(*layout.keys(), strict=None)
        else:
            xkeys = layout.keys()
            ykeys = [None]
        self._xkeys = list(dict.fromkeys(xkeys))
        self._ykeys = list(dict.fromkeys(ykeys))

        self._xticks, self._yticks = [], []
        self.rows, self.cols = layout.shape
        self.fig_inches = self._get_size()
        _, _, self.layout = self._create_subplots(layout, None, ranges, create_axes=False)
        self.border_extents = self._compute_borders()
        width, height, _, _, _, _ = self.border_extents
        if self.aspect == 'equal':
            self.aspect = float(width/height)
        # Note that streams are not supported on RasterGridPlot
        # until that is implemented this stub is needed
        self.streams = []

    def _finalize_artist(self, key):
        pass

    def get_extents(self, view, ranges, range_type='combined', **kwargs):
        if range_type == 'hard':
            return (np.nan,)*4
        width, height, _, _, _, _ = self.border_extents
        return (0, 0, width, height)


    def _get_frame(self, key):
        return GridPlot._get_frame(self, key)


    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        _, _, b_w, b_h, widths, heights = self.border_extents

        key = self.keys[-1]
        ranges = self.compute_ranges(self.layout, key, ranges)
        self.handles['projs'] = {}
        x, y = b_w, b_h
        for xidx, xkey in enumerate(self._xkeys):
            w = widths[xidx]
            for yidx, ykey in enumerate(self._ykeys):
                h = heights[yidx]
                if self.layout.ndims > 1:
                    vmap = self.layout.get((xkey, ykey), None)
                else:
                    vmap = self.layout.get(xkey, None)
                pane = vmap.select(**{d.name: val for d, val in zip(self.dimensions, key, strict=None)
                                    if d in vmap.kdims})
                pane = vmap.last.values()[-1] if issubclass(vmap.type, CompositeOverlay) else vmap.last
                data = get_raster_array(pane) if pane else None
                ranges = self.compute_ranges(vmap, key, ranges)
                opts = self.lookup_options(pane, 'style')[self.cyclic_index]
                plot = self.handles['axis'].imshow(data, extent=(x,x+w, y, y+h), **opts)
                cdim = pane.vdims[0].label
                valrange = match_spec(pane, ranges).get(cdim, pane.range(cdim))['combined']
                plot.set_clim(valrange)
                if data is None:
                    plot.set_visible(False)
                self.handles['projs'][(xkey, ykey)] = plot
                y += h + b_h
                if xidx == 0:
                    self._yticks.append(y-b_h-h/2.)
            y = b_h
            x += w + b_w
            self._xticks.append(x-b_w-w/2.)

        kwargs = self._get_axis_kwargs()
        return self._finalize_axis(key, ranges=ranges, **kwargs)

    @mpl_rc_context
    def update_frame(self, key, ranges=None):
        grid = self._get_frame(key)
        ranges = self.compute_ranges(self.layout, key, ranges)
        for xkey in self._xkeys:
            for ykey in self._ykeys:
                plot = self.handles['projs'][(xkey, ykey)]
                grid_key = (xkey, ykey) if self.layout.ndims > 1 else (xkey,)
                element = grid.data.get(grid_key, None)
                if element:
                    plot.set_visible(True)
                    img = element.values()[0] if isinstance(element, CompositeOverlay) else element
                    data = get_raster_array(img)
                    plot.set_data(data)
                else:
                    plot.set_visible(False)

        kwargs = self._get_axis_kwargs()
        return self._finalize_axis(key, ranges=ranges, **kwargs)


    def _get_axis_kwargs(self):
        xdim = self.layout.kdims[0]
        ydim = self.layout.kdims[1] if self.layout.ndims > 1 else None
        xticks = (self._xticks, [xdim.pprint_value(l) for l in self._xkeys])
        yticks = (self._yticks, [ydim.pprint_value(l) if ydim else ''
                                 for l in self._ykeys])
        return dict(dimensions=[xdim, ydim], xticks=xticks, yticks=yticks)


    def _compute_borders(self):
        ndims = self.layout.ndims
        width_fn = lambda x: x.range(0)
        height_fn = lambda x: x.range(1)
        width_extents = [max_range(self.layout[x, :].traverse(width_fn, [Element]))
                         for x in unique_iterator(self.layout.dimension_values(0))]
        if ndims > 1:
            height_extents = [max_range(self.layout[:, y].traverse(height_fn, [Element]))
                              for y in unique_iterator(self.layout.dimension_values(1))]
        else:
            height_extents = [max_range(self.layout.traverse(height_fn, [Element]))]
        widths = [extent[0]-extent[1] for extent in width_extents]
        heights = [extent[0]-extent[1] for extent in height_extents]
        width, height = np.sum(widths), np.sum(heights)
        border_width = (width*self.padding)/(len(widths)+1)
        border_height = (height*self.padding)/(len(heights)+1)
        width += width*self.padding
        height += height*self.padding

        return width, height, border_width, border_height, widths, heights


    def __len__(self):
        return max([len(self.keys), 1])
