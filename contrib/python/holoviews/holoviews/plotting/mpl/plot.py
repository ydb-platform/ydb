from contextlib import contextmanager
from itertools import chain

import matplotlib as mpl
import numpy as np
import param
from matplotlib import (
    animation,
    gridspec,
    pyplot as plt,
    rcParams,
)
from matplotlib.font_manager import font_scalings
from mpl_toolkits.mplot3d import Axes3D  # noqa (For 3D plots)

from ...core import (
    AdjointLayout,
    Collator,
    CompositeOverlay,
    Element,
    Empty,
    GridMatrix,
    GridSpace,
    HoloMap,
    Layout,
    NdLayout,
)
from ...core.options import SkipRendering, Store
from ...core.util import int_to_alpha, int_to_roman, wrap_tuple_streams
from ..plot import (
    DimensionedPlot,
    GenericAdjointLayoutPlot,
    GenericCompositePlot,
    GenericElementPlot,
    GenericLayoutPlot,
)
from ..util import attach_streams, collate, displayable
from .util import compute_ratios, fix_aspect, get_old_rcparams


@contextmanager
def _rc_context(rcparams):
    """Context manager that temporarily overrides the pyplot rcParams.

    """
    old_rcparams = get_old_rcparams()
    mpl.rcParams.clear()
    mpl.rcParams.update(dict(old_rcparams, **rcparams))
    try:
        yield
    finally:
        mpl.rcParams.clear()
        mpl.rcParams.update(old_rcparams)

def mpl_rc_context(f):
    """Decorator for MPLPlot methods applying the matplotlib rc params
    in the plots fig_rcparams while when method is called.

    """
    def wrapper(self, *args, **kwargs):
        with _rc_context(self.fig_rcparams):
            return f(self, *args, **kwargs)
    return wrapper


class MPLPlot(DimensionedPlot):
    """An MPLPlot object draws a matplotlib figure object when called or
    indexed but can also return a matplotlib animation object as
    appropriate. MPLPlots take element objects such as Image, Contours
    or Points as inputs and plots them in the appropriate format using
    matplotlib. As HoloMaps are supported, all plots support animation
    via the anim() method.

    """

    backend = 'matplotlib'

    sideplots = {}

    backend_opts = param.Dict(default={}, doc="""
        A dictionary of custom options to apply to the plot or
        subcomponents of the plot. The keys in the dictionary mirror
        attribute access on the underlying models stored in the plot's
        handles, e.g. {'colorbar.margin': 10} will index the colorbar
        in the Plot.handles and then set the margin to 10.""")

    fig_alpha = param.Number(default=1.0, bounds=(0, 1), doc="""
        Alpha of the overall figure background.""")

    fig_bounds = param.NumericTuple(default=(0.15, 0.15, 0.85, 0.85),
                                       doc="""
        The bounds of the overall figure as a 4-tuple of the form
        (left, bottom, right, top), defining the size of the border
        around the subplots.""")

    fig_inches = param.Parameter(default=4, doc="""
        The overall matplotlib figure size in inches.  May be set as
        an integer in which case it will be used to autocompute a
        size. Alternatively may be set with an explicit tuple or list,
        in which case it will be applied directly after being scaled
        by fig_size. If either the width or height is set to None,
        it will be computed automatically.""")

    fig_latex = param.Boolean(default=False, doc="""
        Whether to use LaTeX text in the overall figure.""")

    fig_rcparams = param.Dict(default={}, doc="""
        matplotlib rc parameters to apply to the overall figure.""")

    fig_size = param.Number(default=100., bounds=(1, None), doc="""
        Size relative to the supplied overall fig_inches in percent.""")

    gridstyle = param.Dict(default={}, doc="""
        Allows customizing the grid style, e.g. grid_color defines
        the line color for both grids while xgrid_color exclusively
        customizes the x-axis grid lines.""")

    initial_hooks = param.HookList(default=[], doc="""
        Optional list of hooks called before plotting the data onto
        the axis (now marked for deprecation). The hook is passed the
        plot object and the displayed object; other plotting handles
        can be accessed via plot.handles.""")

    sublabel_format = param.String(default=None, allow_None=True, doc="""
        Allows labeling the subaxes in each plot with various formatters
        including {Alpha}, {alpha}, {numeric} and {roman}.""")

    sublabel_offset = param.Number(default=0, bounds=(0, None), doc="""
        Allows offsetting the sublabel index.""")

    sublabel_position = param.NumericTuple(default=(-0.35, 0.85), doc="""
         Position relative to the plot for placing the optional subfigure label.""")

    sublabel_size = param.Number(default=18, doc="""
         Size of optional subfigure label.""")

    sublabel_skip = param.List(default=None, item_type=int, doc="""
        List of elements to skip when labeling subplots. Numbering starts at 1.""")

    projection = param.Parameter(default=None, doc="""
        The projection of the plot axis, default of None is equivalent to
        2D plot, '3d' and 'polar' are also supported by matplotlib by default.
        May also supply a custom projection that is either a matplotlib
        projection type or implements the `_as_mpl_axes` method.""")

    show_frame = param.Boolean(default=False, doc="""
        Whether or not to show a complete frame around the plot.""")

    _close_figures = True

    def __init__(self, fig=None, axis=None, **params):
        self._create_fig = True
        super().__init__(**params)
        # List of handles to matplotlib objects for animation update
        self.fig_scale = self.fig_size/100.
        if isinstance(self.fig_inches, (tuple, list)):
            self.fig_inches = [None if i is None else i*self.fig_scale
                               for i in self.fig_inches]
        else:
            self.fig_inches *= self.fig_scale
        if self.fig_latex:
            self.fig_rcparams['text.usetex'] = True

        if self.renderer.interactive:
            plt.ion()
            self._close_figures = False
        elif not self.renderer.notebook_context:
            plt.ioff()

        fig, axis = self._init_axis(fig, axis)

        self.handles['fig'] = fig
        self.handles['axis'] = axis
        self.handles['bbox_extra_artists'] = []


    @mpl_rc_context
    def _init_axis(self, fig, axis):
        """Return an axis which may need to be initialized from
        a new figure.

        """
        if not fig and self._create_fig:
            fig = plt.figure()
            l, b, r, t = self.fig_bounds
            inches = self.fig_inches
            fig.subplots_adjust(left=l, bottom=b, right=r, top=t)
            fig.patch.set_alpha(self.fig_alpha)
            if isinstance(inches, (tuple, list)):
                inches = list(inches)
                if inches[0] is None:
                    inches[0] = inches[1]
                elif inches[1] is None:
                    inches[1] = inches[0]
                fig.set_size_inches(list(inches))
            else:
                fig.set_size_inches([inches, inches])
            axis = fig.add_subplot(111, projection=self.projection)
            axis.set_aspect('auto')
        return fig, axis


    def _get_fontsize_defaults(self):
        base = rcParams['font.size']
        sizes = {
            "label": rcParams['axes.labelsize'],
            "title": rcParams['axes.titlesize'],
            "xticks": rcParams['xtick.labelsize'],
            "yticks": rcParams['ytick.labelsize'],
            "legend": rcParams['legend.fontsize'],
        }
        scaled = dict(sizes)
        for k, v in sizes.items():
            if isinstance(v, str):
                scaled[k] = base * font_scalings[v]
        scaled['ticks'] = scaled['xticks']
        return scaled


    def _subplot_label(self, axis):
        if self.sublabel_skip and self.layout_num in self.sublabel_skip:
            return
        layout_num = self.layout_num if self.subplot else 1
        if self.sublabel_skip:
            if any(n < 1 for n in self.sublabel_skip):
                raise ValueError('sublabel_skip values must be greater than 0')
            sublabel_num = len(set(range(layout_num)) - set(self.sublabel_skip))
        else:
            sublabel_num = layout_num
        sublabel_num += self.sublabel_offset
        if self.sublabel_format and not self.adjoined and sublabel_num > 0:
            from matplotlib.offsetbox import AnchoredText
            labels = {}
            if '{Alpha}' in self.sublabel_format:
                labels['Alpha'] = int_to_alpha(sublabel_num-1)
            elif '{alpha}' in self.sublabel_format:
                labels['alpha'] = int_to_alpha(sublabel_num-1, upper=False)
            elif '{numeric}' in self.sublabel_format:
                labels['numeric'] = sublabel_num
            elif '{Roman}' in self.sublabel_format:
                labels['Roman'] = int_to_roman(sublabel_num)
            elif '{roman}' in self.sublabel_format:
                labels['roman'] = int_to_roman(sublabel_num).lower()
            at = AnchoredText(self.sublabel_format.format(**labels), loc=3,
                              bbox_to_anchor=self.sublabel_position, frameon=False,
                              prop=dict(size=self.sublabel_size, weight='bold'),
                              bbox_transform=axis.transAxes)
            at.patch.set_visible(False)
            axis.add_artist(at)
            sublabel = at.txt.get_children()[0]
            self.handles['sublabel'] = sublabel
            self.handles['bbox_extra_artists'] += [sublabel]



    def _finalize_axis(self, key):
        """General method to finalize the axis and plot.

        """
        if 'title' in self.handles:
            self.handles['title'].set_visible(self.show_title)

        self.drawn = True
        if self.subplot:
            return self.handles['axis']
        else:
            fig = self.handles['fig']
            if not getattr(self, 'overlaid', False) and self._close_figures:
                plt.close(fig)
            return fig


    @property
    def state(self):
        return self.handles['fig']

    def anim(self, start=0, stop=None, fps=30):
        """Method to return a matplotlib animation. The start and stop
        frames may be specified as well as the fps.

        """
        figure = self.state or self.initialize_plot()
        anim = animation.FuncAnimation(figure, self.update_frame,
                                       frames=self.keys,
                                       interval = 1000.0/fps)
        # Close the figure handle
        if self._close_figures: plt.close(figure)
        return anim


    def update(self, key):
        if len(self) == 1 and key in (0, self.keys[0]) and not self.drawn:
            return self.initialize_plot()
        return self.__getitem__(key)



class CompositePlot(GenericCompositePlot, MPLPlot):
    """CompositePlot provides a baseclass for plots coordinate multiple
    subplots to form a Layout.

    """

    shared_axes = param.Boolean(default=True, doc="""
        Whether axes ranges should be shared across the layout, if
        disabled switches axiswise normalization option on globally.""")

    def _link_dimensioned_streams(self):
        """Should perform any linking required to update titles when dimensioned
        streams change.

        """
        streams = [s for s in self.streams if any(k in self.dimensions for k in s.contents)]
        for s in streams:
            s.add_subscriber(self._stream_update, 1)

    def _stream_update(self, **kwargs):
        contents = [k for s in self.streams for k in s.contents]
        key = tuple(None if d in contents else k for d, k in zip(self.dimensions, self.current_key, strict=None))
        key = wrap_tuple_streams(key, self.dimensions, self.streams)
        self._update_title(key)

    def _update_title(self, key):
        title = self._format_title(key) if self.show_title else ''
        if 'title' in self.handles:
            self.handles['title'].set_text(title)
        elif 'axis' in self.handles and self.handles['axis'].figure is not None:
            title = self.handles['axis'].set_title(title, **self._fontsize('title'))
            self.handles['title'] = title

    @mpl_rc_context
    def update_frame(self, key, ranges=None):
        ranges = self.compute_ranges(self.layout, key, ranges)
        for subplot in self.subplots.values():
            subplot.update_frame(key, ranges=ranges)
        self._update_title(key)



class GridPlot(CompositePlot):
    """Plot a group of elements in a grid layout based on a GridSpace element
    object.

    """

    aspect = param.Parameter(default='equal', doc="""
        Aspect ratios on GridPlot should be automatically determined.""")

    padding = param.Number(default=0.1, doc="""
        The amount of padding as a fraction of the total Grid size""")

    shared_xaxis = param.Boolean(default=False, doc="""
        If enabled the x-axes of the GridSpace will be drawn from the
        objects inside the Grid rather than the GridSpace dimensions.""")

    shared_yaxis = param.Boolean(default=False, doc="""
        If enabled the x-axes of the GridSpace will be drawn from the
        objects inside the Grid rather than the GridSpace dimensions.""")

    show_legend = param.Boolean(default=False, doc="""
        Legends add to much clutter in a grid and are disabled by default.""")

    xaxis = param.Selector(default='bottom',
                                 objects=['bottom', 'top', None], doc="""
        Whether and where to display the xaxis, supported options are
        'bottom', 'top' and None.""")

    yaxis = param.Selector(default='left',
                                 objects=['left', 'right', None], doc="""
        Whether and where to display the yaxis, supported options are
        'left', 'right' and None.""")

    xrotation = param.Integer(default=0, bounds=(0, 360), doc="""
        Rotation angle of the xticks.""")

    yrotation = param.Integer(default=0, bounds=(0, 360), doc="""
        Rotation angle of the yticks.""")

    def __init__(self, layout, axis=None, create_axes=True, ranges=None,
                 layout_num=1, keys=None, **params):
        if not isinstance(layout, GridSpace):
            raise Exception("GridPlot only accepts GridSpace.")
        super().__init__(layout, layout_num=layout_num,
                         ranges=ranges, keys=keys, **params)
        # Compute ranges layoutwise
        grid_kwargs = {}
        if axis is not None:
            bbox = axis.get_position()
            l, b, w, h = bbox.x0, bbox.y0, bbox.width, bbox.height
            grid_kwargs = {'left': l, 'right': l+w, 'bottom': b, 'top': b+h}
            self.position = (l, b, w, h)

        self.cols, self.rows = layout.shape
        self.fig_inches = self._get_size()
        self._layoutspec = gridspec.GridSpec(self.rows, self.cols, **grid_kwargs)

        with mpl.rc_context(rc=self.fig_rcparams):
            self.subplots, self.subaxes, self.layout = self._create_subplots(layout, axis,
                                                                             ranges, create_axes)
        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])

    def _get_size(self):
        max_dim = max(self.layout.shape)
        # Reduce plot size as GridSpace gets larger
        shape_factor = 1. / max_dim
        # Expand small grids to a sensible viewing size
        expand_factor = 1 + (max_dim - 1) * 0.1
        scale_factor = expand_factor * shape_factor
        cols, rows = self.layout.shape
        if isinstance(self.fig_inches, (tuple, list)):
            fig_inches = list(self.fig_inches)
            if fig_inches[0] is None:
                fig_inches[0] = fig_inches[1] * (cols/rows)
            if fig_inches[1] is None:
                fig_inches[1] = fig_inches[0] * (rows/cols)
            return fig_inches
        else:
            fig_inches = (self.fig_inches,)*2
            return (scale_factor * cols * fig_inches[0],
                    scale_factor * rows * fig_inches[1])


    def _create_subplots(self, layout, axis, ranges, create_axes):
        norm_opts = self._traverse_options(layout, 'norm', ['axiswise'], [Element])
        axiswise = all(norm_opts.get('axiswise', []))
        if not ranges:
            self.handles['fig'].set_size_inches(self.fig_inches)
        subplots, subaxes = {}, {}
        frame_ranges = self.compute_ranges(layout, None, ranges)
        keys = self.keys[:1] if self.dynamic else self.keys
        frame_ranges = dict([(key, self.compute_ranges(layout, key, frame_ranges))
                                    for key in keys])
        collapsed_layout = layout.clone(shared_data=False, id=layout.id)
        r, c = (0, 0)
        for coord in layout.keys(full_grid=True):
            if not isinstance(coord, tuple): coord = (coord,)
            view = layout.data.get(coord, None)
            # Create subplot
            if type(view) in (Layout, NdLayout):
                raise SkipRendering("Cannot plot nested Layouts.")
            if not displayable(view):
                view = collate(view)
            if view is not None:
                vtype = view.type if isinstance(view, HoloMap) else view.__class__
                opts = self.lookup_options(view, 'plot').options
            else:
                vtype = None

            # Create axes
            kwargs = {}
            if create_axes:
                projection = self._get_projection(view) if vtype else None
                subax = plt.subplot(self._layoutspec[r, c], projection=projection)
                if not axiswise and self.shared_xaxis and self.xaxis is not None:
                    self.xaxis = 'top'
                if not axiswise and self.shared_yaxis and self.yaxis is not None:
                    self.yaxis = 'right'

                # Disable subplot axes depending on shared axis options
                # and the position in the grid
                if (self.shared_xaxis or self.shared_yaxis) and not axiswise:

                    if c == 0 and r != 0:
                        subax.xaxis.set_ticks_position('none')
                        kwargs['xaxis'] = 'bottom-bare'
                    if c != 0 and r == 0 and not layout.ndims == 1:
                        subax.yaxis.set_ticks_position('none')
                        kwargs['yaxis'] = 'left-bare'
                    if r != 0 and c != 0:
                        kwargs['xaxis'] = 'bottom-bare'
                        kwargs['yaxis'] = 'left-bare'
                    if not self.shared_xaxis:
                        kwargs['xaxis'] = 'bottom-bare'
                    if not self.shared_yaxis:
                        kwargs['yaxis'] = 'left-bare'
                else:
                    kwargs['xaxis'] = 'bottom-bare'
                    kwargs['yaxis'] = 'left-bare'
                subaxes[(r, c)] = subax
            else:
                subax = None
            if vtype and issubclass(vtype, CompositeOverlay) and (c == self.cols - 1 and
                                                                  r == self.rows//2):
                kwargs['show_legend'] = self.show_legend
                kwargs['legend_position'] = 'right'
            if (not isinstance(self.layout, GridMatrix) and not
                ((c == self.cols//2 and r == 0) or
                (c == 0 and r == self.rows//2))):
                kwargs['labelled'] = []

            # Create subplot
            if view is not None:
                params = dict(fig=self.handles['fig'], axis=subax,
                              dimensions=self.dimensions, show_title=False,
                              subplot=not create_axes, ranges=frame_ranges,
                              uniform=self.uniform, keys=self.keys,
                              show_legend=False, renderer=self.renderer)
                plotting_class = Store.registry['matplotlib'][vtype]
                subplot = plotting_class(view,  **dict(opts, **dict(params, **kwargs)))
                collapsed_layout[coord] = subplot.layout if isinstance(subplot, CompositePlot) else subplot.hmap
                subplots[(r, c)] = subplot
            elif subax is not None:
                subax.set_visible(False)
            if r != self.rows-1:
                r += 1
            else:
                r = 0
                c += 1
        if create_axes:
            self.handles['axis'] = self._layout_axis(layout, axis)
            self._adjust_subplots(self.handles['axis'], subaxes)

        return subplots, subaxes, collapsed_layout


    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        # Get the extent of the layout elements (not the whole layout)
        key = self.keys[-1]
        axis = self.handles['axis']
        subplot_kwargs = {}
        ranges = self.compute_ranges(self.layout, key, ranges)
        for subplot in self.subplots.values():
            subplot.initialize_plot(ranges=ranges, **subplot_kwargs)

        if self.show_title:
            title = axis.set_title(self._format_title(key),
                                   **self._fontsize('title'))
            self.handles['title'] = title

        self._readjust_axes(axis)
        self.drawn = True
        if self.subplot: return self.handles['axis']
        if self._close_figures: plt.close(self.handles['fig'])
        return self.handles['fig']


    def _readjust_axes(self, axis):
        if self.subplot:
            axis.set_position(self.position)
            if self.aspect == 'equal':
                axis.set_aspect(float(self.rows)/self.cols)
            self.handles['fig'].canvas.draw()
            self._adjust_subplots(self.handles['axis'], self.subaxes)


    def _layout_axis(self, layout, axis):
        fig = self.handles['fig']
        axkwargs = {'gid': str(self.position)} if axis else {}
        layout_axis = fig.add_subplot(1,1,1, **axkwargs)

        if axis:
            axis.set_visible(False)
            layout_axis.set_position(self.position)
        layout_axis.patch.set_visible(False)

        for ax, ax_obj in zip(['x', 'y'], [layout_axis.xaxis, layout_axis.yaxis], strict=None):
            tick_fontsize = self._fontsize(f'{ax}ticks','labelsize', common=False)
            if tick_fontsize: ax_obj.set_tick_params(**tick_fontsize)

        # Set labels
        layout_axis.set_xlabel(layout.kdims[0].pprint_label,
                               **self._fontsize('xlabel'))
        if layout.ndims == 2:
            layout_axis.set_ylabel(layout.kdims[1].pprint_label,
                               **self._fontsize('ylabel'))

        # Compute and set x- and y-ticks
        dims = layout.kdims
        keys = layout.keys()
        if layout.ndims == 1:
            dim1_keys = keys
            dim2_keys = [0]
            layout_axis.get_yaxis().set_visible(False)
        else:
            dim1_keys, dim2_keys = zip(*keys, strict=None)
            layout_axis.set_ylabel(dims[1].pprint_label)
            layout_axis.set_aspect(float(self.rows)/self.cols)

        # Process ticks
        plot_width = (1.0 - self.padding) / self.cols
        border_width = self.padding / (self.cols-1) if self.cols > 1 else 0
        xticks = [(plot_width/2)+(r*(plot_width+border_width)) for r in range(self.cols)]
        plot_height = (1.0 - self.padding) / self.rows
        border_height = self.padding / (self.rows-1) if layout.ndims > 1 else 0
        yticks = [(plot_height/2)+(r*(plot_height+border_height)) for r in range(self.rows)]

        layout_axis.set_xticks(xticks)
        layout_axis.set_xticklabels([dims[0].pprint_value(l)
                                     for l in sorted(set(dim1_keys))])
        for tick in layout_axis.get_xticklabels():
            tick.set_rotation(self.xrotation)

        ydim = dims[1] if layout.ndims > 1 else None
        layout_axis.set_yticks(yticks)
        layout_axis.set_yticklabels([ydim.pprint_value(l) if ydim else ''
                                     for l in sorted(set(dim2_keys))])
        for tick in layout_axis.get_yticklabels():
            tick.set_rotation(self.yrotation)

        if not self.show_frame:
            layout_axis.spines['right' if self.yaxis == 'left' else 'left'].set_visible(False)
            layout_axis.spines['bottom' if self.xaxis == 'top' else 'top'].set_visible(False)

        axis = layout_axis
        if self.xaxis is not None:
            axis.xaxis.set_ticks_position(self.xaxis)
            axis.xaxis.set_label_position(self.xaxis)
        else:
            axis.xaxis.set_visible(False)

        if self.yaxis is not None:
            axis.yaxis.set_ticks_position(self.yaxis)
            axis.yaxis.set_label_position(self.yaxis)
        else:
            axis.yaxis.set_visible(False)

        for pos in ['left', 'right', 'top', 'bottom']:
            axis.spines[pos].set_visible(False)

        return layout_axis


    def _adjust_subplots(self, axis, subaxes):
        bbox = axis.get_position()
        l, b, w, h = bbox.x0, bbox.y0, bbox.width, bbox.height

        if self.padding:
            width_padding = w/(1./self.padding)
            height_padding = h/(1./self.padding)
        else:
            width_padding, height_padding = 0, 0

        if self.cols == 1:
            b_w = 0
        else:
            b_w = width_padding / (self.cols - 1)

        if self.rows == 1:
            b_h = 0
        else:
            b_h = height_padding / (self.rows - 1)
        ax_w = (w - (width_padding if self.cols > 1 else 0)) / self.cols
        ax_h = (h - (height_padding if self.rows > 1 else 0)) / self.rows

        r, c = (0, 0)
        for ax in subaxes.values():
            xpos = l + (c*ax_w) + (c * b_w)
            ypos = b + (r*ax_h) + (r * b_h)
            if r != self.rows-1:
                r += 1
            else:
                r = 0
                c += 1
            if ax is not None:
                ax.set_position([xpos, ypos, ax_w, ax_h])



class AdjointLayoutPlot(MPLPlot, GenericAdjointLayoutPlot):
    """Initially, a AdjointLayoutPlot computes an appropriate layout based for
    the number of Views in the AdjointLayout object it has been given, but
    when embedded in a NdLayout, it can recompute the layout to
    match the number of rows and columns as part of a larger grid.

    """

    layout_dict = {'Single': ['main'],
                   'Dual': ['main', 'right'],
                   'Triple': ['top', None, 'main', 'right'],
                   'Embedded Dual': [None, 'main']}

    def __init__(self, layout, layout_type, subaxes, subplots, **params):
        # The AdjointLayout ViewableElement object
        self.layout = layout
        # Type may be set to 'Embedded Dual' by a call it grid_situate
        self.layout_type = layout_type
        self.view_positions = self.layout_dict[self.layout_type]

        # The supplied (axes, view) objects as indexed by position
        self.subaxes = {pos: ax for ax, pos in zip(subaxes, self.view_positions, strict=None)}
        super().__init__(subplots=subplots, **params)

    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        """Plot all the views contained in the AdjointLayout Object using axes
        appropriate to the layout configuration. All the axes are
        supplied by LayoutPlot - the purpose of the call is to
        invoke subplots with correct options and styles and hide any
        empty axes as necessary.

        """
        for pos in self.view_positions:
            # Pos will be one of 'main', 'top' or 'right' or None
            view = self.layout.get(pos, None)
            subplot = self.subplots.get(pos, None)
            ax = self.subaxes.get(pos, None)
            # If no view object or empty position, disable the axis
            if None in [view, pos, subplot]:
                ax.set_axis_off()
                continue
            subplot.initialize_plot(ranges=ranges)

        self.adjust_positions()
        self.drawn = True

    def adjust_positions(self, redraw=True):
        """Make adjustments to the positions of subplots (if available)
        relative to the main plot axes as required.

        This method is called by LayoutPlot after an initial pass
        used to position all the Layouts together. This method allows
        LayoutPlots to make final adjustments to the axis positions.

        """
        checks = [self.view_positions, self.subaxes, self.subplots]
        right = all('right' in check for check in checks)
        top = all('top' in check for check in checks)
        if "main" not in self.subplots or not (top or right):
            return
        if redraw:
            self.handles['fig'].canvas.draw()
        main_ax = self.subplots['main'].handles['axis']
        bbox = main_ax.get_position()
        if right:
            ax = self.subaxes['right']
            subplot = self.subplots['right']
            if isinstance(subplot, AdjoinedPlot):
                subplot_size = subplot.subplot_size
                border_size = subplot.border_size
            else:
                subplot_size = 0.25
                border_size = 0.25
            ax.set_position([bbox.x1 + bbox.width * border_size,
                             bbox.y0,
                             bbox.width * subplot_size, bbox.height])
            if isinstance(subplot, GridPlot):
                ax.set_aspect('equal')
        if top:
            ax = self.subaxes['top']
            subplot = self.subplots['top']
            if isinstance(subplot, AdjoinedPlot):
                subplot_size = subplot.subplot_size
                border_size = subplot.border_size
            else:
                subplot_size = 0.25
                border_size = 0.25
            ax.set_position([bbox.x0,
                             bbox.y1 + bbox.height * border_size,
                             bbox.width, bbox.height * subplot_size])
            if isinstance(subplot, GridPlot):
                ax.set_aspect('equal')

    @mpl_rc_context
    def update_frame(self, key, ranges=None):
        for pos in self.view_positions:
            subplot = self.subplots.get(pos)
            if subplot is not None:
                subplot.update_frame(key, ranges)

    def __len__(self):
        return max([1 if self.keys is None else len(self.keys), 1])


class LayoutPlot(GenericLayoutPlot, CompositePlot):
    """A LayoutPlot accepts either a Layout or a NdLayout and
    displays the elements in a cartesian grid in scanline order.

    """

    absolute_scaling = param.Selector(default=False, doc="""
      If aspect_weight is enabled absolute_scaling determines whether
      axes are scaled relative to the widest plot or whether the
      aspect scales the axes in absolute terms.""")

    aspect_weight = param.Number(default=0, doc="""
      Weighting of the individual aspects when computing the Layout
      grid aspects and overall figure size.""")

    fig_bounds = param.NumericTuple(default=(0.05, 0.05, 0.95, 0.95), doc="""
      The bounds of the figure as a 4-tuple of the form
      (left, bottom, right, top), defining the size of the border
      around the subplots.""")

    tight = param.Boolean(default=False, doc="""
      Tightly fit the axes in the layout within the fig_bounds
      and tight_padding.""")

    tight_padding = param.Parameter(default=3, doc="""
      Integer or tuple specifying the padding in inches in a tight layout.""")

    hspace = param.Number(default=0.5, doc="""
      Specifies the space between horizontally adjacent elements in the grid.
      Default value is set conservatively to avoid overlap of subplots.""")

    vspace = param.Number(default=0.3, doc="""
      Specifies the space between vertically adjacent elements in the grid.
      Default value is set conservatively to avoid overlap of subplots.""")

    fontsize = param.Parameter(default={'title':16}, allow_None=True)

    # Whether to enable fix for non-square figures
    v17_layout_format = True

    def __init__(self, layout, keys=None, **params):
        super().__init__(layout=layout, keys=keys, **params)
        with mpl.rc_context(rc=self.fig_rcparams):
            self.subplots, self.subaxes, self.layout = self._compute_gridspec(layout)
        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])

    def _compute_gridspec(self, layout):
        """Computes the tallest and widest cell for each row and column
        by examining the Layouts in the GridSpace. The GridSpec is then
        instantiated and the LayoutPlots are configured with the
        appropriate embedded layout_types. The first element of the
        returned tuple is a dictionary of all the LayoutPlots indexed
        by row and column. The second dictionary in the tuple supplies
        the grid indices needed to instantiate the axes for each
        LayoutPlot.

        """
        layout_items = layout.grid_items()
        layout_dimensions = layout.kdims if isinstance(layout, NdLayout) else None

        layouts = {}
        col_widthratios, row_heightratios = {}, {}
        for (r, c) in self.coords:
            # Get view at layout position and wrap in AdjointLayout
            _, view = layout_items.get((c, r) if self.transpose else (r, c), (None, None))
            if isinstance(view, NdLayout):
                raise SkipRendering("Cannot render NdLayout nested inside a Layout")
            layout_view = view if isinstance(view, AdjointLayout) else AdjointLayout([view])
            layouts[(r, c)] = layout_view

            # Compute shape of AdjointLayout element
            layout_lens = {1:'Single', 2:'Dual', 3:'Triple'}
            layout_type = layout_lens[len(layout_view)]

            # Get aspects
            main = layout_view.main
            main = main.last if isinstance(main, HoloMap) else main
            main_options = self.lookup_options(main, 'plot').options if main else {}
            if main and not isinstance(main_options.get('aspect', 1), str):
                main_aspect = np.nan if isinstance(main, Empty) else main_options.get('aspect', 1)
                main_aspect = self.aspect_weight*main_aspect + 1-self.aspect_weight
            else:
                main_aspect = np.nan

            if layout_type in ['Dual', 'Triple']:
                el = layout_view.get('right', None)
                eltype = type(el)
                if el and eltype in MPLPlot.sideplots:
                    plot_type = MPLPlot.sideplots[type(el)]
                    ratio = 0.6*(plot_type.subplot_size+plot_type.border_size)
                    width_ratios = [4, 4*ratio]
                else:
                    width_ratios = [4, 1]
            else:
                width_ratios = [4]

            inv_aspect = 1./main_aspect if main_aspect else np.nan
            if layout_type in ['Embedded Dual', 'Triple']:
                el = layout_view.get('top', None)
                eltype = type(el)
                if el and eltype in MPLPlot.sideplots:
                    plot_type = MPLPlot.sideplots[type(el)]
                    ratio = 0.6*(plot_type.subplot_size+plot_type.border_size)
                    height_ratios = [4*ratio, 4]
                else:
                    height_ratios = [1, 4]
            else:
                height_ratios = [4]

            if not isinstance(main_aspect, (str, type(None))):
                width_ratios = [wratio * main_aspect for wratio in width_ratios]
                height_ratios = [hratio * inv_aspect for hratio in height_ratios]
            layout_shape = (len(width_ratios), len(height_ratios))

            # For each row and column record the width and height ratios
            # of the LayoutPlot with the most horizontal or vertical splits
            # and largest aspect
            prev_heights = row_heightratios.get(r, (0, []))
            if layout_shape[1] > prev_heights[0]:
                row_heightratios[r] = [layout_shape[1], prev_heights[1]]
            row_heightratios[r][1].append(height_ratios)

            prev_widths = col_widthratios.get(c, (0, []))
            if layout_shape[0] > prev_widths[0]:
                col_widthratios[c] = (layout_shape[0], prev_widths[1])
            col_widthratios[c][1].append(width_ratios)


        col_splits = [v[0] for __, v in sorted(col_widthratios.items())]
        row_splits = [v[0] for ___, v in sorted(row_heightratios.items())]

        widths = np.array([r for col in col_widthratios.values()
                           for ratios in col[1] for r in ratios])/4

        wr_unnormalized = compute_ratios(col_widthratios, False)
        hr_list = compute_ratios(row_heightratios)
        wr_list = compute_ratios(col_widthratios)

        # Compute the number of rows and cols
        cols, rows = len(wr_list), len(hr_list)


        wr_list = [r if np.isfinite(r) else 1 for r in wr_list]
        hr_list = [r if np.isfinite(r) else 1 for r in hr_list]

        width = sum([r if np.isfinite(r) else 1 for r in wr_list])
        yscale = width/sum([(1/v)*4 if np.isfinite(v) else 4 for v in wr_unnormalized])
        if self.absolute_scaling:
            width = width*np.nanmax(widths)

        xinches, yinches = None, None
        if not isinstance(self.fig_inches, (tuple, list)):
            xinches = self.fig_inches * width
            yinches = xinches/yscale
        elif self.fig_inches[0] is None:
            xinches = self.fig_inches[1] * yscale
            yinches = self.fig_inches[1]
        elif self.fig_inches[1] is None:
            xinches = self.fig_inches[0]
            yinches = self.fig_inches[0] / yscale
        if xinches and yinches:
            self.handles['fig'].set_size_inches([xinches, yinches])

        self.gs = gridspec.GridSpec(rows, cols,
                                    width_ratios=wr_list,
                                    height_ratios=hr_list,
                                    wspace=self.hspace,
                                    hspace=self.vspace)

        # Explicitly clear Matplotlib figures to avoid
        # "Auto-removal of overlapping axes" warning.
        self.handles['fig'].clf()

        # Situate all the Layouts in the grid and compute the gridspec
        # indices for all the axes required by each LayoutPlot.
        gidx = 0
        layout_count = 0
        tight = self.tight
        collapsed_layout = layout.clone(shared_data=False, id=layout.id)
        frame_ranges = self.compute_ranges(layout, None, None)
        keys = self.keys[:1] if self.dynamic else self.keys
        frame_ranges = dict([(key, self.compute_ranges(layout, key, frame_ranges))
                                    for key in keys])
        layout_subplots, layout_axes = {}, {}
        for r, c in self.coords:
            # Compute the layout type from shape
            wsplits = col_splits[c]
            hsplits = row_splits[r]
            if (wsplits, hsplits) == (1,1):
                layout_type = 'Single'
            elif (wsplits, hsplits) == (2,1):
                layout_type = 'Dual'
            elif (wsplits, hsplits) == (1,2):
                layout_type = 'Embedded Dual'
            elif (wsplits, hsplits) == (2,2):
                layout_type = 'Triple'

            # Get the AdjoinLayout at the specified coordinate
            view = layouts[(r, c)]
            positions = AdjointLayoutPlot.layout_dict[layout_type]

            # Create temporary subplots to get projections types
            # to create the correct subaxes for all plots in the layout
            _, _, projs = self._create_subplots(layouts[(r, c)], positions,
                                                None, frame_ranges, create=False)
            gidx, gsinds = self.grid_situate(gidx, layout_type, cols)

            layout_key, _ = layout_items.get((r, c), (None, None))
            if isinstance(layout, NdLayout) and layout_key:
                layout_dimensions = dict(zip(layout_dimensions, layout_key, strict=None))

            # Generate the axes and create the subplots with the appropriate
            # axis objects, handling any Empty objects.
            obj = layouts[(r, c)]
            empty = isinstance(obj.main, Empty)
            if view.main is None:
                continue
            elif empty:
                obj = AdjointLayout([])
            elif not view.traverse(lambda x: x, [Element]):
                self.param.warning(f'{obj.main} is empty, skipping subplot.')
                continue
            elif self.transpose:
                layout_count = (c*self.rows+(r+1))
            else:
                layout_count += 1
            subaxes = [plt.subplot(self.gs[ind], projection=proj)
                       for ind, proj in zip(gsinds, projs, strict=None)]
            subplot_data = self._create_subplots(obj, positions,
                                                 layout_dimensions, frame_ranges,
                                                 dict(zip(positions, subaxes, strict=None)),
                                                 num=0 if empty else layout_count)
            subplots, adjoint_layout, _ = subplot_data
            layout_axes[(r, c)] = subaxes

            # Generate the AdjointLayoutsPlot which will coordinate
            # plotting of AdjointLayouts in the larger grid
            plotopts = self.lookup_options(view, 'plot').options
            layout_plot = AdjointLayoutPlot(adjoint_layout, layout_type, subaxes, subplots,
                                            fig=self.handles['fig'], **plotopts)
            layout_subplots[(r, c)] = layout_plot
            tight = not any(type(p) is GridPlot for p in layout_plot.subplots.values()) and tight
            if layout_key:
                collapsed_layout[layout_key] = adjoint_layout

        # Apply tight layout if enabled and incompatible
        # GridPlot isn't present.
        if tight:
            if isinstance(self.tight_padding, (tuple, list)):
                wpad, hpad = self.tight_padding
                padding = dict(w_pad=wpad, h_pad=hpad)
            else:
                padding = dict(w_pad=self.tight_padding, h_pad=self.tight_padding)
            self.gs.tight_layout(self.handles['fig'], rect=self.fig_bounds, **padding)

        return layout_subplots, layout_axes, collapsed_layout

    def grid_situate(self, current_idx, layout_type, subgrid_width):
        """Situate the current AdjointLayoutPlot in a LayoutPlot. The
        LayoutPlot specifies a layout_type into which the AdjointLayoutPlot
        must be embedded. This enclosing layout is guaranteed to have
        enough cells to display all the views.

        Based on this enforced layout format, a starting index
        supplied by LayoutPlot (indexing into a large gridspec
        arrangement) is updated to the appropriate embedded value. It
        will also return a list of gridspec indices associated with
        the all the required layout axes.

        """
        # Set the layout configuration as situated in a NdLayout

        if layout_type == 'Single':
            start, inds = current_idx+1, [current_idx]
        elif layout_type == 'Dual':
            start, inds = current_idx+2, [current_idx, current_idx+1]

        bottom_idx = current_idx + subgrid_width
        if layout_type == 'Embedded Dual':
            bottom = ((current_idx+1) % subgrid_width) == 0
            grid_idx = (bottom_idx if bottom else current_idx)+1
            start, inds = grid_idx, [current_idx, bottom_idx]
        elif layout_type == 'Triple':
            bottom = ((current_idx+2) % subgrid_width) == 0
            grid_idx = (bottom_idx if bottom else current_idx) + 2
            start, inds = grid_idx, [current_idx, current_idx+1,
                              bottom_idx, bottom_idx+1]

        return start, inds

    def _create_subplots(self, layout, positions, layout_dimensions, ranges, axes=None, num=1, create=True):
        """Plot all the views contained in the AdjointLayout Object using axes
        appropriate to the layout configuration. All the axes are
        supplied by LayoutPlot - the purpose of the call is to
        invoke subplots with correct options and styles and hide any
        empty axes as necessary.

        """
        if axes is None:
            axes = {}
        subplots = {}
        projections = []
        adjoint_clone = layout.clone(shared_data=False, id=layout.id)
        subplot_opts = dict(show_title=False, adjoined=layout)
        for pos in positions:
            # Pos will be one of 'main', 'top' or 'right' or None
            view = layout.get(pos, None)
            if not displayable(view):
                view = collate(view)
            ax = axes.get(pos, None)
            if view is None or not view.traverse(lambda x: x, [Element]):
                projections.append(None)
                continue

            # Determine projection type for plot
            projections.append(self._get_projection(view))

            if not create:
                continue

            # Customize plotopts depending on position.
            plotopts = self.lookup_options(view, 'plot').options

            # Options common for any subplot
            override_opts = {}
            sublabel_opts = {}
            if pos == 'main':
                own_params = self.param.values(onlychanged=True).items()
                sublabel_opts = {k: v for k, v in own_params
                                 if 'sublabel_' in k}
            elif pos == 'right':
                right_opts = dict(invert_axes=True,
                                  xaxis=None)
                override_opts = dict(subplot_opts, **right_opts)
            elif pos == 'top':
                top_opts = dict(yaxis=None)
                override_opts = dict(subplot_opts, **top_opts)

            # Override the plotopts as required
            plotopts = dict(sublabel_opts, **plotopts)
            plotopts.update(override_opts, fig=self.handles['fig'])
            vtype = view.type if isinstance(view, HoloMap) else view.__class__
            if isinstance(view, GridSpace):
                plotopts['create_axes'] = ax is not None
            plot_type = Store.registry['matplotlib'][vtype]
            if pos != 'main' and vtype in MPLPlot.sideplots:
                plot_type = MPLPlot.sideplots[vtype]
            num = num if len(self.coords) > 1 else 0
            subplots[pos] = plot_type(view, axis=ax, keys=self.keys,
                                      dimensions=self.dimensions,
                                      layout_dimensions=layout_dimensions,
                                      ranges=ranges, subplot=True,
                                      uniform=self.uniform, layout_num=num,
                                      renderer=self.renderer, **plotopts)
            if isinstance(view, (Element, HoloMap, Collator, CompositeOverlay)):
                adjoint_clone[pos] = subplots[pos].hmap
            else:
                adjoint_clone[pos] = subplots[pos].layout
        return subplots, adjoint_clone, projections


    @mpl_rc_context
    def initialize_plot(self):
        key = self.keys[-1]
        ranges = self.compute_ranges(self.layout, key, None)
        for subplot in self.subplots.values():
            subplot.initialize_plot(ranges=ranges)

        # Create title handle
        title_obj = None
        title = self._format_title(key)
        if self.show_title and len(self.coords) > 1 and title:
            title_obj = self.handles['fig'].suptitle(title, **self._fontsize('title'))
            self.handles['title'] = title_obj
            self.handles['bbox_extra_artists'] += [title_obj]

        fig = self.handles['fig']
        if (not self.traverse(specs=[GridPlot]) and not isinstance(self.fig_inches, tuple)
            and self.v17_layout_format):
            traverse_fn = lambda x: x.handles.get('bbox_extra_artists', None)
            extra_artists = list(
                chain.from_iterable(artists for artists in self.traverse(traverse_fn)
                                    if artists is not None)
            )
            fix_aspect(fig, self.rows, self.cols,
                       title_obj, extra_artists,
                       vspace=self.vspace*self.fig_scale,
                       hspace=self.hspace*self.fig_scale)
            colorbars = self.traverse(specs=[lambda x: hasattr(x, 'colorbar')])
            for cbar_plot in colorbars:
                if cbar_plot.colorbar:
                    cbar_plot._draw_colorbar(redraw=False)
            adjoineds = self.traverse(specs=[AdjointLayoutPlot])
            for adjoined in adjoineds:
                if len(adjoined.subplots) > 1:
                    adjoined.adjust_positions(redraw=False)
        return self._finalize_axis(None)



class AdjoinedPlot(DimensionedPlot):

    aspect = param.Parameter(default='auto', doc="""
        Aspect ratios on SideHistogramPlot should be determined by the
        AdjointLayoutPlot.""")

    bgcolor = param.Parameter(default=(1, 1, 1, 0), doc="""
        Make plot background invisible.""")

    border_size = param.Number(default=0.25, doc="""
        The size of the border expressed as a fraction of the main plot.""")

    show_title = param.Boolean(default=False, doc="""
        Titles should be disabled on all SidePlots to avoid clutter.""")

    subplot_size = param.Number(default=0.25, doc="""
        The size subplots as expressed as a fraction of the main plot.""")

    show_xlabel = param.Boolean(default=False, doc="""
        Whether to show the x-label of the plot. Disabled by default
        because plots are often too cramped to fit the title correctly.""")
