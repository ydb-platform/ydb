import copy
import math
import warnings
from types import FunctionType

import matplotlib.colors as mpl_colors
import numpy as np
import param
from matplotlib import ticker
from matplotlib.dates import date2num
from matplotlib.image import AxesImage

from ...core import (
    CompositeOverlay,
    Dataset,
    DynamicMap,
    Element,
    Element3D,
    NdOverlay,
    util,
)
from ...core.dimension import Dimension
from ...core.options import Keywords, abbreviated_exception
from ...core.util import dtype_kind
from ...element import Graph, Path
from ...streams import Stream
from ...util.transform import dim
from ..plot import GenericElementPlot, GenericOverlayPlot
from ..util import color_intervals, dim_range_key, process_cmap
from .plot import MPLPlot, mpl_rc_context
from .util import MPL_VERSION, EqHistNormalize, validate, wrap_formatter


class ElementPlot(GenericElementPlot, MPLPlot):

    apply_ticks = param.Boolean(default=True, doc="""
        Whether to apply custom ticks.""")

    aspect = param.Parameter(default='square', doc="""
        The aspect ratio mode of the plot. By default, a plot may
        select its own appropriate aspect ratio but sometimes it may
        be necessary to force a square aspect ratio (e.g. to display
        the plot as an element of a grid). The modes 'auto' and
        'equal' correspond to the axis modes of the same name in
        matplotlib, a numeric value specifying the ratio between plot
        width and height may also be passed. To control the aspect
        ratio between the axis scales use the data_aspect option
        instead.""")

    data_aspect = param.Number(default=None, doc="""
        Defines the aspect of the axis scaling, i.e. the ratio of
        y-unit to x-unit.""")

    invert_zaxis = param.Boolean(default=False, doc="""
        Whether to invert the plot z-axis.""")

    labelled = param.List(default=['x', 'y'], doc="""
        Whether to plot the 'x' and 'y' labels.""")

    logz  = param.Boolean(default=False, doc="""
         Whether to apply log scaling to the y-axis of the Chart.""")

    xformatter = param.ClassSelector(
        default=None, class_=(str, ticker.Formatter, FunctionType), doc="""
        Formatter for ticks along the x-axis.""")

    yformatter = param.ClassSelector(
        default=None, class_=(str, ticker.Formatter, FunctionType), doc="""
        Formatter for ticks along the y-axis.""")

    zformatter = param.ClassSelector(
        default=None, class_=(str, ticker.Formatter, FunctionType), doc="""
        Formatter for ticks along the z-axis.""")

    zaxis = param.Boolean(default=True, doc="""
        Whether to display the z-axis.""")

    zlabel = param.String(default=None, doc="""
        An explicit override of the z-axis label, if set takes precedence
        over the dimension label.""")

    zrotation = param.Integer(default=0, bounds=(0, 360), doc="""
        Rotation angle of the zticks.""")

    zticks = param.Parameter(default=None, doc="""
        Ticks along z-axis specified as an integer, explicit list of
        tick locations, list of tuples containing the locations and
        labels or a matplotlib tick locator object. If set to None
        default matplotlib ticking behavior is applied.""")

    # Element Plots should declare the valid style options for matplotlib call
    style_opts = []

    # No custom legend options
    _legend_opts = {}

    # Declare which styles cannot be mapped to a non-scalar dimension
    _nonvectorized_styles = ['marker', 'alpha', 'cmap', 'angle', 'visible']

    # Whether plot has axes, disables setting axis limits, labels and ticks
    _has_axes = True

    def __init__(self, element, **params):
        super().__init__(element, **params)
        check = self.hmap.last
        if isinstance(check, CompositeOverlay):
            check = check.values()[0] # Should check if any are 3D plots
        if isinstance(check, Element3D):
            self.projection = '3d'

        for hook in self.initial_hooks:
            try:
                hook(self, element)
            except Exception as e:
                self.param.warning(f"Plotting hook {hook!r} could not be "
                                   f"applied:\n\n {e}")

    def _finalize_axis(self, key, element=None, title=None, dimensions=None, ranges=None, xticks=None,
                       yticks=None, zticks=None, xlabel=None, ylabel=None, zlabel=None):
        """Applies all the axis settings before the axis or figure is returned.
        Only plots with zorder 0 get to apply their settings.

        When the number of the frame is supplied as n, this method looks
        up and computes the appropriate title, axis labels and axis bounds.

        """
        if element is None:
            element = self._get_frame(key)
        self.current_frame = element
        if not dimensions and element and not self.subplots:
            el = element.traverse(lambda x: x, [Element])
            if el:
                el = el[0]
                dimensions = el.nodes.dimensions() if isinstance(el, Graph) else el.dimensions()
        axis = self.handles['axis']

        subplots = list(self.subplots.values()) if self.subplots else []
        if self.zorder == 0 and key is not None:
            if self.bgcolor:
                if MPL_VERSION <= (1, 5, 9):
                    axis.set_axis_bgcolor(self.bgcolor)
                else:
                    axis.set_facecolor(self.bgcolor)

            # Apply title
            title = self._format_title(key)
            if self.show_title and title is not None:
                fontsize = self._fontsize('title')
                if 'title' in self.handles:
                    self.handles['title'].set_text(title)
                else:
                    self.handles['title'] = axis.set_title(title, **fontsize)

            # Apply subplot label
            self._subplot_label(axis)

            # Apply axis options if axes are enabled
            if element is not None and not any(not sp._has_axes for sp in [self, *subplots]):
                # Set axis labels
                if dimensions:
                    self._set_labels(axis, dimensions, xlabel, ylabel, zlabel)
                else:
                    if self.xlabel is not None:
                        axis.set_xlabel(self.xlabel)
                    if self.ylabel is not None:
                        axis.set_ylabel(self.ylabel)
                    if self.zlabel is not None and hasattr(axis, 'set_zlabel'):
                        axis.set_zlabel(self.zlabel)

                if not subplots:
                    legend = axis.get_legend()
                    if legend:
                        legend.set_visible(self.show_legend)
                        self.handles["bbox_extra_artists"] += [legend]
                    # Apply grid settings
                    self._update_grid(axis)

                # Apply log axes
                if self.logx:
                    axis.set_xscale('log')
                if self.logy:
                    axis.set_yscale('log')

                if not (isinstance(self.projection, str) and self.projection == '3d'):
                    self._set_axis_position(axis, 'x', self.xaxis)
                    self._set_axis_position(axis, 'y', self.yaxis)

                # Apply ticks
                if self.apply_ticks:
                    self._finalize_ticks(axis, dimensions, xticks, yticks, zticks)

                # Set axes limits
                self._set_axis_limits(axis, element, subplots, ranges)

            # Apply aspects
            if self.aspect is not None and self.projection != 'polar' and not self.adjoined:
                self._set_aspect(axis, self.aspect)

        if not subplots and not self.drawn:
            self._finalize_artist(element)

        self._execute_hooks(element)
        return super()._finalize_axis(key)

    def _execute_hooks(self, element):
        super()._execute_hooks(element)
        self._update_backend_opts()

    def _finalize_ticks(self, axis, dimensions, xticks, yticks, zticks):
        """Finalizes the ticks on the axes based on the supplied ticks
        and Elements. Sets the axes position as well as tick positions,
        labels and fontsize.

        """
        ndims = len(dimensions) if dimensions else 0
        xdim = dimensions[0] if ndims else None
        ydim = dimensions[1] if ndims > 1 else None

        # Tick formatting
        if xdim:
            self._set_axis_formatter(axis.xaxis, xdim, self.xformatter)
        if ydim:
            self._set_axis_formatter(axis.yaxis, ydim, self.yformatter)
        if isinstance(self.projection, str) and self.projection == '3d':
            zdim = dimensions[2] if ndims > 2 else None
            if zdim or self.zformatter is not None:
                self._set_axis_formatter(axis.zaxis, zdim, self.zformatter)

        xticks = xticks if xticks else self.xticks
        self._set_axis_ticks(axis.xaxis, xticks, log=self.logx,
                             rotation=self.xrotation)

        yticks = yticks if yticks else self.yticks
        self._set_axis_ticks(axis.yaxis, yticks, log=self.logy,
                             rotation=self.yrotation)

        if isinstance(self.projection, str) and self.projection == '3d':
            zticks = zticks if zticks else self.zticks
            self._set_axis_ticks(axis.zaxis, zticks, log=self.logz,
                                 rotation=self.zrotation)

        axes_str = 'xy'
        axes_list = [axis.xaxis, axis.yaxis]

        if hasattr(axis, 'zaxis'):
            axes_str += 'z'
            axes_list.append(axis.zaxis)

        for ax, ax_obj in zip(axes_str, axes_list, strict=None):
            tick_fontsize = self._fontsize(f'{ax}ticks','labelsize',common=False)
            if tick_fontsize: ax_obj.set_tick_params(**tick_fontsize)

    def _update_backend_opts(self):
        plot = self.handles["fig"]

        model_accessor_aliases = {
            "figure": "fig",
            "axes": "axis",
            "ax": "axis",
            "colorbar": "cbar",
        }

        for opt, val in self.backend_opts.items():
            parsed_opt = self._parse_backend_opt(
                opt, plot, model_accessor_aliases)
            if parsed_opt is None:
                continue

            model, attr_accessor = parsed_opt
            if not attr_accessor.startswith("set_"):
                attr_accessor = f"set_{attr_accessor}"

            if not isinstance(model, list):
                # to reduce the need for many if/else; cast to list
                # to do the same thing for both single and multiple models
                models = [model]
            else:
                models = model

            try:
                for m in models:
                    getattr(m, attr_accessor)(val)
            except AttributeError as exc:
                valid_options = [attr for attr in dir(models[0]) if attr.startswith("set_")]
                kws = Keywords(values=valid_options)
                matches = sorted(kws.fuzzy_match(attr_accessor))
                self.param.warning(
                    f"Encountered error: {exc}, or could not find "
                    f"{attr_accessor!r} method on {type(models[0]).__name__!r} "
                    f"model. Ensure the custom option spec {opt!r} you provided references a "
                    f"valid method on the specified model. Similar options include {matches!r}"
                )

    def _update_grid(self, axis):
        """Updates grid settings on the matplotlib axis based on gridstyle options.

        """
        if self.show_grid is False:
            axis.grid(False)
            return

        style_items = self.gridstyle.items()
        # Options that apply to both x and y grids, x, and then y
        both = {
            k.removeprefix('grid_'): v
            for k, v in style_items
            if not k.startswith(('x', 'y'))
        }

        # Merge options - both options first, then axis-specific
        xopts = {
            **both,
            **{k.removeprefix('xgrid_').removeprefix("x_"): v for k, v in style_items if k.startswith('x')}
        }

        yopts = {
            **both,
            **{k.removeprefix('ygrid_').removeprefix("y_"): v for k, v in style_items if k.startswith('y')}
        }

        # Apply grid visibility first
        axis.grid(True)

        # Apply x-axis grid styling
        if xopts:
            xgridlines = axis.get_xgridlines()
            for line in xgridlines:
                for prop, val in xopts.items():
                    if fn := getattr(line, f'set_{prop}', None):
                        fn(val)
                    else:
                        self.param.warning(
                            f"Grid line has no property 'set_{prop}' to set grid style."
                        )

        # Apply y-axis grid styling
        if yopts:
            ygridlines = axis.get_ygridlines()
            for line in ygridlines:
                for prop, val in yopts.items():
                    if fn := getattr(line, f'set_{prop}', None):
                        fn(val)
                    else:
                        self.param.warning(
                            f"Grid line has no property 'set_{prop}' to set grid style."
                        )

    def _finalize_artist(self, element):
        """Allows extending the _finalize_axis method with Element
        specific options.

        """

    def _set_labels(self, axes, dimensions, xlabel=None, ylabel=None, zlabel=None):
        """Sets the labels of the axes using the supplied list of dimensions.
        Optionally explicit labels may be supplied to override the dimension
        label.

        """
        xlabel, ylabel, zlabel = self._get_axis_labels(dimensions, xlabel, ylabel, zlabel)
        if self.invert_axes:
            xlabel, ylabel = ylabel, xlabel
        if xlabel and self.xaxis and 'x' in self.labelled:
            axes.set_xlabel(xlabel, **self._fontsize('xlabel'))
        if ylabel and self.yaxis and 'y' in self.labelled:
            axes.set_ylabel(ylabel, **self._fontsize('ylabel'))
        if zlabel and self.zaxis and 'z' in self.labelled:
            axes.set_zlabel(zlabel, **self._fontsize('zlabel'))


    def _set_axis_formatter(self, axis, dim, formatter):
        """Set axis formatter based on dimension formatter.

        """
        if isinstance(dim, list): dim = dim[0]
        if formatter is not None or dim is None:
            pass
        elif dim.value_format:
            formatter = dim.value_format
        elif dim.type in dim.type_formatters:
            formatter = dim.type_formatters[dim.type]
        if formatter:
            axis.set_major_formatter(wrap_formatter(formatter))


    def get_aspect(self, xspan, yspan):
        """Computes the aspect ratio of the plot

        """
        if isinstance(self.aspect, (int, float)):
            return self.aspect
        elif self.aspect == 'square':
            return 1
        elif self.aspect == 'equal':
            if (
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
            return xspan/yspan
        return 1


    def _set_aspect(self, axes, aspect):
        """Set the aspect on the axes based on the aspect setting.

        """
        if isinstance(self.projection, str) and self.projection == '3d':
            return

        if ((isinstance(aspect, str) and aspect != 'square') or
            self.data_aspect):
            data_ratio = self.data_aspect or aspect
        else:
            (x0, x1), (y0, y1) = axes.get_xlim(), axes.get_ylim()
            xsize = np.log(x1) - np.log(x0) if self.logx else x1-x0
            ysize = np.log(y1) - np.log(y0) if self.logy else y1-y0
            xsize = max(abs(xsize), 1e-30)
            ysize = max(abs(ysize), 1e-30)
            data_ratio = 1./(ysize/xsize)
            if aspect != 'square':
                data_ratio = data_ratio/aspect
        axes.set_aspect(data_ratio)


    def _set_axis_limits(self, axis, view, subplots, ranges):
        """Compute extents for current view and apply as axis limits

        """
        # Extents
        extents = self.get_extents(view, ranges)
        if not extents or self.overlaid:
            axis.autoscale_view(scalex=True, scaley=True)
            return

        valid_lim = lambda c: util.isnumeric(c) and not np.isnan(c)
        coords = [coord if isinstance(coord, np.datetime64) or np.isreal(coord) else np.nan for coord in extents]
        coords = [date2num(util.dt64_to_dt(c)) if isinstance(c, np.datetime64) else c
                  for c in coords]
        if (isinstance(self.projection, str) and self.projection == '3d') or len(extents) == 6:
            l, b, zmin, r, t, zmax = coords
            if self.invert_zaxis or any(p.invert_zaxis for p in subplots):
                zmin, zmax = zmax, zmin
            if zmin != zmax:
                if valid_lim(zmin):
                    axis.set_zlim(bottom=zmin)
                if valid_lim(zmax):
                    axis.set_zlim(top=zmax)
        elif isinstance(self.projection, str) and self.projection == "polar":
            _, b, _, t = coords
            l = 0
            r = 2 * np.pi
        else:
            l, b, r, t = coords

        if self.invert_axes:
            l, b, r, t = b, l, t, r

        invertx = self.invert_xaxis or any(p.invert_xaxis for p in subplots)
        xlim, scalex = self._compute_limits(l, r, self.logx, invertx, 'left', 'right')
        inverty = self.invert_yaxis or any(p.invert_yaxis for p in subplots)
        ylim, scaley =  self._compute_limits(b, t, self.logy, inverty, 'bottom', 'top')
        if xlim:
            axis.set_xlim(**xlim)
        if ylim:
            axis.set_ylim(**ylim)
        axis.autoscale_view(scalex=scalex, scaley=scaley)


    def _compute_limits(self, low, high, log, invert, low_key, high_key):
        scale = True
        lims = {}
        valid_lim = lambda c: util.isnumeric(c) and not np.isnan(c)
        if not isinstance(low, util.datetime_types) and log and (low is None or low <= 0):
            low = 0.01 if high > 0.01 else 10**(np.log10(high)-2)
            self.param.warning(
                "Logarithmic axis range encountered value less "
                "than or equal to zero, please supply explicit "
                f"lower-bound to override default of {low:.3f}.")
        if invert:
            high, low = low, high
        if isinstance(low, util.cftime_types) or low != high:
            if valid_lim(low):
                lims[low_key] = low
                scale = False
            if valid_lim(high):
                lims[high_key] = high
                scale = False
        return lims, scale


    def _set_axis_position(self, axes, axis, option):
        """Set the position and visibility of the xaxis or yaxis by
        supplying the axes object, the axis to set, i.e. 'x' or 'y'
        and an option to specify the position and visibility of the axis.
        The option may be None, 'bare' or positional, i.e. 'left' and
        'right' for the yaxis and 'top' and 'bottom' for the xaxis.
        May also combine positional and 'bare' into for example 'left-bare'.

        """
        positions = {'x': ['bottom', 'top'], 'y': ['left', 'right']}[axis]
        axis = axes.xaxis if axis == 'x' else axes.yaxis
        if option in [None, False]:
            axis.set_visible(False)
            for pos in positions:
                axes.spines[pos].set_visible(False)
        else:
            if option is True:
                option = positions[0]
            if 'bare' in option:
                axis.set_ticklabels([])
                axis.set_label_text('')
            if option != 'bare':
                option = option.split('-')[0]
                axis.set_ticks_position(option)
                axis.set_label_position(option)
        if not self.overlaid and not self.show_frame and self.projection != 'polar':
            pos = (positions[1] if (option and (option == 'bare' or positions[0] in option))
                   else positions[0])
            axes.spines[pos].set_visible(False)


    def _set_axis_ticks(self, axis, ticks, log=False, rotation=0):
        """Allows setting the ticks for a particular axis either with
        a tuple of ticks, a tick locator object, an integer number
        of ticks, a list of tuples containing positions and labels
        or a list of positions. Also supports enabling log ticking
        if an integer number of ticks is supplied and setting a
        rotation for the ticks.

        """
        if isinstance(ticks, np.ndarray):
            ticks = list(ticks)
        if isinstance(ticks, (list, tuple)) and all(isinstance(l, list) for l in ticks):
            axis.set_ticks(ticks[0])
            axis.set_ticklabels(ticks[1])
        elif isinstance(ticks, ticker.Locator):
            axis.set_major_locator(ticks)
        elif ticks is not None and not ticks:
            axis.set_ticks([])
        elif isinstance(ticks, int):
            if log:
                locator = ticker.LogLocator(numticks=ticks,
                                            subs=range(1,10))
            else:
                locator = ticker.MaxNLocator(ticks)
            axis.set_major_locator(locator)
        elif isinstance(ticks, (list, tuple)):
            labels = None
            if all(isinstance(t, tuple) for t in ticks):
                ticks, labels = zip(*ticks, strict=None)
            axis.set_ticks(ticks)
            if labels:
                axis.set_ticklabels(labels)
        for tick in axis.get_ticklabels():
            tick.set_rotation(rotation)


    @mpl_rc_context
    def update_frame(self, key, ranges=None, element=None):
        """Set the plot(s) to the given frame number.  Operates by
        manipulating the matplotlib objects held in the self._handles
        dictionary.

        If n is greater than the number of available frames, update
        using the last available frame.

        """
        reused = isinstance(self.hmap, DynamicMap) and self.overlaid
        self.prev_frame =  self.current_frame
        if not reused and element is None:
            element = self._get_frame(key)
        elif element is not None:
            self.current_key = key
            self.current_frame = element

        if element is not None:
            self.param.update(**self.lookup_options(element, 'plot').options)
        axis = self.handles['axis']

        axes_visible = element is not None or self.overlaid
        axis.xaxis.set_visible(axes_visible and self.xaxis)
        axis.yaxis.set_visible(axes_visible and self.yaxis)
        axis.patch.set_alpha(np.min([int(axes_visible), 1]))

        for hname, handle in self.handles.items():
            hideable = hasattr(handle, 'set_visible')
            if hname not in ['axis', 'fig'] and hideable:
                handle.set_visible(element is not None)
        if element is None:
            return

        ranges = self.compute_ranges(self.hmap, key, ranges)
        ranges = util.match_spec(element, ranges)

        max_cycles = self.style._max_cycles
        style = self.lookup_options(element, 'style')
        self.style = style.max_cycles(max_cycles) if max_cycles else style

        labels = getattr(self, 'legend_labels', {})
        label = element.label if self.show_legend else ''
        style = dict(label=labels.get(label, label), zorder=self.zorder, **self.style[self.cyclic_index])
        axis_kwargs = self.update_handles(key, axis, element, ranges, style)
        self._finalize_axis(key, element=element, ranges=ranges,
                            **(axis_kwargs if axis_kwargs else {}))

    def render_artists(self, element, ranges, style, ax):
        plot_data, plot_kwargs, axis_kwargs = self.get_data(element, ranges, style)
        legend = plot_kwargs.pop('cat_legend', None)
        with abbreviated_exception():
            handles = self.init_artists(ax, plot_data, plot_kwargs)
        if legend and 'artist' in handles and hasattr(handles['artist'], 'legend_elements'):
            legend_handles, _ = handles['artist'].legend_elements()
            leg = ax.legend(legend_handles, legend['factors'],
                            title=legend['title'], **self._legend_opts)
            ax.add_artist(leg)
        return handles, axis_kwargs

    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        element = self.hmap.last
        ax = self.handles['axis']
        key = list(self.hmap.data.keys())[-1]
        dim_map = dict(zip((d.name for d in self.hmap.kdims), key, strict=None))
        key = tuple(dim_map.get(d.name, None) for d in self.dimensions)
        ranges = self.compute_ranges(self.hmap, key, ranges)
        self.current_ranges = ranges
        self.current_frame = element
        self.current_key = key

        ranges = util.match_spec(element, ranges)

        style = dict(zorder=self.zorder, **self.style[self.cyclic_index])
        if self.show_legend:
            style['label'] = element.label
        handles, axis_kwargs = self.render_artists(element, ranges, style, ax)
        self.handles.update(handles)

        trigger = self._trigger
        self._trigger = []
        Stream.trigger(trigger)

        return self._finalize_axis(self.keys[-1], element=element, ranges=ranges,
                                   **axis_kwargs)


    def init_artists(self, ax, plot_args, plot_kwargs):
        """Initializes the artist based on the plot method declared on
        the plot.

        """
        plot_method = self._plot_methods.get('batched' if self.batched else 'single')
        plot_fn = getattr(ax, plot_method)
        if 'norm' in plot_kwargs: # vmin/vmax should now be exclusively in norm
            plot_kwargs.pop('vmin', None)
            plot_kwargs.pop('vmax', None)
        with (warnings.catch_warnings(), np.errstate(invalid="ignore")):
            # scatter have a default cmap and with an empty array will emit this warning
            warnings.filterwarnings('ignore', "No data for colormapping provided via 'c'")
            artist = plot_fn(*plot_args, **plot_kwargs)
        return {'artist': artist[0] if isinstance(artist, list) and
                len(artist) == 1 else artist}


    def update_handles(self, key, axis, element, ranges, style):
        """Update the elements of the plot.

        """
        self.teardown_handles()
        handles, axis_kwargs = self.render_artists(element, ranges, style, axis)
        self.handles.update(handles)
        return axis_kwargs


    def _apply_transforms(self, element, ranges, style):
        new_style = dict(style)
        for k, v in style.items():
            if isinstance(v, (Dimension, str)):
                if validate(k, v) == True:
                    continue
                elif isinstance(element, Graph) and v in element.nodes:
                    v = dim(element.nodes.get_dimension(v))
                elif v in element:
                    v = dim(element.get_dimension(v))
                elif any(d==v for d in self.overlay_dims):
                    v = dim(next(d for d in self.overlay_dims if d==v))

            if not isinstance(v, dim):
                continue
            elif (not v.applies(element) and v.dimension not in self.overlay_dims):
                new_style.pop(k)
                self.param.warning(
                    f'Specified {k} dim transform {v!r} could not be '
                    'applied, as not all dimensions could be resolved.')
                continue

            if v.dimension in self.overlay_dims:
                ds = Dataset({d.name: v for d, v in self.overlay_dims.items()},
                             list(self.overlay_dims))
                val = v.apply(ds, ranges=ranges, flat=True)[0]
            elif type(element) is Path:
                val = np.concatenate([v.apply(el, ranges=ranges, flat=True)
                                      for el in element.split()])
            elif 'node' in k:
                val = v.apply(element.nodes, ranges=ranges)
            else:
                val = v.apply(element, ranges)

            if (not np.isscalar(val) and len(util.unique_array(val)) == 1 and
                ("color" not in k or validate('color', val))):
                val = val[0]
            if isinstance(val, util.arraylike_types):
                val = np.asarray(val)

            if not np.isscalar(val) and k in self._nonvectorized_styles:
                element = type(element).__name__
                raise ValueError(f'Mapping a dimension to the "{k}" '
                                 'style option is not supported by the '
                                 f'{element} element using the {self.renderer.backend} '
                                 f'backend. To map the "{v.dimension}" dimension '
                                 f'to the {k} use a groupby operation '
                                 'to overlay your data along the dimension.')

            style_groups = getattr(self, '_style_groups', [])
            groups = [sg for sg in style_groups if k.startswith(sg)]
            group = groups[0] if groups else None
            prefix = '' if group is None else group+'_'
            if (k in (prefix+'c', prefix+'color') and isinstance(val, util.arraylike_types)
                and not validate('color', val)):
                new_style.pop(k)
                self._norm_kwargs(element, ranges, new_style, v, val, prefix)
                if dtype_kind(val) in 'OSUM':
                    range_key = dim_range_key(v)
                    if range_key in ranges and 'factors' in ranges[range_key]:
                        factors = ranges[range_key]['factors']
                    else:
                        factors = util.unique_array(val)
                    val = util.search_indices(val, factors)
                    labels = getattr(self, 'legend_labels', {})
                    factors = [labels.get(f, f) for f in factors]
                    new_style['cat_legend'] = {
                        'title': v.dimension, 'prop': 'c', 'factors': factors
                    }
                k = prefix+'c'
            new_style[k] = val

        for k, val in list(new_style.items()):
            # If mapped to color/alpha override static fill/line style
            if k == 'c':
                new_style.pop('color', None)

            style_groups = getattr(self, '_style_groups', [])
            groups = [sg for sg in style_groups if k.startswith(sg)]
            group = groups[0] if groups else None
            prefix = '' if group is None else group+'_'

            # Check if element supports fill and line style
            supports_fill = (
                (prefix != 'edge' or getattr(self, 'filled', True))
                and any(o.startswith(prefix+'face') for o in self.style_opts))

            if k in (prefix+'c', prefix+'color') and isinstance(val, util.arraylike_types):
                fill_style = new_style.get(prefix+'facecolor')
                if fill_style and validate('color', fill_style):
                    new_style.pop('facecolor')

                line_style = new_style.get(prefix+'edgecolor')

                # If glyph has fill and line style is set overriding line color
                if supports_fill and line_style is not None:
                    continue

                if line_style and validate('color', line_style):
                    new_style.pop('edgecolor')
            elif k == 'facecolors' and not isinstance(new_style.get('color', new_style.get('c')), np.ndarray):
                # Color overrides facecolors if defined
                new_style.pop('color', None)
                new_style.pop('c', None)

        return new_style


    def teardown_handles(self):
        """If no custom update_handles method is supplied this method
        is called to tear down any previous handles before replacing
        them.

        """
        if 'artist' in self.handles:
            self.handles['artist'].remove()




class ColorbarPlot(ElementPlot):

    clabel = param.String(default=None, doc="""
        An explicit override of the color bar label, if set takes precedence
        over the title key in colorbar_opts.""")

    clim = param.Tuple(default=(np.nan, np.nan), length=2, doc="""
        User-specified colorbar axis range limits for the plot, as a
        tuple (low,high). If specified, takes precedence over data
        and dimension ranges.""")

    clim_percentile = param.ClassSelector(default=False, class_=(int, float, bool), doc="""
        Percentile value to compute colorscale robust to outliers. If
        True, uses 2nd and 98th percentile; otherwise uses the specified
        numerical percentile value.""")

    cformatter = param.ClassSelector(
        default=None, class_=(str, ticker.Formatter, FunctionType), doc="""
        Formatter for ticks along the colorbar axis.""")

    colorbar = param.Boolean(default=False, doc="""
        Whether to draw a colorbar.""")

    colorbar_opts = param.Dict(default={}, doc="""
        Allows setting specific styling options for the colorbar.""")

    color_levels = param.ClassSelector(default=None, class_=(int, list), doc="""
        Number of discrete colors to use when colormapping or a set of color
        intervals defining the range of values to map each color to.""")

    cnorm = param.Selector(default='linear', objects=['linear', 'log', 'eq_hist'], doc="""
        Color normalization to be applied during colormapping.""")

    clipping_colors = param.Dict(default={}, doc="""
        Dictionary to specify colors for clipped values, allows
        setting color for NaN values and for values above and below
        the min and max value. The min, max or NaN color may specify
        an RGB(A) color as a color hex string of the form #FFFFFF or
        #FFFFFFFF or a length 3 or length 4 tuple specifying values in
        the range 0-1 or a named HTML color.""")

    cbar_padding = param.Number(default=0.01, doc="""
        Padding between colorbar and other plots.""")

    cbar_ticks = param.Parameter(default=None, doc="""
        Ticks along colorbar-axis specified as an integer, explicit
        list of tick locations, list of tuples containing the
        locations and labels or a matplotlib tick locator object. If
        set to None default matplotlib ticking behavior is
        applied.""")

    cbar_width = param.Number(default=0.05, doc="""
        Width of the colorbar as a fraction of the main plot""")

    cbar_extend = param.Selector(
        objects=['neither', 'both', 'min', 'max'], default=None, doc="""
        If not 'neither', make pointed end(s) for out-of- range values."""
    )

    rescale_discrete_levels = param.Boolean(default=True, doc="""
        If ``cnorm='eq_hist`` and there are only a few discrete values,
        then ``rescale_discrete_levels=True`` decreases the lower
        limit of the autoranged span so that the values are rendering
        towards the (more visible) top of the palette, thus
        avoiding washout of the lower values.  Has no effect if
        ``cnorm!=`eq_hist``. Set this value to False if you need to
        match historical unscaled behavior, prior to HoloViews 1.14.4.""")

    symmetric = param.Boolean(default=False, doc="""
        Whether to make the colormap symmetric around zero.""")

    _colorbars = {}

    _default_nan = '#8b8b8b'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _adjust_cbar(self, cbar, label, dim):
        noalpha = math.floor(self.style[self.cyclic_index].get('alpha', 1)) == 1

        for lb in ['clabel', 'labels']:
            labelsize = self._fontsize(lb, common=False).get('fontsize')
            if labelsize is not None:
                break

        if (cbar.solids and noalpha):
            cbar.solids.set_edgecolor("face")
        cbar.set_label(label, fontsize=labelsize)
        if isinstance(self.cbar_ticks, ticker.Locator):
            cbar.ax.yaxis.set_major_locator(self.cbar_ticks)
        elif self.cbar_ticks == 0:
            cbar.set_ticks([])
        elif isinstance(self.cbar_ticks, int):
            locator = ticker.MaxNLocator(self.cbar_ticks)
            cbar.ax.yaxis.set_major_locator(locator)
        elif isinstance(self.cbar_ticks, list):
            if all(isinstance(t, tuple) for t in self.cbar_ticks):
                ticks, labels = zip(*self.cbar_ticks, strict=None)
            else:
                ticks, labels = zip(*[(t, dim.pprint_value(t))
                                        for t in self.cbar_ticks], strict=None)
            cbar.set_ticks(ticks)
            cbar.set_ticklabels(labels)

        for tk in ['cticks', 'ticks']:
            ticksize = self._fontsize(tk, common=False).get('fontsize')
            if ticksize is not None:
                cbar.ax.tick_params(labelsize=ticksize)
                break


    def _finalize_artist(self, element):
        if self.colorbar:
            dims = [h for k, h in self.handles.items() if k.endswith('color_dim')]
            for d in dims:
                self._draw_colorbar(element, d)


    def _draw_colorbar(self, element=None, dimension=None, redraw=True):
        if element is None:
            element = self.hmap.last
        artist = self.handles.get('artist', None)
        fig = self.handles['fig']
        axis = self.handles['axis']
        ax_colorbars, position = ColorbarPlot._colorbars.get(id(axis), ([], None))
        specs = [spec[:2] for _, _, spec, _ in ax_colorbars]
        spec = util.get_spec(element)

        if position is None or not redraw:
            if redraw:
                fig.canvas.draw()
            bbox = axis.get_position()
            l, b, w, h = bbox.x0, bbox.y0, bbox.width, bbox.height
        else:
            l, b, w, h = position

        # Get colorbar label
        if isinstance(dimension, dim):
            dimension = dimension.dimension
        dimension = element.get_dimension(dimension)
        if self.clabel is not None:
            label = self.clabel
        elif dimension:
            label = dimension.pprint_label
        elif element.vdims:
            label = element.vdims[0].pprint_label
        elif dimension is None:
            label = ''

        padding = self.cbar_padding
        width = self.cbar_width
        if spec[:2] not in specs:
            offset = len(ax_colorbars)
            scaled_w = w*width
            cax = fig.add_axes([l+w+padding+(scaled_w+padding+w*0.15)*offset,
                                b, scaled_w, h])
            cbar = fig.colorbar(artist, cax=cax, ax=axis,
                extend=self.cbar_extend, **self.colorbar_opts)
            self._set_axis_formatter(cbar.ax.yaxis, dimension, self.cformatter)
            self._adjust_cbar(cbar, label, dimension)
            self.handles['cax'] = cax
            self.handles['cbar'] = cbar
            ylabel = cax.yaxis.get_label()
            self.handles['bbox_extra_artists'] += [cax, ylabel]
            ax_colorbars.append((artist, cax, spec, label))

        for i, (_artist, cax, _spec, _label) in enumerate(ax_colorbars):
            scaled_w = w*width
            cax.set_position([l+w+padding+(scaled_w+padding+w*0.15)*i,
                              b, scaled_w, h])

        ColorbarPlot._colorbars[id(axis)] = (ax_colorbars, (l, b, w, h))


    def _norm_kwargs(self, element, ranges, opts, vdim, values=None, prefix=''):
        """Returns valid color normalization kwargs
        to be passed to matplotlib plot function.

        """
        dim_name = dim_range_key(vdim)
        if values is None:
            if isinstance(vdim, dim):
                values = vdim.apply(element, flat=True)
            else:
                expanded = not (
                    isinstance(element, Dataset) and
                    element.interface.multi and
                    (getattr(element, 'level', None) is not None or
                    element.interface.isunique(element, vdim.name, True))
                )
                values = np.asarray(element.dimension_values(vdim, expanded=expanded))

        # Store dimension being colormapped for colorbars
        if prefix+'color_dim' not in self.handles:
            self.handles[prefix+'color_dim'] = vdim

        clim = opts.pop(prefix+'clims', None)

        # check if there's an actual value (not np.nan)
        if clim is None and self.clim is not None and any(util.isfinite(cl) for cl in self.clim):
            clim = self.clim

        if clim is None:
            if not len(values):
                clim = (0, 0)
                categorical = False
            elif dtype_kind(values) in 'uif':
                if dim_name in ranges:
                    if self.clim_percentile and 'robust' in ranges[dim_name]:
                        clim = ranges[dim_name]['robust']
                    else:
                        clim = ranges[dim_name]['combined']
                elif isinstance(vdim, dim):
                    if dtype_kind(values) == 'M':
                        clim = values.min(), values.max()
                    elif len(values) == 0:
                        clim = np.nan, np.nan
                    else:
                        try:
                            with warnings.catch_warnings():
                                warnings.filterwarnings('ignore', r'All-NaN (slice|axis) encountered')
                                clim = (np.nanmin(values), np.nanmax(values))
                        except Exception:
                            clim = np.nan, np.nan
                else:
                    clim = element.range(vdim)
                if self.logz:
                    # Lower clim must be >0 when logz=True
                    # Choose the maximum between the lowest non-zero value
                    # and the overall range
                    if clim[0] == 0:
                        clim = (values[values!=0].min(), clim[1])
                if self.symmetric:
                    clim = -np.abs(clim).max(), np.abs(clim).max()
                categorical = False
            else:
                range_key = dim_range_key(vdim)
                if range_key in ranges and 'factors' in ranges[range_key]:
                    factors = ranges[range_key]['factors']
                else:
                    factors = util.unique_array(values)
                clim = (0, len(factors)-1)
                categorical = True
        else:
            categorical = dtype_kind(values) not in 'uif'

        if self.cnorm == 'eq_hist':
            opts[prefix+'norm'] = EqHistNormalize(
                vmin=clim[0], vmax=clim[1],
                rescale_discrete_levels=self.rescale_discrete_levels
            )
        if self.cnorm == 'log' or self.logz:
            if self.symmetric:
                norm = mpl_colors.SymLogNorm(vmin=clim[0], vmax=clim[1],
                                             linthresh=clim[1]/np.e)
            else:
                norm = mpl_colors.LogNorm(vmin=clim[0], vmax=clim[1])
            opts[prefix+'norm'] = norm
        opts[prefix+'vmin'] = clim[0]
        opts[prefix+'vmax'] = clim[1]

        cmap = opts.get(prefix+'cmap', opts.get('cmap', 'viridis'))
        if dtype_kind(values) not in 'OSUM':
            ncolors = None
            if isinstance(self.color_levels, int):
                ncolors = self.color_levels
            elif isinstance(self.color_levels, list):
                ncolors = len(self.color_levels) - 1
                if isinstance(cmap, list) and len(cmap) != ncolors:
                    raise ValueError('The number of colors in the colormap '
                                     'must match the intervals defined in the '
                                     f'color_levels, expected {ncolors} colors found {len(cmap)}.')
            try:
                el_min, el_max = np.nanmin(values), np.nanmax(values)
            except ValueError:
                el_min, el_max = -np.inf, np.inf
        else:
            ncolors = clim[-1]+1
            el_min, el_max = -np.inf, np.inf
        vmin = -np.inf if opts[prefix+'vmin'] is None else opts[prefix+'vmin']
        vmax = np.inf if opts[prefix+'vmax'] is None else opts[prefix+'vmax']
        if self.cbar_extend is None:
            if el_min < vmin and el_max > vmax:
                self.cbar_extend = 'both'
            elif el_min < vmin:
                self.cbar_extend = 'min'
            elif el_max > vmax:
                self.cbar_extend = 'max'
            else:
                self.cbar_extend = 'neither'

        # Define special out-of-range colors on colormap
        colors = {}
        for k, val in self.clipping_colors.items():
            if val == 'transparent':
                colors[k] = {'color': 'w', 'alpha': 0}
            elif isinstance(val, tuple):
                colors[k] = {'color': val[:3],
                             'alpha': val[3] if len(val) > 3 else 1}
            elif isinstance(val, str):
                color = val
                alpha = 1
                if color.startswith('#') and len(color) == 9:
                    alpha = int(color[-2:], 16)/255.
                    color = color[:-2]
                colors[k] = {'color': color, 'alpha': alpha}

        if not isinstance(cmap, mpl_colors.Colormap):
            if isinstance(cmap, dict):
                # The palette needs to correspond to the map's limits (vmin/vmax). So use the same
                # factors as in the map's clim computation above.
                range_key = dim_range_key(vdim)
                if range_key in ranges and 'factors' in ranges[range_key]:
                    factors = ranges[range_key]['factors']
                else:
                    factors = util.unique_array(values)
                palette = [cmap.get(f, colors.get('NaN', {'color': self._default_nan})['color'])
                           for f in factors]
            else:
                palette = process_cmap(cmap, ncolors, categorical=categorical)
                if isinstance(self.color_levels, list):
                    palette, (vmin, vmax) = color_intervals(palette, self.color_levels, clip=(vmin, vmax))
            cmap = mpl_colors.ListedColormap(palette)

        cmap = copy.copy(cmap)
        if 'max' in colors: cmap.set_over(**colors['max'])
        if 'min' in colors: cmap.set_under(**colors['min'])
        if 'NaN' in colors: cmap.set_bad(**colors['NaN'])
        opts[prefix+'cmap'] = cmap


class LegendPlot(ElementPlot):

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    legend_cols = param.Integer(default=None, doc="""
       Number of legend columns in the legend.""")

    legend_labels = param.Dict(default={}, doc="""
        A mapping that allows overriding legend labels.""")

    legend_position = param.Selector(objects=['inner', 'right',
                                                    'bottom', 'top',
                                                    'left', 'best',
                                                    'top_right',
                                                    'top_left',
                                                    'bottom_left',
                                                    'bottom_right'],
                                           default='inner', doc="""
        Allows selecting between a number of predefined legend position
        options. The predefined options may be customized in the
        legend_specs class attribute. By default, 'inner', 'right',
        'bottom', 'top', 'left', 'best', 'top_right', 'top_left',
        'bottom_right' and 'bottom_left' are supported.""")

    legend_opts = param.Dict(default={}, doc="""
        Allows setting specific styling options for the colorbar.""")

    legend_specs = {'inner': {},
                    'best': {},
                    'left':   dict(bbox_to_anchor=(-.15, 1), loc=1),
                    'right':  dict(bbox_to_anchor=(1.05, 1), loc=2),
                    'top':    dict(bbox_to_anchor=(0., 1.02, 1., .102),
                                   ncol=3, loc=3, mode="expand", borderaxespad=0.),
                    'bottom': dict(ncol=3, mode="expand", loc=2,
                                   bbox_to_anchor=(0., -0.25, 1., .102),
                                   borderaxespad=0.1),
                    'top_right': dict(loc=1),
                    'top_left': dict(loc=2),
                    'bottom_left': dict(loc=3),
                    'bottom_right': dict(loc=4)}

    @property
    def _legend_opts(self):
        leg_spec = self.legend_specs[self.legend_position]
        if self.legend_cols: leg_spec['ncol'] = self.legend_cols
        legend_opts = self.legend_opts.copy()
        legend_opts.update(**dict(leg_spec, **self._fontsize('legend')))
        return legend_opts


class OverlayPlot(LegendPlot, GenericOverlayPlot):
    """OverlayPlot supports compositors processing of Overlays across maps.

    """

    _passed_handles = ['fig', 'axis']

    _propagate_options = ['aspect', 'fig_size', 'xaxis', 'yaxis', 'zaxis',
                          'labelled', 'bgcolor', 'fontsize', 'gridstyle', 'invert_axes',
                          'show_frame', 'show_grid', 'logx', 'logy', 'logz',
                          'xticks', 'yticks', 'zticks', 'xrotation', 'yrotation',
                          'zrotation', 'invert_xaxis', 'invert_yaxis',
                          'invert_zaxis', 'title', 'title_format', 'padding',
                          'xlabel', 'ylabel', 'zlabel', 'xlim', 'ylim', 'zlim',
                          'xformatter', 'yformatter', 'data_aspect', 'fontscale',
                          'legend_opts']

    def __init__(self, overlay, ranges=None, **params):
        if 'projection' not in params:
            params['projection'] = self._get_projection(overlay)
        super().__init__(overlay, ranges=ranges, **params)

    def _finalize_artist(self, element):
        for subplot in self.subplots.values():
            subplot._finalize_artist(element)

    def _adjust_legend(self, overlay, axis):
        """Accumulate the legend handles and labels for all subplots
        and set up the legend

        """
        legend_data = []
        legend_plot = True
        dimensions = overlay.kdims
        title = ', '.join([d.label for d in dimensions])
        labels = self.legend_labels
        for key, subplot in self.subplots.items():
            element = overlay.data.get(key, False)
            if not subplot.show_legend or not element: continue
            title = ', '.join([d.name for d in dimensions])
            handle = subplot.traverse(lambda p: p.handles['artist'],
                                      [lambda p: 'artist' in p.handles])
            if getattr(subplot, '_legend_plot', None) is not None:
                legend_plot = True
            elif isinstance(overlay, NdOverlay):
                label = ','.join([dim.pprint_value(k, print_unit=True)
                                  for k, dim in zip(key, dimensions, strict=None)])
                if handle:
                    legend_data.append((handle, label))
            elif isinstance(subplot, OverlayPlot):
                legend_data += subplot.handles.get('legend_data', {}).items()
            elif element.label and handle:
                legend_data.append((handle, labels.get(element.label, element.label)))
        all_handles, all_labels = list(zip(*legend_data, strict=None)) if legend_data else ([], [])
        data = {}
        used_labels = []
        for handle, label in zip(all_handles, all_labels, strict=None):
            # Ensure that artists with multiple handles are supported
            if isinstance(handle, list): handle = tuple(handle)
            handle = tuple(h for h in handle if not isinstance(h, (AxesImage, list)))
            if not handle:
                continue
            if handle and (handle not in data) and label and label not in used_labels:
                data[handle] = label
                used_labels.append(label)
        if (not len(set(data.values())) > 0) or not self.show_legend:
            legend = axis.get_legend()
            if legend and not (legend_plot or self.show_legend):
                legend.set_visible(False)
        else:
            leg = axis.legend(list(data.keys()), list(data.values()),
                              title=title, **self._legend_opts)
            title_fontsize = self._fontsize('legend_title')
            if title_fontsize:
                leg.get_title().set_fontsize(title_fontsize['fontsize'])
            leg.set_zorder(10e6)
            self.handles['legend'] = leg
            self.handles['bbox_extra_artists'].append(leg)
        self.handles['legend_data'] = data

    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        axis = self.handles['axis']
        key = self.keys[-1]
        element = self._get_frame(key)

        ranges = self.compute_ranges(self.hmap, key, ranges)
        for k, subplot in self.subplots.items():
            subplot.initialize_plot(ranges=ranges)
            if isinstance(element, CompositeOverlay):
                frame = element.get(k, None)
                subplot.current_frame = frame

        if self.show_legend and element is not None:
            self._adjust_legend(element, axis)

        return self._finalize_axis(key, element=element, ranges=ranges,
                                   title=self._format_title(key))

    @mpl_rc_context
    def update_frame(self, key, ranges=None, element=None):
        axis = self.handles['axis']
        reused = isinstance(self.hmap, DynamicMap) and self.overlaid
        self.prev_frame =  self.current_frame
        if element is None and not reused:
            element = self._get_frame(key)
        elif element is not None:
            self.current_frame = element
            self.current_key = key
        empty = element is None

        if isinstance(self.hmap, DynamicMap):
            range_obj = element
        else:
            range_obj = self.hmap
        items = [] if element is None else list(element.data.items())

        if not empty:
            ranges = self.compute_ranges(range_obj, key, ranges)
        for k, subplot in self.subplots.items():
            el = None if empty else element.get(k, None)
            if isinstance(self.hmap, DynamicMap) and not empty:
                idx, spec, exact = self._match_subplot(k, subplot, items, element)
                if idx is not None:
                    _, el = items.pop(idx)
                    if not exact:
                        self._update_subplot(subplot, spec)
            subplot.update_frame(key, ranges, el)

        if isinstance(self.hmap, DynamicMap) and items:
            self._create_dynamic_subplots(key, items, ranges)

        # Update plot options
        plot_opts = self.lookup_options(element, 'plot').options
        inherited = self._traverse_options(element, 'plot',
                                           self._propagate_options,
                                           defaults=False)
        plot_opts.update(**{k: v[0] for k, v in inherited.items()
                            if k not in plot_opts})
        self.param.update(**plot_opts)

        if self.show_legend and not empty:
            self._adjust_legend(element, axis)

        self._finalize_axis(key, element=element, ranges=ranges)
