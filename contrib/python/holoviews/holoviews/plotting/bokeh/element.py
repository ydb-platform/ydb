import base64
import warnings
from collections import defaultdict
from itertools import chain
from textwrap import dedent
from types import FunctionType

import bokeh
import bokeh.plotting
import numpy as np
import param
from bokeh.document.events import ModelChangedEvent
from bokeh.model import Model
from bokeh.models import (
    BinnedTicker,
    ColorBar,
    ColorMapper,
    CustomAction,
    CustomJS,
    EqHistColorMapper,
    GlyphRenderer,
    Legend,
    Renderer,
    Span,
    Title,
    tools,
)
from bokeh.models.axes import CategoricalAxis, DatetimeAxis
from bokeh.models.dom import Div
from bokeh.models.formatters import (
    CustomJSTickFormatter,
    MercatorTickFormatter,
    TickFormatter,
)
from bokeh.models.layouts import TabPanel, Tabs
from bokeh.models.mappers import (
    CategoricalColorMapper,
    LinearColorMapper,
    LogColorMapper,
)
from bokeh.models.ranges import DataRange1d, FactorRange, Range1d
from bokeh.models.scales import LogScale
from bokeh.models.tickers import (
    BasicTicker,
    FixedTicker,
    LogTicker,
    MercatorTicker,
    Ticker,
)
from bokeh.models.tools import Tool

from ...core import Dataset, Dimension, DynamicMap, Element, util
from ...core.options import Keywords, SkipRendering, abbreviated_exception
from ...core.overlay import CompositeOverlay, NdOverlay
from ...core.util import dtype_kind
from ...element import Annotation, Contours, Graph, Path, Tiles, VectorField
from ...streams import Buffer, PlotSize, RangeXY
from ...util.transform import dim
from ...util.warnings import warn
from ..plot import GenericElementPlot, GenericOverlayPlot
from ..util import color_intervals, dim_axis_label, dim_range_key, process_cmap
from .plot import BokehPlot
from .styles import (
    base_properties,
    legend_dimensions,
    line_properties,
    mpl_to_bokeh,
    property_prefixes,
    rgba_tuple,
    text_properties,
    validate,
)
from .tabular import TablePlot
from .util import (
    BOKEH_GE_3_2_0,
    BOKEH_GE_3_4_0,
    BOKEH_GE_3_5_0,
    BOKEH_GE_3_6_0,
    BOKEH_GE_3_7_0,
    BOKEH_GE_3_8_0,
    TOOL_TYPES,
    cds_column_replace,
    compute_layout_properties,
    date_to_integer,
    decode_bytes,
    dtype_fix_hook,
    get_axis_class,
    get_scale,
    get_tab_title,
    get_ticker_axis_props,
    glyph_order,
    hold_policy,
    hold_render,
    match_ax_type,
    match_dim_specs,
    match_yaxis_type_to_range,
    prop_is_none,
    property_to_dict,
    recursive_model_update,
    remove_legend,
    theme_attr_json,
    wrap_formatter,
)

if BOKEH_GE_3_8_0:
    from bokeh.models.axes import TimedeltaAxis

try:
    TOOLS_MAP = Tool._known_aliases
except Exception:
    TOOLS_MAP = TOOL_TYPES


class ElementPlot(BokehPlot, GenericElementPlot):

    active_tools = param.List(default=None, doc="""
        Allows specifying which tools are active by default. Note
        that only one tool per gesture type can be active, e.g.
        both 'pan' and 'box_zoom' are drag tools, so if both are
        listed only the last one will be active. As a default 'pan'
        and 'wheel_zoom' will be used if the tools are enabled.""")

    align = param.Selector(default='start', objects=['start', 'center', 'end'], doc="""
        Alignment (vertical or horizontal) of the plot in a layout.""")

    apply_hard_bounds = param.Boolean(default=False, doc="""
        If True, the navigable bounds of the plot will be set based
        on the more extreme of extents between the data or xlim/ylim ranges.
        If dim ranges are set, the hard bounds will be set to the dim ranges.""")

    autorange = param.Selector(default=None, objects=['x', 'y', None], doc="""
        Whether to auto-range along either the x- or y-axis, i.e.
        when panning or zooming along the orthogonal axis it will
        ensure all the data along the selected axis remains visible.""")

    border = param.Number(default=10, doc="""
        Minimum border around plot.""")

    aspect = param.Parameter(default=None, doc="""
        The aspect ratio mode of the plot. By default, a plot may
        select its own appropriate aspect ratio but sometimes it may
        be necessary to force a square aspect ratio (e.g. to display
        the plot as an element of a grid). The modes 'auto' and
        'equal' correspond to the axis modes of the same name in
        matplotlib, a numeric value specifying the ratio between plot
        width and height may also be passed. To control the aspect
        ratio between the axis scales use the data_aspect option
        instead.""")

    backend_opts = param.Dict(default={}, doc="""
        A dictionary of custom options to apply to the plot or
        subcomponents of the plot. The keys in the dictionary mirror
        attribute access on the underlying models stored in the plot's
        handles, e.g. {'colorbar.margin': 10} will index the colorbar
        in the Plot.handles and then set the margin to 10.""")

    data_aspect = param.Number(default=None, doc="""
        Defines the aspect of the axis scaling, i.e. the ratio of
        y-unit to x-unit.""")

    width = param.Integer(default=300, allow_None=True, bounds=(0, None), doc="""
        The width of the component (in pixels). This can be either
        fixed or preferred width, depending on width sizing policy.""")

    height = param.Integer(default=300, allow_None=True, bounds=(0, None), doc="""
        The height of the component (in pixels).  This can be either
        fixed or preferred height, depending on height sizing policy.""")

    frame_width = param.Integer(default=None, allow_None=True, bounds=(0, None), doc="""
        The width of the component (in pixels). This can be either
        fixed or preferred width, depending on width sizing policy.""")

    frame_height = param.Integer(default=None, allow_None=True, bounds=(0, None), doc="""
        The height of the component (in pixels).  This can be either
        fixed or preferred height, depending on height sizing policy.""")

    min_width = param.Integer(default=None, bounds=(0, None), doc="""
        Minimal width of the component (in pixels) if width is adjustable.""")

    min_height = param.Integer(default=None, bounds=(0, None), doc="""
        Minimal height of the component (in pixels) if height is adjustable.""")

    max_width = param.Integer(default=None, bounds=(0, None), doc="""
        Minimal width of the component (in pixels) if width is adjustable.""")

    max_height = param.Integer(default=None, bounds=(0, None), doc="""
        Minimal height of the component (in pixels) if height is adjustable.""")

    margin = param.Parameter(default=None, doc="""
        Allows to create additional space around the component. May
        be specified as a two-tuple of the form (vertical, horizontal)
        or a four-tuple (top, right, bottom, left).""")

    multi_y = param.Boolean(default=False, doc="""
       Enables multiple axes (one per value dimension) in
       overlays and useful for creating twin-axis plots.

       When enabled, axis options are no longer propagated between the
       elements and the overlay container, allowing customization on a
       per-axis basis.""")

    scalebar = param.Boolean(default=False, doc="""
        Whether to display a scalebar.""")

    scalebar_range = param.Selector(default="x", objects=["x", "y"], doc="""
        Whether to have the scalebar on the x or y axis.""")

    scalebar_unit = param.ClassSelector(default=None, class_=(str, tuple), doc="""
        Unit of the scalebar. The order of how this will be done is by:

        1. This value if it is set.
        2. The elements kdim unit (if exist).
        3. Meter

        If the value is a tuple, the first value will be the unit and the
        second will be the base unit.

        The scalebar_unit is only used if scalebar is True.""")

    scalebar_location = param.Selector(
        default=None,
        objects=[
            "top_left", "top_center", "top_right",
            "center_left", "center_center", "center_right",
            "bottom_left", "bottom_center", "bottom_right",
            "top", "left", "center", "right","bottom"
        ],
        doc="""
            Location anchor for positioning scale bar.

            Default to 'bottom_right', except if subcoordinate_y is True then it will default to 'right'.

            The scalebar_location is only used if scalebar is True.""")

    scalebar_label = param.String(
        default="@{value} @{unit}", doc="""
        The label template.

        This can use special variables:
        * ``@{value}`` The current value. Optionally can provide a number
            formatter with e.g. ``@{value}{%.2f}``.
        * ``@{unit}`` The unit of measure.

        The scalebar_label is only used if scalebar is True.""")

    scalebar_tool = param.Boolean(default=True, doc="""
        Whether to show scalebar tools in the toolbar,
        the tools are used to control scalebars visibility.

        The scalebar_tool is only used if scalebar is True.""")

    scalebar_opts = param.Dict(
        default={}, doc="""
        Allows setting specific styling options for the scalebar.
        See https://docs.bokeh.org/en/latest/docs/reference/models/annotations.html#bokeh.models.ScaleBar
        for more information.

        The scalebar_opts is only used if scalebar is True.""")

    subcoordinate_y = param.ClassSelector(default=False, class_=(bool, tuple), doc="""
       Enables sub-coordinate systems for this plot. Accepts also a numerical
       two-tuple that must be a range between 0 and 1, the plot will be
       rendered on this vertical range of the axis.""")

    subcoordinate_scale = param.Number(default=1, bounds=(0, None), inclusive_bounds=(False, True), doc="""
       Scale factor for subcoordinate ranges to control the level of overlap.""")

    responsive = param.Selector(default=False, objects=[False, True, 'width', 'height'])

    fontsize = param.Parameter(default={'title': '12pt'}, allow_None=True,  doc="""
       Specifies various fontsizes of the displayed text.

       Finer control is available by supplying a dictionary where any
       unmentioned keys reverts to the default sizes, e.g:

          {'ticks': '20pt', 'title': '15pt', 'ylabel': '5px', 'xlabel': '5px'}""")

    gridstyle = param.Dict(default={}, doc="""
        Allows customizing the grid style, e.g. grid_line_color defines
        the line color for both grids while xgrid_line_color exclusively
        customizes the x-axis grid lines.""")

    labelled = param.List(default=['x', 'y'], doc="""
        Whether to plot the 'x' and 'y' labels.""")

    lod = param.Dict(default={'factor': 10, 'interval': 300,
                              'threshold': 2000, 'timeout': 500}, doc="""
        Bokeh plots offer "Level of Detail" (LOD) capability to
        accommodate large (but not huge) amounts of data. The available
        options are:

          * factor    : Decimation factor to use when applying
                        decimation.
          * interval  : Interval (in ms) downsampling will be enabled
                        after an interactive event.
          * threshold : Number of samples before downsampling is enabled.
          * timeout   : Timeout (in ms) for checking whether interactive
                        tool events are still occurring.""")

    show_frame = param.Boolean(default=True, doc="""
        Whether or not to show a complete frame around the plot.""")

    shared_axes = param.Boolean(default=True, doc="""
        Whether to invert the share axes across plots
        for linked panning and zooming.""")

    default_tools = param.List(default=['save', 'pan', 'wheel_zoom',
                                        'auto_box_zoom', 'reset'],
        doc="A list of plugin tools to use on the plot.")

    tools = param.List(default=[], doc="""
        A list of plugin tools to use on the plot.""")

    hover_tooltips = param.ClassSelector(class_=(list, str), doc="""
        A list of dimensions to be displayed in the hover tooltip.""")

    hover_formatters = param.Dict(doc="""
        A dict of formatting options for the hover tooltip.""")

    hover_mode = param.Selector(default='mouse', objects=['mouse', 'vline', 'hline'], doc="""
        The hover mode determines how the hover tool is activated.""")

    xticks = param.ClassSelector(class_=(int, list, tuple, np.ndarray, Ticker), default=None, doc="""
        Ticks along x-axis specified as an integer, explicit list of
        tick locations, or bokeh Ticker object. If set to None default
        bokeh ticking behavior is applied.""")

    yticks = param.ClassSelector(class_=(int, list, tuple, np.ndarray, Ticker), default=None, doc="""
        Ticks along y-axis specified as an integer, explicit list of
        tick locations, or bokeh Ticker object. If set to None default
        bokeh ticking behavior is applied.""")

    toolbar = param.Selector(default='right',
                                   objects=["above", "below",
                                            "left", "right", "disable", None],
                                   doc="""
        The toolbar location, must be one of 'above', 'below',
        'left', 'right', None.""")

    xformatter = param.ClassSelector(
        default=None, class_=(str, TickFormatter, FunctionType), doc="""
        Formatter for ticks along the x-axis.""")

    yformatter = param.ClassSelector(
        default=None, class_=(str, TickFormatter, FunctionType), doc="""
        Formatter for ticks along the x-axis.""")

    _categorical = False
    _allow_implicit_categories = True

    # Declare which styles cannot be mapped to a non-scalar dimension
    _nonvectorized_styles = []

    # Declares the default types for continuous x- and y-axes
    _x_range_type = Range1d
    _y_range_type = Range1d

    # Whether the plot supports streaming data
    _stream_data = True

    def __init__(self, element, plot=None, **params):
        self._subcoord_standalone_ = None
        self.current_ranges = None
        super().__init__(element, **params)
        self.handles = {} if plot is None else self.handles['plot']
        self.static = len(self.hmap) == 1 and len(self.keys) == len(self.hmap)
        if isinstance(self, GenericOverlayPlot):
            self.callbacks, self.source_streams = [], []
        else:
            self.callbacks, self.source_streams = self._construct_callbacks()
        self.static_source = False
        self.streaming = [s for s in self.streams if isinstance(s, Buffer)]
        self.geographic = bool(self.hmap.last.traverse(lambda x: x, Tiles))
        if self.geographic and self.projection is None:
            self.projection = 'mercator'

        # Whether axes are shared between plots
        self._shared = {'x-main-range': False, 'y-main-range': False}
        self._js_on_data_callbacks = []

        # Flag to check whether plot has been updated
        self._updated = False
        # Counter to keep track of last stream update
        self._stream_count = None

    def _hover_opts(self, element):
        if self.batched:
            dims = list(self.hmap.last.kdims)
        else:
            dims = list(self.overlay_dims.keys())
        dims += element.dimensions()
        return list(util.unique_iterator(dims)), {}

    def _replace_hover_label_group(self, element, tooltip):
        if isinstance(tooltip, tuple):
            has_label = hasattr(element, 'label') and element.label
            has_group = hasattr(element, 'group') and element.group != element.param.group.default
            if not has_label and not has_group:
                return tooltip

            if ("$label" in tooltip or "${label}" in tooltip):
                tooltip = (tooltip[0], element.label)
            elif ("$group" in tooltip or "${group}" in tooltip):
                tooltip = (tooltip[0], element.group)
        elif isinstance(tooltip, str):
            if "$label" in tooltip:
                tooltip = tooltip.replace("$label", element.label)
            elif "${label}" in tooltip:
                tooltip = tooltip.replace("${label}", element.label)

            if "$group" in tooltip:
                tooltip = tooltip.replace("$group", element.group)
            elif "${group}" in tooltip:
                tooltip = tooltip.replace("${group}", element.group)
        return tooltip

    def _replace_hover_value_aliases(self, tooltip, tooltips_dict):
        for name, tuple_ in tooltips_dict.items():
            # some elements, like image, rename the tooltip, e.g. @y -> $y
            # let's replace those, so the hover tooltip is discoverable
            # ensure it works for `(@x, @y)` -> `($x, $y)` too
            if isinstance(tooltip, tuple):
                value_alias = tuple_[1]
                if f"@{name}" in tooltip[1]:
                    tooltip = (tooltip[0], tooltip[1].replace(f"@{name}", value_alias))
                elif f"@{{{name}}}" in tooltip[1]:
                    tooltip = (tooltip[0], tooltip[1].replace(f"@{{{name}}}", value_alias))
            elif isinstance(tooltip, str):
                if f"@{name}" in tooltip:
                    tooltip = tooltip.replace(f"@{name}", tuple_[1])
                elif f"@{{{name}}}" in tooltip:
                    tooltip = tooltip.replace(f"@{{{name}}}", tuple_[1])
        return tooltip

    def _prepare_hover_kwargs(self, element):
        tooltips, hover_opts = self._hover_opts(element)

        dim_aliases = {
            f"{dim.label} ({dim.unit})" if dim.unit else dim.label: dim.label
            for dim in element.kdims + element.vdims
        }

        # make dict so it's easy to get the tooltip for a given dimension;
        tooltips_dict = {}
        units_dict = {}
        for ttp in tooltips:
            if isinstance(ttp, tuple):
                label = ttp[0]
                tuple_ = (ttp[0], ttp[1])
            elif isinstance(ttp, Dimension):
                label = ttp.label
                # three brackets means replacing variable,
                # and then wrapping in brackets, like @{air}
                unit = f" ({ttp.unit})" if ttp.unit else ""
                tuple_ = (
                    ttp.pprint_label,
                    f"@{{{util.dimension_sanitizer(ttp.name)}}}"
                )
                units_dict[label] = unit
            elif isinstance(ttp, str):
                label = ttp
                # three brackets means replacing variable,
                # and then wrapping in brackets, like @{air}
                tuple_ = (ttp, f"@{{{util.dimension_sanitizer(ttp)}}}")

            if label in dim_aliases:
                label = dim_aliases[label]

            # key is the vanilla data column/dimension name
            # value should always be a tuple (label, value)
            tooltips_dict[label] = tuple_

        # subset the tooltips to only the ones user wants
        if self.hover_tooltips:
            # If hover tooltips are defined as a list of strings or tuples
            if isinstance(self.hover_tooltips, list):
                new_tooltips = []
                for tooltip in self.hover_tooltips:
                    if isinstance(tooltip, str):
                        # make into a tuple
                        new_tooltip = tooltips_dict.get(tooltip.lstrip("@"))
                        if new_tooltip is None:
                            label = tooltip.lstrip("$").lstrip("@")
                            value = tooltip if "$" in tooltip else f"@{{{tooltip.lstrip('@')}}}"
                            new_tooltip = (label, value)
                        new_tooltips.append(new_tooltip)
                    elif isinstance(tooltip, tuple):
                        unit = units_dict.get(tooltip[0])
                        tooltip = self._replace_hover_value_aliases(tooltip, tooltips_dict)
                        if unit:
                            tooltip = (f"{tooltip[0]}{unit}", tooltip[1])
                        new_tooltips.append(tooltip)
                    else:
                        raise ValueError('Hover tooltips must be a list with items of strings or tuples.')
                tooltips = new_tooltips
            else:
                # Likely HTML str
                tooltips = self._replace_hover_value_aliases(self.hover_tooltips, tooltips_dict)
        else:
            tooltips = list(tooltips_dict.values())

        # replace the label and group in the tooltips
        if isinstance(tooltips, list):
            tooltips = [self._replace_hover_label_group(element, ttp) for ttp in tooltips]
        elif isinstance(tooltips, str):
            tooltips = self._replace_hover_label_group(element, tooltips)

        if self.hover_formatters:
            hover_opts['formatters'] = self.hover_formatters

        if self.hover_mode:
            hover_opts["mode"] = self.hover_mode

        return tooltips, hover_opts

    def _init_tools(self, element, callbacks=None):
        """Processes the list of tools to be supplied to the plot.

        """
        if callbacks is None:
            callbacks = []

        tooltips, hover_opts = self._prepare_hover_kwargs(element)

        if not tooltips:
            tooltips = None

        callbacks = callbacks+self.callbacks
        cb_tools, tool_names = [], []
        hover = False
        for cb in callbacks:
            for handle in cb.models:
                if handle and handle in TOOLS_MAP:
                    tool_names.append(handle)
                    if handle == 'hover':
                        tool = tools.HoverTool(
                            tooltips=tooltips, tags=['hv_created'],
                            **hover_opts)
                        hover = tool
                    else:
                        tool = TOOLS_MAP[handle]()
                    cb_tools.append(tool)
                    self.handles[handle] = tool

        all_tools = cb_tools + self.default_tools + self.tools
        if self.hover_tooltips:
            no_hover = (
                "hover" not in all_tools and
                not (any(isinstance(tool, tools.HoverTool) for tool in all_tools))
            )
            if no_hover:
                all_tools.append("hover")

        tool_list = []
        for tool in all_tools:
            if tool in tool_names:
                continue
            if tool in ['vline', 'hline']:
                tool_opts = dict(hover_opts, mode=tool)
                tool = tools.HoverTool(
                    tooltips=tooltips, tags=['hv_created'], **tool_opts
                )
            elif BOKEH_GE_3_2_0 and isinstance(tool, str) and tool.endswith(
                ('wheel_zoom', 'zoom_in', 'zoom_out')
            ):
                zoom_kwargs = {}
                tags = ['hv_created']
                if self.subcoordinate_y and not tool.startswith('x'):
                    zoom_dims = 'height'
                    zoom_kwargs['level'] = 1
                    tags.append(tool)
                elif tool.startswith('x'):
                    zoom_dims = 'width'
                elif tool.startswith('y'):
                    zoom_dims = 'height'
                else:
                    zoom_dims = 'both'
                zoom_kwargs['dimensions'] = zoom_dims
                zoom_kwargs['tags'] = tags
                if tool.endswith('wheel_zoom'):
                    # Setting `zoom_together` for multi-y axis support.
                    zoom_kwargs['zoom_together'] = 'none'
                    zoom_type = tools.WheelZoomTool
                elif tool.endswith('zoom_in'):
                    zoom_type = tools.ZoomInTool
                elif tool.endswith('zoom_out'):
                    zoom_type = tools.ZoomOutTool
                tool = zoom_type(**zoom_kwargs)
            tool_list.append(tool)

        copied_tools = []
        skip_models = (Span,)
        for tool in tool_list:
            if isinstance(tool, tools.Tool):
                properties = {
                    p: v.clone() if isinstance(v, Model) and not isinstance(v, skip_models) else v
                    for p, v in tool.properties_with_values(include_defaults=False).items()
                }
                tool = type(tool)(**properties)
            copied_tools.append(tool)

        hover_tools = [t for t in copied_tools if isinstance(t, tools.HoverTool)]
        if 'hover' in copied_tools:
            hover = tools.HoverTool(tooltips=tooltips, tags=['hv_created'], **hover_opts)
            copied_tools[copied_tools.index('hover')] = hover
        elif any(hover_tools):
            hover = hover_tools[0]
        if hover:
            self.handles['hover'] = hover

        if self.subcoordinate_y:
            zoom_tools = {}
            _zoom_types = (tools.WheelZoomTool, tools.ZoomInTool, tools.ZoomOutTool)
            for t in copied_tools:
                if isinstance(t, _zoom_types) and 'hv_created' in t.tags and len(t.tags) == 2:
                    zoom_tools[t.tags[1]] = t
            if zoom_tools:
                self.handles['zooms_subcoordy'] = zoom_tools

        box_tools = [t for t in copied_tools if isinstance(t, tools.BoxSelectTool)]
        if box_tools:
            self.handles['box_select'] = box_tools[0]
        lasso_tools = [t for t in copied_tools if isinstance(t, tools.LassoSelectTool)]
        if lasso_tools:
            self.handles['lasso_select'] = lasso_tools[0]

        # Link the selection properties between tools
        if box_tools and lasso_tools:
            box_tools[0].js_link('mode', lasso_tools[0], 'mode')
            lasso_tools[0].js_link('mode', box_tools[0], 'mode')

        return copied_tools

    def _update_hover(self, element):
        tool = self.handles['hover']
        if 'hv_created' in tool.tags:
            tooltips, _hover_opts = self._prepare_hover_kwargs(element)
            tool.tooltips = tooltips
        else:
            plot_opts = element.opts.get('plot', 'bokeh')
            new_hover = [t for t in plot_opts.kwargs.get('tools', [])
                         if isinstance(t, tools.HoverTool)]
            if new_hover:
                tool.tooltips = new_hover[0].tooltips

    def _get_hover_data(self, data, element, dimensions=None):
        """Initializes hover data based on Element dimension values.
        If empty initializes with no data.

        """
        has_hover = 'hover' in self.handles
        if not has_hover and not self.overlay_dims:
            return

        if has_hover and not self.static_source:
            for d in (dimensions or element.dimensions()):
                dim = util.dimension_sanitizer(d.name)
                if dim not in data:
                    data[dim] = element.dimension_values(d)

        if not data:
            return

        for k, v in self.overlay_dims.items():
            dim = util.dimension_sanitizer(k.name)
            if dim not in data:
                data[dim] = [v] * len(next(iter(data.values())))

    def _shared_axis_range(self, plots, specs, range_type, axis_type, pos):
        """Given a list of other plots return the shared axis from another
        plot by matching the dimensions specs stored as tags on the
        dimensions. Returns None if there is no such axis.

        """
        dim_range = None
        categorical = range_type is FactorRange
        for plot in plots:
            if plot is None or specs is None:
                continue
            ax = 'x' if pos == 0 else 'y'
            plot_range = getattr(plot, f'{ax}_range', None)
            axes = getattr(plot, f'{ax}axis', None)
            extra_ranges = getattr(plot, f'extra_{ax}_ranges', {})

            if (
                plot_range and plot_range.tags and
                match_dim_specs(plot_range.tags[0], specs) and
                match_ax_type(axes[0], axis_type) and
                not (categorical and not isinstance(dim_range, FactorRange))
            ):
                dim_range = plot_range

            if dim_range is not None:
                break

            for extra_range in extra_ranges.values():
                if (
                    extra_range.tags and match_dim_specs(extra_range.tags[0], specs) and
                    match_yaxis_type_to_range(axes, axis_type, extra_range.name) and
                    not (categorical and not isinstance(dim_range, FactorRange))
                ):
                    dim_range = extra_range
                    break

        return dim_range

    @property
    def _subcoord_overlaid(self):
        """Indicates when the context is a subcoordinate plot, either from within
        the overlay rendering or one of its subplots. Used to skip code paths
        when rendering an element outside of an overlay.

        """
        if self._subcoord_standalone_ is not None:
            return self._subcoord_standalone_
        self._subcoord_standalone_ = (
            (isinstance(self, OverlayPlot) and self.subcoordinate_y) or
            (not isinstance(self, OverlayPlot) and self.overlaid and self.subcoordinate_y)
        )
        return self._subcoord_standalone_

    def _axis_props(self, plots, subplots, element, ranges, pos, *, dim=None,
                    range_tags_extras=None, extra_range_name=None):

        if range_tags_extras is None:
            range_tags_extras = []
        el = element.traverse(lambda x: x, [lambda el: isinstance(el, Element) and not isinstance(el, (Annotation, Tiles))])
        el = el[0] if el else element
        if isinstance(el, Graph):
            el = el.nodes

        range_el = el if self.batched and not isinstance(self, OverlayPlot) else element

        if pos == 1 and 'subcoordinate_y' in range_tags_extras and dim and dim.range != (None, None):
            dims = [dim]
            v0, v1 = dim.range
            axis_label = str(dim)
            specs = ((dim.label, dim.unit),)
        # For y-axes check if we explicitly passed in a dimension.
        # This is used by certain plot types to create an axis from
        # a synthetic dimension and exclusively supported for y-axes.
        elif pos == 1 and dim:
            dims = [dim]
            v0, v1 = util.max_range([
                elrange.get(dim.label, {'combined': (None, None)})['combined']
                for elrange in ranges.values()
            ])
            axis_label = str(dim)
            specs = ((dim.label, dim.unit),)
        else:
            try:
                l, b, r, t = self.get_extents(range_el, ranges, dimension=dim)
            except TypeError:
                # Backward compatibility for e.g. GeoViews=<1.10.1 since dimension
                # is a newly added keyword argument in HoloViews 1.17
                l, b, r, t = self.get_extents(range_el, ranges)
            if self.invert_axes:
                l, b, r, t = b, l, t, r
            if pos == 1 and self._subcoord_overlaid:
                if isinstance(self.subcoordinate_y, bool):
                    if self.ylim and all(np.isfinite(val) for val in self.ylim):
                        v0, v1 = self.ylim
                    else:
                        offset = self.subcoordinate_scale / 2.
                        # This sum() is equal to n+1, where n is the number of elements contained
                        # in the overlay with subcoordinate_y=True (including the the root overlay,
                        # which has subcoordinate_y=True due to option propagation)
                        v0, v1 = 0-offset, sum(self.traverse(lambda p: p.subcoordinate_y))-2+offset
                else:
                    v0, v1 = 0, 1
            else:
                v0, v1 = (l, r) if pos == 0 else (b, t)

            axis_dims = list(self._get_axis_dims(el))
            if self.invert_axes:
                axis_dims[0], axis_dims[1] = axis_dims[:2][::-1]
            dims = axis_dims[pos]
            if dims:
                if not isinstance(dims, list):
                    dims = [dims]
                specs = tuple((d.label, d.unit) for d in dims)
            else:
                specs = None

            if dim:
                axis_label = str(dim)
            else:
                xlabel, ylabel, _zlabel = self._get_axis_labels(dims if dims else (None, None))
                if self.invert_axes:
                    xlabel, ylabel = ylabel, xlabel
                axis_label = ylabel if pos else xlabel
            if dims:
                dims = dims[:2][::-1]

        categorical = any(self.traverse(lambda plot: plot._categorical))
        if self.subcoordinate_y:
            categorical = False
        elif dims is not None and any(dim.label in ranges and 'factors' in ranges[dim.label] for dim in dims):
            categorical = True
        else:
            categorical = any(isinstance(v, (str, bytes)) for v in (v0, v1))

        range_types = (self._x_range_type, self._y_range_type)
        if self.invert_axes: range_types = range_types[::-1]
        range_type = range_types[pos]
        # If multi_x/y then grab opts from element
        axis_type = 'log' if (self.logx, self.logy)[pos] else 'auto'
        if dims:
            if len(dims) > 1 or range_type is FactorRange:
                axis_type = 'auto'
                categorical = True
            elif el.get_dimension(dims[0]):
                dim_type = el.get_dimension_type(dims[0])
                if isinstance(v0, util.datetime_types) or dim_type in util.datetime_types:
                    axis_type = 'datetime'
                elif BOKEH_GE_3_8_0 and (isinstance(v0, util.timedelta_types) or dim_type in util.timedelta_types):
                    axis_type = 'timedelta'

        norm_opts = self.lookup_options(el, 'norm').options
        shared_name = extra_range_name or ('x-main-range' if pos == 0 else 'y-main-range')
        if plots and self.shared_axes and not norm_opts.get('axiswise', False) and not dim:
            dim_range = self._shared_axis_range(plots, specs, range_type, axis_type, pos)
            if dim_range:
                self._shared[shared_name] = True

        # If we have a single dimension grab it so it can be set as the Range name
        name = None
        if dim:
            name = dim.name
        elif dims and len(dims) == 1:
            name = dims[0].name

        if self._shared.get(shared_name) and not dim:
            pass
        elif categorical:
            axis_type = 'auto'
            dim_range = FactorRange(name=name)
        elif None in [v0, v1] or any(
            True if isinstance(el, (str, bytes, *util.cftime_types))
            else not util.isfinite(el) for el in [v0, v1]
        ):
            dim_range = range_type(name=name)
        elif issubclass(range_type, FactorRange):
            dim_range = range_type(name=name)
        else:
            dim_range = range_type(start=v0, end=v1, name=name)

        if not dim_range.tags and specs is not None:
            dim_range.tags.append(specs)
            dim_range.tags.append(range_tags_extras)

        if extra_range_name:
            dim_range.name = extra_range_name
        return axis_type, axis_label, dim_range

    def _create_extra_axes(self, plots, subplots, element, ranges):
        if self.invert_axes:
            axpos0, axpos1 = 'below', 'above'
        else:
            axpos0, axpos1 = 'left', 'right'

        ax_specs, yaxes, dimensions = {}, {}, {}
        subcoordinate_axes = 0
        for el, (sp_key, sp) in zip(element, self.subplots.items(), strict=None):
            ax_dims = sp._get_axis_dims(el)[:2]
            if sp.invert_axes:
                ax_dims[::-1]
            yd = ax_dims[1]
            opts = el.opts.get('plot', backend='bokeh').kwargs
            if not isinstance(yd, Dimension) or yd.name in yaxes:
                continue
            if self._subcoord_overlaid:
                if opts.get('subcoordinate_y') is None:
                    continue
                if sp.overlay_dims:
                    ax_name = ', '.join(d.pprint_value(k) for d, k in zip(element.kdims, sp_key, strict=None))
                else:
                    ax_name = el.label
                subcoordinate_axes += 1
            else:
                ax_name = yd.name
            dimensions[ax_name] = yd
            yaxes[ax_name] = {
                'position': opts.get('yaxis', axpos1 if yaxes else axpos0),
                'autorange': opts.get('autorange', None),
                'logx': opts.get('logx', False),
                'logy': opts.get('logy', False),
                'invert_yaxis': opts.get('invert_yaxis', False),
                # 'xlim': opts.get('xlim', (np.nan, np.nan)), # TODO
                'ylim': opts.get('ylim', (np.nan, np.nan)),
                'label': opts.get('ylabel', dim_axis_label(yd)),
                'fontsize': {
                    'axis_label_text_font_size': sp._fontsize('ylabel').get('fontsize'),
                    'major_label_text_font_size': sp._fontsize('yticks').get('fontsize')
                },
                'subcoordinate_y': (subcoordinate_axes - 1) if self._subcoord_overlaid else None
            }

        for ydim, info in yaxes.items():
            range_tags_extras = {'invert_yaxis': info['invert_yaxis']}
            if info['subcoordinate_y'] is not None:
                range_tags_extras['subcoordinate_y'] = info['subcoordinate_y']
            if info['autorange'] == 'y':
                range_tags_extras['autorange'] = True
                lowerlim, upperlim = info['ylim'][0], info['ylim'][1]
                if not ((lowerlim is None) or np.isnan(lowerlim)):
                    range_tags_extras['y-lowerlim'] = lowerlim
                if not ((upperlim is None) or np.isnan(upperlim)):
                    range_tags_extras['y-upperlim'] = upperlim
            else:
                range_tags_extras['autorange'] = False
            ax_props = self._axis_props(
                plots, subplots, element, ranges, pos=1, dim=dimensions[ydim],
                range_tags_extras=range_tags_extras,
                extra_range_name=ydim
            )
            log_enabled = info['logx'] if self.invert_axes else info['logy']
            ax_type = 'log' if log_enabled else ax_props[0]
            ax_specs[ydim] = (
                ax_type, info['label'], ax_props[2], info['position'], info['fontsize']
            )
        return yaxes, ax_specs

    def _init_plot(self, key, element, plots, ranges=None):
        """Initializes Bokeh figure to draw Element into and sets basic
        figure and axis attributes including axes types, labels,
        titles and plot height and width.

        """
        subplots = list(self.subplots.values()) if self.subplots else []

        axis_specs = {'x': {}, 'y': {}}
        axis_specs['x']['x'] = (*self._axis_props(plots, subplots, element, ranges, pos=0), self.xaxis, {})
        if self.multi_y and subplots:
            if not BOKEH_GE_3_2_0:
                self.param.warning('Independent axis zooming for multi_y=True only supported for Bokeh >=3.2')
            _yaxes, extra_axis_specs = self._create_extra_axes(plots, subplots, element, ranges)
            axis_specs['y'].update(extra_axis_specs)
        else:
            range_tags_extras = {'invert_yaxis': self.invert_yaxis}
            if self.autorange == 'y':
                range_tags_extras['autorange'] = True
                lowerlim, upperlim = self.ylim
                if not ((lowerlim is None) or np.isnan(lowerlim)):
                    range_tags_extras['y-lowerlim'] = lowerlim
                if not ((upperlim is None) or np.isnan(upperlim)):
                    range_tags_extras['y-upperlim'] = upperlim
            else:
                range_tags_extras['autorange'] = False
            axis_specs['y']['y'] = (
                *self._axis_props(plots, subplots, element, ranges, pos=1, range_tags_extras=range_tags_extras), self.yaxis, {}
            )

        if self._subcoord_overlaid:
            _, extra_axis_specs = self._create_extra_axes(plots, subplots, element, ranges)
            axis_specs['y'].update(extra_axis_specs)

        properties, axis_props = {}, {'x': {}, 'y': {}}
        for axis, axis_spec in axis_specs.items():
            for (axis_dim, (axis_type, axis_label, axis_range, axis_position, fontsize)) in axis_spec.items():
                scale = get_scale(axis_range, axis_type)
                if f'{axis}_range' in properties:
                    properties[f'extra_{axis}_ranges'] = extra_ranges = properties.get(f'extra_{axis}_ranges', {})
                    extra_ranges[axis_dim] = axis_range
                    if not self.subcoordinate_y:
                        properties[f'extra_{axis}_scales'] = extra_scales = properties.get(f'extra_{axis}_scales', {})
                        extra_scales[axis_dim] = scale
                else:
                    properties[f'{axis}_range'] = axis_range
                    properties[f'{axis}_scale'] = scale
                    properties[f'{axis}_axis_type'] = axis_type
                    if axis_label and axis in self.labelled:
                        properties[f'{axis}_axis_label'] = axis_label
                    locs = {'left': 'left', 'right': 'right'} if axis == 'y' else {'bottom': 'below', 'top': 'above'}
                    if axis_position in (None, False):
                        axis_props[axis]['visible'] = False
                    axis_props[axis].update(fontsize)
                    for loc, pos in locs.items():
                        if isinstance(axis_position, str) and loc in axis_position:
                            properties[f'{axis}_axis_location'] = pos

        if not self.show_frame:
            properties['outline_line_alpha'] = 0

        if self.show_title and self.adjoined is None:
            title = self._format_title(key, separator=' ')
        else:
            title = ''

        if self.toolbar != 'disable':
            tools = self._init_tools(element)
            properties['tools'] = tools
            properties['toolbar_location'] = self.toolbar
        else:
            properties['tools'] = []
            properties['toolbar_location'] = None

        if self.renderer.webgl:
            properties['output_backend'] = 'webgl'

        properties.update(**self._plot_properties(key, element))

        with warnings.catch_warnings():
            # Bokeh raises warnings about duplicate tools but these
            # are not really an issue
            warnings.simplefilter('ignore', UserWarning)
            fig = bokeh.plotting.figure(title=title, **properties)
        fig.xaxis[0].update(**axis_props['x'])
        fig.yaxis[0].update(**axis_props['y'])
        fig.toolbar.autohide = self.autohide_toolbar

        # Set up handlers to configure following behavior on streaming plots
        if self.streaming:
            fig.on_event('rangesupdate', self._disable_follow)
            fig.on_event('reset', self._reset_follow)
            code = "export default (_, cb_obj) => { cb_obj.origin.hold_render = false }"
            fig.js_on_event('reset', CustomJS(code=code))

        # Do not add the extra axes to the layout if subcoordinates are used
        if self._subcoord_overlaid:
            return fig

        multi_ax = 'x' if self.invert_axes else 'y'
        for axis_dim, range_obj in properties.get(f'extra_{multi_ax}_ranges', {}).items():
            axis_type, axis_label, _, axis_position, fontsize = axis_specs[multi_ax][axis_dim]
            ax_cls, ax_kwargs = get_axis_class(axis_type, range_obj, dim=1)
            ax_kwargs[f'{multi_ax}_range_name'] = axis_dim
            ax_kwargs.update(fontsize)
            if axis_position is None:
                ax_kwargs['visible'] = False
                axis_position = 'above' if multi_ax == 'x' else 'right'
            if multi_ax in self.labelled:
                ax_kwargs['axis_label'] = axis_label
            ax = ax_cls(**ax_kwargs)
            fig.add_layout(ax, axis_position)
        return fig

    def _disable_follow(self, event):
        hold = self.state.hold_render
        for stream in self.streaming:
            stream.following = False
        if not hold:
            stream.trigger(self.streaming)
            self.state.hold_render = True

    def _reset_follow(self, event):
        self.state.hold_render = False
        for stream in self.streaming:
            stream.following = True
        stream.trigger(self.streaming)

    def _plot_properties(self, key, element):
        """Returns a dictionary of plot properties.

        """
        init = 'plot' not in self.handles
        size_multiplier = self.renderer.size/100.
        options = self._traverse_options(element, 'plot', ['width', 'height'], defaults=False)

        logger = self.param if init else None
        aspect_props, dimension_props = compute_layout_properties(
            self.width, self.height, self.frame_width, self.frame_height,
            options.get('width'), options.get('height'), self.aspect, self.data_aspect,
            self.responsive, size_multiplier, logger=logger)

        if not init:
            if aspect_props['aspect_ratio'] is None:
                aspect_props['aspect_ratio'] = self.state.aspect_ratio

        plot_props = {
            'align':         self.align,
            'margin':        self.margin,
            'max_width':     self.max_width,
            'max_height':    self.max_height,
            'min_width':     self.min_width,
            'min_height':    self.min_height
        }
        plot_props.update(aspect_props)
        if not self.drawn:
            plot_props.update(dimension_props)

        if self.bgcolor:
            plot_props['background_fill_color'] = self.bgcolor
        if self.border is not None:
            for p in ['left', 'right', 'top', 'bottom']:
                plot_props['min_border_'+p] = self.border
        lod = dict(self.param["lod"].default, **self.lod) if "lod" in self.param else self.lod
        for lod_prop, v in lod.items():
            plot_props['lod_'+lod_prop] = v
        return plot_props

    def _set_active_tools(self, plot):
        """Activates the list of active tools

        """
        if plot is None or self.toolbar == "disable":
            return

        if self.active_tools is None:
            enabled_tools = set(self.default_tools + self.tools)
            active_tools =  {'pan', 'wheel_zoom'} & enabled_tools
        else:
            active_tools = self.active_tools

        if active_tools == []:
            # Removes Bokeh default behavior of having Pan enabled by default
            plot.toolbar.active_drag = None

        for tool in active_tools:
            if isinstance(tool, str):
                tool_type = TOOL_TYPES.get(tool, type(None))
                matching = [t for t in plot.toolbar.tools
                            if isinstance(t, tool_type)]
                if not matching:
                    self.param.warning(
                        f'Tool of type {tool!r} could not be found '
                        'and could not be activated by default.'
                    )
                    continue
                tool = matching[0]
            if isinstance(tool, tools.Drag):
                plot.toolbar.active_drag = tool
            if isinstance(tool, tools.Scroll):
                plot.toolbar.active_scroll = tool
            if isinstance(tool, tools.Tap):
                plot.toolbar.active_tap = tool
            if isinstance(tool, tools.InspectTool):
                plot.toolbar.active_inspect.append(tool)

    def _title_properties(self, key, plot, element):
        if self.show_title and self.adjoined is None:
            title = self._format_title(key, separator=' ')
        else:
            title = ''
        opts = dict(text=title)

        # this will override theme if not set to the default 12pt
        title_font = self._fontsize('title').get('fontsize')
        if title_font != '12pt':
            opts['text_font_size'] = title_font
        return opts

    def _populate_axis_handles(self, plot):
        self.handles['xaxis'] = plot.xaxis[0]
        self.handles['x_range'] = plot.x_range
        self.handles['extra_x_ranges'] = plot.extra_x_ranges
        self.handles['extra_x_scales'] = plot.extra_x_scales
        self.handles['yaxis'] = plot.yaxis[0]
        self.handles['y_range'] = plot.y_range
        self.handles['extra_y_ranges'] = plot.extra_y_ranges
        self.handles['extra_y_scales'] = plot.extra_y_scales

    def _axis_properties(self, axis, key, plot, dimension=None,
                         ax_mapping=None):
        """Returns a dictionary of axis properties depending
        on the specified axis.

        """
        # need to copy dictionary by calling dict() on it
        if ax_mapping is None:
            ax_mapping = {'x': 0, 'y': 1}
        axis_props = dict(theme_attr_json(self.renderer.theme, 'Axis'))

        if ((axis == 'x' and self.xaxis in ['bottom-bare', 'top-bare', 'bare']) or
            (axis == 'y' and self.yaxis in ['left-bare', 'right-bare', 'bare'])):
            zero_pt = '0pt'
            axis_props['axis_label_text_font_size'] = zero_pt
            axis_props['major_label_text_font_size'] = zero_pt
            axis_props['major_tick_line_color'] = None
            axis_props['minor_tick_line_color'] = None
        else:
            labelsize = self._fontsize(f'{axis}label').get('fontsize')
            if labelsize:
                axis_props['axis_label_text_font_size'] = labelsize
            ticksize = self._fontsize(f'{axis}ticks', common=False).get('fontsize')
            if ticksize:
                axis_props['major_label_text_font_size'] = ticksize
            rotation = self.xrotation if axis == 'x' else self.yrotation
            if rotation:
                axis_props['major_label_orientation'] = np.radians(rotation)
            ticker = self.xticks if axis == 'x' else self.yticks
            if not (self._subcoord_overlaid and axis == 'y'):
                axis_props.update(get_ticker_axis_props(ticker))
            elif not self.drawn:
                ticks, labels = [], []
                idx = 0
                for el, (sp_key, sp) in zip(self.current_frame, self.subplots.items(), strict=None):
                    if not sp.subcoordinate_y:
                        continue
                    ycenter = idx if isinstance(sp.subcoordinate_y, bool) else 0.5 * sum(sp.subcoordinate_y)
                    idx += 1
                    ticks.append(ycenter)
                    if el.label or not self.current_frame.kdims:
                        labels.append(el.label)
                    else:
                        labels.append(', '.join(d.pprint_value(k) for d, k in zip(self.current_frame.kdims, sp_key, strict=None)))
                axis_props['ticker'] = FixedTicker(ticks=ticks)
                if labels is not None:
                    axis_props['major_label_overrides'] = dict(zip(ticks, labels, strict=None))
        formatter = self.xformatter if axis == 'x' else self.yformatter
        if formatter:
            formatter = wrap_formatter(formatter, axis)
            if formatter is not None:
                axis_props['formatter'] = formatter
        elif CustomJSTickFormatter is not None and ax_mapping and isinstance(dimension, Dimension):
            formatter = None
            if dimension.value_format:
                formatter = dimension.value_format
            elif dimension.type in dimension.type_formatters:
                formatter = dimension.type_formatters[dimension.type]

        if axis == 'x':
            axis_obj = plot.xaxis[0]
        elif axis == 'y':
            axis_obj = plot.yaxis[0]

        if (self.geographic and isinstance(self.projection, str)
            and self.projection == 'mercator'):
            dimension = 'lon' if axis == 'x' else 'lat'
            axis_props['ticker'] = MercatorTicker(dimension=dimension)
            axis_props['formatter'] = MercatorTickFormatter(dimension=dimension)
            box_zoom = self.state.select(type=tools.BoxZoomTool)
            if box_zoom:
                box_zoom[0].match_aspect = True
            wheel_zoom = self.state.select(type=tools.WheelZoomTool)
            if wheel_zoom:
                wheel_zoom[0].zoom_on_axis = False
        elif isinstance(axis_obj, CategoricalAxis):
            for axis_prop in list(axis_props):
                if axis_prop.startswith('major_label'):
                    # set the group labels equal to major (actually minor)
                    new_axis_prop = axis_prop.replace('major_label', 'group')
                    axis_props[new_axis_prop] = axis_props[axis_prop]

            # major ticks are actually minor ticks in a categorical
            # so if user inputs minor ticks sizes, then use that;
            # else keep major (group) == minor (subgroup)
            msize = self._fontsize(f'minor_{axis}ticks',
                common=False).get('fontsize')
            if msize is not None:
                axis_props['major_label_text_font_size'] = msize

        return axis_props

    def _update_plot(self, key, plot, element=None):
        """Updates plot parameters on every frame

        """
        plot.update(**self._plot_properties(key, element))
        if not self.multi_y:
            self._update_labels(key, plot, element)
        self._update_title(key, plot, element)
        self._update_grid(plot)

    def _update_labels(self, key, plot, element):
        el = element.traverse(lambda x: x, [Element])
        el = el[0] if el else element
        dimensions = self._get_axis_dims(el)
        props = {axis: self._axis_properties(axis, key, plot, dim)
                 for axis, dim in zip(['x', 'y'], dimensions, strict=None)}
        xlabel, ylabel, _zlabel = self._get_axis_labels(dimensions)
        if self.invert_axes:
            xlabel, ylabel = ylabel, xlabel
        props['x']['axis_label'] = xlabel if 'x' in self.labelled or self.xlabel else ''
        props['y']['axis_label'] = ylabel if 'y' in self.labelled or self.ylabel else ''
        recursive_model_update(plot.xaxis[0], props.get('x', {}))
        recursive_model_update(plot.yaxis[0], props.get('y', {}))

    def _update_title(self, key, plot, element):
        if plot.title:
            plot.title.update(**self._title_properties(key, plot, element))
        else:
            plot.title = Title(**self._title_properties(key, plot, element))

    def _update_backend_opts(self):
        plot = self.handles["plot"]
        model_accessor_aliases = {
            "cbar": "colorbar",
            "p": "plot",
            "xaxes": "xaxis",
            "yaxes": "yaxis",
        }

        for opt, val in self.backend_opts.items():
            parsed_opt = self._parse_backend_opt(
                opt, plot, model_accessor_aliases)
            if parsed_opt is None:
                continue
            model, attr_accessor = parsed_opt

            # not using isinstance because some models inherit from list
            if not isinstance(model, list):
                # to reduce the need for many if/else; cast to list
                # to do the same thing for both single and multiple models
                models = [model]
            else:
                models = model

            valid_options = models[0].properties()
            if attr_accessor not in valid_options:
                kws = Keywords(values=valid_options)
                matches = sorted(kws.fuzzy_match(attr_accessor))
                self.param.warning(
                    f"Could not find {attr_accessor!r} property on {type(models[0]).__name__!r} "
                    f"model. Ensure the custom option spec {opt!r} you provided references a "
                    f"valid attribute on the specified model. "
                    f"Similar options include {matches!r}"
                )

                continue

            for m in models:
                setattr(m, attr_accessor, val)


    def _update_grid(self, plot):
        if not self.show_grid:
            plot.xgrid.grid_line_color = None
            plot.ygrid.grid_line_color = None
            return
        replace = ['bounds', 'bands', 'visible', 'level', 'ticker', 'visible']
        style_items = list(self.gridstyle.items())
        both = {k: v for k, v in style_items if k.startswith(('grid_', 'minor_grid'))}
        xgrid = {k.replace('xgrid', 'grid'): v for k, v in style_items if 'xgrid' in k}
        ygrid = {k.replace('ygrid', 'grid'): v for k, v in style_items if 'ygrid' in k}
        xopts = {k.replace('grid_', '') if any(r in k for r in replace) else k: v
                 for k, v in dict(both, **xgrid).items()}
        yopts = {k.replace('grid_', '') if any(r in k for r in replace) else k: v
                 for k, v in dict(both, **ygrid).items()}
        if plot.xaxis and 'ticker' not in xopts:
            xopts['ticker'] = plot.xaxis[0].ticker
        if plot.yaxis and 'ticker' not in yopts:
            yopts['ticker'] = plot.yaxis[0].ticker
        plot.xgrid[0].update(**xopts)
        plot.ygrid[0].update(**yopts)

    def _update_ranges(self, element, ranges):
        x_range = self.handles['x_range']
        y_range = self.handles['y_range']
        plot = self.handles['plot']

        self._update_main_ranges(element, x_range, y_range, ranges)

        if self._subcoord_overlaid:
            return

        # ALERT: stream handling not handled
        streaming = False
        multi_dim = 'x' if self.invert_axes else 'y'
        for axis_dim, extra_y_range in self.handles[f'extra_{multi_dim}_ranges'].items():
            _, b, _, t = self.get_extents(element, ranges, dimension=axis_dim)
            factors = self._get_dimension_factors(element, ranges, axis_dim)
            extra_scale = self.handles[f'extra_{multi_dim}_scales'][axis_dim] # Assumes scales and ranges zip
            log = isinstance(extra_scale, LogScale)
            range_update = (not (self.model_changed(extra_y_range) or self.model_changed(plot))
                            and self.framewise)
            if self.drawn and not range_update:
                continue
            self._update_range(
                extra_y_range, b, t, factors,
                self._get_tag(extra_y_range, 'invert_yaxis'),
                self._shared.get(extra_y_range.name, False), log, streaming
            )

    def _update_main_ranges(self, element, x_range, y_range, ranges, subcoord=False):
        plot = self.handles['plot']

        l, b, r, t = None, None, None, None
        if any(isinstance(r, (Range1d, DataRange1d)) for r in [x_range, y_range]):
            if self.multi_y:
                range_dim = x_range.name if self.invert_axes else y_range.name
            else:
                range_dim = None
            try:
                l, b, r, t = self.get_extents(element, ranges, dimension=range_dim)
            except TypeError:
                # Backward compatibility for e.g. GeoViews=<1.10.1 since dimension
                # is a newly added keyword argument in HoloViews 1.17
                l, b, r, t = self.get_extents(element, ranges)
            if self.invert_axes:
                l, b, r, t = b, l, t, r

        xfactors, yfactors = None, None
        if any(isinstance(ax_range, FactorRange) for ax_range in [x_range, y_range]):
            xfactors, yfactors = self._get_factors(element, ranges)
        framewise = self.framewise
        streaming = (self.streaming and any(stream._triggering and stream.following
                                            for stream in self.streaming))
        xupdate = not subcoord and ((not (self.model_changed(x_range) or self.model_changed(plot))
                    and (framewise or streaming))
                   or xfactors is not None)
        yupdate = (not (self.model_changed(x_range) or self.model_changed(plot))
                    and (framewise or streaming) or yfactors is not None)

        options = self._traverse_options(element, 'plot', ['width', 'height'], defaults=False)
        fixed_width = (self.frame_width or options.get('width'))
        fixed_height = (self.frame_height or options.get('height'))
        constrained_width = options.get('min_width') or options.get('max_width')
        constrained_height = options.get('min_height') or options.get('max_height')

        data_aspect = (self.aspect == 'equal' or self.data_aspect)
        xaxis, yaxis = self.handles['xaxis'], self.handles['yaxis']
        categorical = isinstance(xaxis, CategoricalAxis) or isinstance(yaxis, CategoricalAxis)
        datetime = isinstance(xaxis, DatetimeAxis) or isinstance(yaxis, DatetimeAxis)
        timedelta = BOKEH_GE_3_8_0 and (isinstance(xaxis, TimedeltaAxis) or isinstance(yaxis, TimedeltaAxis))
        range_streams = [s for s in self.streams if isinstance(s, RangeXY)]

        if data_aspect and (categorical or datetime or timedelta):
            self.param.warning(
                'Cannot set data_aspect if one or both axes are '
                'categorical, datetime, or timedelta, data_aspect will '
                'be ignored.'
            )
        elif data_aspect:
            plot = self.handles['plot']
            xspan = r-l if util.is_number(l) and util.is_number(r) else None
            yspan = t-b if util.is_number(b) and util.is_number(t) else None

            if self.drawn or (fixed_width and fixed_height) or (constrained_width or constrained_height):
                # After initial draw or if aspect is explicit
                # adjust range to match the plot dimension aspect
                ratio = self.data_aspect or 1
                if self.aspect == 'square':
                    frame_aspect = 1
                elif self.aspect and self.aspect != 'equal':
                    frame_aspect = self.aspect
                elif plot.frame_height and plot.frame_width:
                    frame_aspect = plot.frame_height/plot.frame_width
                else:
                    # Skip if aspect can't be determined
                    return

                if self.drawn:
                    current_l, current_r = plot.x_range.start, plot.x_range.end
                    current_b, current_t = plot.y_range.start, plot.y_range.end
                    current_xspan, current_yspan = (current_r-current_l), (current_t-current_b)
                else:
                    current_l, current_r, current_b, current_t = l, r, b, t
                    current_xspan, current_yspan = xspan, yspan

                if any(rs._triggering for rs in range_streams):
                    # If the event was triggered by a RangeXY stream
                    # event we want to get the latest range span
                    # values so we do not accidentally trigger a
                    # loop of events
                    l, r, b, t = current_l, current_r, current_b, current_t
                    xspan, yspan = current_xspan, current_yspan

                size_streams = [s for s in self.streams if isinstance(s, PlotSize)]
                if any(ss._triggering for ss in size_streams) and self._updated:
                    # Do not trigger on frame size changes, except for
                    # the initial one which can be important if width
                    # and/or height constraints have forced different
                    # aspect. After initial event we skip because size
                    # changes can trigger event loops if the tick
                    # labels change the canvas size
                    return

                desired_xspan = yspan*(ratio/frame_aspect)
                desired_yspan = xspan/(ratio/frame_aspect)
                if ((np.allclose(desired_xspan, xspan, rtol=0.05) and
                     np.allclose(desired_yspan, yspan, rtol=0.05)) or
                    not (util.isfinite(xspan) and util.isfinite(yspan))):
                    pass
                elif desired_yspan >= yspan:
                    desired_yspan = current_xspan/(ratio/frame_aspect)
                    ypad = (desired_yspan-yspan)/2.
                    b, t = b-ypad, t+ypad
                    yupdate = True
                else:
                    desired_xspan = current_yspan*(ratio/frame_aspect)
                    xpad = (desired_xspan-xspan)/2.
                    l, r = l-xpad, r+xpad
                    xupdate = True
            elif not (fixed_height and fixed_width):
                # Set initial aspect
                aspect = self.get_aspect(xspan, yspan)
                width = plot.frame_width or plot.width or 300
                height = plot.frame_height or plot.height or 300

                if not (fixed_width or fixed_height) and not self.responsive:
                    fixed_height = True

                if fixed_height:
                    plot.frame_height = height
                    plot.frame_width = int(height/aspect)
                    plot.width, plot.height = None, None
                elif fixed_width:
                    plot.frame_width = width
                    plot.frame_height = int(width*aspect)
                    plot.width, plot.height = None, None
                else:
                    plot.aspect_ratio = 1./aspect

            box_zoom = plot.select(type=tools.BoxZoomTool)
            scroll_zoom = plot.select(type=tools.WheelZoomTool)
            if box_zoom:
                box_zoom.match_aspect = True
            if scroll_zoom:
                scroll_zoom.zoom_on_axis = False
        elif any(rs._triggering for rs in range_streams):
            xupdate, yupdate = False, False

        if not self.drawn or xupdate:
            self._update_range(x_range, l, r, xfactors, self.invert_xaxis,
                               self._shared['x-main-range'], self.logx, streaming)

        # If subcoordinate_y is enabled we iterate over each of the
        # subcoordinate ranges and let the subplot handle the update
        if self.subcoordinate_y and yupdate and not subcoord:
            updated = set()
            for sp in (self.subplots or {}).values():
                if isinstance(sp, GenericOverlayPlot):
                    subcoord = False
                    el_ranges = ranges
                else:
                    sp_range = sp.handles.get('y_range')
                    if not sp_range or sp_range.name in updated:
                        continue
                    el_ranges = util.match_spec(sp.current_frame, ranges)
                    updated.add(sp_range.name)
                    subcoord = True
                sp._update_main_ranges(
                    sp.current_frame, sp.handles['x_range'], sp.handles['y_range'],
                    el_ranges, subcoord=subcoord
                )
        elif (not self.drawn or yupdate) and (not self.subcoordinate_y or subcoord):
            self._update_range(
                y_range, b, t, yfactors, self._get_tag(y_range, 'invert_yaxis'),
                self._shared['y-main-range'], self.logy, streaming
            )

    def _get_tag(self, model, tag_name):
        """Get a tag from a Bokeh model

        Parameters
        ----------
        model : Model
            Bokeh model
        tag_name : str
            Name of tag to get

        Returns
        -------
        tag_value : Value of tag or False if not found
        """
        for tag in model.tags:
            if isinstance(tag, dict) and tag_name in tag:
                return tag[tag_name]
        return False

    def _update_range(self, axis_range, low, high, factors, invert, shared, log, streaming=False):
        if isinstance(axis_range, FactorRange):
            factors = list(decode_bytes(factors))
            if invert: factors = factors[::-1]
            axis_range.factors = factors
            return

        if not (isinstance(axis_range, (Range1d, DataRange1d)) and self.apply_ranges):
            return

        if isinstance(low, util.cftime_types):
            pass
        elif (low == high and low is not None):
            if isinstance(low, util.datetime_types):
                offset = np.timedelta64(500, 'ms')
                low, high = np.datetime64(low), np.datetime64(high)
                low -= offset
                high += offset
            else:
                offset = abs(low*0.1 if low else 0.5)
                low -= offset
                high += offset
        if shared:
            shared = (axis_range.start, axis_range.end)
            low, high = util.max_range([(low, high), shared])
        if invert: low, high = high, low
        if not isinstance(low, util.datetime_types) and log and (low is None or low <= 0):
            low = 0.01 if high > 0.01 else 10**(np.log10(high)-2)
            self.param.warning(
                "Logarithmic axis range encountered value less "
                "than or equal to zero, please supply explicit "
                f"lower bound to override default of {low:.3f}.")
        updates = {}
        if util.isfinite(low):
            updates['start'] = (axis_range.start, low)
            updates['reset_start'] = updates['start']
        if util.isfinite(high):
            updates['end'] = (axis_range.end, high)
            updates['reset_end'] = updates['end']
        for k, (old, new) in updates.items():
            if isinstance(new, util.cftime_types):
                new = date_to_integer(new)
            axis_range.update(**{k:new})
            if streaming and not k.startswith('reset_'):
                axis_range.trigger(k, old, new)

    def _setup_autorange(self):
        """Sets up a callback which will iterate over available data
        renderers and auto-range along one axis.

        """
        if not isinstance(self, OverlayPlot) and not self.apply_ranges:
            return

        if self.autorange is None:
            return
        dim = self.autorange
        if dim == 'x':
            didx = 0
            odim = 'y'
        else:
            didx = 1
            odim = 'x'
        if not self.padding:
            p0, p1 = 0, 0
        elif isinstance(self.padding, tuple):
            pad = self.padding[didx]
            if isinstance(pad, tuple):
                p0, p1 = pad
            else:
                p0, p1 = pad, pad
        else:
            p0, p1 = self.padding, self.padding

        callback = CustomJS(code=f"""
        const cb = function() {{

          function get_padded_range(key, lowerlim, upperlim, invert) {{
            let vmin = range_limits[key][0]
            let vmax = range_limits[key][1]

            if (lowerlim !== null) {{
             vmin = lowerlim
            }}

            if (upperlim !== null) {{
             vmax = upperlim
            }}

            const span = vmax-vmin
            const lower = vmin-(span*{p0})
            const upper = vmax+(span*{p1})
            return invert ? [upper, lower] : [lower, upper]
          }}

          let plot_view = Bokeh.index.find_one(plot)
          if (plot_view == null) {{
            return
          }}

          let range_limits = {{}}
          for (const dr of plot.data_renderers) {{
            let renderer
            if (plot_view.renderer_views !== undefined) {{
              // Changed to a Map in Bokeh 3.5
              renderer = plot_view.renderer_views.get(dr)
            }} else {{
              renderer = plot_view.renderer_view(dr)
            }}
            const glyph_view = renderer.glyph_view

            let [vmin, vmax] = [Infinity, -Infinity]
            let y_range_name = renderer.model.y_range_name

            if (!renderer.glyph.model.tags.includes('no_apply_ranges')) {{
              const index = glyph_view.index.index
              for (let pos = 0; pos < index._boxes.length - 4; pos += 4) {{
                const [x0, y0, x1, y1] = index._boxes.slice(pos, pos+4)
                if ({odim}0 > plot.{odim}_range.start && {odim}1 < plot.{odim}_range.end) {{
                  vmin = Math.min(vmin, {dim}0)
                  vmax = Math.max(vmax, {dim}1)
                }}
              }}
            }}

            if (y_range_name in range_limits) {{
              const [vmin_old, vmax_old] = range_limits[y_range_name]
              range_limits[y_range_name] = [Math.min(vmin, vmin_old), Math.max(vmax, vmax_old)]
            }} else {{
              range_limits[y_range_name] = [vmin, vmax]
            }}
          }}

          let range_tags_extras = plot.{dim}_range.tags[1]
          if (range_tags_extras['autorange']) {{
            let lowerlim = range_tags_extras['y-lowerlim'] ?? null
            let upperlim = range_tags_extras['y-upperlim'] ?? null
            let [start, end] = get_padded_range('default', lowerlim, upperlim, range_tags_extras['invert_yaxis'])
            if ((start != end) && window.Number.isFinite(start) && window.Number.isFinite(end)) {{
              plot.{dim}_range.setv({{start, end}})
            }}
          }}

          for (let key in plot.extra_{dim}_ranges) {{
            const extra_range = plot.extra_{dim}_ranges[key]
            let range_tags_extras = extra_range.tags[1]

            let lowerlim = range_tags_extras['y-lowerlim'] ?? null
            let upperlim = range_tags_extras['y-upperlim'] ?? null

            if (range_tags_extras['autorange']) {{
             let [start, end] = get_padded_range(key, lowerlim, upperlim, range_tags_extras['invert_yaxis'])
             if ((start != end) && window.Number.isFinite(start) && window.Number.isFinite(end)) {{
              extra_range.setv({{start, end}})
              }}
            }}
          }}
        }}
        // The plot changes will not propagate to the glyph until
        // after the data change event has occurred.
        setTimeout(cb, 0);
        """, args={'plot': self.state})
        self.state.js_on_event('rangesupdate', callback)
        self._js_on_data_callbacks.append(callback)

    def _categorize_data(self, data, cols, dims):
        """Transforms non-string or integer types in datasource if the
        axis to be plotted on is categorical. Accepts the column data
        source data, the columns corresponding to the axes and the
        dimensions for each axis, changing the data inplace.

        """
        if self.invert_axes:
            cols = cols[::-1]
            dims = dims[:2][::-1]
        ranges = [self.handles[f'{ax}_range'] for ax in 'xy']
        for i, col in enumerate(cols):
            column = data[col]
            if (isinstance(ranges[i], FactorRange) and
                (isinstance(column, list) or util.dtype_kind(column) not in 'SU')):
                data[col] = [dims[i].pprint_value(v) for v in column]


    def get_aspect(self, xspan, yspan):
        """Computes the aspect ratio of the plot

        """
        if 'plot' in self.handles and self.state.frame_width and self.state.frame_height:
            return self.state.frame_width/self.state.frame_height
        elif self.data_aspect:
            return (yspan/xspan)*self.data_aspect
        elif self.aspect == 'equal':
            return yspan/xspan
        elif self.aspect == 'square':
            return 1
        elif self.aspect is not None:
            return self.aspect
        elif self.width is not None and self.height is not None:
            return self.width/self.height
        else:
            return 1

    def _get_dimension_factors(self, element, ranges, dimension):
        if dimension.values:
            values = dimension.values
        elif 'factors' in ranges.get(dimension.label, {}):
            values = ranges[dimension.label]['factors']
        else:
            values = element.dimension_values(dimension, False)
        values = np.asarray(values)
        if not self._allow_implicit_categories:
            values = values if dtype_kind(values) in 'SU' else []
        return [v if dtype_kind(values) in 'SU' else dimension.pprint_value(v) for v in values]

    def _get_factors(self, element, ranges):
        """Get factors for categorical axes.

        """
        xdim, ydim = element.dimensions()[:2]
        xvals = self._get_dimension_factors(element, ranges, xdim)
        yvals = self._get_dimension_factors(element, ranges, ydim)
        coords = (xvals, yvals)
        if self.invert_axes: coords = coords[::-1]
        return coords

    def _process_legend(self):
        """Disables legends if show_legend is disabled.

        """
        for l in self.handles['plot'].legend:
            l.items[:] = []
            l.border_line_alpha = 0
            l.background_fill_alpha = 0

    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        mapping['tags'] = ['apply_ranges' if self.apply_ranges else 'no_apply_ranges']
        properties = mpl_to_bokeh(properties)
        plot_method = self._plot_methods.get('batched' if self.batched else 'single')
        if isinstance(plot_method, tuple):
            # Handle alternative plot method for flipped axes
            plot_method = plot_method[int(self.invert_axes)]
        if 'legend_field' in properties and 'legend_label' in properties:
            del properties['legend_label']

        if self.handles['x_range'].name in plot.extra_x_ranges and not self.subcoordinate_y:
            properties['x_range_name'] = self.handles['x_range'].name
        if self.handles['y_range'].name in plot.extra_y_ranges and not self.subcoordinate_y:
            properties['y_range_name'] = self.handles['y_range'].name

        if "name" not in properties:
            properties["name"] = properties.get("legend_label") or properties.get("legend_field")

        if self._subcoord_overlaid:
            y_source_range = self.handles['y_range']
            if isinstance(self.subcoordinate_y, bool):
                if "subcoordinate_y" not in y_source_range.tags[1]:
                    # See https://github.com/holoviz/holoviews/issues/6071
                    msg = 'Failed retrieving "subcoordinate_y". Labels mismatched for initial and updated DynamicMap plots.'
                    raise RuntimeError(msg)
                center = y_source_range.tags[1]['subcoordinate_y']
                offset = self.subcoordinate_scale/2.
                ytarget_range = dict(start=center-offset, end=center+offset)
            else:
                ytarget_range = dict(start=self.subcoordinate_y[0], end=self.subcoordinate_y[1])
            plot = plot.subplot(
                x_source=plot.x_range,
                x_target=plot.x_range,
                y_source=y_source_range,
                y_target=Range1d(**ytarget_range),
            )
        renderer = getattr(plot, plot_method)(**dict(properties, **mapping))
        return renderer, renderer.glyph

    def _element_transform(self, transform, element, ranges):
        return transform.apply(element, ranges=ranges, flat=True)

    def _apply_transforms(self, element, data, ranges, style, group=None):
        new_style = dict(style)
        prefix = group+'_' if group else ''
        for k, v in dict(style).items():
            if isinstance(v, (Dimension, str)):
                if validate(k, v) == True:
                    continue
                elif v in element:
                    v = dim(element.get_dimension(v))
                elif isinstance(element, Graph) and v in element.nodes:
                    v = dim(element.nodes.get_dimension(v))
                elif any(d==v for d in self.overlay_dims):
                    v = dim(next(d for d in self.overlay_dims if d==v))

            if (not isinstance(v, dim) or (group is not None and not k.startswith(group))):
                continue
            elif (not v.applies(element) and v.dimension not in self.overlay_dims):
                new_style.pop(k)
                self.param.warning(
                    f'Specified {k} dim transform {v!r} could not be applied, '
                    'as not all dimensions could be resolved.')
                continue

            if v.dimension in self.overlay_dims:
                ds = Dataset({d.name: v for d, v in self.overlay_dims.items()},
                             list(self.overlay_dims))
                val = v.apply(ds, ranges=ranges, flat=True)[0]
            elif 'node' in k:
                val = v.apply(element.nodes, ranges=ranges)
            else:
                val = self._element_transform(v, element, ranges)

            if (not util.isscalar(val) and len(util.unique_array(val)) == 1 and
                (('color' not in k or validate('color', val)) or k in self._nonvectorized_styles)):
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
                elif data and len(val) != len(next(iter(data.values()))):
                    if isinstance(element, VectorField):
                        val = np.tile(val, 3)
                    elif isinstance(element, Path) and not isinstance(element, Contours):
                        val = val[:-1]
                    else:
                        continue

            if k == 'angle':
                val = np.deg2rad(val)
            elif k.endswith('font_size'):
                if util.isscalar(val) and isinstance(val, int):
                    val = str(v)+'pt'
                elif isinstance(val, np.ndarray) and dtype_kind(val) in 'ifu':
                    val = [str(int(s))+'pt' for s in val]
            if util.isscalar(val):
                key = val
            else:
                # Node marker does not handle {'field': ...}
                key = k if k == 'node_marker' else {'field': k}
                data[k] = val

            # If color is not valid colorspec add colormapper
            numeric = isinstance(val, util.arraylike_types) and dtype_kind(val) in 'uifMmb'
            colormap = style.get(prefix+'cmap')
            if ('color' in k and isinstance(val, util.arraylike_types) and
                (numeric or not validate('color', val) or isinstance(colormap, dict))):
                kwargs = {}
                if dtype_kind(val) not in 'ifMu':
                    range_key = dim_range_key(v)
                    if range_key in ranges and 'factors' in ranges[range_key]:
                        factors = ranges[range_key]['factors']
                    else:
                        factors = util.unique_array(val)
                    if isinstance(val, util.arraylike_types) and dtype_kind(val) == 'b':
                        factors = factors.astype(str)
                    kwargs['factors'] = factors
                cmapper = self._get_colormapper(v, element, ranges,
                                                dict(style), name=k+'_color_mapper',
                                                group=group, **kwargs)
                field = k
                categorical = isinstance(cmapper, CategoricalColorMapper)

                if categorical:
                    if dtype_kind(val) in 'ifMub':
                        field = k + '_str__'
                        if v.dimension in element:
                            formatter = element.get_dimension(v.dimension).pprint_value
                        else:
                            formatter = str
                        data[field] = [formatter(d) for d in val]
                    if getattr(self, 'show_legend', False):
                        legend_labels = getattr(self, 'legend_labels', False)
                        if legend_labels:
                            label_field = f'_{field}_labels'
                            data[label_field] = [legend_labels.get(v, v) for v in val]
                            new_style['legend_field'] = label_field
                        else:
                            new_style['legend_field'] = field
                key = {'field': field, 'transform': cmapper}
            new_style[k] = key

        # Process color/alpha styles and expand to fill/line style
        for style, val in new_style.copy().items():  # noqa: PLR1704
            for s in ('alpha', 'color'):
                if prefix+s != style or style not in data or validate(s, val, True):
                    continue
                supports_fill = any(
                    o.startswith(prefix+'fill') and (prefix != 'edge_' or getattr(self, 'filled', True))
                    for o in self.style_opts)
                for pprefix in [p+'_' for p in property_prefixes]+['']:
                    fill_key = prefix+pprefix+'fill_'+s
                    fill_style = new_style.get(fill_key)

                    # Do not override custom nonselection/muted alpha
                    if ((pprefix in ('nonselection_', 'muted_') and s == 'alpha')
                        or fill_key not in self.style_opts):
                        continue

                    # Override empty and non-vectorized fill_style if not hover style
                    hover = pprefix == 'hover_'
                    if ((fill_style is None or (validate(s, fill_style, True) and not hover))
                        and supports_fill):
                        new_style[fill_key] = val

                    line_key = prefix+pprefix+'line_'+s
                    line_style = new_style.get(line_key)

                    # If glyph has fill and line style is set overriding line color
                    if supports_fill and line_style is not None:
                        continue

                    # If glyph does not support fill override non-vectorized line_color
                    if ((line_style is not None and (validate(s, line_style) and not hover)) or
                        (line_style is None and not supports_fill)):
                        new_style[line_key] = val

        return new_style


    def _glyph_properties(self, plot, element, source, ranges, style, group=None):
        properties = dict(style, source=source)
        if self.show_legend:
            if self.overlay_dims:
                legend = ', '.join([d.pprint_value(v, print_unit=True) for d, v in
                                    self.overlay_dims.items()])
            else:
                legend = element.label
            if legend and self.overlaid:
                properties['legend_label'] = legend
        return properties


    def _filter_properties(self, properties, glyph_type, allowed):
        glyph_props = dict(properties)
        for gtype in ((glyph_type, '') if glyph_type else ('',)):
            for prop in ('color', 'alpha'):
                glyph_prop = properties.get(gtype+prop)
                if glyph_prop is not None and ('line_'+prop not in glyph_props or gtype):
                    glyph_props['line_'+prop] = glyph_prop
                if glyph_prop is not None and ('fill_'+prop not in glyph_props or gtype):
                    glyph_props['fill_'+prop] = glyph_prop

            props = {k[len(gtype):]: v for k, v in glyph_props.items()
                     if k.startswith(gtype)}
            if self.batched:
                glyph_props = dict(props, **glyph_props)
            else:
                glyph_props.update(props)
        return {k: v for k, v in glyph_props.items() if k in allowed}


    def _update_glyph(self, renderer, properties, mapping, glyph, source, data):
        allowed_properties = glyph.properties()
        properties = mpl_to_bokeh(properties)
        merged = dict(properties, **mapping)
        legend_props = ('legend_field', 'legend_label')
        for lp in legend_props:
            legend = merged.pop(lp, None)
            if legend is not None:
                break
        columns = list(source.data.keys())
        glyph_updates = []
        for glyph_type in ('', 'selection_', 'nonselection_', 'hover_', 'muted_'):
            if renderer:
                glyph = getattr(renderer, glyph_type+'glyph', None)
                if glyph == 'auto':
                    base_glyph = renderer.glyph
                    props = base_glyph.properties_with_values()
                    glyph = type(base_glyph)(**{k: v for k, v in props.items()
                                                if not prop_is_none(v)})
                    setattr(renderer, glyph_type+'glyph', glyph)
            if not glyph or (not renderer and glyph_type):
                continue
            filtered = self._filter_properties(merged, glyph_type, allowed_properties)

            # Ensure that data is populated before updating glyph
            dataspecs = glyph.dataspecs()
            for spec in dataspecs:
                new_spec = property_to_dict(filtered.get(spec))
                old_spec = property_to_dict(getattr(glyph, spec))
                new_field = new_spec.get('field') if isinstance(new_spec, dict) else new_spec
                old_field = old_spec.get('field') if isinstance(old_spec, dict) else old_spec
                if (data is None) or (new_field not in data or new_field in source.data or new_field == old_field):
                    continue
                columns.append(new_field)
            glyph_updates.append((glyph, filtered))

        # If a dataspec has changed and the CDS.data will be replaced
        # the GlyphRenderer will not find the column, therefore we
        # craft an event which will make the column available.
        cds_replace = True if data is None else cds_column_replace(source, data)
        if not cds_replace:
            if not self.static_source:
                self._update_datasource(source, data)
            if hasattr(self, 'selected') and self.selected is not None:
                self._update_selected(source)
        elif self.document:
            server = self.renderer.mode == 'server'
            with hold_policy(self.document, 'collect', server=server):
                empty_data = {c: [] for c in columns}
                event = ModelChangedEvent(
                    document=self.document,
                    model=source,
                    attr='data',
                    new=empty_data,
                    setter='empty'
                )
                self.document.callbacks._held_events.append(event)

        if legend is not None:
            for leg in self.state.legend:
                for item in leg.items:
                    if renderer in item.renderers:
                        if isinstance(legend, dict):
                            label = legend
                        elif lp != 'legend':
                            prop = 'value' if 'label' in lp else 'field'
                            label = {prop: legend}
                        elif isinstance(item.label, dict):
                            label = {next(iter(item.label)): legend}
                        else:
                            label = {'value': legend}
                        item.label = label

        for glyph, update in glyph_updates:
            glyph.update(**update)

        if data is not None and cds_replace and not self.static_source:
            self._update_datasource(source, data)


    def _postprocess_hover(self, renderer, source):
        """Attaches renderer to hover tool and processes tooltips to
        ensure datetime data is displayed correctly.

        """
        hover = self.handles.get('hover')
        if hover is None:
            return
        if not isinstance(hover.tooltips, (str, Div)) and 'hv_created' in hover.tags:
            for k, values in source.data.items():
                key = f'@{{{k}}}'
                if (
                    (len(values) and isinstance(values[0], util.datetime_types)) or
                    (len(values) and isinstance(values[0], np.ndarray) and dtype_kind(values[0]) == 'M')
                ):
                    hover.tooltips = [(l, f+'{%F %T}' if f == key else f) for l, f in hover.tooltips]
                    hover.formatters[key] = "datetime"

        if hover.renderers == 'auto':
            hover.renderers = []
        if renderer not in hover.renderers:
            hover.renderers.append(renderer)

    def _init_glyphs(self, plot, element, ranges, source):
        style_element = element.last if self.batched else element

        # Get data and initialize data source
        if self.batched:
            current_id = tuple(element.traverse(lambda x: x._plot_id, [Element]))
            data, mapping, style = self.get_batched_data(element, ranges)
        else:
            style = self.style[self.cyclic_index]
            data, mapping, style = self.get_data(element, ranges, style)
            current_id = element._plot_id

        with abbreviated_exception():
            style = self._apply_transforms(element, data, ranges, style)

        if source is None:
            source = self._init_datasource(data)
        self.handles['previous_id'] = current_id
        self.handles['source'] = self.handles['cds'] = source
        self.handles['selected'] = source.selected

        properties = self._glyph_properties(plot, style_element, source, ranges, style)
        if 'legend_label' in properties and 'legend_field' in mapping:
            mapping.pop('legend_field')

        with abbreviated_exception():
            renderer, glyph = self._init_glyph(plot, mapping, properties)
        self.handles['glyph'] = glyph
        if isinstance(renderer, Renderer):
            self.handles['glyph_renderer'] = renderer

        self._postprocess_hover(renderer, source)

        if self.scalebar:
            self._draw_scalebar(plot=plot, renderer=renderer)

        zooms_subcoordy = self.handles.get('zooms_subcoordy')
        if zooms_subcoordy is not None:
            for zoom in zooms_subcoordy.values():
                # The default renderer is 'auto', instead we want to
                # store the subplot renderer to aggregate them and set
                # the final tool with a list of all the renderers.
                zoom.renderers = [renderer]

        # Update plot, source and glyph
        with abbreviated_exception():
            self._update_glyph(renderer, properties, mapping, glyph, source, source.data)

    def _find_axes(self, plot, element):
        """Looks up the axes and plot ranges given the plot and an element.

        """
        axis_dims = self._get_axis_dims(element)[:2]
        x, y = axis_dims[::-1] if self.invert_axes else axis_dims
        if isinstance(x, Dimension) and x.name in plot.extra_x_ranges:
            x_range = plot.extra_x_ranges[x.name]
            xaxes = [xaxis for xaxis in plot.xaxis if xaxis.x_range_name == x.name]
            x_axis = (xaxes if xaxes else plot.xaxis)[0]
        else:
            x_range = plot.x_range
            x_axis = plot.xaxis[0]
        if isinstance(y, Dimension) and y.name in plot.extra_y_ranges:
            y_range = plot.extra_y_ranges[y.name]
            yaxes = [yaxis for yaxis in plot.yaxis if yaxis.y_range_name == y.name]
            y_axis = (yaxes if yaxes else plot.yaxis)[0]
        else:
            y_range = plot.y_range
            y_axis = plot.yaxis[0]
        return (x_axis, y_axis), (x_range, y_range)

    def initialize_plot(self, ranges=None, plot=None, plots=None, source=None):
        """Initializes a new plot object with the last available frame.

        """
        # Get element key and ranges for frame
        if self.batched:
            element = [el for el in self.hmap.data.values() if el][-1]
        else:
            element = self.hmap.last
        key = util.wrap_tuple(self.hmap.last_key)
        ranges = self.compute_ranges(self.hmap, key, ranges)
        self.current_ranges = ranges
        self.current_frame = element
        self.current_key = key
        style_element = element.last if self.batched else element
        ranges = util.match_spec(style_element, ranges)
        # Initialize plot, source and glyph
        if plot is None:
            plot = self._init_plot(key, style_element, ranges=ranges, plots=plots)
            self._populate_axis_handles(plot)
        else:
            axes, plot_ranges = self._find_axes(plot, element)
            self.handles['xaxis'], self.handles['yaxis'] = axes
            self.handles['x_range'], self.handles['y_range'] = plot_ranges
            if self._subcoord_overlaid:
                if style_element.label in plot.extra_y_ranges:
                    self.handles['subcoordinate_y_range'] = plot.y_range
                    self.handles['y_range'] = plot.extra_y_ranges.pop(style_element.label)
                elif self.overlay_dims:
                    key = ', '.join(d.pprint_value(v) for d, v in self.overlay_dims.items())
                    self.handles['y_range'] = plot.extra_y_ranges.pop(key)

        if self.apply_hard_bounds:
            self._apply_hard_bounds(element, ranges)

        self.handles['plot'] = plot

        if self.autorange:
            self._setup_autorange()
        self._init_glyphs(plot, element, ranges, source)
        if not self.overlaid:
            self._update_plot(key, plot, style_element)
            self._update_ranges(style_element, ranges)

        for cb in self.callbacks:
            cb.initialize()

        if self.top_level:
            self.init_links()

        if not self.overlaid:
            self._set_active_tools(plot)
            self._process_legend()
            self._setup_data_callbacks(plot)
        self._execute_hooks(element)

        self.drawn = True

        return plot

    def _apply_hard_bounds(self, element, ranges):
        """Apply hard bounds to the x and y ranges of the plot. If xlim/ylim is set, limit the
        initial viewable range to xlim/ylim, but allow navigation up to the abs max between
        the data range and xlim/ylim. If dim range is set (e.g. via redim.range), enforce
        as hard bounds.

        """

        def validate_bound(bound):
            return bound if util.isfinite(bound) else None

        min_extent_x, min_extent_y, max_extent_x, max_extent_y = map(
            validate_bound, self.get_extents(element, ranges, range_type='combined', lims_as_soft_ranges=True)
        )

        def set_bounds(axis, min_extent, max_extent):
            """Set the bounds for a given axis, using None if both extents are None or identical"""
            try:
                self.handles[axis].bounds = None if min_extent == max_extent else (min_extent, max_extent)
            except ValueError:
                self.handles[axis].bounds = None

        set_bounds('x_range', min_extent_x, max_extent_x)
        set_bounds('y_range', min_extent_y, max_extent_y)

    def _setup_data_callbacks(self, plot):
        if not self._js_on_data_callbacks:
            return
        for renderer in plot.select({'type': GlyphRenderer}):
            cds = renderer.data_source
            for cb in self._js_on_data_callbacks:
                if cb not in cds.js_property_callbacks.get('change:data', []):
                    cds.js_on_change('data', cb)

    def _update_glyphs(self, element, ranges, style):
        plot = self.handles['plot']
        glyph = self.handles.get('glyph')
        source = self.handles['source']
        mapping = {}

        # Cache frame object id to skip updating data if unchanged
        previous_id = self.handles.get('previous_id', None)
        if self.batched:
            current_id = tuple(element.traverse(lambda x: x._plot_id, [Element]))
        else:
            current_id = element._plot_id
        self.handles['previous_id'] = current_id
        self.static_source = (self.dynamic and (current_id == previous_id))
        if self.batched:
            data, mapping, style = self.get_batched_data(element, ranges)
        else:
            data, mapping, style = self.get_data(element, ranges, style)

        # Include old data if source static
        if self.static_source:
            for k, v in source.data.items():
                if k not in data:
                    data[k] = v
                elif not len(data[k]) and len(source.data):
                    data[k] = source.data[k]

        with abbreviated_exception():
            style = self._apply_transforms(element, data, ranges, style)

        if glyph:
            properties = self._glyph_properties(plot, element, source, ranges, style)
            renderer = self.handles.get('glyph_renderer')
            if 'visible' in style and hasattr(renderer, 'visible'):
                renderer.visible = style['visible']
            with abbreviated_exception():
                self._update_glyph(renderer, properties, mapping, glyph, source, data)
        elif not self.static_source:
            self._update_datasource(source, data)


    def _reset_ranges(self):
        """Resets RangeXY streams if norm option is set to framewise

        """
        # Skipping conditional to temporarily revert fix (see https://github.com/holoviz/holoviews/issues/4396)
        # This fix caused PlotSize change events to rerender
        # rasterized/datashaded with the full extents which was wrong
        if self.overlaid or True:
            return
        for el, callbacks in self.traverse(lambda x: (x.current_frame, x.callbacks)):
            if el is None:
                continue
            for callback in callbacks:
                norm = self.lookup_options(el, 'norm').options
                if norm.get('framewise'):
                    for s in callback.streams:
                        if isinstance(s, RangeXY) and not s._triggering:
                            s.reset()

    @hold_render
    def update_frame(self, key, ranges=None, plot=None, element=None):
        """Updates an existing plot with data corresponding
        to the key.

        """
        self._reset_ranges()
        reused = isinstance(self.hmap, DynamicMap) and (self.overlaid or self.batched)
        self.prev_frame =  self.current_frame
        if not reused and element is None:
            element = self._get_frame(key)
        elif element is not None:
            self.current_key = key
            self.current_frame = element

        renderer = self.handles.get('glyph_renderer', None)
        glyph = self.handles.get('glyph', None)
        visible = element is not None
        if hasattr(renderer, 'visible'):
            renderer.visible = visible
        if hasattr(glyph, 'visible'):
            glyph.visible = visible

        if ((self.batched and not element) or element is None or (not self.dynamic and self.static) or
            (self.streaming and self.streaming[0].data is self.current_frame.data and not self.streaming[0]._triggering)):
            return

        if self.batched:
            style_element = element.last
            max_cycles = None
        else:
            style_element = element
            max_cycles = self.style._max_cycles
        style = self.lookup_options(style_element, 'style')
        self.style = style.max_cycles(max_cycles) if max_cycles else style

        if not self.overlaid:
            ranges = self.compute_ranges(self.hmap, key, ranges)
        else:
            self.ranges.update(ranges)
        self.param.update(**self.lookup_options(style_element, 'plot').options)
        ranges = util.match_spec(style_element, ranges)
        self.current_ranges = ranges
        plot = self.handles['plot']
        if not self.overlaid:
            self._update_ranges(style_element, ranges)
            self._update_plot(key, plot, style_element)
            self._set_active_tools(plot)
            self._setup_data_callbacks(plot)
            self._updated = True

        if 'hover' in self.handles:
            self._update_hover(element)
            if 'cds' in self.handles:
                cds = self.handles['cds']
                self._postprocess_hover(renderer, cds)

        if self.apply_hard_bounds:
            self._apply_hard_bounds(element, ranges)

        self._update_glyphs(element, ranges, self.style[self.cyclic_index])
        self._execute_hooks(element)


    def _execute_hooks(self, element):
        dtype_fix_hook(self, element)
        super()._execute_hooks(element)
        self._update_backend_opts()


    def model_changed(self, model):
        """Determines if the bokeh model was just changed on the frontend.
        Useful to suppress boomeranging events, e.g. when the frontend
        just sent an update to the x_range this should not trigger an
        update on the backend.

        """
        callbacks = [cb for cbs in self.traverse(lambda x: x.callbacks)
                     for cb in cbs]
        stream_metadata = [stream._metadata for cb in callbacks
                           for stream in cb.streams if stream._metadata]
        return any(md['id'] == model.ref['id'] for models in stream_metadata
                   for md in models.values())

    @property
    def framewise(self):
        """Property to determine whether the current frame should have
        framewise normalization enabled. Required for bokeh plotting
        classes to determine whether to send updated ranges for each
        frame.

        """
        current_frames = [el for f in self.traverse(lambda x: x.current_frame)
                          for el in (f.traverse(lambda x: x, [Element])
                                     if f else [])]
        current_frames = util.unique_iterator(current_frames)
        return any(self.lookup_options(frame, 'norm').options.get('framewise')
                   for frame in current_frames)

    def _draw_scalebar(self, *, plot, renderer):
        """Draw scalebar on the plot

        This will draw a scalebar on the plot. See the documentation for
        the parameters: `scalebar`, `scalebar_location`, `scalebar_label`,
        `scalebar_opts`, and `scalebar_unit` for more information.

        Requires Bokeh 3.4

        For scalebar on a subcoordinate_y plot Bokeh 3.6 is needed.

        """
        if not BOKEH_GE_3_4_0:
            raise RuntimeError("Scalebar requires Bokeh >= 3.4.0")
        elif not BOKEH_GE_3_6_0 and self._subcoord_overlaid:
            warn("Scalebar with subcoordinate_y requires Bokeh >= 3.6.0", RuntimeWarning)

        from bokeh.models import Metric, ScaleBar

        kdims = self.current_frame.kdims
        unit = self.scalebar_unit or kdims[0].unit or "m"
        if isinstance(unit, tuple):
            unit, base_unit = unit[:2]
        else:
            base_unit = unit

        if self._subcoord_overlaid:
            srange = renderer.coordinates.y_source
            orientation = "vertical"
            # Integer is used for the location as `major_label_overrides` overrides the
            # label with {0: labelA, 1: labelB}
            location = (self.scalebar_location or "right", len(plot.renderers) - 1)
            default_scalebar_opts = {
                'bar_line_width': 3,
                'label_location': 'left',
                'background_fill_color': 'white',
                'background_fill_alpha': 0.6,
                'length_sizing': 'adaptive',
                'bar_length_units': 'data',
                'bar_length': 0.8,
                # Adding location so people can overwrite the default
                'location': location,
            }
        else:
            srange = plot.x_range if self.scalebar_range == "x" else plot.y_range
            orientation = "horizontal" if self.scalebar_range == "x" else "vertical"
            location = self.scalebar_location or "bottom_right"
            default_scalebar_opts = {"background_fill_alpha": 0.8, "location": location}

        scale_bar = ScaleBar(
            range=srange,
            orientation=orientation,
            unit=unit,
            dimensional=Metric(base_unit=base_unit),
            label=self.scalebar_label,
            **dict(default_scalebar_opts, **self.scalebar_opts),
        )
        self.handles['scalebar'] = scale_bar
        plot.add_layout(scale_bar)

        if plot.toolbar and self.scalebar_tool:
            existing_tool = [t for t in plot.toolbar.tools if t.description == "Toggle ScaleBar"]
            if existing_tool:
                existing_tool[0].callback.args["scale_bars"] = plot.select(ScaleBar)
            else:
                ruler_icon = """\
                <?xml version="1.0" encoding="iso-8859-1"?>
                <svg fill="#a1a6a9" height="52px" width="52px" version="1.1" id="Layer_1" xmlns="http://www.w3.org/2000/svg"
                     xmlns:xlink="http://www.w3.org/1999/xlink" viewBox="0 0 512 512" xml:space="preserve">
                <g>
                    <g>
                        <path d="M402.826,0L0.001,402.827L109.174,512l402.826-402.826L402.826,0z M43.671,402.827l25.789-25.789l32.752,32.752
                            l21.834-21.834l-32.752-32.752l25.789-25.789l21.834,21.834l21.834-21.834l-21.834-21.834l25.789-25.789l21.834,21.834
                            l21.834-21.834l-21.834-21.834l25.79-25.79l32.752,32.752l21.834-21.834l-32.752-32.752l25.789-25.789l21.834,21.834
                            l21.834-21.834l-21.834-21.834l25.789-25.789l21.835,21.834l21.834-21.834l-21.834-21.834l25.79-25.79l32.752,32.752
                            l21.834-21.834L377.037,69.46l25.789-25.789l65.504,65.504L109.174,468.33L43.671,402.827z"/>
                    </g>
                </g>
                </svg>"""
                encoded_icon = base64.b64encode(dedent(ruler_icon).encode()).decode('ascii')
                active_kwargs = {"active_callback": "auto", "active": True} if BOKEH_GE_3_7_0 else {}
                scalebar_tool = CustomAction(
                    icon=f"data:image/svg+xml;base64,{encoded_icon}",
                    description="Toggle ScaleBar",
                    callback=CustomJS(
                        args={"scale_bars": plot.select(ScaleBar)},
                        code="""
                        export default ({scale_bars}) => {
                            for (let i = 0; i < scale_bars.length; i++) {
                                scale_bars[i].visible = !scale_bars[i].visible
                            }
                        }""",
                    ),
                    **active_kwargs,
                )
                plot.toolbar.tools.append(scalebar_tool)


class CompositeElementPlot(ElementPlot):
    """A CompositeElementPlot is an Element plot type that coordinates
    drawing of multiple glyphs.

    """

    # Mapping between glyph names and style groups
    _style_groups = {}

    # Defines the order in which glyphs are drawn, defined by glyph name
    _draw_order = []

    def _init_glyphs(self, plot, element, ranges, source, data=None, mapping=None, style=None):
        # Get data and initialize data source
        if None in (data, mapping):
            style = self.style[self.cyclic_index]
            data, mapping, style = self.get_data(element, ranges, style)

        keys = glyph_order(dict(data, **mapping), self._draw_order)

        source_cache = {}
        current_id = element._plot_id
        self.handles['previous_id'] = current_id
        for key in keys:
            style_group = self._style_groups.get('_'.join(key.split('_')[:-1]))
            group_style = dict(style)
            ds_data = data.get(key, {})
            with abbreviated_exception():
                group_style = self._apply_transforms(element, ds_data, ranges, group_style, style_group)
            if id(ds_data) in source_cache:
                source = source_cache[id(ds_data)]
            else:
                source = self._init_datasource(ds_data)
                source_cache[id(ds_data)] = source
            self.handles[key+'_source'] = source
            properties = self._glyph_properties(plot, element, source, ranges, group_style, style_group)
            properties = self._process_properties(key, properties, mapping.get(key, {}))

            with abbreviated_exception():
                renderer, glyph = self._init_glyph(plot, mapping.get(key, {}), properties, key)
            self.handles[key+'_glyph'] = glyph
            if isinstance(renderer, Renderer):
                self.handles[key+'_glyph_renderer'] = renderer

            self._postprocess_hover(renderer, source)

            # Update plot, source and glyph
            with abbreviated_exception():
                self._update_glyph(renderer, properties, mapping.get(key, {}), glyph,
                                   source, source.data)
        if getattr(self, 'colorbar', False):
            for k, v in list(self.handles.items()):
                if not k.endswith('color_mapper'):
                    continue
                self._draw_colorbar(plot, v, k.replace('color_mapper', ''))


    def _process_properties(self, key, properties, mapping):
        key = '_'.join(key.split('_')[:-1]) if '_' in key else key
        style_group = self._style_groups[key]
        group_props = {}
        for k, v in properties.items():
            if k in self.style_opts:
                group = k.split('_')[0]
                if group == style_group:
                    if k in mapping:
                        v = mapping[k]
                    k = '_'.join(k.split('_')[1:])
                else:
                    continue
            group_props[k] = v
        return group_props


    def _update_glyphs(self, element, ranges, style):
        plot = self.handles['plot']

        # Cache frame object id to skip updating data if unchanged
        previous_id = self.handles.get('previous_id', None)
        if self.batched:
            current_id = tuple(element.traverse(lambda x: x._plot_id, [Element]))
        else:
            current_id = element._plot_id
        self.handles['previous_id'] = current_id
        self.static_source = (self.dynamic and (current_id == previous_id))
        data, mapping, style = self.get_data(element, ranges, style)

        keys = glyph_order(dict(data, **mapping), self._draw_order)
        for key in keys:
            gdata = data.get(key)
            source = self.handles[key+'_source']
            glyph = self.handles.get(key+'_glyph')
            if glyph:
                group_style = dict(style)
                style_group = self._style_groups.get('_'.join(key.split('_')[:-1]))
                with abbreviated_exception():
                    group_style = self._apply_transforms(element, gdata, ranges, group_style, style_group)
                properties = self._glyph_properties(plot, element, source, ranges, group_style, style_group)
                properties = self._process_properties(key, properties, mapping[key])
                renderer = self.handles.get(key+'_glyph_renderer')
                with abbreviated_exception():
                    self._update_glyph(renderer, properties, mapping[key],
                                       glyph, source, gdata)
            elif not self.static_source and gdata is not None:
                self._update_datasource(source, gdata)


    def _init_glyph(self, plot, mapping, properties, key):
        """Returns a Bokeh glyph object.

        """
        properties = mpl_to_bokeh(properties)
        plot_method = '_'.join(key.split('_')[:-1])
        renderer = getattr(plot, plot_method)(**dict(properties, **mapping))
        return renderer, renderer.glyph


class ColorbarPlot(ElementPlot):
    """ColorbarPlot provides methods to create colormappers and colorbar
    models which can be added to a glyph. Additionally it provides
    parameters to control the position and other styling options of
    the colorbar. The default colorbar_position options are defined
    by the colorbar_specs, but may be overridden by the colorbar_opts.

    """

    colorbar_specs = {'right':     {'pos': 'right',
                                    'opts': {'location': (0, 0)}},
                      'left':      {'pos': 'left',
                                    'opts':{'location':(0, 0)}},
                      'bottom':    {'pos': 'below',
                                    'opts': {'location': (0, 0),
                                             'orientation':'horizontal'}},
                      'top':       {'pos': 'above',
                                    'opts': {'location':(0, 0),
                                             'orientation':'horizontal'}},
                      'top_right':   {'pos': 'center',
                                      'opts': {'location': 'top_right'}},
                      'top_left':    {'pos': 'center',
                                      'opts': {'location': 'top_left'}},
                      'bottom_left': {'pos': 'center',
                                      'opts': {'location': 'bottom_left',
                                               'orientation': 'horizontal'}},
                      'bottom_right': {'pos': 'center',
                                      'opts': {'location': 'bottom_right',
                                               'orientation': 'horizontal'}}}

    color_levels = param.ClassSelector(default=None, class_=(int, list, range), doc="""
        Number of discrete colors to use when colormapping or a set of color
        intervals defining the range of values to map each color to.""")

    cformatter = param.ClassSelector(
        default=None, class_=(str, TickFormatter, FunctionType), doc="""
        Formatter for ticks along the colorbar axis.""")

    clabel = param.String(default=None, doc="""
        An explicit override of the color bar label. If set, takes precedence
        over the title key in colorbar_opts.""")

    clim = param.Tuple(default=(np.nan, np.nan), length=2, doc="""
        User-specified colorbar axis range limits for the plot, as a tuple (low,high).
        If specified, takes precedence over data and dimension ranges.""")

    clim_percentile = param.ClassSelector(default=False, class_=(int, float, bool), doc="""
        Percentile value to compute colorscale robust to outliers. If
        True, uses 2nd and 98th percentile; otherwise uses the specified
        numerical percentile value.""")


    cnorm = param.Selector(default='linear', objects=['linear', 'log', 'eq_hist'], doc="""
        Color normalization to be applied during colormapping.""")

    colorbar = param.Boolean(default=False, doc="""
        Whether to display a colorbar.""")

    colorbar_position = param.Selector(objects=list(colorbar_specs),
                                             default="right", doc="""
        Allows selecting between a number of predefined colorbar position
        options. The predefined options may be customized in the
        colorbar_specs class attribute.""")

    colorbar_opts = param.Dict(default={}, doc="""
        Allows setting specific styling options for the colorbar overriding
        the options defined in the colorbar_specs class attribute. Includes
        location, orientation, height, width, scale_alpha, title, title_props,
        margin, padding, background_fill_color and more.""")

    clipping_colors = param.Dict(default={}, doc="""
        Dictionary to specify colors for clipped values, allows
        setting color for NaN values and for values above and below
        the min and max value. The min, max or NaN color may specify
        an RGB(A) color as a color hex string of the form #FFFFFF or
        #FFFFFFFF or a length 3 or length 4 tuple specifying values in
        the range 0-1 or a named HTML color.""")

    cticks = param.ClassSelector(class_=(int, list, tuple, np.ndarray, Ticker), default=None, doc="""
        Ticks along colorbar-axis specified as an integer, explicit list of
        tick locations, or bokeh Ticker object. If set to None default
        bokeh ticking behavior is applied.""")

    logz = param.Boolean(default=False, doc="""
         Whether to apply log scaling to the z-axis.""")

    rescale_discrete_levels = param.Boolean(default=True, doc="""
        If ``cnorm='eq_hist`` and there are only a few discrete values,
        then ``rescale_discrete_levels=True`` decreases the lower
        limit of the autoranged span so that the values are rendering
        towards the (more visible) top of the palette, thus
        avoiding washout of the lower values.  Has no effect if
        ``cnorm!=`eq_hist``.""")

    symmetric = param.Boolean(default=False, doc="""
        Whether to make the colormap symmetric around zero.""")

    _colorbar_defaults = dict(bar_line_color='black', label_standoff=8,
                              major_tick_line_color='black')

    _default_nan = '#8b8b8b'

    _nonvectorized_styles = [*base_properties, 'cmap', 'palette']

    def _draw_colorbar(self, plot, color_mapper, prefix=''):
        if CategoricalColorMapper and isinstance(color_mapper, CategoricalColorMapper):
            return
        if isinstance(color_mapper, EqHistColorMapper):
            ticker = BinnedTicker(mapper=color_mapper)
        elif isinstance(color_mapper, LogColorMapper) and color_mapper.low > 0:
            ticker = LogTicker()
        else:
            ticker = BasicTicker()
        cbar_opts = dict(self.colorbar_specs[self.colorbar_position])

        # Check if there is a colorbar in the same position
        pos = cbar_opts['pos']
        if any(isinstance(model, ColorBar) for model in getattr(plot, pos, [])):
            return

        if self.clabel:
            self.colorbar_opts.update({'title': self.clabel})

        if self.cformatter is not None:
            self.colorbar_opts.update({'formatter': wrap_formatter(self.cformatter, 'c')})

        if self.cticks is not None:
            self.colorbar_opts.update(get_ticker_axis_props(self.cticks))

        for tk in ['cticks', 'ticks']:
            ticksize = self._fontsize(tk, common=False).get('fontsize')
            if ticksize is not None:
                self.colorbar_opts.update({'major_label_text_font_size': ticksize})
                break

        for lb in ['clabel', 'labels']:
            labelsize = self._fontsize(lb, common=False).get('fontsize')
            if labelsize is not None:
                self.colorbar_opts.update({'title_text_font_size': labelsize})
                break

        opts = dict(cbar_opts['opts'], color_mapper=color_mapper, ticker=ticker,
                    **self._colorbar_defaults)
        color_bar = ColorBar(**dict(opts, **self.colorbar_opts))

        plot.add_layout(color_bar, pos)
        self.handles[prefix+'colorbar'] = color_bar


    def _get_colormapper(self, eldim, element, ranges, style, factors=None, colors=None,
                         group=None, name='color_mapper'):
        # The initial colormapper instance is cached the first time
        # and then only updated
        if eldim is None and colors is None:
            return None
        dim_name = dim_range_key(eldim)

        # Attempt to find matching colormapper on the adjoined plot
        if self.adjoined:
            cmappers = self.adjoined.traverse(
                lambda x: (x.handles.get('color_dim'),
                           x.handles.get(name),
                           [v for v in x.handles.values()
                            if isinstance(v, ColorMapper)])
                )
            cmappers = [(cmap, mappers) for cdim, cmap, mappers in cmappers
                        if cdim == eldim]
            if cmappers:
                cmapper, mappers  = cmappers[0]
                if not cmapper:
                    if mappers and mappers[0]:
                        cmapper = mappers[0]
                    else:
                        return None
                self.handles['color_mapper'] = cmapper
                return cmapper
            else:
                return None

        ncolors = None if factors is None else len(factors)
        if eldim:
            # check if there's an actual value (not np.nan)
            if all(util.isfinite(cl) for cl in self.clim):
                low, high = self.clim
            elif dim_name in ranges:
                if self.clim_percentile and 'robust' in ranges[dim_name]:
                    low, high = ranges[dim_name]['robust']
                else:
                    low, high = ranges[dim_name]['combined']
                dlow, dhigh = ranges[dim_name]['data']
                if (util.is_int(low, int_like=True) and
                    util.is_int(high, int_like=True) and
                    util.is_int(dlow) and
                    util.is_int(dhigh)):
                    low, high = int(low), int(high)
            elif isinstance(eldim, dim):
                # For dim objects, check if the referenced dimension has an explicit range
                actual_dim = element.get_dimension(eldim.dimension)
                if actual_dim and actual_dim.range != (None, None):
                    low, high = actual_dim.range
                # Fallback to data range if dimension lookup fails
                else:
                    low, high = element.range(eldim.dimension.name)
            else:
                low, high = element.range(eldim.name)
            if self.symmetric:
                sym_max = max(abs(low), high)
                low, high = -sym_max, sym_max
            low = self.clim[0] if util.isfinite(self.clim[0]) else low
            high = self.clim[1] if util.isfinite(self.clim[1]) else high
        else:
            low, high = None, None

        prefix = '' if group is None else group+'_'
        cmap = colors or style.get(prefix+'cmap', style.get('cmap', 'viridis'))
        nan_colors = {k: rgba_tuple(v) for k, v in self.clipping_colors.items()}
        if isinstance(cmap, dict):
            factors = list(cmap)
            palette = [cmap.get(f, nan_colors.get('NaN', self._default_nan)) for f in factors]
            if isinstance(eldim, dim):
                if eldim.dimension in element:
                    formatter = element.get_dimension(eldim.dimension).pprint_value
                else:
                    formatter = str
            else:
                formatter = eldim.pprint_value
            factors = [formatter(f) for f in factors]
        else:
            categorical = ncolors is not None
            if isinstance(self.color_levels, int):
                ncolors = self.color_levels
            elif isinstance(self.color_levels, list):
                ncolors = len(self.color_levels) - 1
                if isinstance(cmap, list) and len(cmap) != ncolors:
                    raise ValueError('The number of colors in the colormap '
                                     'must match the intervals defined in the '
                                     f'color_levels, expected {ncolors} colors found {len(cmap)}.')
            palette = process_cmap(cmap, ncolors, categorical=categorical)
            if isinstance(self.color_levels, list):
                palette, (low, high) = color_intervals(palette, self.color_levels, clip=(low, high))
        colormapper, opts = self._get_cmapper_opts(low, high, factors, nan_colors)

        cmapper = self.handles.get(name)
        if cmapper is not None:
            if cmapper.palette != palette:
                cmapper.palette = palette
            opts = {k: opt for k, opt in opts.items()
                    if getattr(cmapper, k) != opt}
            if opts:
                cmapper.update(**opts)
        else:
            cmapper = colormapper(palette=palette, **opts)
            self.handles[name] = cmapper
            self.handles['color_dim'] = eldim
        return cmapper


    def _get_color_data(self, element, ranges, style, name='color', factors=None, colors=None,
                        int_categories=False):
        data, mapping = {}, {}
        cdim = element.get_dimension(self.color_index)
        color = style.get(name, None)
        if cdim and ((isinstance(color, str) and color in element) or isinstance(color, dim)):
            self.param.warning(
                f"Cannot declare style mapping for '{name}' option and "
                "declare a color_index; ignoring the color_index.")
            cdim = None
        if not cdim:
            return data, mapping

        cdata = element.dimension_values(cdim)
        field = util.dimension_sanitizer(cdim.name)
        dtypes = 'iOSU' if int_categories else 'OSU'

        if factors is None and (isinstance(cdata, list) or dtype_kind(cdata) in dtypes):
            range_key = dim_range_key(cdim)
            if range_key in ranges and 'factors' in ranges[range_key]:
                factors = ranges[range_key]['factors']
            else:
                factors = util.unique_array(cdata)
        if factors is not None and int_categories and dtype_kind(cdata) == 'i':
            field += '_str__'
            cdata = [str(f) for f in cdata]
            factors = [str(f) for f in factors]

        mapper = self._get_colormapper(cdim, element, ranges, style,
                                       factors, colors)
        if factors is None and isinstance(mapper, CategoricalColorMapper):
            field += '_str__'
            cdata = [cdim.pprint_value(c) for c in cdata]
            factors = True

        data[field] = cdata
        if factors is not None and self.show_legend:
            mapping['legend_field'] = field
        mapping[name] = {'field': field, 'transform': mapper}

        return data, mapping


    def _get_cmapper_opts(self, low, high, factors, colors):
        if factors is None:
            opts = {}
            if self.cnorm == 'linear':
                colormapper = LinearColorMapper
            if self.cnorm == 'log' or self.logz:
                colormapper = LogColorMapper
                if util.is_int(low) and util.is_int(high) and low == 0:
                    low = 1
                    if 'min' not in colors:
                        # Make integer 0 be transparent
                        colors['min'] = 'rgba(0, 0, 0, 0)'
                elif util.is_number(low) and low <= 0:
                    self.param.warning(
                        "Log color mapper lower bound <= 0 and will not "
                        "render correctly. Ensure you set a positive "
                        "lower bound on the color dimension or using "
                        "the `clim` option."
                    )
            elif self.cnorm == 'eq_hist':
                colormapper = EqHistColorMapper
                opts['rescale_discrete_levels'] = self.rescale_discrete_levels
            if isinstance(low, (bool, np.bool_)): low = int(low)
            if isinstance(high, (bool, np.bool_)): high = int(high)
            # Pad zero-range to avoid breaking colorbar (as of bokeh 1.0.4)
            if low == high:
                offset = self.default_span / 2
                low -= offset
                high += offset
            if util.isfinite(low):
                opts['low'] = low
            if util.isfinite(high):
                opts['high'] = high
            color_opts = [('NaN', 'nan_color'), ('max', 'high_color'), ('min', 'low_color')]
            opts.update({opt: colors[name] for name, opt in color_opts if name in colors})
        else:
            colormapper = CategoricalColorMapper
            factors = map(str, decode_bytes(factors))
            opts = dict(factors=list(factors))
            if 'NaN' in colors:
                opts['nan_color'] = colors['NaN']
        return colormapper, opts


    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object and optionally creates a colorbar.

        """
        ret = super()._init_glyph(plot, mapping, properties)
        if self.colorbar:
            for k, v in list(self.handles.items()):
                if not k.endswith('color_mapper'):
                    continue
                self._draw_colorbar(plot, v, k.replace('color_mapper', ''))
        return ret


class LegendPlot(ElementPlot):

    legend_cols = param.Integer(default=0, bounds=(0, None), doc="""
        Number of columns for legend.""")

    legend_labels = param.Dict(default=None, doc="""
        Label overrides.""")

    legend_muted = param.Boolean(default=False, doc="""
        Controls whether the legend entries are muted by default.""")

    legend_offset = param.NumericTuple(default=(0, 0), doc="""
        If legend is placed outside the axis, this determines the
        (width, height) offset in pixels from the original position.""")

    legend_position = param.Selector(objects=["top_right",
                                                    "top_left",
                                                    "bottom_left",
                                                    "bottom_right",
                                                    'right', 'left',
                                                    'top', 'bottom'],
                                                    default="top_right",
                                                    doc="""
        Allows selecting between a number of predefined legend position
        options. The predefined options may be customized in the
        legend_specs class attribute.""")

    legend_opts = param.Dict(default={}, doc="""
        Allows setting specific styling options for the colorbar.""")

    legend_specs = {
        'right': 'right', 'left': 'left', 'top': 'above', 'bottom': 'below'
    }

    def _process_legend(self, plot=None):
        plot = plot or self.handles['plot']
        if not plot.legend:
            return
        legend = plot.legend[0]
        cmappers = [cmapper for cmapper in self.handles.values()
                   if isinstance(cmapper, CategoricalColorMapper)]
        categorical = bool(cmappers)
        if ((not categorical and not self.overlaid and len(legend.items) == 1)
            or not self.show_legend):
            legend.items[:] = []
        else:
            if self.legend_cols:
                plot.legend.nrows = self.legend_cols
            else:
                plot.legend.orientation = 'horizontal' if self.legend_cols else 'vertical'

            pos = self.legend_position
            if pos in self.legend_specs:
                plot.legend[:] = []
                legend.location = self.legend_offset
                if pos in ['top', 'bottom'] and not self.legend_cols:
                    plot.legend.orientation = 'horizontal'
                plot.add_layout(legend, self.legend_specs[pos])
            else:
                legend.location = pos

            # Apply muting and misc legend opts
            for leg in plot.legend:
                leg.update(**self.legend_opts)
                for item in leg.items:
                    for r in item.renderers:
                        r.muted = self.legend_muted


class AnnotationPlot:
    """Mix-in plotting subclass for AnnotationPlots which do not have a legend.

    """


class OverlayPlot(GenericOverlayPlot, LegendPlot):

    tabs = param.Boolean(default=False, doc="""
        Whether to display overlaid plots in separate panes""")

    style_opts = (legend_dimensions + ['border_'+p for p in line_properties] +
                  text_properties + ['background_fill_color', 'background_fill_alpha'])

    multiple_legends = param.Boolean(default=False, doc="""
        Whether to split the legend for subplots into multiple legends.""")

    _propagate_options = ['width', 'height', 'xaxis', 'yaxis', 'labelled',
                          'bgcolor', 'fontsize', 'invert_axes', 'show_frame',
                          'show_grid', 'logx', 'logy', 'xticks', 'toolbar',
                          'yticks', 'xrotation', 'yrotation', 'lod',
                          'border', 'invert_xaxis', 'invert_yaxis', 'sizing_mode',
                          'title', 'title_format', 'legend_position', 'legend_offset',
                          'legend_cols', 'gridstyle', 'legend_muted', 'padding',
                          'xlabel', 'ylabel', 'xlim', 'ylim', 'zlim',
                          'xformatter', 'yformatter', 'active_tools',
                          'min_height', 'max_height', 'min_width', 'min_height',
                          'margin', 'aspect', 'data_aspect', 'frame_width',
                          'frame_height', 'responsive', 'fontscale', 'subcoordinate_y',
                          'subcoordinate_scale', 'autorange', 'default_tools', 'autohide_toolbar']

    def __init__(self, overlay, **kwargs):
        self._multi_y_propagation = self.lookup_options(overlay, 'plot').options.get('multi_y', False)
        super().__init__(overlay, **kwargs)
        self.callbacks, self.source_streams = self._construct_callbacks()
        self._multi_y_propagation = False

    @property
    def _x_range_type(self):
        for v in self.subplots.values():
            if not isinstance(v._x_range_type, Range1d):
                return v._x_range_type
        return self._x_range_type

    @property
    def _y_range_type(self):
        for v in self.subplots.values():
            if not isinstance(v._y_range_type, Range1d):
                return v._y_range_type
        return self._y_range_type

    @property
    def _is_batched(self):
        return super()._is_batched and not self.subcoordinate_y

    def _process_legend(self, overlay):
        plot = self.handles['plot']
        subplots = self.traverse(lambda x: x, [lambda x: x is not self])
        legend_plots = any(p is not None for p in subplots
                           if isinstance(p, LegendPlot) and
                           not isinstance(p, OverlayPlot))
        non_annotation = [p for p in subplots if not
                          isinstance(p, (AnnotationPlot, OverlayPlot))]
        if (not self.show_legend or len(plot.legend) == 0 or
            (len(non_annotation) <= 1 and not (self.dynamic or legend_plots))):
            return super()._process_legend()
        elif not plot.legend:
            return

        legend = plot.legend[0]

        options = {}
        properties = self.lookup_options(self.hmap.last, 'style')[self.cyclic_index]
        for k, v in properties.items():
            if k in line_properties and 'line' not in k:
                ksplit = k.split('_')
                k = '_'.join(ksplit[:1]+'line'+ksplit[1:])
            if k in text_properties:
                k = 'label_' + k
            if k.startswith('legend_'):
                k = k[7:]
            options[k] = v

        pos = self.legend_position
        if pos in ['top', 'bottom'] and not self.legend_cols:
            options['orientation'] = 'horizontal'

        if overlay is not None and overlay.kdims:
            title = ', '.join([d.label for d in overlay.kdims])
            options['title'] = title

        options.update(self._fontsize('legend', 'label_text_font_size'))
        options.update(self._fontsize('legend_title', 'title_text_font_size'))
        if self.legend_cols:
            options.update({"ncols": self.legend_cols})
        legend.update(**options)

        if pos in self.legend_specs:
            pos = self.legend_specs[pos]
        else:
            legend.location = pos

        if 'legend_items' not in self.handles:
            self.handles['legend_items'] = []
        legend_items = self.handles['legend_items']
        legend_labels = {
            tuple(sorted(property_to_dict(i.label).items()))
            if isinstance(property_to_dict(i.label), dict) else i.label: i
            for i in legend_items
        }
        for item in legend.items:
            item_label = property_to_dict(item.label)
            label = tuple(sorted(item_label.items())) if isinstance(item_label, dict) else item_label
            if not label or (isinstance(item_label, dict) and not item_label.get('value', True)):
                continue

            if label in legend_labels:
                prev_item = legend_labels[label]
                prev_item.renderers[:] = list(util.unique_iterator(prev_item.renderers+item.renderers))
            else:
                legend_labels[label] = item
                legend_items.append(item)
                if item not in self.handles['legend_items']:
                    self.handles['legend_items'].append(item)

        # Ensure that each renderer is only singly referenced by a legend item
        filtered = []
        renderers = []
        for item in legend_items:
            item.renderers[:] = [r for r in item.renderers if r not in renderers]
            if (item in filtered or not item.renderers or
                not any(r.visible or 'hv_legend' in r.tags for r in item.renderers)):
                continue
            item_label = property_to_dict(item.label)
            if isinstance(item_label, dict) and 'value' in item_label and self.legend_labels:
                label = item_label['value']
                item.label = {'value': self.legend_labels.get(label, label)}
            renderers += item.renderers
            filtered.append(item)
        legend.items[:] = list(util.unique_iterator(filtered))

        if self.multiple_legends:
            remove_legend(plot, legend)
            properties = legend.properties_with_values(include_defaults=False)
            legend_group = []
            for item in legend.items:
                if not isinstance(item.label, dict) or 'value'  in item.label:
                    legend_group.append(item)
                    continue
                new_legend = Legend(**dict(properties, items=[item]))
                new_legend.location = self.legend_offset
                plot.add_layout(new_legend, pos)
            if legend_group:
                new_legend = Legend(**dict(properties, items=legend_group))
                new_legend.location = self.legend_offset
                plot.add_layout(new_legend, pos)
            legend.items[:] = []
        elif pos in ['above', 'below', 'right', 'left']:
            remove_legend(plot, legend)
            legend.location = self.legend_offset
            plot.add_layout(legend, pos)

        # Apply muting and misc legend opts
        for leg in plot.legend:
            leg.update(**self.legend_opts)
            for item in leg.items:
                for r in item.renderers:
                    r.muted = self.legend_muted or r.muted

    def _init_tools(self, element, callbacks=None):
        """Processes the list of tools to be supplied to the plot.

        """
        if callbacks is None:
            callbacks = []
        hover_tools = {}
        zooms_subcoordy = {}
        _zoom_types = (tools.WheelZoomTool, tools.ZoomInTool, tools.ZoomOutTool)
        init_tools, tool_types = [], []
        for key, subplot in self.subplots.items():
            el = element.get(key)
            if el is not None:
                el_tools = subplot._init_tools(el, self.callbacks)
                for tool in el_tools:
                    if isinstance(tool, str):
                        tool_type = TOOL_TYPES.get(tool)
                    else:
                        tool_type = type(tool)
                    if isinstance(tool, tools.HoverTool):
                        if isinstance(tool.tooltips, bokeh.models.dom.Div):
                            tooltips = tool.tooltips
                        else:
                            tooltips = tuple(tool.tooltips) if tool.tooltips else ()
                        if tooltips in hover_tools:
                            continue
                        else:
                            hover_tools[tooltips] = tool
                    elif (
                        self.subcoordinate_y and isinstance(tool, _zoom_types)
                        and 'hv_created' in tool.tags and len(tool.tags) == 2
                    ):
                        if tool.tags[1] in zooms_subcoordy:
                            continue
                        else:
                            zooms_subcoordy[tool.tags[1]] = tool
                            self.handles['zooms_subcoordy'] = zooms_subcoordy
                    elif tool_type in tool_types:
                        continue
                    else:
                        tool_types.append(tool_type)
                    init_tools.append(tool)
        self.handles['hover_tools'] = hover_tools
        return init_tools

    def _merge_tools(self, subplot):
        """Merges tools on the overlay with those on the subplots.

        """
        if self.batched and 'hover' in subplot.handles:
            self.handles['hover'] = subplot.handles['hover']
        elif 'hover' in subplot.handles and 'hover_tools' in self.handles:
            hover = subplot.handles['hover']
            if hover.tooltips and isinstance(hover.tooltips, bokeh.models.dom.Div):
                tooltips = hover.tooltips
            elif hover.tooltips and not isinstance(hover.tooltips, str):
                tooltips = tuple((name, spec.replace('{%F %T}', '')) for name, spec in hover.tooltips)
            else:
                tooltips = ()
            tool = self.handles['hover_tools'].get(tooltips)
            if tool:
                tool_renderers = [] if tool.renderers == 'auto' else tool.renderers
                hover_renderers = [] if hover.renderers == 'auto' else hover.renderers
                renderers = [r for r in tool_renderers + hover_renderers if r is not None]
                tool.renderers = list(util.unique_iterator(renderers))
                if 'hover' not in self.handles:
                    self.handles['hover'] = tool
        if 'zooms_subcoordy' in subplot.handles and 'zooms_subcoordy' in self.handles:
            for subplot_zoom, overlay_zoom in zip(
                subplot.handles['zooms_subcoordy'].values(),
                self.handles['zooms_subcoordy'].values(), strict=None,
            ):
                renderers = list(util.unique_iterator(overlay_zoom.renderers + subplot_zoom.renderers))
                overlay_zoom.renderers = renderers

    def _postprocess_subcoordinate_y_groups(self, overlay, plot):
        """Add a zoom tool per group to the overlay.

        """
        # First, just process and validate the groups and their content.
        groups = defaultdict(list)

        # If there are groups AND there are subcoordinate_y elements without a group.
        if any(el.group != type(el).__name__ for el in overlay) and any(
            el.opts.get('plot').kwargs.get('subcoordinate_y', False)
            and el.group == type(el).__name__
            for el in overlay
        ):
            raise ValueError(
                'The subcoordinate_y overlay contains elements with a defined group, each '
                'subcoordinate_y element in the overlay must have a defined group.'
            )

        for el in overlay:
            # group is the Element type per default (e.g. Curve, Spike).
            if el.group == type(el).__name__:
                continue
            if not el.opts.get('plot').kwargs.get('subcoordinate_y', False):
                raise ValueError(
                    f"All elements in group {el.group!r} must set the option "
                    f"'subcoordinate_y=True'. Not found for: {el}"
                )
            groups[el.group].append(el)

        # No need to go any further if there's just one group.
        if len(groups) <= 1:
            return

        # At this stage, there's only one zoom tool (e.g. 1 wheel_zoom) that
        # has all the renderers (e.g. all the curves in the overlay).
        # For the non wheel_zoom tools, we want to create as many zoom tools
        # as groups, for each group the zoom tool must have the renderers of
        # the elements of the group.
        # For the wheel_zoom tool, if Bokeh 3.5 is used, we want to enable
        # the group hit-testing behavior.
        zoom_tools = self.handles['zooms_subcoordy']
        for zoom_tool_name, zoom_tool in zoom_tools.items():
            renderers_per_group = defaultdict(list)
            # We loop through each overlay sub-elements and empty the list of
            # renderers of the initial tool.
            for el in overlay:
                if el.group not in groups:
                    continue
                renderers_per_group[el.group].append(zoom_tool.renderers.pop(0))

            if zoom_tool.renderers:
                raise RuntimeError(f'Found unexpected zoom renderers {zoom_tool.renderers}')

            if zoom_tool_name == 'wheel_zoom' and BOKEH_GE_3_5_0:
                zoom_tool.update(
                    hit_test=True,
                    hit_test_mode='hline',
                    hit_test_behavior=list(renderers_per_group.values()),
                    renderers=list(chain(*renderers_per_group.values())),
                )
            else:
                new_ztools = []
                # Create a new tool per group with the right renderers and a custom description.
                for grp, grp_renderers in renderers_per_group.items():
                    new_tool = zoom_tool.clone()
                    new_tool.renderers = grp_renderers
                    new_tool.description = f"{zoom_tool_name.replace('_', ' ').title()} ({grp})"
                    new_ztools.append(new_tool)
                # Revert tool order so the upper tool in the toolbar corresponds to the
                # upper group in the overlay.
                new_ztools = new_ztools[::-1]

                # Update the handle for good measure.
                zoom_tools[zoom_tool_name] = new_ztools

                # Replace the original tool by the new ones
                idx = plot.tools.index(zoom_tool)
                plot.tools[idx:idx+1] = new_ztools

    def _get_dimension_factors(self, overlay, ranges, dimension):
        factors = []
        for k, sp in self.subplots.items():
            el = overlay.data.get(k)
            if el is None or not sp.apply_ranges or not sp._has_axis_dimension(el, dimension):
                continue
            dim = el.get_dimension(dimension)
            elranges = util.match_spec(el, ranges)
            fs = sp._get_dimension_factors(el, elranges, dim)
            if len(fs):
                factors.append(fs)
        return list(util.unique_iterator(chain(*factors)))

    def _get_factors(self, overlay, ranges):
        xfactors, yfactors = [], []
        for k, sp in self.subplots.items():
            el = overlay.data.get(k)
            if el is not None:
                elranges = util.match_spec(el, ranges)
                xfs, yfs = sp._get_factors(el, elranges)
                if len(xfs):
                    xfactors.append(xfs)
                if len(yfs):
                    yfactors.append(yfs)
        xfactors = list(util.unique_iterator(chain(*xfactors)))
        yfactors = list(util.unique_iterator(chain(*yfactors)))
        return xfactors, yfactors

    def _get_axis_dims(self, element):
        subplots = list(self.subplots.values())
        if subplots:
            return subplots[0]._get_axis_dims(element)
        return super()._get_axis_dims(element)

    def initialize_plot(self, ranges=None, plot=None, plots=None):
        if self.multi_y and self.subcoordinate_y:
            raise ValueError('multi_y and subcoordinate_y are not supported together.')
        if self.subcoordinate_y:
            labels = self.hmap.last.traverse(lambda x: x.label, [
                lambda el: isinstance(el, Element) and el.opts.get('plot').kwargs.get('subcoordinate_y', False)
            ])
            if isinstance(self.hmap.last, NdOverlay):
                pass
            elif any(not label for label in labels):
                raise ValueError(
                    'Every Element plotted on a subcoordinate_y axis must have '
                    'a label or be part of an NdOverlay.'
                )
            elif len(set(labels)) != len(labels):
                raise ValueError(
                    'Elements wrapped in a subcoordinate_y overlay must all have '
                    'a unique label.'
                )
        key = util.wrap_tuple(self.hmap.last_key)
        nonempty = [(k, el) for k, el in self.hmap.data.items() if el]
        if not nonempty:
            raise SkipRendering('All Overlays empty, cannot initialize plot.')
        dkey, element = nonempty[-1]
        ranges = self.compute_ranges(self.hmap, key, ranges)

        self.tabs = self.tabs or any(isinstance(sp, TablePlot) for sp in self.subplots.values())
        if plot is None and not self.tabs and not self.batched:
            plot = self._init_plot(key, element, ranges=ranges, plots=plots)
            self._populate_axis_handles(plot)
        self.handles['plot'] = plot

        if plot and not self.overlaid:
            self._update_plot(key, plot, element)
            self._update_ranges(element, ranges)

        panels = []
        subcoord_y_glyph_renderers = []
        for key, subplot in self.subplots.items():
            frame = None
            if self.tabs:
                subplot.overlaid = False
            child = subplot.initialize_plot(ranges, plot, plots)
            if isinstance(element, CompositeOverlay):
                # Ensure that all subplots are in the same state
                frame = element.get(key, None)
                subplot.current_frame = frame
                subplot.current_key = dkey
            if self.batched:
                self.handles['plot'] = child
            if self.tabs:
                title = subplot._format_title(key, dimensions=False)
                if not title:
                    title = get_tab_title(key, frame, self.hmap.last)
                panels.append(TabPanel(child=child, title=title))
            self._merge_tools(subplot)
            if getattr(subplot, "subcoordinate_y", False) and (
                glyph_renderer := subplot.handles.get("glyph_renderer")
            ):
                subcoord_y_glyph_renderers.append(glyph_renderer)

        if self.subcoordinate_y and plot:
            # Reverse the subcoord-y renderers only.
            reversed_renderers = subcoord_y_glyph_renderers[::-1]
            reordered = []
            for item in plot.renderers:
                if item not in subcoord_y_glyph_renderers:
                    reordered.append(item)
                else:
                    reordered.append(reversed_renderers.pop(0))
            plot.renderers = reordered

        if self.subcoordinate_y:
            self._postprocess_subcoordinate_y_groups(element, plot)

        if self.tabs:
            self.handles['plot'] = Tabs(
                tabs=panels, width=self.width, height=self.height,
                min_width=self.min_width, min_height=self.min_height,
                max_width=self.max_width, max_height=self.max_height,
                sizing_mode='fixed'
            )
        elif not self.overlaid:
            self._process_legend(element)
            self._set_active_tools(plot)
        self.drawn = True
        self.handles['plots'] = plots

        if 'plot' in self.handles and not self.tabs:
            plot = self.handles['plot']
            self.handles['xaxis'] = plot.xaxis[0]
            self.handles['yaxis'] = plot.yaxis[0]
            self.handles['x_range'] = plot.x_range
            self.handles['y_range'] = plot.y_range

        for cb in self.callbacks:
            cb.initialize()

        if self.top_level:
            self.init_links()

        if self.autorange:
            self._setup_autorange()

        self._execute_hooks(element)

        return self.handles['plot']

    @hold_render
    def update_frame(self, key, ranges=None, element=None):
        """Update the internal state of the Plot to represent the given
        key tuple (where integers represent frames). Returns this
        state.

        """
        self._reset_ranges()
        reused = isinstance(self.hmap, DynamicMap) and self.overlaid
        self.prev_frame =  self.current_frame
        if not reused and element is None:
            element = self._get_frame(key)
        elif element is not None:
            self.current_frame = element
            self.current_key = key
        items = [] if element is None else list(element.data.items())

        if isinstance(self.hmap, DynamicMap):
            range_obj = element
        else:
            range_obj = self.hmap

        if element is not None:
            ranges = self.compute_ranges(range_obj, key, ranges)

            # Update plot options
            plot_opts = self.lookup_options(element, 'plot').options
            inherited = self._traverse_options(element, 'plot',
                                               self._propagate_options,
                                               defaults=False)
            plot_opts.update(**{k: v[0] for k, v in inherited.items() if k not in plot_opts})
            self.param.update(**plot_opts)

            if not self.overlaid and not self.tabs and not self.batched:
                self._update_ranges(element, ranges)

        # Determine which stream (if any) triggered the update
        triggering = [stream for stream in self.streams if stream._triggering]
        for k, subplot in self.subplots.items():
            el = None

            # If in Dynamic mode propagate elements to subplots
            if isinstance(self.hmap, DynamicMap) and element:
                # In batched mode NdOverlay is passed to subplot directly
                if self.batched:
                    el = element
                # If not batched get the Element matching the subplot
                elif element is not None:
                    idx, _spec, exact = self._match_subplot(k, subplot, items, element)
                    if idx is not None and exact:
                        _, el = items.pop(idx)

                # Skip updates to subplots when its streams is not one of
                # the streams that initiated the update
                if (triggering and all(s not in triggering for s in subplot.streams) and
                    subplot not in self.dynamic_subplots):
                    continue
            subplot.update_frame(key, ranges, element=el)

        if not self.batched and isinstance(self.hmap, DynamicMap) and items:
            init_kwargs = {'plots': self.handles['plots']}
            if not self.tabs:
                init_kwargs['plot'] = self.handles['plot']
            self._create_dynamic_subplots(key, items, ranges, **init_kwargs)
            if overlay_hover := self.handles.get('hover'):
                if overlay_hover.renderers == 'auto':
                    overlay_hover.renderers = []
                for k, _ in items:
                    if k in self.subplots and 'glyph_renderer' in self.subplots[k].handles:
                        renderer = self.subplots[k].handles['glyph_renderer']
                        if renderer not in overlay_hover.renderers:
                            overlay_hover.renderers.append(renderer)
            if not self.overlaid and not self.tabs:
                self._process_legend(element)

        if element and not self.overlaid and not self.tabs and not self.batched:
            plot = self.handles['plot']
            self._update_plot(key, plot, element)
            self._set_active_tools(plot)
            self._setup_data_callbacks(plot)

        self._updated = True
        self._process_legend(element)
        self._execute_hooks(element)
