import calendar
import datetime as dt
import re
import time
from collections import defaultdict
from contextlib import contextmanager, suppress
from itertools import permutations

import numpy as np
from bokeh.core.property.datetime import Datetime
from bokeh.core.validation import silence
from bokeh.core.validation.check import is_silenced
from bokeh.layouts import group_tools
from bokeh.model import Model
from bokeh.models import CustomJS, tools
from bokeh.models.axes import (
    CategoricalAxis,
    DatetimeAxis,
    LinearAxis,
    LogAxis,
    MercatorAxis,
)
from bokeh.models.formatters import PrintfTickFormatter, TickFormatter
from bokeh.models.layouts import Column, GridBox, LayoutDOM, Row, Spacer, Tabs
from bokeh.models.plots import GridPlot, Plot
from bokeh.models.ranges import DataRange1d, FactorRange, Range1d
from bokeh.models.scales import CategoricalScale, LinearScale, LogScale
from bokeh.models.tickers import BasicTicker, FixedTicker, Ticker
from bokeh.models.widgets import DataTable, Div
from bokeh.plotting import figure
from bokeh.themes import built_in_themes
from bokeh.themes.theme import Theme

from ...core import util
from ...core.layout import Layout
from ...core.ndmapping import NdMapping
from ...core.overlay import NdOverlay, Overlay
from ...core.spaces import DynamicMap, get_nested_dmaps
from ...core.util import (
    arraylike_types,
    callable_name,
    cftime_to_timestamp,
    cftime_types,
    dtype_kind,
    isnumeric,
    unique_array,
)
from ...core.util.dependencies import _no_import_version
from ...util.warnings import warn
from ..util import dim_axis_label

BOKEH_VERSION = _no_import_version("bokeh")
BOKEH_GE_3_2_0 = BOKEH_VERSION >= (3, 2, 0)
BOKEH_GE_3_3_0 = BOKEH_VERSION >= (3, 3, 0)
BOKEH_GE_3_4_0 = BOKEH_VERSION >= (3, 4, 0)
BOKEH_GE_3_5_0 = BOKEH_VERSION >= (3, 5, 0)
BOKEH_GE_3_6_0 = BOKEH_VERSION >= (3, 6, 0)
BOKEH_GE_3_7_0 = BOKEH_VERSION >= (3, 7, 0)
BOKEH_GE_3_8_0 = BOKEH_VERSION >= (3, 8, 0)
BOKEH_GE_3_9_0 = BOKEH_VERSION >= (3, 9, 0)


if BOKEH_GE_3_8_0:
    from bokeh.models.axes import TimedeltaAxis


TOOL_TYPES = {
    'pan': tools.PanTool,
    'xpan': tools.PanTool,
    'ypan': tools.PanTool,
    'xwheel_pan': tools.WheelPanTool,
    'ywheel_pan': tools.WheelPanTool,
    'wheel_zoom': tools.WheelZoomTool,
    'xwheel_zoom': tools.WheelZoomTool,
    'ywheel_zoom': tools.WheelZoomTool,
    'zoom_in': tools.ZoomInTool,
    'xzoom_in': tools.ZoomInTool,
    'yzoom_in': tools.ZoomInTool,
    'zoom_out': tools.ZoomOutTool,
    'xzoom_out': tools.ZoomOutTool,
    'yzoom_out': tools.ZoomOutTool,
    'click': tools.TapTool,
    'tap': tools.TapTool,
    'doubletap': tools.TapTool,
    'crosshair': tools.CrosshairTool,
    'xcrosshair': tools.CrosshairTool,
    'ycrosshair': tools.CrosshairTool,
    'box_select': tools.BoxSelectTool,
    'xbox_select': tools.BoxSelectTool,
    'ybox_select': tools.BoxSelectTool,
    'poly_select': tools.PolySelectTool,
    'lasso_select': tools.LassoSelectTool,
    'auto_box_zoom': tools.BoxZoomTool,
    'box_zoom': tools.BoxZoomTool,
    'xbox_zoom': tools.BoxZoomTool,
    'ybox_zoom': tools.BoxZoomTool,
    'hover': tools.HoverTool,
    'save': tools.SaveTool,
    'undo': tools.UndoTool,
    'redo': tools.RedoTool,
    'reset': tools.ResetTool,
    'help': tools.HelpTool,
    'box_edit': tools.BoxEditTool,
    'point_draw': tools.PointDrawTool,
    'poly_draw': tools.PolyDrawTool,
    'poly_edit': tools.PolyEditTool,
    'freehand_draw': tools.FreehandDrawTool,
    'copy': tools.CopyTool,
    'examine': tools.ExamineTool,
    'fullscreen': tools.FullscreenTool,
    'line_edit': tools.LineEditTool,
}

if BOKEH_GE_3_6_0:
    TOOL_TYPES.update({
        'pan_down': tools.ClickPanTool,
        'pan_east': tools.ClickPanTool,
        'pan_left': tools.ClickPanTool,
        'pan_north': tools.ClickPanTool,
        'pan_right': tools.ClickPanTool,
        'pan_south': tools.ClickPanTool,
        'pan_up': tools.ClickPanTool,
        'pan_west': tools.ClickPanTool,
    })


def convert_timestamp(timestamp):
    """Converts bokehJS timestamp to datetime64.

    """
    datetime = dt.datetime.fromtimestamp(timestamp/1000, tz=dt.timezone.utc)
    return np.datetime64(datetime.replace(tzinfo=None))


def prop_is_none(value):
    """Checks if property value is None.

    """
    return (value is None or
            (isinstance(value, dict) and 'value' in value
             and value['value'] is None))


def decode_bytes(array):
    """Decodes an array, list or tuple of bytestrings to avoid python 3
    bokeh serialization errors

    """
    if (not len(array) or (isinstance(array, arraylike_types) and dtype_kind(array) != 'O')):
        return array
    decoded = [v.decode('utf-8') if isinstance(v, bytes) else v for v in array]
    if isinstance(array, np.ndarray):
        return np.asarray(decoded)
    elif isinstance(array, tuple):
        return tuple(decoded)
    return decoded


def layout_padding(plots, renderer):
    """Pads Nones in a list of lists of plots with empty plots.

    """
    widths, heights = defaultdict(int), defaultdict(int)
    for r, row in enumerate(plots):
        for c, p in enumerate(row):
            if p is not None:
                width, height = renderer.get_size(p)
                widths[c] = max(widths[c], width)
                heights[r] = max(heights[r], height)

    expanded_plots = []
    for r, row in enumerate(plots):
        expanded_plots.append([])
        for c, p in enumerate(row):
            if p is None:
                p = empty_plot(widths[c], heights[r])
            elif hasattr(p, 'width') and p.width == 0 and p.height == 0:
                p.width = widths[c]
                p.height = heights[r]
            expanded_plots[r].append(p)
    return expanded_plots


def compute_plot_size(plot):
    """Computes the size of bokeh models that make up a layout such as
    figures, rows, columns, and Plot.

    """
    if isinstance(plot, (GridBox, GridPlot)):
        ndmapping = NdMapping({(x, y): fig for fig, y, x in plot.children}, kdims=['x', 'y'])
        cols = ndmapping.groupby('x')
        rows = ndmapping.groupby('y')
        width = sum([max([compute_plot_size(f)[0] for f in col]) for col in cols])
        height = sum([max([compute_plot_size(f)[1] for f in row]) for row in rows])
        return width, height
    elif isinstance(plot, (Div, tools.Toolbar)):
        # Cannot compute size for Div or Toolbar
        return 0, 0
    elif isinstance(plot, (Row, Column, Tabs)):
        if not plot.children: return 0, 0
        if isinstance(plot, Row) or (isinstance(plot, tools.Toolbar) and plot.toolbar_location not in ['right', 'left']):
            w_agg, h_agg = (np.sum, np.max)
        elif isinstance(plot, Tabs):
            w_agg, h_agg = (np.max, np.max)
        else:
            w_agg, h_agg = (np.max, np.sum)
        widths, heights = zip(*[compute_plot_size(child) for child in plot.children], strict=None)
        return w_agg(widths), h_agg(heights)
    elif isinstance(plot, figure):
        if plot.width:
            width = plot.width
        else:
            width = plot.frame_width + plot.min_border_right + plot.min_border_left
        if plot.height:
            height = plot.height
        else:
            height = plot.frame_height + plot.min_border_bottom + plot.min_border_top
        return width, height
    elif isinstance(plot, (Plot, DataTable, Spacer)):
        return plot.width, plot.height
    else:
        return 0, 0


def compute_layout_properties(
        width, height, frame_width, frame_height, explicit_width,
        explicit_height, aspect, data_aspect, responsive, size_multiplier,
        logger=None):
    """Utility to compute the aspect, plot width/height and sizing_mode
    behavior.

    Parameters
    ----------
    width : int
        Plot width
    height : int
        Plot height
    frame_width : int
        Plot frame width
    frame_height : int
        Plot frame height
    explicit_width : list
        List of user supplied widths
    explicit_height : list
        List of user supplied heights
    aspect : float
        Plot aspect
    data_aspect : float
        Scaling between x-axis and y-axis ranges
    responsive : boolean
        Whether the plot should resize responsively
    size_multiplier : float
        Multiplier for supplied plot dimensions
    logger : param.Parameters
        Parameters object to issue warnings on

    Returns
    -------
    Returns two dictionaries one for the aspect and sizing modes,
    and another for the plot dimensions.
    """
    fixed_width = (explicit_width or frame_width)
    fixed_height = (explicit_height or frame_height)
    fixed_aspect = aspect or data_aspect
    if aspect == 'square':
        aspect = 1
    elif aspect == 'equal':
        data_aspect = 1

    # Plot dimensions
    height = None if height is None else int(height*size_multiplier)
    width = None if width is None else int(width*size_multiplier)
    frame_height = None if frame_height is None else int(frame_height*size_multiplier)
    frame_width = None if frame_width is None else int(frame_width*size_multiplier)
    actual_width = frame_width or width
    actual_height = frame_height or height

    if frame_width is not None:
        width = None
    if frame_height is not None:
        height = None

    sizing_mode = 'fixed'
    if responsive:
        if fixed_height and fixed_width:
            responsive = False
            if logger:
                logger.warning("responsive mode could not be enabled "
                               "because fixed width and height were "
                               "specified.")
        elif fixed_width:
            height = None
            sizing_mode = 'fixed' if fixed_aspect else 'stretch_height'
        elif fixed_height:
            width = None
            sizing_mode = 'fixed' if fixed_aspect else 'stretch_width'
        else:
            width, height = None, None
            if fixed_aspect:
                if responsive == 'width':
                    sizing_mode = 'scale_width'
                elif responsive == 'height':
                    sizing_mode = 'scale_height'
                else:
                    sizing_mode = 'scale_both'
            elif responsive == 'width':
                sizing_mode = 'stretch_both'
            elif responsive == 'height':
                sizing_mode = 'stretch_height'
            else:
                sizing_mode = 'stretch_both'


    if fixed_aspect:
        if ((explicit_width and not frame_width) != (explicit_height and not frame_height)) and logger:
            logger.warning('Due to internal constraints, when aspect and '
                           'width/height is set, the bokeh backend uses '
                           'those values as frame_width/frame_height instead. '
                           'This ensures the aspect is respected, but means '
                           'that the plot might be slightly larger than '
                           'anticipated. Set the frame_width/frame_height '
                           'explicitly to suppress this warning.')

        aspect_type = 'data_aspect' if data_aspect else 'aspect'
        if fixed_width and fixed_height and aspect:
            if aspect == 'equal':
                data_aspect = 1
            elif not data_aspect:
                aspect = None
                if logger:
                    logger.warning(
                        f"{aspect_type} value was ignored because absolute width and "
                        "height values were provided. Either supply "
                        "explicit frame_width and frame_height to achieve "
                        "desired aspect OR supply a combination of width "
                        "or height and an aspect value.")
        elif fixed_width and responsive:
            height = None
            responsive = False
            if logger:
                logger.warning("responsive mode could not be enabled "
                               "because fixed width and aspect were "
                               "specified.")
        elif fixed_height and responsive:
            width = None
            responsive = False
            if logger:
                logger.warning("responsive mode could not be enabled "
                               "because fixed height and aspect were "
                               "specified.")
        elif responsive == 'width':
            sizing_mode = 'scale_width'
        elif responsive == 'height':
            sizing_mode = 'scale_height'

    if responsive == 'width' and fixed_width:
        responsive = False
        if logger:
            logger.warning("responsive width mode could not be enabled "
                           "because a fixed width was defined.")
    if responsive == 'height' and fixed_height:
        responsive = False
        if logger:
            logger.warning("responsive height mode could not be enabled "
                           "because a fixed height was defined.")

    match_aspect = False
    aspect_scale = 1
    aspect_ratio = None
    if data_aspect:
        match_aspect = True
        if (fixed_width and fixed_height):
            frame_width, frame_height = frame_width or width, frame_height or height
        elif fixed_width or not fixed_height:
            height = None
        elif fixed_height or not fixed_width:
            width = None

        aspect_scale = data_aspect
        if aspect == 'equal':
            aspect_scale = 1
        elif responsive:
            aspect_ratio = aspect
    elif (fixed_width and fixed_height):
        pass
    elif isnumeric(aspect):
        if responsive:
            aspect_ratio = aspect
        elif fixed_width:
            frame_width = actual_width
            frame_height = int(actual_width/aspect)
            width, height = None, None
        else:
            frame_width = int(actual_height*aspect)
            frame_height = actual_height
            width, height = None, None
    elif aspect is not None and logger:
        logger.warning('aspect value of type %s not recognized, '
                       'provide a numeric value, \'equal\' or '
                       '\'square\'.')

    aspect_info = {
        'aspect_ratio': aspect_ratio,
        'aspect_scale': aspect_scale,
        'match_aspect': match_aspect,
        'sizing_mode' : sizing_mode
    }
    dimension_info =  {
        'frame_width' : frame_width,
        'frame_height': frame_height,
        'height' : height,
        'width'  : width
    }

    return aspect_info, dimension_info


def merge_tools(plot_grid, *, disambiguation_properties=None, hide_toolbar=False, autohide=False):
    """Merges tools defined on a grid of plots into a single toolbar.
    All tools of the same type are merged unless they define one
    of the disambiguation properties. By default `name`, `icon`, `tags`
    and `description` can be used to prevent tools from being merged.

    """
    plot_tools = []
    for row in plot_grid:
        for item in row:
            if isinstance(item, LayoutDOM):
                for p in item.select(dict(type=Plot)):
                    plot_tools.extend(p.toolbar.tools)
            if hide_toolbar and hasattr(item, 'toolbar_location'):
                item.toolbar_location = None
            if isinstance(item, GridPlot):
                item.toolbar_location = None

    def merge(tool, group):
        if issubclass(tool, (tools.SaveTool, tools.CopyTool, tools.ExamineTool, tools.FullscreenTool)):
            return tool()
        else:
            return None

    if not disambiguation_properties:
        disambiguation_properties = {'name', 'icon', 'tags', 'description'}

    ignore = set()
    for tool in plot_tools:
        for p in tool.properties_with_values():
            if p not in disambiguation_properties:
                ignore.add(p)

    toolbar_kwargs = {"autohide": autohide}
    if plot_tools:
        toolbar_kwargs["tools"] = group_tools(plot_tools, merge=merge, ignore=ignore)
    return tools.Toolbar(**toolbar_kwargs)


def sync_legends(bokeh_layout):
    """This syncs the legends of all plots in a grid based on their name.

    Parameters
    ----------
    bokeh_layout : bokeh.models.{GridPlot, Row, Column}
        Gridplot to sync legends of.
    """
    if len(bokeh_layout.children) < 2:
        return

    # Collect all glyph with names
    items = defaultdict(list)
    click_policies = set()
    for fig in bokeh_layout.children:
        if isinstance(fig, tuple):  # GridPlot
            fig = fig[0]
        if not isinstance(fig, figure):
            continue
        for r in fig.renderers:
            if r.name:
                items[r.name].append(r)
        if fig.legend:
            click_policies.add(fig.legend[0].click_policy)

    click_policies.discard("none")  # If legend is not visible, click_policy is "none"
    if len(click_policies) > 1:
        warn("Click policy of legends are not the same, no syncing will happen.")
        return
    elif not click_policies:
        return

    # Link all glyphs with the same name
    mapping = {"mute": "muted", "hide": "visible"}
    policy = mapping.get(next(iter(click_policies)))
    code = f"dst.{policy} = src.{policy}"
    for item in items.values():
        for src, dst in permutations(item, 2):
            src.js_on_change(
                policy,
                CustomJS(code=code, args=dict(src=src, dst=dst)),
            )


def select_legends(holoviews_layout, figure_index=None, legend_position="top_right"):
    """Only displays selected legends in plot layout.

    Parameters
    ----------
    holoviews_layout : Holoviews Layout
        Holoviews Layout with legends.
    figure_index : list[int] | bool | int | None
        Index of the figures which legends to show.
        If None is chosen, only the first figures legend is shown
        If True is chosen, all legends are shown.
    legend_position : str
        Position of the legend(s).
    """
    if figure_index is None:
        figure_index = [0]
    elif isinstance(figure_index, bool):
        figure_index = range(len(holoviews_layout)) if figure_index else []
    elif isinstance(figure_index, int):
        figure_index = [figure_index]
    if not isinstance(holoviews_layout, Layout):
        holoviews_layout = [holoviews_layout]

    for i, plot in enumerate(holoviews_layout):
        if not isinstance(plot, (NdOverlay, Overlay)):
            continue
        if i in figure_index:
            plot.opts(show_legend=True, legend_position=legend_position)
        else:
            plot.opts(show_legend=False)

    if isinstance(holoviews_layout, list):
        return holoviews_layout[0]

    return holoviews_layout


@contextmanager
def silence_warnings(*warnings):
    """Context manager for silencing bokeh validation warnings.

    """
    silenced = set()
    for warning in warnings:
        if not is_silenced(warning):
            silenced.add(warning)
            silence(warning)
    try:
        yield
    finally:
        for warning in silenced:
            silence(warning, False)


def empty_plot(width, height):
    """Creates an empty and invisible plot of the specified size.

    """
    return Spacer(width=width, height=height)


def remove_legend(plot, legend):
    """Removes a legend from a bokeh plot.

    """
    valid_places = ['left', 'right', 'above', 'below', 'center']
    plot.legend[:] = [l for l in plot.legend if l is not legend]
    for place in valid_places:
        place = getattr(plot, place)
        if legend in place:
            place.remove(legend)


def font_size_to_pixels(size):
    """Convert a fontsize to a pixel value

    """
    if size is None or not isinstance(size, str):
        return
    conversions = {'em': 16, 'pt': 16/12.}
    val = re.findall(r'\d+', size)
    unit = re.findall('[a-z]+', size)
    if (val and not unit) or (val and unit[0] == 'px'):
        return int(val[0])
    elif val and unit[0] in conversions:
        return (int(int(val[0]) * conversions[unit[0]]))


def make_axis(axis, size, factors, dim, flip=False, rotation=0,
              label_size=None, tick_size=None, axis_height=35):
    factors = list(map(dim.pprint_value, factors))
    nchars = np.max([len(f) for f in factors])
    ranges = FactorRange(factors=factors)
    ranges2 = Range1d(start=0, end=1)
    axis_label = dim_axis_label(dim)
    reset = "range.setv({start: 0, end: range.factors.length})"
    customjs = CustomJS(args=dict(range=ranges), code=reset)
    ranges.js_on_change('start', customjs)

    axis_props = {}
    if label_size:
        axis_props['axis_label_text_font_size'] = label_size
    if tick_size:
        axis_props['major_label_text_font_size'] = tick_size

    tick_px = font_size_to_pixels(tick_size)
    if tick_px is None:
        tick_px = 8
    label_px = font_size_to_pixels(label_size)
    if label_px is None:
        label_px = 10

    rotation = np.radians(rotation)
    if axis == 'x':
        align = 'center'
        # Adjust height to compensate for label rotation
        height = int(axis_height + np.abs(np.sin(rotation)) *
                     ((nchars*tick_px)*0.82)) + tick_px + label_px
        opts = dict(x_axis_type='auto', x_axis_label=axis_label,
                    x_range=ranges, y_range=ranges2, height=height,
                    width=size)
    else:
        # Adjust width to compensate for label rotation
        align = 'left' if flip else 'right'
        width = int(axis_height + np.abs(np.cos(rotation)) *
                    ((nchars*tick_px)*0.82)) + tick_px + label_px
        opts = dict(y_axis_label=axis_label, x_range=ranges2,
                    y_range=ranges, height=size, width=width)

    p = figure(toolbar_location=None, tools=[], **opts)
    p.outline_line_alpha = 0
    p.grid.grid_line_alpha = 0

    if axis == 'x':
        p.align = 'end'
        p.yaxis.visible = False
        axis = p.xaxis[0]
        if flip:
            p.above = p.below
            p.below = []
            p.xaxis[:] = p.above
    else:
        p.xaxis.visible = False
        axis = p.yaxis[0]
        if flip:
            p.right = p.left
            p.left = []
            p.yaxis[:] = p.right
    axis.major_label_orientation = rotation
    axis.major_label_text_align = align
    axis.major_label_text_baseline = 'middle'
    axis.update(**axis_props)
    return p


def hsv_to_rgb(hsv):
    """Vectorized HSV to RGB conversion, adapted from:
    https://stackoverflow.com/questions/24852345/hsv-to-rgb-color-conversion

    """
    h, s, v = (hsv[..., i] for i in range(3))
    shape = h.shape
    i = np.int_(h*6.)
    f = h*6.-i

    q = f
    t = 1.-f
    i = np.ravel(i)
    f = np.ravel(f)
    i%=6

    t = np.ravel(t)
    q = np.ravel(q)
    s = np.ravel(s)
    v = np.ravel(v)

    clist = (1-s*np.vstack([np.zeros_like(f),np.ones_like(f),q,t]))*v

    #0:v 1:p 2:q 3:t
    order = np.array([[0,3,1],[2,0,1],[1,0,3],[1,2,0],[3,1,0],[0,1,2]])
    rgb = clist[order[i], np.arange(np.prod(shape))[:,None]]

    return rgb.reshape((*shape, 3))


def pad_width(model, table_padding=0.85, tabs_padding=1.2):
    """Computes the width of a model and sets up appropriate padding
    for Tabs and DataTable types.

    """
    if isinstance(model, Row):
        vals = [pad_width(child) for child in model.children]
        width = np.max([v for v in vals if v is not None])
    elif isinstance(model, Column):
        vals = [pad_width(child) for child in model.children]
        width = np.sum([v for v in vals if v is not None])
    elif isinstance(model, Tabs):
        vals = [pad_width(t) for t in model.tabs]
        width = np.max([v for v in vals if v is not None])
        for submodel in model.tabs:
            submodel.width = width
            width = int(tabs_padding*width)
    elif isinstance(model, DataTable):
        width = model.width
        model.width = int(table_padding*width)
    elif isinstance(model, Div):
        width = model.width
    elif model:
        width = model.width
    else:
        width = 0
    return width


def pad_plots(plots):
    """Accepts a grid of bokeh plots in form of a list of lists and
    wraps any DataTable or Tabs in a Column with appropriate
    padding. Required to avoid overlap in gridplot.

    """
    widths = []
    for row in plots:
        row_widths = []
        for p in row:
            width = pad_width(p)
            row_widths.append(width)
        widths.append(row_widths)

    plots = [[Column(p, width=w) if isinstance(p, (DataTable, Tabs)) else p
              for p, w in zip(row, ws, strict=None)] for row, ws in zip(plots, widths, strict=None)]
    return plots


def filter_toolboxes(plots):
    """Filters out toolboxes out of a list of plots to be able to compose
    them into a larger plot.

    """
    if isinstance(plots, list):
        plots = [filter_toolboxes(plot) for plot in plots]
    elif hasattr(plots, 'toolbar'):
        plots.toolbar_location = None
    elif hasattr(plots, 'children'):
        plots.children = [filter_toolboxes(child) for child in plots.children
                          if not isinstance(child, tools.Toolbar)]
    return plots


def get_tab_title(key, frame, overlay):
    """Computes a title for bokeh tabs from the key in the overlay, the
    element and the containing (Nd)Overlay.

    """
    if isinstance(overlay, Overlay):
        if frame is not None:
            title = []
            if frame.label:
                title.append(frame.label)
                if frame.group != frame.param.objects('existing')['group'].default:
                    title.append(frame.group)
            else:
                title.append(frame.group)
        else:
            title = key
        title = ' '.join(title)
    else:
        title = ' | '.join([d.pprint_value_string(k) for d, k in
                            zip(overlay.kdims, key, strict=None)])
    return title


def get_default(model, name, theme=None):
    """Looks up the default value for a bokeh model property.

    """
    overrides = None
    if theme is not None:
        if isinstance(theme, str):
            theme = built_in_themes[theme]
        overrides = theme._for_class(model)
    descriptor = model.lookup(name)
    return descriptor.property.themed_default(model, name, overrides)


def filter_batched_data(data, mapping):
    """Iterates over the data and mapping for a ColumnDataSource and
    replaces columns with repeating values with a scalar. This is
    purely and optimization for scalar types.

    """
    for k, v in list(mapping.items()):
        if isinstance(v, dict) and 'field' in v:
            if 'transform' in v:
                continue
            v = v['field']
        elif not isinstance(v, str):
            continue
        values = data[v]
        try:
            if len(unique_array(values)) == 1:
                mapping[k] = values[0]
                del data[v]
        except Exception:
            pass

def cds_column_replace(source, data):
    """Determine if the CDS.data requires a full replacement or simply
    needs to be updated. A replacement is required if untouched
    columns are not the same length as the columns being updated.

    """
    current_length = [len(v) for v in source.data.values()
                      if isinstance(v, (list, *arraylike_types))]
    new_length = [len(v) for v in data.values() if isinstance(v, (list, np.ndarray))]
    untouched = [k for k in source.data if k not in data]
    return bool(untouched and current_length and new_length and current_length[0] != new_length[0])


@contextmanager
def hold_policy(document, policy, server=False):
    """Context manager to temporary override the hold policy.

    """
    old_policy = document.callbacks.hold_value
    document.callbacks._hold = policy
    try:
        yield
    finally:
        if server and not old_policy:
            document.unhold()
        else:
            document.callbacks._hold = old_policy


def recursive_model_update(model, props):
    """Recursively updates attributes on a model including other
    models. If the type of the new model matches the old model
    properties are simply updated, otherwise the model is replaced.

    """
    updates = {}
    valid_properties = model.properties_with_values()
    for k, v in props.items():
        if isinstance(v, Model):
            nested_model = getattr(model, k)
            if type(v) is type(nested_model):
                nested_props = v.properties_with_values(include_defaults=False)
                recursive_model_update(nested_model, nested_props)
            else:
                try:
                    setattr(model, k, v)
                except Exception as e:
                    if isinstance(v, dict) and 'value' in v:
                        setattr(model, k, v['value'])
                    else:
                        raise e
        elif k in valid_properties and v != valid_properties[k]:
            if isinstance(v, dict) and 'value' in v:
                v = v['value']
            updates[k] = v
    model.update(**updates)


def update_shared_sources(f):
    """Context manager to ensures data sources shared between multiple
    plots are cleared and updated appropriately avoiding warnings and
    allowing empty frames on subplots. Expects a list of
    shared_sources and a mapping of the columns expected columns for
    each source in the plots handles.

    """
    def wrapper(self, *args, **kwargs):
        source_cols = self.handles.get('source_cols', {})
        shared_sources = self.handles.get('shared_sources', [])
        doc = self.document
        for source in shared_sources:
            source.data.clear()
            if doc:
                event_obj = doc.callbacks
                event_obj._held_events = event_obj._held_events[:-1]

        ret = f(self, *args, **kwargs)

        for source in shared_sources:
            expected = source_cols[id(source)]
            found = [c for c in expected if c in source.data]
            empty = np.full_like(source.data[found[0]], np.nan) if found else []
            patch = {c: empty for c in expected if c not in source.data}
            source.data.update(patch)
        return ret
    return wrapper


def hold_render(f):
    """Decorator that will hold render on a Bokeh ElementPlot until after
    the method has been called.

    """
    def wrapper(self, *args, **kwargs):
        hold = self.state.hold_render
        doc = self.state.document
        self.state.hold_render = True
        if doc:
            with doc.models.freeze():
                try:
                    return f(self, *args, **kwargs)
                finally:
                    self.state.hold_render = hold
        else:
            try:
                return f(self, *args, **kwargs)
            finally:
                self.state.hold_render = hold
    return wrapper


def categorize_array(array, dim):
    """Uses a Dimension instance to convert an array of values to categorical
    (i.e. string) values and applies escaping for colons, which bokeh
    treats as a categorical suffix.

    """
    return np.array([dim.pprint_value(x) for x in array])


class periodic:
    """Mocks the API of periodic Thread in hv.core.util, allowing a smooth
    API transition on bokeh server.

    """

    def __init__(self, document):
        self.document = document
        self.callback = None
        self.period = None
        self.count = None
        self.counter = None
        self._start_time = None
        self.timeout = None
        self._pcb = None

    @property
    def completed(self):
        return self.counter is None

    def start(self):
        self._start_time = time.time()
        if self.document is None:
            raise RuntimeError('periodic was registered to be run on bokeh'
                               'server but no document was found.')
        self._pcb = self.document.add_periodic_callback(self._periodic_callback, self.period)

    def __call__(self, period, count, callback, timeout=None, block=False):
        if isinstance(count, int):
            if count < 0: raise ValueError('Count value must be positive')
        elif type(count) is not type(None):
            raise ValueError('Count value must be a positive integer or None')

        self.callback = callback
        self.period = period*1000.
        self.timeout = timeout
        self.count = count
        self.counter = 0
        return self

    def _periodic_callback(self):
        self.callback(self.counter)
        self.counter += 1

        if self.timeout is not None:
            dt = (time.time() - self._start_time)
            if dt > self.timeout:
                self.stop()
        if self.counter == self.count:
            self.stop()

    def stop(self):
        self.counter = None
        self.timeout = None
        try:
            self.document.remove_periodic_callback(self._pcb)
        except ValueError: # Already stopped
            pass
        self._pcb = None

    def __repr__(self):
        return f'periodic({self.period}, {self.count}, {callable_name(self.callback)})'
    def __str__(self):
        return repr(self)


def attach_periodic(plot):
    """Attaches plot refresh to all streams on the object.

    """
    def append_refresh(dmap):
        for subdmap in get_nested_dmaps(dmap):
            subdmap.periodic._periodic_util = periodic(plot.document)
    return plot.hmap.traverse(append_refresh, [DynamicMap])


def date_to_integer(date):
    """Converts support date types to milliseconds since epoch

    Attempts highest precision conversion of different datetime
    formats to milliseconds since the epoch (1970-01-01 00:00:00).
    If datetime is a cftime with a non-standard calendar the
    caveats described in hv.core.util.cftime_to_timestamp apply.

    Parameters
    ----------
    date : Date- or datetime-like object

    Returns
    -------
    Milliseconds since 1970-01-01 00:00:00
    """
    import pandas as pd
    if isinstance(date, pd.Timestamp):
        try:
            date = date.to_datetime64()
        except Exception:
            date = date.to_datetime()

    if isinstance(date, np.datetime64):
        return date.astype('datetime64[ms]').astype(float)
    elif isinstance(date, cftime_types):
        return cftime_to_timestamp(date, 'ms')

    if hasattr(date, 'timetuple'):
        dt_int = calendar.timegm(date.timetuple())*1000
    else:
        raise ValueError('Datetime type not recognized')
    return dt_int


def glyph_order(keys, draw_order=None):
    """Orders a set of glyph handles using regular sort and an explicit
    sort order. The explicit draw order must take the form of a list
    of glyph names while the keys should be glyph names with a custom
    suffix. The draw order may only match subset of the keys and any
    matched items will take precedence over other entries.

    """
    if draw_order is None:
        draw_order = []
    keys = sorted(keys)
    def order_fn(glyph):
        matches = [item for item in draw_order if glyph.startswith(item)]
        return ((draw_order.index(matches[0]), glyph) if matches else
                (1e9+keys.index(glyph), glyph))
    return sorted(keys, key=order_fn)


def colormesh(X, Y):
    """Generates line paths for a quadmesh given 2D arrays of X and Y
    coordinates.

    """
    X1 = X[0:-1, 0:-1].ravel()
    Y1 = Y[0:-1, 0:-1].ravel()
    X2 = X[1:, 0:-1].ravel()
    Y2 = Y[1:, 0:-1].ravel()
    X3 = X[1:, 1:].ravel()
    Y3 = Y[1:, 1:].ravel()
    X4 = X[0:-1, 1:].ravel()
    Y4 = Y[0:-1, 1:].ravel()

    X = np.column_stack([X1, X2, X3, X4, X1])
    Y = np.column_stack([Y1, Y2, Y3, Y4, Y1])
    return X, Y


def theme_attr_json(theme, attr):
    if isinstance(theme, str) and theme in built_in_themes:
        return built_in_themes[theme]._json['attrs'].get(attr, {})
    elif isinstance(theme, Theme):
        return theme._json['attrs'].get(attr, {})
    else:
        return {}


def multi_polygons_data(element):
    """Expands polygon data which contains holes to a bokeh multi_polygons
    representation. Multi-polygons split by nans are expanded and the
    correct list of holes is assigned to each sub-polygon.

    """
    xs, ys = (element.dimension_values(kd, expanded=False) for kd in element.kdims)
    holes = element.holes()
    xsh, ysh = [], []
    for x, y, multi_hole in zip(xs, ys, holes, strict=None):
        xhs = [[h[:, 0] for h in hole] for hole in multi_hole]
        yhs = [[h[:, 1] for h in hole] for hole in multi_hole]
        array = np.column_stack([x, y])
        splits = np.where(np.isnan(array[:, :2].astype('float')).sum(axis=1))[0]
        arrays = np.split(array, splits+1) if len(splits) else [array]
        multi_xs, multi_ys = [], []
        for i, (path, hx, hy) in enumerate(zip(arrays, xhs, yhs, strict=None)):
            if i != (len(arrays)-1):
                path = path[:-1]
            multi_xs.append([path[:, 0], *hx])
            multi_ys.append([path[:, 1], *hy])
        xsh.append(multi_xs)
        ysh.append(multi_ys)
    return xsh, ysh


def match_dim_specs(specs1, specs2):
    """Matches dimension specs used to link axes.

    Axis dimension specs consists of a list of tuples corresponding
    to each dimension, each tuple spec has the form (name, label, unit).
    The name and label must match exactly while the unit only has to
    match if both specs define one.

    """
    if (specs1 is None or specs2 is None) or (len(specs1) != len(specs2)):
        return False
    for spec1, spec2 in zip(specs1, specs2, strict=None):
        for s1, s2 in zip(spec1, spec2, strict=None):
            if s1 is None or s2 is None:
                continue
            if s1 != s2:
                return False
    return True


def get_scale(range_input, axis_type):
    if isinstance(range_input, (DataRange1d, Range1d)) and axis_type in ["linear", "datetime", "mercator", "auto", "timedelta", None]:
        return LinearScale()
    elif isinstance(range_input, (DataRange1d, Range1d)) and axis_type == "log":
        return LogScale()
    elif isinstance(range_input, FactorRange):
        return CategoricalScale()
    else:
        raise ValueError(f"Unable to determine proper scale for: '{range_input}'")


def get_axis_class(axis_type, range_input, dim): # Copied from bokeh
    if axis_type is None:
        return None, {}
    elif axis_type == "linear":
        return LinearAxis, {}
    elif axis_type == "log":
        return LogAxis, {}
    elif axis_type == "datetime":
        return DatetimeAxis, {}
    elif BOKEH_GE_3_8_0 and axis_type == "timedelta":
        return TimedeltaAxis, {}
    elif axis_type == "mercator":
        return MercatorAxis, dict(dimension='lon' if dim == 0 else 'lat')
    elif axis_type == "auto":
        if isinstance(range_input, FactorRange):
            return CategoricalAxis, {}
        elif isinstance(range_input, Range1d):
            try:
                value = range_input.start
                # Datetime accepts ints/floats as timestamps, but we don't want
                # to assume that implies a datetime axis
                if Datetime.is_timestamp(value):
                    return LinearAxis, {}
                Datetime.validate(Datetime(), value)
                return DatetimeAxis, {}
            except ValueError:
                pass
        return LinearAxis, {}
    else:
        raise ValueError(f"Unrecognized axis_type: '{axis_type!r}'")


def match_ax_type(ax, range_type):
    """Ensure the range_type matches the axis model being matched.

    """
    if isinstance(ax, CategoricalAxis):
        return range_type == 'categorical'
    elif isinstance(ax, DatetimeAxis):
        return range_type == 'datetime'
    elif BOKEH_GE_3_8_0 and isinstance(ax, TimedeltaAxis):
        return range_type == 'timedelta'
    else:
        return range_type in ('auto', 'log')


def match_yaxis_type_to_range(yax, range_type, range_name):
    """Apply match_ax_type to the y-axis found by the given range name

    """
    for axis in yax:
        if axis.y_range_name == range_name:
            return match_ax_type(axis, range_type)
    raise ValueError('No axis with given range found')


def wrap_formatter(formatter, axis):
    """Wraps formatting function or string in
    appropriate bokeh formatter type.

    """
    if isinstance(formatter, TickFormatter):
        pass
    else:
        formatter = PrintfTickFormatter(format=formatter)
    return formatter


def property_to_dict(x):
    """Convert Bokeh's property Field and Value to a dictionary

    """
    try:
        from bokeh.core.property.vectorization import Field, Unspecified, Value

        if isinstance(x, (Field, Value)):
            x = {k: v for k, v in x.__dict__.items() if v != Unspecified}
    except ImportError:
        pass

    return x


def dtype_fix_hook(plot, element):
    # Work-around for problems seen in:
    # https://github.com/holoviz/holoviews/issues/5722
    # https://github.com/holoviz/holoviews/issues/5726
    # https://github.com/holoviz/holoviews/issues/5941
    # Should be fixed in Bokeh:
    # https://github.com/bokeh/bokeh/issues/13155

    with suppress(Exception):
        renderers = plot.handles["plot"].renderers
        for renderer in renderers:
            with suppress(Exception):
                data = renderer.data_source.data
                for k, v in data.items():
                    if hasattr(v, "dtype") and dtype_kind(v) == "U":
                        data[k] = v.tolist()


def get_ticker_axis_props(ticker):
    axis_props = {}
    if isinstance(ticker, np.ndarray):
        ticker = list(ticker)
    if isinstance(ticker, Ticker):
        axis_props['ticker'] = ticker
    elif isinstance(ticker, int):
        axis_props['ticker'] = BasicTicker(desired_num_ticks=ticker)
    elif isinstance(ticker, (tuple, list)):
        if all(isinstance(t, tuple) for t in ticker):
            ticks, labels = zip(*ticker, strict=None)
            # Ensure floats which are integers are serialized as ints
            # because in JS the lookup fails otherwise
            ticks = [int(t) if isinstance(t, float) and t.is_integer() else t
                        for t in ticks]
            labels = [l if isinstance(l, str) else str(l)
                        for l in labels]
        else:
            ticks, labels = ticker, None
        if ticks and util.isdatetime(ticks[0]):
            ticks = [util.dt_to_int(tick, 'ms') for tick in ticks]
        axis_props['ticker'] = FixedTicker(ticks=ticks)
        if labels is not None:
            axis_props['major_label_overrides'] = dict(zip(ticks, labels, strict=None))
    return axis_props
