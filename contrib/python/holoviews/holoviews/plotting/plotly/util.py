import base64
import copy
import re

import numpy as np
from packaging.version import Version
from plotly import __version__, colors

from ...core.util import isfinite, max_range
from ..util import color_intervals, process_cmap

PLOTLY_VERSION = Version(__version__).release
PLOTLY_GE_6_0_0 = PLOTLY_VERSION >= (6, 0, 0)
PLOTLY_SCATTERMAP = "scattermap" if PLOTLY_GE_6_0_0 else "scattermapbox"
PLOTLY_MAP = "map" if PLOTLY_GE_6_0_0 else "mapbox"

# Constants
# ---------

# Trace types that are individually positioned with their own domain.
# These are traces that don't overlay on top of each other in a shared subplot,
# so they are positioned individually.  All other trace types are associated
# with a layout subplot type (xaxis/yaxis, polar, scene etc.)
#
# Each of these trace types has a `domain` property with `x`/`y` properties
_domain_trace_types = {'parcoords', 'pie', 'table', 'sankey', 'parcats'}

# Subplot types that are each individually positioned with a domain
#
# Each of these subplot types has a `domain` property with `x`/`y` properties.
# Note that this set does not contain `xaxis`/`yaxis` because these behave a
# little differently.
_subplot_types = {'scene', 'geo', 'polar', 'ternary', PLOTLY_MAP}

# For most subplot types, a trace is associated with a particular subplot
# using a trace property with a name that matches the subplot type. For
# example, a `scatter3d.scene` property set to `'scene2'` associates a
# scatter3d trace with the second `scene` subplot in the figure.
#
# There are a few subplot types that don't follow this pattern, and instead
# the trace property is just named `subplot`.  For example setting
# the `scatterpolar.subplot` property to `polar3` associates the scatterpolar
# trace with the third polar subplot in the figure
_subplot_prop_named_subplot = {'polar', 'ternary', PLOTLY_MAP}

# Mapping from trace type to subplot type(s).
_trace_to_subplot = {
    # xaxis/yaxis
    'bar':                  ['xaxis', 'yaxis'],
    'box':                  ['xaxis', 'yaxis'],
    'candlestick':          ['xaxis', 'yaxis'],
    'carpet':               ['xaxis', 'yaxis'],
    'contour':              ['xaxis', 'yaxis'],
    'contourcarpet':        ['xaxis', 'yaxis'],
    'heatmap':              ['xaxis', 'yaxis'],
    'heatmapgl':            ['xaxis', 'yaxis'],
    'histogram':            ['xaxis', 'yaxis'],
    'histogram2d':          ['xaxis', 'yaxis'],
    'histogram2dcontour':   ['xaxis', 'yaxis'],
    'ohlc':                 ['xaxis', 'yaxis'],
    'pointcloud':           ['xaxis', 'yaxis'],
    'scatter':              ['xaxis', 'yaxis'],
    'scattercarpet':        ['xaxis', 'yaxis'],
    'scattergl':            ['xaxis', 'yaxis'],
    'violin':               ['xaxis', 'yaxis'],

    # scene
    'cone':         ['scene'],
    'mesh3d':       ['scene'],
    'scatter3d':    ['scene'],
    'streamtube':   ['scene'],
    'surface':      ['scene'],

    # geo
    'choropleth': ['geo'],
    'scattergeo': ['geo'],

    # polar
    'barpolar':         ['polar'],
    'scatterpolar':     ['polar'],
    'scatterpolargl':   ['polar'],

    # ternary
    'scatterternary': ['ternary'],

    # mapbox
    PLOTLY_SCATTERMAP: [PLOTLY_MAP],
}

# trace types that support legends
legend_trace_types = {
    'scatter',
    'bar',
    'box',
    'histogram',
    'histogram2dcontour',
    'contour',
    'scatterternary',
    'violin',
    'waterfall',
    'pie',
    'scatter3d',
    'scattergeo',
    'scattergl',
    'splom',
    'pointcloud',
    PLOTLY_SCATTERMAP,
    'scattercarpet',
    'contourcarpet',
    'ohlc',
    'candlestick',
    'scatterpolar',
    'scatterpolargl',
    'barpolar',
    'area',
}

# Aliases - map common style options to more common names

STYLE_ALIASES = {'alpha': 'opacity',
                 'cell_height': 'height',
                 'marker': 'symbol',
                 "max_zoom": "maxzoom",
                 "min_zoom": "minzoom",}

# Regular expression to extract any trailing digits from a subplot-style
# string.
_subplot_re = re.compile(r'\D*(\d+)')


def _get_subplot_number(subplot_val):
    """Extract the subplot number from a subplot value string.

    'x3' -> 3
    'polar2' -> 2
    'scene' -> 1
    'y' -> 1

    Notes
    -----
    The absence of a subplot number (e.g. 'y') is treated by plotly as
    a subplot number of 1

    Parameters
    ----------
    subplot_val : str
        Subplot string value (e.g. 'scene4')

    Returns
    -------
    int
    """
    match = _subplot_re.match(subplot_val)
    if match:
        subplot_number = int(match.group(1))
    else:
        subplot_number = 1
    return subplot_number


def _get_subplot_val_prefix(subplot_type):
    """Get the subplot value prefix for a subplot type. For most subplot types
    this is equal to the subplot type string itself. For example, a
    `scatter3d.scene` value of `scene2` is used to associate the scatter3d
    trace with the `layout.scene2` subplot.

    However, the `xaxis`/`yaxis` subplot types are exceptions to this pattern.
    For example, a `scatter.xaxis` value of `x2` is used to associate the
    scatter trace with the `layout.xaxis2` subplot.

    Parameters
    ----------
    subplot_type : str
        Subplot string value (e.g. 'scene4')

    Returns
    -------
    str
    """
    if subplot_type == 'xaxis':
        subplot_val_prefix = 'x'
    elif subplot_type == 'yaxis':
        subplot_val_prefix = 'y'
    else:
        subplot_val_prefix = subplot_type
    return subplot_val_prefix


def _get_subplot_prop_name(subplot_type):
    """Get the name of the trace property used to associate a trace with a
    particular subplot type.  For most subplot types this is equal to the
    subplot type string. For example, the `scatter3d.scene` property is used
    to associate a `scatter3d` trace with a particular `scene` subplot.

    However, for some subplot types the trace property is not named after the
    subplot type.  For example, the `scatterpolar.subplot` property is used
    to associate a `scatterpolar` trace with a particular `polar` subplot.


    Parameters
    ----------
    subplot_type : str
        Subplot string value (e.g. 'scene4')

    Returns
    -------
    str
    """
    if subplot_type in _subplot_prop_named_subplot:
        subplot_prop_name = 'subplot'
    else:
        subplot_prop_name = subplot_type
    return subplot_prop_name


def _normalize_subplot_ids(fig):
    """Make sure a layout subplot property is initialized for every subplot that
    is referenced by a trace in the figure.

    For example, if a figure contains a `scatterpolar` trace with the `subplot`
    property set to `polar3`, this function will make sure the figure's layout
    has a `polar3` property, and will initialize it to an empty dict if it
    does not

    Notes
    -----
    This function mutates the input figure dict

    Parameters
    ----------
    fig : dict
        A plotly figure dict
    """
    layout = fig.setdefault('layout', {})
    for trace in fig.get('data', None):
        trace_type = trace.get('type', 'scatter')
        subplot_types = _trace_to_subplot.get(trace_type, [])
        for subplot_type in subplot_types:

            subplot_prop_name = _get_subplot_prop_name(subplot_type)
            subplot_val_prefix = _get_subplot_val_prefix(subplot_type)
            subplot_val = trace.get(subplot_prop_name, subplot_val_prefix)

            # extract trailing number (if any)
            subplot_number = _get_subplot_number(subplot_val)

            if subplot_number > 1:
                layout_prop_name = subplot_type + str(subplot_number)
            else:
                layout_prop_name = subplot_type

            if layout_prop_name not in layout:
                layout[layout_prop_name] = {}


def _get_max_subplot_ids(fig):
    """Given an input figure, return a dict containing the max subplot number
    for each subplot type in the figure

    Parameters
    ----------
    fig : dict
        A plotly figure dict

    Returns
    -------
    dict
        A dict from subplot type strings to integers indicating the largest
        subplot number in the figure of that subplot type
    """
    max_subplot_ids = {subplot_type: 0
                       for subplot_type in _subplot_types}
    max_subplot_ids['xaxis'] = 0
    max_subplot_ids['yaxis'] = 0

    # Check traces
    for trace in fig.get('data', []):
        trace_type = trace.get('type', 'scatter')
        subplot_types = _trace_to_subplot.get(trace_type, [])
        for subplot_type in subplot_types:

            subplot_prop_name = _get_subplot_prop_name(subplot_type)
            subplot_val_prefix = _get_subplot_val_prefix(subplot_type)
            subplot_val = trace.get(subplot_prop_name, subplot_val_prefix)

            # extract trailing number (if any)
            subplot_number = _get_subplot_number(subplot_val)

            max_subplot_ids[subplot_type] = max(
                max_subplot_ids[subplot_type], subplot_number)

    # check annotations/shapes/images
    layout = fig.get('layout', {})
    for layout_prop in ['annotations', 'shapes', 'images']:
        for obj in layout.get(layout_prop, []):
            xref = obj.get('xref', 'x')
            if xref != 'paper':
                xref_number = _get_subplot_number(xref)
                max_subplot_ids['xaxis'] = max(max_subplot_ids['xaxis'], xref_number)

            yref = obj.get('yref', 'y')
            if yref != 'paper':
                yref_number = _get_subplot_number(yref)
                max_subplot_ids['yaxis'] = max(max_subplot_ids['yaxis'], yref_number)

    return max_subplot_ids


def _offset_subplot_ids(fig, offsets):
    """Apply offsets to the subplot id numbers in a figure.

    Notes
    -----
    1. This function mutates the input figure dict

    2. This function assumes that the normalize_subplot_ids function has
    already been run on the figure, so that all layout subplot properties in
    use are explicitly present in the figure's layout.

    Parameters
    ----------
    fig : dict
        A plotly figure dict
    offsets : dict
        A dict from subplot types to the offset to be applied for each subplot
        type.  This dict matches the form of the dict returned by
        get_max_subplot_ids
    """
    # Offset traces
    for trace in fig.get('data', None):
        trace_type = trace.get('type', 'scatter')
        subplot_types = _trace_to_subplot.get(trace_type, [])

        for subplot_type in subplot_types:
            subplot_prop_name = _get_subplot_prop_name(subplot_type)

            # Compute subplot value prefix
            subplot_val_prefix = _get_subplot_val_prefix(subplot_type)
            subplot_val = trace.get(subplot_prop_name, subplot_val_prefix)
            subplot_number = _get_subplot_number(subplot_val)

            offset_subplot_number = (
                    subplot_number + offsets.get(subplot_type, 0))

            if offset_subplot_number > 1:
                trace[subplot_prop_name] = (
                        subplot_val_prefix + str(offset_subplot_number))
            else:
                trace[subplot_prop_name] = subplot_val_prefix

    # layout subplots
    layout = fig.setdefault('layout', {})
    new_subplots = {}

    for subplot_type in offsets:
        offset = offsets[subplot_type]
        if offset < 1:
            continue

        for layout_prop in list(layout.keys()):
            if layout_prop.startswith(subplot_type):
                subplot_number = _get_subplot_number(layout_prop)
                new_subplot_number = subplot_number + offset
                new_layout_prop = subplot_type + str(new_subplot_number)
                new_subplots[new_layout_prop] = layout.pop(layout_prop)

    layout.update(new_subplots)

    # xaxis/yaxis anchors
    x_offset = offsets.get('xaxis', 0)
    y_offset = offsets.get('yaxis', 0)

    for layout_prop in list(layout.keys()):
        if layout_prop.startswith('xaxis'):
            xaxis = layout[layout_prop]
            anchor = xaxis.get('anchor', 'y')
            anchor_number = _get_subplot_number(anchor) + y_offset
            if anchor_number > 1:
                xaxis['anchor'] = 'y' + str(anchor_number)
            else:
                xaxis['anchor'] = 'y'
        elif layout_prop.startswith('yaxis'):
            yaxis = layout[layout_prop]
            anchor = yaxis.get('anchor', 'x')
            anchor_number = _get_subplot_number(anchor) + x_offset
            if anchor_number > 1:
                yaxis['anchor'] = 'x' + str(anchor_number)
            else:
                yaxis['anchor'] = 'x'

    # Axis matches references
    for layout_prop in list(layout.keys()):
        if layout_prop[1:5] == 'axis':
            axis = layout[layout_prop]
            matches_val = axis.get('matches', None)
            if matches_val:

                if matches_val[0] == 'x':
                    matches_number = _get_subplot_number(matches_val) + x_offset
                elif matches_val[0] == 'y':
                    matches_number = _get_subplot_number(matches_val) + y_offset
                else:
                    continue

                suffix = str(matches_number) if matches_number > 1 else ""
                axis['matches'] = matches_val[0] + suffix

    # annotations/shapes/images
    for layout_prop in ['annotations', 'shapes', 'images']:
        for obj in layout.get(layout_prop, []):
            if x_offset:
                xref = obj.get('xref', 'x')
                if xref != 'paper':
                    xref_number = _get_subplot_number(xref)
                    obj['xref'] = 'x' + str(xref_number + x_offset)

            if y_offset:
                yref = obj.get('yref', 'y')
                if yref != 'paper':
                    yref_number = _get_subplot_number(yref)
                    obj['yref'] = 'y' + str(yref_number + y_offset)


def _scale_translate(fig, scale_x, scale_y, translate_x, translate_y):
    """Scale a figure and translate it to sub-region of the original
    figure canvas.

    Notes
    -----
    1. If the input figure has a title, this title is converted into an
    annotation and scaled along with the rest of the figure.

    2. This function mutates the input fig dict

    3. This function assumes that the normalize_subplot_ids function has
    already been run on the figure, so that all layout subplot properties in
    use are explicitly present in the figure's layout.

    Parameters
    ----------
    fig : dict
        A plotly figure dict
    scale_x : float
        Factor by which to scale the figure in the x-direction. This will
        typically be a value < 1.  E.g. a value of 0.5 will cause the
        resulting figure to be half as wide as the original.
    scale_y : float
        Factor by which to scale the figure in the y-direction. This will
        typically be a value < 1
    translate_x : float
        Factor by which to translate the scaled figure in the x-direction in
        normalized coordinates.
    translate_y : float
        Factor by which to translate the scaled figure in the x-direction in
        normalized coordinates.
    """
    data = fig.setdefault('data', [])
    layout = fig.setdefault('layout', {})

    def scale_translate_x(x):
        return [min(x[0] * scale_x + translate_x, 1),
                min(x[1] * scale_x + translate_x, 1)]

    def scale_translate_y(y):
        return [min(y[0] * scale_y + translate_y, 1),
                min(y[1] * scale_y + translate_y, 1)]

    def perform_scale_translate(obj):
        domain = obj.setdefault('domain', {})
        x = domain.get('x', [0, 1])
        y = domain.get('y', [0, 1])

        domain['x'] = scale_translate_x(x)
        domain['y'] = scale_translate_y(y)

    # Scale/translate traces
    for trace in data:
        trace_type = trace.get('type', 'scatter')
        if trace_type in _domain_trace_types:
            perform_scale_translate(trace)

    # Scale/translate subplot containers
    for prop in layout:
        for subplot_type in _subplot_types:
            if prop.startswith(subplot_type):
                perform_scale_translate(layout[prop])

    for prop in layout:
        if prop.startswith('xaxis'):
            xaxis = layout[prop]
            x_domain = xaxis.get('domain', [0, 1])
            xaxis['domain'] = scale_translate_x(x_domain)
        elif prop.startswith('yaxis'):
            yaxis = layout[prop]
            y_domain = yaxis.get('domain', [0, 1])
            yaxis['domain'] = scale_translate_y(y_domain)

    # convert title to annotation
    # This way the annotation will be scaled with the reset of the figure
    annotations = layout.get('annotations', [])

    title = layout.pop('title', None)
    if title:
        titlefont = layout.pop('titlefont', {})
        title_fontsize = titlefont.get('size', 17)
        min_fontsize = 12
        titlefont['size'] = round(min_fontsize +
                                  (title_fontsize - min_fontsize) * scale_x)

        annotations.append({
            'text': title,
            'showarrow': False,
            'xref': 'paper',
            'yref': 'paper',
            'x': 0.5,
            'y': 1.01,
            'xanchor': 'center',
            'yanchor': 'bottom',
            'font': titlefont
        })
        layout['annotations'] = annotations

    # annotations
    for obj in layout.get('annotations', []):
        if obj.get('xref', None) == 'paper':
            obj['x'] = obj.get('x', 0.5) * scale_x + translate_x
        if obj.get('yref', None) == 'paper':
            obj['y'] = obj.get('y', 0.5) * scale_y + translate_y

    # shapes
    for obj in layout.get('shapes', []):
        if obj.get('xref', None) == 'paper':
            obj['x0'] = obj.get('x0', 0.25) * scale_x + translate_x
            obj['x1'] = obj.get('x1', 0.75) * scale_x + translate_x
        if obj.get('yref', None) == 'paper':
            obj['y0'] = obj.get('y0', 0.25) * scale_y + translate_y
            obj['y1'] = obj.get('y1', 0.75) * scale_y + translate_y

    # images
    for obj in layout.get('images', []):
        if obj.get('xref', None) == 'paper':
            obj['x'] = obj.get('x', 0.5) * scale_x + translate_x
            obj['sizex'] = obj.get('sizex', 0) * scale_x
        if obj.get('yref', None) == 'paper':
            obj['y'] = obj.get('y', 0.5) * scale_y + translate_y
            obj['sizey'] = obj.get('sizey', 0) * scale_y


def merge_figure(fig, subfig):
    """Merge a sub-figure into a parent figure

    Notes
    -----
    This function mutates the input fig dict, but it does not mutate
    the subfig dict

    Parameters
    ----------
    fig : dict
        The plotly figure dict into which the sub figure will be merged
    subfig : dict
        The plotly figure dict that will be copied and then merged into `fig`
    """
    # traces
    data = fig.setdefault('data', [])
    data.extend(copy.deepcopy(subfig.get('data', [])))

    # layout
    layout = fig.setdefault('layout', {})
    merge_layout(layout, subfig.get('layout', {}))


def merge_layout(obj, subobj):
    """Merge layout objects recursively

    Notes
    -----
    This function mutates the input obj dict, but it does not mutate
    the subobj dict

    Parameters
    ----------
    obj : dict
        dict into which the sub-figure dict will be merged
    subobj : dict
        dict that sill be copied and merged into `obj`
    """
    for prop, val in subobj.items():
        if isinstance(val, dict) and prop in obj:
            # recursion
            merge_layout(obj[prop], val)
        elif (isinstance(val, list) and
              obj.get(prop, None) and
              isinstance(obj[prop][0], dict)):

            # append
            obj[prop].extend(val)
        elif prop == "style" and val == "white-bg" and obj.get("style", None):
            # Handle special cases
            # Don't let layout.mapbox.style of "white-bg" override other
            # background
            pass
        elif val is not None:
            # init/overwrite
            obj[prop] = copy.deepcopy(val)


def _compute_subplot_domains(widths, spacing):
    """Compute normalized domain tuples for a list of widths and a subplot
    spacing value

    Parameters
    ----------
    widths : list of float
        List of the desired widths of each subplot. The length of this list
        is also the specification of the number of desired subplots
    spacing : float
        Spacing between subplots in normalized coordinates

    Returns
    -------
    list of tuple of float
    """
    # normalize widths
    widths_sum = float(sum(widths))
    total_spacing = (len(widths) - 1) * spacing
    total_width = widths_sum + total_spacing

    relative_spacing = spacing / (widths_sum + total_spacing)
    relative_widths = [(w / total_width) for w in widths]

    domains = []

    for c in range(len(widths)):
        domain_start = c * relative_spacing + sum(relative_widths[:c])
        domain_stop = min(1, domain_start + relative_widths[c])
        domains.append((domain_start, domain_stop))

    return domains


def figure_grid(figures_grid,
                row_spacing=50,
                column_spacing=50,
                share_xaxis=False,
                share_yaxis=False,
                width=None,
                height=None
                ):
    """Construct a figure from a 2D grid of sub-figures

    Parameters
    ----------
    figures_grid : list of list of (dict or None)
        2D list of plotly figure dicts that will be combined in a grid to
        produce the resulting figure.  None values maybe used to leave empty
        grid cells
    row_spacing : float (default 50)
        Vertical spacing between rows in the grid in pixels
    column_spacing : float (default 50)
        Horizontal spacing between columns in the grid in pixels
        coordinates
    share_xaxis : bool (default False)
        Share x-axis between sub-figures in the same column. Also link all x-axes in the
        figure. This will only work if each sub-figure has a single x-axis
    share_yaxis : bool (default False)
        Share y-axis between sub-figures in the same row. Also link all y-axes in the
        figure. This will only work if each subfigure has a single y-axis
    width : int (default None)
        Final figure width. If not specified, width is the sum of the max width of
        the figures in each column
    height : int (default None)
        Final figure width. If not specified, height is the sum of the max height of
        the figures in each row

    Returns
    -------
    dict
        A plotly figure dict
    """
    # Initialize row heights / column widths
    row_heights = [-1 for _ in figures_grid]
    column_widths = [-1 for _ in figures_grid[0]]

    nrows = len(row_heights)
    ncols = len(column_widths)

    responsive = True
    for r in range(nrows):
        for c in range(ncols):
            fig_element = figures_grid[r][c]
            if not fig_element:
                continue
            responsive &= fig_element.get('config', {}).get('responsive', False)

    default = None if responsive else 400
    for r in range(nrows):
        for c in range(ncols):
            fig_element = figures_grid[r][c]
            if not fig_element:
                continue

            w = fig_element.get('layout', {}).get('width', default)
            if w:
                column_widths[c] = max(w, column_widths[c])

            h = fig_element.get('layout', {}).get('height', default)
            if h:
                row_heights[r] = max(h, row_heights[r])

    if width:
        available_column_width = width - (ncols - 1) * column_spacing
        column_width_scale = available_column_width / sum(column_widths)
        column_widths = [wi * column_width_scale for wi in column_widths]
    else:
        column_width_scale = 1.0

    if height:
        available_row_height = height - (nrows - 1) * row_spacing
        row_height_scale = available_row_height / sum(row_heights)
        row_heights = [hi * row_height_scale for hi in row_heights]
    else:
        row_height_scale = 1.0

    # Compute domain widths/heights for subplots
    column_domains = _compute_subplot_domains(column_widths, column_spacing)
    row_domains = _compute_subplot_domains(row_heights, row_spacing)

    output_figure = {'data': [], 'layout': {}}

    for r, (fig_row, row_domain) in enumerate(zip(figures_grid, row_domains, strict=None)):
        for c, (fig, column_domain) in enumerate(zip(fig_row, column_domains, strict=None)):
            if fig:
                fig = copy.deepcopy(fig)

                _normalize_subplot_ids(fig)

                subplot_offsets = _get_max_subplot_ids(output_figure)

                if share_xaxis:
                    subplot_offsets['xaxis'] = c
                    if r != 0:
                        # Only use xaxes from bottom row
                        fig.get('layout', {}).pop('xaxis', None)

                if share_yaxis:
                    subplot_offsets['yaxis'] = r
                    if c != 0:
                        # Only use yaxes from first column
                        fig.get('layout', {}).pop('yaxis', None)

                _offset_subplot_ids(fig, subplot_offsets)

                if responsive:
                    scale_x = 1./ncols
                    scale_y = 1./nrows
                    px = (0.2/(ncols) if ncols > 1 else 0)
                    py = (0.2/(nrows) if nrows > 1 else 0)
                    sx = scale_x-px
                    sy = scale_y-py
                    _scale_translate(fig, sx, sy, scale_x*c+px/2., scale_y*r+py/2.)
                else:
                    fig_height = fig['layout'].get('height', default) * row_height_scale
                    fig_width = fig['layout'].get('width', default) * column_width_scale
                    scale_x = (column_domain[1] - column_domain[0]) * (fig_width / column_widths[c])
                    scale_y = (row_domain[1] - row_domain[0]) * (fig_height / row_heights[r])
                    _scale_translate(
                        fig, scale_x, scale_y, column_domain[0], row_domain[0]
                    )

                merge_figure(output_figure, fig)

    if responsive:
        output_figure['config'] = {'responsive': True}

    # Set output figure width/height
    if height:
        output_figure['layout']['height'] = height
    elif responsive:
        output_figure['layout']['autosize'] = True
    else:
        output_figure['layout']['height'] = (
            sum(row_heights) + row_spacing * (nrows - 1)
        )

    if width:
        output_figure['layout']['width'] = width
    elif responsive:
        output_figure['layout']['autosize'] = True
    else:
        output_figure['layout']['width'] = (
                sum(column_widths) + column_spacing * (ncols - 1)
        )

    if share_xaxis:
        for prop, val in output_figure['layout'].items():
            if prop.startswith('xaxis'):
                val['matches'] = 'x'

    if share_yaxis:
        for prop, val in output_figure['layout'].items():
            if prop.startswith('yaxis'):
                val['matches'] = 'y'

    return output_figure


def get_colorscale(cmap, levels=None, cmin=None, cmax=None):
    """Converts a cmap spec to a plotly colorscale

    Parameters
    ----------
    cmap
        A recognized colormap by name or list of colors
    levels
        A list or integer declaring the color-levels
    cmin
        The lower bound of the color range
    cmax
        The upper bound of the color range

    Returns
    -------
    A valid plotly colorscale
    """
    ncolors = levels if isinstance(levels, int) else None
    if isinstance(levels, list):
        ncolors = len(levels) - 1
        if isinstance(cmap, list) and len(cmap) != ncolors:
            raise ValueError('The number of colors in the colormap '
                             'must match the intervals defined in the '
                             f'color_levels, expected {ncolors} colors found {len(cmap)}.')
    try:
        palette = process_cmap(cmap, ncolors)
    except Exception as e:
        colorscale = colors.PLOTLY_SCALES.get(cmap)
        if colorscale is None:
            raise e
        return colorscale

    if isinstance(levels, int):
        colorscale = []
        scale = np.linspace(0, 1, levels+1)
        for i in range(levels+1):
            if i == 0:
                colorscale.append((scale[0], palette[i]))
            elif i == levels:
                colorscale.append((scale[-1], palette[-1]))
            else:
                colorscale.append((scale[i], palette[i-1]))
                colorscale.append((scale[i], palette[i]))
        return colorscale
    elif isinstance(levels, list):
        palette, (cmin, cmax) = color_intervals(
            palette, levels, clip=(cmin, cmax))
    return colors.make_colorscale(palette)


def configure_matching_axes_from_dims(fig, matching_prop='_dim'):
    """Configure matching axes for a figure

    Notes
    -----
    This function mutates the input figure

    Parameters
    ----------
    fig : dict
        The figure dictionary to process.
    matching_prop : str
        The name of the axis property that should be used to determine that two axes
        should be matched together.  If the property is missing or None, axes will not
        be matched
    """
    # Build mapping from matching properties to (axis, ref) tuples
    axis_map = {}

    for k, v in fig.get('layout', {}).items():
        if k[1:5] == 'axis':
            matching_val = v.get(matching_prop, None)
            axis_map.setdefault(matching_val, [])

            # Get axis reference as used by matching ('xaxis3' -> 'x3')
            axis_ref = k.replace('axis', '')

            # Append axis entry to mapping
            axis_pair = (axis_ref, v)
            axis_map[matching_val].append(axis_pair)

    # Set matching
    for _, axis_pairs in axis_map.items():
        if len(axis_pairs) < 2:
            continue

        matches_reference, linked_axis = axis_pairs[0]
        for _, axis in axis_pairs[1:]:
            axis['matches'] = matches_reference
            if 'range' in axis and 'range' in linked_axis:
                linked_axis['range'] = [
                    v if isfinite(v) else None for v in max_range([axis['range'], linked_axis['range']])
                ]


def clean_internal_figure_properties(fig):
    """Remove all HoloViews internal properties (those with leading underscores) from the
    input figure.

    Notes
    -----
    This function mutates the input figure

    Parameters
    ----------
    fig : dict
        The figure dictionary to process.
    """
    fig_props = list(fig)
    for prop in fig_props:
        val = fig[prop]
        if prop.startswith('_'):
            fig.pop(prop)
        elif isinstance(val, dict):
            clean_internal_figure_properties(val)
        elif isinstance(val, (list, tuple)) and val and isinstance(val[0], dict):
            for el in val:
                clean_internal_figure_properties(el)


def _convert_numpy_in_fig_dict(fig_dict):
    if isinstance(fig_dict, dict):
        if fig_dict.keys() == {"dtype", "bdata"}:
            return np.frombuffer(base64.b64decode(fig_dict["bdata"]), dtype=fig_dict["dtype"])
        elif fig_dict.keys() == {"dtype", "bdata", "shape"}:
            shape = list(map(int, fig_dict["shape"].split(",")))
            return np.frombuffer(base64.b64decode(fig_dict["bdata"]), dtype=fig_dict["dtype"]).reshape(shape)
        return {key: _convert_numpy_in_fig_dict(value) for key, value in fig_dict.items()}
    elif isinstance(fig_dict, list):
        return [_convert_numpy_in_fig_dict(item) for item in fig_dict]
    else:
        return fig_dict
