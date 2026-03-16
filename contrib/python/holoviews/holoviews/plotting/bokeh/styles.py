"""Defines valid style options, validation and utilities

"""

import numpy as np
from bokeh.core.properties import (
    Angle,
    Color,
    DashPattern,
    FontSize,
    MarkerType,
    Percent,
    Size,
)

try:
    from matplotlib import cm, colors
except ImportError:
    cm, colors = None, None

from ...core.options import abbreviated_exception
from ...core.util import arraylike_types
from ...util.transform import dim
from ..util import COLOR_ALIASES, RGB_HEX_REGEX, rgb2hex

# Define shared style properties for bokeh plots

property_prefixes = ['selection', 'nonselection', 'muted', 'hover']

base_properties = ['visible', 'muted']

line_base_properties = ['line_color', 'line_alpha', 'color', 'alpha', 'line_width',
                        'line_join', 'line_cap', 'line_dash', 'line_dash_offset']
line_properties = line_base_properties + [f'{prefix}_{prop}'
                                          for prop in line_base_properties
                                          for prefix in property_prefixes]

fill_base_properties = ['fill_color', 'fill_alpha']
fill_properties = fill_base_properties + [f'{prefix}_{prop}'
                                          for prop in fill_base_properties
                                          for prefix in property_prefixes]

border_properties = ['border_' + prop for prop in [*line_base_properties, 'radius']]

hatch_properties = ['hatch_color', 'hatch_scale', 'hatch_weight',
                    'hatch_extra', 'hatch_pattern', 'hatch_alpha']
background_properties = ['background_' + prop for prop in fill_base_properties + hatch_properties]

text_properties = ['text_font', 'text_font_size', 'text_font_style', 'text_color',
                   'text_alpha', 'text_align', 'text_baseline']

legend_dimensions = ['label_standoff', 'label_width', 'label_height', 'glyph_width',
                     'glyph_height', 'legend_padding', 'legend_spacing', 'click_policy']

# Conversion between matplotlib and bokeh markers

markers = {
    '+': {'marker': 'cross'},
    's': {'marker': 'square'},
    'd': {'marker': 'diamond'},
    '^': {'marker': 'triangle', 'angle': 0},
    '>': {'marker': 'triangle', 'angle': -np.pi/2},
    'v': {'marker': 'triangle', 'angle': np.pi},
    '<': {'marker': 'triangle', 'angle': np.pi/2},
    '1': {'marker': 'triangle', 'angle': 0},
    '2': {'marker': 'triangle', 'angle': -np.pi/2},
    '3': {'marker': 'triangle', 'angle': np.pi},
    '4': {'marker': 'triangle', 'angle': np.pi/2},
    'o': {'marker': 'circle'},
    '*': {'marker': 'asterisk'},
}


def mpl_to_bokeh(properties):
    """Utility to process style properties converting any
    matplotlib specific options to their nearest bokeh
    equivalent.

    """
    new_properties = {}
    for k, v in properties.items():
        if isinstance(v, dict):
            new_properties[k] = v
        elif k == 's':
            new_properties['size'] = v
        elif k == 'marker':
            new_properties.update(markers.get(v, {'marker': v}))
        elif (k == 'color' or k.endswith('_color')) and not isinstance(v, (dict, dim)):
            with abbreviated_exception():
                v = COLOR_ALIASES.get(v, v)
            if isinstance(v, tuple):
                with abbreviated_exception():
                    v = rgb2hex(v)
            new_properties[k] = v
        else:
            new_properties[k] = v
    new_properties.pop('cmap', None)
    return new_properties

# Validation

alpha     = Percent()
angle     = Angle()
color     = Color()
dash_pattern = DashPattern()
font_size = FontSize()
marker    = MarkerType()
size      = Size()

validators = {
    'angle'     : angle.is_valid,
    'alpha'     : alpha.is_valid,
    'color'     : lambda x: (
        color.is_valid(x) or (isinstance(x, str) and RGB_HEX_REGEX.match(x))
    ),
    'font_size' : font_size.is_valid,
    'line_dash' : dash_pattern.is_valid,
    'marker'    : lambda x: marker.is_valid(x) or x in markers,
    'size'      : size.is_valid,
}

def get_validator(style):
    for k, v in validators.items():
        if style.endswith(k):
            return v


def validate(style, value, scalar=False):
    """Validates a style and associated value.

    Parameters
    ----------
    style : str
       The style to validate (e.g. 'color', 'size' or 'marker')
    value :
       The style value to validate
    scalar : bool

    Returns
    -------
    valid : boolean or None
       If validation is supported returns boolean, otherwise None
    """
    validator = get_validator(style)
    if validator is None:
        return None
    if isinstance(value, (*arraylike_types, list)):
        if scalar:
            return False
        return all(validator(v) for v in value)
    return validator(value)

# Utilities

def rgba_tuple(rgba):
    """Ensures RGB(A) tuples in the range 0-1 are scaled to 0-255.

    """
    if isinstance(rgba, tuple):
        return tuple(int(c*255) if i<3 else c for i, c in enumerate(rgba))
    else:
        return COLOR_ALIASES.get(rgba, rgba)


def expand_batched_style(style, opts, mapping, nvals):
    """Computes styles applied to a batched plot by iterating over the
    supplied list of style options and expanding any options found in
    the supplied style dictionary returning a data and mapping defining
    the data that should be added to the ColumnDataSource.

    """
    opts = sorted(opts, key=lambda x: x in ['color', 'alpha'])
    applied_styles = set(mapping)
    style_data, style_mapping = {}, {}
    for opt in opts:
        if 'color' in opt:
            alias = 'color'
        elif 'alpha' in opt:
            alias = 'alpha'
        else:
            alias = None
        if opt not in style or opt in mapping:
            continue
        elif opt == alias:
            if alias in applied_styles:
                continue
            elif 'line_'+alias in applied_styles:
                if 'fill_'+alias not in opts:
                    continue
                opt = 'fill_'+alias
                val = style[alias]
            elif 'fill_'+alias in applied_styles:
                opt = 'line_'+alias
                val = style[alias]
            else:
                val = style[alias]
        else:
            val = style[opt]
        style_mapping[opt] = {'field': opt}
        applied_styles.add(opt)
        if 'color' in opt and isinstance(val, tuple):
            val = rgb2hex(val)
        style_data[opt] = [val]*nvals
    return style_data, style_mapping
