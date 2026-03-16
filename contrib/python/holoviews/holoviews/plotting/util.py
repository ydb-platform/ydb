import bisect
import re
import traceback
import warnings
from collections import defaultdict, namedtuple

import numpy as np
import param
from packaging.version import Version

from ..core import (
    AdjointLayout,
    CompositeOverlay,
    DynamicMap,
    GridSpace,
    HoloMap,
    Layout,
    NdLayout,
    NdOverlay,
    Overlay,
)
from ..core.ndmapping import item_check
from ..core.operation import Operation
from ..core.options import CallbackError, Cycle
from ..core.spaces import get_nested_streams
from ..core.util import (
    arraylike_types,
    closest_match,
    disable_constant,
    dtype_kind,
    get_overlay_spec,
    is_number,
    isfinite,
    match_spec,
    unique_iterator,
    wrap_tuple,
)
from ..element import Points
from ..streams import LinkedStream, Params
from ..util.transform import dim


def displayable(obj):
    """Predicate that returns whether the object is displayable or not
    (i.e. whether the object obeys the nesting hierarchy)

    """
    if isinstance(obj, Overlay) and any(isinstance(o, (HoloMap, GridSpace, AdjointLayout))
                                        for o in obj):
        return False
    if isinstance(obj, HoloMap):
        return obj.type not in [Layout, GridSpace, NdLayout, DynamicMap]
    if isinstance(obj, (GridSpace, Layout, NdLayout)):
        for el in obj.values():
            if not displayable(el):
                return False
        return True
    return True


class Warning(param.Parameterized): pass
display_warning = Warning(name='Warning')

def collate(obj):
    if isinstance(obj, Overlay):
        nested_type = next(type(o).__name__ for o in obj
                       if isinstance(o, (HoloMap, GridSpace, AdjointLayout)))
        display_warning.param.warning(
            f"Nesting {nested_type}s within an Overlay makes it difficult to "
            "access your data or control how it appears; we recommend "
            "calling .collate() on the Overlay in order to follow the "
            "recommended nesting structure shown in the Composing Data "
            "user guide (https://goo.gl/2YS8LJ)")

        return obj.collate()
    if isinstance(obj, DynamicMap):
        if obj.type in [DynamicMap, HoloMap]:
            obj_name = obj.type.__name__
            raise Exception(f"Nesting a {obj_name} inside a DynamicMap is not "
                            "supported. Ensure that the DynamicMap callback "
                            "returns an Element or (Nd)Overlay. If you have "
                            "applied an operation ensure it is not dynamic by "
                            "setting dynamic=False.")
        return obj.collate()
    if isinstance(obj, HoloMap):
        display_warning.param.warning(
            f"Nesting {obj.type.__name__}s within a {type(obj).__name__} "
            "makes it difficult to access your data or control how it appears; "
            f"we recommend calling .collate() on the {type(obj).__name__} "
            "in order to follow the recommended nesting structure shown "
            "in the Composing Data user guide (https://goo.gl/2YS8LJ)"
        )
        return obj.collate()
    elif isinstance(obj, (Layout, NdLayout)):
        try:
            display_warning.param.warning(
                "Layout contains HoloMaps which are not nested in the "
                "recommended format for accessing your data; calling "
                ".collate() on these objects will resolve any violations "
                "of the recommended nesting presented in the Composing Data "
                "tutorial (https://goo.gl/2YS8LJ)")
            expanded = []
            for el in obj.values():
                if isinstance(el, HoloMap) and not displayable(el):
                    collated_layout = Layout(el.collate())
                    expanded.extend(collated_layout.values())
            return Layout(expanded)
        except Exception as e:
            raise Exception(undisplayable_info(obj)) from e
    else:
        raise Exception(undisplayable_info(obj))


def isoverlay_fn(obj):
    """Determines whether object is a DynamicMap returning (Nd)Overlay types.

    """
    return isinstance(obj, CompositeOverlay) or (isinstance(obj, DynamicMap) and (isinstance(obj.last, CompositeOverlay)))


def overlay_depth(obj):
    """Computes the depth of a DynamicMap overlay if it can be determined
    otherwise return None.

    """
    if isinstance(obj, DynamicMap):
        if isinstance(obj.last, CompositeOverlay):
            return len(obj.last)
        elif obj.last is None:
            return None
        return 1
    else:
        return 1


def compute_overlayable_zorders(obj, path=None):
    """Traverses an overlayable composite container to determine which
    objects are associated with specific (Nd)Overlay layers by
    z-order, making sure to take DynamicMap Callables into
    account. Returns a mapping between the zorders of each layer and a
    corresponding lists of objects.

    Used to determine which overlaid subplots should be linked with
    Stream callbacks.

    """
    if path is None:
        path = []
    path = [*path, obj]
    zorder_map = defaultdict(list)

    # Process non-dynamic layers
    if not isinstance(obj, DynamicMap):
        if isinstance(obj, CompositeOverlay):
            for z, o in enumerate(obj):
                zorder_map[z] = [o, obj]
        elif isinstance(obj, HoloMap):
            for el in obj.values():
                if isinstance(el, CompositeOverlay):
                    for k, v in compute_overlayable_zorders(el, path).items():
                        zorder_map[k] += [*v, obj]
                else:
                    zorder_map[0] += [obj, el]
        elif obj not in zorder_map[0]:
            zorder_map[0].append(obj)
        return zorder_map

    isoverlay = isinstance(obj.last, CompositeOverlay)
    isdynoverlay = obj.callback._is_overlay
    if obj not in zorder_map[0] and not isoverlay:
        zorder_map[0].append(obj)
    depth = overlay_depth(obj)

    # Process the inputs of the DynamicMap callback
    dmap_inputs = obj.callback.inputs if obj.callback.link_inputs else []
    for z, inp in enumerate(dmap_inputs):
        no_zorder_increment = False
        if any(not (isoverlay_fn(p) or p.last is None) for p in path) and isoverlay_fn(inp):
            # If overlay has been collapsed do not increment zorder
            no_zorder_increment = True

        input_depth = overlay_depth(inp)
        if depth is not None and input_depth is not None and depth < input_depth:
            # Skips branch of graph where the number of elements in an
            # overlay has been reduced but still contains more than one layer
            if depth > 1:
                continue
            else:
                no_zorder_increment = True

        # Recurse into DynamicMap.callback.inputs and update zorder_map
        z = z if isdynoverlay else 0
        deep_zorders = compute_overlayable_zorders(inp, path=path)
        offset = max(zorder_map.keys())
        for dz, objs in deep_zorders.items():
            global_z = offset+z if no_zorder_increment else offset+dz+z
            zorder_map[global_z] = list(unique_iterator(zorder_map[global_z]+objs))

    # If object branches but does not declare inputs (e.g. user defined
    # DynamicMaps returning (Nd)Overlay) add the items on the DynamicMap.last
    found = any(isinstance(p, DynamicMap) and p.callback._is_overlay for p in path)
    linked =  any(isinstance(s, (LinkedStream, Params)) and s.linked
                  for s in obj.streams)
    if (found or linked) and isoverlay and not isdynoverlay:
        offset = max(zorder_map.keys())
        for z, o in enumerate(obj.last):
            if isoverlay and linked:
                zorder_map[offset+z].append(obj)
            if o not in zorder_map[offset+z]:
                zorder_map[offset+z].append(o)
    return zorder_map


def is_dynamic_overlay(dmap):
    """Traverses a DynamicMap graph and determines if any components
    were overlaid dynamically (i.e. by * on a DynamicMap).

    """
    if not isinstance(dmap, DynamicMap):
        return False
    elif dmap.callback._is_overlay:
        return True
    else:
        return any(is_dynamic_overlay(dm) for dm in dmap.callback.inputs)


def split_dmap_overlay(obj, depth=0):
    """Splits a DynamicMap into the original component layers it was
    constructed from by traversing the graph to search for dynamically
    overlaid components (i.e. constructed by using * on a DynamicMap).
    Useful for assigning subplots of an OverlayPlot the streams that
    are responsible for driving their updates. Allows the OverlayPlot
    to determine if a stream update should redraw a particular
    subplot.

    """
    layers, streams = [], []
    if isinstance(obj, DynamicMap):
        initialize_dynamic(obj)
        if issubclass(obj.type, NdOverlay) and not depth:
            for _ in obj.last.values():
                layers.append(obj)
                streams.append(obj.streams)
        elif issubclass(obj.type, Overlay):
            if obj.callback.inputs and is_dynamic_overlay(obj):
                for inp in obj.callback.inputs:
                    split, sub_streams = split_dmap_overlay(inp, depth+1)
                    layers += split
                    streams += [s+obj.streams for s in sub_streams]
            else:
                for _ in obj.last.values():
                    layers.append(obj)
                    streams.append(obj.streams)
        else:
            layers.append(obj)
            streams.append(obj.streams)
        return layers, streams
    if isinstance(obj, Overlay):
        for _k, v in obj.items():
            layers.append(v)
            streams.append([])
    else:
        layers.append(obj)
        streams.append([])
    return layers, streams


def initialize_dynamic(obj):
    """Initializes all DynamicMap objects contained by the object

    """
    dmaps = obj.traverse(lambda x: x, specs=[DynamicMap])
    for dmap in dmaps:
        if dmap.unbounded:
            # Skip initialization until plotting code
            continue
        if not len(dmap):
            dmap[dmap._initial_key()]


def get_plot_frame(map_obj, key_map, cached=False):
    """Returns the current frame in a mapping given a key mapping.

    Parameters
    ----------
    obj
        Nested Dimensioned object
    key_map
        Dictionary mapping between dimensions and key value
    cached
        Whether to allow looking up key in cache

    Returns
    -------
    The item in the mapping corresponding to the supplied key.
    """
    if (map_obj.kdims and len(map_obj.kdims) == 1 and map_obj.kdims[0] == 'Frame' and
        not isinstance(map_obj, DynamicMap)):
        # Special handling for static plots
        return map_obj.last
    key = tuple(key_map[kd.name] for kd in map_obj.kdims if kd.name in key_map)
    if key in map_obj.data and cached:
        return map_obj.data[key]
    else:
        try:
            return map_obj[key]
        except KeyError:
            return None
        except (StopIteration, CallbackError) as e:
            raise e
        except Exception:
            print(traceback.format_exc())
            return None


def get_nested_plot_frame(obj, key_map, cached=False):
    """Extracts a single frame from a nested object.

    Replaces any HoloMap or DynamicMap in the nested data structure,
    with the item corresponding to the supplied key.

    Parameters
    ----------
    obj
        Nested Dimensioned object
    key_map
        Dictionary mapping between dimensions and key value
    cached
        Whether to allow looking up key in cache

    Returns
    -------
    Nested datastructure where maps are replaced with single frames
    """
    clone = obj.map(lambda x: x)

    # Ensure that DynamicMaps in the cloned frame have
    # identical callback inputs to allow memoization to work
    for it1, it2 in zip(obj.traverse(lambda x: x), clone.traverse(lambda x: x), strict=None):
        if isinstance(it1, DynamicMap):
            with disable_constant(it2.callback):
                it2.callback.inputs = it1.callback.inputs
    with item_check(False):
        return clone.map(lambda x: get_plot_frame(x, key_map, cached=cached),
                         [DynamicMap, HoloMap], clone=False)


def undisplayable_info(obj, html=False):
    """Generate helpful message regarding an undisplayable object

    """
    collate = '<tt>collate</tt>' if html else 'collate'
    info = "For more information, please consult the Composing Data tutorial (https://git.io/vtIQh)"
    if isinstance(obj, HoloMap):
        error = f"HoloMap of {obj.type.__name__} objects cannot be displayed."
        remedy = f"Please call the {collate} method to generate a displayable object"
    elif isinstance(obj, Layout):
        error = "Layout containing HoloMaps of Layout or GridSpace objects cannot be displayed."
        remedy = f"Please call the {collate} method on the appropriate elements."
    elif isinstance(obj, GridSpace):
        error = "GridSpace containing HoloMaps of Layouts cannot be displayed."
        remedy = f"Please call the {collate} method on the appropriate elements."

    if not html:
        return f'{error}\n{remedy}\n{info}'
    else:
        return "<center>{msg}</center>".format(msg=('<br>'.join(
            [f'<b>{error}</b>', remedy, f'<i>{info}</i>'])))


def compute_sizes(sizes, size_fn, scaling_factor, scaling_method, base_size):
    """Scales point sizes according to a scaling factor,
    base size and size_fn, which will be applied before
    scaling.

    """
    if dtype_kind(sizes) not in ('i', 'f'):
        return None
    if scaling_method == 'area':
        pass
    elif scaling_method == 'width':
        scaling_factor = scaling_factor**2
    else:
        raise ValueError(
            f'Invalid value for argument "scaling_method": "{scaling_method}". '
            'Valid values are: "width", "area".')
    sizes = size_fn(sizes)
    return (base_size*scaling_factor*sizes)


def get_axis_padding(padding):
    """Process a padding value supplied as a tuple or number and returns
    padding values for x-, y- and z-axis.

    """
    if isinstance(padding, tuple):
        if len(padding) == 2:
            xpad, ypad = padding
            zpad = 0
        elif len(padding) == 3:
            xpad, ypad, zpad = padding
        else:
            raise ValueError('Padding must be supplied as an number applied '
                             'to all axes or a length two or three tuple '
                             'corresponding to the x-, y- and optionally z-axis')
    else:
        xpad, ypad, zpad = (padding,)*3
    return (xpad, ypad, zpad)


def get_minimum_span(low, high, span):
    """If lower and high values are equal ensures they are separated by
    the defined span.

    """
    if is_number(low) and low == high:
        if isinstance(low, np.datetime64):
            span = span * np.timedelta64(1, 's')
        low, high = low-span, high+span
    return low, high


def get_range(element, ranges, dimension):
    """Computes the data, soft- and hard-range along a dimension given
    an element and a dictionary of ranges.

    """
    if dimension and dimension != 'categorical':
        if ranges and dimension.label in ranges:
            drange = ranges[dimension.label]['data']
            srange = ranges[dimension.label]['soft']
            hrange = ranges[dimension.label]['hard']
        else:
            drange = element.range(dimension, dimension_range=False)
            srange = dimension.soft_range
            hrange = dimension.range
    else:
        drange = srange = hrange = (np.nan, np.nan)
    return drange, srange, hrange


def get_sideplot_ranges(plot, element, main, ranges):
    """Utility to find the range for an adjoined
    plot given the plot, the element, the
    Element the plot is adjoined to and the
    dictionary of ranges.

    """
    key = plot.current_key
    dims = element.dimensions()
    dim = dims[0] if 'frequency' in dims[1].name or 'count' in dims[1].name else dims[1]
    range_item = main
    if isinstance(main, HoloMap):
        if issubclass(main.type, CompositeOverlay):
            range_item = next(hm for hm in main._split_overlays()[1]
                          if dim in hm.dimensions('all'))
    else:
        range_item = HoloMap({0: main}, kdims=['Frame'])
        ranges = match_spec(range_item.last, ranges)

    if dim.label in ranges:
        main_range = ranges[dim.label]['combined']
    else:
        framewise = plot.lookup_options(range_item.last, 'norm').options.get('framewise')
        if framewise and range_item.get(key, False):
            main_range = range_item[key].range(dim)
        else:
            main_range = range_item.range(dim)

    # If .main is an NdOverlay or a HoloMap of Overlays get the correct style
    if isinstance(range_item, HoloMap):
        range_item = range_item.last
    if isinstance(range_item, CompositeOverlay):
        range_item = next(ov for ov in range_item
                      if dim in ov.dimensions('all'))
    return range_item, main_range, dim


def within_range(range1, range2):
    """Checks whether range1 is within the range specified by range2.

    """
    range1 = [r if isfinite(r) else None for r in range1]
    range2 = [r if isfinite(r) else None for r in range2]
    return ((range1[0] is None or range2[0] is None or range1[0] >= range2[0]) and
            (range1[1] is None or range2[1] is None or range1[1] <= range2[1]))


def validate_unbounded_mode(holomaps, dynmaps):
    composite = HoloMap(enumerate(holomaps), kdims=['testing_kdim'])
    holomap_kdims = set(unique_iterator([kd.name for dm in holomaps for kd in dm.kdims]))
    hmranges = {d: composite.range(d) for d in holomap_kdims}
    if any(not {d.name for d in dm.kdims} <= holomap_kdims
                        for dm in dynmaps):
        raise Exception('DynamicMap that are unbounded must have key dimensions that are a '
                        'subset of dimensions of the HoloMap(s) defining the keys.')
    elif not all(within_range(hmrange, dm.range(d)) for dm in dynmaps
                              for d, hmrange in hmranges.items() if d in dm.kdims):
        raise Exception('HoloMap(s) have keys outside the ranges specified on '
                        'the DynamicMap(s).')


def get_dynamic_mode(composite):
    """Returns the common mode of the dynamic maps in given composite object

    """
    dynmaps = composite.traverse(lambda x: x, [DynamicMap])
    holomaps = composite.traverse(lambda x: x, ['HoloMap'])
    dynamic_unbounded = any(m.unbounded for m in dynmaps)
    if holomaps:
        validate_unbounded_mode(holomaps, dynmaps)
    elif dynamic_unbounded and not holomaps:
        raise Exception("DynamicMaps in unbounded mode must be displayed alongside "
                        "a HoloMap to define the sampling.")
    return dynmaps and not holomaps, dynamic_unbounded


def initialize_unbounded(obj, dimensions, key):
    """Initializes any DynamicMaps in unbounded mode.

    """
    select = dict(zip([d.name for d in dimensions], key, strict=None))
    try:
        obj.select(selection_specs=[DynamicMap], **select)
    except KeyError:
        pass


def dynamic_update(plot, subplot, key, overlay, items):
    """Given a plot, subplot and dynamically generated (Nd)Overlay
    find the closest matching Element for that plot.

    """
    match_spec = get_overlay_spec(overlay,
                                  wrap_tuple(key),
                                  subplot.current_frame)
    specs = [(i, get_overlay_spec(overlay, wrap_tuple(k), el))
             for i, (k, el) in enumerate(items)]
    closest = closest_match(match_spec, specs)
    if closest is None:
        return closest, None, False
    matched = specs[closest][1]
    return closest, matched, match_spec == matched


def map_colors(arr, crange, cmap, hex=True):
    """Maps an array of values to RGB hex strings, given
    a color range and colormap.

    """
    if isinstance(crange, arraylike_types):
        xsorted = np.argsort(crange)
        ypos = np.searchsorted(crange, arr)
        arr = xsorted[ypos]
    else:
        if isinstance(crange, tuple):
            cmin, cmax = crange
        else:
            cmin, cmax = np.nanmin(arr), np.nanmax(arr)
        arr = (arr - cmin) / (cmax-cmin)
        arr = np.ma.array(arr, mask=np.logical_not(np.isfinite(arr)))
    arr = cmap(arr)
    if hex:
        return rgb2hex(arr)
    else:
        return arr


def resample_palette(palette, ncolors, categorical, cmap_categorical):
    """Resample the number of colors in a palette to the selected number.

    """
    if len(palette) != ncolors:
        if categorical and cmap_categorical:
            palette = [palette[i%len(palette)] for i in range(ncolors)]
        else:
            lpad, rpad = -0.5, 0.49999999999
            indexes = np.linspace(lpad, (len(palette)-1)+rpad, ncolors)
            palette = [palette[int(np.round(v))] for v in indexes]
    return palette


def mplcmap_to_palette(cmap, ncolors=None, categorical=False):
    """Converts a matplotlib colormap to palette of RGB hex strings."

    """
    import matplotlib as mpl
    from matplotlib.colors import Colormap, ListedColormap

    ncolors = ncolors or 256
    if not isinstance(cmap, Colormap):
        # Alias bokeh Category cmaps with mpl tab cmaps
        if cmap.startswith('Category'):
            cmap = cmap.replace('Category', 'tab')

        if Version(mpl.__version__).release < (3, 5, 0):
            from matplotlib import cm
            try:
                cmap = cm.get_cmap(cmap)
            except Exception:
                cmap = cm.get_cmap(cmap.lower())
        else:
            from matplotlib import colormaps
            cmap = colormaps.get(cmap, colormaps.get(cmap.lower()))

    if isinstance(cmap, ListedColormap):
        if categorical:
            palette = [rgb2hex(cmap.colors[i%cmap.N]) for i in range(ncolors)]
            return palette
        elif cmap.N > ncolors:
            palette = [rgb2hex(c) for c in cmap(np.arange(cmap.N))]
            if len(palette) != ncolors:
                palette = [palette[int(v)] for v in np.linspace(0, len(palette)-1, ncolors)]
            return palette
    return [rgb2hex(c) for c in cmap(np.linspace(0, 1, ncolors))]


def colorcet_cmap_to_palette(cmap, ncolors=None, categorical=False):
    from colorcet import palette

    categories = ['glasbey']

    ncolors = ncolors or 256
    cmap_categorical = any(c in cmap for c in categories)

    if cmap.endswith('_r'):
        palette = list(reversed(palette[cmap[:-2]]))
    else:
        palette = palette[cmap]

    return resample_palette(palette, ncolors, categorical, cmap_categorical)


def bokeh_palette_to_palette(cmap, ncolors=None, categorical=False):
    from bokeh import palettes

    # Handle categorical colormaps to avoid interpolation
    categories = ['accent', 'category', 'dark', 'colorblind', 'pastel',
                  'set1', 'set2', 'set3', 'paired']
    cmap_categorical = any(cat in cmap.lower() for cat in categories)
    reverse = False
    if cmap.endswith('_r'):
        cmap = cmap[:-2]
        reverse = True

    # Some colormaps are inverted compared to matplotlib
    inverted = (not cmap_categorical and cmap.capitalize() not in palettes.mpl
                and not cmap.startswith('fire'))
    if inverted:
        reverse=not reverse
    ncolors = ncolors or 256

    # Alias mpl tab cmaps with bokeh Category cmaps
    if cmap.startswith('tab'):
        cmap = cmap.replace('tab', 'Category')

    # Process as bokeh palette
    if cmap in palettes.all_palettes:
        palette = palettes.all_palettes[cmap]
    else:
        palette = getattr(palettes, cmap, getattr(palettes, cmap.capitalize(), None))
    if palette is None:
        raise ValueError(f"Supplied palette {cmap} not found among bokeh palettes")
    elif isinstance(palette, dict) and (cmap in palette or cmap.capitalize() in palette):
        # Some bokeh palettes are doubly nested
        palette = palette.get(cmap, palette.get(cmap.capitalize()))

    if isinstance(palette, dict):
        palette = palette[max(palette)]
        if not cmap_categorical:
            if len(palette) < ncolors:
                palette = polylinear_gradient(palette, ncolors)
    elif callable(palette):
        palette = palette(ncolors)
    if reverse: palette = palette[::-1]

    return list(resample_palette(palette, ncolors, categorical, cmap_categorical))


def linear_gradient(start_hex, finish_hex, n=10):
    """Interpolates the color gradient between to hex colors

    """
    s = hex2rgb(start_hex)
    f = hex2rgb(finish_hex)
    gradient = [s]
    for t in range(1, n):
        curr_vector = [int(s[j] + (float(t)/(n-1))*(f[j]-s[j])) for j in range(3)]
        gradient.append(curr_vector)
    return [rgb2hex([c/255. for c in rgb]) for rgb in gradient]


def polylinear_gradient(colors, n):
    """Interpolates the color gradients between a list of hex colors.

    """
    n_out = int(float(n) / (len(colors)-1))
    gradient = linear_gradient(colors[0], colors[1], n_out)

    if len(colors) == len(gradient):
        return gradient

    for col in range(1, len(colors) - 1):
        next_colors = linear_gradient(colors[col], colors[col+1], n_out+1)
        gradient += next_colors[1:] if len(next_colors) > 1 else next_colors
    return gradient


cmap_info=[]
CMapInfo=namedtuple('CMapInfo',['name','provider','category','source','bg'])
providers = ['matplotlib', 'bokeh', 'colorcet']


def _list_cmaps(provider=None, records=False):
    """List available colormaps by combining matplotlib, bokeh, and
    colorcet colormaps or palettes if available. May also be
    narrowed down to a particular provider or list of providers.

    """
    if provider is None:
        provider = providers
    elif isinstance(provider, str):
        if provider not in providers:
            raise ValueError(f'Colormap provider {provider!r} not recognized, must '
                             f'be one of {providers!r}')
        provider = [provider]

    cmaps = []

    def info(provider,names):
        return [CMapInfo(name=n,provider=provider,category=None,source=None,bg=None) for n in names] \
               if records else list(names)

    if 'matplotlib' in provider:
        try:
            import matplotlib as mpl
            from matplotlib import cm

            # matplotlib.colormaps has been introduced in matplotlib 3.5
            # matplotlib.cm._cmap_registry was removed in matplotlib 3.6
            if hasattr(mpl, "colormaps"):
                mpl_cmaps = list(mpl.colormaps)
            elif hasattr(cm, '_cmap_registry'):
                mpl_cmaps = list(cm._cmap_registry)
            else:
                mpl_cmaps = list(cm.cmaps_listed)+list(cm.datad)
            cmaps += info('matplotlib', mpl_cmaps)
            cmaps += info('matplotlib', [cmap+'_r' for cmap in mpl_cmaps
                                         if not cmap.endswith('_r')])
        except ImportError:
            pass
    if 'bokeh' in provider:
        try:
            from bokeh import palettes
            cmaps += info('bokeh', palettes.all_palettes)
            cmaps += info('bokeh', [p+'_r' for p in palettes.all_palettes
                                    if not p.endswith('_r')])
        except ImportError:
            pass
    if 'colorcet' in provider:
        try:
            from colorcet import glasbey_hv, palette_n
            cet_maps = palette_n.copy()
            cet_maps['glasbey_hv'] = glasbey_hv # Add special hv-specific map
            cmaps += info('colorcet', cet_maps)
            cmaps += info('colorcet', [p+'_r' for p in cet_maps if not p.endswith('_r')])
        except ImportError:
            pass
    return sorted(unique_iterator(cmaps))


def register_cmaps(category, provider, source, bg, names):
    """Maintain descriptions of colormaps that include the following information:

    name     - string name for the colormap
    category - intended use or purpose, mostly following matplotlib
    provider - package providing the colormap directly
    source   - original source or creator of the colormaps
    bg       - base/background color expected for the map
               ('light','dark','medium','any' (unknown or N/A))

    """
    for name in names:
        bisect.insort(cmap_info, CMapInfo(name=name, provider=provider,
                                          category=category, source=source,
                                          bg=bg))


def list_cmaps(provider=None, records=False, name=None, category=None, source=None,
               bg=None, reverse=None):
    """Return colormap names matching the specified filters.

    """
    # Only uses names actually imported and currently available
    available = _list_cmaps(provider=provider, records=True)

    matches = set()

    for avail in available:
        aname=avail.name
        matched=False
        basename=aname[:-2] if aname.endswith('_r') else aname

        if (reverse is None or
            (reverse==True and aname.endswith('_r')) or
            (reverse==False and not aname.endswith('_r'))):
            for r in cmap_info:
               if (r.name==basename):
                   matched=True

                   # cmap_info stores only non-reversed info, so construct
                   # suitable values for reversed version if appropriate
                   r=r._replace(name=aname)
                   if aname.endswith('_r') and (r.category != 'Diverging'):
                       if r.bg=='light':
                           r=r._replace(bg='dark')
                       elif r.bg=='dark':
                           r=r._replace(bg='light')

                   if ((    name is None or     name in r.name) and
                       (provider is None or provider in r.provider) and
                       (category is None or category in r.category) and
                       (  source is None or   source in r.source) and
                       (      bg is None or       bg in r.bg)):
                       matches.add(r)
            if not matched and (category is None or category=='Miscellaneous'):
                # Return colormaps that exist but are not found in cmap_info
                # under the 'Miscellaneous' category, with no source or bg
                r = CMapInfo(aname,provider=avail.provider,category='Miscellaneous',source=None,bg=None)
                matches.add(r)

    # Return results sorted by category if category information is provided
    if records:
        return sorted(
            matches,
            key=lambda r: (r.category.split(" ")[-1], r.bg or "", r.name.lower(), r.provider, r.source or "")
        )
    else:
        return list(unique_iterator(sorted([rec.name for rec in matches], key=lambda n:n.lower())))


register_cmaps('Uniform Sequential', 'matplotlib', 'bids', 'dark',
    ['viridis', 'plasma', 'inferno', 'magma', 'cividis'])

register_cmaps('Mono Sequential', 'matplotlib', 'colorbrewer', 'light',
    ['Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds',
     'YlOrBr', 'YlOrRd', 'OrRd', 'PuRd', 'RdPu', 'BuPu',
     'GnBu', 'PuBu', 'YlGnBu', 'PuBuGn', 'BuGn', 'YlGn'])

register_cmaps('Other Sequential', 'matplotlib', 'misc', 'light',
    ['gist_yarg', 'binary'])

register_cmaps('Other Sequential', 'matplotlib', 'misc', 'dark',
    ['afmhot', 'gray', 'bone', 'gist_gray', 'gist_heat',
     'hot', 'pink'])

register_cmaps('Other Sequential', 'matplotlib', 'misc', 'any',
    ['copper', 'spring', 'summer', 'autumn', 'winter', 'cool', 'Wistia'])

register_cmaps('Diverging', 'matplotlib', 'colorbrewer', 'light',
    ['BrBG', 'PiYG', 'PRGn', 'PuOr', 'RdBu', 'RdGy',
     'RdYlBu', 'RdYlGn', 'Spectral'])

register_cmaps('Diverging', 'matplotlib', 'misc', 'light',
    ['coolwarm', 'bwr', 'seismic'])

register_cmaps('Categorical', 'matplotlib', 'colorbrewer', 'any',
    ['Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2',
     'Set1', 'Set2', 'Set3'])

register_cmaps('Categorical', 'matplotlib', 'd3', 'any',
    ['tab10', 'tab20', 'tab20b', 'tab20c'])

register_cmaps('Rainbow', 'matplotlib', 'misc', 'dark',
    ['nipy_spectral', 'gist_ncar'])

register_cmaps('Rainbow', 'matplotlib', 'misc', 'any',
    ['brg', 'hsv', 'gist_rainbow', 'rainbow', 'jet'])

register_cmaps('Miscellaneous', 'matplotlib', 'misc', 'dark',
    ['CMRmap', 'cubehelix', 'gist_earth', 'gist_stern',
     'gnuplot', 'gnuplot2', 'ocean', 'terrain'])

register_cmaps('Miscellaneous', 'matplotlib', 'misc', 'any',
    ['flag', 'prism'])


register_cmaps('Uniform Sequential', 'colorcet', 'cet', 'dark',
    ['bgyw', 'bgy', 'kbc', 'bmw', 'bmy', 'kgy', 'gray',
     'dimgray', 'fire'])

register_cmaps('Uniform Sequential', 'colorcet', 'cet', 'any',
    ['blues', 'kr', 'kg', 'kb'])

register_cmaps('Uniform Diverging', 'colorcet', 'cet', 'light',
    ['coolwarm', 'gwv', 'bwy', 'cwr'])

register_cmaps('Uniform Diverging', 'colorcet', 'cet', 'dark',
    ['bkr', 'bky'])

register_cmaps('Uniform Diverging', 'colorcet', 'cet', 'medium',
    ['bjy'])

register_cmaps('Uniform Rainbow', 'colorcet', 'cet', 'any',
    ['rainbow', 'colorwheel','isolum'])


register_cmaps('Uniform Sequential', 'bokeh', 'bids', 'dark',
    ['Viridis', 'Plasma', 'Inferno', 'Magma'])

register_cmaps('Mono Sequential', 'bokeh', 'colorbrewer', 'light',
    ['Blues', 'BuGn', 'BuPu', 'GnBu', 'Greens', 'Greys',
     'OrRd', 'Oranges', 'PuBu', 'PuBuGn', 'PuRd', 'Purples',
     'RdPu', 'Reds', 'YlGn', 'YlGnBu', 'YlOrBr', 'YlOrRd'])

register_cmaps('Diverging', 'bokeh', 'colorbrewer', 'light',
    ['BrBG', 'PiYG', 'PRGn', 'PuOr', 'RdBu', 'RdGy',
     'RdYlBu', 'RdYlGn', 'Spectral'])

register_cmaps('Categorical', 'bokeh', 'd3', 'any',
    ['Category10', 'Category20', 'Category20b', 'Category20c'])

register_cmaps('Categorical', 'bokeh', 'colorbrewer', 'any',
    ['Accent', 'Dark2', 'Paired', 'Pastel1', 'Pastel2',
     'Set1', 'Set2', 'Set3'])

register_cmaps('Categorical', 'bokeh', 'misc', 'any',
    ['Colorblind'])

register_cmaps('Uniform Categorical', 'colorcet', 'cet', 'any',
    ['glasbey', 'glasbey_cool', 'glasbey_warm', 'glasbey_hv'])

register_cmaps('Uniform Categorical', 'colorcet', 'cet', 'dark',
    ['glasbey_light'])

register_cmaps('Uniform Categorical', 'colorcet', 'cet', 'light',
    ['glasbey_dark'])


def process_cmap(cmap, ncolors=None, provider=None, categorical=False):
    """Convert valid colormap specifications to a list of colors.

    """
    providers_checked="matplotlib, bokeh, or colorcet" if provider is None else provider

    if isinstance(cmap, Cycle):
        palette = [rgb2hex(c) if isinstance(c, tuple) else c for c in cmap.values]
    elif isinstance(cmap, tuple):
        palette = list(cmap)
    elif isinstance(cmap, list):
        palette = cmap
    elif isinstance(cmap, str):
        mpl_cmaps = _list_cmaps('matplotlib')
        bk_cmaps = _list_cmaps('bokeh')
        cet_cmaps = _list_cmaps('colorcet')
        if provider == 'matplotlib' or (provider is None and (cmap in mpl_cmaps or cmap.lower() in mpl_cmaps)):
            palette = mplcmap_to_palette(cmap, ncolors, categorical)
        elif provider == 'bokeh' or (provider is None and (cmap in bk_cmaps or cmap.capitalize() in bk_cmaps)):
            palette = bokeh_palette_to_palette(cmap, ncolors, categorical)
        elif provider == 'colorcet' or (provider is None and cmap in cet_cmaps):
            palette = colorcet_cmap_to_palette(cmap, ncolors, categorical)
        else:
            raise ValueError(f"Supplied cmap {cmap} not found among {providers_checked} colormaps.")
    else:
        try:
            # Try processing as matplotlib colormap
            palette = mplcmap_to_palette(cmap, ncolors)
        except Exception:
            palette = None
    if not isinstance(palette, list):
        raise TypeError(f"cmap argument {cmap} expects a list, Cycle or valid {providers_checked} colormap or palette.")
    if ncolors and (n_palette := len(palette)) != ncolors:
        return [palette[i%n_palette] for i in range(ncolors)]
    return palette


def color_intervals(colors, levels, clip=None, N=255):
    """Maps the supplied colors into bins defined by the supplied levels.
    If a clip tuple is defined the bins are clipped to the defined
    range otherwise the range is computed from the levels and returned.

    Parameters
    ----------
    colors : list
      List of colors (usually hex string or named colors)
    levels : list or array_like
      Levels specifying the bins to map the colors to
    clip : tuple, optional
      Lower and upper limits of the color range
    N : int
      Number of discrete colors to map the range onto

    Returns
    -------
    cmap : list
      List of colors
    clip : tuple
      Lower and upper bounds of the color range
    """
    if len(colors) != len(levels)-1:
        raise ValueError('The number of colors in the colormap '
                         'must match the intervals defined in the '
                         f'color_levels, expected {N} colors found {len(colors)}.')
    intervals = np.diff(levels)
    cmin, cmax = min(levels), max(levels)
    interval = cmax-cmin
    cmap = []
    for intv, c in zip(intervals, colors, strict=None):
        cmap += [c]*round(N*(intv/interval))
    if clip is not None:
        clmin, clmax = clip
        lidx = round(N*((clmin-cmin)/interval))
        uidx = len(cmap) - round(N*((cmax-clmax)/interval))
        if lidx == uidx:
            uidx = lidx+1
        cmap = cmap[lidx:uidx]
        if clmin == clmax:
            idx = np.argmin(np.abs(np.array(levels)-clmin))
            clip = levels[idx: idx+2] if len(levels) > idx+2 else levels[idx-1: idx+1]
    return cmap, clip


def dim_axis_label(dimensions, separator=', '):
    """Returns an axis label for one or more dimensions.

    """
    if not isinstance(dimensions, list): dimensions = [dimensions]
    return separator.join([d.pprint_label for d in dimensions])


def scale_fontsize(size, scaling):
    """Scales a numeric or string font size.

    """
    ext = None
    if isinstance(size, str):
        match = re.match(r"[-+]?\d*\.\d+|\d+", size)
        if match:
            value = match.group()
            ext = size.replace(value, '')
            size = float(value)
        else:
            return size

    if scaling:
        size = size * scaling

    if ext is not None:
        size = f'{size:.3f}'.rstrip('0').rstrip('.') + ext
    return size


def attach_streams(plot, obj, precedence=1.1):
    """Attaches plot refresh to all streams on the object.

    """
    def append_refresh(dmap):
        for stream in get_nested_streams(dmap):
            if plot.refresh not in stream._subscribers:
                stream.add_subscriber(plot.refresh, precedence)
    return obj.traverse(append_refresh, [DynamicMap])


def traverse_setter(obj, attribute, value):
    """Traverses the object and sets the supplied attribute on the
    object. Supports Dimensioned and DimensionedPlot types.

    """
    obj.traverse(lambda x: setattr(x, attribute, value))


def _get_min_distance_numpy(element):
    """NumPy based implementation of get_min_distance

    """
    xys = element.array([0, 1])
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', r'invalid value encountered in')
        xys = xys.astype('float32').view(np.complex64)
        distances = np.abs(xys.T-xys)
        np.fill_diagonal(distances, np.inf)
        distances = distances[distances>0]
        if len(distances):
            return distances.min()
    return 0


def get_min_distance(element):
    """Gets the minimum sampling distance of the x- and y-coordinates
    in a grid.

    """
    try:
        from scipy.spatial.distance import pdist
        return pdist(element.array([0, 1])).min()
    except Exception:
        return _get_min_distance_numpy(element)


def get_directed_graph_paths(element, arrow_length):
    """Computes paths for a directed path which include an arrow to
    indicate the directionality of each edge.

    """
    edgepaths = element._split_edgepaths
    edges = edgepaths.split(datatype='array', dimensions=edgepaths.kdims)
    arrows = []
    for e in edges:
        sx, sy = e[0]
        ex, ey = e[1]
        rad = np.arctan2(ey-sy, ex-sx)
        xa0 = ex - np.cos(rad+np.pi/8)*arrow_length
        ya0 = ey - np.sin(rad+np.pi/8)*arrow_length
        xa1 = ex - np.cos(rad-np.pi/8)*arrow_length
        ya1 = ey - np.sin(rad-np.pi/8)*arrow_length
        arrow = np.array([(sx, sy), (ex, ey), (np.nan, np.nan),
                          (xa0, ya0), (ex, ey), (xa1, ya1)])
        arrows.append(arrow)
    return arrows


def rgb2hex(rgb):
    """Convert RGB(A) tuple to hex.

    """
    if len(rgb) > 3:
        rgb = rgb[:-1]
    return "#{:02x}{:02x}{:02x}".format(*(int(v*255) for v in rgb))


def dim_range_key(eldim):
    """Returns the key to look up a dimension range.

    """
    if isinstance(eldim, dim):
        dim_name = repr(eldim)
        if dim_name.startswith("dim('") and dim_name.endswith("')"):
            dim_name = dim_name[5:-2]
    else:
        dim_name = eldim.label
    return dim_name


def hex2rgb(hex):
  """Convert hex code to RGB integers
  "#FFFFFF" -> [255,255,255]

  """
  # Pass 16 to the integer function for change of base
  return [int(hex[i:i+2], 16) for i in range(1,6,2)]


class apply_nodata(Operation):

    link_inputs = param.Boolean(default=True)

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN so
        that it is transparent (by default) when plotted.""")

    def _replace_value(self, data):
        """Replace `nodata` value in data with NaN, if specified in opts

        """
        data = data.astype('float64')
        mask = data!=self.p.nodata
        if hasattr(data, 'where'):
            return data.where(mask, np.nan)
        return np.where(mask, data, np.nan)

    def _process(self, element, key=None):
        if self.p.nodata is None:
            return element
        if hasattr(element, 'interface'):
            vdim = element.vdims[0]
            dtype = element.interface.dtype(element, vdim)
            if dtype_kind(dtype) not in 'iu':
                return element
            transform = dim(vdim, self._replace_value)
            return element.transform(**{vdim.name: transform})
        else:
            array = element.dimension_values(2, flat=False).T
            if dtype_kind(array) not in 'iu':
                return element
            array = array.astype('float64')
            return element.clone(self._replace_value(array))


RGB_HEX_REGEX = re.compile(r'^#(?:[0-9a-fA-F]{3}){1,2}$')

COLOR_ALIASES = {
    'b': (0, 0, 1),
    'c': (0, 0.75, 0.75),
    'g': (0, 0.5, 0),
    'k': (0, 0, 0),
    'm': (0.75, 0, 0.75),
    'r': (1, 0, 0),
    'w': (1, 1, 1),
    'y': (0.75, 0.75, 0),
    'transparent': (0, 0, 0, 0)
}


# linear_kryw_0_100_c71 (aka "fire"):
# A perceptually uniform equivalent of matplotlib's "hot" colormap, from
# https://peterkovesi.com/projects/colourmaps

fire_colors = linear_kryw_0_100_c71 = [\
[0,        0,           0         ],  [0.027065, 2.143e-05,   0         ],
[0.052054, 7.4728e-05,  0         ],  [0.071511, 0.00013914,  0         ],
[0.08742,  0.0002088,   0         ],  [0.10109,  0.00028141,  0         ],
[0.11337,  0.000356,    2.4266e-17],  [0.12439,  0.00043134,  3.3615e-17],
[0.13463,  0.00050796,  2.1604e-17],  [0.14411,  0.0005856,   0         ],
[0.15292,  0.00070304,  0         ],  [0.16073,  0.0013432,   0         ],
[0.16871,  0.0014516,   0         ],  [0.17657,  0.0012408,   0         ],
[0.18364,  0.0015336,   0         ],  [0.19052,  0.0017515,   0         ],
[0.19751,  0.0015146,   0         ],  [0.20401,  0.0015249,   0         ],
[0.20994,  0.0019639,   0         ],  [0.21605,  0.002031,    0         ],
[0.22215,  0.0017559,   0         ],  [0.22808,  0.001546,    1.8755e-05],
[0.23378,  0.0016315,   3.5012e-05],  [0.23955,  0.0017194,   3.3352e-05],
[0.24531,  0.0018097,   1.8559e-05],  [0.25113,  0.0019038,   1.9139e-05],
[0.25694,  0.0020015,   3.5308e-05],  [0.26278,  0.0021017,   3.2613e-05],
[0.26864,  0.0022048,   2.0338e-05],  [0.27451,  0.0023119,   2.2453e-05],
[0.28041,  0.0024227,   3.6003e-05],  [0.28633,  0.0025363,   2.9817e-05],
[0.29229,  0.0026532,   1.9559e-05],  [0.29824,  0.0027747,   2.7666e-05],
[0.30423,  0.0028999,   3.5752e-05],  [0.31026,  0.0030279,   2.3231e-05],
[0.31628,  0.0031599,   1.2902e-05],  [0.32232,  0.0032974,   3.2915e-05],
[0.32838,  0.0034379,   3.2803e-05],  [0.33447,  0.0035819,   2.0757e-05],
[0.34057,  0.003731,    2.3831e-05],  [0.34668,  0.0038848,   3.502e-05 ],
[0.35283,  0.0040418,   2.4468e-05],  [0.35897,  0.0042032,   1.1444e-05],
[0.36515,  0.0043708,   3.2793e-05],  [0.37134,  0.0045418,   3.012e-05 ],
[0.37756,  0.0047169,   1.4846e-05],  [0.38379,  0.0048986,   2.796e-05 ],
[0.39003,  0.0050848,   3.2782e-05],  [0.3963,   0.0052751,   1.9244e-05],
[0.40258,  0.0054715,   2.2667e-05],  [0.40888,  0.0056736,   3.3223e-05],
[0.41519,  0.0058798,   2.159e-05 ],  [0.42152,  0.0060922,   1.8214e-05],
[0.42788,  0.0063116,   3.2525e-05],  [0.43424,  0.0065353,   2.2247e-05],
[0.44062,  0.006765,    1.5852e-05],  [0.44702,  0.0070024,   3.1769e-05],
[0.45344,  0.0072442,   2.1245e-05],  [0.45987,  0.0074929,   1.5726e-05],
[0.46631,  0.0077499,   3.0976e-05],  [0.47277,  0.0080108,   1.8722e-05],
[0.47926,  0.0082789,   1.9285e-05],  [0.48574,  0.0085553,   3.0063e-05],
[0.49225,  0.0088392,   1.4313e-05],  [0.49878,  0.0091356,   2.3404e-05],
[0.50531,  0.0094374,   2.8099e-05],  [0.51187,  0.0097365,   6.4695e-06],
[0.51844,  0.010039,    2.5791e-05],  [0.52501,  0.010354,    2.4393e-05],
[0.53162,  0.010689,    1.6037e-05],  [0.53825,  0.011031,    2.7295e-05],
[0.54489,  0.011393,    1.5848e-05],  [0.55154,  0.011789,    2.3111e-05],
[0.55818,  0.012159,    2.5416e-05],  [0.56485,  0.012508,    1.5064e-05],
[0.57154,  0.012881,    2.541e-05 ],  [0.57823,  0.013283,    1.6166e-05],
[0.58494,  0.013701,    2.263e-05 ],  [0.59166,  0.014122,    2.3316e-05],
[0.59839,  0.014551,    1.9432e-05],  [0.60514,  0.014994,    2.4323e-05],
[0.6119,   0.01545,     1.3929e-05],  [0.61868,  0.01592,     2.1615e-05],
[0.62546,  0.016401,    1.5846e-05],  [0.63226,  0.016897,    2.0838e-05],
[0.63907,  0.017407,    1.9549e-05],  [0.64589,  0.017931,    2.0961e-05],
[0.65273,  0.018471,    2.0737e-05],  [0.65958,  0.019026,    2.0621e-05],
[0.66644,  0.019598,    2.0675e-05],  [0.67332,  0.020187,    2.0301e-05],
[0.68019,  0.020793,    2.0029e-05],  [0.68709,  0.021418,    2.0088e-05],
[0.69399,  0.022062,    1.9102e-05],  [0.70092,  0.022727,    1.9662e-05],
[0.70784,  0.023412,    1.7757e-05],  [0.71478,  0.024121,    1.8236e-05],
[0.72173,  0.024852,    1.4944e-05],  [0.7287,   0.025608,    2.0245e-06],
[0.73567,  0.02639,     1.5013e-07],  [0.74266,  0.027199,    0         ],
[0.74964,  0.028038,    0         ],  [0.75665,  0.028906,    0         ],
[0.76365,  0.029806,    0         ],  [0.77068,  0.030743,    0         ],
[0.77771,  0.031711,    0         ],  [0.78474,  0.032732,    0         ],
[0.79179,  0.033741,    0         ],  [0.79886,  0.034936,    0         ],
[0.80593,  0.036031,    0         ],  [0.81299,  0.03723,     0         ],
[0.82007,  0.038493,    0         ],  [0.82715,  0.039819,    0         ],
[0.83423,  0.041236,    0         ],  [0.84131,  0.042647,    0         ],
[0.84838,  0.044235,    0         ],  [0.85545,  0.045857,    0         ],
[0.86252,  0.047645,    0         ],  [0.86958,  0.049578,    0         ],
[0.87661,  0.051541,    0         ],  [0.88365,  0.053735,    0         ],
[0.89064,  0.056168,    0         ],  [0.89761,  0.058852,    0         ],
[0.90451,  0.061777,    0         ],  [0.91131,  0.065281,    0         ],
[0.91796,  0.069448,    0         ],  [0.92445,  0.074684,    0         ],
[0.93061,  0.08131,     0         ],  [0.93648,  0.088878,    0         ],
[0.94205,  0.097336,    0         ],  [0.9473,   0.10665,     0         ],
[0.9522,   0.1166,      0         ],  [0.95674,  0.12716,     0         ],
[0.96094,  0.13824,     0         ],  [0.96479,  0.14963,     0         ],
[0.96829,  0.16128,     0         ],  [0.97147,  0.17303,     0         ],
[0.97436,  0.18489,     0         ],  [0.97698,  0.19672,     0         ],
[0.97934,  0.20846,     0         ],  [0.98148,  0.22013,     0         ],
[0.9834,   0.23167,     0         ],  [0.98515,  0.24301,     0         ],
[0.98672,  0.25425,     0         ],  [0.98815,  0.26525,     0         ],
[0.98944,  0.27614,     0         ],  [0.99061,  0.28679,     0         ],
[0.99167,  0.29731,     0         ],  [0.99263,  0.30764,     0         ],
[0.9935,   0.31781,     0         ],  [0.99428,  0.3278,      0         ],
[0.995,    0.33764,     0         ],  [0.99564,  0.34735,     0         ],
[0.99623,  0.35689,     0         ],  [0.99675,  0.3663,      0         ],
[0.99722,  0.37556,     0         ],  [0.99765,  0.38471,     0         ],
[0.99803,  0.39374,     0         ],  [0.99836,  0.40265,     0         ],
[0.99866,  0.41145,     0         ],  [0.99892,  0.42015,     0         ],
[0.99915,  0.42874,     0         ],  [0.99935,  0.43724,     0         ],
[0.99952,  0.44563,     0         ],  [0.99966,  0.45395,     0         ],
[0.99977,  0.46217,     0         ],  [0.99986,  0.47032,     0         ],
[0.99993,  0.47838,     0         ],  [0.99997,  0.48638,     0         ],
[1,        0.4943,      0         ],  [1,        0.50214,     0         ],
[1,        0.50991,     1.2756e-05],  [1,        0.51761,     4.5388e-05],
[1,        0.52523,     9.6977e-05],  [1,        0.5328,      0.00016858],
[1,        0.54028,     0.0002582 ],  [1,        0.54771,     0.00036528],
[1,        0.55508,     0.00049276],  [1,        0.5624,      0.00063955],
[1,        0.56965,     0.00080443],  [1,        0.57687,     0.00098902],
[1,        0.58402,     0.0011943 ],  [1,        0.59113,     0.0014189 ],
[1,        0.59819,     0.0016626 ],  [1,        0.60521,     0.0019281 ],
[1,        0.61219,     0.0022145 ],  [1,        0.61914,     0.0025213 ],
[1,        0.62603,     0.0028496 ],  [1,        0.6329,      0.0032006 ],
[1,        0.63972,     0.0035741 ],  [1,        0.64651,     0.0039701 ],
[1,        0.65327,     0.0043898 ],  [1,        0.66,        0.0048341 ],
[1,        0.66669,     0.005303  ],  [1,        0.67336,     0.0057969 ],
[1,        0.67999,     0.006317  ],  [1,        0.68661,     0.0068648 ],
[1,        0.69319,     0.0074406 ],  [1,        0.69974,     0.0080433 ],
[1,        0.70628,     0.0086756 ],  [1,        0.71278,     0.0093486 ],
[1,        0.71927,     0.010023  ],  [1,        0.72573,     0.010724  ],
[1,        0.73217,     0.011565  ],  [1,        0.73859,     0.012339  ],
[1,        0.74499,     0.01316   ],  [1,        0.75137,     0.014042  ],
[1,        0.75772,     0.014955  ],  [1,        0.76406,     0.015913  ],
[1,        0.77039,     0.016915  ],  [1,        0.77669,     0.017964  ],
[1,        0.78298,     0.019062  ],  [1,        0.78925,     0.020212  ],
[1,        0.7955,      0.021417  ],  [1,        0.80174,     0.02268   ],
[1,        0.80797,     0.024005  ],  [1,        0.81418,     0.025396  ],
[1,        0.82038,     0.026858  ],  [1,        0.82656,     0.028394  ],
[1,        0.83273,     0.030013  ],  [1,        0.83889,     0.031717  ],
[1,        0.84503,     0.03348   ],  [1,        0.85116,     0.035488  ],
[1,        0.85728,     0.037452  ],  [1,        0.8634,      0.039592  ],
[1,        0.86949,     0.041898  ],  [1,        0.87557,     0.044392  ],
[1,        0.88165,     0.046958  ],  [1,        0.88771,     0.04977   ],
[1,        0.89376,     0.052828  ],  [1,        0.8998,      0.056209  ],
[1,        0.90584,     0.059919  ],  [1,        0.91185,     0.063925  ],
[1,        0.91783,     0.068579  ],  [1,        0.92384,     0.073948  ],
[1,        0.92981,     0.080899  ],  [1,        0.93576,     0.090648  ],
[1,        0.94166,     0.10377   ],  [1,        0.94752,     0.12051   ],
[1,        0.9533,      0.14149   ],  [1,        0.959,       0.1672    ],
[1,        0.96456,     0.19823   ],  [1,        0.96995,     0.23514   ],
[1,        0.9751,      0.2786    ],  [1,        0.97992,     0.32883   ],
[1,        0.98432,     0.38571   ],  [1,        0.9882,      0.44866   ],
[1,        0.9915,      0.51653   ],  [1,        0.99417,     0.58754   ],
[1,        0.99625,     0.65985   ],  [1,        0.99778,     0.73194   ],
[1,        0.99885,     0.80259   ],  [1,        0.99953,     0.87115   ],
[1,        0.99989,     0.93683   ],  [1,        1,           1         ]]

# Bokeh palette
fire = [f'#{int(r * 255):02x}{int(g * 255):02x}{int(b * 255):02x}'
        for r,g,b in fire_colors]

# Qualitative color maps, for use in colorizing categories
# Originally from Cynthia Brewer (http://colorbrewer2.org), via Bokeh
# Sets 1, 2, and 3 combined, minus indistinguishable colors
# Copied from datashader.colors
Sets1to3 = [
    '#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00', '#ffff33', '#a65628',
    '#f781bf', '#999999', '#66c2a5', '#fc8d62', '#8da0cb', '#a6d854', '#ffd92f',
    '#e5c494', '#ffffb3', '#fb8072', '#fdb462', '#fccde5', '#d9d9d9', '#ccebc5',
    '#ffed6f',
]

class categorical_legend(Operation):
    """Generates a Points element which contains information for generating
    a legend by inspecting the pipeline of a datashaded RGB element.

    """

    backend = param.String(doc="Backend to use for rendering the categorical legend.")

    cmap = param.Parameter(default=Sets1to3, allow_None=False, doc="""
        Colormap used to color the categories in the legend.
    """)

    def _process(self, element, key=None):
        import datashader as ds

        from ..operation.datashader import datashade, rasterize, shade
        rasterize_op = element.pipeline.find(rasterize, skip_nonlinked=False)
        if rasterize_op is None:
            return None
        hvds = element.dataset
        input_el = element.pipeline.operations[0](hvds)
        agg = rasterize_op._get_aggregator(input_el, rasterize_op.aggregator)
        if not isinstance(agg, (ds.count_cat, ds.by)):
            return
        column = agg.column
        if hasattr(hvds.data, 'dtypes') and hasattr(hvds.data.dtypes[column], 'categories'):
            try:
                cats = list(hvds.data.dtypes[column].categories)
            except TypeError:
                # Issue #5619, cudf.core.index.StringIndex is not iterable.
                cats = list(hvds.data.dtypes[column].categories.to_pandas())
            except AttributeError:
                cats = list(unique_iterator(hvds.data[column]))
            if cats == ['__UNKNOWN_CATEGORIES__']:
                cats = list(hvds.data[column].cat.as_known().categories)
        else:
            cats = list(hvds.dimension_values(column, expanded=False))

        if isinstance(rasterize_op, datashade):
            shade_op = rasterize_op
        else:
            shade_op = element.pipeline.find(shade, skip_nonlinked=False)
        if shade_op is None:
            colors = process_cmap(self.p.cmap, ncolors=len(cats), categorical=True)
        else:
            colors = shade_op.color_key or self.p.cmap
        color_data = [(0, 0, cat) for cat in cats]
        if isinstance(colors, list):
            ncolors = len(colors)
            cat_colors = {cat: colors[i % ncolors] for i, cat in enumerate(cats)}
        else:
            cat_colors = {cat: colors[cat] for cat in cats}
        cmap = {}
        for cat, color in cat_colors.items():
            if isinstance(color, tuple):
                color = rgb2hex([v/256 for v in color[:3]])
            cmap[cat] = color
        return Points(color_data, vdims=['category']).opts(
            cmap=cmap, color='category', show_legend=True,
            backend=self.p.backend, visible=False)


class flatten_stack(Operation):
    """Thin wrapper around datashader's shade operation to flatten
    ImageStacks into RGB elements.

    Used for the MPL and Plotly backends because these backends
    do not natively support ImageStacks, unlike Bokeh.

    """

    shade_params = param.Dict(default={}, doc="""
        Additional parameters passed to datashader's shade operation.""")

    def _process(self, element, key=None):
        try:
            from ..operation.datashader import shade
        except ImportError as exc:
            raise ImportError('Flattening ImageStacks requires datashader.') from exc
        return shade(element, **self.shade_params)
