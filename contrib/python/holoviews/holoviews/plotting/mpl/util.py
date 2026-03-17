import inspect
import re
import warnings

import matplotlib as mpl
import numpy as np
from matplotlib import (
    ticker,
    units as munits,
)
from matplotlib.colors import Normalize, cnames
from matplotlib.lines import Line2D
from matplotlib.markers import MarkerStyle
from matplotlib.patches import Path, PathPatch
from matplotlib.rcsetup import validate_fontsize, validate_fonttype, validate_hatch
from matplotlib.transforms import Affine2D, Bbox, TransformedBbox
from packaging.version import Version

try:  # starting Matplotlib 3.4.0
    from matplotlib._enums import (
        CapStyle as validate_capstyle,
        JoinStyle as validate_joinstyle,
    )
except ImportError:  # before Matplotlib 3.4.0
    from matplotlib.rcsetup import validate_capstyle, validate_joinstyle

try:
    from nc_time_axis import CalendarDateTime, NetCDFTimeConverter
    nc_axis_available = True
except ImportError:
    from matplotlib.dates import DateConverter
    NetCDFTimeConverter = DateConverter
    nc_axis_available = False

from ...core.util import arraylike_types, cftime_types, is_number
from ...element import RGB, Polygons, Raster
from ..util import COLOR_ALIASES, RGB_HEX_REGEX

MPL_VERSION = Version(mpl.__version__).release
MPL_GE_3_7_0 = MPL_VERSION >= (3, 7, 0)
MPL_GE_3_9_0 = MPL_VERSION >= (3, 9, 0)
MPL_GE_3_10_0 = MPL_VERSION >= (3, 10, 0)
MPL_GE_3_10_1 = MPL_VERSION >= (3, 10, 1)


def is_color(color):
    """Checks if supplied object is a valid color spec.

    """
    if not isinstance(color, str):
        return False
    elif RGB_HEX_REGEX.match(color):
        return True
    elif color in COLOR_ALIASES:
        return True
    elif color in cnames:
        return True
    return False

validators = {
    'alpha': lambda x: is_number(x) and (0 <= x <= 1),
    'capstyle': validate_capstyle,
    'color': is_color,
    'fontsize': validate_fontsize,
    'fonttype': validate_fonttype,
    'hatch': validate_hatch,
    'joinstyle': validate_joinstyle,
    'marker': lambda x: (
        x in Line2D.markers
        or isinstance(x, (MarkerStyle, Path))
        or (isinstance(x, str) and x.startswith('$') and x.endswith('$'))
    ),
    's': lambda x: is_number(x) and (x >= 0)
}

def get_old_rcparams():
    deprecated_rcparams = [
        'text.latex.unicode',
        'examples.directory',
        'savefig.frameon', # deprecated in MPL 3.1, to be removed in 3.3
        'verbose.level', # deprecated in MPL 3.1, to be removed in 3.3
        'verbose.fileo', # deprecated in MPL 3.1, to be removed in 3.3
        'datapath', # deprecated in MPL 3.2.1, to be removed in 3.3
        'text.latex.preview', # deprecated in MPL 3.3.1
        'animation.avconv_args', # deprecated in MPL 3.3.1
        'animation.avconv_path', # deprecated in MPL 3.3.1
        'animation.html_args', # deprecated in MPL 3.3.1
        'keymap.all_axes', # deprecated in MPL 3.3.1
        'savefig.jpeg_quality' # deprecated in MPL 3.3.1
    ]
    old_rcparams = {
        k: v for k, v in mpl.rcParams.items()
        if MPL_VERSION < (3, 0, 0) or k not in deprecated_rcparams
    }
    return old_rcparams


def get_validator(style):
    for k, v in validators.items():
        if style.endswith(k) and (len(style) != 1 or style == k):
            return v


def validate(style, value, vectorized=True):
    """Validates a style and associated value.

    Parameters
    ----------
    style : str
       The style to validate (e.g. 'color', 'size' or 'marker')
    value :
       The style value to validate
    vectorized : bool
       Whether validator should allow vectorized setting

    Returns
    -------
    valid : boolean or None
       If validation is supported returns boolean, otherwise None
    """
    validator = get_validator(style)
    if validator is None:
        return None
    if isinstance(value, (*arraylike_types, list)) and vectorized:
        return all(validator(v) for v in value)
    try:
        valid = validator(value)
        return False if valid == False else True
    except Exception:
        return False


def filter_styles(style, group, other_groups, blacklist=None):
    """Filters styles which are specific to a particular artist, e.g.
    for a GraphPlot this will filter options specific to the nodes and
    edges.

    Parameters
    ----------
    style : dict
        Dictionary of styles and values
    group : str
        Group within the styles to filter for
    other_groups : list
        Other groups to filter out
    blacklist : list, optional
        List of options to filter out

    Returns
    -------
    filtered : dict
        Filtered dictionary of styles
    """
    if blacklist is None:
        blacklist = []
    group = group+'_'
    filtered = {}
    for k, v in style.items():
        if (any(k.startswith(p) for p in other_groups)
            or k.startswith(group) or k in blacklist):
            continue
        filtered[k] = v
    for k, v in style.items():
        if not k.startswith(group) or k in blacklist:
            continue
        filtered[k[len(group):]] = v
    return filtered


def wrap_formatter(formatter):
    """Wraps formatting function or string in
    appropriate matplotlib formatter type.

    """
    if isinstance(formatter, ticker.Formatter):
        return formatter
    elif callable(formatter):
        args = [arg for arg in inspect.getfullargspec(formatter).args
                if arg != 'self']
        wrapped = formatter
        if len(args) == 1:
            def wrapped(val, pos=None):
                return formatter(val)
        return ticker.FuncFormatter(wrapped)
    elif isinstance(formatter, str):
        if re.findall(r"\{(\w+)\}", formatter):
            return ticker.StrMethodFormatter(formatter)
        else:
            return ticker.FormatStrFormatter(formatter)

def unpack_adjoints(ratios):
    new_ratios = {}
    offset = 0
    for k, (num, ratio_values) in sorted(ratios.items()):
        unpacked = [[] for _ in range(num)]
        for r in ratio_values:
            nr = len(r)
            for i in range(num):
                unpacked[i].append(r[i] if i < nr else np.nan)
        for i, r in enumerate(unpacked):
            new_ratios[k+i+offset] = r
        offset += num-1
    return new_ratios

def normalize_ratios(ratios):
    normalized = {}
    for i, v in enumerate(zip(*ratios.values(), strict=None)):
        arr = np.array(v)
        normalized[i] = arr/float(np.nanmax(arr))
    return normalized

def compute_ratios(ratios, normalized=True):
    unpacked = unpack_adjoints(ratios)
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', r'All-NaN (slice|axis) encountered')
        if normalized:
            unpacked = normalize_ratios(unpacked)
        sorted_ratios = sorted(unpacked.items())
        return np.nanmax(np.vstack([v for _, v in sorted_ratios]), axis=0)


def axis_overlap(ax1, ax2):
    """Tests whether two axes overlap vertically

    """
    b1, t1 = ax1.get_position().intervaly
    b2, t2 = ax2.get_position().intervaly
    return t1 > b2 and b1 < t2


def resolve_rows(rows):
    """Recursively iterate over lists of axes merging
    them by their vertical overlap leaving a list
    of rows.

    """
    merged_rows = []
    for row in rows:
        overlap = False
        for mrow in merged_rows:
            if any(axis_overlap(ax1, ax2) for ax1 in row
                   for ax2 in mrow):
                mrow += row
                overlap = True
                break
        if not overlap:
            merged_rows.append(row)
    if rows == merged_rows:
        return rows
    else:
        return resolve_rows(merged_rows)


def fix_aspect(fig, nrows, ncols, title=None, extra_artists=None,
               vspace=0.2, hspace=0.2):
    """Calculate heights and widths of axes and adjust
    the size of the figure to match the aspect.

    """
    if extra_artists is None:
        extra_artists = []
    fig.canvas.draw()
    w, _h = fig.get_size_inches()

    # Compute maximum height and width of each row and columns
    rows = resolve_rows([[ax] for ax in fig.axes])
    rs, cs = len(rows), max([len(r) for r in rows])
    heights = [[] for i in range(cs)]
    widths = [[] for i in range(rs)]
    for r, row in enumerate(rows):
        for c, ax in enumerate(row):
            bbox = ax.get_tightbbox(fig.canvas.get_renderer())
            heights[c].append(bbox.height)
            widths[r].append(bbox.width)
    height = (max([sum(c) for c in heights])) + nrows*vspace*fig.dpi
    width = (max([sum(r) for r in widths])) + ncols*hspace*fig.dpi

    # Compute aspect and set new size (in inches)
    aspect = height/width
    offset = 0
    if title and title.get_text():
        offset = title.get_window_extent().height/fig.dpi
    fig.set_size_inches(w, (w*aspect)+offset)

    # Redraw and adjust title position if defined
    fig.canvas.draw()
    if title and title.get_text():
        extra_artists = [a for a in extra_artists
                         if a is not title]
        bbox = get_tight_bbox(fig, extra_artists)
        top = bbox.intervaly[1]
        if title and title.get_text():
            title.set_y(top/(w*aspect))


def get_tight_bbox(fig, bbox_extra_artists=None, pad=None):
    """Compute a tight bounding box around all the artists in the figure.

    """
    if bbox_extra_artists is None:
        bbox_extra_artists = []
    renderer = fig.canvas.get_renderer()
    bbox_inches = fig.get_tightbbox(renderer)
    bbox_artists = bbox_extra_artists[:]
    bbox_artists += fig.get_default_bbox_extra_artists()
    bbox_filtered = []
    for a in bbox_artists:
        bbox = a.get_window_extent(renderer)
        if isinstance(bbox, tuple):
            continue
        if a.get_clip_on():
            clip_box = a.get_clip_box()
            if clip_box is not None:
                bbox = Bbox.intersection(bbox, clip_box)
            clip_path = a.get_clip_path()
            if clip_path is not None and bbox is not None:
                clip_path = clip_path.get_fully_transformed_path()
                bbox = Bbox.intersection(bbox,
                                         clip_path.get_extents())
        if (
            bbox is not None and
            (bbox.width != 0 or bbox.height != 0) and
            np.isfinite(bbox).all()
        ):
            bbox_filtered.append(bbox)
    if bbox_filtered:
        _bbox = Bbox.union(bbox_filtered)
        trans = Affine2D().scale(1.0 / fig.dpi)
        bbox_extra = TransformedBbox(_bbox, trans)
        bbox_inches = Bbox.union([bbox_inches, bbox_extra])
    return bbox_inches.padded(pad) if pad else bbox_inches


def get_raster_array(image):
    """Return the array data from any Raster or Image type

    """
    if isinstance(image, RGB):
        rgb = image.rgb
        data = np.dstack([np.flipud(rgb.dimension_values(d, flat=False))
                          for d in rgb.vdims])
    else:
        data = image.dimension_values(2, flat=False)
        if type(image) is Raster:
            data = data.T
        else:
            data = np.flipud(data)
    return data


def ring_coding(array):
    """Produces matplotlib Path codes for exterior and interior rings
    of a polygon geometry.

    """
    # The codes will be all "LINETO" commands, except for "MOVETO"s at the
    # beginning of each subpath
    n = len(array)
    codes = np.ones(n, dtype=Path.code_type) * Path.LINETO
    codes[0] = Path.MOVETO
    codes[-1] = Path.CLOSEPOLY
    return codes


def polygons_to_path_patches(element):
    """Converts Polygons into list of lists of matplotlib.patches.PathPatch
    objects including any specified holes. Each list represents one
    (multi-)polygon.

    """
    paths = element.split(datatype='array', dimensions=element.kdims)
    has_holes = isinstance(element, Polygons) and element.interface.has_holes(element)
    holes = element.interface.holes(element) if has_holes else None
    mpl_paths = []
    for i, path in enumerate(paths):
        splits = np.where(np.isnan(path[:, :2].astype('float')).sum(axis=1))[0]
        arrays = np.split(path, splits+1) if len(splits) else [path]
        subpath = []
        for j, array in enumerate(arrays):
            if j != (len(arrays)-1):
                array = array[:-1]
            if (array[0] != array[-1]).any():
                array = np.append(array, array[:1], axis=0)
            interiors = []
            for interior in (holes[i][j] if has_holes else []):
                if (interior[0] != interior[-1]).any():
                    interior = np.append(interior, interior[:1], axis=0)
                interiors.append(interior)
            vertices = np.concatenate([array, *interiors])
            codes = np.concatenate([ring_coding(array)]+
                                   [ring_coding(h) for h in interiors])
            subpath.append(PathPatch(Path(vertices, codes)))
        mpl_paths.append(subpath)
    return mpl_paths


class CFTimeConverter(NetCDFTimeConverter):
    """Defines conversions for cftime types by extending nc_time_axis.

    """

    @classmethod
    def convert(cls, value, unit, axis):
        if not nc_axis_available:
            raise ValueError('In order to display cftime types with '
                             'matplotlib install the nc_time_axis '
                             'library using pip or from conda-forge '
                             'using:\n\tconda install -c conda-forge '
                             'nc_time_axis')
        if isinstance(value, cftime_types):
            value = CalendarDateTime(value.datetime, value.calendar)
        elif isinstance(value, np.ndarray):
            value = np.array([CalendarDateTime(v.datetime, v.calendar) for v in value])
        return super().convert(value, unit, axis)


class EqHistNormalize(Normalize):

    def __init__(self, vmin=None, vmax=None, clip=False, rescale_discrete_levels=True, nbins=256**2, ncolors=256):
        super().__init__(vmin, vmax, clip)
        self._nbins = nbins
        self._bin_edges = None
        self._ncolors = ncolors
        self._color_bins = np.linspace(0, 1, ncolors+1)
        self._rescale = rescale_discrete_levels

    def binning(self, data, n=256):
        low = data.min() if self.vmin is None else self.vmin
        high = data.max() if self.vmax is None else self.vmax
        nbins = self._nbins
        eq_bin_edges = np.linspace(low, high, nbins+1)
        full_hist, _ = np.histogram(data, eq_bin_edges)

        # Remove zeros, leaving extra element at beginning for rescale_discrete_levels
        nonzero = np.nonzero(full_hist)[0]
        nhist = len(nonzero)
        if nhist > 1:
            hist = np.zeros(nhist+1)
            hist[1:] = full_hist[nonzero]
            eq_bin_centers = np.concatenate([[0.], (eq_bin_edges[nonzero] + eq_bin_edges[nonzero+1]) / 2.])
            eq_bin_centers[0] = 2*eq_bin_centers[1] - eq_bin_centers[-1]
        else:
            hist = full_hist
            eq_bin_centers = np.convolve(eq_bin_edges, [0.5, 0.5], mode='valid')

        # CDF scaled from 0 to 1 except for first value
        cdf = np.cumsum(hist)
        lo = cdf[1]
        diff = cdf[-1] - lo
        with np.errstate(divide='ignore', invalid='ignore'):
            cdf = (cdf - lo) / diff
        cdf[0] = -1.0

        lower_span = 0
        if self._rescale:
            discrete_levels = nhist
            m = -0.5/98.0
            c = 1.5 - 2*m
            multiple = m*discrete_levels + c
            if (multiple > 1):
                lower_span = 1 - multiple

        cdf_bins = np.linspace(lower_span, 1, n+1)
        binning = np.interp(cdf_bins, cdf, eq_bin_centers)
        if not self._rescale:
            binning[0] = low
        binning[-1] = high
        return binning

    def __call__(self, data, clip=None):
        return self.process_value(data)[0]

    def process_value(self, data):
        if isinstance(data, np.ndarray):
            self._bin_edges = self.binning(data, self._ncolors)
        isscalar = np.isscalar(data)
        data = np.array([data]) if isscalar else data
        interped = np.interp(data, self._bin_edges, self._color_bins)
        return np.ma.array(interped), isscalar

    def inverse(self, value):
        if self._bin_edges is None:
            raise ValueError("Not invertible until eq_hist has been computed")
        return np.interp([value], self._color_bins, self._bin_edges)[0]


for cft in cftime_types:
    munits.registry[cft] = CFTimeConverter()
