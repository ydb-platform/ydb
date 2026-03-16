"""Window utilities and related functions.

A window is an instance of Window

    Window(column_offset, row_offset, width, height)

or a 2D N-D array indexer in the form of a tuple.

    ((row_start, row_stop), (col_start, col_stop))

The latter can be evaluated within the context of a given height and
width and a boolean flag specifying whether the evaluation is boundless
or not. If boundless=True, negative index values do not mean index from
the end of the array dimension as they do in the boundless=False case.

The newer float precision read-write window capabilities of Rasterio
require instances of Window to be used.
"""

import collections
from collections.abc import Iterable
import functools
import math
import warnings

from affine import Affine
import attr
import numpy as np

from rasterio.errors import WindowError, RasterioDeprecationWarning
from rasterio.transform import rowcol, guard_transform


class WindowMethodsMixin:
    """Mixin providing methods for window-related calculations.
    These methods are wrappers for the functionality in
    `rasterio.windows` module.

    A subclass with this mixin MUST provide the following
    properties: `transform`, `height` and `width`.

    """

    def window(self, left, bottom, right, top, precision=None):
        """Get the window corresponding to the bounding coordinates.

        The resulting window is not cropped to the row and column
        limits of the dataset.

        Parameters
        ----------
        left: float
            Left (west) bounding coordinate
        bottom: float
            Bottom (south) bounding coordinate
        right: float
            Right (east) bounding coordinate
        top: float
            Top (north) bounding coordinate
        precision: int, optional
            This parameter is unused, deprecated in rasterio 1.3.0, and
            will be removed in version 2.0.0.

        Returns
        -------
        window: Window

        """
        if precision is not None:
            warnings.warn(
                "The precision parameter is unused, deprecated, and will be removed in 2.0.0.",
                RasterioDeprecationWarning,
            )

        return from_bounds(
            left,
            bottom,
            right,
            top,
            transform=guard_transform(self.transform),
        )

    def window_transform(self, window):
        """Get the affine transform for a dataset window.

        Parameters
        ----------
        window: rasterio.windows.Window
            Dataset window

        Returns
        -------
        transform: Affine
            The affine transform matrix for the given window

        """
        gtransform = guard_transform(self.transform)
        return transform(window, gtransform)

    def window_bounds(self, window):
        """Get the bounds of a window

        Parameters
        ----------
        window: rasterio.windows.Window
            Dataset window

        Returns
        -------
        bounds : tuple
            x_min, y_min, x_max, y_max for the given window

        """
        transform = guard_transform(self.transform)
        return bounds(window, transform)


def iter_args(function):
    """Decorator to allow function to take either ``*args`` or
    a single iterable which gets expanded to ``*args``.
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        if len(args) == 1 and isinstance(args[0], Iterable):
            return function(*args[0])
        else:
            return function(*args)
    return wrapper


def toranges(window):
    """Normalize Windows to range tuples"""
    if isinstance(window, Window):
        return window.toranges()
    else:
        return window


def get_data_window(arr, nodata=None):
    """Window covering the input array's valid data pixels.

    Parameters
    ----------
    arr: numpy ndarray, <= 3 dimensions
    nodata: number
        If None, will either return a full window if arr is not a masked
        array, or will use the mask to determine non-nodata pixels.
        If provided, it must be a number within the valid range of the
        dtype of the input array.

    Returns
    -------
    Window
    """
    if not 0 < arr.ndim <=3 :
        raise WindowError(
            "get_data_window input array must have 1, 2, or 3 dimensions")

    # If nodata is defined, construct mask from that value
    # Otherwise retrieve mask from array (if it is masked)
    # Finally try returning a full window (nodata=None and nothing in arr is masked)
    if nodata is not None:
        if np.isnan(nodata):
            arr_mask = ~np.isnan(arr)
        else:
            arr_mask = arr != nodata
    elif np.ma.is_masked(arr):
        arr_mask = ~np.ma.getmask(arr)
    else:
        if arr.ndim == 1:
            full_window = ((0, arr.size), (0, 0))
        else:
            full_window = ((0, arr.shape[-2]), (0, arr.shape[-1]))
        return Window.from_slices(*full_window)

    if arr.ndim == 3:
        arr_mask = np.any(arr_mask, axis=0)

    # We only have 1 or 2 dimension cases to process
    v = []
    for nz in arr_mask.nonzero():
        if nz.size:
            v.append((nz.min(), nz.max() + 1))
        else:
            v.append((0, 0))

    if arr_mask.ndim == 1:
        v.append((0, 0))

    return Window.from_slices(*v)


def _compute_union(w1, w2):
    """Compute the union of two windows"""
    col_off = min(w1.col_off, w2.col_off)
    row_off = min(w1.row_off, w2.row_off)
    width = max(w1.col_off+w1.width, w2.col_off+w2.width) - col_off
    height = max(w1.row_off+w1.height, w2.row_off+w2.height) - row_off
    return col_off, row_off, width, height


def _union(w1, w2):
    coeffs = _compute_union(w1, w2)
    return Window(*coeffs)


@iter_args
def union(*windows):
    """
    Union windows and return the outermost extent they cover.

    Parameters
    ----------
    windows: sequence
        One or more Windows.

    Returns
    -------
    Window
    """
    return functools.reduce(_union, windows)


@iter_args
def intersection(*windows):
    """Innermost extent of window intersections.

    Will raise WindowError if windows do not intersect.

    Parameters
    ----------
    windows: sequence
        One or more Windows.

    Returns
    -------
    Window
    """
    return functools.reduce(_intersection, windows)


def _compute_intersection(w1, w2):
    """ Compute intersection of window 1 and window 2"""
    col_off = max(w1.col_off, w2.col_off)
    row_off = max(w1.row_off, w2.row_off)
    width = min(w1.col_off+w1.width, w2.col_off+w2.width) - col_off
    height = min(w1.row_off+w1.height, w2.row_off+w2.height) - row_off
    return col_off, row_off, width, height


def _intersection(w1, w2):
    """ Compute intersection of window 1 and window 2"""
    coeffs = _compute_intersection(w1, w2)
    if coeffs[2] > 0 and coeffs[3] > 0:
        return Window(*coeffs)
    else:
        raise WindowError(f"Intersection is empty {w1} {w2}")


@iter_args
def intersect(*windows):
    """Test if all given windows intersect.

    Parameters
    ----------
    windows: sequence
        One or more Windows.

    Returns
    -------
    bool
        True if all windows intersect.
    """
    try:
        intersection(*windows)
        return True
    except WindowError:
        return False


def from_bounds(
    left, bottom, right, top, transform=None, height=None, width=None, precision=None
):
    """Get the window corresponding to the bounding coordinates.

    Parameters
    ----------
    left: float, required
        Left (west) bounding coordinates
    bottom: float, required
        Bottom (south) bounding coordinates
    right: float, required
        Right (east) bounding coordinates
    top: float, required
        Top (north) bounding coordinates
    transform: Affine, required
        Affine transform matrix.
    precision, height, width: int, optional
        These parameters are unused, deprecated in rasterio 1.3.0, and
        will be removed in version 2.0.0.

    Returns
    -------
    Window
        A new Window.

    Raises
    ------
    WindowError
        If a window can't be calculated.

    """
    if height is not None or width is not None or precision is not None:
        warnings.warn(
            "The height, width, and precision parameters are unused, deprecated, and will be removed in 2.0.0.",
            RasterioDeprecationWarning,
        )

    if not isinstance(transform, Affine):  # TODO: RPCs?
        raise WindowError("A transform object is required to calculate the window")
    if (right - left) / transform.a < 0:
        raise WindowError("Bounds and transform are inconsistent")
    if (bottom - top) / transform.e < 0:
        raise WindowError("Bounds and transform are inconsistent")

    rows, cols = rowcol(
        transform,
        [left, right, right, left],
        [top, top, bottom, bottom],
        op=float,
    )
    row_start, row_stop = min(rows), max(rows)
    col_start, col_stop = min(cols), max(cols)

    return Window(
        col_off=col_start,
        row_off=row_start,
        width=max(col_stop - col_start, 0.0),
        height=max(row_stop - row_start, 0.0),
    )


def transform(window, transform):
    """Construct an affine transform matrix relative to a window.

    Parameters
    ----------
    window: Window
        The input window.
    transform: Affine
        an affine transform matrix.

    Returns
    -------
    Affine
        The affine transform matrix for the given window

    """
    window = evaluate(window, height=0, width=0)
    x, y = transform * (window.col_off or 0.0, window.row_off or 0.0)
    return Affine.translation(
        x - transform.c, y - transform.f) * transform


def bounds(window, transform, height=0, width=0):
    """Get the spatial bounds of a window.

    Parameters
    ----------
    window: Window
        The input window.
    transform: Affine
        an affine transform matrix.

    Returns
    -------
    left, bottom, right, top: float
        A tuple of spatial coordinate bounding values.
    """
    window = evaluate(window, height=height, width=width)

    row_min = window.row_off
    row_max = row_min + window.height
    col_min = window.col_off
    col_max = col_min + window.width

    left, bottom = transform * (col_min, row_max)
    right, top = transform * (col_max, row_min)
    return left, bottom, right, top


def crop(window, height, width):
    """Crops a window to given height and width.

    Parameters
    ----------
    window : Window.
        The input window.
    height, width : int
        The number of rows and cols in the cropped window.

    Returns
    -------
    Window
        A new Window object.
    """
    window = evaluate(window, height=height, width=width)

    row_start = min(max(window.row_off, 0), height)
    col_start = min(max(window.col_off, 0), width)
    row_stop = max(0, min(window.row_off + window.height, height))
    col_stop = max(0, min(window.col_off + window.width, width))

    return Window(col_start, row_start, col_stop - col_start,
                  row_stop - row_start)


def evaluate(window, height, width, boundless=False):
    """Evaluates a window tuple that may contain relative index values.

    The height and width of the array the window targets is the context
    for evaluation.

    Parameters
    ----------
    window: Window or tuple of (rows, cols).
        The input window.
    height, width: int
        The number of rows or columns in the array that the window
        targets.

    Returns
    -------
    Window
        A new Window object with absolute index values.
    """
    if isinstance(window, Window):
        return window
    else:
        rows, cols = window
        return Window.from_slices(rows=rows, cols=cols, height=height,
                                  width=width, boundless=boundless)


def shape(window, height=-1, width=-1):
    """The shape of a window.

    height and width arguments are optional if there are no negative
    values in the window.

    Parameters
    ----------
    window: Window
        The input window.
    height, width : int, optional
        The number of rows or columns in the array that the window
        targets.

    Returns
    -------
    num_rows, num_cols
        The number of rows and columns of the window.
    """
    evaluated = evaluate(window, height, width)
    return evaluated.height, evaluated.width


def window_index(window, height=0, width=0):
    """Construct a pair of slice objects for ndarray indexing

    Starting indexes are rounded down, Stopping indexes are rounded up.

    Parameters
    ----------
    window: Window
        The input window.

    Returns
    -------
    row_slice, col_slice: slice
        A pair of slices in row, column order

    """
    window = evaluate(window, height=height, width=width)
    return window.toslices()


def round_window_to_full_blocks(window, block_shapes, height=0, width=0):
    """Round window to include full expanse of intersecting tiles.

    Parameters
    ----------
    window: Window
        The input window.

    block_shapes : tuple of block shapes
        The input raster's block shape. All bands must have the same
        block/stripe structure

    Returns
    -------
    Window
    """
    if len(set(block_shapes)) != 1:  # pragma: no cover
        raise WindowError(
            "All bands must have the same block/stripe structure")

    window = evaluate(window, height=height, width=width)

    height_shape = block_shapes[0][0]
    width_shape = block_shapes[0][1]

    (row_start, row_stop), (col_start, col_stop) = window.toranges()

    row_min = int(row_start // height_shape) * height_shape
    row_max = int(row_stop // height_shape) * height_shape + \
        (height_shape if row_stop % height_shape != 0 else 0)

    col_min = int(col_start // width_shape) * width_shape
    col_max = int(col_stop // width_shape) * width_shape + \
        (width_shape if col_stop % width_shape != 0 else 0)

    return Window(col_min, row_min, col_max - col_min, row_max - row_min)


def validate_length_value(instance, attribute, value):
    if value and value < 0:
        raise ValueError("Number of columns or rows must be non-negative")


@attr.s(slots=True, frozen=True)
class Window:
    """Windows are rectangular subsets of rasters.

    This class abstracts the 2-tuples mentioned in the module docstring
    and adds methods and new constructors.

    Attributes
    ----------
    col_off, row_off: float
        The offset for the window.
    width, height: float
        Lengths of the window.

    Notes
    -----
    Previously the lengths were called 'num_cols' and 'num_rows' but
    this is a bit confusing in the new float precision world and the
    attributes have been changed. The originals are deprecated.
    """
    col_off = attr.ib()
    row_off = attr.ib()
    width = attr.ib(validator=validate_length_value)
    height = attr.ib(validator=validate_length_value)

    def __repr__(self):
        """Return a nicely formatted representation string"""
        return (
            "Window(col_off={self.col_off}, row_off={self.row_off}, "
            "width={self.width}, height={self.height})").format(
                self=self)

    def flatten(self):
        """A flattened form of the window.

        Returns
        -------
        col_off, row_off, width, height: float
            Window offsets and lengths.
        """
        return (self.col_off, self.row_off, self.width, self.height)

    def todict(self):
        """A mapping of attribute names and values.

        Returns
        -------
        dict
        """
        return collections.OrderedDict(
            col_off=self.col_off, row_off=self.row_off, width=self.width,
            height=self.height)

    def toranges(self):
        """Makes an equivalent pair of range tuples"""
        return (
            (self.row_off, self.row_off + self.height),
            (self.col_off, self.col_off + self.width))

    def toslices(self):
        """Slice objects for use as an ndarray indexer.

        Returns
        -------
        row_slice, col_slice: slice
            A pair of slices in row, column order

        """
        (r0, r1), (c0, c1) = self.toranges()

        if r0 < 0:
            r0 = 0
        if r1 < 0:
            r1 = 0
        if c0 < 0:
            c0 = 0
        if c1 < 0:
            c1 = 0

        return (
            slice(int(math.floor(r0)), int(math.ceil(r1))),
            slice(int(math.floor(c0)), int(math.ceil(c1))),
        )

    @classmethod
    def from_slices(cls, rows, cols, height=-1, width=-1, boundless=False):
        """Construct a Window from row and column slices or tuples / lists of
        start and stop indexes. Converts the rows and cols to offsets, height,
        and width.

        In general, indexes are defined relative to the upper left corner of
        the dataset: rows=(0, 10), cols=(0, 4) defines a window that is 4
        columns wide and 10 rows high starting from the upper left.

        Start indexes may be `None` and will default to 0.
        Stop indexes may be `None` and will default to width or height, which
        must be provided in this case.

        Negative start indexes are evaluated relative to the lower right of the
        dataset: rows=(-2, None), cols=(-2, None) defines a window that is 2
        rows high and 2 columns wide starting from the bottom right.

        Parameters
        ----------
        rows, cols: slice, tuple, or list
            Slices or 2 element tuples/lists containing start, stop indexes.
        height, width: float
            A shape to resolve relative values against. Only used when a start
            or stop index is negative or a stop index is None.
        boundless: bool, optional
            Whether the inputs are bounded (default) or not.

        Returns
        -------
        Window
        """

        # Normalize to slices
        if isinstance(rows, (tuple, list)):
            if len(rows) != 2:
                raise WindowError("rows must have a start and stop index")
            rows = slice(*rows)

        elif not isinstance(rows, slice):
            raise WindowError("rows must be a slice, tuple, or list")

        if isinstance(cols, (tuple, list)):
            if len(cols) != 2:
                raise WindowError("cols must have a start and stop index")
            cols = slice(*cols)

        elif not isinstance(cols, slice):
            raise WindowError("cols must be a slice, tuple, or list")

        # Height and width are required if stop indices are implicit
        if rows.stop is None and height < 0:
            raise WindowError("height is required if row stop index is None")

        if cols.stop is None and width < 0:
            raise WindowError("width is required if col stop index is None")

        # Convert implicit indices to offsets, height, and width
        row_off = 0.0 if rows.start is None else rows.start
        row_stop = height if rows.stop is None else rows.stop

        col_off = 0.0 if cols.start is None else cols.start
        col_stop = width if cols.stop is None else cols.stop

        if not boundless:
            if (row_off < 0 or row_stop < 0):
                if height < 0:
                    raise WindowError("height is required when providing "
                                      "negative indexes")

                if row_off < 0:
                    row_off += height

                if row_stop < 0:
                    row_stop += height

            if (col_off < 0 or col_stop < 0):
                if width < 0:
                    raise WindowError("width is required when providing "
                                      "negative indexes")

                if col_off < 0:
                    col_off += width

                if col_stop < 0:
                    col_stop += width

        num_cols = max(col_stop - col_off, 0.0)
        num_rows = max(row_stop - row_off, 0.0)

        return cls(col_off=col_off, row_off=row_off, width=num_cols,
                   height=num_rows)

    def round_lengths(self, **kwds):
        """Return a copy with width and height rounded.

        Lengths are rounded to the nearest whole number. The offsets are
        not changed.

        Parameters
        ----------
        kwds : dict
            Collects keyword arguments that are no longer used.

        Returns
        -------
        Window

        """
        width = math.floor(self.width + 0.5)
        height = math.floor(self.height + 0.5)
        return Window(self.col_off, self.row_off, width, height)

    def round_shape(self, **kwds):
        warnings.warn(
            "round_shape is deprecated and will be removed in Rasterio 2.0.0.",
            RasterioDeprecationWarning,
        )
        return self.round_lengths(**kwds)

    def round_offsets(self, **kwds):
        """Return a copy with column and row offsets rounded.

        Offsets are rounded to the preceding whole number. The lengths
        are not changed.

        Parameters
        ----------
        kwds : dict
            Collects keyword arguments that are no longer used.

        Returns
        -------
        Window

        """
        row_off = math.floor(self.row_off + 0.1)
        col_off = math.floor(self.col_off + 0.1)
        return Window(col_off, row_off, self.width, self.height)

    def round(self, ndigits=None):
        """Round a window's offsets and lengths

        Rounding to a very small fraction of a pixel can help treat
        floating point issues arising from computation of windows.
        """
        return Window(
            round(self.col_off, ndigits=ndigits),
            round(self.row_off, ndigits=ndigits),
            round(self.width, ndigits=ndigits),
            round(self.height, ndigits=ndigits),
        )

    def crop(self, height, width):
        """Return a copy cropped to height and width"""
        return crop(self, height, width)

    def intersection(self, other):
        """Return the intersection of this window and another

        Parameters
        ----------

        other: Window
            Another window

        Returns
        -------
        Window
        """
        return intersection([self, other])


def subdivide(window, height, width):
    """Divide a window into smaller windows.

    Windows have no overlap and will be at most the desired
    height and width. Smaller windows will be generated where
    the height and width do not evenly divide the window dimensions.

    Parameters
    ----------
    window : Window
        Source window to subdivide.
    height : int
        Subwindow height.
    width : int
        Subwindow width.

    Returns
    -------
    list of Windows
    """
    subwindows = []

    irow = window.row_off + window.height
    icol = window.col_off + window.width

    row_off = window.row_off
    col_off = window.col_off
    while row_off < irow:
        if row_off + height > irow:
            _height = irow - row_off
        else:
            _height = height

        while col_off < icol:
            if col_off + width > icol:
                _width = icol - col_off
            else:
                _width = width

            subwindows.append(Window(col_off, row_off, _width, _height))
            col_off += width

        row_off += height
        col_off = window.col_off
    return subwindows
