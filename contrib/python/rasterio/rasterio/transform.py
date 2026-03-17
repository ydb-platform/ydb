"""Geospatial transforms"""

from contextlib import ExitStack
from functools import partial
import numpy as np
import warnings
from numbers import Number

from affine import Affine

from rasterio.env import env_ctx_if_needed
from rasterio._transform import (
    _transform_from_gcps,
    RPCTransformerBase,
    GCPTransformerBase,
)
from rasterio.enums import TransformDirection, TransformMethod
from rasterio.control import GroundControlPoint
from rasterio.rpc import RPC
from rasterio.errors import TransformError, RasterioDeprecationWarning

IDENTITY = Affine.identity()
GDAL_IDENTITY = IDENTITY.to_gdal()


class TransformMethodsMixin:
    """Mixin providing methods for calculations related
    to transforming between rows and columns of the raster
    array and the coordinates.

    These methods are wrappers for the functionality in
    `rasterio.transform` module.

    A subclass with this mixin MUST provide a `transform`
    property.

    """

    def xy(
        self,
        row,
        col,
        z=None,
        offset="center",
        transform_method=TransformMethod.affine,
        **rpc_options
    ):
        """Get the coordinates x, y of a pixel at row, col.

        The pixel's center is returned by default, but a corner can be returned
        by setting `offset` to one of `ul, ur, ll, lr`.

        Parameters
        ----------
        row : int
            Pixel row.
        col : int
            Pixel column.
        z : float, optional
            Height associated with coordinates. Primarily used for RPC based
            coordinate transformations. Ignored for affine based
            transformations. Default: 0.
        offset : str, optional
            Determines if the returned coordinates are for the center of the
            pixel or for a corner.
        transform_method: TransformMethod, optional
            The coordinate transformation method. Default: `TransformMethod.affine`.
        rpc_options: dict, optional
            Additional arguments passed to GDALCreateRPCTransformer

        Returns
        -------
        tuple
            x, y

        """
        transform = getattr(self, transform_method.value)
        if transform_method is TransformMethod.gcps:
            transform = transform[0]
        if not transform:
            raise AttributeError(f"Dataset has no {transform_method}")
        return xy(transform, row, col, zs=z, offset=offset, **rpc_options)

    def index(
        self,
        x,
        y,
        z=None,
        op=None,
        precision=None,
        transform_method=TransformMethod.affine,
        **rpc_options
    ):
        """Get the (row, col) index of the pixel containing (x, y).

        Parameters
        ----------
        x : float
            x value in coordinate reference system
        y : float
            y value in coordinate reference system
        z : float, optional
            Height associated with coordinates. Primarily used for RPC
            based coordinate transformations. Ignored for affine based
            transformations. Default: 0.
        op : function, optional (default: numpy.floor)
            Function to convert fractional pixels to whole numbers
            (floor, ceiling, round)
        transform_method: TransformMethod, optional
            The coordinate transformation method. Default:
            `TransformMethod.affine`.
        rpc_options: dict, optional
            Additional arguments passed to GDALCreateRPCTransformer
        precision : int, optional
            This parameter is unused, deprecated in rasterio 1.3.0, and
            will be removed in version 2.0.0.

        Returns
        -------
        tuple: int, int
            (row index, col index)

        """
        if precision is not None:
            warnings.warn(
                "The precision parameter is unused, deprecated, and will be removed in 2.0.0.",
                RasterioDeprecationWarning,
            )

        transform = getattr(self, transform_method.value)
        if transform_method is TransformMethod.gcps:
            transform = transform[0]
        if not transform:
            raise AttributeError(f"Dataset has no {transform_method}")
        return tuple(int(val) for val in rowcol(transform, x, y, zs=z, op=op, **rpc_options))


def get_transformer(transform, **rpc_options):
    """Return the appropriate transformer class"""
    if transform is None:
        raise ValueError("Invalid transform")
    if isinstance(transform, Affine):
        transformer_cls = partial(AffineTransformer, transform)
    elif isinstance(transform, RPC):
        transformer_cls = partial(RPCTransformer, transform, **rpc_options)
    else:
        transformer_cls = partial(GCPTransformer, transform)
    return transformer_cls


def tastes_like_gdal(seq):
    """Return True if `seq` matches the GDAL geotransform pattern."""
    return tuple(seq) == GDAL_IDENTITY or (
        seq[2] == seq[4] == 0.0 and seq[1] > 0 and seq[5] < 0)


def guard_transform(transform):
    """Return an Affine transformation instance."""
    if not isinstance(transform, Affine):
        if tastes_like_gdal(transform):
            raise TypeError(
                "GDAL-style transforms have been deprecated.  This "
                "exception will be raised for a period of time to highlight "
                "potentially confusing errors, but will eventually be removed.")
        else:
            transform = Affine(*transform)
    return transform


def from_origin(west, north, xsize, ysize):
    """Return an Affine transformation given upper left and pixel sizes.

    Return an Affine transformation for a georeferenced raster given
    the coordinates of its upper left corner `west`, `north` and pixel
    sizes `xsize`, `ysize`.

    """
    return Affine.translation(west, north) * Affine.scale(xsize, -ysize)


def from_bounds(west, south, east, north, width, height):
    """Return an Affine transformation given bounds, width and height.

    Return an Affine transformation for a georeferenced raster given
    its bounds `west`, `south`, `east`, `north` and its `width` and
    `height` in number of pixels.

    """
    return Affine.translation(west, north) * Affine.scale(
        (east - west) / width, (south - north) / height)


def array_bounds(height, width, transform):
    """Return the bounds of an array given height, width, and a transform.

    Return the `west, south, east, north` bounds of an array given
    its height, width, and an affine transform.

    """
    a, b, c, d, e, f, _, _, _ = transform
    if b == d == 0:
        west, south, east, north = c, f + e * height, c + a * width, f
    else:
        c0x, c0y = c, f
        c1x, c1y = transform * (0, height)
        c2x, c2y = transform * (width, height)
        c3x, c3y = transform * (width, 0)
        xs = (c0x, c1x, c2x, c3x)
        ys = (c0y, c1y, c2y, c3y)
        west, south, east, north = min(xs), min(ys), max(xs), max(ys)

    return west, south, east, north


def xy(transform, rows, cols, zs=None, offset='center', **rpc_options):
    """Get the x and y coordinates of pixels at `rows` and `cols`.

    The pixel's center is returned by default, but a corner can be returned
    by setting `offset` to one of `ul, ur, ll, lr`.

    Supports affine, Ground Control Point (GCP), or Rational Polynomial
    Coefficients (RPC) based coordinate transformations.

    Parameters
    ----------
    transform : Affine or sequence of GroundControlPoint or RPC
        Transform suitable for input to AffineTransformer, GCPTransformer, or RPCTransformer.
    rows : list or int
        Pixel rows.
    cols : int or sequence of ints
        Pixel columns.
    zs : list or float, optional
        Height associated with coordinates. Primarily used for RPC based
        coordinate transformations. Ignored for affine based
        transformations. Default: 0.
    offset : str, optional
        Determines if the returned coordinates are for the center of the
        pixel or for a corner.
    rpc_options : dict, optional
        Additional arguments passed to GDALCreateRPCTransformer.

    Returns
    -------
    xs : float or list of floats
        x coordinates in coordinate reference system
    ys : float or list of floats
        y coordinates in coordinate reference system

    """
    transformer_cls = get_transformer(transform, **rpc_options)
    with transformer_cls() as transformer:
        return transformer.xy(rows, cols, zs=zs, offset=offset)


def rowcol(
    transform,
    xs,
    ys,
    zs=None,
    op=None,
    precision=None,
    **rpc_options,
):
    """Get rows and cols of the pixels containing (x, y).

    Parameters
    ----------
    transform : Affine or sequence of GroundControlPoint or RPC
        Transform suitable for input to AffineTransformer,
        GCPTransformer, or RPCTransformer.
    xs : list or float
        x values in coordinate reference system.
    ys : list or float
        y values in coordinate reference system.
    zs : list or float, optional
        Height associated with coordinates. Primarily used for RPC based
        coordinate transformations. Ignored for affine based
        transformations. Default: 0.
    op : function, optional (default: numpy.floor)
        Function to convert fractional pixels to whole numbers (floor,
        ceiling, round)
    precision : int or float, optional
        This parameter is unused, deprecated in rasterio 1.3.0, and
        will be removed in version 2.0.0.
    rpc_options : dict, optional
        Additional arguments passed to GDALCreateRPCTransformer.

    Returns
    -------
    rows : array of ints or floats
    cols : array of ints or floats
        Integers are the default. The numerical type is determined by
        the type returned by op().

    """
    if precision is not None:
        warnings.warn(
            "The precision parameter is unused, deprecated, and will be removed in 2.0.0.",
            RasterioDeprecationWarning,
        )

    transformer_cls = get_transformer(transform, **rpc_options)
    with transformer_cls() as transformer:
        return transformer.rowcol(xs, ys, zs=zs, op=op)


def from_gcps(gcps):
    """Make an Affine transform from ground control points.

    Parameters
    ----------
    gcps : sequence of GroundControlPoint
        Such as the first item of a dataset's `gcps` property.

    Returns
    -------
    Affine

    """
    return Affine.from_gdal(*_transform_from_gcps(gcps))


class TransformerBase:
    """Generic GDAL transformer base class

    Notes
    -----
    Subclasses must have a _transformer attribute and implement a `_transform` method.

    """
    def __init__(self):
        self._transformer = None

    @staticmethod
    def _ensure_arr_input(xs, ys, zs=None):
        """Ensure all input coordinates are mapped to array-like objects

        Raises
        ------
        TransformError
            If input coordinates are not all of the same length
        """
        xs = np.atleast_1d(xs)
        ys = np.atleast_1d(ys)
        if zs is not None:
            zs = np.atleast_1d(zs)
        else:
            zs = np.zeros(1)

        try:
            broadcasted = np.broadcast(xs, ys, zs)
        except ValueError as error:
            raise TransformError() from error

        return xs, ys, zs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def rowcol(self, xs, ys, zs=None, op=None, precision=None):
        """Get rows and cols coordinates given geographic coordinates.

        Parameters
        ----------
        xs, ys : float or list of float
            Geographic coordinates
        zs : float or list of float, optional
            Height associated with coordinates. Primarily used for RPC
            based coordinate transformations. Ignored for affine based
            transformations. Default: 0.
        op : function, optional (default: numpy.floor)
            Function to convert fractional pixels to whole numbers
            (floor, ceiling, round)
        precision : int, optional (default: None)
            This parameter is unused, deprecated in rasterio 1.3.0, and
            will be removed in version 2.0.0.

        Raises
        ------
        TypeError
            If coordinate transformation fails.
        ValueError
            If input coordinates are not all equal length.

        Returns
        -------
        tuple of numbers or array of numbers.
            Integers are the default. The numerical type is determined
            by the type returned by op().

        """
        if precision is not None:
            warnings.warn(
                "The precision parameter is unused, deprecated, and will be removed in 2.0.0.",
                RasterioDeprecationWarning,
            )

        IS_SCALAR = isinstance(xs, Number) and isinstance(ys, Number)
        xs, ys, zs = self._ensure_arr_input(xs, ys, zs=zs)

        try:
            new_cols, new_rows = self._transform(
                xs, ys, zs, transform_direction=TransformDirection.reverse
            )

            if op is None:
                new_rows = np.floor(new_rows).astype(dtype=np.int32)
                new_cols = np.floor(new_cols).astype(dtype=np.int32)
            elif isinstance(op, np.ufunc):
                op(new_rows, out=new_rows)
                op(new_cols, out=new_cols)
            else:
                new_rows = np.array(list(map(op, new_rows)))
                new_cols = np.array(list(map(op, new_cols)))

            if IS_SCALAR:
                return new_rows[0], new_cols[0]
            else:
                return new_rows, new_cols

        except TypeError:
            raise TransformError("Invalid inputs")

    def xy(self, rows, cols, zs=None, offset='center'):
        """
        Returns geographic coordinates given dataset rows and cols coordinates

        Parameters
        ----------
        rows, cols : int or list of int
            Image pixel coordinates
        zs : float or list of float, optional
            Height associated with coordinates. Primarily used for RPC based
            coordinate transformations. Ignored for affine based
            transformations. Default: 0.
        offset : str, optional
            Determines if the returned coordinates are for the center of the
            pixel or for a corner. Available options include center, ul, ur, ll,
            lr.
        Raises
        ------
        ValueError
            If input coordinates are not all equal length

        Returns
        -------
        tuple of float or list of float

        """
        IS_SCALAR = isinstance(rows, Number) and isinstance(cols, Number)
        rows, cols, zs = self._ensure_arr_input(rows, cols, zs=zs)

        if offset == 'center':
            coff, roff = (0.5, 0.5)
        elif offset == 'ul':
            coff, roff = (0, 0)
        elif offset == 'ur':
            coff, roff = (1, 0)
        elif offset == 'll':
            coff, roff = (0, 1)
        elif offset == 'lr':
            coff, roff = (1, 1)
        else:
            raise TransformError("Invalid offset")

        try:
            # shift input coordinates according to offset
            T = IDENTITY.translation(coff, roff)
            identity_transformer = AffineTransformer(T)
            offset_cols, offset_rows = identity_transformer._transform(
                cols, rows, zs, transform_direction=TransformDirection.forward
            )
            new_xs, new_ys = self._transform(
                offset_cols, offset_rows, zs, transform_direction=TransformDirection.forward
            )

            if IS_SCALAR:
                return new_xs[0], new_ys[0]
            else:
                return new_xs, new_ys
        except TypeError:
            raise TransformError("Invalid inputs")

    def _transform(self, xs, ys, zs, transform_direction):
        raise NotImplementedError


class GDALTransformerBase(TransformerBase):
    def __init__(self):
        super().__init__()
        self._env = ExitStack()

    def close(self):
        pass

    def __enter__(self):
        self._env.enter_context(env_ctx_if_needed())
        return self

    def __exit__(self, *args):
        self.close()
        if self._env:
            self._env.close()


class AffineTransformer(TransformerBase):
    """A pure Python class related to affine based coordinate transformations."""
    def __init__(self, affine_transform):
        super().__init__()
        if not isinstance(affine_transform, Affine):
            raise ValueError("Not an affine transform")
        self._transformer = affine_transform
        self._transform_arr = np.asarray(affine_transform, dtype='float64').reshape(3, 3)

    def _transform(self, xs, ys, zs, transform_direction):
        bi = np.broadcast(xs, ys)
        input_matrix = np.empty((3, bi.size))
        input_matrix[0] = bi.iters[0]
        input_matrix[1] = bi.iters[1]
        input_matrix[2] = 1

        if transform_direction is TransformDirection.forward:
            transformed = np.matmul(self._transform_arr, input_matrix, out=input_matrix)
        elif transform_direction is TransformDirection.reverse:
            transformed = np.linalg.solve(self._transform_arr, input_matrix)
        return transformed[0], transformed[1]

    def __repr__(self):
        return "<AffineTransformer>"


class RPCTransformer(RPCTransformerBase, GDALTransformerBase):
    """
    Class related to Rational Polynomial Coeffecients (RPCs) based
    coordinate transformations.

    Uses GDALCreateRPCTransformer and GDALRPCTransform for computations. Options
    for GDALCreateRPCTransformer may be passed using `rpc_options`.
    Ensure that GDAL transformer objects are destroyed by calling `close()`
    method or using context manager interface.

    """
    def __init__(self, rpcs, **rpc_options):
        if not isinstance(rpcs, (RPC, dict)):
            raise ValueError("RPCTransformer requires RPC")
        super().__init__(rpcs, **rpc_options)

    def __repr__(self):
        return "<{} RPCTransformer>".format(
            self.closed and 'closed' or 'open')


class GCPTransformer(GCPTransformerBase, GDALTransformerBase):
    """
    Class related to Ground Control Point (GCPs) based
    coordinate transformations.

    Uses GDALCreateGCPTransformer and GDALGCPTransform for computations.
    Ensure that GDAL transformer objects are destroyed by calling `close()`
    method or using context manager interface. If `tps` is set to True,
    uses GDALCreateTPSTransformer and GDALTPSTransform instead.

    """
    def __init__(self, gcps, tps=False):
        if len(gcps) and not isinstance(gcps[0], GroundControlPoint):
            raise ValueError("GCPTransformer requires sequence of GroundControlPoint")
        super().__init__(gcps, tps)

    def __repr__(self):
        return "<{} GCPTransformer>".format(
            self.closed and 'closed' or 'open')
