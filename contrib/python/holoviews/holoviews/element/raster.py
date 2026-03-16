import colorsys
from collections.abc import Mapping
from copy import deepcopy
from operator import itemgetter

import numpy as np
import param

from ..core import Dataset, Dimension, Element2D, Overlay, config, util
from ..core.boundingregion import BoundingBox, BoundingRegion
from ..core.data import ImageInterface
from ..core.data.interface import DataError
from ..core.dimension import dimension_name
from ..core.sheetcoords import SheetCoordinateSystem, Slice
from .chart import Curve
from .geom import Selection2DExpr
from .graphs import TriMesh
from .tabular import Table
from .util import categorical_aggregate2d, compute_slice_bounds


class Raster(Element2D):
    """Raster is a basic 2D element type for presenting either numpy or
    dask arrays as two dimensional raster images.

    Arrays with a shape of (N,M) are valid inputs for Raster whereas
    subclasses of Raster (e.g. RGB) may also accept 3D arrays
    containing channel information.

    Raster does not support slicing like the Image or RGB subclasses
    and the extents are in matrix coordinates if not explicitly
    specified.

    """

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True, doc="""
        The label of the x- and y-dimension of the Raster in form
        of a string or dimension object.""")

    group = param.String(default='Raster', constant=True)

    vdims = param.List(default=[Dimension('z')],
                       bounds=(1, None), doc="""
        The dimension description of the data held in the matrix.""")

    def __init__(self, data, kdims=None, vdims=None, extents=None, **params):
        if data is None or isinstance(data, list) and data == []:
            data = np.zeros((0, 0))
        if extents is None:
            (d1, d2) = data.shape[:2]
            extents = (0, 0, d2, d1)
        super().__init__(data, kdims=kdims, vdims=vdims, extents=extents, **params)

    def __getitem__(self, slices):
        if slices in self.dimensions(): return self.dimension_values(slices)
        slices = util.process_ellipses(self,slices)
        if not isinstance(slices, tuple):
            slices = (slices, slice(None))
        elif len(slices) > (2 + self.depth):
            raise KeyError(f"Can only slice {2 + self.depth} dimensions")
        elif len(slices) == 3 and slices[-1] not in [self.vdims[0].name, slice(None)]:
            raise KeyError(f"{self.vdims[0].name!r} is the only selectable value dimension")

        slc_types = [isinstance(sl, slice) for sl in slices[:2]]
        data = self.data.__getitem__(slices[:2][::-1])
        if all(slc_types):
            return self.clone(data, extents=None)
        elif not any(slc_types):
            return data
        else:
            return self.clone(np.expand_dims(data, axis=slc_types.index(True)),
                              extents=None)

    def range(self, dim, data_range=True, dimension_range=True):
        idx = self.get_dimension_index(dim)
        if data_range and idx == 2:
            dimension = self.get_dimension(dim)
            if self.data.size == 0:
                return np.nan, np.nan
            lower, upper = np.nanmin(self.data), np.nanmax(self.data)
            if not dimension_range:
                return lower, upper
            return util.dimension_range(lower, upper, dimension.range, dimension.soft_range)
        return super().range(dim, data_range, dimension_range)

    def dimension_values(self, dim, expanded=True, flat=True):
        dim_idx = self.get_dimension_index(dim)
        if not expanded and dim_idx == 0:
            return np.array(range(self.data.shape[1]))
        elif not expanded and dim_idx == 1:
            return np.array(range(self.data.shape[0]))
        elif dim_idx in [0, 1]:
            values = np.mgrid[0:self.data.shape[1], 0:self.data.shape[0]][dim_idx]
            return values.flatten() if flat else values
        elif dim_idx == 2:
            arr = self.data.T
            return arr.flatten() if flat else arr
        else:
            return super().dimension_values(dim)

    def sample(self, samples=None, bounds=None, **sample_values):
        """Sample the Raster along one or both of its dimensions,
        returning a reduced dimensionality type, which is either
        a ItemTable, Curve or Scatter. If two dimension samples
        and a new_xaxis is provided the sample will be the value
        of the sampled unit indexed by the value in the new_xaxis
        tuple.

        """
        if samples is None:
            samples = []
        if isinstance(samples, tuple):
            X, Y = samples
            samples = zip(X, Y, strict=None)

        params = dict(self.param.values(onlychanged=True),
                      vdims=self.vdims)
        if len(sample_values) == self.ndims or len(samples):
            if not len(samples):
                samples = zip(*[c if isinstance(c, list) else [c] for _, c in
                               sorted([(self.get_dimension_index(k), v) for k, v in
                                       sample_values.items()])], strict=None)
            table_data = [(*c, self._zdata[self._coord2matrix(c)])
                          for c in samples]
            params['kdims'] = self.kdims
            return Table(table_data, **params)
        else:
            dimension, sample_coord = next(iter(sample_values.items()))
            if isinstance(sample_coord, slice):
                raise ValueError(
                    'Raster sampling requires coordinates not slices,'
                    'use regular slicing syntax.')
            # Indices inverted for indexing
            sample_ind = self.get_dimension_index(dimension)
            if sample_ind is None:
                raise Exception(f"Dimension {dimension} not found during sampling")
            other_dimension = [d for i, d in enumerate(self.kdims) if
                               i != sample_ind]

            # Generate sample slice
            sample = [slice(None) for i in range(self.ndims)]
            coord_fn = (lambda v: (v, 0)) if not sample_ind else (lambda v: (0, v))
            sample[sample_ind] = self._coord2matrix(coord_fn(sample_coord))[abs(sample_ind-1)]

            # Sample data
            x_vals = self.dimension_values(other_dimension[0].name, False)
            ydata = self._zdata[tuple(sample[::-1])]
            if hasattr(self, 'bounds') and sample_ind == 0: ydata = ydata[::-1]
            data = list(zip(x_vals, ydata, strict=None))
            params['kdims'] = other_dimension
            return Curve(data, **params)


    def reduce(self, dimensions=None, function=None, **reduce_map):
        """Reduces the Raster using functions provided via the
        kwargs, where the keyword is the dimension to be reduced.
        Optionally a label_prefix can be provided to prepend to
        the result Element label.

        """
        function, dims = self._reduce_map(dimensions, function, reduce_map)
        if len(dims) == self.ndims:
            if isinstance(function, np.ufunc):
                return function.reduce(self.data, axis=None)
            else:
                return function(self.data)
        else:
            dimension = dims[0]
            other_dimension = [d for d in self.kdims if d.name != dimension]
            oidx = self.get_dimension_index(other_dimension[0])
            x_vals = self.dimension_values(other_dimension[0].name, False)
            reduced = function(self._zdata, axis=oidx)
            if oidx and hasattr(self, 'bounds'):
                reduced = reduced[::-1]
            data = zip(x_vals, reduced, strict=None)
            params = dict(dict(self.param.values(onlychanged=True)),
                          kdims=other_dimension, vdims=self.vdims)
            params.pop('bounds', None)
            params.pop('extents', None)
            return Table(data, **params)

    @property
    def depth(self):
        return len(self.vdims)

    @property
    def _zdata(self):
        return self.data

    def _coord2matrix(self, coord):
        return round(coord[1]), round(coord[0])

    def __len__(self):
        return np.prod(self._zdata.shape)




class Image(Selection2DExpr, Dataset, Raster, SheetCoordinateSystem):
    """Image represents a regularly sampled 2D grid of an underlying
    continuous space of intensity values, which will be colormapped on
    plotting. The grid of intensity values may be specified as a NxM
    sized array of values along with a bounds, but it may also be
    defined through explicit and regularly spaced x/y-coordinate
    arrays of shape M and N respectively. The two most basic supported
    constructors of an Image therefore include:

        Image((X, Y, Z))

    where X is a 1D array of shape M, Y is a 1D array of shape N and
    Z is a 2D array of shape NxM, or equivalently:

        Image(Z, bounds=(x0, y0, x1, y1))

    where Z is a 2D array of shape NxM defining the intensity values
    and the bounds define the (left, bottom, top, right) edges of four
    corners of the grid. Other gridded formats which support declaring
    of explicit x/y-coordinate arrays such as xarray are also
    supported.

    Note that the interpretation of the orientation of the array
    changes depending on whether bounds or explicit coordinates are
    used.

    """

    bounds = param.ClassSelector(class_=BoundingRegion, default=BoundingBox(), doc="""
        The bounding region in sheet coordinates containing the data.""")

    datatype = param.List(default=['grid', 'xarray', 'image', 'cube', 'dataframe', 'dictionary'])

    group = param.String(default='Image', constant=True)

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True, doc="""
        The label of the x- and y-dimension of the Raster in the form
        of a string or dimension object.""")

    vdims = param.List(default=[Dimension('z')],
                       bounds=(1, None), doc="""
        The dimension description of the data held in the matrix.""")

    rtol = param.Number(default=None, doc="""
        The tolerance used to enforce regular sampling for regular, gridded
        data where regular sampling is expected. Expressed as the maximal
        allowable sampling difference between sample locations.""")

    _ndim = 2

    def __init__(self, data, kdims=None, vdims=None, bounds=None, extents=None,
                 xdensity=None, ydensity=None, rtol=None, **params):
        supplied_bounds = bounds
        if isinstance(data, Image):
            bounds = bounds or data.bounds
            xdensity = xdensity or data.xdensity
            ydensity = ydensity or data.ydensity
            if rtol is None: rtol = data.rtol

        extents = extents if extents else (None, None, None, None)
        if (data is None
            or (isinstance(data, (list, tuple)) and not data)
            or (isinstance(data, np.ndarray) and data.size == 0)):
            data = data if isinstance(data, np.ndarray) and data.ndim == 2 else np.zeros((0, 0))
            bounds = 0
            if not xdensity: xdensity = 1
            if not ydensity: ydensity = 1
        elif isinstance(data, np.ndarray) and data.ndim < self._ndim:
            raise ValueError(f'{type(self).__name__} type expects {self._ndim}-D array received {data.ndim}-D '
                             'array.')

        if rtol is not None:
            params['rtol'] = rtol
        else:
            params['rtol'] = config.image_rtol

        Dataset.__init__(self, data, kdims=kdims, vdims=vdims, extents=extents, **params)
        if not self.interface.gridded:
            raise DataError(
                f"{type(self).__name__} type expects gridded data, "
                f"{self.interface.__name__} is columnar. "
                "To display columnar data as gridded use the HeatMap "
                "element or aggregate the data (e.g. using np.histogram2d)."
            )

        dim2, dim1 = self.interface.shape(self, gridded=True)[:2]
        if bounds is None:
            xvals = self.dimension_values(0, False)
            l, r, xdensity, _ = util.bound_range(xvals, xdensity, self._time_unit)
            yvals = self.dimension_values(1, False)
            b, t, ydensity, _ = util.bound_range(yvals, ydensity, self._time_unit)
            bounds = BoundingBox(points=((l, b), (r, t)))
        elif np.isscalar(bounds):
            bounds = BoundingBox(radius=bounds)
        elif isinstance(bounds, (tuple, list, np.ndarray)):
            l, b, r, t = bounds
            bounds = BoundingBox(points=((l, b), (r, t)))

        data_bounds = None
        if self.interface is ImageInterface and not isinstance(data, (np.ndarray, Image)):
            data_bounds = self.bounds.lbrt()

        non_finite = all(not util.isfinite(v) for v in bounds.lbrt())
        if non_finite:
            bounds = BoundingBox(points=((0, 0), (0, 0)))
            xdensity = xdensity if xdensity and util.isfinite(xdensity) else 1
            ydensity = ydensity if ydensity and util.isfinite(ydensity) else 1
        else:
            l, b, r, t = bounds.lbrt()
            xdensity = xdensity if xdensity else util.compute_density(l, r, dim1, self._time_unit)
            ydensity = ydensity if ydensity else util.compute_density(b, t, dim2, self._time_unit)
        SheetCoordinateSystem.__init__(self, bounds, xdensity, ydensity)
        if non_finite:
           self.bounds = BoundingBox(points=((np.nan, np.nan), (np.nan, np.nan)))
        self._validate(data_bounds, supplied_bounds)

    def _validate(self, data_bounds, supplied_bounds):
        if len(self.shape) == 3:
            if self.shape[2] != len(self.vdims):
                raise ValueError(f"Input array has shape {self.shape!r} but {len(self.vdims)} value dimensions defined")

        # Ensure coordinates are regularly sampled
        clsname = type(self).__name__
        xdim, ydim = self.kdims
        xvals, yvals = (self.dimension_values(d, expanded=False, flat=False)
                        for d in self.kdims)
        invalid = []
        if xvals.ndim > 1:
            invalid.append(xdim)
        if yvals.ndim > 1:
            invalid.append(ydim)
        if invalid:
            dims = '{} and {}'.format(*invalid) if len(invalid) > 1 else f'{invalid[0]}'
            raise ValueError(f'{clsname} coordinates must be 1D arrays, '
                             f'{dims} dimension(s) were found to have '
                             'multiple dimensions. Either supply 1D '
                             'arrays or use the QuadMesh element for '
                             'curvilinear coordinates.')

        xvalid = util.validate_regular_sampling(xvals, self.rtol)
        yvalid = util.validate_regular_sampling(yvals, self.rtol)
        msg = ("{clsname} dimension{dims} not evenly sampled to relative "
               "tolerance of {rtol}. Please use the QuadMesh element for "
               "irregularly sampled data or set a higher tolerance on "
               "hv.config.image_rtol or the rtol parameter in the "
               "{clsname} constructor.")
        dims = None
        if not xvalid:
            dims = f' {xdim} is ' if yvalid else f'(s) {xdim} and {ydim} are'
        elif not yvalid:
            dims = f' {ydim} is'
        if dims:
            self.param.warning(
                msg.format(clsname=clsname, dims=dims, rtol=self.rtol))

        if not supplied_bounds:
            return

        if data_bounds is None:
            (x0, x1), (y0, y1) = (self.interface.range(self, kd.name) for kd in self.kdims)
            xstep = (1./self.xdensity)/2.
            ystep = (1./self.ydensity)/2.
            if not isinstance(x0, util.datetime_types):
                x0, x1 = (x0-xstep, x1+xstep)
            if not isinstance(y0, util.datetime_types):
                y0, y1 = (y0-ystep, y1+ystep)
            bounds = (x0, y0, x1, y1)
        else:
            bounds = data_bounds

        not_close = False
        for r, c in zip(bounds, self.bounds.lbrt(), strict=None):
            if isinstance(r, util.datetime_types):
                r = util.dt_to_int(r)
            if isinstance(c, util.datetime_types):
                c = util.dt_to_int(c)
            if util.isfinite(r) and not np.isclose(r, c, rtol=self.rtol):
                not_close = True
        if not_close:
            raise ValueError('Supplied Image bounds do not match the coordinates defined '
                             'in the data. Bounds only have to be declared if no coordinates '
                             'are supplied, otherwise they must match the data. To change '
                             'the displayed extents set the range on the x- and y-dimensions.')

    def clone(self, data=None, shared_data=True, new_type=None, link=True,
              *args, **overrides):
        """Returns a clone of the object with matching parameter values
        containing the specified args and kwargs.

        If shared_data is set to True and no data explicitly supplied,
        the clone will share data with the original. May also supply
        a new_type, which will inherit all shared parameters.

        """
        if data is None and (new_type is None or issubclass(new_type, Image)):
            sheet_params = dict(bounds=self.bounds, xdensity=self.xdensity,
                                ydensity=self.ydensity)
            overrides = dict(sheet_params, **overrides)
        return super().clone(data, shared_data, new_type, link,
                                        *args, **overrides)

    def aggregate(self, dimensions=None, function=None, spreadfn=None, **kwargs):
        agg = super().aggregate(dimensions, function, spreadfn, **kwargs)
        return Curve(agg) if isinstance(agg, Dataset) and len(self.vdims) == 1 else agg

    def select(self, selection_expr=None, selection_specs=None, **selection):
        """Allows selecting data by the slices, sets and scalar values
        along a particular dimension. The indices should be supplied as
        keywords mapping between the selected dimension and
        value. Additionally selection_specs (taking the form of a list
        of type.group.label strings, types or functions) may be
        supplied, which will ensure the selection is only applied if the
        specs match the selected object.

        """
        if isinstance(selection_expr, Mapping):
            if selection:
                raise ValueError("""\
                Selections may be supplied as keyword arguments or as a positional
                argument, never both.""")
            selection = selection_expr
            selection_expr = None
        if selection_specs and not any(self.matches(sp) for sp in selection_specs):
            return self

        selection = {self.get_dimension(k).name: slice(*sel) if isinstance(sel, tuple) else sel
                     for k, sel in selection.items() if k in self.kdims}
        coords = tuple(selection[kd.name] if kd.name in selection else slice(None)
                       for kd in self.kdims)

        shape = self.interface.shape(self, gridded=True)
        if any([isinstance(el, slice) for el in coords]):
            bounds = compute_slice_bounds(coords, self, shape[:2])

            # Situate resampled region into overall slice
            y0, y1, x0, x1 = Slice(bounds, self)
            y0, y1 = shape[0]-y1, shape[0]-y0
            selection = (slice(y0, y1), slice(x0, x1))
            sliced = True
        else:
            y, x = self.sheet2matrixidx(coords[0], coords[1])
            y = shape[0]-y-1
            selection = (y, x)
            sliced = False

        datatype = list(util.unique_iterator([self.interface.datatype, *self.datatype]))
        data = self.interface.ndloc(self, selection)
        if not sliced:
            if np.isscalar(data):
                return data
            elif isinstance(data, tuple):
                data = data[self.ndims:]
            return self.clone(data, kdims=[], new_type=Dataset,
                              datatype=datatype)
        else:
            return self.clone(data, xdensity=self.xdensity, datatype=datatype,
                              ydensity=self.ydensity, bounds=bounds)


    def closest(self, coords=None, **kwargs):
        """Given a single coordinate or multiple coordinates as
        a tuple or list of tuples or keyword arguments matching
        the dimension closest will find the closest actual x/y
        coordinates.

        """
        if coords is None:
            coords = []
        if kwargs and coords:
            raise ValueError("Specify coordinate using as either a list "
                             "keyword arguments not both")
        if kwargs:
            coords = []
            getter = []
            for k, v in kwargs.items():
                idx = self.get_dimension_index(k)
                if np.isscalar(v):
                    coords.append((0, v) if idx else (v, 0))
                else:
                    if isinstance(v, list):
                        coords = [(0, c) if idx else (c, 0) for c in v]
                    if len(coords) not in [0, len(v)]:
                        raise ValueError("Length of samples must match")
                    elif coords:
                        coords = [(t[abs(idx-1)], c) if idx else (c, t[abs(idx-1)])
                                  for c, t in zip(v, coords, strict=None)]
                getter.append(idx)
        else:
            getter = [0, 1]
        getter = itemgetter(*sorted(getter))
        if len(coords) == 1:
            coords = coords[0]
        if isinstance(coords, tuple):
            return getter(self.closest_cell_center(*coords))
        else:
            return [getter(self.closest_cell_center(*el)) for el in coords]


    def range(self, dim, data_range=True, dimension_range=True):
        idx = self.get_dimension_index(dim)
        dimension = self.get_dimension(dim)
        if idx in [0, 1] and data_range and dimension.range == (None, None):
            l, b, r, t = self.bounds.lbrt()
            return (b, t) if idx else (l, r)
        else:
            return super().range(dim, data_range, dimension_range)

    def _coord2matrix(self, coord):
        return self.sheet2matrixidx(*coord)


class ImageStack(Image):
    """ImageStack expands the capabilities of Image to by supporting
    multiple layers of images.

    As there is many ways to represent multiple layers of images,
    the following options are supported:

        1) A 3D Numpy array with the shape (y, x, level)
        2) A list of 2D Numpy arrays with identical shape (y, x)
        3) A dictionary where the keys will be set as the vdims and the
            values are 2D Numpy arrays with identical shapes (y, x).
            If the dictionary's keys matches the kdims of the element,
            they need to be 1D arrays.
        4) A tuple containing (x, y, level_0, level_1, ...),
            where the level is a 2D Numpy array in the shape of (y, x).
        5) An xarray DataArray or Dataset where its `coords` contain the kdims.

    If no kdims are supplied, x and y are used.

    If no vdims are supplied, and the naming can be inferred like with a dictionary
    the levels will be named level_0, level_1, etc.

    """

    vdims = param.List(doc="""
        The dimension description of the data held in the matrix.""")

    group = param.String(default='ImageStack', constant=True)

    _ndim = 3

    _vdim_reductions = {1: Image}

    def __init__(self, data, kdims=None, vdims=None, **params):
        _kdims = kdims or self.kdims
        if isinstance(data, list) and len(data):
            x = np.arange(data[0].shape[1])
            y = np.arange(data[0].shape[0])
            data = (x, y, *data)
        elif isinstance(data, dict):
            first = next(v for k, v in data.items() if k not in _kdims)
            xdim, ydim = map(str, _kdims)
            if xdim not in data:
                data[xdim] = np.arange(first.shape[1])
            if ydim not in data:
                data[ydim] = np.arange(first.shape[0])
        elif isinstance(data, np.ndarray) and data.ndim == 3:
            x = np.arange(data.shape[1])
            y = np.arange(data.shape[0])
            arr = (data[:, :, n] for n in range(data.shape[2]))
            data = (x, y, *arr)
        elif (
            isinstance(data, tuple) and len(data) == 3
            and isinstance(data[2], np.ndarray) and data[2].ndim == 3
        ):
            arr = (data[2][:, :, n] for n in range(data[2].shape[2]))
            data = (data[0], data[1], *arr)

        if vdims is None:
            if isinstance(data, tuple):
                vdims = [Dimension(f"level_{i}") for i in range(len(data[2:]))]
            elif isinstance(data, dict):
                vdims = [Dimension(key) for key in data.keys() if key not in _kdims]
        super().__init__(data, kdims=kdims, vdims=vdims, **params)


class RGB(Image):
    """RGB represents a regularly spaced 2D grid of an underlying
    continuous space of RGB(A) (red, green, blue and alpha) color
    space values. The definition of the grid closely matches the
    semantics of an Image and in the simplest case the grid may be
    specified as a NxMx3 or NxMx4 array of values along with a bounds,
    but it may also be defined through explicit and regularly spaced
    x/y-coordinate arrays. The two most basic supported constructors
    of an RGB element therefore include:

        RGB((X, Y, R, G, B))

    where X is a 1D array of shape M, Y is a 1D array of shape N and
    R/G/B are 2D array of shape NxM, or equivalently:

        RGB(Z, bounds=(x0, y0, x1, y1))

    where Z is a 3D array of stacked R/G/B arrays with shape NxMx3/4
    and the bounds define the (left, bottom, top, right) edges of the
    four corners of the grid. Other gridded formats which support
    declaring of explicit x/y-coordinate arrays such as xarray are
    also supported.

    Note that the interpretation of the orientation changes depending
    on whether bounds or explicit coordinates are used.

    """

    group = param.String(default='RGB', constant=True)

    alpha_dimension = param.ClassSelector(default=Dimension('A',range=(0,1)),
                                          class_=Dimension, instantiate=False,  doc="""
        The alpha dimension definition to add the value dimensions if
        an alpha channel is supplied.""")

    vdims = param.List(
        default=[Dimension('R', range=(0,1)), Dimension('G',range=(0,1)),
                 Dimension('B', range=(0,1))], bounds=(3, 4), doc="""
        The dimension description of the data held in the matrix.

        If an alpha channel is supplied, the defined alpha_dimension
        is automatically appended to this list.""")

    _ndim = 3
    _vdim_reductions = {1: Image}

    @property
    def rgb(self):
        """Returns the corresponding RGB element.

        Other than the updating parameter definitions, this is the
        only change needed to implemented an arbitrary colorspace as a
        subclass of RGB.

        """
        return self

    @classmethod
    def load_image(cls, filename, height=1, array=False, bounds=None, bare=False, **kwargs):
        """Load an image from a file and return an RGB element or array

        Parameters
        ----------
        filename
            Filename of the image to be loaded
        height
            Determines the bounds of the image where the width
            is scaled relative to the aspect ratio of the image.
        array
            Whether to return an array (rather than RGB default)
        bounds
            Bounds for the returned RGB (overrides height)
        bare
            Whether to hide the axes
        kwargs
            Additional kwargs to the RGB constructor

        Returns
        -------
        RGB element or array
        """
        try:
            from PIL import Image
        except ImportError:
            raise ImportError(f"{cls.__name__}.load_image requires PIL (or Pillow).") from None

        with open(filename, 'rb') as f:
            data = np.array(Image.open(f))
            data = data / 255.

        if array:
            return data

        (h, w, _) = data.shape
        if bounds is None:
            f = float(height) / h
            xoffset, yoffset = w*f/2, h*f/2
            bounds=(-xoffset, -yoffset, xoffset, yoffset)
        rgb = cls(data, bounds=bounds, **kwargs)
        if bare:
            rgb.opts(xaxis=None, yaxis=None)
        return rgb

    def __init__(self, data, kdims=None, vdims=None, **params):
        if isinstance(data, Overlay):
            images = data.values()
            if not all(isinstance(im, Image) for im in images):
                raise ValueError("Input overlay must only contain Image elements")
            shapes = [im.data.shape for im in images]
            if not all(shape==shapes[0] for shape in shapes):
                raise ValueError("Images in the input overlays must contain data of the consistent shape")
            ranges = [im.vdims[0].range for im in images]
            if any(None in r for r in ranges):
                raise ValueError("Ranges must be defined on all the value dimensions of all the Images")
            arrays = [(im.data - r[0]) / (r[1] - r[0]) for r,im in zip(ranges, images, strict=None)]
            data = np.dstack(arrays)
        if vdims is None:
            # Need to make a deepcopy of the value so the RGB.default is not shared across instances
            vdims = deepcopy(self.vdims)
        else:
            vdims = list(vdims) if isinstance(vdims, list) else [vdims]

        if self._has_alpha_dimension(data, vdims):
            vdims.append(self.alpha_dimension)
        super().__init__(data, kdims=kdims, vdims=vdims, **params)

    def _has_alpha_dimension(self, data, vdims) -> bool:
        # Handle all forms of packed value dimensions
        if len(vdims) != 3:
            return False

        alpha = self.alpha_dimension

        if hasattr(data, "shape") and data.shape[-1] == 4:
            return True

        if isinstance(data, tuple):
            last = data[-1]
            if isinstance(last, np.ndarray) and last.ndim == 3 and last.shape[-1] == 4:
                return True

        if isinstance(data, dict) and (*map(dimension_name, vdims), alpha.name) in data:
            return True

        if str(alpha) in getattr(data, "data_vars", []):
            return True

        return False


class HSV(RGB):
    """HSV represents a regularly spaced 2D grid of an underlying
    continuous space of HSV (hue, saturation and value) color space
    values. The definition of the grid closely matches the semantics
    of an Image or RGB element and in the simplest case the grid may
    be specified as a NxMx3 or NxMx4 array of values along with a
    bounds, but it may also be defined through explicit and regularly
    spaced x/y-coordinate arrays. The two most basic supported
    constructors of an HSV element therefore include:

        HSV((X, Y, H, S, V))

    where X is a 1D array of shape M, Y is a 1D array of shape N and
    H/S/V are 2D array of shape NxM, or equivalently:

        HSV(Z, bounds=(x0, y0, x1, y1))

    where Z is a 3D array of stacked H/S/V arrays with shape NxMx3/4
    and the bounds define the (left, bottom, top, right) edges of the
    four corners of the grid. Other gridded formats which support
    declaring of explicit x/y-coordinate arrays such as xarray are
    also supported.

    Note that the interpretation of the orientation changes depending
    on whether bounds or explicit coordinates are used.

    """

    group = param.String(default='HSV', constant=True)

    alpha_dimension = param.ClassSelector(default=Dimension('A',range=(0,1)),
                                          class_=Dimension, instantiate=False,  doc="""
        The alpha dimension definition to add the value dimensions if
        an alpha channel is supplied.""")

    vdims = param.List(
        default=[Dimension('H', range=(0,1), cyclic=True),
                 Dimension('S',range=(0,1)),
                 Dimension('V', range=(0,1))], bounds=(3, 4), doc="""
        The dimension description of the data held in the array.

        If an alpha channel is supplied, the defined alpha_dimension
        is automatically appended to this list.""")

    hsv_to_rgb = np.vectorize(colorsys.hsv_to_rgb)

    @property
    def rgb(self):
        """Conversion from HSV to RGB.

        """
        coords = tuple(self.dimension_values(d, expanded=False)
                       for d in self.kdims)
        data = [self.dimension_values(d, flat=False)
                for d in self.vdims]

        hsv = self.hsv_to_rgb(*data[:3])
        if len(self.vdims) == 4:
            hsv += (data[3],)

        params = util.get_param_values(self)
        del params['vdims']
        return RGB(coords+hsv, bounds=self.bounds,
                   xdensity=self.xdensity, ydensity=self.ydensity,
                   **params)


class QuadMesh(Selection2DExpr, Dataset, Element2D):
    """A QuadMesh represents 2D rectangular grid expressed as x- and
    y-coordinates defined as 1D or 2D arrays. Unlike the Image type
    a QuadMesh may be regularly or irregularly spaced and contain
    either bin edges or bin centers. If bin edges are supplied the
    shape of the x/y-coordinate arrays should be one greater than the
    shape of the value array.

    The default interface expects data to be specified in the form:

        QuadMesh((X, Y, Z))

    where X and Y may be 1D or 2D arrays of the shape N(+1) and M(+1)
    respectively or N(+1)xM(+1) and the Z value array should be of
    shape NxM. Other gridded formats such as xarray are also supported
    if installed.

    The grid orientation follows the standard matrix convention: An
    array Z with shape (nrows, ncolumns) is plotted with the column
    number as X and the row number as Y.

    """

    group = param.String(default="QuadMesh", constant=True)

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True)

    vdims = param.List(default=[Dimension('z')], bounds=(1, None))

    _binned = True

    def __init__(self, data, kdims=None, vdims=None, **params):
        if data is None or isinstance(data, list) and data == []:
            data = ([], [], np.zeros((0, 0)))
        super().__init__(data, kdims, vdims, **params)
        if not self.interface.gridded:
            raise DataError(
                f"{type(self).__name__} type expects gridded data, "
                f"{self.interface.__name__} is columnar. "
                "To display columnar data as gridded use the HeatMap "
                "element or aggregate the data (e.g. using np.histogram2d)."
            )

    def trimesh(self):
        """Converts a QuadMesh into a TriMesh.

        """
        # Generate vertices
        xs = self.interface.coords(self, 0, edges=True)
        ys = self.interface.coords(self, 1, edges=True)

        if xs.ndim == 1:
            if np.all(xs[1:] < xs[:-1]):
                xs = xs[::-1]
            if np.all(ys[1:] < ys[:-1]):
                ys = ys[::-1]
            xs, ys = (np.tile(xs[:, np.newaxis], len(ys)).T,
                      np.tile(ys[:, np.newaxis], len(xs)))
        vertices = (xs.T.flatten(), ys.T.flatten())

        # Generate triangle simplexes
        shape = self.dimension_values(2, flat=False).shape
        s0 = shape[0]
        t1 = np.arange(np.prod(shape))
        js = (t1//s0)
        t1s = js*(s0+1)+t1%s0
        t2s = t1s+1
        t3s = (js+1)*(s0+1)+t1%s0
        t4s = t2s
        t5s = t3s
        t6s = t3s+1
        t1 = np.concatenate([t1s, t6s])
        t2 = np.concatenate([t2s, t5s])
        t3 = np.concatenate([t3s, t4s])
        ts = (t1, t2, t3)
        for vd in self.vdims:
            zs = self.dimension_values(vd)
            ts = (*ts, np.concatenate([zs, zs]))

        # Construct TriMesh
        params = util.get_param_values(self)
        params['kdims'] = params['kdims'] + TriMesh.node_type.kdims[2:]
        nodes = TriMesh.node_type((*vertices, np.arange(len(vertices[0]))),
                                  **{k: v for k, v in params.items()
                                     if k != 'vdims'})
        return TriMesh(((ts,), nodes), **{k: v for k, v in params.items()
                                          if k != 'kdims'})



class HeatMap(Selection2DExpr, Dataset, Element2D):
    """HeatMap represents a 2D grid of categorical coordinates which can
    be computed from a sparse tabular representation. A HeatMap does
    not automatically aggregate the supplied values, so if the data
    contains multiple entries for the same coordinate on the 2D grid
    it should be aggregated using the aggregate method before display.

    The HeatMap constructor will support any tabular or gridded data
    format with 2 coordinates and at least one value dimension. A
    simple example:

        HeatMap([(x1, y1, z1), (x2, y2, z2), ...])

    However any tabular and gridded format, including pandas
    DataFrames, dictionaries of columns, xarray DataArrays and more
    are supported if the library is importable.

    """

    group = param.String(default='HeatMap', constant=True)

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True)

    vdims = param.List(default=[Dimension('z')], constant=True)

    def __init__(self, data, kdims=None, vdims=None, **params):
        super().__init__(data, kdims=kdims, vdims=vdims, **params)
        self._gridded = None

    @property
    def gridded(self):
        if self._gridded is None:
            self._gridded = categorical_aggregate2d(self)
        return self._gridded

    @property
    def _unique(self):
        """Reports if the Dataset is unique.

        """
        return self.gridded.label != 'non-unique'

    def range(self, dim, data_range=True, dimension_range=True):
        """Return the lower and upper bounds of values along dimension.

        Parameters
        ----------
        dimension
            The dimension to compute the range on.
        data_range : bool
            Compute range from data values
        dimension_range : bool
            Include Dimension ranges
            Whether to include Dimension range and soft_range
            in range calculation

        Returns
        -------
        Tuple containing the lower and upper bound
        """
        dim = self.get_dimension(dim)
        if dim in self.kdims:
            try:
                self.gridded._binned = True
                if self.gridded is self:
                    return super().range(dim, data_range, dimension_range)
                else:
                    drange = self.gridded.range(dim, data_range, dimension_range)
            except Exception:
                drange = None
            finally:
                self.gridded._binned = False
            if drange is not None:
                return drange
        return super().range(dim, data_range, dimension_range)
