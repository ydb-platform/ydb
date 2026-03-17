import numpy as np

from .. import util
from ..boundingregion import BoundingBox
from ..dimension import dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check
from ..sheetcoords import SheetCoordinateSystem, Slice
from .grid import GridInterface
from .interface import DataError, Interface
from .util import finite_range


class ImageInterface(GridInterface):
    """Interface for 2 or 3D arrays representing images
    of raw luminance values, RGB values or HSV values.

    """

    types = (np.ndarray,)

    datatype = 'image'

    named = False

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        if kdims is None:
            kdims = eltype.kdims
        if vdims is None:
            vdims = eltype.vdims

        kwargs = {}
        dimensions = [dimension_name(d) for d in kdims + vdims]
        if isinstance(data, tuple):
            data = dict(zip(dimensions, data, strict=None))
        if isinstance(data, dict):
            xs, ys = np.asarray(data[kdims[0].name]), np.asarray(data[kdims[1].name])
            xvalid = util.validate_regular_sampling(xs, eltype.rtol or util.config.image_rtol)
            yvalid = util.validate_regular_sampling(ys, eltype.rtol or util.config.image_rtol)
            if not xvalid or not yvalid:
                raise ValueError('ImageInterface only supports regularly sampled coordinates')
            l, r, _xdensity, invertx = util.bound_range(xs, None, eltype._time_unit)
            b, t, _ydensity, inverty = util.bound_range(ys, None, eltype._time_unit)
            kwargs['bounds'] = BoundingBox(points=((l, b), (r, t)))
            if len(vdims) == 1:
                data = np.flipud(np.asarray(data[vdims[0].name]))
            else:
                data = np.dstack([np.flipud(data[vd.name]) for vd in vdims])
            if invertx:
                data = data[:, ::-1]
            if inverty:
                data = data[::-1, :]
            expected = (len(ys), len(xs))
            shape = data.shape[:2]
            error = DataError if len(shape) > 1 and not eltype._binned else ValueError
            if shape != expected and not (not expected and shape == (1,)):
                raise error(f'Key dimension values and value array {vdims[0]} '
                            f'shapes do not match. Expected shape {expected}, '
                            f'actual shape: {shape}', cls)

        if not isinstance(data, np.ndarray) or data.ndim not in [2, 3]:
            raise ValueError('ImageInterface expects a 2D array.')
        elif not issubclass(eltype, SheetCoordinateSystem):
            raise ValueError('ImageInterface may only be used on elements '
                             'that subclass SheetCoordinateSystem.')

        return data, {'kdims':kdims, 'vdims':vdims}, kwargs

    @classmethod
    def irregular(cls, dataset, dim):
        """ImageInterface does not support irregular data

        """
        return False

    @classmethod
    def shape(cls, dataset, gridded=False):
        if gridded:
            return dataset.data.shape[:2]
        else:
            return cls.length(dataset), len(dataset.dimensions())


    @classmethod
    def dtype(cls, dataset, dimension):
        idx = dataset.get_dimension_index(dimension)
        if idx in [0, 1]:
            return np.dtype('float')
        else:
            return dataset.data.dtype


    @classmethod
    def length(cls, dataset):
        return np.prod(dataset.data.shape[:2], dtype=np.intp)


    @classmethod
    def validate(cls, dataset, vdims=True):
        pass

    @classmethod
    def redim(cls, dataset, dimensions):
        return dataset.data

    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        data = dataset.data
        dropped_kdims = [kd for kd in dataset.kdims if kd not in kdims]
        constant = {}
        for kd in dropped_kdims:
            vals = cls.values(dataset, kd.name, expanded=False)
            if len(vals) == 1:
                constant[kd.name] = vals[0]
        if dropped_kdims or constant:
            return tuple(dataset.columns(kdims+vdims).values())

        if vdims is not None and vdims != dataset.vdims and len(dataset.vdims) > 1:
            inds = [dataset.get_dimension_index(vd)-dataset.ndims for vd in vdims]
            return data[..., inds] if len(inds) > 1 else data[..., inds[0]]
        return data

    @classmethod
    def coords(cls, dataset, dim, ordered=False, expanded=False, edges=False):
        dim = dataset.get_dimension(dim, strict=True)
        if expanded:
            data = util.expand_grid_coords(dataset, dim)
            if edges and data.shape == dataset.data.shape:
                data = cls._infer_interval_breaks(data, axis=1)
                data = cls._infer_interval_breaks(data, axis=0)
            return data
        values = cls.values(dataset, dim, expanded=False)
        if edges:
            return cls._infer_interval_breaks(values)
        else:
            return values

    @classmethod
    def range(cls, obj, dim):
        dim = obj.get_dimension(dim, strict=True)
        dim_idx = obj.get_dimension_index(dim)
        if dim_idx in [0, 1] and obj.bounds:
            l, b, r, t = obj.bounds.lbrt()
            if dim_idx:
                (low, high) = (b, t)
                density = obj.ydensity
            else:
                low, high = (l, r)
                density = obj.xdensity
            halfd = (1./density)/2.
            if isinstance(low, util.datetime_types):
                halfd = np.timedelta64(round(halfd), obj._time_unit)
            drange = (low+halfd, high-halfd)
        elif 1 < dim_idx < len(obj.vdims) + 2:
            dim_idx -= 2
            data = np.atleast_3d(obj.data)[:, :, dim_idx]
            if dim.nodata is not None:
                data = cls.replace_value(data, dim.nodata)
            drange = finite_range(data, np.nanmin(data), np.nanmax(data))
        else:
            drange = (None, None)
        return drange


    @classmethod
    def values(
            cls, dataset, dim, expanded=True, flat=True, compute=True, keep_index=False
    ):
        """The set of samples available along a particular dimension.

        """
        dim_idx = dataset.get_dimension_index(dim)
        if dim_idx in [0, 1]:
            l, b, r, t = dataset.bounds.lbrt()
            dim2, dim1 = dataset.data.shape[:2]
            xdate, ydate = isinstance(l, util.datetime_types), isinstance(b, util.datetime_types)
            if l == r or dim1 == 0:
                xlin = np.full((dim1,), l, dtype=('datetime64[us]' if xdate else 'float'))
            elif xdate:
                xlin = util.date_range(l, r, dim1, dataset._time_unit)
            else:
                xstep = float(r - l)/dim1
                xlin = np.linspace(l+(xstep/2.), r-(xstep/2.), dim1)
            if b == t or dim2 == 0:
                ylin = np.full((dim2,), b, dtype=('datetime64[us]' if ydate else 'float'))
            elif ydate:
                ylin = util.date_range(b, t, dim2, dataset._time_unit)
            else:
                ystep = float(t - b)/dim2
                ylin = np.linspace(b+(ystep/2.), t-(ystep/2.), dim2)
            if expanded:
                values = np.meshgrid(ylin, xlin)[abs(dim_idx-1)]
                return values.flatten() if flat else values.T
            else:
                return ylin if dim_idx else xlin
        elif dataset.ndims <= dim_idx < len(dataset.dimensions()):
            # Raster arrays are stored with different orientation
            # than expanded column format, reorient before expanding
            if dataset.data.ndim > 2:
                data = dataset.data[:, :, dim_idx-dataset.ndims]
            else:
                data = dataset.data
            data = np.flipud(data)
            return data.T.flatten() if flat else data
        else:
            return None


    @classmethod
    def mask(cls, dataset, mask, mask_val=np.nan):
        masked = dataset.data.copy().astype('float')
        masked[np.flipud(mask)] = mask_val
        return masked


    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        """Slice the underlying numpy array in sheet coordinates.

        """
        selection = {k: slice(*sel) if isinstance(sel, tuple) else sel
                     for k, sel in selection.items()}
        coords = tuple(selection[kd.name] if kd.name in selection else slice(None)
                       for kd in dataset.kdims)
        if not any([isinstance(el, slice) for el in coords]):
            return dataset.data[dataset.sheet2matrixidx(*coords)]

        # Apply slices
        xidx, yidx = coords
        l, b, r, t = dataset.bounds.lbrt()
        if isinstance(xidx, slice):
            l = l if xidx.start is None else max(l, xidx.start)
            r = r if xidx.stop is None else min(r, xidx.stop)
        if isinstance(yidx, slice):
            b = b if yidx.start is None else max(b, yidx.start)
            t = t if yidx.stop is None else min(t, yidx.stop)
        bounds = BoundingBox(points=((l, b), (r, t)))
        slc = Slice(bounds, dataset)
        return slc.submatrix(dataset.data)


    @classmethod
    def sample(cls, dataset, samples=None):
        """Sample the Raster along one or both of its dimensions,
        returning a reduced dimensionality type, which is either
        a ItemTable, Curve or Scatter. If two dimension samples
        and a new_xaxis is provided the sample will be the value
        of the sampled unit indexed by the value in the new_xaxis
        tuple.

        """
        if samples is None:
            samples = []
        if len(samples[0]) == 1:
            select = {dataset.kdims[0].name: [s[0] for s in samples]}
            return tuple(dataset.select(**select).columns().values())
        return [(*c, dataset.data[dataset._coord2matrix(c)]) for c in samples]


    @classmethod
    def groupby(cls, dataset, dim_names, container_type, group_type, **kwargs):
        # Get dimensions information
        dimensions = [dataset.get_dimension(d) for d in dim_names]
        kdims = [kdim for kdim in dataset.kdims if kdim not in dimensions]

        # Update the kwargs appropriately for Element group types
        group_kwargs = {}
        group_type = dict if group_type == 'raw' else group_type
        if issubclass(group_type, Element):
            group_kwargs.update(util.get_param_values(dataset))
            group_kwargs['kdims'] = kdims
        group_kwargs.update(kwargs)

        if len(dimensions) == 1:
            didx = dataset.get_dimension_index(dimensions[0])
            coords = dataset.dimension_values(dimensions[0], expanded=False)
            xvals = dataset.dimension_values(abs(didx-1), expanded=False)
            samples = [(i, slice(None)) if didx else (slice(None), i)
                       for i in range(dataset.data.shape[abs(didx-1)])]
            data = np.flipud(dataset.data)
            groups = [(c, group_type((xvals, data[s]), **group_kwargs))
                       for s, c in zip(samples, coords, strict=None)]
        else:
            data = zip(*[dataset.dimension_values(i) for i in range(len(dataset.dimensions()))], strict=None)
            groups = [(g[:dataset.ndims], group_type([g[dataset.ndims:]], **group_kwargs))
                      for g in data]

        if issubclass(container_type, NdMapping):
            with item_check(False):
                return container_type(groups, kdims=dimensions)
        else:
            return container_type(groups)


    @classmethod
    def unpack_scalar(cls, dataset, data):
        """Given a dataset object and data in the appropriate format for
        the interface, return a simple scalar.

        """
        if np.isscalar(data) or len(data) != 1:
            return data
        key = next(iter(data.keys()))

        if len(data[key]) == 1 and key in dataset.vdims:
            return data[key][0]


    @classmethod
    def aggregate(cls, dataset, kdims, function, **kwargs):
        kdims = [dimension_name(kd) for kd in kdims]
        axes = tuple(dataset.ndims-dataset.get_dimension_index(kdim)-1
                     for kdim in dataset.kdims if kdim not in kdims)

        data = np.atleast_1d(function(dataset.data, axis=axes, **kwargs))
        if not kdims:
            if len(dataset.vdims) == 1:
                return (data if np.isscalar(data) else data[0], [])
            else:
                return ({vd.name: np.array([v]) for vd, v in zip(dataset.vdims, data, strict=None)}, [])
        elif len(axes) == 1:
            return ({kdims[0]: cls.values(dataset, axes[0], expanded=False),
                    dataset.vdims[0].name: data[::-1] if axes[0] else data}, [])


Interface.register(ImageInterface)
