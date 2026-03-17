import sys
import types

import numpy as np

from .. import util
from ..dimension import Dimension, asdim, dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind
from ..util.dependencies import _no_import_version
from .grid import GridInterface
from .interface import DataError, Interface
from .util import dask_array_module, finite_range

XARRAY_VERSION = _no_import_version("xarray")


def is_cupy(array):
    if 'cupy' not in sys.modules:
        return False
    from cupy import ndarray
    return isinstance(array, ndarray)


class XArrayInterface(GridInterface):

    types = ()

    datatype = 'xarray'

    @classmethod
    def loaded(cls):
        return 'xarray' in sys.modules

    @classmethod
    def applies(cls, obj):
        if not cls.loaded():
            return False
        import xarray as xr
        return isinstance(obj, (xr.Dataset, xr.DataArray))

    @classmethod
    def dimension_type(cls, dataset, dim):
        name = dataset.get_dimension(dim, strict=True).name
        if cls.packed(dataset) and name in dataset.vdims:
            return dataset.data.dtype.type
        return dataset.data[name].dtype.type

    @classmethod
    def dtype(cls, dataset, dim):
        name = dataset.get_dimension(dim, strict=True).name
        if cls.packed(dataset) and name in dataset.vdims:
            return dataset.data.dtype
        return dataset.data[name].dtype

    @classmethod
    def packed(cls, dataset):
        import xarray as xr
        return isinstance(dataset.data, xr.DataArray)

    @classmethod
    def shape(cls, dataset, gridded=False):
        if cls.packed(dataset):
            array = dataset.data[..., 0]
        else:
            array = dataset.data[dataset.vdims[0].name]
        if not gridded:
            return (np.prod(array.shape, dtype=np.intp), len(dataset.dimensions()))
        shape_map = dict(zip(array.dims, array.shape, strict=None))
        return tuple(shape_map.get(kd.name, np.nan) for kd in dataset.kdims[::-1])

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        import xarray as xr
        element_params = eltype.param.objects()
        kdim_param = element_params['kdims']
        vdim_param = element_params['vdims']

        def retrieve_unit_and_label(dim):
            if isinstance(dim, Dimension):
                return dim
            dim = asdim(dim)
            coord = data[dim.name]
            unit = coord.attrs.get('units') if dim.unit is None else dim.unit
            if isinstance(unit, tuple):
                unit = unit[0]
            if isinstance(coord.attrs.get("long_name"), str):
                spec = (dim.name, coord.attrs['long_name'])
            else:
                spec = (dim.name, dim.label)
            nodata = coord.attrs.get('NODATA')
            return dim.clone(spec, unit=unit, nodata=nodata)

        packed = False
        if isinstance(data, xr.DataArray):
            kdim_len = len(kdim_param.default) if kdims is None else len(kdims)
            vdim_len = len(vdim_param.default) if vdims is None else len(vdims)
            if kdim_len == len(data.dims)-1 and data.shape[-1] == vdim_len:
                packed = True
            elif vdims:
                vdim = vdims[0]
            elif data.name:
                vdim = Dimension(data.name)
                vdim.unit = data.attrs.get('units')
                vdim.nodata = data.attrs.get('NODATA')
                label = data.attrs.get('long_name')
                if isinstance(label, str):
                    vdim.label = label
            elif len(vdim_param.default) == 1:
                vdim = asdim(vdim_param.default[0])
                if vdim.name in data.dims:
                    raise DataError("xarray DataArray does not define a name, "
                                    f"and the default of '{vdim.name}' clashes with a "
                                    "coordinate dimension. Give the DataArray "
                                    "a name or supply an explicit value dimension.", cls)
            else:
                raise DataError("xarray DataArray does not define a name "
                                f"and {eltype.__name__} does not define a default value "
                                "dimension. Give the DataArray a name or "
                                "supply an explicit vdim.",
                                cls)
            if not packed:
                if vdim in data.dims:
                    data = data.to_dataset(vdim.name)
                    vdims = [asdim(vd) for vd in data.data_vars]
                else:
                    vdims = [vdim]
                    data = data.to_dataset(name=vdim.name)

        if isinstance(data, (xr.Dataset, xr.DataArray)):
            # Started to warn in xarray 2023.12.0:
            # The return type of `Dataset.dims` will be changed to return a
            # set of dimension names in future, in order to be more consistent
            # with `DataArray.dims`. To access a mapping from dimension names to
            # lengths, please use `Dataset.sizes`.
            data_info =  data.sizes if hasattr(data, "sizes") else data.dims
            if not data.coords:
                data = data.assign_coords(**{k: range(v) for k, v in data_info.items()})
            if vdims is None:
                vdims = list(data.data_vars)
            if kdims is None:
                xrdims = list(data_info)
                xrcoords = list(data.coords)
                kdims = [name for name in data.indexes.keys()
                         if isinstance(data[name].data, np.ndarray)]
                kdims = sorted(kdims, key=lambda x: (xrcoords.index(x) if x in xrcoords else float('inf'), x))
                if packed:
                    kdims = kdims[:-1]
                elif set(xrdims) != set(kdims):
                    virtual_dims = [xd for xd in xrdims if xd not in kdims]
                    for c in data.coords:
                        if c not in kdims and set(data[c].dims) == set(virtual_dims):
                            kdims.append(c)
            kdims = [retrieve_unit_and_label(kd) for kd in kdims]
            vdims = [retrieve_unit_and_label(vd) for vd in vdims]
        else:
            if kdims is None:
                kdims = kdim_param.default
            if vdims is None:
                vdims = vdim_param.default
            kdims = [asdim(kd) for kd in kdims]
            vdims = [asdim(vd) for vd in vdims]
            if isinstance(data, np.ndarray) and data.ndim == 2 and data.shape[1] == len(kdims+vdims):
                data = tuple(data)

            ndims = len(kdims)
            if isinstance(data, tuple):
                dimensions = [d.name for d in kdims+vdims]
                if (len(data) != len(dimensions) and len(data) == (ndims+1) and
                    len(data[-1].shape) == (ndims+1)):
                    value_array = data[-1]
                    data = {d: v for d, v in zip(dimensions, data[:-1], strict=None)}
                    packed = True
                else:
                    data = {d: v for d, v in zip(dimensions, data, strict=None)}
            elif isinstance(data, (list, np.ndarray)) and len(data) == 0:
                dimensions = [d.name for d in kdims + vdims]
                data = {d: np.array([]) for d in dimensions[:ndims]}
                data.update({d: np.empty((0,) * ndims) for d in dimensions[ndims:]})
            if not isinstance(data, dict):
                raise TypeError('XArrayInterface could not interpret data type')
            data = {d: np.asarray(values) if d in kdims else values
                    for d, values in data.items()}
            coord_dims = [data[kd.name].ndim for kd in kdims]
            dims = tuple(f'dim_{i}' for i in range(max(coord_dims)))[::-1]
            coords = {}
            for kd in kdims:
                coord_vals = data[kd.name]
                if coord_vals.ndim > 1:
                    coord = (dims[:coord_vals.ndim], coord_vals)
                else:
                    coord = coord_vals
                coords[kd.name] = coord
            xr_kwargs = {'dims': dims if max(coord_dims) > 1 else list(coords)[::-1]}
            if packed:
                xr_kwargs['dims'] = [*list(coords)[::-1], 'band']
                coords['band'] = list(range(len(vdims)))
                data = xr.DataArray(value_array, coords=coords, **xr_kwargs)
            else:
                arrays = {}
                for vdim in vdims:
                    arr = data[vdim.name]
                    if not isinstance(arr, xr.DataArray):
                        arr = xr.DataArray(arr, coords=coords, **xr_kwargs)
                    arrays[vdim.name] = arr
                data = xr.Dataset(arrays)

        not_found = []
        for d in kdims:
            if not any(d.name == k or (isinstance(v, xr.DataArray) and d.name in v.dims)
                       for k, v in data.coords.items()):
                not_found.append(d)
        if not isinstance(data, (xr.Dataset, xr.DataArray)):
            raise TypeError('Data must be be an xarray Dataset type.')
        elif not_found:
            raise DataError("xarray Dataset must define coordinates "
                            f"for all defined kdims, {not_found} coordinates not found.", cls)

        for vdim in vdims:
            if packed:
                continue
            da = data[vdim.name]
            # Do not enforce validation for irregular arrays since they
            # not need to be canonicalized
            if any(len(da.coords[c].shape) > 1 for c in da.coords):
                continue
            undeclared = []
            for c in da.coords:
                if c in kdims or len(da[c].shape) != 1 or da[c].shape[0] <= 1:
                    # Skip if coord is declared, represents irregular coordinates or is constant
                    continue
                elif all(d in kdims for d in da[c].dims):
                    continue # Skip if coord is alias for another dimension
                elif any(all(d in da[kd.name].dims for d in da[c].dims) for kd in kdims):
                    # Skip if all the dims on the coord are present on another coord
                    continue
                undeclared.append(c)
            if undeclared and eltype.param.kdims.bounds[1] not in (0, None):
                raise DataError(
                    f'The coordinates on the {vdim.name!r} DataArray do not match the '
                    'provided key dimensions (kdims). The following coords '
                    f'were left unspecified: {undeclared!r}. If you are requesting a '
                    'lower dimensional view such as a histogram cast '
                    'the xarray to a columnar format using the .to_dataframe '
                    'or .to_dask_dataframe methods before providing it to '
                    'HoloViews.')
        return data, {'kdims': kdims, 'vdims': vdims}, {}


    @classmethod
    def validate(cls, dataset, vdims=True):
        import xarray as xr
        if isinstance(dataset.data, xr.Dataset):
            Interface.validate(dataset, vdims)
        else:
            not_found = [kd.name for kd in dataset.kdims if kd.name not in dataset.data.coords]
            if not_found:
                raise DataError("Supplied data does not contain specified "
                                "dimensions, the following dimensions were "
                                f"not found: {not_found!r}", cls)

        # Check whether irregular (i.e. multi-dimensional) coordinate
        # array dimensionality matches
        irregular = []
        for kd in dataset.kdims:
            if cls.irregular(dataset, kd):
                irregular.append((kd, dataset.data[kd.name].dims))
        if irregular:
            nonmatching = [f'{kd}: {dims}' for kd, dims in irregular[1:]
                           if set(dims) != set(irregular[0][1])]
            if nonmatching:
                nonmatching = ['{}: {}'.format(*irregular[0]), *nonmatching]
                raise DataError("The dimensions of coordinate arrays "
                                "on irregular data must match. The "
                                "following kdims were found to have "
                                "non-matching array dimensions:\n\n{}".format('\n'.join(nonmatching)), cls)

    @classmethod
    def compute(cls, dataset):
        return dataset.clone(dataset.data.compute())

    @classmethod
    def persist(cls, dataset):
        return dataset.clone(dataset.data.persist())

    @classmethod
    def range(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        dim = dimension.name
        edges = dataset._binned and dimension in dataset.kdims
        if edges:
            data = cls.coords(dataset, dim, edges=True)
        else:
            if cls.packed(dataset) and dim in dataset.vdims:
                data = dataset.data.values[..., dataset.vdims.index(dim)]
            else:
                data = dataset.data[dim]
            if dimension.nodata is not None:
                data = cls.replace_value(data, dimension.nodata)

        if not len(data):
            dmin, dmax = np.nan, np.nan
        elif dtype_kind(data) == 'M' or not edges:
            dmin, dmax = data.min(), data.max()
            if not edges:
                dmin, dmax = dmin.data, dmax.data
        else:
            dmin, dmax = np.nanmin(data), np.nanmax(data)

        da = dask_array_module()
        if da and isinstance(dmin, da.Array):
            dmin, dmax = da.compute(dmin, dmax)
        if isinstance(dmin, np.ndarray) and dmin.shape == ():
            dmin = dmin[()]
        if isinstance(dmax, np.ndarray) and dmax.shape == ():
            dmax = dmax[()]
        dmin = dmin if np.isscalar(dmin) or isinstance(dmin, util.datetime_types) else dmin.item()
        dmax = dmax if np.isscalar(dmax) or isinstance(dmax, util.datetime_types) else dmax.item()
        return finite_range(data, dmin, dmax)


    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        index_dims = [dataset.get_dimension(d, strict=True) for d in dimensions]
        element_dims = [kdim for kdim in dataset.kdims
                        if kdim not in index_dims]

        invalid = [d for d in index_dims if dataset.data[d.name].ndim > 1]
        if invalid:
            if len(invalid) == 1: invalid = f"'{invalid[0]}'"
            raise ValueError(f"Cannot groupby irregularly sampled dimension(s) {invalid}.")

        group_kwargs = {}
        if group_type != 'raw' and issubclass(group_type, Element):
            group_kwargs = dict(util.get_param_values(dataset),
                                kdims=element_dims)
        group_kwargs.update(kwargs)

        drop_dim = any(d not in group_kwargs['kdims'] for d in element_dims)

        group_by = [d.name for d in index_dims]
        data = []
        if len(dimensions) == 1:
            for k, v in dataset.data.groupby(index_dims[0].name, squeeze=False):
                if drop_dim:
                    v = v.to_dataframe().reset_index()
                data.append((k, group_type(v, **group_kwargs)))
        else:
            unique_iters = [cls.values(dataset, d, False) for d in group_by]
            indexes = zip(*util.cartesian_product(unique_iters), strict=None)
            for k in indexes:
                sel = dataset.data.sel(**dict(zip(group_by, k, strict=None)))
                if drop_dim:
                    sel = sel.to_dataframe().reset_index()
                data.append((k, group_type(sel, **group_kwargs)))

        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(data, kdims=index_dims)
        else:
            return container_type(data)


    @classmethod
    def coords(cls, dataset, dimension, ordered=False, expanded=False, edges=False):
        import xarray as xr
        dim = dataset.get_dimension(dimension)
        dim = dimension if dim is None else dim.name
        irregular = cls.irregular(dataset, dim)
        if irregular or expanded:
            if irregular:
                data = dataset.data[dim]
            else:
                data = util.expand_grid_coords(dataset, dim)
            if edges:
                data = cls._infer_interval_breaks(data, axis=1)
                data = cls._infer_interval_breaks(data, axis=0)

            return data.values if isinstance(data, xr.DataArray) else data

        data = np.atleast_1d(dataset.data[dim].data)
        if ordered and data.shape and np.all(data[1:] < data[:-1]):
            data = data[::-1]
        shape = cls.shape(dataset, True)

        if dim in dataset.kdims:
            idx = dataset.get_dimension_index(dim)
            isedges = (len(shape) == dataset.ndims and len(data) == (shape[dataset.ndims-idx-1]+1))
        else:
            isedges = False
        if edges and not isedges:
            data = cls._infer_interval_breaks(data)
        elif not edges and isedges:
            data = np.convolve(data, [0.5, 0.5], 'valid')

        return data.values if isinstance(data, xr.DataArray) else data


    @classmethod
    def values(cls, dataset, dim, expanded=True, flat=True, compute=True, keep_index=False):
        dim = dataset.get_dimension(dim, strict=True)
        packed = cls.packed(dataset) and dim in dataset.vdims
        if packed:
            data = dataset.data.data[..., dataset.vdims.index(dim)]
        else:
            data = dataset.data[dim.name]
            if not keep_index:
                data = data.data
        irregular = cls.irregular(dataset, dim) if dim in dataset.kdims else False
        irregular_kdims = [d for d in dataset.kdims if cls.irregular(dataset, d)]
        if irregular_kdims:
            virtual_coords = list(dataset.data[irregular_kdims[0].name].coords.dims)
        else:
            virtual_coords = []
        if dim in dataset.vdims or irregular:
            if packed:
                data_coords = list(dataset.data.dims)[:-1]
            else:
                data_coords = list(dataset.data[dim.name].dims)
            da = dask_array_module()
            if compute and da and isinstance(data, da.Array):
                data = data.compute()
            if is_cupy(data):
                import cupy
                data = cupy.asnumpy(data)
            if not keep_index:
                data = cls.canonicalize(dataset, data, data_coords=data_coords,
                                        virtual_coords=virtual_coords)
            return data.T.flatten() if flat and not keep_index else data
        elif expanded:
            data = cls.coords(dataset, dim.name, expanded=True)
            return data.T.flatten() if flat else data
        else:
            if keep_index:
                return dataset.data[dim.name]
            return cls.coords(dataset, dim.name, ordered=True)


    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        reduce_dims = [d.name for d in dataset.kdims if d not in dimensions]
        return dataset.data.reduce(function, dim=reduce_dims, **kwargs), []


    @classmethod
    def unpack_scalar(cls, dataset, data):
        """Given a dataset object and data in the appropriate format for
        the interface, return a simple scalar.
        """
        if cls.packed(dataset):
            array = data.squeeze()
            if len(array.shape) == 0:
                return array.item()
        elif len(data.data_vars) == 1:
            array = data[dataset.vdims[0].name].squeeze()
            if len(array.shape) == 0:
                return array.item()
        return data


    @classmethod
    def ndloc(cls, dataset, indices):
        kdims = [d for d in dataset.kdims[::-1]]
        adjusted_indices = []
        slice_dims = []
        for kd, ind in zip(kdims, indices, strict=None):
            if cls.irregular(dataset, kd):
                coords = [c for c in dataset.data.coords if c not in dataset.data.dims]
                dim = dataset.data[kd.name].dims[coords.index(kd.name)]
                shape = dataset.data[kd.name].shape[coords.index(kd.name)]
                coords = np.arange(shape)
            else:
                coords = cls.coords(dataset, kd, False)
                dim = kd.name
            slice_dims.append(dim)
            ncoords = len(coords)
            if np.all(coords[1:] < coords[:-1]):
                if np.isscalar(ind):
                    ind = ncoords-ind-1
                elif isinstance(ind, slice):
                    start = None if ind.stop is None else ncoords-ind.stop
                    stop = None if ind.start is None else ncoords-ind.start
                    ind = slice(start, stop, ind.step)
                elif isinstance(ind, np.ndarray) and dtype_kind(ind) == 'b':
                    ind = ind[::-1]
                elif isinstance(ind, (np.ndarray, list)):
                    ind = [ncoords-i-1 for i in ind]
            if isinstance(ind, list):
                ind = np.array(ind)
            if isinstance(ind, np.ndarray) and dtype_kind(ind) == 'b':
                ind = np.where(ind)[0]
            adjusted_indices.append(ind)

        isel = dict(zip(slice_dims, adjusted_indices, strict=None))
        all_scalar = all(map(np.isscalar, indices))
        if all_scalar and len(indices) == len(kdims) and len(dataset.vdims) == 1:
            return dataset.data[dataset.vdims[0].name].isel(**isel).values.item()

        # Detect if the indexing is selecting samples or slicing the array
        sampled = (all(isinstance(ind, np.ndarray) and dtype_kind(ind) != 'b'
                       for ind in adjusted_indices) and len(indices) == len(kdims))
        if sampled or (all_scalar and len(indices) == len(kdims)):
            import xarray as xr
            if cls.packed(dataset):
                selected = dataset.data.isel({k: xr.DataArray(v) for k, v in isel.items()})
                df = selected.to_dataframe('vdims')[['vdims']].T
                vdims = [vd.name for vd in dataset.vdims]
                return df.rename(columns={i: d for i, d in enumerate(vdims)})[vdims]
            if all_scalar: isel = {k: [v] for k, v in isel.items()}
            selected = dataset.data.isel({k: xr.DataArray(v) for k, v in isel.items()})
            return selected.to_dataframe().reset_index()
        else:
            return dataset.data.isel(**isel)

    @classmethod
    def concat_dim(cls, datasets, dim, vdims):
        import xarray as xr
        concat_kwargs = {"dim": dim.name}
        if XARRAY_VERSION >= (2025, 8, 0):
            concat_kwargs["join"] = "outer"
        return xr.concat(
            [ds.assign_coords(**{dim.name: c}) for c, ds in datasets.items()],
            **concat_kwargs
        )

    @classmethod
    def redim(cls, dataset, dimensions):
        renames = {k: v.name for k, v in dimensions.items()}
        return dataset.data.rename(renames)

    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        dropped_kdims = [kd for kd in dataset.kdims if kd not in kdims]
        constant = {}
        for kd in dropped_kdims:
            vals = cls.values(dataset, kd.name, expanded=False)
            if len(vals) == 1:
                constant[kd.name] = vals[0]
        if len(constant) == len(dropped_kdims):
            dropped = dataset.data.sel(**{k: v for k, v in constant.items()
                                          if k in dataset.data.dims})
            if vdims and cls.packed(dataset):
                return dropped.isel(**{dataset.data.dims[-1]: [dataset.vdims.index(vd) for vd in vdims]})
            return dropped
        elif dropped_kdims:
            return tuple(dataset.columns(kdims+vdims).values())
        return dataset.data

    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        return dataset

    @classmethod
    def mask(cls, dataset, mask, mask_val=np.nan):
        packed = cls.packed(dataset)
        masked = dataset.data.copy()
        if packed:
            data_coords = list(dataset.data.dims)[:-1]
            mask = cls.canonicalize(dataset, mask, data_coords)
            try:
                masked.values[mask] = mask_val
            except ValueError:
                masked = masked.astype('float')
                masked.values[mask] = mask_val
        else:
            orig_mask = mask
            for vd in dataset.vdims:
                dims = list(dataset.data[vd.name].dims)
                if any(cls.irregular(dataset, kd) for kd in dataset.kdims):
                    data_coords = dims
                else:
                    data_coords = [kd.name for kd in dataset.kdims][::-1]
                mask = cls.canonicalize(dataset, orig_mask, data_coords)
                if data_coords != dims:
                    inds = [dims.index(d) for d in data_coords]
                    mask = mask.transpose(inds)
                masked[vd.name] = marr = masked[vd.name].astype('float')
                marr.values[mask] = mask_val
        return masked

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        if selection_mask is not None:
            return dataset.data.where(selection_mask, drop=True)

        validated = {}
        for k, v in selection.items():
            dim = dataset.get_dimension(k, strict=True)
            if cls.irregular(dataset, dim):
                return GridInterface.select(dataset, selection_mask, **selection)
            dim = dim.name
            if isinstance(v, slice):
                v = (v.start, v.stop)
            if isinstance(v, set):
                validated[dim] = list(v)
            elif isinstance(v, tuple):
                dim_vals = dataset.data[k].values
                upper = None if v[1] is None else v[1]-sys.float_info.epsilon*10
                v = v[0], upper
                if dtype_kind(dim_vals) not in 'OSU' and np.all(dim_vals[1:] < dim_vals[:-1]):
                    # If coordinates are inverted invert slice
                    v = v[::-1]
                validated[dim] = slice(*v)
            elif isinstance(v, types.FunctionType):
                validated[dim] = v(dataset[k])
            else:
                validated[dim] = v
        data = dataset.data.sel(**validated)

        # Restore constant dimensions
        indexed = cls.indexed(dataset, selection)
        dropped = dict((d.name, np.atleast_1d(data[d.name]))
                   for d in dataset.kdims
                   if not data[d.name].data.shape)
        if dropped and not indexed:
            data = data.expand_dims(dropped)
            # see https://github.com/pydata/xarray/issues/2891
            # since we only expanded on dimensions of size 1
            # we can monkeypatch the dataarray back to writeable.
            for d in data.values():
                if hasattr(d.data, 'flags'):
                    d.data.flags.writeable = True

        da = dask_array_module()
        if (indexed and len(data.data_vars) == 1 and
            len(data[dataset.vdims[0].name].shape) == 0):
            value = data[dataset.vdims[0].name]
            if da and isinstance(value.data, da.Array):
                value = value.compute()
            return value.item()
        elif indexed:
            values = []
            for vd in dataset.vdims:
                value = data[vd.name]
                if da and isinstance(value.data, da.Array):
                    value = value.compute()
                values.append(value.item())
            return np.array(values)
        return data

    @classmethod
    def length(cls, dataset):
        return np.prod([len(dataset.data[d.name]) for d in dataset.kdims], dtype=np.intp)

    @classmethod
    def dframe(cls, dataset, dimensions):
        import xarray as xr
        if cls.packed(dataset):
            bands = {vd.name: dataset.data[..., i].drop_vars('band')
                     for i, vd in enumerate(dataset.vdims)}
            data = xr.Dataset(bands)
        else:
            data = dataset.data
        data = data.to_dataframe().reset_index()
        if dimensions:
            return data[dimensions]
        return data

    @classmethod
    def sample(cls, dataset, samples=None):
        import pandas as pd

        if samples is None:
            samples = []
        names = [kd.name for kd in dataset.kdims]
        samples = [dataset.data.sel(**{k: [v] for k, v in zip(names, s, strict=None)}).to_dataframe().reset_index()
                   for s in samples]
        return pd.concat(samples)

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        import xarray as xr
        if not vdim:
            raise Exception("Cannot add key dimension to a dense representation.")
        dim = dimension_name(dimension)
        coords = {d.name: cls.coords(dataset, d.name) for d in dataset.kdims}
        arr = xr.DataArray(values, coords=coords, name=dim,
                           dims=tuple(d.name for d in dataset.kdims[::-1]))
        return dataset.data.assign(**{dim: arr})

    @classmethod
    def assign(cls, dataset, new_data):
        import xarray as xr
        data = dataset.data
        prev_coords = set.intersection(*[
            set(var.coords) for var in data.data_vars.values()
        ])
        coords = {}
        for k, v in new_data.items():
            if k not in dataset.kdims:
                continue
            elif isinstance(v, xr.DataArray):
                coords[k] = v.rename(**({v.name: k} if v.name != k else {}))
                continue
            coord_vals = cls.coords(dataset, k)
            if not coord_vals.ndim > 1 and np.all(coord_vals[1:] < coord_vals[:-1]):
                v = v[::-1]
            coords[k] = (k, v)
        if coords:
            data = data.assign_coords(**coords)

        dims = tuple(kd.name for kd in dataset.kdims[::-1])
        vars = {}
        for k, v in new_data.items():
            if k in dataset.kdims:
                continue
            if isinstance(v, xr.DataArray):
                vars[k] = v
            else:
                vars[k] = (dims, cls.canonicalize(dataset, v, data_coords=dims))

        if len(vars) == 1 and list(vars) == [vd.name for vd in dataset.vdims]:
            data = vars[dataset.vdims[0].name]
            used_coords = set(data.coords)
        else:
            if vars:
                data = data.assign(vars)
            used_coords = set.intersection(*[set(var.coords) for var in data.data_vars.values()])
        drop_coords =  set.symmetric_difference(used_coords, prev_coords)
        return data.drop_vars([c for c in drop_coords if c in data.coords]), list(drop_coords)


Interface.register(XArrayInterface)
