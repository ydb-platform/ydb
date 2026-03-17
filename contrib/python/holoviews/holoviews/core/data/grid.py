from collections import defaultdict

import numpy as np

from .. import util
from ..dimension import dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind
from .dictionary import DictInterface
from .interface import DataError, Interface
from .util import dask_array_module, finite_range, get_array_types, is_dask


class GridInterface(DictInterface):
    """Interface for simple dictionary-based dataset format using a
    compressed representation that uses the cartesian product between
    key dimensions. As with DictInterface, the dictionary keys correspond
    to the column (i.e. dimension) names and the values are NumPy arrays
    representing the values in that column.

    To use this compressed format, the key dimensions must be orthogonal
    to one another with each key dimension specifying an axis of the
    multidimensional space occupied by the value dimension data. For
    instance, given an temperature recordings sampled regularly across
    the earth surface, a list of N unique latitudes and M unique
    longitudes can specify the position of NxM temperature samples.

    """

    types = (dict,)

    datatype = 'grid'

    gridded = True

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        if kdims is None:
            kdims = eltype.kdims
        if vdims is None:
            vdims = eltype.vdims

        if not vdims:
            raise ValueError('GridInterface interface requires at least '
                             'one value dimension.')

        ndims = len(kdims)
        dimensions = [dimension_name(d) for d in kdims+vdims]
        vdim_tuple = tuple(dimension_name(vd) for vd in vdims)

        if isinstance(data, tuple):
            if (len(data) != len(dimensions) and len(data) == (ndims+1) and
                len(data[-1].shape) == (ndims+1)):
                value_array = data[-1]
                data = {d: v for d, v in zip(dimensions, data[:-1], strict=None)}
                data[vdim_tuple] = value_array
            else:
                data = {d: v for d, v in zip(dimensions, data, strict=None)}
        elif (isinstance(data, list) and data == []):
            if len(kdims) == 1:
                data = dict([(d, []) for d in dimensions])
            else:
                data = dict([(d.name, np.array([])) for d in kdims])
                if len(vdims) == 1:
                    data[vdims[0].name] = np.zeros((0, 0))
                else:
                    data[vdim_tuple] = np.zeros((0, 0, len(vdims)))
        elif not any(isinstance(data, tuple(t for t in interface.types if t is not None))
                     for interface in cls.interfaces.values()):
            data = {k: v for k, v in zip(dimensions, zip(*data, strict=None), strict=None)}
        elif isinstance(data, np.ndarray):
            if data.shape == (0, 0) and len(vdims) == 1:
                array = data
                data = dict([(d.name, np.array([])) for d in kdims])
                data[vdims[0].name] = array
            elif data.shape == (0, 0, len(vdims)):
                array = data
                data = dict([(d.name, np.array([])) for d in kdims])
                data[vdim_tuple] = array
            else:
                if data.ndim == 1:
                    if eltype._auto_indexable_1d and len(kdims)+len(vdims)>1:
                        data = np.column_stack([np.arange(len(data)), data])
                    else:
                        data = np.atleast_2d(data).T
                data = {k: data[:, i] for i, k in enumerate(dimensions)}
        elif isinstance(data, list) and data == []:
            data = {d: np.array([]) for d in dimensions[:ndims]}
            data.update({d: np.empty((0,) * ndims) for d in dimensions[ndims:]})
        elif not isinstance(data, dict):
            raise TypeError('GridInterface must be instantiated as a '
                            'dictionary or tuple')

        validate_dims = list(kdims)
        if vdim_tuple in data:
            if not isinstance(data[vdim_tuple], get_array_types()):
                data[vdim_tuple] = np.array(data[vdim_tuple])
        else:
            validate_dims += vdims

        for dim in validate_dims:
            name = dimension_name(dim)
            if name not in data:
                raise ValueError(f"Values for dimension {dim} not found")
            if not isinstance(data[name], get_array_types()):
                data[name] = np.array(data[name])

        kdim_names = [dimension_name(d) for d in kdims]
        if vdim_tuple in data:
            vdim_names = [vdim_tuple]
        else:
            vdim_names = [dimension_name(d) for d in vdims]

        expected = tuple([len(data[kd]) for kd in kdim_names])
        irregular_shape = data[kdim_names[0]].shape if kdim_names else ()
        valid_shape = irregular_shape if len(irregular_shape) > 1 else expected[::-1]
        shapes = tuple([data[kd].shape for kd in kdim_names])
        for vdim in vdim_names:
            shape = data[vdim].shape
            error = DataError if len(shape) > 1 else ValueError
            if vdim_tuple in data:
                if shape[-1] != len(vdims):
                    raise error('The shape of the value array does not match the number of value dimensions.')
                shape = shape[:-1]
            if (not expected and shape == (1,)) or (len(shape) > 1 and len({shape, *shapes}) == 1):
                # If empty or an irregular mesh
                pass
            elif len(shape) != len(expected):
                raise error(f'The shape of the {vdim} value array does not '
                            'match the expected dimensionality indicated '
                            f'by the key dimensions. Expected {len(expected)}-D array, '
                            f'found {len(shape)}-D array.')
            elif any((e not in (s, s + 1)) for s, e in zip(shape, valid_shape, strict=None)):
                raise error(f'Key dimension values and value array {vdim} '
                            f'shapes do not match. Expected shape {valid_shape}, '
                            f'actual shape: {shape}', cls)
        return data, {'kdims':kdims, 'vdims':vdims}, {}


    @classmethod
    def concat(cls, datasets, dimensions, vdims):
        from . import Dataset
        with sorted_context(False):
            datasets = NdMapping(datasets, kdims=dimensions)
            datasets = datasets.clone([(k, v.data if isinstance(v, Dataset) else v)
                                       for k, v in datasets.data.items()])
        if len(datasets.kdims) > 1:
            items = datasets.groupby(datasets.kdims[:-1]).data.items()
            return cls.concat([(k, cls.concat(v, v.kdims, vdims=vdims)) for k, v in items],
                              datasets.kdims[:-1], vdims)
        return cls.concat_dim(datasets, datasets.kdims[0], vdims)


    @classmethod
    def concat_dim(cls, datasets, dim, vdims):
        values, grids = zip(*datasets.items(), strict=None)
        new_data = {k: v for k, v in grids[0].items() if k not in vdims}
        new_data[dim.name] = np.array(values)
        for vdim in vdims:
            arrays = [grid[vdim.name] for grid in grids]
            shapes = {arr.shape for arr in arrays}
            if len(shapes) > 1:
                raise DataError('When concatenating gridded data the shape '
                                f'of arrays must match. {cls.__name__} found that arrays '
                                f'along the {vdim.name} dimension do not match.')
            stack = dask_array_module().stack if any(is_dask(arr) for arr in arrays) else np.stack
            new_data[vdim.name] = stack(arrays, -1)
        return new_data


    @classmethod
    def irregular(cls, dataset, dim):
        return dataset.data[dimension_name(dim)].ndim > 1


    @classmethod
    def isscalar(cls, dataset, dim):
        values = cls.values(dataset, dim, expanded=False)
        return values.shape in ((), (1,)) or len(np.unique(values)) == 1


    @classmethod
    def validate(cls, dataset, vdims=True):
        dims = 'all' if vdims else 'key'
        not_found = [d for d in dataset.dimensions(dims, label='name')
                     if d not in dataset.data]
        if not_found and tuple(not_found) not in dataset.data:
            raise DataError("Supplied data does not contain specified "
                            "dimensions, the following dimensions were "
                            f"not found: {not_found!r}", cls)


    @classmethod
    def dimension_type(cls, dataset, dim):
        if dim in dataset.dimensions():
            arr = cls.values(dataset, dim, False, False)
        else:
            return None
        return arr.dtype.type


    @classmethod
    def packed(cls, dataset):
        vdim_tuple = tuple(vd.name for vd in dataset.vdims)
        return vdim_tuple if vdim_tuple in dataset.data else False


    @classmethod
    def dtype(cls, dataset, dimension):
        name = dataset.get_dimension(dimension, strict=True).name
        vdim_tuple = cls.packed(dataset)
        if vdim_tuple and name in vdim_tuple:
            data = dataset.data[vdim_tuple][..., vdim_tuple.index(name)]
        else:
            data = dataset.data[name]
        if util.isscalar(data):
            return np.array([data]).dtype
        else:
            return data.dtype


    @classmethod
    def shape(cls, dataset, gridded=False):
        vdim_tuple = cls.packed(dataset)
        if vdim_tuple:
            shape = dataset.data[vdim_tuple].shape[:-1]
        else:
            shape = dataset.data[dataset.vdims[0].name].shape
        if gridded:
            return shape
        else:
            return (np.prod(shape, dtype=np.intp), len(dataset.dimensions()))


    @classmethod
    def length(cls, dataset):
        return cls.shape(dataset)[0]


    @classmethod
    def _infer_interval_breaks(cls, coord, axis=0):
        """
        >>> GridInterface._infer_interval_breaks(np.arange(5))
        array([-0.5,  0.5,  1.5,  2.5,  3.5,  4.5])
        >>> GridInterface._infer_interval_breaks([[0, 1], [3, 4]], axis=1)
        array([[-0.5,  0.5,  1.5],
               [ 2.5,  3.5,  4.5]])

        """
        coord = np.asarray(coord)
        if coord.shape[axis] == 0:
            return np.array([], dtype=coord.dtype)
        if coord.shape[axis] > 1:
            deltas = 0.5 * np.diff(coord, axis=axis)
        else:
            deltas = np.array([0.5])
        first = np.take(coord, [0], axis=axis) - np.take(deltas, [0], axis=axis)
        last = np.take(coord, [-1], axis=axis) + np.take(deltas, [-1], axis=axis)
        trim_last = tuple(slice(None, -1) if n == axis else slice(None)
                          for n in range(coord.ndim))
        return np.concatenate([first, coord[trim_last] + deltas, last], axis=axis)


    @classmethod
    def coords(cls, dataset, dim, ordered=False, expanded=False, edges=False):
        """Returns the coordinates along a dimension.  Ordered ensures
        coordinates are in ascending order and expanded creates
        ND-array matching the dimensionality of the dataset.

        """
        dim = dataset.get_dimension(dim, strict=True)
        irregular = cls.irregular(dataset, dim)
        if irregular or expanded:
            if irregular:
                data = dataset.data[dim.name]
            else:
                data = util.expand_grid_coords(dataset, dim)
            if edges and data.shape == dataset.data[dataset.vdims[0].name].shape:
                data = cls._infer_interval_breaks(data, axis=1)
                data = cls._infer_interval_breaks(data, axis=0)
            return data

        data = dataset.data[dim.name]
        if ordered and np.all(data[1:] < data[:-1]):
            data = data[::-1]
        shape = cls.shape(dataset, True)
        if dim in dataset.kdims:
            idx = dataset.get_dimension_index(dim)
            isedges = (dim in dataset.kdims and len(shape) == dataset.ndims
                       and len(data) == (shape[dataset.ndims-idx-1]+1))
        else:
            isedges = False
        if edges and not isedges:
            data = cls._infer_interval_breaks(data)
        elif not edges and isedges:
            data = data[:-1] + np.diff(data)/2.
        return data


    @classmethod
    def canonicalize(cls, dataset, data, data_coords=None, virtual_coords=None):
        """Canonicalize takes an array of values as input and reorients
        and transposes it to match the canonical format expected by
        plotting functions. In certain cases the dimensions defined
        via the kdims of an Element may not match the dimensions of
        the underlying data. A set of data_coords may be passed in to
        define the dimensionality of the data, which can then be used
        to np.squeeze the data to remove any constant dimensions. If
        the data is also irregular, i.e. contains multi-dimensional
        coordinates, a set of virtual_coords can be supplied, required
        by some interfaces (e.g. xarray) to index irregular datasets
        with a virtual integer index. This ensures these coordinates
        are not simply dropped.

        """
        if virtual_coords is None:
            virtual_coords = []
        if data_coords is None:
            data_coords = dataset.dimensions('key', label='name')[::-1]

        # Transpose data
        dims = [name for name in data_coords
                if isinstance(cls.coords(dataset, name), get_array_types())]
        dropped = [dims.index(d) for d in dims
                   if d not in dataset.kdims+virtual_coords]
        if dropped:
            if len(dropped) == data.ndim:
                data = data.flatten()
            else:
                data = np.squeeze(data, axis=tuple(dropped))

        if not any(cls.irregular(dataset, d) for d in dataset.kdims):
            inds = [dims.index(kd.name) for kd in dataset.kdims]
            inds = [i - sum([1 for d in dropped if i>=d]) for i in inds]
            if inds:
                data = data.transpose(inds[::-1])

        # Reorient data
        invert = False
        slices = []
        for d in dataset.kdims[::-1]:
            coords = cls.coords(dataset, d)
            if np.all(coords[1:] < coords[:-1]) and not coords.ndim > 1:
                slices.append(slice(None, None, -1))
                invert = True
            else:
                slices.append(slice(None))
        data = data[tuple(slices)] if invert else data

        # Allow lower dimensional views into data
        if len(dataset.kdims) < 2:
            data = data.flatten()
        return data


    @classmethod
    def invert_index(cls, index, length):
        if np.isscalar(index):
            return length - index
        elif isinstance(index, slice):
            start, stop = index.start, index.stop
            new_start, new_stop = None, None
            if start is not None:
                new_stop = length - start
            if stop is not None:
                new_start = length - stop
            return slice(new_start-1, new_stop-1)
        elif isinstance(index, util.Iterable):
            new_index = []
            for ind in index:
                new_index.append(length-ind)
        return new_index


    @classmethod
    def ndloc(cls, dataset, indices):
        selected = {}
        adjusted_inds = []
        all_scalar = True
        for kd, ind in zip(dataset.kdims[::-1], indices, strict=None):
            coords = cls.coords(dataset, kd.name, True)
            if np.isscalar(ind):
                ind = [ind]
            else:
                all_scalar = False
            selected[kd.name] = coords[ind]
            adjusted_inds.append(ind)
        for kd in dataset.kdims:
            if kd.name not in selected:
                coords = cls.coords(dataset, kd.name)
                selected[kd.name] = coords
                all_scalar = False
        for d in dataset.dimensions():
            if d in dataset.kdims and not cls.irregular(dataset, d):
                continue
            arr = cls.values(dataset, d, flat=False, compute=False)
            if all_scalar and len(dataset.vdims) == 1:
                return arr[tuple(ind[0] for ind in adjusted_inds)]
            selected[d.name] = arr[tuple(adjusted_inds)]
        return tuple(selected[d.name] for d in dataset.dimensions())

    @classmethod
    def persist(cls, dataset):
        da = dask_array_module()
        return {k: v.persist() if da and isinstance(v, da.Array) else v
                for k, v in dataset.data.items()}

    @classmethod
    def compute(cls, dataset):
        da = dask_array_module()
        return {k: v.compute() if da and isinstance(v, da.Array) else v
                for k, v in dataset.data.items()}

    @classmethod
    def values(cls, dataset, dim, expanded=True, flat=True, compute=True,
               keep_index=False, canonicalize=True):
        dim = dataset.get_dimension(dim, strict=True)
        if dim in dataset.vdims or dataset.data[dim.name].ndim > 1:
            vdim_tuple = cls.packed(dataset)
            if vdim_tuple:
                data = dataset.data[vdim_tuple][..., dataset.vdims.index(dim)]
            else:
                data = dataset.data[dim.name]
            if canonicalize:
                data = cls.canonicalize(dataset, data)
            da = dask_array_module()
            if compute and da and isinstance(data, da.Array):
                data = data.compute()
            return data.T.flatten() if flat else data
        elif expanded:
            data = cls.coords(dataset, dim.name, expanded=True, ordered=canonicalize)
            return data.T.flatten() if flat else data
        else:
            return cls.coords(dataset, dim.name, ordered=canonicalize)


    @classmethod
    def groupby(cls, dataset, dim_names, container_type, group_type, **kwargs):
        # Get dimensions information
        dimensions = [dataset.get_dimension(d, strict=True) for d in dim_names]
        if 'kdims' in kwargs:
            kdims = kwargs['kdims']
        else:
            kdims = [kdim for kdim in dataset.kdims if kdim not in dimensions]
            kwargs['kdims'] = kdims

        invalid = [d for d in dimensions if dataset.data[d.name].ndim > 1]
        if invalid:
            if len(invalid) == 1: invalid = f"'{invalid[0]}'"
            raise ValueError(f"Cannot groupby irregularly sampled dimension(s) {invalid}.")

        # Update the kwargs appropriately for Element group types
        group_kwargs = {}
        group_type = dict if group_type == 'raw' else group_type
        if issubclass(group_type, Element):
            group_kwargs.update(util.get_param_values(dataset))
        else:
            kwargs.pop('kdims')
        group_kwargs.update(kwargs)

        drop_dim = any(d not in group_kwargs['kdims'] for d in kdims)

        # Find all the keys along supplied dimensions
        keys = [cls.coords(dataset, d.name) for d in dimensions]
        transpose = [dataset.ndims-dataset.kdims.index(kd)-1 for kd in kdims]
        transpose += [i for i in range(dataset.ndims) if i not in transpose]

        # Iterate over the unique entries applying selection masks
        grouped_data = []
        for unique_key in zip(*util.cartesian_product(keys), strict=None):
            select = dict(zip(dim_names, unique_key, strict=None))
            if drop_dim:
                group_data = dataset.select(**select)
                group_data = group_data if np.isscalar(group_data) else group_data.columns()
            else:
                group_data = cls.select(dataset, **select)

            if np.isscalar(group_data) or (isinstance(group_data, get_array_types()) and group_data.shape == ()):
                group_data = {dataset.vdims[0].name: np.atleast_1d(group_data)}
                for dim, v in zip(dim_names, unique_key, strict=None):
                    group_data[dim] = np.atleast_1d(v)
            elif not drop_dim:
                if isinstance(group_data, get_array_types()):
                    group_data = {dataset.vdims[0].name: group_data}
                for vdim in dataset.vdims:
                    data = group_data[vdim.name]
                    data = data.transpose(transpose[::-1])
                    group_data[vdim.name] = np.squeeze(data)
            group_data = group_type(group_data, **group_kwargs)
            grouped_data.append((tuple(unique_key), group_data))

        if issubclass(container_type, NdMapping):
            with item_check(False):
                return container_type(grouped_data, kdims=dimensions)
        else:
            return container_type(grouped_data)


    @classmethod
    def key_select_mask(cls, dataset, values, ind):
        if dtype_kind(values) == 'M':
            ind = util.parse_datetime_selection(ind)
        if isinstance(ind, tuple):
            ind = slice(*ind)
        if isinstance(ind, get_array_types()):
            mask = ind
        elif isinstance(ind, slice):
            mask = True
            if ind.start is not None:
                mask &= ind.start <= values
            if ind.stop is not None:
                mask &= values < ind.stop
            # Expand empty mask
            if mask is True:
                mask = np.ones(values.shape, dtype=np.bool_)
        elif isinstance(ind, (set, list)):
            iter_slcs = []
            for ik in ind:
                iter_slcs.append(values == ik)
            mask = np.logical_or.reduce(iter_slcs)
        elif callable(ind):
            mask = ind(values)
        elif ind is None:
            mask = None
        else:
            index_mask = values == ind
            if (dataset.ndims == 1 or dataset._binned) and np.sum(index_mask) == 0:
                data_index = np.argmin(np.abs(values - ind))
                mask = np.zeros(len(values), dtype=np.bool_)
                mask[data_index] = True
            else:
                mask = index_mask
        if mask is None:
            mask = np.ones(values.shape, dtype=bool)
        return mask


    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        if selection_mask is not None:
            raise ValueError(f"Masked selections currently not supported for {cls.__name__}.")

        dimensions = dataset.kdims
        val_dims = [vdim for vdim in dataset.vdims if vdim in selection]
        if val_dims:
            raise IndexError('Cannot slice value dimensions in compressed format, '
                             'convert to expanded format before slicing.')

        indexed = cls.indexed(dataset, selection)
        full_selection = [(d, selection.get(d.name, selection.get(d.label)))
                          for d in dimensions]
        data = {}
        value_select = []
        for i, (dim, ind) in enumerate(full_selection):
            irregular = cls.irregular(dataset, dim)
            values = cls.coords(dataset, dim, irregular)
            mask = cls.key_select_mask(dataset, values, ind)
            if irregular:
                if np.isscalar(ind) or isinstance(ind, (set, list)):
                    raise IndexError("Indexing not supported for irregularly "
                                     f"sampled data. {ind} value along {dim} dimension."
                                     "must be a slice or 2D boolean mask.")
                mask = mask.max(axis=i)
            elif dataset._binned:
                edges = cls.coords(dataset, dim, False, edges=True)
                inds = np.argwhere(mask)
                if np.isscalar(ind):
                    emin, emax = edges.min(), edges.max()
                    if ind < emin:
                        raise IndexError(f"Index {ind} less than lower bound "
                                         f"of {emin} for {dim} dimension.")
                    elif ind >= emax:
                        raise IndexError(f"Index {ind} more than or equal to upper bound "
                                         f"of {emax} for {dim} dimension.")
                    idx = max([np.digitize([ind], edges)[0]-1, 0])
                    mask = np.zeros(len(values), dtype=np.bool_)
                    mask[idx] = True
                    values = edges[idx:idx+2]
                elif len(inds):
                    values = edges[inds.min(): inds.max()+2]
                else:
                    values = edges[0:0]
            else:
                values = values[mask]
            values, mask = np.asarray(values), np.asarray(mask)
            value_select.append(mask)
            data[dim.name] = np.array([values]) if np.isscalar(values) else values

        int_inds = [np.argwhere(v) for v in value_select][::-1]
        index = np.ix_(*[np.atleast_1d(np.squeeze(ind)) if ind.ndim > 1 else np.atleast_1d(ind)
                         for ind in int_inds])

        for kdim in dataset.kdims:
            if cls.irregular(dataset, dim):
                da = dask_array_module()
                if da and isinstance(dataset.data[kdim.name], da.Array):
                    data[kdim.name] = dataset.data[kdim.name].vindex[index]
                else:
                    data[kdim.name] = np.asarray(data[kdim.name])[index]

        for vdim in dataset.vdims:
            da = dask_array_module()
            if da and isinstance(dataset.data[vdim.name], da.Array):
                data[vdim.name] = dataset.data[vdim.name].vindex[index]
            else:
                data[vdim.name] = np.asarray(dataset.data[vdim.name])[index]

        if indexed:
            if len(dataset.vdims) == 1:
                da = dask_array_module()
                arr = np.squeeze(data[dataset.vdims[0].name])
                if da and isinstance(arr, da.Array):
                    arr = arr.compute()
                return arr if np.isscalar(arr) else arr[()]
            else:
                return np.array([np.squeeze(data[vd.name])
                                 for vd in dataset.vdims])
        return data


    @classmethod
    def mask(cls, dataset, mask, mask_val=np.nan):
        mask = cls.canonicalize(dataset, mask)
        packed = cls.packed(dataset)
        masked = dict(dataset.data)
        if packed:
            masked = dataset.data[packed].copy()
            try:
                masked[mask] = mask_val
            except ValueError:
                masked = masked.astype('float')
                masked[mask] = mask_val
        else:
            for vd in dataset.vdims:
                masked[vd.name] = marr = masked[vd.name].copy()
                try:
                    marr[mask] = mask_val
                except ValueError:
                    masked[vd.name] = marr = marr.astype('float')
                    marr[mask] = mask_val
        return masked


    @classmethod
    def sample(cls, dataset, samples=None):
        """Samples the gridded data into dataset of samples.

        """
        if samples is None:
            samples = []
        ndims = dataset.ndims
        dimensions = dataset.dimensions(label='name')
        arrays = [dataset.data[vdim.name] for vdim in dataset.vdims]
        data = defaultdict(list)

        for sample in samples:
            if np.isscalar(sample): sample = [sample]
            if len(sample) != ndims:
                sample = [sample[i] if i < len(sample) else None
                          for i in range(ndims)]
            sampled, int_inds = [], []
            for d, ind in zip(dimensions, sample, strict=None):
                cdata = dataset.data[d]
                mask = cls.key_select_mask(dataset, cdata, ind)
                inds = np.arange(len(cdata)) if mask is None else np.argwhere(mask)
                int_inds.append(inds)
                sampled.append(cdata[mask])
            for d, arr in zip(dimensions, np.meshgrid(*sampled), strict=None):
                data[d].append(arr)
            for vdim, array in zip(dataset.vdims, arrays, strict=None):
                da = dask_array_module()
                flat_index = np.ravel_multi_index(tuple(int_inds)[::-1], array.shape)
                if da and isinstance(array, da.Array):
                    data[vdim.name].append(array.flatten().vindex[tuple(flat_index)])
                else:
                    data[vdim.name].append(array.flat[flat_index])
        concatenated = {d: np.concatenate(arrays).flatten() for d, arrays in data.items()}
        return concatenated


    @classmethod
    def aggregate(cls, dataset, kdims, function, **kwargs):
        kdims = [dimension_name(kd) for kd in kdims]
        data = {kdim: dataset.data[kdim] for kdim in kdims}
        axes = tuple(dataset.ndims-dataset.get_dimension_index(kdim)-1
                     for kdim in dataset.kdims if kdim not in kdims)
        da = dask_array_module()
        dropped = []
        vdim_tuple = cls.packed(dataset)
        if vdim_tuple:
            values = dataset.data[vdim_tuple]
            if axes:
                data[vdim_tuple] = function(values, axis=axes, **kwargs)
            else:
                data[vdim_tuple] = values
        else:
            for vdim in dataset.vdims:
                values = dataset.data[vdim.name]
                atleast_1d = da.atleast_1d if is_dask(values) else np.atleast_1d
                try:
                    data[vdim.name] = atleast_1d(function(values, axis=axes, **kwargs))
                except TypeError:
                    dropped.append(vdim)
        return data, dropped


    @classmethod
    def reindex(cls, dataset, kdims, vdims):
        dropped_kdims = [kd for kd in dataset.kdims if kd not in kdims]
        dropped_vdims = ([vdim for vdim in dataset.vdims
                          if vdim not in vdims] if vdims else [])
        constant = {}
        for kd in dropped_kdims:
            vals = cls.values(dataset, kd.name, expanded=False)
            if len(vals) == 1:
                constant[kd.name] = vals[0]
        data = {k: values for k, values in dataset.data.items()
                if k not in dropped_kdims+dropped_vdims}

        if len(constant) == len(dropped_kdims):
            joined_dims = kdims+dropped_kdims
            axes = tuple(dataset.ndims-dataset.kdims.index(d)-1
                         for d in joined_dims)
            dropped_axes = tuple(dataset.ndims-joined_dims.index(d)-1
                                 for d in dropped_kdims)
            for vdim in vdims:
                vdata = data[vdim.name]
                if len(axes) > 1:
                    vdata = vdata.transpose(axes[::-1])
                if dropped_axes:
                    vdata = np.squeeze(vdata, axis=dropped_axes)
                data[vdim.name] = vdata
            return data
        elif dropped_kdims:
            return tuple(dataset.columns(kdims+vdims).values())
        return data


    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        if not vdim:
            raise Exception("Cannot add key dimension to a dense representation.")
        dim = dimension_name(dimension)
        return dict(dataset.data, **{dim: values})


    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        if not by or by in [dataset.kdims, dataset.dimensions()]:
            return dataset.data
        else:
            raise Exception('Compressed format cannot be sorted, either instantiate '
                            'in the desired order or use the expanded format.')

    @classmethod
    def iloc(cls, dataset, index):
        rows, cols = index
        scalar = False
        if np.isscalar(cols):
            scalar = np.isscalar(rows)
            cols = [dataset.get_dimension(cols, strict=True)]
        elif isinstance(cols, slice):
            cols = dataset.dimensions()[cols]
        else:
            cols = [dataset.get_dimension(d, strict=True) for d in cols]

        if np.isscalar(rows):
            rows = [rows]

        new_data = []
        for d in cols:
            new_data.append(cls.values(dataset, d, compute=False)[rows])

        if scalar:
            da = dask_array_module()
            if new_data and (da and isinstance(new_data[0], da.Array)):
                return new_data[0].compute()[0]
            return new_data[0][0]
        return tuple(new_data)

    @classmethod
    def range(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        if dataset._binned and dimension in dataset.kdims:
            expanded = cls.irregular(dataset, dimension)
            array = cls.coords(dataset, dimension, expanded=expanded, edges=True)
        else:
            array = cls.values(dataset, dimension, expanded=False, flat=False)

        if dimension.nodata is not None:
            array = cls.replace_value(array, dimension.nodata)

        da = dask_array_module()
        if len(array) == 0:
            return np.nan, np.nan

        if dtype_kind(array) == 'M':
            dmin, dmax = array.min(), array.max()
        else:
            try:
                dmin, dmax = (np.nanmin(array), np.nanmax(array))
            except TypeError:
                return np.nan, np.nan
        if da and isinstance(array, da.Array):
            return finite_range(array, *da.compute(dmin, dmax))
        return finite_range(array, dmin, dmax)

    @classmethod
    def assign(cls, dataset, new_data):
        data = dict(dataset.data)
        for k, v in new_data.items():
            if k in dataset.kdims:
                coords = cls.coords(dataset, k)
                if not coords.ndim > 1 and np.all(coords[1:] < coords[:-1]):
                    v = v[::-1]
                data[k] = v
            else:
                data[k] = cls.canonicalize(dataset, v)
        return data



Interface.register(GridInterface)
