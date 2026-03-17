import numpy as np

from .. import util
from ..dimension import dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind
from .interface import DataError, Interface


class ArrayInterface(Interface):

    types = (np.ndarray,)

    datatype = 'array'

    named = False

    @classmethod
    def dimension_type(cls, dataset, dim):
        return dataset.data.dtype.type

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        if kdims is None:
            kdims = eltype.kdims
        if vdims is None:
            vdims = eltype.vdims

        dimensions = [dimension_name(d) for d in kdims + vdims]
        if ((isinstance(data, dict) or util.is_dataframe(data)) and
            all(d in data for d in dimensions)):
            dataset = [d if isinstance(d, np.ndarray) else np.asarray(data[d]) for d in dimensions]
            if len({dtype_kind(d) for d in dataset}) > 1:
                raise ValueError('ArrayInterface expects all columns to be of the same dtype')
            data = np.column_stack(dataset)
        elif isinstance(data, dict) and not all(d in data for d in dimensions):
            dict_data = sorted(data.items())
            dataset = zip(*((util.wrap_tuple(k)+util.wrap_tuple(v))
                            for k, v in dict_data), strict=None)
            data = np.column_stack(list(dataset))
        elif isinstance(data, tuple):
            data = [d if isinstance(d, np.ndarray) else np.asarray(d) for d in data]
            if len({dtype_kind(d) for d in data}) > 1:
                raise ValueError('ArrayInterface expects all columns to be of the same dtype')
            elif cls.expanded(data):
                data = np.column_stack(data)
            else:
                raise ValueError('ArrayInterface expects data to be of uniform shape.')
        elif isinstance(data, list) and data == []:
            data = np.empty((0,len(dimensions)))
        elif not isinstance(data, np.ndarray):
            data = np.array([], ndmin=2).T if data is None else list(data)
            try:
                data = np.array(data)
            except Exception:
                data = None

        if kdims is None:
            kdims = eltype.kdims
        if vdims is None:
            vdims = eltype.vdims

        if data is None or data.ndim > 2 or dtype_kind(data) in ['S', 'U', 'O']:
            raise ValueError("ArrayInterface interface could not handle input type.")
        elif data.ndim == 1:
            if eltype._auto_indexable_1d and len(kdims)+len(vdims)>1:
                data = np.column_stack([np.arange(len(data)), data])
            else:
                data = np.atleast_2d(data).T

        return data, {'kdims':kdims, 'vdims':vdims}, {}

    @classmethod
    def validate(cls, dataset, vdims=True):
        ndims = len(dataset.dimensions()) if vdims else dataset.ndims
        ncols = dataset.data.shape[1] if dataset.data.ndim > 1 else 1
        if ncols < ndims:
            raise DataError("Supplied data does not match specified "
                            f"dimensions, expected at least {ndims} columns.", cls)


    @classmethod
    def isscalar(cls, dataset, dim):
        idx = dataset.get_dimension_index(dim)
        return len(np.unique(dataset.data[:, idx])) == 1


    @classmethod
    def array(cls, dataset, dimensions):
        if dimensions:
            indices = [dataset.get_dimension_index(d) for d in dimensions]
            return dataset.data[:, indices]
        else:
            return dataset.data


    @classmethod
    def dtype(cls, dataset, dimension):
        return dataset.data.dtype


    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        data = dataset.data.copy()
        return np.insert(data, dim_pos, values, axis=1)


    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        data = dataset.data
        if len(by) == 1:
            sorting = cls.values(dataset, by[0]).argsort()
        else:
            dtypes = [(d.name, dataset.data.dtype) for d in dataset.dimensions()]
            sort_fields = tuple(dataset.get_dimension(d).name for d in by)
            sorting = dataset.data.view(dtypes, np.recarray).T
            sorting = sorting.argsort(order=sort_fields)[0]
        sorted_data = data[sorting]
        return sorted_data[::-1] if reverse else sorted_data


    @classmethod
    def values(cls, dataset, dim, expanded=True, flat=True, compute=True, keep_index=False):
        data = dataset.data
        dim_idx = dataset.get_dimension_index(dim)
        if data.ndim == 1:
            data = np.atleast_2d(data).T
        values = data[:, dim_idx]
        if not expanded:
            return util.unique_array(values)
        return values


    @classmethod
    def mask(cls, dataset, mask, mask_value=np.nan):
        masked = np.copy(dataset.data)
        masked[mask] = mask_value
        return masked


    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        # DataFrame based tables don't need to be reindexed
        dims = kdims + vdims
        data = [dataset.dimension_values(d) for d in dims]
        return np.column_stack(data)


    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        data = dataset.data

        # Get dimension objects, labels, indexes and data
        dimensions = [dataset.get_dimension(d, strict=True) for d in dimensions]
        dim_idxs = [dataset.get_dimension_index(d) for d in dimensions]
        kdims = [kdim for kdim in dataset.kdims
                 if kdim not in dimensions]
        vdims = dataset.vdims

        # Find unique entries along supplied dimensions
        # by creating a view that treats the selected
        # groupby keys as a single object.
        indices = data[:, dim_idxs].copy()
        group_shape = indices.dtype.itemsize * indices.shape[1]
        view = indices.view(np.dtype((np.void, group_shape)))
        _, idx = np.unique(view, return_index=True)
        idx.sort()
        unique_indices = indices[idx]

        # Get group
        group_kwargs = {}
        if group_type != 'raw' and issubclass(group_type, Element):
            group_kwargs.update(util.get_param_values(dataset))
            group_kwargs['kdims'] = kdims
        group_kwargs.update(kwargs)

        # Iterate over the unique entries building masks
        # to apply the group selection
        grouped_data = []
        col_idxs = [dataset.get_dimension_index(d) for d in dataset.dimensions()
                    if d not in dimensions]
        for group in unique_indices:
            mask = np.logical_and.reduce([data[:, d_idx] == group[i]
                                          for i, d_idx in enumerate(dim_idxs)])
            group_data = data[mask][:, col_idxs]
            if not group_type == 'raw':
                if issubclass(group_type, dict):
                    group_data = {d.name: group_data[:, i] for i, d in
                                  enumerate(kdims+vdims)}
                else:
                    group_data = group_type(group_data, **group_kwargs)
            grouped_data.append((tuple(group), group_data))

        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(grouped_data, kdims=dimensions)
        else:
            return container_type(grouped_data)


    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        if selection_mask is None:
            selection_mask = cls.select_mask(dataset, selection)
        indexed = cls.indexed(dataset, selection)
        data = np.atleast_2d(dataset.data[selection_mask, :])
        if len(data) == 1 and indexed and len(dataset.vdims) == 1:
            data = data[0, dataset.ndims]
        return data


    @classmethod
    def sample(cls, dataset, samples=None):
        if samples is None:
            samples = []
        data = dataset.data
        mask = False
        for sample in samples:
            sample_mask = True
            if np.isscalar(sample): sample = [sample]
            for i, v in enumerate(sample):
                sample_mask &= data[:, i]==v
            mask |= sample_mask

        return data[mask]


    @classmethod
    def unpack_scalar(cls, dataset, data):
        """Given a dataset object and data in the appropriate format for
        the interface, return a simple scalar.

        """
        if data.shape == (1, 1):
            return data[0, 0]
        return data


    @classmethod
    def assign(cls, dataset, new_data):
        data = dataset.data.copy()
        for d, arr in new_data.items():
            if dataset.get_dimension(d) is None:
                continue
            idx = dataset.get_dimension_index(d)
            data[:, idx] = arr
        new_cols = [arr for d, arr in new_data.items() if dataset.get_dimension(d) is None]
        return np.column_stack([data, *new_cols])


    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        reindexed = dataset.reindex(dimensions)
        grouped = (cls.groupby(reindexed, dimensions, list, 'raw')
                   if len(dimensions) else [((), reindexed.data)])

        rows = []
        for k, group in grouped:
            if isinstance(function, np.ufunc):
                reduced = function.reduce(group, axis=0, **kwargs)
            else:
                reduced = function(group, axis=0, **kwargs)
            rows.append(np.concatenate([k, (reduced,) if np.isscalar(reduced) else reduced]))
        return np.atleast_2d(rows), []


    @classmethod
    def iloc(cls, dataset, index):
        rows, cols = index
        if np.isscalar(cols):
            if isinstance(cols, str):
                cols = dataset.get_dimension_index(cols)
            if np.isscalar(rows):
                return dataset.data[rows, cols]
            cols = [dataset.get_dimension_index(cols)]
        elif not isinstance(cols, slice):
            cols = [dataset.get_dimension_index(d) for d in cols]

        if np.isscalar(rows):
            rows = [rows]
        data = dataset.data[rows, :][:, cols]
        if data.ndim == 1:
            return np.atleast_2d(data).T
        return data

Interface.register(ArrayInterface)
