import sys

import numpy as np

from .. import util
from ..dimension import Dimension
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind
from .interface import Interface
from .pandas import PandasInterface


class DaskInterface(PandasInterface):
    """The DaskInterface allows a Dataset objects to wrap a dask
    DataFrame object. Using dask allows loading data lazily
    and performing out-of-core operations on the data, making
    it possible to work on datasets larger than memory.

    The DaskInterface covers almost the complete API exposed
    by the PandasInterface with two notable exceptions:

    1) Sorting is not supported and any attempt at sorting will
       be ignored with a warning.

    2) Dask does not easily support adding a new column to an existing
       dataframe unless it is a scalar, add_dimension will therefore
       error when supplied a non-scalar value.

    3) Not all functions can be easily applied to a dask dataframe so
       some functions applied with aggregate and reduce will not work.

    """

    types = ()

    datatype = 'dask'

    default_partitions = 100

    @classmethod
    def loaded(cls):
        return 'dask.dataframe' in sys.modules and 'pandas' in sys.modules

    @classmethod
    def applies(cls, obj):
        if not cls.loaded():
            return False
        import dask.dataframe as dd
        return isinstance(obj, (dd.DataFrame, dd.Series))

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        import dask.dataframe as dd

        data, dims, extra = PandasInterface.init(eltype, data, kdims, vdims)
        if not isinstance(data, dd.DataFrame):
            data = dd.from_pandas(data, npartitions=cls.default_partitions, sort=False)
        kdims = [d.name if isinstance(d, Dimension) else d for d in dims['kdims']]

        # If a key dimension can be found, speculatively reset index
        # to work around lacking dask support for MultiIndex
        if any(d for d in kdims if d not in data.columns):
            reset = data.reset_index()
            if all(d for d in kdims if d in reset.columns):
                data = reset
        return data, dims, extra

    @classmethod
    def compute(cls, dataset):
        return dataset.clone(dataset.data.compute())

    @classmethod
    def persist(cls, dataset):
        return dataset.clone(dataset.data.persist())

    @classmethod
    def shape(cls, dataset):
        return (len(dataset.data), len(dataset.data.columns))

    @classmethod
    def range(cls, dataset, dimension):
        import dask.dataframe as dd
        dimension = dataset.get_dimension(dimension, strict=True)
        column = dataset.data[dimension.name]
        if dtype_kind(column) == 'O':
            try:
                column = np.sort(column[column.notnull()].compute())
                return (column[0], column[-1]) if len(column) else (None, None)
            except TypeError:
                return (None, None)
        else:
            if dimension.nodata is not None:
                column = cls.replace_value(column, dimension.nodata)
            return dd.compute(column.min(), column.max())

    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        dataset.param.warning('Dask dataframes do not support sorting')
        return dataset.data

    @classmethod
    def values(cls, dataset, dim, expanded=True, flat=True, compute=True, keep_index=False):
        dim = dataset.get_dimension(dim)
        data = dataset.data[dim.name]
        if not expanded:
            data = data.unique()
        if keep_index:
            return data.compute() if compute else data
        else:
            return data.compute().values if compute else data.values

    @classmethod
    def select_mask(cls, dataset, selection):
        """Given a Dataset object and a dictionary with dimension keys and
        selection keys (i.e. tuple ranges, slices, sets, lists. or literals)
        return a boolean mask over the rows in the Dataset object that
        have been selected.

        """
        select_mask = None
        for dim, k in selection.items():
            if isinstance(k, tuple):
                k = slice(*k)
            masks = []
            alias = dataset.get_dimension(dim).name
            series = dataset.data[alias]
            if isinstance(k, slice):
                if k.start is not None:
                    # Workaround for dask issue #3392
                    kval = util.numpy_scalar_to_python(k.start)
                    masks.append(kval <= series)
                if k.stop is not None:
                    kval = util.numpy_scalar_to_python(k.stop)
                    masks.append(series < kval)
            elif isinstance(k, (set, list)):
                iter_slc = None
                for ik in k:
                    mask = series == ik
                    if iter_slc is None:
                        iter_slc = mask
                    else:
                        iter_slc |= mask
                masks.append(iter_slc)
            elif callable(k):
                masks.append(k(series))
            else:
                masks.append(series == k)
            for mask in masks:
                if select_mask is not None:
                    select_mask &= mask
                else:
                    select_mask = mask
        return select_mask

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        df = dataset.data
        if selection_mask is not None:
            import dask.array as da
            if isinstance(selection_mask, da.Array):
                return df.loc[selection_mask]
            return df[selection_mask]
        selection_mask = cls.select_mask(dataset, selection)
        indexed = cls.indexed(dataset, selection)
        df = df if selection_mask is None else df[selection_mask]
        if indexed and len(df) == 1 and len(dataset.vdims) == 1:
            return df[dataset.vdims[0].name].compute().iloc[0]
        return df

    @classmethod
    def _select_mask_neighbor(cls, dataset, selection):
        """Runs select mask and expand the True values to include its neighbors

        Example

        select_mask =          [False, False, True, True, False, False]
        select_mask_neighbor = [False, True,  True, True, True,  False]

        """
        mask = cls.select_mask(dataset, selection)
        mask = mask.to_dask_array().compute_chunk_sizes()
        extra = mask[1:] ^ mask[:-1]
        mask[1:] |= extra
        mask[:-1] |= extra
        return mask

    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        index_dims = [dataset.get_dimension(d) for d in dimensions]
        element_dims = [kdim for kdim in dataset.kdims
                        if kdim not in index_dims]

        group_kwargs = {}
        if group_type != 'raw' and issubclass(group_type, Element):
            group_kwargs = dict(util.get_param_values(dataset),
                                kdims=element_dims)
        group_kwargs.update(kwargs)

        # Propagate dataset
        group_kwargs['dataset'] = dataset.dataset

        data = []
        group_by = [d.name for d in index_dims]
        groupby = dataset.data.groupby(group_by)
        if len(group_by) == 1:
            column = dataset.data[group_by[0]]
            if column.dtype.name == 'category':
                try:
                    indices = ((ind,) for ind in column.cat.categories)
                except NotImplementedError:
                    indices = ((ind,) for ind in column.unique().compute())
            else:
                indices = ((ind,) for ind in column.unique().compute())
        else:
            group_tuples = dataset.data[group_by].itertuples()
            indices = util.unique_iterator(ind[1:] for ind in group_tuples)
        for coord in indices:
            if any(isinstance(c, float) and np.isnan(c) for c in coord):
                continue
            if len(coord) == 1 and not util.PANDAS_GE_2_2_0:
                coord = coord[0]
            group = group_type(groupby.get_group(coord), **group_kwargs)
            data.append((coord, group))
        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(data, kdims=index_dims)
        else:
            return container_type(data)

    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        data = dataset.data
        cols = [d.name for d in dataset.kdims if d in dimensions]
        vdims = dataset.dimensions('value', label='name')
        dtypes = data.dtypes
        numeric = [c for c, dtype in zip(dtypes.index, dtypes.values, strict=None)
                   if dtype_kind(dtype) in 'iufc' and c in vdims]
        reindexed = data[cols+numeric]

        inbuilts = {'amin': 'min', 'amax': 'max', 'mean': 'mean',
                    'std': 'std', 'sum': 'sum', 'var': 'var'}
        if len(dimensions):
            groups = reindexed.groupby(cols)
            if (function.__name__ in inbuilts):
                agg = getattr(groups, inbuilts[function.__name__])()
            else:
                agg = groups.apply(function)
            df = agg.reset_index()
        else:
            if (function.__name__ in inbuilts):
                agg = getattr(reindexed, inbuilts[function.__name__])()
            else:
                raise NotImplementedError
            import pandas as pd
            df = pd.DataFrame(agg.compute()).T

        dropped = []
        for vd in vdims:
            if vd not in df.columns:
                dropped.append(vd)
        return df, dropped

    @classmethod
    def unpack_scalar(cls, dataset, data):
        """Given a dataset object and data in the appropriate format for
        the interface, return a simple scalar.

        """
        import dask.dataframe as dd
        if len(data.columns) > 1 or len(data) != 1:
            return data
        if isinstance(data, dd.DataFrame):
            data = data.compute()
        return data.iat[0,0]

    @classmethod
    def sample(cls, dataset, samples=None):
        if samples is None:
            samples = []
        data = dataset.data
        dims = dataset.dimensions('key', label='name')
        mask = None
        for sample in samples:
            if np.isscalar(sample): sample = [sample]
            for c, v in zip(dims, sample, strict=None):
                dim_mask = data[c]==v
                if mask is None:
                    mask = dim_mask
                else:
                    mask |= dim_mask
        return data[mask]

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        data = dataset.data
        if dimension.name not in data.columns:
            if not np.isscalar(values):
                if len(values):
                    err = ('Dask dataframe does not support assigning '
                           'non-scalar value.')
                    raise NotImplementedError(err)
                values = None
            data = data.assign(**{dimension.name: values})
        return data

    @classmethod
    def concat_fn(cls, dataframes, **kwargs):
        import dask.dataframe as dd
        return dd.concat(dataframes, **kwargs)

    @classmethod
    def dframe(cls, dataset, dimensions):
        if dimensions:
            return dataset.data[dimensions].compute()
        else:
            return dataset.data.compute()

    @classmethod
    def nonzero(cls, dataset):
        return True

    @classmethod
    def iloc(cls, dataset, index):
        """Dask does not support iloc, therefore iloc will execute
        the call graph and lose the laziness of the operation.

        """
        rows, cols = index
        scalar = False
        if isinstance(cols, slice):
            cols = [d.name for d in dataset.dimensions()][cols]
        elif np.isscalar(cols):
            scalar = np.isscalar(rows)
            cols = [dataset.get_dimension(cols).name]
        else:
            cols = [dataset.get_dimension(d).name for d in index[1]]
        if np.isscalar(rows):
            rows = [rows]

        data = {}
        for c in cols:
            data[c] = dataset.data[c].compute().iloc[rows].values
        if scalar:
            return data[cols[0]][0]
        return tuple(data.values())


Interface.register(DaskInterface)
