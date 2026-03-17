import sys
import warnings
from itertools import product

import numpy as np

from .. import util
from ..dimension import dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind
from .interface import DataError, Interface
from .pandas import PandasInterface
from .util import finite_range


class cuDFInterface(PandasInterface):
    """The cuDFInterface allows a Dataset objects to wrap a cuDF
    DataFrame object. Using cuDF allows working with columnar
    data on a GPU. Most operations leave the data in GPU memory,
    however to plot the data it has to be loaded into memory.

    The cuDFInterface covers almost the complete API exposed
    by the PandasInterface with two notable exceptions:

    1. Aggregation and groupby do not have a consistent sort order
       (see https://github.com/rapidsai/cudf/issues/4237)
    2. Not all functions can be easily applied to a cuDF so
       some functions applied with aggregate and reduce will not work.
    """

    datatype = 'cuDF'

    types = ()

    @classmethod
    def loaded(cls):
        return 'cudf' in sys.modules

    @classmethod
    def applies(cls, obj):
        if not cls.loaded():
            return False
        import cudf
        return isinstance(obj, (cudf.DataFrame, cudf.Series))

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        import cudf
        import pandas as pd

        element_params = eltype.param.objects()
        kdim_param = element_params['kdims']
        vdim_param = element_params['vdims']

        if isinstance(data, (cudf.Series, pd.Series)):
            data = data.to_frame()

        if not isinstance(data, cudf.DataFrame):
            data, _, _ = PandasInterface.init(eltype, data, kdims, vdims)
            data = cudf.from_pandas(data)

        columns = list(data.columns)
        ncols = len(columns)
        index_names = [data.index.name]
        if index_names == [None]:
            index_names = ['index']
        if eltype._auto_indexable_1d and ncols == 1 and kdims is None:
            kdims = list(index_names)

        if isinstance(kdim_param.bounds[1], int):
            ndim = min([kdim_param.bounds[1], len(kdim_param.default)])
        else:
            ndim = None
        nvdim = vdim_param.bounds[1] if isinstance(vdim_param.bounds[1], int) else None
        if kdims and vdims is None:
            vdims = [c for c in columns if c not in kdims]
        elif vdims and kdims is None:
            kdims = [c for c in columns if c not in vdims][:ndim]
        elif kdims is None:
            kdims = list(columns[:ndim])
            if vdims is None:
                vdims = [d for d in columns[ndim:((ndim+nvdim) if nvdim else None)]
                         if d not in kdims]
        elif kdims == [] and vdims is None:
            vdims = list(columns[:nvdim if nvdim else None])

        # Handle reset of index if kdims reference index by name
        for kd in kdims:
            kd = dimension_name(kd)
            if kd in columns:
                continue
            if any(kd == ('index' if name is None else name)
                   for name in index_names):
                data = data.reset_index()
                break
        if any(isinstance(d, (np.int64, int)) for d in kdims+vdims):
            raise DataError("cudf DataFrame column names used as dimensions "
                            "must be strings not integers.", cls)

        if kdims:
            kdim = dimension_name(kdims[0])
            if eltype._auto_indexable_1d and ncols == 1 and kdim not in columns:
                data = data.copy()
                data.insert(0, kdim, np.arange(len(data)))

        for d in kdims+vdims:
            d = dimension_name(d)
            if len([c for c in columns if c == d]) > 1:
                raise DataError('Dimensions may not reference duplicated DataFrame '
                                f'columns (found duplicate {d!r} columns). If you want to plot '
                                'a column against itself simply declare two dimensions '
                                'with the same name.', cls)
        return data, {'kdims':kdims, 'vdims':vdims}, {}


    @classmethod
    def range(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        column = dataset.data[dimension.name]
        if dimension.nodata is not None:
            column = cls.replace_value(column, dimension.nodata)
        if dtype_kind(column) == 'O':
            return np.nan, np.nan
        else:
            return finite_range(column, column.min(), column.max())


    @classmethod
    def values(cls, dataset, dim, expanded=True, flat=True, compute=True,
               keep_index=False):
        dim = dataset.get_dimension(dim, strict=True)
        data = dataset.data[dim.name]
        if not expanded:
            data = data.unique()
            return data.values_host if compute else data.values
        elif keep_index:
            return data
        elif compute:
            return data.values_host
        try:
            return data.values
        except Exception:
            return data.values_host

    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        # Get dimensions information
        dimensions = [dataset.get_dimension(d).name for d in dimensions]
        kdims = [kdim for kdim in dataset.kdims if kdim not in dimensions]

        # Update the kwargs appropriately for Element group types
        group_kwargs = {}
        group_type = dict if group_type == 'raw' else group_type
        if issubclass(group_type, Element):
            group_kwargs.update(util.get_param_values(dataset))
            group_kwargs['kdims'] = kdims
        group_kwargs.update(kwargs)

        # Propagate dataset
        group_kwargs['dataset'] = dataset.dataset

        # Find all the keys along supplied dimensions
        keys = product(*(dataset.data[dimensions[0]].unique().values_host for d in dimensions))

        # Iterate over the unique entries applying selection masks
        grouped_data = []
        for unique_key in util.unique_iterator(keys):
            group_data = dataset.select(**dict(zip(dimensions, unique_key, strict=None)))
            if not len(group_data):
                continue
            group_data = group_type(group_data, **group_kwargs)
            grouped_data.append((unique_key, group_data))

        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                kdims = [dataset.get_dimension(d) for d in dimensions]
                return container_type(grouped_data, kdims=kdims)
        else:
            return container_type(grouped_data)


    @classmethod
    def select_mask(cls, dataset, selection):
        """Given a Dataset object and a dictionary with dimension keys and
        selection keys (i.e. tuple ranges, slices, sets, lists, or literals)
        return a boolean mask over the rows in the Dataset object that
        have been selected.

        """
        mask = None
        for dim, sel in selection.items():
            if isinstance(sel, tuple):
                sel = slice(*sel)
            arr = cls.values(dataset, dim, keep_index=True)
            if util.isdatetime(arr):
                try:
                    sel = util.parse_datetime_selection(sel)
                except Exception:
                    pass

            new_masks = []
            if isinstance(sel, slice):
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', r'invalid value encountered')
                    if sel.start is not None:
                        # Comparison has to be in this order due to issues with datetime comparison (see #6407)
                        new_masks.append(arr >= sel.start)
                    if sel.stop is not None:
                        new_masks.append(arr < sel.stop)
                if not new_masks:
                    continue
                new_mask = new_masks[0]
                for imask in new_masks[1:]:
                    new_mask &= imask
            elif isinstance(sel, (set, list)):
                for v in sel:
                    new_masks.append(arr==v)
                if not new_masks:
                    continue
                new_mask = new_masks[0]
                for imask in new_masks[1:]:
                    new_mask |= imask
            elif callable(sel):
                new_mask = sel(arr)
            else:
                new_mask = arr == sel

            if mask is None:
                mask = new_mask
            else:
                mask &= new_mask
        return mask

    @classmethod
    def _select_mask_neighbor(cls, dataset, selection):
        """Runs select mask and expand the True values to include its neighbors

        Example

        select_mask =          [False, False, True, True, False, False]
        select_mask_neighbor = [False, True,  True, True, True,  False]

        """
        mask = cls.select_mask(dataset, selection).to_cupy()
        extra = (mask[1:] ^ mask[:-1])
        mask[1:] |= extra
        mask[:-1] |= extra
        return mask

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        df = dataset.data
        if selection_mask is None:
            selection_mask = cls.select_mask(dataset, selection)

        indexed = cls.indexed(dataset, selection)
        if selection_mask is not None:
            df = df.loc[selection_mask]
        if indexed and len(df) == 1 and len(dataset.vdims) == 1:
            return df[dataset.vdims[0].name].iloc[0]
        return df

    @classmethod
    def concat_fn(cls, dataframes, **kwargs):
        import cudf
        return cudf.concat(dataframes, **kwargs)

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        data = dataset.data.copy()
        if dimension.name not in data:
            data[dimension.name] = values
        return data

    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        import pandas as pd
        from pandas.api.types import is_numeric_dtype

        data = dataset.data
        cols = [d.name for d in dataset.kdims if d in dimensions]
        vdims = dataset.dimensions('value', label='name')
        reindexed = data[cols+vdims]
        agg = function.__name__
        if len(dimensions):
            agg_map = {'amin': 'min', 'amax': 'max'}
            agg = agg_map.get(agg, agg)
            grouped = reindexed.groupby(cols, sort=False)
            if not hasattr(grouped, agg):
                raise ValueError(f'{agg} aggregation is not supported on cudf DataFrame.')
            numeric_cols = [
                c for c, d in zip(reindexed.columns, reindexed.dtypes, strict=True)
                if is_numeric_dtype(d) and c not in cols
            ]
            df = getattr(grouped[numeric_cols], agg)().reset_index()
        else:
            agg_map = {'amin': 'min', 'amax': 'max', 'size': 'count'}
            agg = agg_map.get(agg, agg)
            if not hasattr(reindexed, agg):
                raise ValueError(f'{agg} aggregation is not supported on cudf DataFrame.')
            agg = getattr(reindexed, agg)()
            try:
                data = {col: [v] for col, v in zip(agg.index.values_host, agg.to_numpy(), strict=True)}
            except Exception:
                # Give FutureWarning: 'The to_array method will be removed in a future cuDF release.
                # Consider using `to_numpy` instead.'
                # Seen in cudf=21.12.01
                data = {col: [v] for col, v in zip(agg.index.values_host, agg.to_array(), strict=True)}
            df = pd.DataFrame(data, columns=list(agg.index.values_host))

        dropped = []
        for vd in vdims:
            if vd not in df.columns:
                dropped.append(vd)
        return df, dropped


    @classmethod
    def iloc(cls, dataset, index):
        import cudf

        rows, cols = index
        scalar = False
        columns = list(dataset.data.columns)
        if isinstance(cols, slice):
            cols = [d.name for d in dataset.dimensions()][cols]
        elif np.isscalar(cols):
            scalar = np.isscalar(rows)
            cols = [dataset.get_dimension(cols).name]
        else:
            cols = [dataset.get_dimension(d).name for d in index[1]]
        col_index = [columns.index(c) for c in cols]
        if np.isscalar(rows):
            rows = [rows]

        if scalar:
            return dataset.data[cols[0]].iloc[rows[0]]
        result = dataset.data.iloc[rows, col_index]

        # cuDF does not handle single rows and cols indexing correctly
        # as of cudf=0.10.0 so we have to convert Series back to DataFrame
        if isinstance(result, cudf.Series):
            if len(cols) == 1:
                result = result.to_frame(cols[0])
            else:
                result = result.to_frame().T
        return result


    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        cols = [dataset.get_dimension(d, strict=True).name for d in by]
        return dataset.data.sort_values(by=cols, ascending=not reverse)


    @classmethod
    def dframe(cls, dataset, dimensions):
        if dimensions:
            return dataset.data[dimensions].to_pandas()
        else:
            return dataset.data.to_pandas()


Interface.register(cuDFInterface)
