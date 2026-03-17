from typing import TYPE_CHECKING

import numpy as np

from .. import util
from ..dimension import Dimension, dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind
from ..util.dependencies import PANDAS_GE_2_1_0, _LazyModule
from .interface import DataError, Interface
from .util import finite_range

if TYPE_CHECKING:
    import pandas as pd
else:
    pd = _LazyModule("pandas")



class PandasAPI:
    """This class is used to describe the interface as having a pandas-like API.

    The reason to have this class is that it is not always
    possible to directly inherit from the PandasInterface.

    This class should not have any logic as it should be used like:
        if issubclass(interface, PandasAPI):
            ...

    """


class PandasInterface(Interface, PandasAPI):

    datatype = 'dataframe'

    @classmethod
    def loaded(cls):
        # 2025-02: As long as it is a required dependency and to not break
        # existing behavior we will for now always return True
        return bool(pd)

    @classmethod
    def applies(cls, obj):
        if not cls.loaded():
            return False
        return type(obj) is pd.DataFrame or type(obj) is pd.Series

    @classmethod
    def dimension_type(cls, dataset, dim):
        return cls.dtype(dataset, dim).type

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        element_params = eltype.param.objects()
        kdim_param = element_params['kdims']
        vdim_param = element_params['vdims']
        if util.is_series(data):
            name = data.name or util.anonymous_dimension_label
            data = data.to_frame(name=name)
        if util.is_dataframe(data):
            ncols = len(data.columns)
            index_names = cls.indexes(data)
            if eltype._auto_indexable_1d and ncols == 1 and kdims is None:
                kdims = list(index_names)

            if isinstance(kdim_param.bounds[1], int):
                ndim = min([kdim_param.bounds[1], len(kdim_param.default)])
            else:
                ndim = None
            nvdim = vdim_param.bounds[1] if isinstance(vdim_param.bounds[1], int) else None
            if kdims and vdims is None:
                vdims = [c for c in data.columns if c not in kdims]
            elif vdims and kdims is None:
                kdims = [c for c in data.columns if c not in vdims][:ndim]
            elif kdims is None:
                kdims = list(data.columns[:ndim])
                if vdims is None:
                    vdims = [d for d in data.columns[ndim:((ndim+nvdim) if nvdim else None)]
                             if d not in kdims]
            elif kdims == [] and vdims is None:
                vdims = list(data.columns[:nvdim if nvdim else None])

            if any(not isinstance(d, (str, Dimension)) for d in kdims+vdims):
                raise DataError(
                    "Having a non-string as a column name in a DataFrame is not supported."
                )

            if kdims and not (len(kdims) == len(index_names) and {dimension_name(kd) for kd in kdims} == set(index_names)):
                kdim = dimension_name(kdims[0])
                if eltype._auto_indexable_1d and ncols == 1 and kdim not in [*data.columns, *cls.indexes(data)]:
                    data = data.copy()
                    data.insert(0, kdim, np.arange(len(data)))

            for d in kdims+vdims:
                d = dimension_name(d)
                if len([c for c in data.columns if c == d]) > 1:
                    raise DataError('Dimensions may not reference duplicated DataFrame '
                                    f'columns (found duplicate {d!r} columns). If you want to plot '
                                    'a column against itself simply declare two dimensions '
                                    'with the same name.', cls)
        else:
            # Check if data is of non-numeric type
            # Then use defined data type
            kdims = kdims if kdims else kdim_param.default
            vdims = vdims if vdims else vdim_param.default
            columns = list(util.unique_iterator([dimension_name(d) for d in kdims+vdims]))

            if isinstance(data, dict) and all(c in data for c in columns):
                data = dict((d, data[d]) for d in columns)
            elif isinstance(data, list) and len(data) == 0:
                data = {c: np.array([]) for c in columns}
            elif isinstance(data, (list, dict)) and data in ([], {}):
                data = None
            elif (isinstance(data, dict) and not all(d in data for d in columns) and
                  not any(isinstance(v, np.ndarray) for v in data.values())):
                column_data = sorted(data.items())
                k, v = column_data[0]
                if len(util.wrap_tuple(k)) != len(kdims) or len(util.wrap_tuple(v)) != len(vdims):
                    raise ValueError("Dictionary data not understood, should contain a column "
                                    "per dimension or a mapping between key and value dimension "
                                    "values.")
                column_data = zip(*((util.wrap_tuple(k)+util.wrap_tuple(v))
                                    for k, v in column_data), strict=None)
                data = dict(((c, col) for c, col in zip(columns, column_data, strict=None)))
            elif isinstance(data, np.ndarray):
                if data.ndim == 1:
                    if eltype._auto_indexable_1d and len(kdims)+len(vdims)>1:
                        data = (np.arange(len(data)), data)
                    else:
                        data = np.atleast_2d(data).T
                else:
                    data = tuple(data[:, i] for i in range(data.shape[1]))

            if isinstance(data, tuple):
                data = [np.array(d) if not isinstance(d, np.ndarray) else d for d in data]
                min_dims = (kdim_param.bounds[0] or 0) + (vdim_param.bounds[0] or 0)
                if any(d.ndim > 1 for d in data):
                    raise ValueError('PandasInterface cannot interpret multi-dimensional arrays.')
                elif len(data) < min_dims:
                    raise DataError(f'Data contains fewer columns than the {eltype.__name__} element expects. Expected '
                                    f'at least {min_dims} columns but found only {len(data)} columns.')
                elif not cls.expanded(data):
                    raise ValueError('PandasInterface expects data to be of uniform shape.')
                data = pd.DataFrame(dict(zip(columns, data, strict=None)), columns=columns)
            elif ((isinstance(data, dict) and any(c not in data for c in columns)) or
                  (isinstance(data, list) and any(isinstance(d, dict) and c not in d for d in data for c in columns))):
                raise ValueError('PandasInterface could not find specified dimensions in the data.')
            else:
                data = pd.DataFrame(data, columns=columns)
        return data, {'kdims': kdims, 'vdims': vdims}, {}

    @classmethod
    def isscalar(cls, dataset, dim):
        name = dataset.get_dimension(dim, strict=True).name
        return len(dataset.data[name].unique()) == 1

    @classmethod
    def dtype(cls, dataset, dimension):
        dim = dataset.get_dimension(dimension, strict=True)
        name = dim.name
        df = dataset.data
        if cls.isindex(dataset, dim):
            data = cls.index_values(dataset, dim)
        else:
            data = df[name]
        if util.isscalar(data):
            return np.array([data]).dtype
        else:
            return data.dtype

    @classmethod
    def indexes(cls, data):
        index_names = data.index.names if isinstance(data, pd.DataFrame) else [data.index.name]
        if index_names == [None]:
            index_names = ['_index'] if 'index' in data.columns else ['index']
        return index_names

    @classmethod
    def isindex(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        if dimension.name in dataset.data.columns:
            return False
        return dimension.name in cls.indexes(dataset.data)

    @classmethod
    def index_values(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        index = dataset.data.index
        if isinstance(index, pd.MultiIndex):
            return index.get_level_values(dimension.name)
        return index

    @classmethod
    def validate(cls, dataset, vdims=True):
        dim_types = 'all' if vdims else 'key'
        dimensions = dataset.dimensions(dim_types, label='name')
        cols = list(dataset.data.columns) + cls.indexes(dataset.data)
        not_found = [d for d in dimensions if d not in cols]
        if not_found:
            raise DataError("Supplied data does not contain specified "
                            "dimensions, the following dimensions were "
                            f"not found: {not_found!r}", cls)

    @classmethod
    def range(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        if cls.isindex(dataset, dimension):
            column = cls.index_values(dataset, dimension)
        else:
            column = dataset.data[dimension.name]
        if dtype_kind(column) == 'O':
            if not isinstance(dataset.data, pd.DataFrame):
                column = column.sort(inplace=False)
            else:
                column = column.sort_values()
            try:
                column = column[~column.isin([None, pd.NA])]
            except Exception:
                pass
            if not len(column):
                return np.nan, np.nan
            if isinstance(column, pd.Index):
                return column[0], column[-1]
            return column.iloc[0], column.iloc[-1]
        else:
            if dimension.nodata is not None:
                column = cls.replace_value(column, dimension.nodata)
            cmin, cmax = finite_range(column, column.min(), column.max())
            if dtype_kind(column) == 'M' and getattr(column.dtype, 'tz', None):
                return (cmin.to_pydatetime().replace(tzinfo=None),
                        cmax.to_pydatetime().replace(tzinfo=None))
            return cmin, cmax


    @classmethod
    def concat_fn(cls, dataframes, **kwargs):
        return pd.concat(dataframes, sort=False, **kwargs)


    @classmethod
    def concat(cls, datasets, dimensions, vdims):
        dataframes = []
        for key, ds in datasets:
            data = ds.data.copy()
            for d, k in zip(dimensions, key, strict=None):
                data[d.name] = k
            dataframes.append(data)
        return cls.concat_fn(dataframes)


    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        index_dims = [dataset.get_dimension(d, strict=True) for d in dimensions]
        element_dims = [kdim for kdim in dataset.kdims
                        if kdim not in index_dims]

        group_kwargs = {}
        if group_type != 'raw' and issubclass(group_type, Element):
            group_kwargs = dict(util.get_param_values(dataset),
                                kdims=element_dims)
        group_kwargs.update(kwargs)

        # Propagate dataset
        group_kwargs['dataset'] = dataset.dataset

        group_by = [d.name for d in index_dims]
        if len(group_by) == 1 and util.PANDAS_VERSION >= (1, 5, 0):
            # Because of this deprecation warning from pandas 1.5.0:
            # In a future version of pandas, a length 1 tuple will be returned
            # when iterating over a groupby with a grouper equal to a list of length 1.
            # Don't supply a list with a single grouper to avoid this warning.
            group_by = group_by[0]
        groupby_kwargs = {"sort": False}
        if PANDAS_GE_2_1_0:
            groupby_kwargs["observed"] = False
        data = [(k, group_type(v, **group_kwargs)) for k, v in
                dataset.data.groupby(group_by, **groupby_kwargs)]
        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(data, kdims=index_dims)
        else:
            return container_type(data)


    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        cols = [d.name for d in dataset.kdims if d in dimensions]
        vdims = dataset.dimensions('value', label='name')
        reindexed = cls.dframe(dataset, dimensions=cols+vdims)
        if function in [np.std, np.var]:
            # Fix for consistency with other backend
            # pandas uses ddof=1 for std and var
            fn = lambda x: function(x, ddof=0)
        else:
            fn = util._PANDAS_FUNC_LOOKUP.get(function, function)
        if len(dimensions):
            # The reason to use `numeric_cols` is to prepare for when pandas will not
            # automatically drop columns that are not numerical for numerical
            # functions, e.g., `np.mean`.
            # pandas started warning about this in v1.5.0
            if function in [np.size]:
                # np.size actually works with non-numerical columns
                numeric_cols = [
                    c for c in reindexed.columns if c not in cols
                ]
            else:
                from pandas.api.types import is_numeric_dtype
                numeric_cols = [
                    c for c, d in zip(reindexed.columns, reindexed.dtypes, strict=None)
                    if is_numeric_dtype(d) and c not in cols
                ]
            groupby_kwargs = {"sort": False}
            if PANDAS_GE_2_1_0:
                groupby_kwargs["observed"] = False
            grouped = reindexed.groupby(cols, **groupby_kwargs)
            df = grouped[numeric_cols].aggregate(fn, **kwargs).reset_index()
        else:
            agg = reindexed.apply(fn, **kwargs)
            data = {col: [v] for col, v in zip(agg.index, agg.values, strict=None)}
            df = pd.DataFrame(data, columns=list(agg.index))

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
        if len(data) != 1 or len(data.columns) > 1:
            return data
        return data.iat[0,0]


    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        data = dataset.data
        if isinstance(data.index, pd.MultiIndex):
            kdims = [kdims] if isinstance(kdims, (str, Dimension)) else kdims
            data = data.reset_index().set_index(list(map(str, kdims)), drop=True)
        return data

    @classmethod
    def mask(cls, dataset, mask, mask_value=np.nan):
        masked = dataset.data.copy()
        cols = [vd.name for vd in dataset.vdims]
        masked.loc[mask, cols] = mask_value
        return masked

    @classmethod
    def redim(cls, dataset, dimensions):
        column_renames = {k: v.name for k, v in dimensions.items()}
        return dataset.data.rename(columns=column_renames)


    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        cols = [dataset.get_dimension(d, strict=True).name for d in by]

        if not isinstance(dataset.data, pd.DataFrame):
            return dataset.data.sort(columns=cols, ascending=not reverse)
        return dataset.data.sort_values(by=cols, ascending=not reverse)

    @classmethod
    def sorted_index(cls, df):
        if hasattr(df.index, 'is_lexsorted'):
            return df.index.is_lexsorted()
        return df.index.is_monotonic_increasing

    @classmethod
    def sort_depth(cls, df):
        try:
            from pandas.core.indexes.multi import _lexsort_depth
            return _lexsort_depth(df.index.codes, df.index.nlevels)
        except (ImportError, AttributeError):
            return 0

    @classmethod
    def index_selection(cls, df, selection):
        indexes = cls.indexes(df)
        nindex = len(indexes)
        sorted_index = cls.sorted_index(df)
        if sorted_index:
            depth = df.index.nlevels
        else:
            depth = cls.sort_depth(df)
        index_sel = {}
        skip_index = True
        for level, idx in enumerate(indexes):
            if idx not in selection:
                index_sel[idx] = slice(None, None)
                continue
            skip_index = False
            sel = selection[idx]
            if isinstance(sel, tuple) and len(sel) < 4:
                sel = slice(*sel)
            elif not isinstance(sel, (list, slice)):
                sel = [sel]
            if isinstance(sel, slice) and nindex > 1 and not sorted_index and level>depth:
                # If the index is not monotonic we cannot slice
                # so return indexer up to the point it is valid
                return index_sel
            index_sel[idx] = sel
        return {} if skip_index else index_sel

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        df = dataset.data
        if selection_mask is None:
            if index_sel:= cls.index_selection(df, selection):
                try:
                    if len(index_sel) == 1:
                        df = df[next(iter(index_sel.values()))]
                    else:
                        df = df.loc[tuple(index_sel.values()), :]
                except KeyError:
                    # If index lookup fails we fall back to boolean indexing
                    index_sel = {}
            column_sel = {k: v for k, v in selection.items() if k not in index_sel}
            if column_sel:
                selection_mask = cls.select_mask(dataset.clone(data=df), column_sel)

        indexed = cls.indexed(dataset, selection)
        if isinstance(selection_mask, pd.Series):
            df = df[selection_mask]
        elif selection_mask is not None:
            df = df.iloc[selection_mask]
        if indexed and len(df) == 1 and len(dataset.vdims) == 1:
            return df[dataset.vdims[0].name].iloc[0]
        return df

    @classmethod
    def values(
        cls,
        dataset,
        dim,
        expanded=True,
        flat=True,
        compute=True,
        keep_index=False,
    ):
        dim = dataset.get_dimension(dim, strict=True)
        isindex = cls.isindex(dataset, dim)
        if isindex:
            data = cls.index_values(dataset, dim)
        else:
            data = dataset.data[dim.name]
        if keep_index:
            return data
        if dtype_kind(data) == 'M' and getattr(data.dtype, 'tz', None):
            data = (data if isindex else data.dt).tz_localize(None)
        if not expanded:
            return pd.unique(data)
        if hasattr(data, 'values'):
            data = data.values
            # https://pandas.pydata.org/docs/dev/user_guide/copy_on_write.html#read-only-numpy-arrays
            if hasattr(data, "flags") and not data.flags.writeable:
                data = data.copy()
        return data


    @classmethod
    def sample(cls, dataset, samples=None):
        if samples is None:
            samples = []
        data = dataset.data
        mask = None
        for sample in samples:
            sample_mask = None
            if np.isscalar(sample): sample = [sample]
            for i, v in enumerate(sample):
                submask = data.iloc[:, i]==v
                if sample_mask is None:
                    sample_mask = submask
                else:
                    sample_mask &= submask
            if mask is None:
                mask = sample_mask
            else:
                mask |= sample_mask
        return data[mask]


    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        data = dataset.data.copy()
        if dimension.name not in data:
            data.insert(dim_pos, dimension.name, values)
        return data

    @classmethod
    def assign(cls, dataset, new_data):
        return dataset.data.assign(**new_data)

    @classmethod
    def as_dframe(cls, dataset):
        """Returns the data of a Dataset as a dataframe avoiding copying
        if it already a dataframe type.

        """
        if issubclass(dataset.interface, PandasInterface):
            if any(cls.isindex(dataset, dim) for dim in dataset.dimensions()):
                return dataset.data.reset_index()
            return dataset.data
        else:
            return dataset.dframe()

    @classmethod
    def dframe(cls, dataset, dimensions):
        data = dataset.data
        if dimensions:
            if any(cls.isindex(dataset, d) for d in dimensions):
                data = data.reset_index()
            return data[dimensions]
        else:
            return data.copy()

    @classmethod
    def iloc(cls, dataset, index):
        rows, cols = index
        scalar = False
        if isinstance(cols, slice):
            cols = [d.name for d in dataset.dimensions()][cols]
        elif np.isscalar(cols):
            scalar = np.isscalar(rows)
            dim = dataset.get_dimension(cols)
            if dim is None:
                raise ValueError('column is out of bounds')
            cols = [dim.name]
        else:
            cols = [dataset.get_dimension(d).name for d in cols]
        if np.isscalar(rows):
            rows = [rows]

        data = dataset.data
        indexes = cls.indexes(data)
        columns = list(data.columns)
        id_cols = [columns.index(c) for c in cols if c not in indexes]
        if not id_cols:
            if len(indexes) > 1:
                data = data.index.to_frame()[cols].iloc[rows].reset_index(drop=True)
                data = data.values.ravel()[0] if scalar else data
            else:
                data = data.index.values[rows[0]] if scalar else data.index[rows]
            return data
        if scalar:
            return data.iloc[rows[0], id_cols[0]]
        return data.iloc[rows, id_cols]


Interface.register(PandasInterface)
