import builtins

import narwhals.stable.v2 as nw
import numpy as np

from .. import util
from ..dimension import Dimension, dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from .interface import DataError, Interface

_AGG_FUNC_LOOKUP = {
    builtins.sum: "sum",
    builtins.max: "max",
    builtins.min: "min",
    np.all: "all",
    np.any: "any",
    np.sum: "sum",
    np.nansum: "sum",
    np.mean: "mean",
    np.nanmean: "mean",
    # np.prod: "prod",
    # np.nanprod: "prod",
    np.std: "std",
    np.nanstd: "std",
    np.var: "var",
    np.nanvar: "var",
    np.median: "median",
    np.nanmedian: "median",
    np.max: "max",
    np.nanmax: "max",
    np.min: "min",
    np.nanmin: "min",
    np.size: "len",
}

_EAGER_TYPE = {
    nw.Implementation.DASK: nw.Implementation.PANDAS,
    nw.Implementation.IBIS: nw.Implementation.PYARROW,
    nw.Implementation.DUCKDB: nw.Implementation.PYARROW,
}

# Does not support drop_nulls
_NO_DROP_NULL = [nw.Implementation.DUCKDB, nw.Implementation.IBIS]


def _is_geodataframe(obj) -> True:
    # Do not use for GeoPandas and SpatialPandas
    try:
        return "geo" in type(obj).__name__.lower()
    except Exception:
        return False


class NarwhalsDtype:
    __slots__ = ("dtype",)

    def __init__(self, dtype):
        self.dtype = dtype

    @property
    def kind(self):
        return util.dtype_kind(self)

    def __repr__(self):
        return repr(self.dtype)

    @property
    def type(self):
        return type(self.dtype)


class NarwhalsInterface(Interface):
    datatype = "narwhals"
    narwhals_backend = None

    @classmethod
    def applies(cls, obj):
        return (
            isinstance(obj, (nw.Series, nw.DataFrame, nw.LazyFrame))
            or nw.dependencies.is_into_dataframe(obj)
            or nw.dependencies.is_into_series(obj)
            or nw.dependencies.is_ibis_table(obj)
            or bool(cls.narwhals_backend and isinstance(obj, (dict, tuple, np.ndarray)))
        ) and not _is_geodataframe(obj)

    @classmethod
    def dimension_type(cls, dataset, dim):
        return cls.dtype(dataset, dim)

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        element_params = eltype.param.objects()
        kdim_param = element_params["kdims"]
        vdim_param = element_params["vdims"]

        if cls.narwhals_backend and isinstance(data, dict):
            data = nw.from_dict(data, backend=cls.narwhals_backend)
        elif cls.narwhals_backend and isinstance(data, tuple):
            dims = map(str, (*(kdims or ()), *(vdims or ())))
            data = nw.from_dict(
                dict(zip(dims, data, strict=False)), backend=cls.narwhals_backend
            )
        elif cls.narwhals_backend and isinstance(data, np.ndarray):
            dims = list(map(str, (*(kdims or ()), *(vdims or ()))))
            data = nw.from_numpy(data, schema=dims, backend=cls.narwhals_backend)
        else:
            data = nw.from_native(data, allow_series=True)

        if isinstance(data, nw.Series):
            name = data.name or util.anonymous_dimension_label
            # Currently does not work: data = data.to_frame(name=name)
            data = data.rename(name).to_frame()
        if isinstance(data, (nw.DataFrame, nw.LazyFrame)):
            if isinstance(kdim_param.bounds[1], int):
                ndim = min([kdim_param.bounds[1], len(kdim_param.default)])
            else:
                ndim = None
            nvdim = (
                vdim_param.bounds[1] if isinstance(vdim_param.bounds[1], int) else None
            )
            columns = list(data.collect_schema())
            if kdims and vdims is None:
                vdims = [c for c in columns if c not in kdims]
            elif vdims and kdims is None:
                kdims = [c for c in columns if c not in vdims][:ndim]
            elif kdims is None:
                kdims = list(columns[:ndim])
                if vdims is None:
                    vdims = [
                        d
                        for d in columns[ndim : ((ndim + nvdim) if nvdim else None)]
                        if d not in kdims
                    ]
            elif kdims == [] and vdims is None:
                vdims = list(columns[: nvdim if nvdim else None])

            if any(not isinstance(d, (str, Dimension)) for d in kdims + vdims):
                raise DataError(
                    "Having a non-string as a column name in a DataFrame is not supported."
                )
            for d in kdims + vdims:
                d = dimension_name(d)
                if len([c for c in columns if c == d]) > 1:
                    raise DataError(
                        "Dimensions may not reference duplicated DataFrame "
                        f"columns (found duplicate {d!r} columns). If you want to plot "
                        "a column against itself simply declare two dimensions "
                        "with the same name.",
                        cls,
                    )
        return data, {"kdims": kdims, "vdims": vdims}, {}

    @classmethod
    def isscalar(cls, dataset, dim):
        name = dataset.get_dimension(dim, strict=True).name
        return len(dataset.data[name].unique()) == 1

    @classmethod
    def dtype(cls, dataset, dimension):
        dim = dataset.get_dimension(dimension, strict=True)
        nw_type = dataset.data.collect_schema()[dim.name]
        return NarwhalsDtype(nw_type)

    @classmethod
    def validate(cls, dataset, vdims=True):
        dim_types = "all" if vdims else "key"
        dimensions = dataset.dimensions(dim_types, label="name")
        cols = list(dataset.data.collect_schema())
        not_found = [d for d in dimensions if d not in cols]
        if not_found:
            raise DataError(
                "Supplied data does not contain specified "
                "dimensions, the following dimensions were "
                f"not found: {not_found!r}",
                cls,
            )

    @classmethod
    def range(cls, dataset, dimension):
        dimension = dataset.get_dimension(dimension, strict=True)
        dtype = cls.dtype(dataset, dimension)
        name = dimension.name
        is_lazy = isinstance(dataset.data, nw.LazyFrame)
        df_column = dataset.data.select(name)
        if util.dtype_kind(dtype) == "O":
            df_column = df_column.sort(by=name)
            cmin, cmax = df_column.head(0), df_column.tail(0)
            if is_lazy:
                cmin, cmax = cmin.collect(), cmax.collect()
            if not len(cmin):
                return np.nan, np.nan
            return cmin.item(), cmax.item()
        else:
            col = nw.col(name)
            if dimension.nodata is not None:
                df_column = df_column.select(
                    nw.when(col != dimension.nodata).then(col)
                )
            if dataset.data.implementation not in _NO_DROP_NULL:
                col = nw.col(name).drop_nulls()
            # NOTE: Some narwhals backends (duckdb) will return nan as
            # the max value
            df_column = df_column.select(cmin=col.min(), cmax=col.max())
            if is_lazy:
                df_column = df_column.collect()
            if not len(df_column):
                return np.nan, np.nan
            return df_column.item(0, "cmin"), df_column.item(0, "cmax")

    @classmethod
    def concat_fn(cls, dataframes, **kwargs):
        return nw.concat(dataframes, **kwargs)

    @classmethod
    def concat(cls, datasets, dimensions, vdims):
        dataframes = []
        for key, ds in datasets:
            data = cls._narwhals_clone(ds.data)
            new_columns = [
                nw.lit(val).alias(dim.name)
                for dim, val in zip(dimensions, key, strict=None)
            ]
            data = data.with_columns(new_columns)
            dataframes.append(data)
        return cls.concat_fn(dataframes)

    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        index_dims = [dataset.get_dimension(d, strict=True) for d in dimensions]
        element_dims = [kdim for kdim in dataset.kdims if kdim not in index_dims]

        group_kwargs = {}
        if group_type != "raw" and issubclass(group_type, Element):
            group_kwargs = dict(util.get_param_values(dataset), kdims=element_dims)
        group_kwargs.update(kwargs)
        group_kwargs["dataset"] = dataset.dataset

        org_data = dataset.data
        if isinstance(org_data, nw.LazyFrame):
            # NOTE(LazyFrame): forced conversion
            org_data = org_data.collect()

        group_by = [d.name for d in index_dims]
        data = [
            (k, group_type(v, **group_kwargs))
            for k, v in org_data.group_by(group_by)
        ]

        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(data, kdims=index_dims)
        else:
            return container_type(data)

    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        cols = [d.name for d in dataset.kdims if d in dimensions]
        vdims = dataset.dimensions("value", label="name")
        reindexed = cls.dframe(dataset, dimensions=cols + vdims)
        function = _AGG_FUNC_LOOKUP.get(function, function)
        expr = getattr(nw.all(), function)
        expr = expr(ddof=0) if function == "var" else expr()
        if len(dimensions):
            columns = reindexed.collect_schema()
            if function in ["len"]:
                numeric_cols = [c for c in columns if c not in cols]
            else:
                numeric_cols = [
                    k
                    for k, v in columns.items()
                    if isinstance(v, nw.dtypes.NumericType)
                ]
            all_cols = list(set(numeric_cols) | set(cols))
            grouped = reindexed.select(all_cols).group_by(cols)
            df = grouped.agg(expr, **kwargs)
        else:
            df = reindexed.select(expr, **kwargs)

        dropped = []
        columns = list(df.collect_schema())
        for vd in vdims:
            if vd not in columns:
                dropped.append(vd)
        return df, dropped

    @classmethod
    def unpack_scalar(cls, dataset, data):
        """Given a dataset object and data in the appropriate format for
        the interface, return a simple scalar.

        """
        is_scalar, is_lazy = cls._is_scalar_size(data)
        if is_scalar:
            return (data.collect() if is_lazy else data).item()
        return data

    @classmethod
    def _is_scalar_size(cls, data) -> tuple[bool, bool]:
        is_lazy = isinstance(data, nw.LazyFrame)
        cols = data.collect_schema()
        ncols = len(cols)
        if ncols != 1:
            return False, is_lazy
        nrows = data.select(nw.col(next(iter(cols))).len())
        nrows = (nrows.collect() if is_lazy else nrows).item()
        return nrows == 1, is_lazy

    @classmethod
    def mask(cls, dataset, mask, mask_value=None):
        data = dataset.data
        if not dataset.vdims:
            return data

        if data.implementation != nw.Implementation.POLARS:
            # polars will convert None to Null
            mask_value = np.nan

        cols = [vd.name for vd in dataset.vdims]
        mask_series = nw.new_series(
            name="__mask__",
            values=mask,
            backend=_EAGER_TYPE.get(data.implementation, data.implementation),
        )
        return (
            data.with_columns(mask_series)
            .with_columns(
                [
                    nw.when(nw.col("__mask__"))
                    .then(nw.col(col))
                    .otherwise(mask_value)
                    .alias(col)
                    for col in cols
                ]
            )
            .drop("__mask__")
        )

    @classmethod
    def redim(cls, dataset, dimensions):
        column_renames = {k: v.name for k, v in dimensions.items()}
        return dataset.data.rename(column_renames)

    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        cols = [dataset.get_dimension(d, strict=True).name for d in by]
        return dataset.data.sort(by=cols, descending=reverse)

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        df = dataset.data
        only_scalar_selection = True
        if selection_mask is None:
            column_sel = {k: v for k, v in selection.items()}
            if column_sel:
                only_scalar_selection = all(isinstance(v, (str, int)) for v in column_sel.values())
                selection_mask = cls.select_mask(dataset, column_sel)

        if selection_mask is not None:
            if isinstance(selection_mask, np.ndarray):
                selection_mask = nw.new_series(
                    name="__mask__",
                    values=selection_mask,
                    backend=_EAGER_TYPE.get(df.implementation, df.implementation),
                )
            if not isinstance(selection_mask, nw.Expr) and isinstance(df, nw.LazyFrame):
                # NOTE(LazyFrame): forced conversion
                df = df.collect()
            df = df.filter(selection_mask)
        if selection and len(dataset.vdims) == 1:
            indexed = selection.keys() - set(map(str, dataset.kdims))
            if not indexed:
                df_vdim = df.select(str(dataset.vdims[0]))
                is_scalar, is_lazy = cls._is_scalar_size(df_vdim)
                if is_scalar and only_scalar_selection:
                    return (df_vdim.collect() if is_lazy else df_vdim).item()
        return df

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
            name = dataset.get_dimension(dim).name
            if isinstance(k, slice):
                if k.start is not None:
                    masks.append(k.start <= nw.col(name))
                if k.stop is not None:
                    masks.append(nw.col(name) < k.stop)
            elif isinstance(k, (set, list)):
                iter_slc = None
                for ik in k:
                    mask = nw.col(name) == ik
                    if iter_slc is None:
                        iter_slc = mask
                    else:
                        iter_slc |= mask
                masks.append(iter_slc)
            elif callable(k):
                masks.append(nw.col(name).pipe(k))
            else:
                masks.append(nw.col(name) == k)

            for mask in masks:
                if select_mask is not None:
                    select_mask &= mask
                else:
                    select_mask = mask
        return select_mask

    @classmethod
    def _select_mask_neighbor(cls, dataset, selection):
        """Runs select mask and expand the True values to include its neighbors

        Example

        select_mask =          [False, False, True, True, False, False]
        select_mask_neighbor = [False, True,  True, True, True,  False]

        """
        if isinstance(dataset.data, nw.LazyFrame):
            raise NotImplementedError(
                "_select_mask_neighbor does not support LazyFrame"
            )
        mask = cls.select_mask(dataset, selection)
        return mask | mask.shift(1).fill_null(False) | mask.shift(-1).fill_null(False)

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
        data = dataset.data.select(dim.name)
        is_lazy = isinstance(data, nw.LazyFrame)
        if not expanded:
            # LazyFrame does not support maintain_order, it can therefore for some
            # backends return non-deterministic results even for the same data.
            # It looks like polars.LazyFrame support it, but not duckdb
            data = data.unique(**({} if is_lazy else {"maintain_order": True}))
        if is_lazy:
            if compute:
                return data.collect()[dim.name]
            else:
                return data  # Cannot slice LazyFrame
        return data[dim.name]

    @classmethod
    def sample(cls, dataset, samples=None):
        if samples is None:
            samples = []
        data = dataset.data
        columns = list(data.collect_schema())
        mask = None
        for sample in samples:
            sample_mask = None
            if np.isscalar(sample):
                sample = [sample]
            for col, value in zip(columns, sample, strict=False):
                submask = nw.col(col) == value
                if sample_mask is None:
                    sample_mask = submask
                else:
                    sample_mask &= submask
            if mask is None:
                mask = sample_mask
            else:
                mask |= sample_mask
        return data.filter(mask)

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        data = cls._narwhals_clone(dataset.data)
        cols = list(data.collect_schema())
        if dimension.name not in cols:
            cols = [*cols[:dim_pos], dimension.name, *cols[dim_pos:]]
            if not isinstance(values, nw.Series):
                if np.isscalar(values):
                    values = nw.lit(values)
                else:
                    values = nw.new_series(
                        dimension.name,
                        values,
                        backend=_EAGER_TYPE.get(data.implementation, data.implementation),
                    )
            if isinstance(data, nw.LazyFrame) and isinstance(values, nw.Series):
                # NOTE(LazyFrame): forced conversion
                data = data.collect()
            data = data.with_columns(**{dimension.name: values}).select(cols)
        return data

    @classmethod
    def assign(cls, dataset, new_data):
        return dataset.data.with_columns(**new_data)

    @classmethod
    def as_dframe(cls, dataset):
        """Returns the data of a Dataset as a dataframe avoiding copying
        if it already a dataframe type.

        """
        if issubclass(dataset.interface, NarwhalsInterface):
            return dataset.data
        else:
            return dataset.dframe()

    @classmethod
    def dframe(cls, dataset, dimensions):
        if dimensions:
            data = dataset.data.select(dimensions)
        else:
            data = dataset.data
        return cls._narwhals_clone(data)

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
                raise ValueError("column is out of bounds")
            cols = [dim.name]
        else:
            cols = [dataset.get_dimension(d).name for d in cols]
        if np.isscalar(rows):
            rows = [rows]

        data = dataset.data
        is_lazy = isinstance(data, nw.LazyFrame)
        if scalar:
            data = data.collect() if is_lazy else data
            return data.item(rows[0], cols[0])
        if is_lazy:
            if isinstance(rows, slice) and rows == slice(None):
                return data.select(cols)  # Special case
            else:
                # NOTE(LazyFrame): forced conversion
                data = data.select(cols).collect()
                return data[rows]
        return data[rows, cols]

    @classmethod
    def nonzero(cls, dataset):
        if isinstance(dataset.data, nw.LazyFrame):
            return True
        else:
            return super().nonzero(dataset)

    @classmethod
    def compute(cls, dataset):
        """Should return a computed version of the Dataset."""
        if isinstance(dataset.data, nw.LazyFrame):
            return dataset.clone(data=dataset.data.collect())
        else:
            return dataset

    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        dims = [*(kdims or ()), *(vdims or ())]
        if dims:
            expr = nw.col(list(map(str, dims)))
            return dataset.data.select(expr)
        return dataset.data

    @classmethod
    def shape(cls, dataset):
        data = dataset.data
        if not isinstance(data, nw.LazyFrame):
            return data.shape
        cols = data.collect_schema()
        ncols = len(cols)
        if ncols == 0:
            return 0, 0
        nrows = data.select(nw.col(next(iter(cols))).len()).collect().item()
        return nrows, ncols

    @classmethod
    def _narwhals_clone(cls, data):
        if isinstance(data, nw.LazyFrame):
            # NOTE(LazyFrame): No Conversion
            # nw.LazyFrame cannot be cloned
            # though it seems like polars can
            return data
        return data.clone()

    @classmethod
    def length(cls, dataset):
        nrows, _ = cls.shape(dataset)
        return nrows

    @classmethod
    def histogram(cls, data, bins, density=True, weights=None):
        if isinstance(data, (nw.DataFrame, nw.LazyFrame)):
            columns = list(data.collect_schema())
            if len(columns) > 1:
                msg = "Histogram can only be computed for a single column"
                raise ValueError(msg)
            if isinstance(data, nw.LazyFrame):
                data = data.collect()
            data = data[columns[0]]
        return super().histogram(data.to_numpy(), bins, density, weights)


Interface.register(NarwhalsInterface)
