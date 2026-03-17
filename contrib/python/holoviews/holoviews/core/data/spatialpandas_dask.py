import sys

import numpy as np

from .dask import DaskInterface
from .interface import Interface
from .spatialpandas import SpatialPandasInterface


class DaskSpatialPandasInterface(SpatialPandasInterface):

    base_interface = DaskInterface

    datatype = 'dask_spatialpandas'

    @classmethod
    def loaded(cls):
        return 'spatialpandas.dask' in sys.modules

    @classmethod
    def data_types(cls):
        from spatialpandas.dask import DaskGeoDataFrame, DaskGeoSeries
        return (DaskGeoDataFrame, DaskGeoSeries)

    @classmethod
    def series_type(cls):
        from spatialpandas.dask import DaskGeoSeries
        return DaskGeoSeries

    @classmethod
    def frame_type(cls):
        from spatialpandas.dask import DaskGeoDataFrame
        return DaskGeoDataFrame

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        import dask
        import dask.dataframe as dd
        data, dims, params = super().init(
            eltype, data, kdims, vdims
        )
        if not isinstance(data, cls.frame_type()):
            # convert-string will convert the object dtype to string
            # even if the object is a list
            # https://github.com/dask/dask/issues/10631
            with dask.config.set({"dataframe.convert-string": False}):
                data = dd.from_pandas(data, npartitions=1)
        return data, dims, params

    @classmethod
    def partition_values(cls, df, dataset, dimension, expanded, flat):
        ds = dataset.clone(df, datatype=['spatialpandas'])
        return ds.interface.values(ds, dimension, expanded, flat)

    @classmethod
    def values(cls, dataset, dimension, expanded=True, flat=True, compute=True, keep_index=False):
        if compute and not keep_index:
            dtype = cls.dtype(dataset, dimension)
            meta = np.array([], dtype=dtype.base)
            return dataset.data.map_partitions(
                cls.partition_values, meta=meta, dataset=dataset,
                dimension=dimension, expanded=expanded, flat=flat
            ).compute()
        values = super().values(
            dataset, dimension, expanded, flat, compute, keep_index
        )
        if compute and not keep_index and hasattr(values, 'compute'):
            return values.compute()
        return values

    @classmethod
    def split(cls, dataset, start, end, datatype, **kwargs):
        ds = dataset.clone(dataset.data.compute(), datatype=['spatialpandas'])
        return ds.interface.split(ds, start, end, datatype, **kwargs)

    @classmethod
    def iloc(cls, dataset, index):
        rows, _cols = index
        if rows is not None:
            raise NotImplementedError
        return super().iloc(dataset, index)

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        return cls.base_interface.add_dimension(dataset, dimension, dim_pos, values, vdim)

    @classmethod
    def dframe(cls, dataset, dimensions):
        if dimensions:
            return dataset.data[dimensions].compute()
        else:
            return dataset.data.compute()

Interface.register(DaskSpatialPandasInterface)
