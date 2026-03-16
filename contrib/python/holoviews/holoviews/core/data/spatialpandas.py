import sys
from collections import defaultdict

import numpy as np

from ..dimension import dimension_name
from ..util import isscalar, unique_array, unique_iterator
from .interface import DataError, Interface
from .multipath import MultiInterface, ensure_ring
from .pandas import PandasInterface


class SpatialPandasInterface(MultiInterface):

    base_interface = PandasInterface

    datatype = 'spatialpandas'

    multi = True

    types = ()

    @classmethod
    def loaded(cls):
        return 'spatialpandas' in sys.modules

    @classmethod
    def applies(cls, obj):
        if not cls.loaded():
            return False
        is_sdf = isinstance(obj, cls.data_types())
        if 'geopandas' in sys.modules and 'geoviews' not in sys.modules:
            import geopandas as gpd
            is_sdf |= isinstance(obj, (gpd.GeoDataFrame, gpd.GeoSeries))
        return is_sdf

    @classmethod
    def data_types(cls):
        from spatialpandas import GeoDataFrame, GeoSeries
        return (GeoDataFrame, GeoSeries, cls.array_type())

    @classmethod
    def array_type(cls):
        from spatialpandas.geometry import GeometryArray
        return GeometryArray

    @classmethod
    def series_type(cls):
        from spatialpandas import GeoSeries
        return GeoSeries

    @classmethod
    def frame_type(cls):
        from spatialpandas import GeoDataFrame
        return GeoDataFrame

    @classmethod
    def geo_column(cls, data):
        col = 'geometry'
        stypes = cls.series_type()
        if col in data and isinstance(data[col], stypes):
            return col
        cols = [c for c in data.columns if isinstance(data[c], stypes)]
        if not cols:
            raise ValueError('No geometry column found in spatialpandas.GeoDataFrame, '
                             'use the PandasInterface instead.')
        return cols[0]

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        from spatialpandas import GeoDataFrame

        if kdims is None:
            kdims = eltype.kdims

        if vdims is None:
            vdims = eltype.vdims

        if isinstance(data, cls.series_type()):
            data = data.to_frame()

        if 'geopandas' in sys.modules:
            import geopandas as gpd
            if isinstance(data, gpd.GeoSeries):
                data = data.to_frame()
            if isinstance(data, gpd.GeoDataFrame):
                data = GeoDataFrame(data)
        if isinstance(data, list):
            if 'shapely' in sys.modules:
                data = from_shapely(data)
            if isinstance(data, list):
                data = from_multi(eltype, data, kdims, vdims)
        elif isinstance(data, cls.array_type()):
            data = GeoDataFrame({'geometry': data})
        elif not isinstance(data, cls.frame_type()):
            raise ValueError(f"{cls.__name__} only support spatialpandas DataFrames.")
        elif 'geometry' not in data:
            cls.geo_column(data)

        import pandas as pd

        index_names = data.index.names if isinstance(data, pd.DataFrame) else [data.index.name]
        if index_names == [None]:
            index_names = ['index']

        for kd in kdims+vdims:
            kd = dimension_name(kd)
            if kd in data.columns:
                continue
            if any(kd == ('index' if name is None else name)
                   for name in index_names):
                data = data.reset_index()
                break

        return data, {'kdims': kdims, 'vdims': vdims}, {}

    @classmethod
    def validate(cls, dataset, vdims=True):
        dim_types = 'key' if vdims else 'all'
        geom_dims = cls.geom_dims(dataset)
        if len(geom_dims) != 2:
            raise DataError(f'Expected {type(dataset).__name__} instance to declare two key '
                            'dimensions corresponding to the geometry '
                            f'coordinates but {len(geom_dims)} dimensions were found '
                            'which did not refer to any columns.', cls)
        not_found = [d.name for d in dataset.dimensions(dim_types)
                     if d not in geom_dims and d.name not in dataset.data]
        if not_found:
            raise DataError("Supplied data does not contain specified "
                             "dimensions, the following dimensions were "
                             f"not found: {not_found!r}", cls)

    @classmethod
    def dtype(cls, dataset, dimension):
        dim = dataset.get_dimension(dimension, strict=True)
        if dim in cls.geom_dims(dataset):
            col = cls.geo_column(dataset.data)
            return dataset.data[col].dtype.subtype
        return dataset.data[dim.name].dtype

    @classmethod
    def has_holes(cls, dataset):
        from spatialpandas.geometry import (
            MultiPolygon,
            MultiPolygonDtype,
            Polygon,
            PolygonDtype,
        )
        col = cls.geo_column(dataset.data)
        series = dataset.data[col]
        if not isinstance(series.dtype, (MultiPolygonDtype, PolygonDtype)):
            return False
        for geom in series:
            if isinstance(geom, Polygon) and len(geom.data) > 1:
                return True
            elif isinstance(geom, MultiPolygon):
                for p in geom.data:
                    if len(p) > 1:
                        return True
        return False

    @classmethod
    def holes(cls, dataset):
        holes = []
        if not len(dataset.data):
            return holes
        col = cls.geo_column(dataset.data)
        series = dataset.data[col]
        return [geom_to_holes(geom) for geom in series]

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        xdim, ydim = cls.geom_dims(dataset)
        selection.pop(xdim.name, None)
        selection.pop(ydim.name, None)
        df = dataset.data
        if not selection:
            return df
        elif selection_mask is None:
            selection_mask = cls.select_mask(dataset, selection)
        indexed = cls.indexed(dataset, selection)
        df = df[selection_mask]
        if indexed and len(df) == 1 and len(dataset.vdims) == 1:
            return df[dataset.vdims[0].name].iloc[0]
        return df

    @classmethod
    def select_mask(cls, dataset, selection):
        return cls.base_interface.select_mask(dataset, selection)

    @classmethod
    def geom_dims(cls, dataset):
        return [d for d in dataset.kdims + dataset.vdims
                if d.name not in dataset.data]

    @classmethod
    def dimension_type(cls, dataset, dim):
        dim = dataset.get_dimension(dim)
        return cls.dtype(dataset, dim).type

    @classmethod
    def isscalar(cls, dataset, dim, per_geom=False):
        """Tests if dimension is scalar in each subpath.

        """
        dim = dataset.get_dimension(dim)
        if (dim in cls.geom_dims(dataset)):
            return False
        elif per_geom:
            return all(isscalar(v) or len(list(unique_array(v))) == 1
                       for v in dataset.data[dim.name])
        dim = dataset.get_dimension(dim)
        return len(dataset.data[dim.name].unique()) == 1

    @classmethod
    def range(cls, dataset, dim):
        dim = dataset.get_dimension(dim)
        geom_dims = cls.geom_dims(dataset)
        if dim in geom_dims:
            col = cls.geo_column(dataset.data)
            idx = geom_dims.index(dim)
            bounds = dataset.data[col].total_bounds
            if idx == 0:
                return (bounds[0], bounds[2])
            else:
                return (bounds[1], bounds[3])
        else:
            return cls.base_interface.range(dataset, dim)

    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        geo_dims = cls.geom_dims(dataset)
        if any(d in geo_dims for d in dimensions):
            raise DataError("SpatialPandasInterface does not allow grouping "
                            "by geometry dimension.", cls)
        return cls.base_interface.groupby(dataset, dimensions, container_type, group_type, **kwargs)

    @classmethod
    def aggregate(cls, columns, dimensions, function, **kwargs):
        raise NotImplementedError

    @classmethod
    def sample(cls, columns, samples=None):
        if samples is None:
            samples = []
        raise NotImplementedError

    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        return dataset.data

    @classmethod
    def shape(cls, dataset):
        return (cls.length(dataset), len(dataset.dimensions()))

    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        geo_dims = cls.geom_dims(dataset)
        if any(d in geo_dims for d in by):
            raise DataError("SpatialPandasInterface does not allow sorting "
                            "by geometry dimension.", cls)
        return cls.base_interface.sort(dataset, by, reverse)

    @classmethod
    def length(cls, dataset):
        from spatialpandas.geometry import MultiPointDtype, Point
        col_name = cls.geo_column(dataset.data)
        column = dataset.data[col_name]
        geom_type = cls.geom_type(dataset)
        if not isinstance(column.dtype, MultiPointDtype) and geom_type != 'Point':
            return cls.base_interface.length(dataset)
        length = 0
        for geom in column:
            if isinstance(geom, Point):
                length += 1
            else:
                length += (len(geom.buffer_values)//2)
        return length

    @classmethod
    def nonzero(cls, dataset):
        return bool(len(dataset.data.head(1)))

    @classmethod
    def redim(cls, dataset, dimensions):
        return cls.base_interface.redim(dataset, dimensions)

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        data = dataset.data.copy()
        geom_col = cls.geo_column(dataset.data)
        if dim_pos >= list(data.columns).index(geom_col):
            dim_pos -= 1
        if dimension.name not in data:
            data.insert(dim_pos, dimension.name, values)
        return data

    @classmethod
    def iloc(cls, dataset, index):
        from spatialpandas import GeoSeries
        from spatialpandas.geometry import MultiPointDtype
        rows, cols = index
        geom_dims = cls.geom_dims(dataset)
        geom_col = cls.geo_column(dataset.data)
        scalar = False
        columns = list(dataset.data.columns)
        if isinstance(cols, slice):
            cols = [d.name for d in dataset.dimensions()][cols]
        elif np.isscalar(cols):
            scalar = np.isscalar(rows)
            cols = [dataset.get_dimension(cols).name]
        else:
            cols = [dataset.get_dimension(d).name for d in index[1]]
        if not all(d in cols for d in geom_dims):
            raise DataError("Cannot index a dimension which is part of the "
                            "geometry column of a spatialpandas DataFrame.", cls)
        cols = list(unique_iterator([
            columns.index(geom_col) if c in geom_dims else columns.index(c) for c in cols
        ]))

        if not isinstance(dataset.data[geom_col].dtype, MultiPointDtype):
            if scalar:
                return dataset.data.iloc[rows[0], cols[0]]
            elif isscalar(rows):
                rows = [rows]
            return dataset.data.iloc[rows, cols]

        geoms = dataset.data[geom_col]
        count = 0
        new_geoms, indexes = [], []
        for i, geom in enumerate(geoms):
            length = int(len(geom.buffer_values)/2)
            if np.isscalar(rows):
                if count <= rows < (count+length):
                    idx = (rows-count)*2
                    data = geom.buffer_values[idx:idx+2]
                    new_geoms.append(type(geom)(data))
                    indexes.append(i)
                    break
            elif isinstance(rows, slice):
                if rows.start is not None and rows.start > (count+length):
                    continue
                elif rows.stop is not None and rows.stop < count:
                    break
                start = None if rows.start is None else max(rows.start - count, 0)*2
                stop = None if rows.stop is None else min(rows.stop - count, length)*2
                if rows.step is not None:
                    dataset.param.warning(".iloc step slicing currently not supported for"
                                          "the multi-tabular data format.")
                sliced = geom.buffer_values[start:stop]
                if len(sliced):
                    indexes.append(i)
                    new_geoms.append(type(geom)(sliced))
            else:
                sub_rows = [v for r in rows for v in ((r-count)*2, (r-count)*2+1)
                            if count <= r < (count+length)]
                if sub_rows:
                    indexes.append(i)
                    idxs = np.array(sub_rows, dtype=int)
                    new_geoms.append(type(geom)(geom.buffer_values[idxs]))
            count += length

        new = dataset.data.iloc[indexes].copy()
        new[geom_col] = GeoSeries(new_geoms)
        return new

    @classmethod
    def values(cls, dataset, dimension, expanded=True, flat=True, compute=True, keep_index=False):
        dimension = dataset.get_dimension(dimension)
        geom_dims = dataset.interface.geom_dims(dataset)
        data = dataset.data
        isgeom = (dimension in geom_dims)
        geom_col = cls.geo_column(dataset.data)
        is_points = cls.geom_type(dataset) == 'Point'
        if isgeom and keep_index:
            return data[geom_col]
        elif not isgeom:
            if is_points:
                return data[dimension.name].values
            return get_value_array(data, dimension, expanded, keep_index, geom_col, is_points)
        elif not len(data):
            return np.array([])

        geom_type = cls.geom_type(dataset)
        index = geom_dims.index(dimension)
        geom_series = data[geom_col]
        if compute and hasattr(geom_series, 'compute'):
            geom_series = geom_series.compute()
        return geom_array_to_array(geom_series.values, index, expanded, geom_type)

    @classmethod
    def split(cls, dataset, start, end, datatype, **kwargs):
        from spatialpandas import GeoDataFrame, GeoSeries

        from ...element import Polygons

        objs = []
        if not len(dataset.data):
            return []
        xdim, ydim = cls.geom_dims(dataset)
        value_dims = [dim for dim in dataset.kdims+dataset.vdims
                      if dim not in (xdim, ydim)]
        row = dataset.data.iloc[0]
        col = cls.geo_column(dataset.data)
        geom_type = cls.geom_type(dataset)
        if datatype is not None:
            arr = geom_to_array(row[col], geom_type=geom_type)
            d = {(xdim.name, ydim.name): arr}
            d.update({dim.name: row[dim.name] for dim in value_dims})
            ds = dataset.clone(d, datatype=['dictionary'])

        holes = cls.holes(dataset) if cls.has_holes(dataset) else None
        for i, row in dataset.data.iterrows():
            if datatype is None:
                gdf = GeoDataFrame({c: GeoSeries([row[c]]) if c == 'geometry' else [row[c]]
                                    for c in dataset.data.columns})
                objs.append(dataset.clone(gdf))
                continue

            geom = row[col]
            gt = geom_type or get_geom_type(dataset.data, col)
            arr = geom_to_array(geom, geom_type=gt)
            d = {xdim.name: arr[:, 0], ydim.name: arr[:, 1]}
            d.update({dim.name: row[dim.name] for dim in value_dims})
            if datatype in ('dictionary', 'columns'):
                if holes is not None:
                    d[Polygons._hole_key] = holes[i]
                d['geom_type'] = gt
                objs.append(d)
                continue

            ds.data = d
            if datatype == 'array':
                obj = ds.array(**kwargs)
            elif datatype == 'dataframe':
                obj = ds.dframe(**kwargs)
            else:
                raise ValueError(f"{datatype} datatype not support")
            objs.append(obj)
        return objs

    @classmethod
    def dframe(cls, dataset, dimensions):
        if dimensions:
            return dataset.data[dimensions]
        else:
            return dataset.data.copy()

    @classmethod
    def as_dframe(cls, dataset):
        return dataset.data




def get_geom_type(gdf, col):
    """Return the HoloViews geometry type string for the geometry column.

    Parameters
    ----------
    gdf
        The GeoDataFrame to get the geometry from
    col
        The geometry column

    Returns
    -------
    A string representing the type of geometry
    """
    from spatialpandas.geometry import (
        LineDtype,
        MultiLineDtype,
        MultiPointDtype,
        MultiPolygonDtype,
        PointDtype,
        PolygonDtype,
        RingDtype,
    )

    column = gdf[col]
    if isinstance(column.dtype, (PointDtype, MultiPointDtype)):
        return 'Point'
    elif isinstance(column.dtype, (LineDtype, MultiLineDtype)):
        return 'Line'
    elif isinstance(column.dtype, (PolygonDtype, MultiPolygonDtype)):
        return 'Polygon'
    elif isinstance(column.dtype, RingDtype):
        return 'Ring'


def geom_to_array(geom, index=None, multi=False, geom_type=None):
    """Converts spatialpandas geometry to an array.

    Parameters
    ----------
    geom : spatialpandas geometry

    index
        The column index to return
    multi
        Whether to concatenate multiple arrays or not

    Returns
    -------
    Array or list of arrays.
    """
    from spatialpandas.geometry import (
        Line,
        MultiPoint,
        MultiPolygon,
        Point,
        Polygon,
        Ring,
    )
    if isinstance(geom, Point):
        if index is None:
            return np.array([[geom.x, geom.y]])
        arrays = [np.array([geom.y if index else geom.x])]
    elif isinstance(geom, (Polygon, Line, Ring)):
        exterior = geom.data[0] if isinstance(geom, Polygon) else geom.data
        arr = np.array(exterior.as_py()).reshape(-1, 2)
        if isinstance(geom, (Polygon, Ring)):
            arr = ensure_ring(arr)
        arrays = [arr if index is None else arr[:, index]]
    elif isinstance(geom, MultiPoint):
        if index is None:
            arrays = [np.array(geom.buffer_values).reshape(-1, 2)]
        else:
            arrays = [np.array(geom.buffer_values[index::2])]
    else:
        arrays = []
        for g in geom.data:
            exterior = g[0] if isinstance(geom, MultiPolygon) else g
            arr = np.array(exterior.as_py()).reshape(-1, 2)
            if isinstance(geom, MultiPolygon):
                arr = ensure_ring(arr)
            arrays.append(arr if index is None else arr[:, index])
            if geom_type != 'Point':
                arrays.append([[np.nan, np.nan]] if index is None else [np.nan])
        if geom_type != 'Point':
            arrays = arrays[:-1]
    if multi:
        return arrays
    elif len(arrays) == 1:
        return arrays[0]
    else:
        return np.concatenate(arrays)


def geom_array_to_array(geom_array, index, expand=False, geom_type=None):
    """Converts spatialpandas extension arrays to a flattened array.

    Parameters
    ----------
    geom : spatialpandas geometry

    index
        The column index to return

    Returns
    -------
    Flattened array
    """
    from spatialpandas.geometry import MultiPointArray, PointArray
    if isinstance(geom_array, PointArray):
        return geom_array.y if index else geom_array.x
    arrays = []
    multi_point = isinstance(geom_array, MultiPointArray) or geom_type == 'Point'
    for geom in geom_array:
        array = geom_to_array(geom, index, multi=expand, geom_type=geom_type)
        if expand:
            arrays.extend(array)
            if not multi_point:
                arrays.append([np.nan])
        else:
            arrays.append(array)
    if expand:
        if not multi_point:
            arrays = arrays[:-1]
        return np.concatenate(arrays) if arrays else np.array([])
    else:
        array = np.empty(len(arrays), dtype=object)
        array[:] = arrays
        return array


def geom_length(geom):
    from spatialpandas.geometry import MultiLine, MultiPolygon, Polygon, Ring
    if isinstance(geom, Polygon):
        offset = 0
        exterior = geom.data[0]
        if exterior[0] != exterior[-2] or exterior[1] != exterior[-1]:
            offset = 1
        return len(exterior)//2 + offset
    elif isinstance(geom, (MultiPolygon, MultiLine)):
        length = 0
        for g in geom.data:
            offset = 0
            if isinstance(geom, MultiLine):
                exterior = g
            else:
                exterior = g[0]
                if exterior[0] != exterior[-2] or exterior[1] != exterior[-1]:
                    offset = 1
            length += (len(exterior)//2 + 1) + offset
        return length-1 if length else 0
    else:
        offset = 0
        exterior = geom.buffer_values
        if isinstance(geom, Ring) and (exterior[0] != exterior[-2] or exterior[1] != exterior[-1]):
            offset = 1
        return len(exterior)//2


def get_value_array(data, dimension, expanded, keep_index, geom_col,
                    is_points, geom_length=geom_length):
    """Returns an array of values from a GeoDataFrame.

    Parameters
    ----------
    data : GeoDataFrame

    dimension
        The dimension to get the values from
    expanded
        Whether to expand the value array
    keep_index
        Whether to return a Series
    geom_col
        The column in the data that contains the geometries
    is_points
        Whether the geometries are points
    geom_length
        The function used to compute the length of each geometry

    Returns
    -------
    An array containing the values along a dimension
    """
    if not len(data):
        return np.array([])
    column = data[dimension.name]
    if keep_index:
        return column
    is_scalars = [isscalar(value) for value in column]
    all_scalar = all(is_scalars)
    if all_scalar and not expanded:
        return np.asarray(column)
    arrays, scalars = [], []
    for i, geom in enumerate(data[geom_col]):
        val = column.iloc[i]
        scalar = is_scalars[i]
        if scalar:
            val = np.array([val])
        if not scalar and len(unique_array(val)) == 1:
            val = val[:1]
            scalar = True
        scalars.append(scalar)
        if not expanded or not scalar:
            arrays.append(val)
        elif scalar:
            length = 1 if is_points else geom_length(geom)
            arrays.append(np.full(length, val))
        if expanded and not is_points and not i == (len(data[geom_col])-1):
            arrays.append(np.array([np.nan]))

    if expanded:
        return np.concatenate(arrays) if len(arrays) > 1 else arrays[0]
    elif (all_scalar and arrays):
        return np.array([a[0] for a in arrays])
    else:
        array = np.empty(len(arrays), dtype=object)
        array[:] = [a[0] if s else a for s, a in zip(scalars, arrays, strict=None)]
        return array


def geom_to_holes(geom):
    """Extracts holes from spatialpandas Polygon geometries.

    Parameters
    ----------
    geom : spatialpandas geometry

    Returns
    -------
    List of arrays representing holes
    """
    from spatialpandas.geometry import MultiPolygon, Polygon
    if isinstance(geom, Polygon):
        holes = []
        for i, hole in enumerate(geom.data):
            if i == 0:
                continue
            hole = ensure_ring(np.array(hole.as_py()).reshape(-1, 2))
            holes.append(hole)
        return [holes]
    elif isinstance(geom, MultiPolygon):
        holes = []
        for poly in geom.data:
            poly_holes = []
            for i, hole in enumerate(poly):
                if i == 0:
                    continue
                arr = ensure_ring(np.array(hole.as_py()).reshape(-1, 2))
                poly_holes.append(arr)
            holes.append(poly_holes)
        return holes
    elif 'Multi' in type(geom).__name__:
        return [[]]*len(geom)
    else:
        return [[]]


def to_spatialpandas(data, xdim, ydim, columns=None, geom='point'):
    """Converts list of dictionary format geometries to spatialpandas line geometries.

    Parameters
    ----------
    data
        List of dictionaries representing individual geometries
    xdim
        Name of x-coordinates column
    ydim
        Name of y-coordinates column
    columns
        List of columns to add
    geom
        The type of geometry

    Returns
    -------
    A spatialpandas.GeoDataFrame version of the data
    """
    from spatialpandas import GeoDataFrame, GeoSeries
    from spatialpandas.geometry import (
        Line,
        LineArray,
        MultiLineArray,
        MultiPointArray,
        MultiPolygonArray,
        Point,
        PointArray,
        Polygon,
        PolygonArray,
        Ring,
        RingArray,
    )

    from ...element import Polygons

    if columns is None:
        columns = []
    poly = any(Polygons._hole_key in d for d in data) or geom == 'Polygon'
    if poly:
        geom_type = Polygon
        single_array, multi_array = PolygonArray, MultiPolygonArray
    elif geom == 'Line':
        geom_type = Line
        single_array, multi_array = LineArray, MultiLineArray
    elif geom == 'Ring':
        geom_type = Ring
        single_array, multi_array = RingArray, MultiLineArray
    else:
        geom_type = Point
        single_array, multi_array = PointArray, MultiPointArray

    array_type = None
    hole_arrays, geom_arrays = [], []
    for geom_data in data:
        geom_data = dict(geom_data)
        if xdim not in geom_data or ydim not in geom_data:
            raise ValueError('Could not find geometry dimensions')
        xs, ys = geom_data.pop(xdim), geom_data.pop(ydim)
        xscalar, yscalar = isscalar(xs), isscalar(ys)
        if xscalar and yscalar:
            xs, ys = np.array([xs]), np.array([ys])
        elif xscalar:
            xs = np.full_like(ys, xs)
        elif yscalar:
            ys = np.full_like(xs, ys)
        geom_array = np.column_stack([xs, ys])

        if geom_type in (Polygon, Ring):
            geom_array = ensure_ring(geom_array)

        splits = np.where(np.isnan(geom_array[:, :2].astype('float')).sum(axis=1))[0]
        split_geoms = np.split(geom_array, splits+1) if len(splits) else [geom_array]
        split_holes = geom_data.pop(Polygons._hole_key, None)
        if split_holes is not None:
            if len(split_holes) != len(split_geoms):
                raise DataError('Polygons with holes containing multi-geometries '
                                'must declare a list of holes for each geometry.',
                                SpatialPandasInterface)
            else:
                split_holes = [[ensure_ring(np.asarray(h)) for h in hs] for hs in split_holes]

        geom_arrays.append(split_geoms)
        hole_arrays.append(split_holes)
        if geom_type is Point:
            if len(splits) > 1 or any(len(g) > 1 for g in split_geoms):
                array_type = multi_array
            elif array_type is None:
                array_type = single_array
        elif len(splits):
            array_type = multi_array
        elif array_type is None:
            array_type = single_array

    converted = defaultdict(list)
    for geom_data, arrays, holes in zip(data, geom_arrays, hole_arrays, strict=None):
        parts = []
        for i, g in enumerate(arrays):
            if i != (len(arrays)-1):
                g = g[:-1]
            if len(g) < (3 if poly else 2) and geom_type is not Point:
                continue
            if poly:
                parts.append([])
                subparts = parts[-1]
            else:
                subparts = parts
            subparts.append(g[:, :2])
            if poly and holes is not None:
                subparts += [np.array(h) for h in holes[i]]

        for c, v in geom_data.items():
            converted[c].append(v)

        if array_type is PointArray:
            parts = parts[0].flatten()
        elif array_type is MultiPointArray:
            parts = np.concatenate([sp.flatten() for sp in parts])
        elif array_type is multi_array:
            parts = [[ssp.flatten() for ssp in sp] if poly else sp.flatten() for sp in parts]
        else:
            parts = [np.asarray(sp).flatten() for sp in parts[0]] if poly else parts[0].flatten()
        converted['geometry'].append(parts)

    if converted:
        geometries = converted['geometry']
        if array_type is PointArray:
            geometries = np.concatenate(geometries)
        geom_array = array_type(geometries)
        if poly:
            geom_array = geom_array.oriented()
        converted['geometry'] = GeoSeries(geom_array)
    else:
        converted['geometry'] = GeoSeries(single_array([]))
    return GeoDataFrame(converted, columns=['geometry', *columns])


def to_geom_dict(eltype, data, kdims, vdims, interface=None):
    """Converts data from any list format to a dictionary based format.

    Parameters
    ----------
    eltype
        Element type to convert
    data
        The original data
    kdims
        The declared key dimensions
    vdims
        The declared value dimensions

    Returns
    -------
    A list of dictionaries containing geometry coordinates and values.
    """
    from . import Dataset

    xname, yname = (kd.name for kd in kdims[:2])
    if isinstance(data, dict):
        data = {k: v if isscalar(v) else _asarray(v) for k, v in data.items()}
        return data
    new_el = Dataset(data, kdims, vdims)
    if new_el.interface is interface:
        return new_el.data
    new_dict = {}
    for d in new_el.dimensions():
        if d in (xname, yname):
            scalar = False
        else:
            scalar = new_el.interface.isscalar(new_el, d)
        vals = new_el.dimension_values(d, not scalar)
        new_dict[d.name] = vals[0] if scalar else vals
    return new_dict


def from_multi(eltype, data, kdims, vdims):
    """Converts list formats into spatialpandas.GeoDataFrame.

    Parameters
    ----------
    eltype
        Element type to convert
    data
        The original data
    kdims
        The declared key dimensions
    vdims
        The declared value dimensions

    Returns
    -------
    A GeoDataFrame containing in the list based format.
    """
    import pandas as pd
    from spatialpandas import GeoDataFrame

    xname, yname = (kd.name for kd in kdims[:2])

    new_data, types, geom_types = [], [], []
    for d in data:
        types.append(type(d))
        new_dict = to_geom_dict(eltype, d, kdims, vdims, SpatialPandasInterface)
        if 'geom_type' in new_dict and new_dict['geom_type'] not in geom_types:
            geom_types.append(new_dict['geom_type'])
        new_data.append(new_dict)
        if not isinstance(new_data[-1], dict):
            types[-1] = type(new_data[-1])
    if len(set(types)) > 1:
        raise DataError('Mixed types not supported')
    if new_data and types[0] is GeoDataFrame:
        data = pd.concat(new_data)
    else:
        columns = [d.name for d in kdims+vdims if d not in (xname, yname)]
        if len(geom_types) == 1:
            geom = geom_types[0]
        else:
            geom = SpatialPandasInterface.geom_type(eltype)
        data = to_spatialpandas(new_data, xname, yname, columns, geom)
    return data


def from_shapely(data):
    """Converts shapely based data formats to spatialpandas.GeoDataFrame.

    Parameters
    ----------
    data
        A list of shapely objects or dictionaries containing
        shapely objects

    Returns
    -------
    A GeoDataFrame containing the shapely geometry data.
    """
    from shapely.geometry.base import BaseGeometry
    from spatialpandas import GeoDataFrame, GeoSeries

    if not data:
        pass
    elif all(isinstance(d, BaseGeometry) for d in data):
        data = GeoSeries(data).to_frame()
    elif all(isinstance(d, dict) and 'geometry' in d and isinstance(d['geometry'], BaseGeometry)
             for d in data):
        new_data = {col: [] for col in data[0]}
        for d in data:
            for col, val in d.items():
                new_data[col].append(val if isscalar(val) or isinstance(val, BaseGeometry) else np.asarray(val))
        new_data['geometry'] = GeoSeries(new_data['geometry'])
        data = GeoDataFrame(new_data)
    return data


def _asarray(v):
    """Convert input to array

    First it tries with a normal `np.asarray(v)` if this does not work
    it tries with `np.asarray(v, dtype=object)`.

    The ValueError raised is because of an inhomogeneous shape of the input,
    which raises an error in numpy v1.24 and above.

    Reason why it is not located in holoviews.core.util is that there is a already a
    function called `asarray`.

    """
    try:
        return np.asarray(v)
    except ValueError:
        return np.asarray(v, dtype=object)


Interface.register(SpatialPandasInterface)
