import numpy as np

from .. import util
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from .dictionary import DictInterface
from .interface import DataError, Interface


class MultiInterface(Interface):
    """MultiInterface allows wrapping around a list of tabular datasets
    including dataframes, the columnar dictionary format or 2D tabular
    NumPy arrays. Using the split method the list of tabular data can
    be split into individual datasets.

    The interface makes the data appear a list of tabular datasets as
    a single dataset. The interface may be used to represent geometries
    so the behavior depends on the type of geometry being represented.

    """

    types = ()

    datatype = 'multitabular'

    subtypes = ['dictionary', 'dataframe', 'array', 'dask', 'narwhals']

    geom_types = ['Polygon', 'Ring', 'Line', 'Point']

    multi = True

    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        from ...element import Path, Polygons

        new_data = []
        dims = {'kdims': eltype.kdims, 'vdims': eltype.vdims}
        if kdims is not None:
            dims['kdims'] = kdims
        if vdims is not None:
            dims['vdims'] = vdims

        if (isinstance(data, list) and len(data) and
            all(isinstance(d, tuple) and all(util.isscalar(v) for v in d) for d in data)):
            data = [data]
        elif not isinstance(data, list):
            interface  = [Interface.interfaces.get(st).applies(data)
                          for st in cls.subtypes if st in Interface.interfaces]
            if (interface or isinstance(data, tuple)) and issubclass(eltype, Path):
                data = [data]
            else:
                raise ValueError('MultiInterface data must be a list of tabular data types.')
        prev_interface, prev_dims = None, None
        for d in data:
            datatype = cls.subtypes
            if isinstance(d, dict):
                if Polygons._hole_key in d:
                    datatype = [dt for dt in datatype
                                if hasattr(Interface.interfaces.get(dt), 'has_holes')]
                geom_type = d.get('geom_type')
                if geom_type is not None and geom_type not in cls.geom_types:
                    raise DataError(f"Geometry type '{geom_type}' not recognized, "
                                    f"must be one of {cls.geom_types}.")
                else:
                    datatype = [dt for dt in datatype
                                if hasattr(Interface.interfaces.get(dt), 'geom_type')]
            d, interface, dims, _ = Interface.initialize(eltype, d, kdims, vdims,
                                                         datatype=datatype)
            if prev_interface:
                if prev_interface != interface:
                    raise DataError('MultiInterface subpaths must all have matching datatype.', cls)
                if dims['kdims'] != prev_dims['kdims']:
                    raise DataError('MultiInterface subpaths must all have matching kdims.', cls)
                if dims['vdims'] != prev_dims['vdims']:
                    raise DataError('MultiInterface subpaths must all have matching vdims.', cls)
            new_data.append(d)
            prev_interface, prev_dims = interface, dims
        return new_data, dims, {}

    @classmethod
    def validate(cls, dataset, vdims=True):
        if not dataset.data:
            return

        from holoviews.element import Polygons
        ds = cls._inner_dataset_template(dataset, validate_vdims=vdims)
        for d in dataset.data:
            ds.data = d
            ds.interface.validate(ds, vdims)
            if isinstance(dataset, Polygons) and ds.interface is DictInterface:
                holes = ds.interface.holes(ds)
                if not isinstance(holes, list):
                    raise DataError('Polygons holes must be declared as a list-of-lists.', cls)
                subholes = holes[0]
                coords = ds.data[ds.kdims[0].name]
                splits = np.isnan(coords.astype('float')).sum()
                if len(subholes) != (splits+1):
                    raise DataError('Polygons with holes containing multi-geometries '
                                    'must declare a list of holes for each geometry.', cls)


    @classmethod
    def geom_type(cls, dataset):
        from holoviews.element import Path, Points, Polygons
        if isinstance(dataset, type):
            eltype = dataset
        else:
            eltype = type(dataset)
            if isinstance(dataset.data, list):
                ds = cls._inner_dataset_template(dataset)
                if hasattr(ds.interface, 'geom_type'):
                    geom_type = ds.interface.geom_type(ds)
                    if geom_type is not None:
                        return geom_type
        if issubclass(eltype, Polygons):
            return 'Polygon'
        elif issubclass(eltype, Path):
            return 'Line'
        elif issubclass(eltype, Points):
            return 'Point'

    @classmethod
    def _inner_dataset_template(cls, dataset, validate_vdims=True):
        """Returns a Dataset template used as a wrapper around the data
        contained within the multi-interface dataset.

        """
        from . import Dataset
        vdims = dataset.vdims if getattr(dataset, 'level', None) is None else []
        return Dataset(dataset.data[0], datatype=cls.subtypes,
                       kdims=dataset.kdims, vdims=vdims,
                       _validate_vdims=validate_vdims)

    @classmethod
    def assign(cls, dataset, new_data):
        ds = cls._inner_dataset_template(dataset)
        assigned = []
        for i, d in enumerate(dataset.data):
            ds.data = d
            new = ds.interface.assign(ds, {k: v[i:i+1] for k, v in new_data.items()})
            assigned.append(new)
        return assigned

    @classmethod
    def dimension_type(cls, dataset, dim):
        if not dataset.data:
            # Note: Required to make empty datasets work at all (should fix)
            # Other interfaces declare equivalent of empty array
            # which defaults to float type
            return float
        ds = cls._inner_dataset_template(dataset)
        return ds.interface.dimension_type(ds, dim)

    @classmethod
    def range(cls, dataset, dim):
        if not dataset.data:
            return (None, None)
        ranges = []
        ds = cls._inner_dataset_template(dataset)

        # Backward compatibility for Contours/Polygons level
        level = getattr(dataset, 'level', None)
        dim = dataset.get_dimension(dim)
        if level is not None and dim is dataset.vdims[0]:
            return (level, level)

        for d in dataset.data:
            ds.data = d
            ranges.append(ds.interface.range(ds, dim))
        return util.max_range(ranges)

    @classmethod
    def has_holes(cls, dataset):
        if not dataset.data:
            return False
        ds = cls._inner_dataset_template(dataset)
        for d in dataset.data:
            ds.data = d
            if ds.interface.has_holes(ds):
                return True
        return False

    @classmethod
    def holes(cls, dataset):
        holes = []
        if not dataset.data:
            return holes
        ds = cls._inner_dataset_template(dataset)
        for d in dataset.data:
            ds.data = d
            holes += ds.interface.holes(ds)
        return holes

    @classmethod
    def isscalar(cls, dataset, dim, per_geom=False):
        """Tests if dimension is scalar in each subpath.

        """
        if not dataset.data:
            return True
        geom_type = cls.geom_type(dataset)
        ds = cls._inner_dataset_template(dataset)
        combined = []
        for d in dataset.data:
            ds.data = d
            values = ds.interface.values(ds, str(dim), expanded=False)
            unique = list(util.unique_iterator(values))
            if len(unique) > 1:
                return False
            elif per_geom and geom_type != 'Point':
                continue
            unique = unique[0]
            if unique not in combined:
                if combined:
                    return False
                combined.append(unique)
        return True

    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        """Applies selectiong on all the subpaths.

        """
        from ...element import Polygons
        if not dataset.data:
            return dataset.data
        elif selection_mask is not None:
            return [d for b, d in zip(selection_mask, dataset.data, strict=None) if b]
        ds = cls._inner_dataset_template(dataset)
        skipped = (Polygons._hole_key,)
        if hasattr(ds.interface, 'geo_column'):
            skipped += (ds.interface.geo_column(ds),)
        data = []
        for d in dataset.data:
            ds.data = d
            selection_mask = ds.interface.select_mask(ds, selection)
            sel = ds.interface.select(ds, selection_mask)
            is_dict = isinstance(sel, dict)
            if ((not len(sel) and not is_dict) or
                (is_dict and any(False if util.isscalar(v) else len(v) == 0
                                 for k, v in sel.items() if k not in skipped))):
                continue
            data.append(sel)
        return data

    @classmethod
    def select_paths(cls, dataset, index):
        """Allows selecting paths with usual NumPy slicing index.

        """
        selection = np.array([{0: p} for p in dataset.data])[index]
        if isinstance(selection, dict):
            return [selection[0]]
        return [s[0] for s in selection]

    @classmethod
    def aggregate(cls, dataset, dimensions, function, **kwargs):
        raise NotImplementedError('Aggregation currently not implemented')

    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        # Get dimensions information
        dimensions = [dataset.get_dimension(d) for d in dimensions]
        kdims = [kdim for kdim in dataset.kdims if kdim not in dimensions]

        # Update the kwargs appropriately for Element group types
        group_kwargs = {}
        group_type = list if group_type == 'raw' else group_type
        if issubclass(group_type, Element):
            group_kwargs.update(util.get_param_values(dataset))
            group_kwargs['kdims'] = kdims
        group_kwargs.update(kwargs)

        # Find all the keys along supplied dimensions
        values = []
        for d in dimensions:
            if not cls.isscalar(dataset, d, True):
                raise ValueError('MultiInterface can only apply groupby '
                                 f'on scalar dimensions, {d} dimension '
                                 'is not scalar')
            vals = cls.values(dataset, d, False, True)
            values.append(vals)
        values = tuple(values)

        # Iterate over the unique entries applying selection masks
        from . import Dataset
        ds = Dataset(values, dimensions)
        keys = (tuple(vals[i] for vals in values) for i in range(len(vals)))
        grouped_data = []
        for unique_key in util.unique_iterator(keys):
            mask = ds.interface.select_mask(ds, dict(zip(dimensions, unique_key, strict=None)))
            selection = [data for data, m in zip(dataset.data, mask, strict=None) if m]
            group_data = group_type(selection, **group_kwargs)
            grouped_data.append((unique_key, group_data))

        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(grouped_data, kdims=dimensions)
        else:
            return container_type(grouped_data)

    @classmethod
    def sample(cls, dataset, samples=None):
        if samples is None:
            samples = []
        raise NotImplementedError('Sampling operation on subpaths not supported')

    @classmethod
    def shape(cls, dataset):
        """Returns the shape of all subpaths, making it appear like a
        single array of concatenated subpaths separated by NaN values.

        """
        if not dataset.data:
            return (0, len(dataset.dimensions()))
        elif cls.geom_type(dataset) != 'Point':
            return (len(dataset.data), len(dataset.dimensions()))

        rows, cols = 0, 0
        ds = cls._inner_dataset_template(dataset)
        for d in dataset.data:
            ds.data = d
            r, cols = ds.interface.shape(ds)
            rows += r
        return rows, cols

    @classmethod
    def length(cls, dataset):
        """Returns the length of the multi-tabular dataset making it appear
        like a single array of concatenated subpaths separated by NaN
        values.

        """
        if not dataset.data:
            return 0
        elif cls.geom_type(dataset) != 'Point':
            return len(dataset.data)
        length = 0
        ds = cls._inner_dataset_template(dataset)
        for d in dataset.data:
            ds.data = d
            length += ds.interface.length(ds)
        return length

    @classmethod
    def dtype(cls, dataset, dimension):
        if not dataset.data:
            return np.dtype('float')
        ds = cls._inner_dataset_template(dataset)
        return ds.interface.dtype(ds, dimension)

    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        by = [dataset.get_dimension(d).name for d in by]
        if len(by) == 1:
            sorting = cls.values(dataset, by[0], False).argsort()
        else:
            arrays = [dataset.dimension_values(d, False) for d in by]
            sorting = util.arglexsort(arrays)
        return [dataset.data[s] for s in sorting]

    @classmethod
    def nonzero(cls, dataset):
        return bool(dataset.data)

    @classmethod
    def reindex(cls, dataset, kdims=None, vdims=None):
        new_data = []
        ds = cls._inner_dataset_template(dataset)
        for d in dataset.data:
            ds.data = d
            new_data.append(ds.reindex(kdims, vdims))
        return new_data

    @classmethod
    def redim(cls, dataset, dimensions):
        if not dataset.data:
            return dataset.data
        new_data = []
        ds = cls._inner_dataset_template(dataset)
        for d in dataset.data:
            ds.data = d
            new_data.append(ds.interface.redim(ds, dimensions))
        return new_data

    @classmethod
    def values(cls, dataset, dimension, expanded=True, flat=True,
               compute=True, keep_index=False):
        """Returns a single concatenated array of all subpaths separated
        by NaN values. If expanded keyword is False an array of arrays
        is returned.

        """
        if not dataset.data:
            return np.array([])
        values, scalars = [], []
        all_scalar = True
        ds = cls._inner_dataset_template(dataset)
        geom_type = cls.geom_type(dataset)
        is_points = geom_type == 'Point'
        is_geom = dimension in dataset.kdims[:2]
        for d in dataset.data:
            ds.data = d
            dvals = ds.interface.values(
                ds, dimension, True, flat, compute, keep_index
            )
            scalar = len(util.unique_array(dvals)) == 1 and not is_geom
            gt = ds.interface.geom_type(ds) if hasattr(ds.interface, 'geom_type') else None

            if gt is None:
                gt = geom_type

            if (gt in ('Polygon', 'Ring') and (not scalar or expanded) and
                not geom_type == 'Points' and len(dvals)):
                gvals = ds.array([0, 1])
                dvals = ensure_ring(gvals, dvals)
            if scalar and not expanded:
                dvals = dvals[:1]
            all_scalar &= scalar

            scalars.append(scalar)
            if not len(dvals):
                continue
            values.append(dvals)
            if not is_points and expanded:
                values.append([np.nan])

        if not values:
            return np.array([])
        elif expanded or (all_scalar and not is_geom):
            if not is_points and expanded:
                values = values[:-1]
            return np.concatenate(values) if values else np.array([])
        else:
            array = np.empty(len(values), dtype=object)
            array[:] = [a[0] if s else a for s, a in zip(scalars, values, strict=None)]
            return array

    @classmethod
    def split(cls, dataset, start, end, datatype, **kwargs):
        """Splits a multi-interface Dataset into regular Datasets using
        regular tabular interfaces.

        """
        objs = []
        if datatype is None:
            for d in dataset.data[start: end]:
                objs.append(dataset.clone([d]))
            return objs
        elif not dataset.data:
            return objs

        geom_type = cls.geom_type(dataset)
        ds = dataset.clone([])
        for d in dataset.data[start:end]:
            ds.data = [d]
            if datatype == 'array':
                obj = ds.array(**kwargs)
            elif datatype == 'dataframe':
                obj = ds.dframe(**kwargs)
            elif datatype in ('columns', 'dictionary'):
                if hasattr(ds.interface, 'geom_type'):
                    gt = ds.interface.geom_type(ds)
                if gt is None:
                    gt = geom_type
                if isinstance(ds.data[0], dict):
                    obj = dict(ds.data[0])
                    xd, yd = ds.kdims
                    if (geom_type in ('Polygon', 'Ring') or
                        xd not in obj or yd not in obj):
                        obj[xd.name] = ds.interface.values(ds, xd)
                        obj[yd.name] = ds.interface.values(ds, yd)
                else:
                    obj = ds.columns()
                if gt is not None:
                    obj['geom_type'] = gt
            else:
                raise ValueError(f"{datatype} datatype not support")
            objs.append(obj)
        return objs

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        if not len(dataset.data):
            return dataset.data
        elif values is None or util.isscalar(values):
            values = [values]*len(dataset.data)
        elif not len(values) == len(dataset.data):
            raise ValueError('Added dimension values must be scalar or '
                             'match the length of the data.')

        new_data = []
        template = cls._inner_dataset_template(dataset)
        array_type = template.interface.datatype == 'array'
        for d, v in zip(dataset.data, values, strict=None):
            template.data = d
            if array_type:
                ds = template.clone(template.columns())
            else:
                ds = template
            new_data.append(ds.interface.add_dimension(ds, dimension, dim_pos, v, vdim))
        return new_data

    @classmethod
    def iloc(cls, dataset, index):
        rows, cols = index
        scalar = np.isscalar(cols) and np.isscalar(rows)

        template = cls._inner_dataset_template(dataset)
        if cls.geom_type(dataset) != 'Point':
            geoms = cls.select_paths(dataset, rows)
            new_data = []
            for d in geoms:
                template.data = d
                new_data.append(template.iloc[:, cols].data)
            return new_data

        count = 0
        new_data = []
        for d in dataset.data:
            template.data = d
            length = len(template)
            if np.isscalar(rows):
                if (count+length) > rows >= count:
                    data = template.iloc[rows-count, cols]
                    return data if scalar else [data.data]
            elif isinstance(rows, slice):
                if rows.start is not None and rows.start > (count+length):
                    continue
                elif rows.stop is not None and rows.stop < count:
                    break
                start = None if rows.start is None else max(rows.start - count, 0)
                stop = None if rows.stop is None else min(rows.stop - count, length)
                if rows.step is not None:
                    dataset.param.warning(".iloc step slicing currently not supported for"
                                          "the multi-tabular data format.")
                slc = slice(start, stop)
                new_data.append(template.iloc[slc, cols].data)
            else:
                sub_rows = [r-count for r in rows if 0 <= (r-count) < (count+length)]
                new = template.iloc[sub_rows, cols]
                if len(new):
                    new_data.append(new.data)
            count += length
        return new_data


def ensure_ring(geom, values=None):
    """Ensure the (multi-)geometry forms a ring.

    Checks the start- and end-point of each geometry to ensure they
    form a ring, if not the start point is inserted at the end point.
    If a values array is provided (which must match the geometry in
    length) then the insertion will occur on the values instead,
    ensuring that they will match the ring geometry.

    Parameters
    ----------
        geom
            2-D array of geometry coordinates
        values
            Optional array of values

    Returns
    -------
    Array where values have been inserted and ring closing indexes

    """
    if values is None:
        values = geom

    breaks = np.where(np.isnan(geom.astype('float')).sum(axis=1))[0]
    starts = [0, *(breaks + 1)]
    ends = [*(breaks - 1), len(geom) - 1]
    zipped = zip(geom[starts], geom[ends], ends, values[starts], strict=None)
    unpacked = tuple(zip(*[(v, i+1) for s, e, i, v in zipped
                     if (s!=e).any()], strict=None))
    if not unpacked:
        return values
    inserts, inds = unpacked
    return np.insert(values, list(inds), list(inserts), axis=0)


Interface.register(MultiInterface)
