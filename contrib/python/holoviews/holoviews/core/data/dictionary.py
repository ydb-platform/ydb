from collections import OrderedDict, defaultdict

import numpy as np

from .. import util
from ..dimension import dimension_name
from ..element import Element
from ..ndmapping import NdMapping, item_check, sorted_context
from ..util import dtype_kind, isscalar
from .interface import DataError, Interface


class DictInterface(Interface):
    """Interface for simple dictionary-based dataset format. The dictionary
    keys correspond to the column (i.e. dimension) names and the values
    are collections representing the values in that column.

    """

    types = (dict, OrderedDict)

    datatype = 'dictionary'

    @classmethod
    def dimension_type(cls, dataset, dim):
        name = dataset.get_dimension(dim, strict=True).name
        values = dataset.data[name]
        return type(values) if isscalar(values) else values.dtype.type


    @classmethod
    def init(cls, eltype, data, kdims, vdims):
        if kdims is None:
            kdims = eltype.kdims
        if vdims is None:
            vdims = eltype.vdims

        dimensions = [dimension_name(d) for d in kdims + vdims]
        if (isinstance(data, list) and all(isinstance(d, dict) for d in data) and
            not all(c in d for d in data for c in dimensions)):
            raise ValueError('DictInterface could not find specified dimensions in the data.')
        elif isinstance(data, tuple):
            data = {d: v for d, v in zip(dimensions, data, strict=None)}
        elif util.is_dataframe(data) and all(d in data for d in dimensions):
            data = {d: data[d] for d in dimensions}
        elif isinstance(data, np.ndarray):
            if data.ndim == 1:
                if eltype._auto_indexable_1d and len(kdims)+len(vdims)>1:
                    data = np.column_stack([np.arange(len(data)), data])
                else:
                    data = np.atleast_2d(data).T
            data = {k: data[:,i] for i,k in enumerate(dimensions)}
        elif isinstance(data, list) and data == []:
            data = {key: [] for key in dimensions}
        elif isinstance(data, list) and isscalar(data[0]):
            if eltype._auto_indexable_1d:
                data = {dimensions[0]: np.arange(len(data)), dimensions[1]: data}
            else:
                data = {dimensions[0]: data}
        elif (isinstance(data, list) and isinstance(data[0], tuple) and len(data[0]) == 2
              and any(isinstance(v, tuple) for v in data[0])):
            dict_data = zip(*((util.wrap_tuple(k)+util.wrap_tuple(v))
                              for k, v in data), strict=None)
            data = {k: np.array(v) for k, v in zip(dimensions, dict_data, strict=None)}
        # Ensure that interface does not consume data of other types
        # with an iterator interface
        elif not any(isinstance(data, tuple(t for t in interface.types if t is not None))
                     for interface in cls.interfaces.values()):
            data = {k: v for k, v in zip(dimensions, zip(*data, strict=None), strict=None)}
        elif (isinstance(data, dict) and not any(isinstance(v, np.ndarray) for v in data.values()) and not
              any(d in data or any(d in k for k in data if isinstance(k, tuple)) for d in dimensions)):
            # For data where both keys and values are dimension values
            # e.g. {('A', 'B'): (1, 2)} (should consider deprecating)
            dict_data = sorted(data.items())
            k, v = dict_data[0]
            if len(util.wrap_tuple(k)) != len(kdims) or len(util.wrap_tuple(v)) != len(vdims):
                raise ValueError("Dictionary data not understood, should contain a column "
                                 "per dimension or a mapping between key and value dimension "
                                 "values.")
            dict_data = zip(*((util.wrap_tuple(k)+util.wrap_tuple(v))
                              for k, v in dict_data), strict=None)
            data = {k: np.array(v) for k, v in zip(dimensions, dict_data, strict=None)}

        if not isinstance(data, cls.types):
            raise ValueError("DictInterface interface couldn't convert data.""")

        unpacked = []
        for d, vals in data.items():
            if isinstance(d, tuple):
                vals = np.asarray(vals)
                if vals.shape == (0,):
                    for sd in d:
                        unpacked.append((sd, np.array([], dtype=vals.dtype)))
                elif not vals.ndim == 2 and vals.shape[1] == len(d):
                    raise ValueError("Values for %s dimensions did not have "
                                     "the expected shape.")
                else:
                    for i, sd in enumerate(d):
                        unpacked.append((sd, vals[:, i]))
            elif d not in dimensions:
                unpacked.append((d, vals))
            else:
                if not isscalar(vals):
                    vals = np.asarray(vals)
                    if not vals.ndim == 1 and d in dimensions:
                        raise ValueError('DictInterface expects data for each column to be flat.')
                unpacked.append((d, vals))

        if not cls.expanded([vs for d, vs in unpacked if d in dimensions and not isscalar(vs)]):
            raise ValueError('DictInterface expects data to be of uniform shape.')
        # OrderedDict can't be replaced with dict: https://github.com/holoviz/holoviews/pull/5925
        if isinstance(data, OrderedDict):
            data.update(unpacked)
        else:
            data = OrderedDict(unpacked)

        return data, {'kdims':kdims, 'vdims':vdims}, {}


    @classmethod
    def validate(cls, dataset, vdims=True):
        dim_types = 'all' if vdims else 'key'
        dimensions = dataset.dimensions(dim_types, label='name')
        not_found = [d for d in dimensions if d not in dataset.data]
        if not_found:
            raise DataError('Following columns specified as dimensions '
                            f'but not found in data: {not_found}', cls)
        lengths = [(dim, 1 if isscalar(dataset.data[dim]) else len(dataset.data[dim]))
                   for dim in dimensions]
        if len({l for d, l in lengths if l > 1}) > 1:
            lengths = ', '.join(['{}: {}'.format(*l) for l in sorted(lengths)])
            raise DataError('Length of columns must be equal or scalar, '
                            f'columns have lengths: {lengths}', cls)


    @classmethod
    def unpack_scalar(cls, dataset, data):
        """Given a dataset object and data in the appropriate format for
        the interface, return a simple scalar.

        """
        if len(data) != 1:
            return data
        key = next(iter(data.keys()))

        if len(data[key]) == 1 and key in dataset.vdims:
            scalar = data[key][0]
            return scalar.compute() if hasattr(scalar, 'compute') else scalar
        return data


    @classmethod
    def isscalar(cls, dataset, dim):
        name = dataset.get_dimension(dim, strict=True).name
        values = dataset.data[name]
        if isscalar(values):
            return True
        if dtype_kind(values) == 'O':
            unique = set(values)
        else:
            unique = np.unique(values)
            if (~util.isfinite(unique)).all():
                return True
        return len(unique) == 1


    @classmethod
    def shape(cls, dataset):
        return cls.length(dataset), len(dataset.data),

    @classmethod
    def length(cls, dataset):
        lengths = [len(vals) for d, vals in dataset.data.items()
                   if d in dataset.dimensions() and not isscalar(vals)]
        return max(lengths) if lengths else 1

    @classmethod
    def array(cls, dataset, dimensions):
        if not dimensions:
            dimensions = dataset.dimensions(label='name')
        else:
            dimensions = [dataset.get_dimensions(d).name for d in dimensions]
        arrays = [dataset.data[dim.name] for dim in dimensions]
        return np.column_stack([np.full(len(dataset), arr) if isscalar(arr) else arr
                                for arr in arrays])

    @classmethod
    def add_dimension(cls, dataset, dimension, dim_pos, values, vdim):
        dim = dimension_name(dimension)
        data = list(dataset.data.items())
        data.insert(dim_pos, (dim, values))
        return dict(data)

    @classmethod
    def redim(cls, dataset, dimensions):
        all_dims = dataset.dimensions()
        renamed = []
        for k, v in dataset.data.items():
            if k in dimensions:
                k = dimensions[k].name
            elif k in all_dims:
                k = dataset.get_dimension(k).name
            renamed.append((k, v))
        return dict(renamed)


    @classmethod
    def concat(cls, datasets, dimensions, vdims):
        columns = defaultdict(list)
        for key, ds in datasets:
            for k, vals in ds.data.items():
                columns[k].append(np.atleast_1d(vals))
            for d, k in zip(dimensions, key, strict=None):
                columns[d.name].append(np.full(len(ds), k))

        template = datasets[0][1]
        dims = dimensions+template.dimensions()
        return dict([(d.name, np.concatenate(columns[d.name])) for d in dims])


    @classmethod
    def mask(cls, dataset, mask, mask_value=np.nan):
        masked = dict(dataset.data)
        for vd in dataset.vdims:
            new_array = np.copy(dataset.data[vd.name])
            new_array[mask] = mask_value
            masked[vd.name] = new_array
        return masked


    @classmethod
    def sort(cls, dataset, by=None, reverse=False):
        if by is None:
            by = []
        by = [dataset.get_dimension(d).name for d in by]
        if len(by) == 1:
            sorting = cls.values(dataset, by[0]).argsort()
        else:
            arrays = [dataset.dimension_values(d) for d in by]
            sorting = util.arglexsort(arrays)
        return dict([(d, v if isscalar(v) else (v[sorting][::-1] if reverse else v[sorting]))
                            for d, v in dataset.data.items()])


    @classmethod
    def range(cls, dataset, dimension):
        dim = dataset.get_dimension(dimension, strict=True)
        column = dataset.data[dim.name]
        if isscalar(column):
            return column, column
        return Interface.range(dataset, dimension)


    @classmethod
    def values(cls, dataset, dim, expanded=True, flat=True, compute=True, keep_index=False):
        dim = dataset.get_dimension(dim, strict=True).name
        values = dataset.data.get(dim)
        if isscalar(values):
            if not expanded:
                return np.array([values])
            values = np.full(len(dataset), values, dtype=np.array(values).dtype)
        else:
            if not expanded:
                return util.unique_array(values)
            values = np.asarray(values)
        return values


    @classmethod
    def assign(cls, dataset, new_data):
        data = dict(dataset.data)
        data.update(new_data)
        return data


    @classmethod
    def reindex(cls, dataset, kdims, vdims):
        dimensions = [dataset.get_dimension(d).name for d in kdims+vdims]
        return dict([(d, dataset.dimension_values(d))
                            for d in dimensions])


    @classmethod
    def groupby(cls, dataset, dimensions, container_type, group_type, **kwargs):
        # Get dimensions information
        dimensions = [dataset.get_dimension(d) for d in dimensions]
        kdims = [kdim for kdim in dataset.kdims if kdim not in dimensions]
        vdims = dataset.vdims

        # Update the kwargs appropriately for Element group types
        group_kwargs = {}
        group_type = dict if group_type == 'raw' else group_type
        if issubclass(group_type, Element):
            group_kwargs.update(util.get_param_values(dataset))
            group_kwargs['kdims'] = kdims
        group_kwargs.update(kwargs)

        # Find all the keys along supplied dimensions
        keys = (tuple(dataset.data[d.name] if isscalar(dataset.data[d.name])
                      else dataset.data[d.name][i] for d in dimensions)
                for i in range(len(dataset)))

        # Iterate over the unique entries applying selection masks
        grouped_data = []
        for unique_key in util.unique_iterator(keys):
            mask = cls.select_mask(dataset, dict(zip(dimensions, unique_key, strict=None)))
            group_data = dict((d.name, dataset.data[d.name] if isscalar(dataset.data[d.name])
                                       else dataset.data[d.name][mask])
                                      for d in kdims+vdims)
            group_data = group_type(group_data, **group_kwargs)
            grouped_data.append((unique_key, group_data))

        if issubclass(container_type, NdMapping):
            with item_check(False), sorted_context(False):
                return container_type(grouped_data, kdims=dimensions)
        else:
            return container_type(grouped_data)


    @classmethod
    def select(cls, dataset, selection_mask=None, **selection):
        if selection_mask is None:
            selection_mask = cls.select_mask(dataset, selection)
        empty = not selection_mask.sum()
        dimensions = dataset.dimensions()
        if empty:
            return {d.name: np.array([], dtype=cls.dtype(dataset, d))
                    for d in dimensions}
        indexed = cls.indexed(dataset, selection)
        data = {}
        for k, v in dataset.data.items():
            if k not in dimensions or isscalar(v):
                data[k] = v
            else:
                data[k] = v[selection_mask]
        if indexed and len(next(iter(data.values()))) == 1 and len(dataset.vdims) == 1:
            value = data[dataset.vdims[0].name]
            return value if isscalar(value) else value[0]
        return data


    @classmethod
    def sample(cls, dataset, samples=None):
        if samples is None:
            samples = []
        mask = False
        for sample in samples:
            sample_mask = True
            if isscalar(sample): sample = [sample]
            for i, v in enumerate(sample):
                name = dataset.get_dimension(i).name
                sample_mask &= (dataset.data[name]==v)
            mask |= sample_mask
        return {k: col if isscalar(col) else np.array(col)[mask]
                for k, col in dataset.data.items()}


    @classmethod
    def aggregate(cls, dataset, kdims, function, **kwargs):
        kdims = [dataset.get_dimension(d, strict=True).name for d in kdims]
        vdims = dataset.dimensions('value', label='name')
        groups = cls.groupby(dataset, kdims, list, dict)
        aggregated = dict([(k, []) for k in kdims+vdims])

        dropped = []
        for key, group in groups:
            key = key if isinstance(key, tuple) else (key,)
            for kdim, val in zip(kdims, key, strict=None):
                aggregated[kdim].append(val)
            for vdim, arr in group.items():
                if vdim in dataset.vdims:
                    if isscalar(arr):
                        aggregated[vdim].append(arr)
                        continue
                    try:
                        if isinstance(function, np.ufunc):
                            reduced = function.reduce(arr, **kwargs)
                        else:
                            reduced = function(arr, **kwargs)
                        aggregated[vdim].append(reduced)
                    except TypeError:
                        dropped.append(vdim)
        return aggregated, list(util.unique_iterator(dropped))


    @classmethod
    def iloc(cls, dataset, index):
        rows, cols = index
        scalar = False
        if isscalar(cols):
            scalar = isscalar(rows)
            cols = [dataset.get_dimension(cols, strict=True)]
        elif isinstance(cols, slice):
            cols = dataset.dimensions()[cols]
        else:
            cols = [dataset.get_dimension(d, strict=True) for d in cols]

        if isscalar(rows):
            rows = [rows]

        new_data = {}
        for d, values in dataset.data.items():
            if d in cols:
                if isscalar(values):
                    new_data[d] = values
                else:
                    new_data[d] = values[rows]

        if scalar:
            arr = new_data[cols[0].name]
            return arr if isscalar(arr) else arr[0]
        return new_data


    @classmethod
    def geom_type(cls, dataset):
        return dataset.data.get('geom_type')

    @classmethod
    def has_holes(cls, dataset):
        from holoviews.element import Polygons
        key = Polygons._hole_key
        return key in dataset.data and isinstance(dataset.data[key], list)

    @classmethod
    def holes(cls, dataset):
        from holoviews.element import Polygons
        key = Polygons._hole_key
        if key in dataset.data:
            holes = []
            for hs in dataset.data[key]:
                subholes = []
                for h in hs:
                    hole = np.asarray(h)
                    if (hole[0, :] != hole[-1, :]).all():
                        hole = np.concatenate([hole, hole[:1]])
                    subholes.append(hole)
                holes.append(subholes)
            return [holes]
        else:
            return super().holes(dataset)


Interface.register(DictInterface)
