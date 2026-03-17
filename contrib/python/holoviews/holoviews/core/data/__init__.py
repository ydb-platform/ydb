import copy
import types
from collections.abc import Mapping
from contextlib import contextmanager
from functools import wraps
from itertools import pairwise

import numpy as np
import param
from param.parameterized import ParameterizedMetaclass

from .. import util as core_util
from ..accessors import Redim
from ..dimension import (
    Dimension,
    Dimensioned,
    LabelledData,
    dimension_name,
    process_dimensions,
)
from ..element import Element
from ..ndmapping import MultiDimensionalMapping
from ..spaces import DynamicMap, HoloMap
from .array import ArrayInterface
from .cudf import cuDFInterface  # noqa (API import)
from .dask import DaskInterface  # noqa (API import)
from .dictionary import DictInterface  # noqa (API import)
from .grid import GridInterface  # noqa (API import)
from .ibis import IbisInterface  # noqa (API import)
from .image import ImageInterface  # noqa (API import)
from .interface import Interface, iloc, ndloc
from .multipath import MultiInterface  # noqa (API import)
from .narwhals import NarwhalsInterface  # noqa: F401
from .pandas import PandasAPI, PandasInterface  # noqa (API import)
from .spatialpandas import SpatialPandasInterface  # noqa (API import)
from .spatialpandas_dask import DaskSpatialPandasInterface  # noqa (API import)
from .xarray import XArrayInterface  # noqa (API import)

default_datatype = 'dataframe'

datatypes = ['dataframe', 'dictionary', 'grid', 'xarray', 'multitabular',
             'spatialpandas', 'dask_spatialpandas', 'dask', 'cuDF', 'array', 'narwhals',
             'ibis']


_TABULAR_DATATYPE = ['dataframe', 'dask', 'ibis', 'cuDF', 'narwhals']

def concat(datasets, datatype=None):
    """Concatenates collection of datasets along NdMapping dimensions.

    Concatenates multiple datasets wrapped in an NdMapping type along
    all of its dimensions. Before concatenation all datasets are cast
    to the same datatype, which may be explicitly defined or
    implicitly derived from the first datatype that is
    encountered. For columnar data concatenation adds the columns for
    the dimensions being concatenated along and then concatenates all
    the old and new columns. For gridded data a new axis is created
    for each dimension being concatenated along and then
    hierarchically concatenates along each dimension.

    Parameters
    ----------
    datasets
        NdMapping of Datasets to concatenate
    datatype
        Datatype to cast data to before concatenation

    Returns
    -------
    Concatenated dataset
    """
    return Interface.concatenate(datasets, datatype)


class DataConversion:
    """DataConversion is a very simple container object which can be
    given an existing Dataset Element and provides methods to convert
    the Dataset into most other Element types.

    """

    def __init__(self, element):
        self._element = element

    def __call__(self, new_type, kdims=None, vdims=None, groupby=None,
                 sort=False, **kwargs):
        """Generic conversion method for Dataset based Element
        types. Supply the Dataset Element type to convert to and
        optionally the key dimensions (kdims), value dimensions
        (vdims) and the dimensions.  to group over. Converted Columns
        can be automatically sorted via the sort option and kwargs can
        be passed through.

        """
        element_params = new_type.param.objects()
        kdim_param = element_params['kdims']
        vdim_param = element_params['vdims']
        if isinstance(kdim_param.bounds[1], int):
            ndim = min([kdim_param.bounds[1], len(kdim_param.default)])
        else:
            ndim = None
        nvdim = vdim_param.bounds[1] if isinstance(vdim_param.bounds[1], int) else None
        if kdims is None:
            kd_filter = groupby or []
            if not isinstance(kd_filter, list):
                kd_filter = [groupby]
            kdims = [kd for kd in self._element.kdims if kd not in kd_filter][:ndim]
        elif kdims and not isinstance(kdims, list): kdims = [kdims]
        if vdims is None:
            vdims = [d for d in self._element.vdims if d not in kdims][:nvdim]
        if vdims and not isinstance(vdims, list): vdims = [vdims]

        # Checks Element type supports dimensionality
        type_name = new_type.__name__
        for dim_type, dims in (('kdims', kdims), ('vdims', vdims)):
            min_d, max_d = element_params[dim_type].bounds
            if ((min_d is not None and len(dims) < min_d) or
                (max_d is not None and len(dims) > max_d)):
                raise ValueError(f"{type_name} {dim_type} must be between length {min_d} and {max_d}.")

        if groupby is None:
            groupby = [d for d in self._element.kdims if d not in kdims+vdims]
        elif groupby and not isinstance(groupby, list):
            groupby = [groupby]

        if self._element.interface.gridded:
            dropped_kdims = [kd for kd in self._element.kdims if kd not in groupby+kdims]
            if dropped_kdims:
                selected = self._element.reindex(groupby+kdims, vdims)
            else:
                selected = self._element
        elif issubclass(self._element.interface, PandasAPI):
            ds_dims = self._element.dimensions()
            ds_kdims = [self._element.get_dimension(d) if d in ds_dims else d
                        for d in groupby+kdims]
            ds_vdims = [self._element.get_dimension(d) if d in ds_dims else d
                        for d in vdims]
            selected = self._element.clone(kdims=ds_kdims, vdims=ds_vdims)
        else:
            selected = self._element.reindex(groupby+kdims, vdims)
        params = {'kdims': [selected.get_dimension(kd, strict=True) for kd in kdims],
                  'vdims': [selected.get_dimension(vd, strict=True) for vd in vdims],
                  'label': selected.label}
        if selected.group != selected.param.objects('existing')['group'].default:
            params['group'] = selected.group
        params.update(kwargs)
        if len(kdims) == selected.ndims or not groupby:
            # Propagate dataset
            params['dataset'] = self._element.dataset
            params['pipeline'] = self._element._pipeline
            element = new_type(selected, **params)
            return element.sort() if sort else element
        group = selected.groupby(groupby, container_type=HoloMap,
                                 group_type=new_type, **params)
        if sort:
            return group.map(lambda x: x.sort(), [new_type])
        else:
            return group


@contextmanager
def disable_pipeline():
    """Disable PipelineMeta class from storing pipelines.

    """
    PipelineMeta.disable = True
    try:
        yield
    finally:
        PipelineMeta.disable = False


class PipelineMeta(ParameterizedMetaclass):

    # Public methods that should not be wrapped
    blacklist = ['__init__', 'clone']

    disable = False

    def __new__(mcs, classname, bases, classdict):

        for method_name in classdict:
            method_fn = classdict[method_name]
            if method_name in mcs.blacklist or method_name.startswith('_'):
                continue
            elif isinstance(method_fn, types.FunctionType):
                classdict[method_name] = mcs.pipelined(method_fn, method_name)

        inst = type.__new__(mcs, classname, bases, classdict)
        return inst

    @staticmethod
    def pipelined(method_fn, method_name):
        @wraps(method_fn)
        def pipelined_fn(*args, **kwargs):
            from ...operation.element import method as method_op
            inst = args[0]
            inst_pipeline = copy.copy(getattr(inst, '_pipeline', None))
            in_method = inst._in_method
            if not in_method:
                inst._in_method = True

            try:
                result = method_fn(*args, **kwargs)
                if PipelineMeta.disable:
                    return result

                op = method_op.instance(
                    input_type=type(inst),
                    method_name=method_name,
                    args=list(args[1:]),
                    kwargs=kwargs,
                )

                if not in_method:
                    if isinstance(result, Dataset):
                        result._pipeline = inst_pipeline.instance(
                            operations=[*inst_pipeline.operations, op],
                            output_type=type(result),
                        )

                    elif isinstance(result, MultiDimensionalMapping):
                        for key, element in result.items():
                            if isinstance(element, Dataset):
                                getitem_op = method_op.instance(
                                    input_type=type(result),
                                    method_name='__getitem__',
                                    args=[key]
                                )
                                element._pipeline = inst_pipeline.instance(
                                    operations=[*inst_pipeline.operations, op, getitem_op],
                                    output_type=type(result),
                                )
            finally:
                if not in_method:
                    inst._in_method = False
            return result

        return pipelined_fn


class Dataset(Element, metaclass=PipelineMeta):
    """Dataset provides a general baseclass for Element types that
    contain structured data and supports a range of data formats.

    The Dataset class supports various methods offering a consistent
    way of working with the stored data regardless of the storage
    format used. These operations include indexing, selection and
    various ways of aggregating or collapsing the data with a supplied
    function.

    """

    datatype = param.List(default=datatypes, doc="""
        A priority list of the data types to be used for storage
        on the .data attribute. If the input supplied to the element
        constructor cannot be put into the requested format, the next
        format listed will be used until a suitable format is found (or
        the data fails to be understood).""")

    group = param.String(default='Dataset', constant=True)

    # In the 1D case the interfaces should not automatically add x-values
    # to supplied data
    _auto_indexable_1d = False

    # Define a class used to transform Datasets into other Element types
    _conversion_interface = DataConversion

    # Whether the key dimensions are specified as bins
    _binned = False

    _vdim_reductions = {}
    _kdim_reductions = {}

    interface: Interface

    def __new__(cls, data=None, kdims=None, vdims=None, **kwargs):
        """Allows casting a DynamicMap to an Element class like hv.Curve, by applying the
        class to each underlying element.

        """
        if isinstance(data, DynamicMap):
            class_name = cls.__name__
            repr_kdims = f'kdims={kdims!r}' if kdims else None
            repr_vdims = f'vdims={vdims!r}' if vdims else None
            repr_kwargs = (', '.join(f'{k}={v!r}' for k,v in kwargs.items())
                           if kwargs else None)
            extras = ', '.join([el for el in [repr_kdims, repr_vdims, repr_kwargs]
                               if el is not None])
            extras = ', ' + extras if extras else ''
            apply_args= f'hv.{class_name}{extras}'
            msg = "Cannot construct a {class_name} from the supplied object of type DynamicMap. Implicitly creating a DynamicMap of {class_name} objects, but instead please explicitly call .apply({apply_args}) on the supplied DynamicMap."
            cls.param.warning(cls, msg.format(class_name=class_name, apply_args=apply_args))
            return data.apply(cls, per_element=True, kdims=kdims, vdims=vdims, **kwargs)
        else:
            return super().__new__(cls)

    def __init__(self, data, kdims=None, vdims=None, **kwargs):
        from ...operation.element import (
            chain as chain_op,
            factory,
        )
        self._in_method = False
        input_data = data
        dataset_provided = 'dataset' in kwargs
        input_dataset = kwargs.pop('dataset', None)
        input_pipeline = kwargs.pop('pipeline', None)
        input_transforms = kwargs.pop('transforms', None)

        if isinstance(data, Element):
            if 'kdims' in kwargs:
                kwargs['kdims'] = [
                    data.get_dimension(kd) if isinstance(kd, str) else kd
                    for kd in kwargs['kdims']
                ]
            if 'kdims' in kwargs:
                kwargs['vdims'] = [
                    data.get_dimension(vd) if isinstance(vd, str) else vd
                    for vd in kwargs['vdims']
                ]
            pvals = core_util.get_param_values(data)
            kwargs.update([(l, pvals[l]) for l in ['group', 'label']
                           if l in pvals and l not in kwargs])
        if isinstance(data, Dataset):
            if not dataset_provided and data._dataset is not None:
                input_dataset = data._dataset
            if input_pipeline is None:
                input_pipeline = data.pipeline
            if input_transforms is None:
                input_transforms = data._transforms

        kwargs.update(process_dimensions(kdims, vdims))
        kdims, vdims = kwargs.get('kdims'), kwargs.get('vdims')

        validate_vdims = kwargs.pop('_validate_vdims', True)
        initialized = Interface.initialize(type(self), data, kdims, vdims,
                                           datatype=kwargs.get('datatype'))
        (data, self.interface, dims, extra_kws) = initialized
        super().__init__(data, **dict(kwargs, **dict(dims, **extra_kws)))
        self.interface.validate(self, validate_vdims)

        # Handle _pipeline property
        if input_pipeline is None:
            input_pipeline = chain_op.instance()

        kwargs['kdims'] = self.kdims
        kwargs['vdims'] = self.vdims
        init_op = factory.instance(
            output_type=type(self),
            args=[],
            kwargs=dict(kwargs, kdims=self.kdims, vdims=self.vdims),
        )
        self._pipeline = input_pipeline.instance(
            operations=[*input_pipeline.operations, init_op],
            output_type=type(self),
        )
        self._transforms = input_transforms or []

        # On lazy interfaces this allows keeping an evaluated version
        # of the dataset in memory
        self._cached = None

        # Handle initializing the dataset property.
        self._dataset = input_dataset
        if self._dataset is None and isinstance(input_data, Dataset) and not dataset_provided:
            if input_data.data is self.data:
                self._dataset = {'kdims': input_data.kdims, 'vdims': input_data.vdims}
            else:
                self._dataset = Dataset(input_data, dataset=None, pipeline=None,
                                        transforms=None, _validate_vdims=False)
                if hasattr(self, '_binned'):
                    self._dataset._binned = self._binned

    def __getstate__(self):
        """Ensures pipelines are dropped

        """
        obj_dict = super().__getstate__()
        if '_pipeline' in obj_dict:
            pipeline = obj_dict['_pipeline']
            obj_dict['_pipeline'] = pipeline.instance(operations=pipeline.operations[:1])
        if '_transforms' in obj_dict:
            obj_dict['_transforms'] = []
        return obj_dict

    @property
    def redim(self):
        return Redim(self, mode='dataset')

    @property
    def dataset(self):
        """The Dataset that this object was created from

        """
        if self._dataset is None:
            if type(self) is Dataset:
                return self
            datatype = list(core_util.unique_iterator(self.datatype+Dataset.datatype))
            dataset = Dataset(self, dataset=None, pipeline=None, transforms=None,
                              _validate_vdims=False, datatype=datatype)
            if hasattr(self, '_binned'):
                dataset._binned = self._binned
            return dataset
        elif not isinstance(self._dataset, Dataset):
            return Dataset(self, _validate_vdims=False, **self._dataset)
        return self._dataset

    @property
    def pipeline(self):
        """Chain operation that evaluates the sequence of operations that was
        used to create this object, starting with the Dataset stored in
        dataset property

        """
        return self._pipeline

    def compute(self):
        """Computes the data to a data format that stores the daata in
        memory, e.g. a Dask dataframe or array is converted to a
        Pandas DataFrame or NumPy array.

        Returns
        -------
        Dataset with the data stored in in-memory format
        """
        return self.interface.compute(self)

    def persist(self):
        """Persists the results of a lazy data interface to memory to
        speed up data manipulation and visualization. If the
        particular data backend already holds the data in memory
        this is a no-op. Unlike the compute method this maintains
        the same data type.

        Returns
        -------
        Dataset with the data persisted to memory
        """
        persisted = self.interface.persist(self)
        if persisted.interface is self.interface:
            return persisted
        self._cached = persisted
        return self

    def closest(self, coords=None, **kwargs):
        """Snaps coordinate(s) to closest coordinate in Dataset

        Parameters
        ----------
        coords
            List of coordinates expressed as tuples
        **kwargs
            Coordinates defined as keyword pairs

        Returns
        -------
        List of tuples of the snapped coordinates

        Raises
        ------
        NotImplementedError
            Raised if snapping is not supported
        """
        if coords is None:
            coords = []
        if self.ndims > 1:
            raise NotImplementedError("Closest method currently only "
                                      "implemented for 1D Elements")

        if kwargs:
            if len(kwargs) > 1:
                raise NotImplementedError("Closest method currently only "
                                          "supports 1D indexes")
            samples = next(iter(kwargs.values()))
            coords = samples if isinstance(samples, list) else [samples]

        xs = self.dimension_values(0)
        if core_util.dtype_kind(xs) in 'SO':
            raise NotImplementedError("Closest only supported for numeric types")
        idxs = [np.argmin(np.abs(xs-coord)) for coord in coords]
        return [type(s)(xs[idx]) for s, idx in zip(coords, idxs, strict=None)]


    def sort(self, by=None, reverse=False):
        """Sorts the data by the values along the supplied dimensions.

        Parameters
        ----------
        by
            Dimension(s) to sort by
        reverse : bool, optional
            Reverse sort order

        Returns
        -------
        Sorted Dataset
        """
        if by is None:
            by = self.kdims
        elif not isinstance(by, list):
            by = [by]
        sorted_columns = self.interface.sort(self, by, reverse)
        return self.clone(sorted_columns)


    def range(self, dim, data_range=True, dimension_range=True):
        """Return the lower and upper bounds of values along dimension.

        Parameters
        ----------
        dim
            The dimension to compute the range on.
        data_range : bool
            Compute range from data values
        dimension_range : bool
            Include Dimension ranges
            Whether to include Dimension range and soft_range
            in range calculation

        Returns
        -------
        Tuple containing the lower and upper bound
        """
        dim = self.get_dimension(dim)

        if dim is None or (not data_range and not dimension_range):
            return (None, None)
        elif all(core_util.isfinite(v) for v in dim.range) and dimension_range:
            return dim.range
        elif dim in self.dimensions() and data_range and bool(self):
            lower, upper = self.interface.range(self, dim)
        else:
            lower, upper = (np.nan, np.nan)
        if not dimension_range:
            return lower, upper
        return core_util.dimension_range(lower, upper, dim.range, dim.soft_range)


    def add_dimension(self, dimension, dim_pos, dim_val, vdim=False, **kwargs):
        """Adds a dimension and its values to the Dataset

        Requires the dimension name or object, the desired position in
        the key dimensions and a key value scalar or array of values,
        matching the length or shape of the Dataset.

        Parameters
        ----------
        dimension
            Dimension or dimension spec to add
        dim_pos : int
            Integer index to insert dimension at
        dim_val : scalar or ndarray
            Dimension value(s) to add
        vdim
            Disabled, this type does not have value dimensions
        **kwargs
            Keyword arguments passed to the cloned element

        Returns
        -------
        Cloned object containing the new dimension
        """
        if isinstance(dimension, (str, tuple)):
            dimension = Dimension(dimension)

        if dimension.name in self.kdims:
            raise Exception(f'{dimension.name} dimension already defined')

        if vdim:
            dims = self.vdims[:]
            dims.insert(dim_pos, dimension)
            dimensions = dict(vdims=dims)
            dim_pos += self.ndims
        else:
            dims = self.kdims[:]
            dims.insert(dim_pos, dimension)
            dimensions = dict(kdims=dims)

        if issubclass(self.interface, ArrayInterface) and np.asarray(dim_val).dtype != self.data.dtype:
            element = self.clone(datatype=[default_datatype])
            data = element.interface.add_dimension(element, dimension, dim_pos, dim_val, vdim)
        else:
            data = self.interface.add_dimension(self, dimension, dim_pos, dim_val, vdim)
        return self.clone(data, **dimensions)


    def select(self, selection_expr=None, selection_specs=None, **selection):
        """Applies selection by dimension name

        Applies a selection along the dimensions of the object using
        keyword arguments. The selection may be narrowed to certain
        objects using selection_specs. For container objects the
        selection will be applied to all children as well.

        Selections may select a specific value, slice or set of values:

        * value: Scalar values will select rows along with an exact
                 match, e.g.:

            ds.select(x=3)

        * slice: Slices may be declared as tuples of the upper and
                 lower bound, e.g.:

            ds.select(x=(0, 3))

        * values: A list of values may be selected using a list or
                  set, e.g.:

            ds.select(x=[0, 1, 2])

        * predicate expression: A holoviews.dim expression, e.g.:

            from holoviews import dim
            ds.select(selection_expr=dim('x') % 2 == 0)

        * selections dictionary: A dictionary of selections per dimension

            ds.select({x: 3})

        Parameters
        ----------
        selection_expr : A dim expression or dictionary of selections.
            holoviews.dim predicate expression specifying selection or
            a dictionary of selections (as an alternative to selecting
            via keyword arguments).
        selection_specs : List of specs to match on
            A list of types, functions, or type[.group][.label]
            strings specifying which objects to apply the
            selection on.
        **selection: Dictionary declaring selections by dimension
            Selections can be scalar values, tuple ranges, lists
            of discrete values and boolean arrays

        Returns
        -------
        Returns an Dimensioned object containing the selected data
        or a scalar if a single value was selected
        """
        from ...util.transform import dim
        dimensions = self.dimensions()
        ndims = len(dimensions)
        sel_dims = (*dimensions, 'selection_mask')
        if isinstance(selection_expr, Mapping):
            if selection:
                raise ValueError("""\
                Selections may be supplied as keyword arguments or as a positional
                argument, never both.""")
            selection = {
                self.get_dimension(dim).name: v
                for dim, v in selection_expr.items()
                if dim in sel_dims or (isinstance(dim, int) and dim < ndims)
            }
            selection_expr = None
        elif selection_expr is not None and not isinstance(selection_expr, dim):
            raise ValueError("""\
            The first positional argument to the Dataset.select method is expected to be a
            holoviews.util.transform.dim expression. Use the selection_specs keyword
            argument to specify a selection specification""")
        elif selection:
            selection = {
                dim: sel for dim, sel in selection.items()
                if dim in sel_dims or (isinstance(dim, int) and dim < ndims)
            }

        if selection_specs is not None and not isinstance(selection_specs, (list, tuple)):
            selection_specs = [selection_specs]
        if (selection_specs and not any(self.matches(sp) for sp in selection_specs)
            or (not selection and not selection_expr)):
            return self

        # Handle selection dim expression
        if selection_expr is not None and selection_expr.ops:
            mask = selection_expr.apply(self, compute=False, keep_index=True)
            selection = {'selection_mask': mask}

        # Handle selection kwargs
        if selection:
            data = self.interface.select(self, **selection)
        else:
            data = self.data

        if np.isscalar(data):
            return data
        else:
            return self.clone(data)


    def reindex(self, kdims=None, vdims=None):
        """Reindexes Dataset dropping static or supplied kdims

        Creates a new object with a reordered or reduced set of key
        dimensions. By default drops all non-varying key dimensions.x

        Parameters
        ----------
        kdims : optional
            New list of key dimensions
        vdims : optional
            New list of value dimensions

        Returns
        -------
        Reindexed object
        """
        gridded = self.interface.gridded
        scalars = []
        if gridded:
            coords = [(d, self.interface.coords(self, d.name)) for d in self.kdims]
            scalars = [d for d, vs in coords if len(vs) == 1]

        if kdims is None:
            # If no key dimensions are defined and interface is gridded
            # drop all scalar key dimensions
            key_dims = [d for d in self.kdims if (not vdims or d not in vdims)
                        and d not in scalars]
        elif not isinstance(kdims, list):
            key_dims = [self.get_dimension(kdims, strict=True)]
        else:
            key_dims = [self.get_dimension(k, strict=True) for k in kdims]
        dropped = [d for d in self.kdims if d not in key_dims and d not in scalars]

        new_type = None
        if vdims is None:
            val_dims = [d for d in self.vdims if not kdims or d not in kdims]
        else:
            val_dims = [self.get_dimension(v, strict=True) for v in vdims]
            new_type = self._vdim_reductions.get(len(val_dims), type(self))

        data = self.interface.reindex(self, key_dims, val_dims)
        datatype = self.datatype
        if gridded and dropped:
            interfaces = self.interface.interfaces
            datatype = [dt for dt in datatype if not
                        getattr(interfaces.get(dt, None), 'gridded', True)]
        return self.clone(data, kdims=key_dims, vdims=val_dims,
                          new_type=new_type, datatype=datatype)


    def __getitem__(self, slices):
        """Allows slicing and selecting values in the Dataset object.
        Supports multiple indexing modes:

           (1) Slicing and indexing along the values of each dimension
               in the columns object using either scalars, slices or
               sets of values.
           (2) Supplying the name of a dimension as the first argument
               will return the values along that dimension as a numpy
               array.
           (3) Slicing of all key dimensions and selecting a single
               value dimension by name.
           (4) A boolean array index matching the length of the Dataset
               object.

        """
        slices = core_util.process_ellipses(self, slices, vdim_selection=True)
        if getattr(getattr(slices, 'dtype', None), 'kind', None) == 'b':
            if not len(slices) == len(self):
                raise IndexError("Boolean index must match length of sliced object")
            return self.clone(self.select(selection_mask=slices))
        elif (isinstance(slices, ()) and len(slices) == 1) or slices is Ellipsis:
            return self
        if not isinstance(slices, tuple): slices = (slices,)
        value_select = None
        if len(slices) == 1 and slices[0] in self.dimensions():
            return self.dimension_values(slices[0])
        elif len(slices) == self.ndims+1 and slices[self.ndims] in self.dimensions():
            selection = dict(zip(self.dimensions('key', label=True), slices, strict=None))
            value_select = slices[self.ndims]
        elif len(slices) == self.ndims+1 and isinstance(slices[self.ndims],
                                                        (Dimension,str)):
            raise IndexError(f"{slices[self.ndims]!r} is not an available value dimension")
        else:
            selection = dict(zip(self.dimensions(label=True), slices, strict=None))
        data = self.select(**selection)
        if value_select:
            if data.shape[0] == 1:
                return data[value_select][0]
            else:
                return data.reindex(vdims=[value_select])
        return data


    def sample(self, samples=None, bounds=None, closest=True, **kwargs):
        """Samples values at supplied coordinates.

        Allows sampling of element with a list of coordinates matching
        the key dimensions, returning a new object containing just the
        selected samples. Supports multiple signatures:

        Sampling with a list of coordinates, e.g.:

            ds.sample([(0, 0), (0.1, 0.2), ...])

        Sampling a range or grid of coordinates, e.g.:

            1D : ds.sample(3)
            2D : ds.sample((3, 3))

        Sampling by keyword, e.g.:

            ds.sample(x=0)

        Parameters
        ----------
        samples : List of nd-coordinates to sample
        bounds
            Bounds of the region to sample
            Defined as two-tuple for 1D sampling and four-tuple
            for 2D sampling.
        closest
            Whether to snap to closest coordinates
        **kwargs
            Coordinates specified as keyword pairs
            Keywords of dimensions and scalar coordinates

        Returns
        -------
        Element containing the sampled coordinates
        """
        if samples is None:
            samples = []
        if kwargs and samples != []:
            raise Exception('Supply explicit list of samples or kwargs, not both.')
        elif kwargs:
            sample = [slice(None) for _ in range(self.ndims)]
            for dim, val in kwargs.items():
                sample[self.get_dimension_index(dim)] = val
            samples = [tuple(sample)]
        elif isinstance(samples, tuple) or core_util.isscalar(samples):
            if self.ndims == 1:
                xlim = self.range(0)
                lower, upper = (xlim[0], xlim[1]) if bounds is None else bounds
                edges = np.linspace(lower, upper, samples+1)
                linsamples = [(l+u)/2.0 for l,u in pairwise(edges)]
            elif self.ndims == 2:
                (rows, cols) = samples
                if bounds:
                    (l,b,r,t) = bounds
                else:
                    l, r = self.range(0)
                    b, t = self.range(1)

                xedges = np.linspace(l, r, cols+1)
                yedges = np.linspace(b, t, rows+1)
                xsamples = [(lx+ux)/2.0 for lx,ux in pairwise(xedges)]
                ysamples = [(ly+uy)/2.0 for ly,uy in pairwise(yedges)]

                Y,X = np.meshgrid(ysamples, xsamples)
                linsamples = list(zip(X.flat, Y.flat, strict=None))
            else:
                raise NotImplementedError("Regular sampling not implemented "
                                          "for elements with more than two dimensions.")
            samples = list(core_util.unique_iterator(self.closest(linsamples)))

        # Note: Special handling sampling of gridded 2D data as Curve
        # may be replaced with more general handling
        # see https://github.com/holoviz/holoviews/issues/1173
        from ...element import Curve, Table

        # If no datatype is selected, default to dictionary
        datatype = [d for d in _TABULAR_DATATYPE if d in self.datatype] or ["dictionary"]
        if len(samples) == 1:
            sel = {kd.name: s for kd, s in zip(self.kdims, samples[0], strict=None)}
            dims = [kd for kd, v in sel.items() if not np.isscalar(v)]
            selection = self.select(**sel)

            # If a 1D cross-section of 2D space return Curve
            if self.interface.gridded and self.ndims == 2 and len(dims) == 1:
                new_type = Curve
                kdims = [self.get_dimension(kd) for kd in dims]
            else:
                new_type = Table
                kdims = self.kdims

            if np.isscalar(selection):
                selection = [samples[0]+(selection,)]
            else:
                reindexed = selection.clone(new_type=Dataset, datatype=datatype).reindex(kdims)
                selection = tuple(reindexed.columns(kdims+self.vdims).values())

            return self.clone(selection, kdims=kdims, new_type=new_type,
                              datatype=datatype)

        lens = {len(core_util.wrap_tuple(s)) for s in samples}
        if len(lens) > 1:
            raise IndexError('Sample coordinates must all be of the same length.')

        if closest:
            try:
                samples = self.closest(samples)
            except NotImplementedError:
                pass
        samples = [core_util.wrap_tuple(s) for s in samples]
        sampled = self.interface.sample(self, samples)
        return self.clone(sampled, new_type=Table, datatype=datatype)


    def reduce(self, dimensions=None, function=None, spreadfn=None, **reductions):
        """Applies reduction along the specified dimension(s).

        Allows reducing the values along one or more key dimension
        with the supplied function. Supports two signatures:

        Reducing with a list of dimensions, e.g.:

            ds.reduce(['x'], np.mean)

        Defining a reduction using keywords, e.g.:

            ds.reduce(x=np.mean)

        Parameters
        ----------
        dimensions
            Dimension(s) to apply reduction on
            Defaults to all key dimensions
        function
            Reduction operation to apply, e.g. numpy.mean
        spreadfn
            Secondary reduction to compute value spread
            Useful for computing a confidence interval, spread, or
            standard deviation.
        **reductions
            Keyword argument defining reduction
            Allows reduction to be defined as keyword pair of
            dimension and function

        Returns
        -------
        The Dataset after reductions have been applied.
        """
        if dimensions is None:
            dimensions = []
        if any(dim in self.vdims for dim in dimensions):
            raise Exception("Reduce cannot be applied to value dimensions")
        function, dims = self._reduce_map(dimensions, function, reductions)
        dims = [d for d in self.kdims if d not in dims]
        return self.aggregate(dims, function, spreadfn)


    def aggregate(self, dimensions=None, function=None, spreadfn=None, **kwargs):
        """Aggregates data on the supplied dimensions.

        Aggregates over the supplied key dimensions with the defined
        function or dim_transform specified as a tuple of the transformed
        dimension name and dim transform.

        Parameters
        ----------
        dimensions
            Dimension(s) to aggregate on
            Default to all key dimensions
        function
            Aggregation function or transform to apply
            Supports both simple functions and dimension transforms
        spreadfn
            Secondary reduction to compute value spread
            Useful for computing a confidence interval, spread, or
            standard deviation.
        **kwargs
            Keyword arguments either passed to the aggregation function
            or to create new names for the transformed variables

        Returns
        -------
        Returns the aggregated Dataset
        """
        from ...util.transform import dim
        if dimensions is None: dimensions = self.kdims
        elif not isinstance(dimensions, list): dimensions = [dimensions]
        if isinstance(function, tuple) or any(isinstance(v, dim) for v in kwargs.values()):
            dataset = self.clone(new_type=Dataset)
            if dimensions:
                dataset = dataset.groupby(dimensions)
            args = () if function is None else (function,)
            transformed = dataset.apply.transform(*args, drop=True, **kwargs)
            if not isinstance(transformed, Dataset):
                transformed = transformed.collapse()
            return transformed.clone(new_type=type(self))

        ndims = len(dimensions)
        min_d, max_d = self.param.objects('existing')['kdims'].bounds
        generic_type = (min_d is not None and ndims < min_d) or (max_d is not None and ndims > max_d)
        new_type = Dataset if generic_type else None

        # Handle functions
        kdims = [self.get_dimension(d, strict=True) for d in dimensions]
        if not self:
            if spreadfn:
                spread_name = spreadfn.__name__
                vdims = [d for vd in self.vdims for d in [vd, vd.clone(f'{vd.name}_{spread_name}')]]
            else:
                vdims = self.vdims
            if not kdims and len(vdims) == 1:
                return np.nan
            return self.clone([], kdims=kdims, vdims=vdims, new_type=new_type)

        vdims = self.vdims
        aggregated, dropped = self.interface.aggregate(self, kdims, function, **kwargs)
        aggregated = self.interface.unpack_scalar(self, aggregated)
        vdims = [vd for vd in vdims if vd not in dropped]

        if spreadfn:
            error, _ = self.interface.aggregate(self, dimensions, spreadfn)
            spread_name = spreadfn.__name__
            ndims = len(vdims)
            error = self.clone(error, kdims=kdims, new_type=Dataset)
            combined = self.clone(aggregated, kdims=kdims, new_type=Dataset)
            for d in vdims:
                dim = d.clone(f'{d.name}_{spread_name}')
                dvals = error.dimension_values(d, flat=False)
                idx = vdims.index(d)
                combined = combined.add_dimension(dim, idx+1, dvals, True)
                vdims = combined.vdims
            return combined.clone(new_type=new_type)

        if np.isscalar(aggregated):
            return aggregated
        else:
            try:
                # Should be checking the dimensions declared on the element are compatible
                return self.clone(aggregated, kdims=kdims, vdims=vdims)
            except Exception:
                datatype = self.param.objects('existing')['datatype'].default
                return self.clone(aggregated, kdims=kdims, vdims=vdims,
                                  new_type=new_type, datatype=datatype)


    def groupby(self, dimensions=None, container_type=HoloMap, group_type=None,
                dynamic=False, **kwargs):
        """Groups object by one or more dimensions

        Applies groupby operation over the specified dimensions
        returning an object of type container_type (expected to be
        dictionary-like) containing the groups.

        Parameters
        ----------
        dimensions
            Dimension(s) to group by
        container_type
            Type to cast group container to
        group_type
            Type to cast each group to
        dynamic
            Whether to return a DynamicMap
        **kwargs
            Keyword arguments to pass to each group

        Returns
        -------
        Returns object of supplied container_type containing the
        groups. If dynamic=True returns a DynamicMap instead.
        """
        if dimensions is None:
            dimensions = []
        if not isinstance(dimensions, list): dimensions = [dimensions]
        if not dimensions: dimensions = self.dimensions('key', True)
        if group_type is None: group_type = type(self)

        dimensions = [self.get_dimension(d, strict=True) for d in dimensions]
        dim_names = [d.name for d in dimensions]

        if dynamic:
            group_dims = [kd for kd in self.kdims if kd not in dimensions]
            kdims = [self.get_dimension(d) for d in kwargs.pop('kdims', group_dims)]
            drop_dim = len(group_dims) != len(kdims)
            group_kwargs = dict(core_util.get_param_values(self), kdims=kdims)
            group_kwargs.update(kwargs)
            def load_subset(*args):
                constraint = dict(zip(dim_names, args, strict=None))
                group = self.select(**constraint)
                if np.isscalar(group):
                    return group_type(([group],), group=self.group,
                                      label=self.label, vdims=self.vdims)
                data = group.reindex(kdims)
                if drop_dim and self.interface.gridded:
                    data = data.columns()
                return group_type(data, **group_kwargs)
            dynamic_dims = [d.clone(values=list(self.interface.values(self, d.name, False)))
                            for d in dimensions]
            return DynamicMap(load_subset, kdims=dynamic_dims)

        return self.interface.groupby(self, dim_names, container_type,
                                      group_type, **kwargs)

    def transform(self, *args, **kwargs):
        """Transforms the Dataset according to a dimension transform.

        Transforms may be supplied as tuples consisting of the
        dimension(s) and the dim transform to apply or keyword
        arguments mapping from dimension(s) to dim transforms. If the
        arg or kwarg declares multiple dimensions the dim transform
        should return a tuple of values for each.

        A transform may override an existing dimension or add a new
        one in which case it will be added as an additional value
        dimension.

        Parameters
        ----------
        args
            Specify the output arguments and transforms as a
            tuple of dimension specs and dim transforms
        drop : bool
            Whether to drop all variables not part of the transform
        keep_index : bool
            Whether to keep indexes
                Whether to apply transform on datastructure with
                index, e.g. pandas.Series or xarray.DataArray,
                (important for dask datastructures where index may
                be required to align datasets).
        kwargs
            Specify new dimensions in the form new_dim=dim_transform

        Returns
        -------
        Transformed dataset with new dimensions
        """
        drop = kwargs.pop('drop', False)
        keep_index = kwargs.pop('keep_index', True)
        transforms = {}
        for s, transform in list(args)+list(kwargs.items()):
            transforms[core_util.wrap_tuple(s)] = transform

        new_data = {}
        for signature, transform in transforms.items():
            applied = transform.apply(
                self, compute=False, keep_index=keep_index
            )
            if len(signature) == 1:
                new_data[signature[0]] = applied
            else:
                for s, vals in zip(signature, applied, strict=None):
                    new_data[s] = vals

        new_dims = []
        for d in new_data:
            if self.get_dimension(d) is None:
                new_dims.append(d)

        ds = self
        if ds.interface.datatype in ('image', 'array'):
            ds = ds.clone(datatype=[dt for dt in ds.datatype if dt != ds.interface.datatype])

        if drop:
            kdims = [ds.get_dimension(d) for d in new_data if d in ds.kdims]
            vdims = [ds.get_dimension(d) or d for d in new_data if d not in ds.kdims]
            data = dict([(dimension_name(d), values) for d, values in new_data.items()])
            return ds.clone(data, kdims=kdims, vdims=vdims)
        else:
            new_data = dict([(dimension_name(d), values) for d, values in new_data.items()])
            data = ds.interface.assign(ds, new_data)
            data, drop = data if isinstance(data, tuple) else (data, [])
            kdims = [kd for kd in self.kdims if kd.name not in drop]
            return ds.clone(data, kdims=kdims, vdims=ds.vdims+new_dims)

    def __len__(self):
        """Number of values in the Dataset.

        """
        return self.interface.length(self)

    def __bool__(self):
        """Whether the Dataset contains any values

        """
        return self.interface.nonzero(self)

    @property
    def shape(self):
        """Returns the shape of the data.

        """
        return self.interface.shape(self)


    def dimension_values(self, dimension, expanded=True, flat=True):
        dim = self.get_dimension(dimension, strict=True)
        values = self.interface.values(self, dim, expanded, flat)
        if dim.nodata is not None:
            # Ensure nodata applies to boolean data in py2
            values = np.where(values==dim.nodata, np.nan, values)
        return values

    def get_dimension_type(self, dim):
        """Get the type of the requested dimension.

        Type is determined by Dimension.type attribute or common
        type of the dimension values, otherwise None.

        Parameters
        ----------
        dimension
            Dimension to look up by name or by index

        Returns
        -------
        Declared type of values along the dimension
        """
        dim_obj = self.get_dimension(dim)
        if dim_obj and dim_obj.type is not None:
            return dim_obj.type
        return self.interface.dimension_type(self, dim_obj)


    def dframe(self, dimensions=None, multi_index=False):
        """Convert dimension values to DataFrame.

        Returns a pandas dataframe of columns along each dimension,
        either completely flat or indexed by key dimensions.

        Parameters
        ----------
        dimensions
            Dimensions to return as columns
        multi_index
            Convert key dimensions to (multi-)index

        Returns
        -------
        DataFrame of columns corresponding to each dimension
        """
        if dimensions is None:
            dimensions = [d.name for d in self.dimensions()]
        else:
            dimensions = [self.get_dimension(d, strict=True).name for d in dimensions]
        df = self.interface.dframe(self, dimensions)
        if multi_index:
            df = df.set_index([d for d in dimensions if d in self.kdims])
        return df


    def columns(self, dimensions=None):
        """Convert dimension values to a dictionary.

        Returns a dictionary of column arrays along each dimension
        of the element.

        Parameters
        ----------
        dimensions
            Dimensions to return as columns

        Returns
        -------
        Dictionary of arrays for each dimension
        """
        if dimensions is None:
            dimensions = self.dimensions()
        else:
            dimensions = [self.get_dimension(d, strict=True) for d in dimensions]
        return dict([(d.name, self.dimension_values(d)) for d in dimensions])


    @property
    def to(self):
        """Returns the conversion interface with methods to convert Dataset

        """
        return self._conversion_interface(self)


    def clone(self, data=None, shared_data=True, new_type=None, link=True,
              *args, **overrides):
        """Clones the object, overriding data and parameters.

        Parameters
        ----------
        data
            New data replacing the existing data
        shared_data : bool, optional
            Whether to use existing data
        new_type : optional
            Type to cast object to
        link : bool, optional
            Whether clone should be linked
            Determines whether Streams and Links attached to
            original object will be inherited.
        *args
            Additional arguments to pass to constructor
        **overrides
            New keyword arguments to pass to constructor

        Returns
        -------
        Cloned object
        """
        if 'datatype' not in overrides:
            datatypes = [self.interface.datatype, *self.datatype]
            overrides['datatype'] = list(core_util.unique_iterator(datatypes))

        if data is None:
            overrides['_validate_vdims'] = False

            # Allows datatype conversions
            if shared_data:
                data = self
                if link:
                    overrides['plot_id'] = self._plot_id
        elif self._in_method and 'dataset' not in overrides:
            overrides['dataset'] = self.dataset

        return super().clone(data, shared_data, new_type, *args, **overrides)

    # Overrides of superclass methods that are needed so that PipelineMeta
    # will find them to wrap with pipeline support
    @wraps(Dimensioned.options)
    def options(self, *args, **kwargs):
        return super().options(*args, **kwargs)

    @wraps(LabelledData.map)
    def map(self, *args, **kwargs):
        return super().map(*args, **kwargs)

    @wraps(LabelledData.relabel)
    def relabel(self, *args, **kwargs):
        return super().relabel(*args, **kwargs)

    @property
    def iloc(self):
        """Returns iloc indexer with support for columnar indexing.

        Returns an iloc object providing a convenient interface to
        slice and index into the Dataset using row and column indices.
        Allow selection by integer index, slice and list of integer
        indices and boolean arrays.

        Examples :

        * Index the first row and column:

            dataset.iloc[0, 0]

        * Select rows 1 and 2 with a slice:

            dataset.iloc[1:3, :]

        * Select with a list of integer coordinates:

            dataset.iloc[[0, 2, 3]]
        """
        return iloc(self)


    @property
    def ndloc(self):
        """Returns ndloc indexer with support for gridded indexing.

        Returns an ndloc object providing nd-array like indexing for
        gridded datasets. Follows NumPy array indexing conventions,
        allowing for indexing, slicing and selecting a list of indices
        on multi-dimensional arrays using integer indices. The order
        of array indices is inverted relative to the Dataset key
        dimensions, e.g. an Image with key dimensions 'x' and 'y' can
        be indexed with ``image.ndloc[iy, ix]``, where ``iy`` and
        ``ix`` are integer indices along the y and x dimensions.

        Examples :

        * Index value in 2D array:

            dataset.ndloc[3, 1]

        * Slice along y-axis of 2D array:

            dataset.ndloc[2:5, :]

        * Vectorized (non-orthogonal) indexing along x- and y-axes:

            dataset.ndloc[[1, 2, 3], [0, 2, 3]]
        """
        return ndloc(self)

    # This is to work with spatialpandas-dask with dask 2025.1
    __dask_tokenize__ = __getstate__
