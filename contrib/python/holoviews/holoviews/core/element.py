from itertools import groupby

import numpy as np
import param

from .dimension import Dimensioned, ViewableElement, asdim
from .layout import Composable, Layout, NdLayout
from .ndmapping import NdMapping
from .overlay import CompositeOverlay, NdOverlay, Overlayable
from .spaces import GridSpace, HoloMap
from .tree import AttrTree
from .util import dtype_kind, get_param_values


class Element(ViewableElement, Composable, Overlayable):
    """Element is the atomic datastructure used to wrap some data with
    an associated visual representation, e.g. an element may
    represent a set of points, an image or a curve. Elements provide
    a common API for interacting with data of different types and
    define how the data map to a set of dimensions and how those
    map to the visual representation.

    """

    group = param.String(default='Element', constant=True)

    __abstract = True

    _selection_streams = ()

    def _get_selection_expr_for_stream_value(self, **kwargs):
        return None, None, None

    def hist(self, dimension=None, num_bins=20, bin_range=None,
             adjoin=True, **kwargs):
        """Computes and adjoins histogram along specified dimension(s).

        Defaults to first value dimension if present otherwise falls
        back to first key dimension.

        Parameters
        ----------
        dimension
            Dimension(s) to compute histogram on
        num_bins : int, optional
            Number of bins
        bin_range : tuple, optional
            Lower and upper bounds of bins
        adjoin : bool, optional
            Whether to adjoin histogram

        Returns
        -------
        AdjointLayout of element and histogram or just the
        histogram
        """
        from ..operation import histogram
        if not isinstance(dimension, list): dimension = [dimension]
        hists = []
        for d in dimension[::-1]:
            hist = histogram(self, num_bins=num_bins, bin_range=bin_range,
                             dimension=d, **kwargs)
            hists.append(hist)
        if adjoin:
            layout = self
            for didx in range(len(dimension)):
                layout = layout << hists[didx]
        elif len(dimension) > 1:
            layout = Layout(hists)
        else:
            layout = hists[0]
        return layout

    #======================#
    # Subclassable methods #
    #======================#


    def __getitem__(self, key):
        if key == ():
            return self
        else:
            raise NotImplementedError(f"{type(self).__name__} currently does not support getitem")

    def __bool__(self):
        """Indicates whether the element is empty.

        Subclasses may override this to signal that the Element
        contains no data and can safely be dropped during indexing.

        """
        return True

    def __contains__(self, dimension):
        """Whether element contains the Dimension

        """
        return dimension in self.dimensions()

    def __iter__(self):
        """Disable iterator interface.

        """
        raise NotImplementedError('Iteration on Elements is not supported.')

    def closest(self, coords, **kwargs):
        """Snap list or dict of coordinates to closest position.

        Parameters
        ----------
        coords
            List of 1D or 2D coordinates
        **kwargs
            Coordinates specified as keyword pairs

        Returns
        -------
        List of tuples of the snapped coordinates

        Raises
        ------
        NotImplementedError
            Raised if snapping is not supported
        """
        raise NotImplementedError

    def sample(self, samples=None, bounds=None, closest=False, **sample_values):
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
        samples
            List of nd-coordinates to sample
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
        raise NotImplementedError


    def reduce(self, dimensions=None, function=None, spreadfn=None, **reduction):
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
        The element after reductions have been applied.
        """
        if dimensions is None:
            dimensions = []
        raise NotImplementedError


    def _reduce_map(self, dimensions, function, reduce_map):
        if dimensions and reduce_map:
            raise Exception("Pass reduced dimensions either as an argument "
                            "or as part of the kwargs not both.")
        if len(set(reduce_map.values())) > 1:
            raise Exception("Cannot define reduce operations with more than "
                            "one function at a time.")
        if reduce_map:
            reduce_map = reduce_map.items()
        if dimensions:
            reduce_map = [(d, function) for d in dimensions]
        elif not reduce_map:
            reduce_map = [(d, function) for d in self.kdims]
        reduced = [(self.get_dimension(d, strict=True).name, fn)
                   for d, fn in reduce_map]
        grouped = [(fn, [dim for dim, _ in grp]) for fn, grp in groupby(reduced, lambda x: x[1])]
        return grouped[0]


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
        import pandas as pd

        if dimensions is None:
            dimensions = [d.name for d in self.dimensions()]
        else:
            dimensions = [self.get_dimension(d, strict=True).name for d in dimensions]
        column_names = dimensions
        dim_vals = dict([(dim, self.dimension_values(dim)) for dim in column_names])
        df = pd.DataFrame(dim_vals)
        if multi_index:
            df = df.set_index([d for d in dimensions if d in self.kdims])
        return df


    def array(self, dimensions=None):
        """Convert dimension values to columnar array.

        Parameters
        ----------
        dimensions
            List of dimensions to return

        Returns
        -------
        Array of columns corresponding to each dimension
        """
        if dimensions is None:
            dims = [d for d in self.kdims + self.vdims]
        else:
            dims = [self.get_dimension(d, strict=True) for d in dimensions]

        columns, types = [], []
        for dim in dims:
            column = np.asanyarray(self.dimension_values(dim))
            columns.append(column)
            types.append(dtype_kind(column))
        if len(set(types)) > 1:
            columns = [c.astype('object') for c in columns]
        return np.column_stack(columns)


class Tabular(Element):
    """Baseclass to give an elements providing an API to generate a
    tabular representation of the object.

    """

    __abstract = True

    @property
    def rows(self):
        """Number of rows in table (including header)

        """
        return len(self) + 1

    @property
    def cols(self):
        """Number of columns in table

        """
        return len(self.dimensions())


    def pprint_cell(self, row, col):
        """Formatted contents of table cell.

        Parameters
        ----------
        row : int
            Integer index of table row
        col : int
            Integer index of table column

        Returns
        -------
        Formatted table cell contents
        """
        ndims = self.ndims
        if col >= self.cols:
            raise Exception(f"Maximum column index is {self.cols-1}")
        elif row >= self.rows:
            raise Exception(f"Maximum row index is {self.cols-1}")
        elif row == 0:
            if col >= ndims:
                if self.vdims:
                    return self.vdims[col - ndims].pprint_label
                else:
                    return ''
            return self.kdims[col].pprint_label
        else:
            dim = self.get_dimension(col)
            return dim.pprint_value(self.iloc[row-1, col])


    def cell_type(self, row, col):
        """Type of the table cell, either 'data' or 'heading'

        Parameters
        ----------
        row : int
            Integer index of table row
        col : int
            Integer index of table column

        Returns
        -------
        Type of the table cell, either 'data' or 'heading'
        """
        return 'heading' if row == 0 else 'data'



class Element2D(Element):

    extents = param.Tuple(default=(None, None, None, None),doc="""
        Allows overriding the extents of the Element in 2D space defined
        as four-tuple defining the (left, bottom, right and top) edges.""")

    __abstract = True


class Element3D(Element2D):

    extents = param.Tuple(default=(None, None, None, None, None, None), doc="""
        Allows overriding the extents of the Element in 3D space
        defined as (xmin, ymin, zmin, xmax, ymax, zmax).""")

    __abstract = True

    _selection_streams = ()


class Collator(NdMapping):
    """Collator is an NdMapping type which can merge any number
    of HoloViews components with whatever level of nesting
    by inserting the Collators key dimensions on the HoloMaps.
    If the items in the Collator do not contain HoloMaps
    they will be created. Collator also supports filtering
    of Tree structures and dropping of constant dimensions.

    """

    drop = param.List(default=[], doc="""
        List of dimensions to drop when collating data, specified
        as strings.""")

    drop_constant = param.Boolean(default=False, doc="""
        Whether to demote any non-varying key dimensions to
        constant dimensions.""")

    filters = param.List(default=[], doc="""
        List of paths to drop when collating data, specified
        as strings or tuples.""")

    group = param.String(default='Collator')

    progress_bar = param.Parameter(default=None, doc="""
         The progress bar instance used to report progress. Set to
         None to disable progress bars.""")

    merge_type = param.ClassSelector(class_=NdMapping, default=HoloMap,
                                     is_instance=False,instantiate=False)

    value_transform = param.Callable(default=None, doc="""
        If supplied the function will be applied on each Collator
        value during collation. This may be used to apply an operation
        to the data or load references from disk before they are collated
        into a displayable HoloViews object.""")

    vdims = param.List(default=[], doc="""
         Collator operates on HoloViews objects, if vdims are specified
         a value_transform function must also be supplied.""")

    _deep_indexable = False
    _auxiliary_component = False

    _nest_order = {HoloMap: ViewableElement,
                   GridSpace: (HoloMap, CompositeOverlay, ViewableElement),
                   NdLayout: (GridSpace, HoloMap, ViewableElement),
                   NdOverlay: Element}

    def __init__(self, data=None, **params):
        if isinstance(data, Element):
            params = dict(get_param_values(data), **params)
            if 'kdims' not in params:
                params['kdims'] = data.kdims
            if 'vdims' not in params:
                params['vdims'] = data.vdims
            data = data.mapping()
        super().__init__(data, **params)


    def __call__(self):
        """Filter each Layout in the Collator with the supplied
        path_filters. If merge is set to True all Layouts are
        merged, otherwise an NdMapping containing all the
        Layouts is returned. Optionally a list of dimensions
        to be ignored can be supplied.

        """
        constant_dims = self.static_dimensions
        ndmapping = NdMapping(kdims=self.kdims)

        num_elements = len(self)
        for idx, (key, data) in enumerate(self.data.items()):
            if isinstance(data, AttrTree):
                data = data.filter(self.filters)
            if len(self.vdims) and self.value_transform:
                vargs = dict(zip(self.dimensions('value', label=True), data, strict=None))
                data = self.value_transform(vargs)
            if not isinstance(data, Dimensioned):
                raise ValueError("Collator values must be Dimensioned objects "
                                 "before collation.")

            varying_keys = [(d, k) for d, k in zip(self.kdims, key, strict=None) if not self.drop_constant or
                            (d not in constant_dims and d not in self.drop)]
            constant_keys = {d: k for d, k in zip(self.kdims, key, strict=None) if d in constant_dims
                             and d not in self.drop and self.drop_constant}
            if varying_keys or constant_keys:
                data = self._add_dimensions(data, varying_keys, constant_keys)
            ndmapping[key] = data
            if self.progress_bar is not None:
                self.progress_bar(float(idx+1)/num_elements*100)

        components = ndmapping.values()
        accumulator = ndmapping.last.clone(components[0].data)
        for component in components:
            accumulator.update(component)
        return accumulator


    @property
    def static_dimensions(self):
        """Return all constant dimensions.

        """
        dimensions = []
        for dim in self.kdims:
            if len(set(self.dimension_values(dim.name))) == 1:
                dimensions.append(dim)
        return dimensions


    def _add_dimensions(self, item, dims, constant_keys):
        """Recursively descend through an Layout and NdMapping objects
        in order to add the supplied dimension values to all contained
        HoloMaps.

        """
        if isinstance(item, Layout):
            item.fixed = False

        dim_vals = [(dim, val) for dim, val in dims[::-1]
                    if dim not in self.drop]
        if isinstance(item, self.merge_type):
            new_item = item.clone(cdims=constant_keys)
            for dim, val in dim_vals:
                dim = asdim(dim)
                if dim not in new_item.kdims:
                    new_item = new_item.add_dimension(dim, 0, val)
        elif isinstance(item, self._nest_order[self.merge_type]):
            if dim_vals:
                dimensions, key = zip(*dim_vals, strict=None)
                new_item = self.merge_type({key: item}, kdims=list(dimensions),
                                           cdims=constant_keys)
            else:
                new_item = item
        else:
            new_item = item.clone(shared_data=False, cdims=constant_keys)
            for k, v in item.items():
                new_item[k] = self._add_dimensions(v, dims[::-1], constant_keys)
        if isinstance(new_item, Layout):
            new_item.fixed = True

        return new_item


__all__ = list({_k for _k, _v in locals().items()
                    if isinstance(_v, type) and issubclass(_v, Dimensioned)})
