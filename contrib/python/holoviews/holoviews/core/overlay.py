"""Supplies Layer and related classes that allow overlaying of Views,
including Overlay. A Layer is the final extension of View base class
that allows Views to be overlaid on top of each other.

Also supplies ViewMap which is the primary multi-dimensional Map type
for indexing, slicing and animating collections of Views.
"""

from functools import reduce

import numpy as np
import param

from .dimension import Dimension, Dimensioned, ViewableElement, ViewableTree
from .layout import AdjointLayout, Composable, Layout, Layoutable
from .ndmapping import UniformNdMapping
from .util import dimensioned_streams, sanitize_identifier, unique_array


class Overlayable:
    """Overlayable provides a mix-in class to support the
    mul operation for overlaying multiple elements.

    """

    def __mul__(self, other):
        """Overlay object with other object.

        """
        # Local import to break the import cyclic dependency
        from .spaces import DynamicMap

        if isinstance(other, DynamicMap):
            from .spaces import Callable
            def dynamic_mul(*args, **kwargs):
                element = other[args]
                return self * element
            callback = Callable(dynamic_mul, inputs=[self, other])
            callback._is_overlay = True
            return other.clone(shared_data=False, callback=callback,
                               streams=dimensioned_streams(other))
        else:
            if isinstance(self, Overlay):
                if not isinstance(other, ViewableElement):
                    return NotImplemented
            elif isinstance(other, UniformNdMapping) and not isinstance(other, CompositeOverlay):
                items = [(k, self * v) for (k, v) in other.items()]
                return other.clone(items)
            elif isinstance(other, (AdjointLayout, ViewableTree)) and not isinstance(other, Overlay):
                return NotImplemented

            try:
                return Overlay([self, other])
            except NotImplementedError:
                return NotImplemented


class CompositeOverlay(ViewableElement, Composable):
    """CompositeOverlay provides a common baseclass for Overlay classes.

    """

    _deep_indexable = True

    def hist(self, dimension=None, num_bins=20, bin_range=None,
             adjoin=True, index=None, show_legend=False, **kwargs):
        """Computes and adjoins histogram along specified dimension(s).

        Defaults to first value dimension if present otherwise falls
        back to first key dimension.

        Parameters
        ----------
        dimension
            Dimension(s) to compute histogram on,
            Falls back the plot dimensions by default.
        num_bins : int, optional
            Number of bins
        bin_range : tuple, optional
            Lower and upper bounds of bins
        adjoin : bool, optional
            Whether to adjoin histogram
        index : int, optional
            Index of layer to apply hist to
        show_legend : bool, optional
            Show legend in histogram
            (don't show legend by default).

        Returns
        -------
        AdjointLayout of element and histogram or just the
        histogram
        """
        # Get main layer to get plot dimensions
        main_layer_int_index = getattr(self, "main_layer", None) or 0
        # Validate index, and extract as integer if not None
        if index is not None:
            valid_ind = isinstance(index, int) and (0 <= index < len(self))
            valid_label = index in [el.label for el in self]
            if not any([valid_ind, valid_label]):
                raise TypeError("Please supply a suitable index or label for the histogram data")
            if valid_ind:
                main_layer_int_index = index
            if valid_label:
                main_layer_int_index = self.keys().index(index)
        if dimension is None:
            # Fallback to default dimensions of main element
            dimension = [dim.name for dim in self.values()[main_layer_int_index].kdims]
        # Compute histogram for each dimension and each element in OverLay
        hists_per_dim = {
            dim: dict([  # All histograms for a given dimension
                (
                    elem_key, elem.hist(
                        adjoin=False, dimension=dim, bin_range=bin_range,
                        num_bins=num_bins, **kwargs
                    )
                )
                for i, (elem_key, elem) in enumerate(self.items())
                if (index is None) or (getattr(elem, "label", None) == index) or (index == i)
            ])
            for dim in dimension
        }
        # Create new Overlays of histograms
        hists_overlay_per_dim = {
            dim: self.clone(hists).opts(show_legend=show_legend)
            for dim, hists in hists_per_dim.items()
        }
        if adjoin:
            layout = self
            for dim in reversed(self.values()[main_layer_int_index].kdims):
                if dim.name in hists_overlay_per_dim:
                    layout = layout << hists_overlay_per_dim[dim.name]
            layout.main_layer = main_layer_int_index
        elif len(dimension) > 1:
            layout = Layout(list(hists_overlay_per_dim.values()))
        else:
            layout = hists_overlay_per_dim[0]
        return layout


    def dimension_values(self, dimension, expanded=True, flat=True):
        values = []
        found = False
        for el in self:
            if dimension in el.dimensions(label=True):
                values.append(el.dimension_values(dimension))
                found = True
        if not found:
            return super().dimension_values(dimension, expanded, flat)
        values = [v for v in values if v is not None and len(v)]
        if not values:
            return np.array()
        vals = np.concatenate(values)
        return vals if expanded else unique_array(vals)


class Overlay(ViewableTree, CompositeOverlay, Layoutable, Overlayable):
    """An Overlay consists of multiple Elements (potentially of
    heterogeneous type) presented one on top each other with a
    particular z-ordering.

    Overlays along with elements constitute the only valid leaf types of
    a Layout and in fact extend the Layout structure. Overlays are
    constructed using the * operator (building an identical structure
    to the + operator).

    """

    def __init__(self, items=None, group=None, label=None, **params):
        self.__dict__['_fixed'] = False
        self.__dict__['_group'] = group
        self.__dict__['_label'] = label
        super().__init__(items, **params)

    def __getitem__(self, key):
        """Allows transparently slicing the Elements in the Overlay
        to select specific layers in an Overlay use the .get method.

        """
        return Overlay([(k, v[key]) for k, v in self.items()])


    def get(self, identifier, default=None):
        """Get a layer in the Overlay.

        Get a particular layer in the Overlay using its path string
        or an integer index.

        Parameters
        ----------
        identifier
            Index or path string of the item to return
        default
            Value to return if no item is found

        Returns
        -------
        The indexed layer of the Overlay
        """
        if isinstance(identifier, int):
            values = list(self.data.values())
            if 0 <= identifier < len(values):
                return values[identifier]
            else:
                return default
        return super().get(identifier, default)

    def collate(self):
        """Collates any objects in the Overlay resolving any issues
        the recommended nesting structure.

        """
        return reduce(lambda x,y: x*y, self.values())

    def decollate(self):
        """Packs Overlay of DynamicMaps into a single DynamicMap that returns an Overlay

        Decollation allows packing an Overlay of DynamicMaps into a single DynamicMap
        that returns an Overlay of simple (non-dynamic) elements. All nested streams
        are lifted to the resulting DynamicMap, and are available in the `streams`
        property.  The `callback` property of the resulting DynamicMap is a pure,
        stateless function of the stream values. To avoid stream parameter name
        conflicts, the resulting DynamicMap is configured with
        positional_stream_args=True, and the callback function accepts stream values
        as positional dict arguments.

        Returns
        -------
        DynamicMap that returns an Overlay
        """
        from .decollate import decollate
        return decollate(self)

    @property
    def group(self):
        if self._group:
            return self._group
        elements = [el for el in self if not el._auxiliary_component]
        values = {el.group for el in elements}
        types = {type(el) for el in elements}
        if values:
            group = next(iter(values))
            vtype = next(iter(types)).__name__
        else:
            group, vtype = [], ''
        if len(values) == 1 and group != vtype:
            return group
        else:
            return type(self).__name__

    @group.setter
    def group(self, group):
        if not sanitize_identifier.allowable(group):
            raise ValueError(f"Supplied group {group} contains invalid characters.")
        else:
            self._group = group

    @property
    def label(self):
        if self._label:
            return self._label
        labels = {el.label for el in self
                  if not el._auxiliary_component}
        if len(labels) == 1:
            return next(iter(labels))
        else:
            return ''

    @label.setter
    def label(self, label):
        if not sanitize_identifier.allowable(label):
            raise ValueError(f"Supplied group {label} contains invalid characters.")
        self._label = label

    @property
    def ddims(self):
        dimensions = []
        dimension_names = []
        for el in self:
            for dim in el.dimensions():
                if dim.name not in dimension_names:
                    dimensions.append(dim)
                    dimension_names.append(dim.name)
        return dimensions

    @property
    def shape(self):
        raise NotImplementedError

    def clone(self, data=None, shared_data=True, new_type=None, link=True, **overrides):
        if data is None and link:
            overrides['plot_id'] = self._plot_id
        return super().clone(data, shared_data=shared_data, new_type=new_type, link=link, **overrides)


class NdOverlay(Overlayable, UniformNdMapping, CompositeOverlay):
    """An NdOverlay allows a group of NdOverlay to be overlaid together. NdOverlay can
    be indexed out of an overlay and an overlay is an iterable that iterates
    over the contained layers.

    """

    kdims = param.List(default=[Dimension('Element')], constant=True, doc="""
        List of dimensions the NdOverlay can be indexed by.""")

    _deep_indexable = True

    def __init__(self, overlays=None, kdims=None, **params):
        super().__init__(overlays, kdims=kdims, **params)

    def decollate(self):
        """Packs NdOverlay of DynamicMaps into a single DynamicMap that returns an
        NdOverlay

        Decollation allows packing a NdOverlay of DynamicMaps into a single DynamicMap
        that returns an NdOverlay of simple (non-dynamic) elements. All nested streams
        are lifted to the resulting DynamicMap, and are available in the `streams`
        property.  The `callback` property of the resulting DynamicMap is a pure,
        stateless function of the stream values. To avoid stream parameter name
        conflicts, the resulting DynamicMap is configured with
        positional_stream_args=True, and the callback function accepts stream values
        as positional dict arguments.

        Returns
        -------
        DynamicMap that returns an NdOverlay
        """
        from .decollate import decollate
        return decollate(self)


__all__ = [
    *{_k for _k, _v in locals().items() if isinstance(_v, type) and issubclass(_v, Dimensioned)},
    "Overlayable"
]
