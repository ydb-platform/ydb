"""Supplies Pane, Layout, NdLayout and AdjointLayout. Pane extends View
to allow multiple Views to be presented side-by-side in a NdLayout. An
AdjointLayout allows one or two Views to be adjoined to a primary View
to act as supplementary elements.

"""

import numpy as np
import param

from . import traversal
from .dimension import Dimension, Dimensioned, ViewableElement, ViewableTree
from .ndmapping import NdMapping, UniformNdMapping


class Layoutable:
    """Layoutable provides a mix-in class to support the
    add operation for creating a layout from the operands.

    """

    def __add__(x, y):
        """Compose objects into a Layout"""
        if any(isinstance(arg, int) for arg in (x, y)):
            raise TypeError(f"unsupported operand type(s) for +: {x.__class__.__name__} and {y.__class__.__name__}. "
                            "If you are trying to use a reduction like `sum(elements)` "
                            "to combine a list of elements, we recommend you use "
                            "`Layout(elements)` (and similarly `Overlay(elements)` for "
                            "making an overlay from a list) instead.")
        try:
            return Layout([x, y])
        except NotImplementedError:
            return NotImplemented

    def __radd__(self, other):
        return self.__class__.__add__(other, self)


class Composable(Layoutable):
    """Composable is a mix-in class to allow Dimensioned objects to be
    embedded within Layouts and GridSpaces.

    """

    def __lshift__(self, other):
        """Compose objects into an AdjointLayout

        """
        if isinstance(other, (ViewableElement, NdMapping, Empty)):
            return AdjointLayout([self, other])
        elif isinstance(other, AdjointLayout):
            return AdjointLayout([*other.data.values(), self])
        else:
            raise TypeError(f'Cannot append {type(other).__name__} to a AdjointLayout')



class Empty(Dimensioned, Composable):
    """Empty may be used to define an empty placeholder in a Layout. It
    can be placed in a Layout just like any regular Element and
    container type via the + operator or by passing it to the Layout
    constructor as a part of a list.

    """

    group = param.String(default='Empty')

    def __init__(self, **params):
        super().__init__(None, **params)



class AdjointLayout(Layoutable, Dimensioned):
    """An AdjointLayout provides a convenient container to lay out some
    marginal plots next to a primary plot. This is often useful to
    display the marginal distributions of a plot next to the primary
    plot. AdjointLayout accepts a list of up to three elements, which
    are laid out as follows with the names 'main', 'top' and 'right'::

        ________________
        |     3     |   |
        |___________|___|
        |           |   |  1:  main
        |           |   |  2:  right
        |     1     | 2 |  3:  top
        |           |   |
        |___________|___|

    """

    kdims = param.List(default=[Dimension('AdjointLayout')], constant=True)

    layout_order = ['main', 'right', 'top']

    _deep_indexable = True
    _auxiliary_component = False

    def __init__(self, data, **params):
        self.main_layer = 0 # The index of the main layer if .main is an overlay
        if data and len(data) > 3:
            raise Exception('AdjointLayout accepts no more than three elements.')

        if data is not None and all(isinstance(v, tuple) for v in data):
            data = dict(data)
        if isinstance(data, dict):
            wrong_pos = [k for k in data if k not in self.layout_order]
            if wrong_pos:
                raise Exception('Wrong AdjointLayout positions provided.')
        elif isinstance(data, list):
            data = dict(zip(self.layout_order, data, strict=None))
        else:
            data = {}

        super().__init__(data, **params)


    def __mul__(self, other, reverse=False):
        layer1 = other if reverse else self
        layer2 = self if reverse else other
        adjoined_items = []
        if isinstance(layer1, AdjointLayout) and isinstance(layer2, AdjointLayout):
            adjoined_items = []
            adjoined_items.append(layer1.main*layer2.main)
            if layer1.right is not None and layer2.right is not None:
                if layer1.right.dimensions() == layer2.right.dimensions():
                    adjoined_items.append(layer1.right*layer2.right)
                else:
                    adjoined_items += [layer1.right, layer2.right]
            elif layer1.right is not None:
                adjoined_items.append(layer1.right)
            elif layer2.right is not None:
                adjoined_items.append(layer2.right)

            if layer1.top is not None and layer2.top is not None:
                if layer1.top.dimensions() == layer2.top.dimensions():
                    adjoined_items.append(layer1.top*layer2.top)
                else:
                    adjoined_items += [layer1.top, layer2.top]
            elif layer1.top is not None:
                adjoined_items.append(layer1.top)
            elif layer2.top is not None:
                adjoined_items.append(layer2.top)
            if len(adjoined_items) > 3:
                raise ValueError("AdjointLayouts could not be overlaid, "
                                 "the dimensions of the adjoined plots "
                                 "do not match and the AdjointLayout can "
                                 "hold no more than two adjoined plots.")
        elif isinstance(layer1, AdjointLayout):
            adjoined_items = [layer1.data[o] for o in self.layout_order
                              if o in layer1.data]
            adjoined_items[0] = layer1.main * layer2
        elif isinstance(layer2, AdjointLayout):
            adjoined_items = [layer2.data[o] for o in self.layout_order
                              if o in layer2.data]
            adjoined_items[0] = layer1 * layer2.main

        if adjoined_items:
            return self.clone(adjoined_items)
        else:
            return NotImplemented


    def __rmul__(self, other):
        return self.__mul__(other, reverse=True)


    @property
    def group(self):
        """Group inherited from main element

        """
        if self.main and self.main.group != type(self.main).__name__:
            return self.main.group
        else:
            return 'AdjointLayout'

    @property
    def label(self):
        """Label inherited from main element

        """
        return self.main.label if self.main else ''


    # Both group and label need empty setters due to param inheritance
    @group.setter
    def group(self, group): pass
    @label.setter
    def label(self, label): pass


    def relabel(self, label=None, group=None, depth=1):
        """Clone object and apply new group and/or label.

        Applies relabeling to child up to the supplied depth.

        Parameters
        ----------
        label : str, optional
            New label to apply to returned object
        group : str, optional
            New group to apply to returned object
        depth : int, optional
            Depth to which relabel will be applied
            If applied to container allows applying relabeling to
            contained objects up to the specified depth

        Returns
        -------
        Returns relabelled object
        """
        return super().relabel(label=label, group=group, depth=depth)


    def get(self, key, default=None):
        """Returns the viewable corresponding to the supplied string
        or integer based key.

        Parameters
        ----------
        key : Numeric or string index
            * 0: 'main'
            * 1: 'right'
            * 2: 'top'
        default
            Value returned if key not found

        Returns
        -------
        Indexed value or supplied default
        """
        return self.data[key] if key in self.data else default


    def dimension_values(self, dimension, expanded=True, flat=True):
        dimension = self.get_dimension(dimension, strict=True).name
        return self.main.dimension_values(dimension, expanded, flat)


    def __getitem__(self, key):
        """Index into the AdjointLayout by index or label

        """
        if key == ():
            return self

        data_slice = None
        if isinstance(key, tuple):
            data_slice = key[1:]
            key = key[0]

        if isinstance(key, int) and key <= len(self):
            if key == 0:  data = self.main
            if key == 1:  data = self.right
            if key == 2:  data = self.top
            if data_slice: data = data[data_slice]
            return data
        elif isinstance(key, str) and key in self.data:
            if data_slice is None:
                return self.data[key]
            else:
                self.data[key][data_slice]
        elif isinstance(key, slice) and key.start is None and key.stop is None:
            return self if data_slice is None else self.clone([el[data_slice]
                                                               for el in self])
        else:
            raise KeyError(f"Key {key} not found in AdjointLayout.")


    def __setitem__(self, key, value):
        if key in ['main', 'right', 'top']:
            if isinstance(value, (ViewableElement, UniformNdMapping, Empty)):
                self.data[key] = value
            else:
                raise ValueError('AdjointLayout only accepts Element types.')
        else:
            raise Exception(f'Position {key} not valid in AdjointLayout.')


    def __lshift__(self, other):
        """Add another plot to the AdjointLayout

        """
        views = [self.data.get(k, None) for k in self.layout_order]
        return AdjointLayout([v for v in views if v is not None] + [other])


    @property
    def ddims(self):
        return self.main.dimensions()

    @property
    def main(self):
        """Returns the main element in the AdjointLayout

        """
        return self.data.get('main', None)

    @property
    def right(self):
        """Returns the right marginal element in the AdjointLayout

        """
        return self.data.get('right', None)

    @property
    def top(self):
        """Returns the top marginal element in the AdjointLayout

        """
        return self.data.get('top', None)

    @property
    def last(self):
        items = [(k, v.last) if isinstance(v, NdMapping) else (k, v)
                 for k, v in self.data.items()]
        return self.__class__(dict(items))


    def keys(self):
        return list(self.data.keys())


    def items(self):
        return list(self.data.items())


    def __iter__(self):
        i = 0
        while i < len(self):
            yield self[i]
            i += 1

    def __len__(self):
        """Number of items in the AdjointLayout

        """
        return len(self.data)



class NdLayout(Layoutable, UniformNdMapping):
    """NdLayout is a UniformNdMapping providing an n-dimensional
    data structure to display the contained Elements and containers
    in a layout. Using the cols method the NdLayout can be rearranged
    with the desired number of columns.

    """

    data_type = (ViewableElement, AdjointLayout, UniformNdMapping)

    def __init__(self, initial_items=None, kdims=None, **params):
        self._max_cols = 4
        self._style = None
        super().__init__(initial_items=initial_items, kdims=kdims,
                         **params)


    @property
    def uniform(self):
        return traversal.uniform(self)


    @property
    def shape(self):
        """Tuple indicating the number of rows and columns in the NdLayout.

        """
        num = len(self.keys())
        if num <= self._max_cols:
            return (1, num)
        nrows = num // self._max_cols
        last_row_cols = num % self._max_cols
        return nrows+(1 if last_row_cols else 0), min(num, self._max_cols)


    def grid_items(self):
        """Compute a dict of {(row,column): (key, value)} elements from the
        current set of items and specified number of columns.

        """
        if list(self.keys()) == []:  return {}
        cols = self._max_cols
        return {(idx // cols, idx % cols): (key, item)
                for idx, (key, item) in enumerate(self.data.items())}


    def cols(self, ncols):
        """Sets the maximum number of columns in the NdLayout.

        Any items beyond the set number of cols will flow onto a new
        row. The number of columns control the indexing and display
        semantics of the NdLayout.

        Parameters
        ----------
        ncols : int
            Number of columns to set on the NdLayout
        """
        self._max_cols = ncols
        return self

    @property
    def last(self):
        """Returns another NdLayout constituted of the last views of the
        individual elements (if they are maps).

        """
        last_items = []
        for (k, v) in self.items():
            if isinstance(v, NdMapping):
                item = (k, v.clone((v.last_key, v.last)))
            elif isinstance(v, AdjointLayout):
                item = (k, v.last)
            else:
                item = (k, v)
            last_items.append(item)
        return self.clone(last_items)


    def clone(self, *args, **overrides):
        """Clones the NdLayout, overriding data and parameters.

        Parameters
        ----------
        data
            New data replacing the existing data
        shared_data : bool, optional
            Whether to use existing data
        new_type : optional
            Type to cast object to
        *args
            Additional arguments to pass to constructor
        **overrides
            New keyword arguments to pass to constructor

        Returns
        -------
        Cloned NdLayout object
        """
        clone = super().clone(*args, **overrides)
        clone._max_cols = self._max_cols
        clone.id = self.id
        return clone


class Layout(Layoutable, ViewableTree):
    """A Layout is an ViewableTree with ViewableElement objects as leaf
    values.

    Unlike ViewableTree, a Layout supports a rich display,
    displaying leaf items in a grid style layout. In addition to the
    usual ViewableTree indexing, Layout supports indexing of items by
    their row and column index in the layout.

    The maximum number of columns in such a layout may be controlled
    with the cols method.

    """

    group = param.String(default='Layout', constant=True)

    _deep_indexable = True

    def __init__(self, items=None, identifier=None, parent=None, **kwargs):
        self.__dict__['_max_cols'] = 4
        super().__init__(items, identifier, parent, **kwargs)

    def decollate(self):
        """Packs Layout of DynamicMaps into a single DynamicMap that returns a Layout

        Decollation allows packing a Layout of DynamicMaps into a single DynamicMap
        that returns a Layout of simple (non-dynamic) elements. All nested streams are
        lifted to the resulting DynamicMap, and are available in the `streams`
        property.  The `callback` property of the resulting DynamicMap is a pure,
        stateless function of the stream values. To avoid stream parameter name
        conflicts, the resulting DynamicMap is configured with
        positional_stream_args=True, and the callback function accepts stream values
        as positional dict arguments.

        Returns
        -------
        DynamicMap that returns a Layout
        """
        from .decollate import decollate
        return decollate(self)

    @property
    def shape(self):
        """Tuple indicating the number of rows and columns in the Layout.

        """
        num = len(self)
        if num <= self._max_cols:
            return (1, num)
        nrows = num // self._max_cols
        last_row_cols = num % self._max_cols
        return nrows+(1 if last_row_cols else 0), min(num, self._max_cols)


    def __getitem__(self, key):
        """Allows indexing Layout by row and column or path

        """
        if isinstance(key, int):
            if key < len(self):
                return list(self.data.values())[key]
            raise KeyError("Element out of range.")
        elif isinstance(key, slice):
            raise KeyError("A Layout may not be sliced, ensure that you "
                           "are slicing on a leaf (i.e. not a branch) of the Layout.")
        if len(key) == 2 and not any([isinstance(k, str) for k in key]):
            if key == (slice(None), slice(None)): return self
            row, col = key
            idx = row * self._max_cols + col
            keys = list(self.data.keys())
            if idx >= len(keys) or col >= self._max_cols:
                raise KeyError(f'Index {key} is outside available item range')
            key = keys[idx]
        return super().__getitem__(key)


    def clone(self, *args, **overrides):
        """Clones the Layout, overriding data and parameters.

        Parameters
        ----------
        data
            New data replacing the existing data
        shared_data : bool, optional
            Whether to use existing data
        new_type : optional
            Type to cast object to
        *args
            Additional arguments to pass to constructor
        **overrides
            New keyword arguments to pass to constructor

        Returns
        -------
        Cloned Layout object
        """
        clone = super().clone(*args, **overrides)
        clone._max_cols = self._max_cols
        return clone


    def cols(self, ncols):
        """Sets the maximum number of columns in the NdLayout.

        Any items beyond the set number of cols will flow onto a new
        row. The number of columns control the indexing and display
        semantics of the NdLayout.

        Parameters
        ----------
        ncols : int
            Number of columns to set on the NdLayout
        """
        self._max_cols = ncols
        return self

    def relabel(self, label=None, group=None, depth=1):
        """Clone object and apply new group and/or label.

        Applies relabeling to children up to the supplied depth.

        Parameters
        ----------
        label : str, optional
            New label to apply to returned object
        group : str, optional
            New group to apply to returned object
        depth : int, optional
            Depth to which relabel will be applied
            If applied to container allows applying relabeling to
            contained objects up to the specified depth

        Returns
        -------
        Returns relabelled object
        """
        return super().relabel(label, group, depth)

    def grid_items(self):
        return {tuple(np.unravel_index(idx, self.shape)): (path, item)
                for idx, (path, item) in enumerate(self.items())}

    def __mul__(self, other, reverse=False):
        from .spaces import HoloMap
        if not isinstance(other, (ViewableElement, HoloMap)):
            return NotImplemented
        layout = Layout([other*v if reverse else v*other for v in self])
        layout._max_cols = self._max_cols
        return layout

    def __rmul__(self, other):
        return self.__mul__(other, reverse=True)


__all__ = list({_k for _k, _v in locals().items()
                    if isinstance(_v, type) and (issubclass(_v, Dimensioned)
                                                 or issubclass(_v, Layout))})
