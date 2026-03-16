import itertools
import types
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from itertools import groupby
from numbers import Number
from types import FunctionType

import numpy as np
import param

from ..streams import Params, Stream, streams_list_from_dict
from . import traversal, util
from .accessors import Opts, Redim
from .dimension import Dimension, ViewableElement
from .layout import AdjointLayout, Empty, Layout, Layoutable, NdLayout
from .ndmapping import NdMapping, UniformNdMapping, item_check
from .options import Store, StoreOptions
from .overlay import CompositeOverlay, NdOverlay, Overlay, Overlayable
from .util import dtype_kind


class HoloMap(Layoutable, UniformNdMapping, Overlayable):
    """A HoloMap is an n-dimensional mapping of viewable elements or
    overlays. Each item in a HoloMap has an tuple key defining the
    values along each of the declared key dimensions, defining the
    discretely sampled space of values.

    The visual representation of a HoloMap consists of the viewable
    objects inside the HoloMap which can be explored by varying one
    or more widgets mapping onto the key dimensions of the HoloMap.

    """

    data_type = (ViewableElement, NdMapping, Layout)

    def __init__(self, initial_items=None, kdims=None, group=None, label=None, **params):
        super().__init__(initial_items, kdims, group, label, **params)

    @property
    def opts(self):
        return Opts(self, mode='holomap')

    def overlay(self, dimensions=None, **kwargs):
        """Group by supplied dimension(s) and overlay each group

        Groups data by supplied dimension(s) overlaying the groups
        along the dimension(s).

        Parameters
        ----------
        dimensions
            Dimension(s) of dimensions to group by

        Returns
        -------
        NdOverlay object(s) with supplied dimensions
        """
        dimensions = self._valid_dimensions(dimensions)
        if len(dimensions) == self.ndims:
            with item_check(False):
                return NdOverlay(self, **kwargs).reindex(dimensions)
        else:
            dims = [d for d in self.kdims if d not in dimensions]
            return self.groupby(dims, group_type=NdOverlay, **kwargs)


    def grid(self, dimensions=None, **kwargs):
        """Group by supplied dimension(s) and lay out groups in grid

        Groups data by supplied dimension(s) laying the groups along
        the dimension(s) out in a GridSpace.

        Parameters
        ----------
        dimensions : Dimension/str or list
            Dimension or list of dimensions to group by

        Returns
        -------
        GridSpace with supplied dimensions
        """
        dimensions = self._valid_dimensions(dimensions)
        if len(dimensions) == self.ndims:
            with item_check(False):
                return GridSpace(self, **kwargs).reindex(dimensions)
        return self.groupby(dimensions, container_type=GridSpace, **kwargs)


    def layout(self, dimensions=None, **kwargs):
        """Group by supplied dimension(s) and lay out groups

        Groups data by supplied dimension(s) laying the groups along
        the dimension(s) out in a NdLayout.

        Parameters
        ----------
        dimensions
            Dimension(s) to group by

        Returns
        -------
        NdLayout with supplied dimensions
        """
        dimensions = self._valid_dimensions(dimensions)
        if len(dimensions) == self.ndims:
            with item_check(False):
                return NdLayout(self, **kwargs).reindex(dimensions)
        return self.groupby(dimensions, container_type=NdLayout, **kwargs)


    def options(self, *args, **kwargs):
        """Applies simplified option definition returning a new object

        Applies options defined in a flat format to the objects
        returned by the DynamicMap. If the options are to be set
        directly on the objects in the HoloMap a simple format may be
        used, e.g.:

            obj.options(cmap='viridis', show_title=False)

        If the object is nested the options must be qualified using
        a type[.group][.label] specification, e.g.:

            obj.options('Image', cmap='viridis', show_title=False)

        or using:

            obj.options({'Image': dict(cmap='viridis', show_title=False)})

        Parameters
        ----------
        *args
            Sets of options to apply to object
            Supports a number of formats including lists of Options
            objects, a type[.group][.label] followed by a set of
            keyword options to apply and a dictionary indexed by
            type[.group][.label] specs.
        backend : optional
            Backend to apply options to
            Defaults to current selected backend
        clone : bool, optional
            Whether to clone object
            Options can be applied inplace with clone=False
        **kwargs: Keywords of options
            Set of options to apply to the object

        Returns
        -------
        Returns the cloned object with the options applied
        """
        data = dict([(k, v.options(*args, **kwargs))
                             for k, v in self.data.items()])
        return self.clone(data)

    def _split_overlays(self):
        """Splits overlays inside the HoloMap into list of HoloMaps

        """
        if not issubclass(self.type, CompositeOverlay):
            return None, self.clone()

        item_maps = {}
        for k, overlay in self.data.items():
            for key, el in overlay.items():
                if key not in item_maps:
                    item_maps[key] = [(k, el)]
                else:
                    item_maps[key].append((k, el))

        maps, keys = [], []
        for k, layermap in item_maps.items():
            maps.append(self.clone(layermap))
            keys.append(k)
        return keys, maps

    def _dimension_keys(self):
        """Helper for __mul__ that returns the list of keys together with
        the dimension labels.

        """
        return [tuple(zip([d.name for d in self.kdims], [k] if self.ndims == 1 else k, strict=None))
                for k in self.keys()]

    def _dynamic_mul(self, dimensions, other, keys):
        """Implements dynamic version of overlaying operation overlaying
        DynamicMaps and HoloMaps where the key dimensions of one is
        a strict superset of the other.

        """
        # If either is a HoloMap compute Dimension values
        if not isinstance(self, DynamicMap) or not isinstance(other, DynamicMap):
            keys = sorted((d, v) for k in keys for d, v in k)
            grouped =  {g: [v for _, v in group]
                             for g, group in groupby(keys, lambda x: x[0])}
            dimensions = [d.clone(values=grouped[d.name]) for d in dimensions]
            map_obj = None

        # Combine streams
        map_obj = self if isinstance(self, DynamicMap) else other
        if isinstance(self, DynamicMap) and isinstance(other, DynamicMap):
            self_streams = util.dimensioned_streams(self)
            other_streams = util.dimensioned_streams(other)
            streams = list(util.unique_iterator(self_streams+other_streams))
        else:
            streams = map_obj.streams

        def dynamic_mul(*key, **kwargs):
            key_map = {d.name: k for d, k in zip(dimensions, key, strict=None)}
            layers = []
            try:
                self_el = self.select(HoloMap, **key_map) if self.kdims else self[()]
                layers.append(self_el)
            except KeyError:
                pass
            try:
                other_el = other.select(HoloMap, **key_map) if other.kdims else other[()]
                layers.append(other_el)
            except KeyError:
                pass
            return Overlay(layers)
        callback = Callable(dynamic_mul, inputs=[self, other])
        callback._is_overlay = True
        if map_obj:
            return map_obj.clone(callback=callback, shared_data=False,
                                 kdims=dimensions, streams=streams)
        else:
            return DynamicMap(callback=callback, kdims=dimensions,
                              streams=streams)



    def __mul__(self, other, reverse=False):
        """Overlays items in the object with another object

        The mul (*) operator implements overlaying of different
        objects.  This method tries to intelligently overlay mappings
        with differing keys. If the UniformNdMapping is mulled with a
        simple ViewableElement each element in the UniformNdMapping is
        overlaid with the ViewableElement. If the element the
        UniformNdMapping is mulled with is another UniformNdMapping it
        will try to match up the dimensions, making sure that items
        with completely different dimensions aren't overlaid.

        """
        if isinstance(other, HoloMap):
            self_set = {d.name for d in self.kdims}
            other_set = {d.name for d in other.kdims}

            # Determine which is the subset, to generate list of keys and
            # dimension labels for the new view
            self_in_other = self_set.issubset(other_set)
            other_in_self = other_set.issubset(self_set)
            dims = [other.kdims, self.kdims] if self_in_other else [self.kdims, other.kdims]
            dimensions = util.merge_dimensions(dims)

            if self_in_other and other_in_self: # superset of each other
                keys = self._dimension_keys() + other._dimension_keys()
                super_keys = util.unique_iterator(keys)
            elif self_in_other: # self is superset
                dimensions = other.kdims
                super_keys = other._dimension_keys()
            elif other_in_self: # self is superset
                super_keys = self._dimension_keys()
            else: # neither is superset
                raise Exception('One set of keys needs to be a strict subset of the other.')

            if isinstance(self, DynamicMap) or isinstance(other, DynamicMap):
                return self._dynamic_mul(dimensions, other, super_keys)

            items = []
            for dim_keys in super_keys:
                # Generate keys for both subset and superset and sort them by the dimension index.
                self_key = tuple(k for p, k in sorted(
                    [(self.get_dimension_index(dim), v) for dim, v in dim_keys
                     if dim in self.kdims]))
                other_key = tuple(k for p, k in sorted(
                    [(other.get_dimension_index(dim), v) for dim, v in dim_keys
                     if dim in other.kdims]))
                new_key = self_key if other_in_self else other_key
                # Append SheetOverlay of combined items
                if (self_key in self) and (other_key in other):
                    if reverse:
                        value = other[other_key] * self[self_key]
                    else:
                        value = self[self_key] * other[other_key]
                    items.append((new_key, value))
                elif self_key in self:
                    items.append((new_key, Overlay([self[self_key]])))
                else:
                    items.append((new_key, Overlay([other[other_key]])))
            return self.clone(items, kdims=dimensions, label=self._label, group=self._group)
        elif isinstance(other, self.data_type) and not isinstance(other, Layout):
            if isinstance(self, DynamicMap):
                def dynamic_mul(*args, **kwargs):
                    element = self[args]
                    if reverse:
                        return other * element
                    else:
                        return element * other
                callback = Callable(dynamic_mul, inputs=[self, other])
                callback._is_overlay = True
                return self.clone(shared_data=False, callback=callback,
                                  streams=util.dimensioned_streams(self))
            items = [(k, other * v) if reverse else (k, v * other)
                     for (k, v) in self.data.items()]
            return self.clone(items, label=self._label, group=self._group)
        else:
            return NotImplemented

    def __lshift__(self, other):
        """Adjoin another object to this one returning an AdjointLayout

        """
        if isinstance(other, (ViewableElement, UniformNdMapping, Empty)):
            return AdjointLayout([self, other])
        elif isinstance(other, AdjointLayout):
            return AdjointLayout([*other.data, self])
        else:
            raise TypeError(f'Cannot append {type(other).__name__} to a AdjointLayout')


    def collate(self, merge_type=None, drop=None, drop_constant=False):
        """Collate allows reordering nested containers

        Collation allows collapsing nested mapping types by merging
        their dimensions. In simple terms in merges nested containers
        into a single merged type.

        In the simple case a HoloMap containing other HoloMaps can
        easily be joined in this way. However collation is
        particularly useful when the objects being joined are deeply
        nested, e.g. you want to join multiple Layouts recorded at
        different times, collation will return one Layout containing
        HoloMaps indexed by Time. Changing the merge_type will allow
        merging the outer Dimension into any other UniformNdMapping
        type.

        Parameters
        ----------
        merge_type
            Type of the object to merge with
        drop
            List of dimensions to drop
        drop_constant
            Drop constant dimensions automatically

        Returns
        -------
        Collated Layout or HoloMap
        """
        if drop is None:
            drop = []
        from .element import Collator
        merge_type=merge_type if merge_type else self.__class__
        return Collator(self, merge_type=merge_type, drop=drop,
                        drop_constant=drop_constant)()

    def decollate(self):
        """Packs HoloMap of DynamicMaps into a single DynamicMap that returns an
        HoloMap

        Decollation allows packing a HoloMap of DynamicMaps into a single DynamicMap
        that returns an HoloMap of simple (non-dynamic) elements. All nested streams
        are lifted to the resulting DynamicMap, and are available in the `streams`
        property.  The `callback` property of the resulting DynamicMap is a pure,
        stateless function of the stream values. To avoid stream parameter name
        conflicts, the resulting DynamicMap is configured with
        positional_stream_args=True, and the callback function accepts stream values
        as positional dict arguments.

        Returns
        -------
        DynamicMap that returns an HoloMap
        """
        from .decollate import decollate
        return decollate(self)

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
        return super().relabel(label=label, group=group, depth=depth)

    def hist(self, dimension=None, num_bins=20, bin_range=None,
             adjoin=True, individually=True, **kwargs):
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
        AdjointLayout of HoloMap and histograms or just the
        histograms
        """
        if dimension is not None and not isinstance(dimension, list):
            dimension = [dimension]
        histmaps = [self.clone(shared_data=False) for _ in (dimension or [None])]

        if individually:
            map_range = None
        else:
            if dimension is None:
                raise Exception("Please supply the dimension to compute a histogram for.")
            map_range = self.range(kwargs['dimension'])

        bin_range = map_range if bin_range is None else bin_range
        style_prefix = 'Custom[<' + self.name + '>]_'
        if issubclass(self.type, (NdOverlay, Overlay)) and 'index' not in kwargs:
            kwargs['index'] = 0

        for k, v in self.data.items():
            hists = v.hist(adjoin=False, dimension=dimension,
                           bin_range=bin_range, num_bins=num_bins,
                           style_prefix=style_prefix, **kwargs)
            if isinstance(hists, Layout):
                for i, hist in enumerate(hists):
                    histmaps[i][k] = hist
            else:
                histmaps[0][k] = hists

        if adjoin:
            layout = self
            for hist in histmaps:
                layout = (layout << hist)
            if issubclass(self.type, (NdOverlay, Overlay)):
                layout.main_layer = kwargs['index']
            return layout
        elif len(histmaps) > 1:
            return Layout(histmaps)
        else:
            return histmaps[0]


class Callable(param.Parameterized):
    """Callable allows wrapping callbacks on one or more DynamicMaps
    allowing their inputs (and in future outputs) to be defined.
    This makes it possible to wrap DynamicMaps with streams and
    makes it possible to traverse the graph of operations applied
    to a DynamicMap.

    Additionally, if the memoize attribute is True, a Callable will
    memoize the last returned value based on the arguments to the
    function and the state of all streams on its inputs, to avoid
    calling the function unnecessarily. Note that because memoization
    includes the streams found on the inputs it may be disabled if the
    stream requires it and is triggering.

    A Callable may also specify a stream_mapping which specifies the
    objects that are associated with interactive (i.e. linked) streams
    when composite objects such as Layouts are returned from the
    callback. This is required for building interactive, linked
    visualizations (for the backends that support them) when returning
    Layouts, NdLayouts or GridSpace objects. When chaining multiple
    DynamicMaps into a pipeline, the link_inputs parameter declares
    whether the visualization generated using this Callable will
    inherit the linked streams. This parameter is used as a hint by
    the applicable backend.

    The mapping should map from an appropriate key to a list of
    streams associated with the selected object. The appropriate key
    may be a type[.group][.label] specification for Layouts, an
    integer index or a suitable NdLayout/GridSpace key. For more
    information see the DynamicMap tutorial at holoviews.org.

    """

    callable = param.Callable(default=None, constant=True, allow_refs=False, doc="""
         The callable function being wrapped.""")

    inputs = param.List(default=[], constant=True, doc="""
         The list of inputs the callable function is wrapping. Used
         to allow deep access to streams in chained Callables.""")

    operation_kwargs = param.Dict(default={}, constant=True, doc="""
        Potential dynamic keyword arguments associated with the
        operation.""")

    link_inputs = param.Boolean(default=True, doc="""
         If the Callable wraps around other DynamicMaps in its inputs,
         determines whether linked streams attached to the inputs are
         transferred to the objects returned by the Callable.

         For example the Callable wraps a DynamicMap with an RangeXY
         stream, this switch determines whether the corresponding
         visualization should update this stream with range changes
         originating from the newly generated axes.""")

    memoize = param.Boolean(default=True, doc="""
         Whether the return value of the callable should be memoized
         based on the call arguments and any streams attached to the
         inputs.""")

    operation = param.Callable(default=None, doc="""
         The function being applied by the Callable. May be used
         to record the transform(s) being applied inside the
         callback function.""")

    stream_mapping = param.Dict(default={}, constant=True, doc="""
         Defines how streams should be mapped to objects returned by
         the Callable, e.g. when it returns a Layout.""")

    def __init__(self, callable, **params):
        super().__init__(callable=callable,
                         **dict(params, name=util.callable_name(callable)))
        self._memoized = {}
        self._is_overlay = False
        self.args = None
        self.kwargs = None
        self._stream_memoization = self.memoize

    @property
    def argspec(self):
        return util.argspec(self.callable)

    @property
    def noargs(self):
        """Returns True if the callable takes no arguments

        """
        noargs = util.ArgSpec(args=[], varargs=None, keywords=None, defaults=None)
        return self.argspec == noargs


    def clone(self, callable=None, **overrides):
        """Clones the Callable optionally with new settings

        Parameters
        ----------
        callable
            New callable function to wrap
        **overrides
            Parameter overrides to apply

        Returns
        -------
        Cloned Callable object
        """
        old = {k: v for k, v in self.param.values().items()
               if k not in ['callable', 'name']}
        params = dict(old, **overrides)
        callable = self.callable if callable is None else callable
        return self.__class__(callable, **params)


    def __call__(self, *args, **kwargs):
        """Calls the callable function with supplied args and kwargs.

        If enabled uses memoization to avoid calling function
        unnecessarily.

        Parameters
        ----------
        *args
            Arguments passed to the callable function
        **kwargs
            Keyword arguments passed to the callable function

        Returns
        -------
        Return value of the wrapped callable function
        """
        # Nothing to do for callbacks that accept no arguments
        kwarg_hash = kwargs.pop('_memoization_hash_', ())
        (self.args, self.kwargs) = (args, kwargs)
        if isinstance(self.callable, param.rx):
            return self.callable.rx.value
        elif not args and not kwargs and not any(kwarg_hash):
            return self.callable()
        inputs = [i for i in self.inputs if isinstance(i, DynamicMap)]
        streams = []
        for stream in [s for i in inputs for s in get_nested_streams(i)]:
            if stream not in streams: streams.append(stream)

        memoize = self._stream_memoization and not any(s.transient and s._triggering for s in streams)
        values = tuple(tuple(sorted(s.hashkey.items())) for s in streams)
        key = args + kwarg_hash + values

        hashed_key = util.deephash(key) if self.memoize else None
        if hashed_key is not None and memoize and hashed_key in self._memoized:
            return self._memoized[hashed_key]

        if self.argspec.varargs is not None:
            # Missing information on positional argument names, cannot promote to keywords
            pass
        elif len(args) != 0: # Turn positional arguments into keyword arguments
            pos_kwargs = {k:v for k,v in zip(self.argspec.args, args, strict=None)}
            ignored = range(len(self.argspec.args),len(args))
            if ignored:
                self.param.warning('Ignoring extra positional argument {}'.format(', '.join(f'{i}' for i in ignored)))
            clashes = set(pos_kwargs.keys()) & set(kwargs.keys())
            if clashes:
                self.param.warning(
                    f'Positional arguments {list(clashes)!r} overridden by keywords')
            args, kwargs = (), dict(pos_kwargs, **kwargs)

        try:
            ret = self.callable(*args, **kwargs)
        except KeyError:
            # KeyError is caught separately because it is used to signal
            # invalid keys on DynamicMap and should not warn
            raise
        except Exception as e:
            posstr = ', '.join([f'{el!r}' for el in self.args]) if self.args else ''
            kwstr = ', '.join(f'{k}={v!r}' for k,v in self.kwargs.items())
            argstr = ', '.join([el for el in [posstr, kwstr] if el])
            message = ("Callable raised \"{e}\".\n"
                       "Invoked as {name}({argstr})")
            self.param.warning(message.format(name=self.name, argstr=argstr, e=repr(e)))
            raise

        if hashed_key is not None:
            self._memoized = {hashed_key : ret}
        return ret



class Generator(Callable):
    """Generators are considered a special case of Callable that accept no
    arguments and never memoize.

    """

    callable = param.ClassSelector(default=None, class_ = types.GeneratorType,
                                   constant=True, doc="""
         The generator that is wrapped by this Generator.""")

    @property
    def argspec(self):
        return util.ArgSpec(args=[], varargs=None, keywords=None, defaults=None)

    def __call__(self):
        try:
            return next(self.callable)
        except StopIteration:
            raise
        except Exception:
            msg = 'Generator {name} raised the following exception:'
            self.param.warning(msg.format(name=self.name))
            raise


def get_nested_dmaps(dmap):
    """Recurses DynamicMap to find DynamicMaps inputs

    Parameters
    ----------
    dmap
        DynamicMap to recurse to look for DynamicMap inputs

    Returns
    -------
    List of DynamicMap instances that were found
    """
    if not isinstance(dmap, DynamicMap):
        return []
    dmaps = [dmap]
    for o in dmap.callback.inputs:
        dmaps.extend(get_nested_dmaps(o))
    return list(set(dmaps))


def get_nested_streams(dmap):
    """Recurses supplied DynamicMap to find all streams

    Parameters
    ----------
    dmap
        DynamicMap to recurse to look for streams

    Returns
    -------
    List of streams that were found
    """
    return list({s for dmap in get_nested_dmaps(dmap) for s in dmap.streams})


@contextmanager
def dynamicmap_memoization(callable_obj, streams):
    """Determine whether the Callable should have memoization enabled
    based on the supplied streams (typically by a
    DynamicMap). Memoization is disabled if any of the streams require
    it it and are currently in a triggered state.

    """
    memoization_state = bool(callable_obj._stream_memoization)
    callable_obj._stream_memoization &= not any(s.transient and s._triggering for s in streams)
    try:
        yield
    finally:
        callable_obj._stream_memoization = memoization_state



class periodic:
    """Implements the utility of the same name on DynamicMap.

    Used to defined periodic event updates that can be started and
    stopped.

    """

    _periodic_util = util.periodic

    def __init__(self, dmap):
        self.dmap = dmap
        self.instance = None

    def __call__(self, period, count=None, param_fn=None, timeout=None, block=True):
        """Periodically trigger the streams on the DynamicMap.

        Run a non-blocking loop that updates the stream parameters using
        the event method. Runs count times with the specified period. If
        count is None, runs indefinitely.

        Parameters
        ----------
        period
            Timeout between events in seconds
        count
            Number of events to trigger
        param_fn
            Function returning stream updates given count
            Stream parameter values should be returned as dictionary
        timeout
            Overall timeout in seconds
        block
            Whether the periodic callbacks should be blocking
        """
        if self.instance is not None and not self.instance.completed:
            raise RuntimeError('Periodic process already running. '
                               'Wait until it completes or call '
                               'stop() before running a new periodic process')
        def inner(i):
            kwargs = {} if param_fn is None else param_fn(i)
            if kwargs:
                self.dmap.event(**kwargs)
            else:
                Stream.trigger(self.dmap.streams)

        instance = self._periodic_util(period, count, inner,
                                       timeout=timeout, block=block)
        instance.start()
        self.instance = instance

    def stop(self):
        """Stop the periodic process.

        """
        self.instance.stop()

    def __str__(self):
        return "<holoviews.core.spaces.periodic method>"



class DynamicMap(HoloMap):
    """A DynamicMap is a type of HoloMap where the elements are dynamically
    generated by a callable. The callable is invoked with values
    associated with the key dimensions or with values supplied by stream
    parameters.

    """

    # Declare that callback is a positional parameter (used in clone)
    __pos_params = ['callback']

    kdims = param.List(default=[], constant=True, doc="""
        The key dimensions of a DynamicMap map to the arguments of the
        callback. This mapping can be by position or by name.""")

    callback = param.ClassSelector(class_=Callable, constant=True, doc="""
        The callable used to generate the elements. The arguments to the
        callable includes any number of declared key dimensions as well
        as any number of stream parameters defined on the input streams.

        If the callable is an instance of Callable it will be used
        directly, otherwise it will be automatically wrapped in one.""")

    streams = param.List(default=[], constant=True, doc="""
       List of Stream instances to associate with the DynamicMap. The
       set of parameter values across these streams will be supplied as
       keyword arguments to the callback when the events are received,
       updating the streams. Can also be supplied as a dictionary that
       maps parameters or panel widgets to callback argument names that
       will then be automatically converted to the equivalent list
       format.""")

    cache_size = param.Integer(default=500, bounds=(1, None), doc="""
       The number of entries to cache for fast access. This is an LRU
       cache where the least recently used item is overwritten once
       the cache is full.""")

    positional_stream_args = param.Boolean(default=False, constant=True, doc="""
       If False, stream parameters are passed to the callback as keyword arguments.
       If True, stream parameters are passed to callback as positional arguments.
       Each positional argument is a dict containing the contents of a stream.
       The positional stream arguments follow the positional arguments for each kdim,
       and they are ordered to match the order of the DynamicMap's streams list.
    """)

    def __init__(self, callback, initial_items=None, streams=None, **params):
        streams = (streams or [])
        if isinstance(streams, dict):
            streams = streams_list_from_dict(streams)

        # If callback is a parameterized method and watch is disabled add as stream
        if param.parameterized.resolve_ref(callback):
            streams.append(callback)
        elif (params.get('watch', True) and (util.is_param_method(callback, has_deps=True) or
            (isinstance(callback, FunctionType) and hasattr(callback, '_dinfo')))):
            streams.append(callback)

        if isinstance(callback, types.GeneratorType):
            callback = Generator(callback)
        elif not isinstance(callback, Callable):
            callback = Callable(callback)

        valid, invalid = Stream._process_streams(streams)
        if invalid:
            msg = ('The supplied streams list contains objects that '
                   'are not Stream instances: {objs}')
            raise TypeError(msg.format(objs = ', '.join(f'{el!r}' for el in invalid)))

        super().__init__(initial_items, callback=callback, streams=valid, **params)

        if self.callback.noargs:
            prefix = 'DynamicMaps using generators (or callables without arguments)'
            if self.kdims:
                raise Exception(prefix + ' must be declared without key dimensions')
            if len(self.streams)> 1:
                raise Exception(prefix + ' must have either streams=[] or a single, '
                                + 'stream instance without any stream parameters')
            if self._stream_parameters() != []:
                raise Exception(prefix + ' cannot accept any stream parameters')

        if self.positional_stream_args:
            self._posarg_keys = None
        else:
            self._posarg_keys = util.validate_dynamic_argspec(
                self.callback, self.kdims, self.streams
            )

        # Set source to self if not already specified
        for stream in self.streams:
            if stream.source is None:
                stream.source = self
            if isinstance(stream, Params):
                for p in stream.parameters:
                    if isinstance(p.owner, Stream) and p.owner.source is None:
                        p.owner.source = self

        self.periodic = periodic(self)

        self._current_key = None

    @property
    def opts(self):
        return Opts(self, mode='dynamicmap')

    @property
    def redim(self):
        return Redim(self, mode='dynamic')

    @property
    def unbounded(self):
        """Returns a list of key dimensions that are unbounded, excluding
        stream parameters. If any of these key dimensions are
        unbounded, the DynamicMap as a whole is also unbounded.

        """
        unbounded_dims = []
        # Dimensioned streams do not need to be bounded
        stream_params = set(self._stream_parameters())
        for kdim in self.kdims:
            if str(kdim) in stream_params:
                continue
            if kdim.values:
                continue
            if None in kdim.range:
                unbounded_dims.append(str(kdim))
        return unbounded_dims

    @property
    def current_key(self):
        """Returns the current key value.

        """
        return self._current_key

    def _stream_parameters(self):
        return util.stream_parameters(
            self.streams, no_duplicates=not self.positional_stream_args
        )

    def _initial_key(self):
        """Construct an initial key for based on the lower range bounds or
        values on the key dimensions.

        """
        key = []
        undefined = []
        stream_params = set(self._stream_parameters())
        for kdim in self.kdims:
            if str(kdim) in stream_params:
                key.append(None)
            elif kdim.default is not None:
                key.append(kdim.default)
            elif kdim.values:
                if all(util.isnumeric(v) for v in kdim.values):
                    key.append(sorted(kdim.values)[0])
                else:
                    key.append(kdim.values[0])
            elif kdim.range[0] is not None:
                key.append(kdim.range[0])
            else:
                undefined.append(kdim)
        if undefined:
            msg = ('Dimension(s) {undefined_dims} do not specify range or values needed '
                   'to generate initial key')
            undefined_dims = ', '.join(f'{str(dim)!r}' for dim in undefined)
            raise KeyError(msg.format(undefined_dims=undefined_dims))

        return tuple(key)


    def _validate_key(self, key):
        """Make sure the supplied key values are within the bounds
        specified by the corresponding dimension range and soft_range.

        """
        if key == () and len(self.kdims) == 0: return ()
        key = util.wrap_tuple(key)
        assert len(key) == len(self.kdims)
        for ind, val in enumerate(key):
            kdim = self.kdims[ind]
            low, high = util.max_range([kdim.range, kdim.soft_range])
            if util.is_number(low) and util.isfinite(low):
                if val < low:
                    raise KeyError(f"Key value {val} below lower bound {low}")
            if util.is_number(high) and util.isfinite(high):
                if val > high:
                    raise KeyError(f"Key value {val} above upper bound {high}")

    def event(self, **kwargs):
        """Updates attached streams and triggers events

        Automatically find streams matching the supplied kwargs to
        update and trigger events on them.

        Parameters
        ----------
        **kwargs
            Events to update streams with
        """
        if self.callback.noargs and self.streams == []:
            self.param.warning(
                'No streams declared. To update a DynamicMaps using '
                'generators (or callables without arguments) use streams=[Next()]')
            return
        if self.streams == []:
            self.param.warning('No streams on DynamicMap, calling event '
                               'will have no effect')
            return

        stream_params = set(self._stream_parameters())
        invalid = [k for k in kwargs.keys() if k not in stream_params]
        if invalid:
            msg = 'Key(s) {invalid} do not correspond to stream parameters'
            raise KeyError(msg.format(invalid = ', '.join(f'{i!r}' for i in invalid)))

        streams = []
        for stream in self.streams:
            contents = stream.contents
            applicable_kws = {k:v for k,v in kwargs.items()
                              if k in set(contents.keys())}
            if not applicable_kws and contents:
                continue
            streams.append(stream)
            rkwargs = util.rename_stream_kwargs(stream, applicable_kws, reverse=True)
            stream.update(**rkwargs)

        Stream.trigger(streams)


    def _style(self, retval):
        """Applies custom option tree to values return by the callback.

        """
        from ..util import opts
        if self.id not in Store.custom_options():
            return retval
        spec = StoreOptions.tree_to_dict(Store.custom_options()[self.id])
        return opts.apply_groups(retval, options=spec)


    def _execute_callback(self, *args):
        """Executes the callback with the appropriate args and kwargs

        """
        self._validate_key(args)      # Validate input key

        # Additional validation needed to ensure kwargs don't clash
        kdims = [kdim.name for kdim in self.kdims]
        kwarg_items = [s.contents.items() for s in self.streams]
        hash_items = tuple(tuple(sorted(s.hashkey.items())) for s in self.streams)+args
        flattened = [(k,v) for kws in kwarg_items for (k,v) in kws
                     if k not in kdims]

        if self.positional_stream_args:
            kwargs = {}
            args = args + tuple([s.contents for s in self.streams])
        elif self._posarg_keys:
            kwargs = dict(flattened, **dict(zip(self._posarg_keys, args, strict=None)))
            args = ()
        else:
            kwargs = dict(flattened)
        if not isinstance(self.callback, Generator):
            kwargs['_memoization_hash_'] = hash_items

        with dynamicmap_memoization(self.callback, self.streams):
            retval = self.callback(*args, **kwargs)
        return self._style(retval)


    def options(self, *args, **kwargs):
        """Applies simplified option definition returning a new object.

        Applies options defined in a flat format to the objects
        returned by the DynamicMap. If the options are to be set
        directly on the objects returned by the DynamicMap a simple
        format may be used, e.g.:

            obj.options(cmap='viridis', show_title=False)

        If the object is nested the options must be qualified using
        a type[.group][.label] specification, e.g.:

            obj.options('Image', cmap='viridis', show_title=False)

        or using:

            obj.options({'Image': dict(cmap='viridis', show_title=False)})

        Parameters
        ----------
        *args
            Sets of options to apply to object
            Supports a number of formats including lists of Options
            objects, a type[.group][.label] followed by a set of
            keyword options to apply and a dictionary indexed by
            type[.group][.label] specs.
        backend : optional
            Backend to apply options to
            Defaults to current selected backend
        clone : bool, optional
            Whether to clone object
            Options can be applied inplace with clone=False
        **kwargs
            Keywords of options
            Set of options to apply to the object

        Returns
        -------
        Returns the cloned object with the options applied
        """
        if 'clone' not in kwargs:
            kwargs['clone'] = True
        return self.opts(*args, **kwargs)


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
        callback = overrides.pop('callback', self.callback)
        if data is None and shared_data:
            data = self.data
            if link and callback is self.callback:
                overrides['plot_id'] = self._plot_id
        clone = super(UniformNdMapping, self).clone(
            callback, shared_data, new_type, link,
            *(data, *args), **overrides)

        # Ensure the clone references this object to ensure
        # stream sources are inherited
        if clone.callback is self.callback:
            from ..operation import function
            with util.disable_constant(clone):
                op = function.instance(fn=lambda x, **kwargs: x)
                clone.callback = clone.callback.clone(
                    inputs=[self], link_inputs=link, operation=op,
                    operation_kwargs={}
                )
        return clone


    def reset(self):
        """Clear the DynamicMap cache

        """
        self.data = {}
        return self


    def _cross_product(self, tuple_key, cache, data_slice):
        """Returns a new DynamicMap if the key (tuple form) expresses a
        cross product, otherwise returns None. The cache argument is a
        dictionary (key:element pairs) of all the data found in the
        cache for this key.

        Each key inside the cross product is looked up in the cache
        (self.data) to check if the appropriate element is
        available. Otherwise the element is computed accordingly.

        The data_slice may specify slices into each value in the
        the cross-product.

        """
        if not any(isinstance(el, (list, set)) for el in tuple_key):
            return None
        if len(tuple_key)==1:
            product = tuple_key[0]
        else:
            args = [set(el) if isinstance(el, (list,set))
                    else {el} for el in tuple_key]
            product = itertools.product(*args)

        data = []
        for inner_key in product:
            key = util.wrap_tuple(inner_key)
            if key in cache:
                val = cache[key]
            else:
                val = self._execute_callback(*key)
            if data_slice:
                val = self._dataslice(val, data_slice)
            data.append((key, val))
        product = self.clone(data)

        if data_slice:
            from ..util import Dynamic
            dmap = Dynamic(self, operation=lambda obj, **dynkwargs: obj[data_slice],
                           streams=self.streams)
            dmap.data = product.data
            return dmap
        return product


    def _slice_bounded(self, tuple_key, data_slice):
        """Slices bounded DynamicMaps by setting the soft_ranges on
        key dimensions and applies data slice to cached and dynamic
        values.

        """
        slices = [el for el in tuple_key if isinstance(el, slice)]
        if any(el.step for el in slices):
            raise Exception("DynamicMap slices cannot have a step argument")
        elif len(slices) not in [0, len(tuple_key)]:
            raise Exception("Slices must be used exclusively or not at all")
        elif not slices:
            return None

        sliced = self.clone(self)
        for i, slc in enumerate(tuple_key):
            (start, stop) = slc.start, slc.stop
            if start is not None and start < sliced.kdims[i].range[0]:
                raise Exception("Requested slice below defined dimension range.")
            if stop is not None and stop > sliced.kdims[i].range[1]:
                raise Exception("Requested slice above defined dimension range.")
            sliced.kdims[i].soft_range = (start, stop)
        if data_slice:
            if not isinstance(sliced, DynamicMap):
                return self._dataslice(sliced, data_slice)
            else:
                from ..util import Dynamic
                if len(self):
                    slices = [slice(None) for _ in range(self.ndims)] + list(data_slice)
                    sliced = super(DynamicMap, sliced).__getitem__(tuple(slices))
                dmap = Dynamic(self, operation=lambda obj, **dynkwargs: obj[data_slice],
                               streams=self.streams)
                dmap.data = sliced.data
                return dmap
        return sliced


    def __getitem__(self, key):
        """Evaluates DynamicMap with specified key.

        Indexing into a DynamicMap evaluates the dynamic function with
        the specified key unless the key and corresponding value are
        already in the cache. This may also be used to evaluate
        multiple keys or even a cross-product of keys if a list of
        values per Dimension are defined. Once values are in the cache
        the DynamicMap can be cast to a HoloMap.

        Parameters
        ----------
        key
            n-dimensional key corresponding to the key dimensions
            Scalar values will be evaluated as normal while lists
            of values will be combined to form the cross-product,
            making it possible to evaluate many keys at once.

        Returns
        -------
        Returns evaluated callback return value for scalar key
        otherwise returns cloned DynamicMap containing the cross-
        product of evaluated items.
        """
        self._current_key = key

        # Split key dimensions and data slices
        sample = False
        if key is Ellipsis:
            return self
        elif isinstance(key, (list, set)) and all(isinstance(v, tuple) for v in key):
            map_slice, data_slice = key, ()
            sample = True
        elif self.positional_stream_args:
            # First positional args are dynamic map kdim indices, remaining args
            # are stream values, not data_slice values
            map_slice, _ = self._split_index(key)
            data_slice = ()
        else:
            map_slice, data_slice = self._split_index(key)
        tuple_key = util.wrap_tuple_streams(map_slice, self.kdims, self.streams)

        # Validation
        if not sample:
            sliced = self._slice_bounded(tuple_key, data_slice)
            if sliced is not None:
                return sliced

        # Cache lookup
        try:
            dimensionless = util.dimensionless_contents(get_nested_streams(self),
                                                        self.kdims, no_duplicates=False)
            empty = self._stream_parameters() == [] and self.kdims==[]
            if dimensionless or empty:
                raise KeyError('Using dimensionless streams disables DynamicMap cache')
            cache = super().__getitem__(key)
        except KeyError:
            cache = None

        # If the key expresses a cross product, compute the elements and return
        product = self._cross_product(tuple_key, cache.data if cache else {}, data_slice)
        if product is not None:
            return product

        # Not a cross product and nothing cached so compute element.
        if cache is not None: return cache
        val = self._execute_callback(*tuple_key)
        if data_slice:
            val = self._dataslice(val, data_slice)
        self._cache(tuple_key, val)
        return val


    def select(self, selection_specs=None, **kwargs):
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

        Parameters
        ----------
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
        if selection_specs is not None and not isinstance(selection_specs, (list, tuple)):
            selection_specs = [selection_specs]
        selection = super().select(selection_specs=selection_specs, **kwargs)
        def dynamic_select(obj, **dynkwargs):
            if selection_specs is not None:
                matches = any(obj.matches(spec) for spec in selection_specs)
            else:
                matches = True
            if matches:
                return obj.select(**kwargs)
            return obj

        if not isinstance(selection, DynamicMap):
            return dynamic_select(selection)
        else:
            from ..util import Dynamic
            dmap = Dynamic(self, operation=dynamic_select, streams=self.streams)
            dmap.data = selection.data
            return dmap


    def _cache(self, key, val):
        """Request that a key/value pair be considered for caching.

        """
        cache_size = (1 if util.dimensionless_contents(
            self.streams, self.kdims, no_duplicates=not self.positional_stream_args)
                      else self.cache_size)
        if len(self) >= cache_size:
            first_key = next(k for k in self.data)
            self.data.pop(first_key)
        self[key] = val


    def map(self, map_fn, specs=None, clone=True, link_inputs=True):
        """Map a function to all objects matching the specs

        Recursively replaces elements using a map function when the
        specs apply, by default applies to all objects, e.g. to apply
        the function to all contained Curve objects:

            dmap.map(fn, hv.Curve)

        Parameters
        ----------
        map_fn : Function to apply to each object
        specs : List of specs to match
            List of types, functions or type[.group][.label] specs
            to select objects to return, by default applies to all
            objects.
        clone : Whether to clone the object or transform inplace

        Returns
        -------
        Returns the object after the map_fn has been applied
        """
        deep_mapped = super().map(map_fn, specs, clone)
        if isinstance(deep_mapped, type(self)):
            from ..util import Dynamic
            def apply_map(obj, **dynkwargs):
                return obj.map(map_fn, specs, clone)
            dmap = Dynamic(self, operation=apply_map, streams=self.streams,
                           link_inputs=link_inputs)
            dmap.data = deep_mapped.data
            return dmap
        return deep_mapped


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
        relabelled = super().relabel(label, group, depth)
        if depth > 0:
            from ..util import Dynamic
            def dynamic_relabel(obj, **dynkwargs):
                return obj.relabel(group=group, label=label, depth=depth-1)
            dmap = Dynamic(self, streams=self.streams, operation=dynamic_relabel)
            dmap.data = relabelled.data
            with util.disable_constant(dmap):
                dmap.group = relabelled.group
                dmap.label = relabelled.label
            return dmap
        return relabelled

    def _split_overlays(self):
        """Splits a DynamicMap into its components. Only well defined for
        DynamicMap with consistent number and order of layers.

        """
        if not len(self):
            raise ValueError('Cannot split DynamicMap before it has been initialized')
        elif not issubclass(self.type, CompositeOverlay):
            return None, self

        from ..util import Dynamic
        keys = list(self.last.data.keys())
        dmaps = []
        for key in keys:
            el = self.last.data[key]
            def split_overlay_callback(obj, overlay_key=key, overlay_el=el, **kwargs):
                spec = util.get_overlay_spec(obj, overlay_key, overlay_el)
                items = list(obj.data.items())
                specs = [(i, util.get_overlay_spec(obj, k, v))
                         for i, (k, v) in enumerate(items)]
                match = util.closest_match(spec, specs)
                if match is None:
                    otype = type(obj).__name__
                    raise KeyError(f'{spec} spec not found in {otype}. The split_overlays method '
                                   'only works consistently for a DynamicMap where the '
                                   f'layers of the {otype} do not change.')
                return items[match][1]
            dmap = Dynamic(self, streams=self.streams, operation=split_overlay_callback)
            dmap.data = dict([(list(self.data.keys())[-1], self.last.data[key])])
            dmaps.append(dmap)
        return keys, dmaps

    def decollate(self):
        """Packs DynamicMap of nested DynamicMaps into a single DynamicMap that
        returns a non-dynamic element

        Decollation allows packing a DynamicMap of nested DynamicMaps into a single
        DynamicMap that returns a simple (non-dynamic) element. All nested streams are
        lifted to the resulting DynamicMap, and are available in the `streams`
        property.  The `callback` property of the resulting DynamicMap is a pure,
        stateless function of the stream values. To avoid stream parameter name
        conflicts, the resulting DynamicMap is configured with
        positional_stream_args=True, and the callback function accepts stream values
        as positional dict arguments.

        Returns
        -------
        DynamicMap that returns a non-dynamic element
        """
        from .decollate import decollate
        return decollate(self)

    def collate(self):
        """Unpacks DynamicMap into container of DynamicMaps

        Collation allows unpacking DynamicMaps which return Layout,
        NdLayout or GridSpace objects into a single such object
        containing DynamicMaps. Assumes that the items in the layout
        or grid that is returned do not change.

        Returns
        -------
        Collated container containing DynamicMaps
        """
        # Initialize
        if self.last is not None:
            initialized = self
        else:
            initialized = self.clone()
            initialized[initialized._initial_key()]

        if not isinstance(initialized.last, (Layout, NdLayout, GridSpace)):
            return self

        container = initialized.last.clone(shared_data=False)
        type_counter = defaultdict(int)

        # Get stream mapping from callback
        remapped_streams = []
        self_dstreams = util.dimensioned_streams(self)
        streams = self.callback.stream_mapping
        for i, (k, v) in enumerate(initialized.last.data.items()):
            vstreams = streams.get(i, [])
            if not vstreams:
                if isinstance(initialized.last, Layout):
                    for l in range(len(k)):
                        path = '.'.join(k[:l])
                        if path in streams:
                            vstreams = streams[path]
                            break
                else:
                    vstreams = streams.get(k, [])
            if any(s in remapped_streams for s in vstreams):
                raise ValueError(
                    "The stream_mapping supplied on the Callable "
                    "is ambiguous please supply more specific Layout "
                    "path specs.")
            remapped_streams += vstreams

            # Define collation callback
            def collation_cb(*args, **kwargs):
                layout = self[args]
                layout_type = type(layout).__name__
                if len(container.keys()) != len(layout.keys()):
                    raise ValueError('Collated DynamicMaps must return '
                                     f'{layout_type} with consistent number of items.')

                key = kwargs['selection_key']
                index = kwargs['selection_index']
                obj_type = kwargs['selection_type']
                dyn_type_map = defaultdict(list)
                for k, v in layout.data.items():
                    if k == key:
                        return layout[k]
                    dyn_type_map[type(v)].append(v)

                dyn_type_counter = {t: len(vals) for t, vals in dyn_type_map.items()}
                if dyn_type_counter != type_counter:
                    raise ValueError(f'The objects in a {layout_type} returned by a '
                                     'DynamicMap must consistently return '
                                     'the same number of items of the '
                                     'same type.')
                return dyn_type_map[obj_type][index]

            callback = Callable(partial(collation_cb, selection_key=k,
                                        selection_index=type_counter[type(v)],
                                        selection_type=type(v)),
                                inputs=[self])
            vstreams = list(util.unique_iterator(self_dstreams + vstreams))
            vdmap = self.clone(callback=callback, shared_data=False,
                               streams=vstreams)
            type_counter[type(v)] += 1

            # Remap source of streams
            for stream in vstreams:
                if stream.source is self:
                    stream.source = vdmap
            container[k] = vdmap

        unmapped_streams = [repr(stream) for stream in self.streams
                            if (stream.source is self) and
                            (stream not in remapped_streams)
                            and stream.linked]
        if unmapped_streams:
            raise ValueError(
                'The following streams are set to be automatically '
                'linked to a plot, but no stream_mapping specifying '
                'which item in the (Nd)Layout to link it to was found:\n{}'.format(', '.join(unmapped_streams))
            )
        return container


    def groupby(self, dimensions=None, container_type=None, group_type=None, **kwargs):
        """Groups DynamicMap by one or more dimensions

        Applies groupby operation over the specified dimensions
        returning an object of type container_type (expected to be
        dictionary-like) containing the groups.

        Parameters
        ----------
        dimensions : Dimension(s) to group by
        container_type : Type to cast group container to
        group_type : Type to cast each group to
        dynamic : Whether to return a DynamicMap
        **kwargs: Keyword arguments to pass to each group

        Returns
        -------
        Returns object of supplied container_type containing the
        groups. If dynamic=True returns a DynamicMap instead.
        """
        if dimensions is None:
            dimensions = self.kdims
        if not isinstance(dimensions, (list, tuple)):
            dimensions = [dimensions]

        container_type = container_type if container_type else type(self)
        group_type = group_type if group_type else type(self)

        outer_kdims = [self.get_dimension(d) for d in dimensions]
        inner_kdims = [d for d in self.kdims if d not in outer_kdims]

        outer_dynamic = issubclass(container_type, DynamicMap)
        inner_dynamic = issubclass(group_type, DynamicMap)

        if ((not outer_dynamic and any(not d.values for d in outer_kdims)) or
            (not inner_dynamic and any(not d.values for d in inner_kdims))):
            raise Exception('Dimensions must specify sampling via '
                            'values to apply a groupby')

        if outer_dynamic:
            def outer_fn(*outer_key, **dynkwargs):
                if inner_dynamic:
                    def inner_fn(*inner_key, **dynkwargs):
                        outer_vals = zip(outer_kdims, util.wrap_tuple(outer_key), strict=None)
                        inner_vals = zip(inner_kdims, util.wrap_tuple(inner_key), strict=None)
                        inner_sel = [(k.name, v) for k, v in inner_vals]
                        outer_sel = [(k.name, v) for k, v in outer_vals]
                        return self.select(**dict(inner_sel+outer_sel))
                    return self.clone([], callback=inner_fn, kdims=inner_kdims)
                else:
                    dim_vals = [(d.name, d.values) for d in inner_kdims]
                    dim_vals += [(d.name, [v]) for d, v in
                                   zip(outer_kdims, util.wrap_tuple(outer_key), strict=None)]
                    with item_check(False):
                        selected = HoloMap(self.select(**dict(dim_vals)))
                        return group_type(selected.reindex(inner_kdims))
            if outer_kdims:
                return self.clone([], callback=outer_fn, kdims=outer_kdims)
            else:
                return outer_fn(())
        else:
            outer_product = itertools.product(*[self.get_dimension(d).values
                                                for d in dimensions])
            groups = []
            for outer in outer_product:
                outer_vals = [(d.name, [o]) for d, o in zip(outer_kdims, outer, strict=None)]
                if inner_dynamic or not inner_kdims:
                    def inner_fn(outer_vals, *key, **dynkwargs):
                        inner_dims = zip(inner_kdims, util.wrap_tuple(key), strict=None)
                        inner_vals = [(d.name, k) for d, k in inner_dims]
                        return self.select(**dict(outer_vals+inner_vals)).last
                    if inner_kdims or self.streams:
                        callback = Callable(partial(inner_fn, outer_vals),
                                            inputs=[self])
                        group = self.clone(
                            callback=callback, kdims=inner_kdims
                        )
                    else:
                        group = inner_fn(outer_vals, ())
                    groups.append((outer, group))
                else:
                    inner_vals = [(d.name, self.get_dimension(d).values)
                                     for d in inner_kdims]
                    with item_check(False):
                        selected = HoloMap(self.select(**dict(outer_vals+inner_vals)))
                        group = group_type(selected.reindex(inner_kdims))
                    groups.append((outer, group))
            return container_type(groups, kdims=outer_kdims)


    def grid(self, dimensions=None, **kwargs):
        """Groups data by supplied dimension(s) laying the groups along
        the dimension(s) out in a GridSpace.

        Parameters
        ----------
        dimensions : Dimension/str or list
            Dimension or list of dimensions to group by

        Returns
        -------
        grid : GridSpace
            GridSpace with supplied dimensions
        """
        return self.groupby(dimensions, container_type=GridSpace, **kwargs)


    def layout(self, dimensions=None, **kwargs):
        """Groups data by supplied dimension(s) laying the groups along
        the dimension(s) out in a NdLayout.

        Parameters
        ----------
        dimensions : Dimension/str or list
            Dimension or list of dimensions to group by

        Returns
        -------
        layout : NdLayout
            NdLayout with supplied dimensions
        """
        return self.groupby(dimensions, container_type=NdLayout, **kwargs)


    def overlay(self, dimensions=None, **kwargs):
        """Group by supplied dimension(s) and overlay each group

        Groups data by supplied dimension(s) overlaying the groups
        along the dimension(s).

        Parameters
        ----------
        dimensions : Dimension(s) of dimensions to group by

        Returns
        -------
        NdOverlay object(s) with supplied dimensions
        """
        if dimensions is None:
            dimensions = self.kdims
        else:
            if not isinstance(dimensions, (list, tuple)):
                dimensions = [dimensions]
            dimensions = [self.get_dimension(d, strict=True)
                          for d in dimensions]
        dims = [d for d in self.kdims if d not in dimensions]
        return self.groupby(dims, group_type=NdOverlay)


    def hist(self, dimension=None, num_bins=20, bin_range=None,
             adjoin=True, **kwargs):
        """Computes and adjoins histogram along specified dimension(s).

        Defaults to first value dimension if present otherwise falls
        back to first key dimension.

        Parameters
        ----------
        dimension : Dimension(s) to compute histogram on
        num_bins : int, optional
            Number of bins
        bin_range : tuple, optional
            Lower and upper bounds of bins
        adjoin : bool, optional
            Whether to adjoin histogram

        Returns
        -------
        AdjointLayout of DynamicMap and adjoined histogram if
        adjoin=True, otherwise just the histogram
        """
        def dynamic_hist(obj, **dynkwargs):
            if isinstance(obj, (NdOverlay, Overlay)):
                index = kwargs.get('index', 0)
                obj = obj.get(index)
            return obj.hist(
                dimension=dimension,
                num_bins=num_bins,
                bin_range=bin_range,
                adjoin=False,
                **kwargs
            )

        from ..util import Dynamic
        hist = Dynamic(self, streams=self.streams, link_inputs=False,
                       operation=dynamic_hist)
        if adjoin:
            return self << hist
        else:
            return hist


    def reindex(self, kdims=None, force=False):
        """Reorders key dimensions on DynamicMap

        Create a new object with a reordered set of key dimensions.
        Dropping dimensions is not allowed on a DynamicMap.

        Parameters
        ----------
        kdims : List of dimensions to reindex the mapping with
        force : Not applicable to a DynamicMap

        Returns
        -------
        Reindexed DynamicMap
        """
        if kdims is None:
            kdims = []
        if not isinstance(kdims, list):
            kdims = [kdims]
        kdims = [self.get_dimension(kd, strict=True) for kd in kdims]
        dropped = [kd for kd in self.kdims if kd not in kdims]
        if dropped:
            raise ValueError("DynamicMap does not allow dropping dimensions, "
                             "reindex may only be used to reorder dimensions.")
        return super().reindex(kdims, force)


    def drop_dimension(self, dimensions):
        raise NotImplementedError('Cannot drop dimensions from a DynamicMap, '
                                  'cast to a HoloMap first.')

    def add_dimension(self, dimension, dim_pos, dim_val, vdim=False, **kwargs):
        raise NotImplementedError('Cannot add dimensions to a DynamicMap, '
                                  'cast to a HoloMap first.')

    def __next__(self):
        if self.callback.noargs:
            return self[()]
        else:
            raise Exception('The next method can only be used for DynamicMaps using'
                            'generators (or callables without arguments)')


class GridSpace(Layoutable, UniformNdMapping):
    """Grids are distinct from Layouts as they ensure all contained
    elements to be of the same type. Unlike Layouts, which have
    integer keys, Grids usually have floating point keys, which
    correspond to a grid sampling in some two-dimensional space. This
    two-dimensional space may have to arbitrary dimensions, e.g. for
    2D parameter spaces.

    """

    kdims = param.List(default=[Dimension("X"), Dimension("Y")], bounds=(1,2))

    def __init__(self, initial_items=None, kdims=None, **params):
        super().__init__(initial_items, kdims=kdims, **params)
        if self.ndims > 2:
            raise Exception('Grids can have no more than two dimensions.')


    def __lshift__(self, other):
        """Adjoins another object to the GridSpace

        """
        if isinstance(other, (ViewableElement, UniformNdMapping)):
            return AdjointLayout([self, other])
        elif isinstance(other, AdjointLayout):
            return AdjointLayout([*other.data, self])
        else:
            raise TypeError(f'Cannot append {type(other).__name__} to a AdjointLayout')


    def _transform_indices(self, key):
        """Snaps indices into the GridSpace to the closest coordinate.

        Parameters
        ----------
        key
            Tuple index into the GridSpace

        Returns
        -------
        Transformed key snapped to closest numeric coordinates
        """
        ndims = self.ndims
        if all(not (isinstance(el, slice) or callable(el)) for el in key):
            dim_inds = []
            for dim in self.kdims:
                dim_type = self.get_dimension_type(dim)
                if isinstance(dim_type, type) and issubclass(dim_type, Number):
                    dim_inds.append(self.get_dimension_index(dim))
            str_keys = iter(key[i] for i in range(self.ndims)
                            if i not in dim_inds)
            num_keys = []
            if dim_inds:
                keys = list({tuple(k[i] if ndims > 1 else k for i in dim_inds)
                             for k in self.keys()})
                q = np.array([tuple(key[i] if ndims > 1 else key for i in dim_inds)])
                idx = np.argmin([np.inner(q - np.array(x), q - np.array(x))
                                 if len(dim_inds) == 2 else np.abs(q-x)
                                     for x in keys])
                num_keys = iter(keys[idx])
            key = tuple(next(num_keys) if i in dim_inds else next(str_keys)
                        for i in range(self.ndims))
        elif any(not (isinstance(el, slice) or callable(el)) for el in key):
            keys = self.keys()
            for i, k in enumerate(key):
                if isinstance(k, slice):
                    continue
                dim_keys = np.array([ke[i] for ke in keys])
                if dtype_kind(dim_keys) in 'OSU':
                    continue
                snapped_val = dim_keys[np.argmin(np.abs(dim_keys-k))]
                key = list(key)
                key[i] = snapped_val
            key = tuple(key)
        return key


    def keys(self, full_grid=False):
        """Returns the keys of the GridSpace

        Parameters
        ----------
        full_grid : bool, optional
            Return full cross-product of keys

        Returns
        -------
        List of keys
        """
        keys = super().keys()
        if self.ndims == 1 or not full_grid:
            return keys
        dim1_keys = list(dict.fromkeys(k[0] for k in keys))
        dim2_keys = list(dict.fromkeys(k[1] for k in keys))
        return [(d1, d2) for d1 in dim1_keys for d2 in dim2_keys]


    @property
    def last(self):
        """The last of a GridSpace is another GridSpace
        constituted of the last of the individual elements. To access
        the elements by their X,Y position, either index the position
        directly or use the items() method.

        """
        if self.type == HoloMap:
            last_items = [(k, v.last if isinstance(v, HoloMap) else v)
                          for (k, v) in self.data.items()]
        else:
            last_items = self.data
        return self.clone(last_items)


    def __len__(self):
        """The maximum depth of all the elements. Matches the semantics
        of __len__ used by Maps. For the total number of elements,
        count the full set of keys.

        """
        return max([(len(v) if hasattr(v, '__len__') else 1) for v in self.values()] + [0])

    @property
    def shape(self):
        """Returns the 2D shape of the GridSpace as (rows, cols).

        """
        keys = self.keys()
        if self.ndims == 1:
            return (len(keys), 1)
        return len({k[0] for k in keys}), len({k[1] for k in keys})

    def decollate(self):
        """Packs GridSpace of DynamicMaps into a single DynamicMap that returns a
        GridSpace

        Decollation allows packing a GridSpace of DynamicMaps into a single DynamicMap
        that returns a GridSpace of simple (non-dynamic) elements. All nested streams
        are lifted to the resulting DynamicMap, and are available in the `streams`
        property.  The `callback` property of the resulting DynamicMap is a pure,
        stateless function of the stream values. To avoid stream parameter name
        conflicts, the resulting DynamicMap is configured with
        positional_stream_args=True, and the callback function accepts stream values
        as positional dict arguments.

        Returns
        -------
        DynamicMap that returns a GridSpace
        """
        from .decollate import decollate
        return decollate(self)


class GridMatrix(GridSpace):
    """GridMatrix is container type for heterogeneous Element types
    laid out in a grid. Unlike a GridSpace the axes of the Grid
    must not represent an actual coordinate space, but may be used
    to plot various dimensions against each other. The GridMatrix
    is usually constructed using the gridmatrix operation, which
    will generate a GridMatrix plotting each dimension in an
    Element against each other.

    """

    def _item_check(self, dim_vals, data):
        if not traversal.uniform(NdMapping([(0, self), (1, data)])):
            raise ValueError(f"HoloMaps dimensions must be consistent in {type(self).__name__}.")
        NdMapping._item_check(self, dim_vals, data)
