"""The streams module defines the streams API that allows visualizations to
generate and respond to events, originating either in Python on the
server-side or in Javascript in the Jupyter notebook (client-side).

"""

import weakref
from collections import defaultdict
from contextlib import contextmanager
from functools import partial
from itertools import groupby
from numbers import Number
from types import FunctionType
from typing import TYPE_CHECKING

import numpy as np
import param

from .core import util
from .core.ndmapping import UniformNdMapping
from .util.warnings import deprecated

if TYPE_CHECKING:
    import pandas as pd
else:
    pd = util.dependencies._LazyModule("pandas", bool_use_sys_modules=True)


# Types supported by Pointer derived streams
@util.types.gen_types
def pointer_types():
    yield from (Number, str, tuple)
    yield from util.datetime_types


POPUP_POSITIONS = [
    "top_right",
    "top_left",
    "bottom_left",
    "bottom_right",
    "right",
    "left",
    "top",
    "bottom",
]

class _SkipTrigger: pass


@contextmanager
def triggering_streams(streams):
    """Temporarily declares the streams as being in a triggered state.
    Needed by DynamicMap to determine whether to memoize on a Callable,
    i.e. if a stream has memoization disabled and is in triggered state
    Callable should disable lookup in the memoization cache. This is
    done by the dynamicmap_memoization context manager.

    """
    for stream in streams:
        stream._triggering = True
    try:
        yield
    finally:
        for stream in streams:
            stream._triggering = False


def streams_list_from_dict(streams):
    """Converts a streams dictionary into a streams list

    """
    params, refs = {}, {}
    for k, v in streams.items():
        v = param.parameterized.transform_reference(v)
        if isinstance(v, param.Parameter) and v.owner is not None:
            params[k] = v
            continue
        deps = param.parameterized.resolve_ref(v, recursive=True)
        if deps:
            refs[k] = v
        else:
            raise TypeError(f'Cannot handle {k!r} value {v!r} in streams dictionary')
    streams = Params.from_params(params)
    if not refs:
        return streams
    return [*streams, ParamRefs(refs=refs, recursive=True)]


class Stream(param.Parameterized):
    """A Stream is simply a parameterized object with parameters that
    change over time in response to update events and may trigger
    downstream events on its subscribers. The Stream parameters can be
    updated using the update method, which will optionally trigger the
    stream. This will notify the subscribers which may be supplied as
    a list of callables or added later using the add_subscriber
    method. The subscribers will be passed a dictionary mapping of the
    parameters of the stream, which are available on the instance as
    the ``contents``.

    Depending on the plotting backend certain streams may
    interactively subscribe to events and changes by the plotting
    backend. For this purpose use the LinkedStream baseclass, which
    enables the linked option by default. A source for the linking may
    be supplied to the constructor in the form of another viewable
    object specifying which part of a plot the data should come from.

    The transient option allows treating stream events as discrete
    updates, resetting the parameters to their default after the
    stream has been triggered. A downstream callback can therefore
    determine whether a stream is active by checking whether the
    stream values match the default (usually None).

    The Stream class is meant for subclassing and subclasses should
    generally add one or more parameters but may also override the
    transform and reset method to preprocess parameters before they
    are passed to subscribers and reset them using custom logic
    respectively.

    """

    # Mapping from a source to a list of streams
    # WeakKeyDictionary to allow garbage collection
    # of unreferenced sources
    registry = weakref.WeakKeyDictionary()

    # Mapping to define callbacks by backend and Stream type.
    # e.g. Stream._callbacks['bokeh'][Stream] = Callback
    _callbacks = defaultdict(dict)


    @classmethod
    def define(cls, name, **kwargs):
        """Utility to quickly and easily declare Stream classes. Designed
        for interactive use such as notebooks and shouldn't replace
        parameterized class definitions in source code that is imported.

        Takes a stream class name and a set of keywords where each
        keyword becomes a parameter. If the value is already a
        parameter, it is simply used otherwise the appropriate parameter
        type is inferred and declared, using the value as the default.

        Supported types: bool, int, float, str, dict, tuple and list

        """
        params = {'name': param.String(default=name)}
        for k, v in kwargs.items():
            kws = dict(default=v, constant=True)
            if isinstance(v, param.Parameter):
                params[k] = v
            elif isinstance(v, bool):
                params[k] = param.Boolean(**kws)
            elif isinstance(v, int):
                params[k] = param.Integer(**kws)
            elif isinstance(v, float):
                params[k] = param.Number(**kws)
            elif isinstance(v, str):
                params[k] = param.String(**kws)
            elif isinstance(v, dict):
                params[k] = param.Dict(**kws)
            elif isinstance(v, tuple):
                params[k] = param.Tuple(**kws)
            elif isinstance(v, list):
                params[k] = param.List(**kws)
            elif isinstance(v, np.ndarray):
                params[k] = param.Array(**kws)
            else:
                params[k] = param.Parameter(**kws)

        # Dynamic class creation using type
        return type(name, (Stream,), params)


    @classmethod
    def trigger(cls, streams):
        """Given a list of streams, collect all the stream parameters into
        a dictionary and pass it to the union set of subscribers.

        Passing multiple streams at once to trigger can be useful when a
        subscriber may be set multiple times across streams but only
        needs to be called once.

        """
        # Union of stream contents
        items = [stream.contents.items() for stream in set(streams)]
        union = [kv for kvs in items for kv in kvs]
        klist = [k for k, _ in union]
        key_clashes = []
        for k, v in union:
            key_count = klist.count(k)
            try:
                value_count = union.count((k, v))
            except Exception:
                # If we can't compare values we assume they are not equal
                value_count = 1
            if key_count > 1 and key_count > value_count and k not in key_clashes:
                key_clashes.append(k)
        if key_clashes:
            print(f'Parameter name clashes for keys {key_clashes!r}')

        # Group subscribers by precedence while keeping the ordering
        # within each group
        subscriber_precedence = defaultdict(list)
        for stream in streams:
            stream._on_trigger()
            for precedence, subscriber in stream._subscribers:
                subscriber_precedence[precedence].append(subscriber)
        sorted_subscribers = sorted(subscriber_precedence.items(), key=lambda x: x[0])
        subscribers = util.unique_iterator([s for _, subscribers in sorted_subscribers
                                            for s in subscribers])

        with triggering_streams(streams):
            for subscriber in subscribers:
                subscriber(**dict(union))

        for stream in streams:
            with util.disable_constant(stream):
                if stream.transient:
                    stream.reset()


    def _on_trigger(self):
        """Called when a stream has been triggered

        """


    @classmethod
    def _process_streams(cls, streams):
        """Processes a list of streams promoting Parameterized objects and
        methods to Param based streams.

        """
        parameterizeds = defaultdict(set)
        valid, invalid = [], []
        for s in streams:
            if isinstance(s, partial):
                s = s.func
            if isinstance(s, Stream):
                pass
            elif isinstance(s, param.Parameter):
                s = Params(s.owner, [s.name])
            elif isinstance(s, param.Parameterized):
                s = Params(s)
            elif util.is_param_method(s):
                if not hasattr(s, "_dinfo"):
                    continue
                s = ParamMethod(s)
            elif isinstance(s, FunctionType) and hasattr(s, "_dinfo"):
                deps = s._dinfo
                dep_params = list(deps['dependencies']) + list(deps.get('kw', {}).values())
                rename = {(p.owner, p.name): k for k, p in deps.get('kw', {}).items()}
                s = Params(parameters=dep_params, rename=rename)
            else:
                deps = param.parameterized.resolve_ref(s)
                if deps:
                    s = Params(parameters=deps)
                else:
                    invalid.append(s)
                    continue
            if isinstance(s, Params):
                pid = id(s.parameterized)
                overlap = (set(s.parameters) & parameterizeds[pid])
                if overlap:
                    pname = type(s.parameterized).__name__
                    param.main.param.warning(
                        f'The {sorted([p.name for p in overlap])} parameter(s) '
                        f'on the {pname} object have '
                        'already been supplied in another stream. '
                        'Ensure that the supplied streams only specify '
                        'each parameter once, otherwise multiple '
                        'events will be triggered when the parameter changes.'
                    )
                parameterizeds[pid] |= set(s.parameters)
            valid.append(s)
        return valid, invalid


    def __init__(self, rename=None, source=None, subscribers=None, linked=False,
                 transient=False, **params):
        """The rename argument allows multiple streams with similar event
        state to be used by remapping parameter names.

        Source is an optional argument specifying the HoloViews
        datastructure that the stream receives events from, as supported
        by the plotting backend.

        Some streams are configured to automatically link to the source
        plot, to disable this set linked=False

        """
        # Source is stored as a weakref to allow it to be garbage collected
        if subscribers is None:
            subscribers = []
        if rename is None:
            rename = {}
        self._source = None if source is None else weakref.ref(source)

        self._subscribers = []
        for subscriber in subscribers:
            self.add_subscriber(subscriber)

        self.linked = linked
        self.transient = transient

        # Whether this stream is currently triggering its subscribers
        self._triggering = False

        # The metadata may provide information about the currently
        # active event, i.e. the source of the stream values may
        # indicate where the event originated from
        self._metadata = {}

        super().__init__(**params)
        self._rename = self._validate_rename(rename)
        if source is not None:
            if source in self.registry:
                self.registry[source].append(self)
            else:
                self.registry[source] = [self]

    def clone(self):
        """Return new stream with identical properties and no subscribers

        """
        return type(self)(**self.contents)

    @property
    def subscribers(self):
        """Property returning the subscriber list

        """
        return [s for p, s in sorted(self._subscribers, key=lambda x: x[0])]


    def clear(self, policy='all'):
        """Clear all subscribers registered to this stream.

        The default policy of 'all' clears all subscribers. If policy is
        set to 'user', only subscribers defined by the user are cleared
        (precedence between zero and one). A policy of 'internal' clears
        subscribers with precedence greater than unity used internally
        by HoloViews.

        """
        policies = ['all', 'user', 'internal']
        if policy not in policies:
            raise ValueError(f'Policy for clearing subscribers must be one of {policies}')
        if policy == 'all':
            remaining = []
        elif policy == 'user':
            remaining = [(p, s) for (p, s) in self._subscribers if p > 1]
        else:
            remaining = [(p, s) for (p, s) in self._subscribers if p <= 1]
        self._subscribers = remaining


    def reset(self):
        """Resets stream parameters to their defaults.

        """
        with util.disable_constant(self):
            for k, p in self.param.objects('existing').items():
                if k != 'name':
                    setattr(self, k, p.default)


    def add_subscriber(self, subscriber, precedence=0):
        """Register a callable subscriber to this stream which will be
        invoked either when event is called or when this stream is
        passed to the trigger classmethod.

        Precedence allows the subscriber ordering to be
        controlled. Users should only add subscribers with precedence
        between zero and one while HoloViews itself reserves the use of
        higher precedence values. Subscribers with high precedence are
        invoked later than ones with low precedence.

        """
        if not callable(subscriber):
            raise TypeError('Subscriber must be a callable.')
        self._subscribers.append((precedence, subscriber))


    def _validate_rename(self, mapping):
        param_names = [k for k in self.param if k != 'name']
        for k, v in mapping.items():
            if k not in param_names:
                raise KeyError(f'Cannot rename {k!r} as it is not a stream parameter')
            if k != v and v in param_names:
                raise KeyError(f'Cannot rename to {v!r} as it clashes with a '
                               'stream parameter of the same name')
        return mapping


    def rename(self, **mapping):
        """The rename method allows stream parameters to be allocated to
        new names to avoid clashes with other stream parameters of the
        same name. Returns a new clone of the stream instance with the
        specified name mapping.

        """
        params = {k: v for k, v in self.param.values().items() if k != 'name'}
        return self.__class__(rename=mapping,
                              source=(self._source() if self._source else None),
                              linked=self.linked, **params)

    @property
    def source(self):
        return self._source() if self._source else None


    @source.setter
    def source(self, source):
        if self.source is not None:
            source_list = self.registry[self.source]
            if self in source_list:
                source_list.remove(self)
            if not source_list:
                self.registry.pop(self.source)

        if source is None:
            self._source = None
            return

        self._source = weakref.ref(source)
        if source in self.registry:
            self.registry[source].append(self)
        else:
            self.registry[source] = [self]


    def transform(self):
        """Method that can be overwritten by subclasses to process the
        parameter values before renaming is applied. Returns a
        dictionary of transformed parameters.

        """
        return {}


    @property
    def contents(self):
        filtered = {k: v for k, v in self.param.values().items() if k != 'name'}
        return {self._rename.get(k, k): v for (k, v) in filtered.items()
                if self._rename.get(k, True) is not None}

    @property
    def hashkey(self):
        """The object the memoization hash is computed from. By default
        returns the stream contents but can be overridden to provide
        a custom hash key.

        """
        return self.contents


    def _set_stream_parameters(self, **kwargs):
        """Sets the stream parameters which are expected to be declared
        constant.

        """
        with util.disable_constant(self):
            self.param.update(**kwargs)

    def event(self, **kwargs):
        """Update the stream parameters and trigger an event.

        """
        skip = self.update(**kwargs)
        if skip is not _SkipTrigger:
            self.trigger([self])

    def update(self, **kwargs):
        """The update method updates the stream parameters (without any
        renaming applied) in response to some event. If the stream has a
        custom transform method, this is applied to transform the
        parameter values accordingly.

        To update and trigger, use the event method.

        """
        self._set_stream_parameters(**kwargs)
        transformed = self.transform()
        if transformed is None:
            return _SkipTrigger
        self._set_stream_parameters(**transformed)

    def __repr__(self):
        cls_name = self.__class__.__name__
        kwargs = ','.join(f'{k}={v!r}'
                          for (k, v) in self.param.values().items() if k != 'name')
        if not self._rename:
            return f'{cls_name}({kwargs})'
        else:
            return f'{cls_name}({self._rename!r}, {kwargs})'


    def __str__(self):
        return repr(self)


class Counter(Stream):
    """Simple stream that automatically increments an integer counter
    parameter every time it is updated.

    """

    counter = param.Integer(default=0, constant=True, bounds=(0, None))

    def transform(self):
        return {'counter': self.counter + 1}


class Pipe(Stream):
    """A Stream used to pipe arbitrary data to a callback.
    Unlike other streams memoization can be disabled for a
    Pipe stream (and is disabled by default).

    """

    data = param.Parameter(default=None, constant=True, doc="""
        Arbitrary data being streamed to a DynamicMap callback.""")

    def __init__(self, data=None, memoize=False, **params):
        super().__init__(data=data, **params)
        self._memoize_counter = 0

    def send(self, data):
        """A convenience method to send an event with data without
        supplying a keyword.

        """
        self.event(data=data)

    def _on_trigger(self):
        self._memoize_counter += 1

    @property
    def hashkey(self):
        return {'_memoize_key': self._memoize_counter}


class Buffer(Pipe):
    """Buffer allows streaming and accumulating incoming chunks of rows
    from tabular datasets. The data may be in the form of a pandas
    DataFrame, 2D arrays of rows and columns or dictionaries of column
    arrays. Buffer will accumulate the last N rows, where N is defined
    by the specified ``length``. The accumulated data is then made
    available via the ``data`` parameter.

    When streaming a DataFrame will reset the DataFrame index by
    default making it available to HoloViews elements as dimensions,
    this may be disabled by setting index=False.

    The ``following`` argument determines whether any plot which is
    subscribed to this stream will update the axis ranges when an
    update is pushed. This makes it possible to control whether zooming
    is allowed while streaming.

    """

    data = param.Parameter(default=None, constant=True, doc="""
        Arbitrary data being streamed to a DynamicMap callback.""")

    def __init__(self, data, length=1000, index=True, following=True, **params):
        if pd and isinstance(data, pd.DataFrame):
            example = data
        elif isinstance(data, np.ndarray):
            if data.ndim != 2:
                raise ValueError("Only 2D array data may be streamed by Buffer.")
            example = data
        elif isinstance(data, dict):
            if not all(isinstance(v, np.ndarray) for v in data.values()):
                raise ValueError("Data in dictionary must be of array types.")
            elif len({len(v) for v in data.values()}) > 1:
                raise ValueError("Columns in dictionary must all be the same length.")
            example = data
        else:
            try:
                from streamz.dataframe import StreamingDataFrame, StreamingSeries
                loaded = True
            except ImportError:
                try:
                    from streamz.dataframe import (
                        DataFrame as StreamingDataFrame,
                        Series as StreamingSeries,
                    )
                    loaded = True
                except ImportError:
                    loaded = False
            if loaded:
                # NOTE: there could still be some code in these classes which handles
                # the streaming interface.
                deprecated("1.23.0", "Buffer's streamz interface")
            if not loaded or not isinstance(data, (StreamingDataFrame, StreamingSeries)):
                raise ValueError("Buffer must be initialized with pandas DataFrame, "
                                 "streamz.StreamingDataFrame or streamz.StreamingSeries.")
            elif isinstance(data, StreamingSeries):
                data = data.to_frame()
            example = data.example
            data.stream.sink(self.send)
            self.sdf = data

        params['data'] = example
        super().__init__(**params)
        self.length = length
        self.following = following
        self._chunk_length = 0
        self._count = 0
        self._index = index


    def verify(self, x):
        """Verify consistency of dataframes that pass through this stream

        """
        if type(x) != type(self.data):  # noqa: E721
            raise TypeError(f"Input expected to be of type {type(self.data).__name__}, got {type(x).__name__}.")
        elif isinstance(x, np.ndarray):
            if x.ndim != 2:
                raise ValueError('Streamed array data must be two-dimensional')
            elif x.shape[1] != self.data.shape[1]:
                raise ValueError(f"Streamed array data expected to have {self.data.shape[1]} columns, "
                                 f"got {x.shape[1]}.")
        elif pd and isinstance(x, pd.DataFrame) and list(x.columns) != list(self.data.columns):
            raise IndexError(f"Input expected to have columns {list(self.data.columns)}, got {list(x.columns)}")
        elif isinstance(x, dict):
            if any(c not in x for c in self.data):
                raise IndexError(f"Input expected to have columns {sorted(self.data.keys())}, got {sorted(x.keys())}")
            elif len({len(v) for v in x.values()}) > 1:
                raise ValueError("Input columns expected to have the "
                                 "same number of rows.")


    def clear(self):
        """Clears the data in the stream

        """
        if isinstance(self.data, np.ndarray):
            data = self.data[:, :0]
        elif pd and isinstance(self.data, pd.DataFrame):
            data = self.data.iloc[:0]
        elif isinstance(self.data, dict):
            data = {k: v[:0] for k, v in self.data.items()}
        with util.disable_constant(self):
            self.data = data
        self.send(data)


    def _concat(self, data):
        """Concatenate and slice the accepted data types to the defined
        length.

        """
        if isinstance(data, np.ndarray):
            data_length = len(data)
            if not self.length:
                data = np.concatenate([self.data, data])
            elif data_length < self.length:
                prev_chunk = self.data[-(self.length-data_length):]
                data = np.concatenate([prev_chunk, data])
            elif data_length > self.length:
                data = data[-self.length:]
        elif pd and isinstance(data, pd.DataFrame):
            data_length = len(data)
            if not self.length:
                data = pd.concat([self.data, data])
            elif data_length < self.length:
                prev_chunk = self.data.iloc[-(self.length-data_length):]
                data = pd.concat([prev_chunk, data])
            elif data_length > self.length:
                data = data.iloc[-self.length:]
        elif isinstance(data, dict) and data:
            data_length = len(next(iter(data.values())))
            new_data = {}
            for k, v in data.items():
                if not self.length:
                    new_data[k] = np.concatenate([self.data[k], v])
                elif data_length < self.length:
                    prev_chunk = self.data[k][-(self.length-data_length):]
                    new_data[k] = np.concatenate([prev_chunk, v])
                elif data_length > self.length:
                    new_data[k] = v[-self.length:]
                else:
                    new_data[k] = v
            data = new_data
        self._chunk_length = data_length
        return data


    def update(self, **kwargs):
        """Overrides update to concatenate streamed data up to defined length.

        """
        data = kwargs.get('data')
        if data is not None:
            self.verify(data)
            kwargs['data'] = self._concat(data)
            self._count += 1
        return super().update(**kwargs)


    @property
    def hashkey(self):
        return {'hash': (self._count, self._memoize_counter)}


class ParamRefs(Stream):
    """
    ParamRefs accepts a dictionary of parameter references, watching
    their dependencies and returning their resolved values.
    """

    recursive = param.Boolean(default=False, constant=True, doc="Whether references should be resolved recursively.")

    refs = param.Dict(doc="Dictionary of references", constant=True)

    def __init__(self, refs=None, watch=True, **params):
        super().__init__(refs=refs or {}, **params)
        self._memoize_counter = 0
        self._watchers = []
        if not watch:
            return

        # Collect all dependencies of the provided references
        parameters = []
        for ref in self.refs.values():
            prefs = param.parameterized.resolve_ref(ref, recursive=self.recursive)
            parameters += [p for p in prefs if p not in parameters]

        # Subscribe to parameters
        keyfn = lambda x: id(x.owner)
        for _, group in groupby(sorted(parameters, key=keyfn), key=keyfn):
            group = list(group)
            watcher = group[0].owner.param.watch(self._watcher, [p.name for p in group])
            self._watchers.append(watcher)

    def unwatch(self):
        """Stop watching parameters."""
        for watcher in self._watchers:
            watcher.inst.param.unwatch(watcher)
        self._watchers.clear()

    def _watcher(self, *events):
        try:
            self._events = list(events)
            self.trigger([self])
        finally:
            self._events = []

    def _on_trigger(self):
        if any(e.type == 'triggered' for e in self._events):
            self._memoize_counter += 1

    @property
    def contents(self):
        return {
            p: param.parameterized.resolve_value(ref, recursive=self.recursive)
            for p, ref in self.refs.items()
        }

    @property
    def hashkey(self):
        hashkey = dict(self.contents)
        hashkey['_memoize_key'] = self._memoize_counter
        return hashkey

    def reset(self):
        pass


class Params(Stream):
    """A Stream that watches the changes in the parameters of the supplied
    Parameterized objects and triggers when they change.

    """

    parameterized = param.ClassSelector(class_=(param.Parameterized,
                                                param.parameterized.ParameterizedMetaclass),
                                        constant=True, allow_None=True, allow_refs=False, doc="""
        Parameterized instance to watch for parameter changes.""")

    parameters = param.List(default=[], constant=True, doc="""
        Parameters on the parameterized to watch.""")

    def __init__(self, parameterized=None, parameters=None, watch=True, watch_only=False, **params):
        if parameters is None:
            parameters = [parameterized.param[p] for p in parameterized.param if p != 'name']
        else:
            parameters = [p if isinstance(p, param.Parameter) else parameterized.param[p]
                          for p in parameters]

        if 'rename' in params:
            rename = {}
            owners = [p.owner for p in parameters]
            for k, v in params['rename'].items():
                if isinstance(k, tuple):
                    rename[k] = v
                else:
                    rename.update({(o, k): v for o in owners})
            params['rename'] = rename

        if 'linked' not in params:
            for p in parameters:
                if isinstance(p.owner, (LinkedStream, Params)) and p.owner.linked:
                    params['linked'] = True

        self._watch_only = watch_only
        super().__init__(parameterized=parameterized, parameters=parameters, **params)
        self._memoize_counter = 0
        self._events = []
        self._watchers = []
        if watch:
            # Subscribe to parameters
            keyfn = lambda x: id(x.owner)
            for _, group in groupby(sorted(parameters, key=keyfn), key=keyfn):
                group = list(group)
                watcher = group[0].owner.param.watch(self._watcher, [p.name for p in group])
                self._watchers.append(watcher)

    def unwatch(self):
        """Stop watching parameters."""
        for watcher in self._watchers:
            watcher.inst.param.unwatch(watcher)
        self._watchers.clear()

    @classmethod
    def from_params(cls, params, **kwargs):
        """Returns Params streams given a dictionary of parameters

        Parameters
        ----------
        params : dict
            Dictionary of parameters

        Returns
        -------
        List of Params streams
        """
        key_fn = lambda x: id(x[1].owner)
        streams = []
        for _, group in groupby(sorted(params.items(), key=key_fn), key_fn):
            group = list(group)
            inst = next(p.owner for _, p in group)
            if inst is None:
                continue
            names = [p.name for _, p in group]
            rename = {p.name: n for n, p in group}
            streams.append(cls(inst, names, rename=rename, **kwargs))
        return streams

    def _validate_rename(self, mapping):
        pnames = [p.name for p in self.parameters]
        for k, v in mapping.items():
            n = k[1] if isinstance(k, tuple) else k
            if n not in pnames:
                raise KeyError(f'Cannot rename {n!r} as it is not a stream parameter')
            if n != v and v in pnames:
                raise KeyError(f'Cannot rename to {v!r} as it clashes with a '
                               'stream parameter of the same name')
        return mapping

    def _watcher(self, *events):
        try:
            self._events = list(events)
            self.trigger([self])
        finally:
            self._events = []

    def _on_trigger(self):
        if any(e.type == 'triggered' for e in self._events):
            self._memoize_counter += 1

    @property
    def hashkey(self):
        hashkey = {}
        for p in self.parameters:
            pkey = (p.owner, p.name)
            pname = self._rename.get(pkey, p.name)
            key = ' '.join([str(id(p.owner)), pname])
            if self._rename.get(pkey, True) is not None:
                hashkey[key] = getattr(p.owner, p.name)
        hashkey['_memoize_key'] = self._memoize_counter
        return hashkey

    def reset(self):
        pass

    def update(self, **kwargs):
        if self._rename:
            owner_updates = defaultdict(dict)
            for (owner, pname), rname in self._rename.items():
                if rname in kwargs:
                    owner_updates[owner][pname] = kwargs[rname]
            for owner, updates in owner_updates.items():
                if isinstance(owner, Stream):
                    owner.update(**updates)
                else:
                    owner.param.update(**updates)
        elif isinstance(self.parameterized, Stream):
            self.parameterized.update(**kwargs)
            return
        else:
            self.parameterized.param.update(**kwargs)

    @property
    def contents(self):
        if self._watch_only:
            return {}
        filtered = {(p.owner, p.name): getattr(p.owner, p.name) for p in self.parameters}
        return {self._rename.get((o, n), n): v for (o, n), v in filtered.items()
                if self._rename.get((o, n), True) is not None}



class ParamMethod(Params):
    """A Stream that watches the parameter dependencies on a method of
    a parameterized class and triggers when one of the parameters
    change.

    """

    parameterized = param.ClassSelector(class_=(param.Parameterized,
                                                param.parameterized.ParameterizedMetaclass),
                                        constant=True, allow_None=True, doc="""
        Parameterized instance to watch for parameter changes.""")

    parameters = param.List(default=[], constant=True, doc="""
        Parameters on the parameterized to watch.""")

    def __init__(self, parameterized, parameters=None, watch=True, **params):
        if not util.is_param_method(parameterized):
            raise ValueError('ParamMethod stream expects a method on a '
                             f'parameterized class, found {type(parameterized).__name__}.')
        method = parameterized
        parameterized = util.get_method_owner(parameterized)
        if not parameters:
            parameters = [p.pobj for p in parameterized.param.method_dependencies(method.__name__)]

        params['watch_only'] = True
        super().__init__(parameterized, parameters, watch, **params)


class Derived(Stream):
    """A Stream that watches the parameters of one or more input streams and produces
    a result that is a pure function of the input stream values.

    If exclusive=True, then all streams except the most recently updated are cleared.

    """

    def __init__(self, input_streams, exclusive=False, **params):
        super().__init__(**params)
        self.input_streams = []
        self._updating = set()
        self._register_streams(input_streams)
        self.exclusive = exclusive
        self.update()

    def _register_streams(self, streams):
        """Register callbacks to watch for changes to input streams

        """
        for stream in streams:
            self._register_stream(stream)

    def _register_stream(self, stream):
        i = len(self.input_streams)

        def perform_update(stream_index=i, **kwargs):
            if stream_index in self._updating:
                return

            # If exclusive, reset other stream values before triggering event
            if self.exclusive:
                for j, input_stream in enumerate(self.input_streams):
                    if stream_index != j:
                        input_stream.reset()
                        self._updating.add(j)
                        try:
                            input_stream.event()
                        finally:
                            self._updating.remove(j)
            self.event()

        stream.add_subscriber(perform_update)
        self.input_streams.append(stream)

    def _unregister_input_streams(self):
        """Unregister callbacks on input streams and clear input streams list

        """
        for stream in self.input_streams:
            stream.source = None
            stream.clear()
        self.input_streams.clear()

    def append_input_stream(self, stream):
        """Add a new input stream

        """
        self._register_stream(stream)

    @property
    def constants(self):
        """Dict of constants for this instance that should be passed to transform_function

        Constant values must not change in response to changes in the values of the
        input streams. They may, however, change in response to other stream property
        updates. For example, these values may change if the Stream's source element
        changes

        """
        return {}

    def transform(self):
        stream_values = [s.contents for s in self.input_streams]
        return self.transform_function(stream_values, self.constants)

    @classmethod
    def transform_function(cls, stream_values, constants):
        """Pure function that transforms input stream param values into the param values
        of this Derived stream.

        Parameters
        ----------
        stream_values : list of dict
            Current values of the stream params for each input_stream
        constants : dict
            Constants as returned by the constants property of an instance of this
            stream type.

        Returns
        -------
        dict
            dict of new Stream values where the keys match this stream's params
        """
        raise NotImplementedError

    def cleanup(self):
        self._unregister_input_streams()

    def __del__(self):
        self.cleanup()


class History(Stream):
    """A Stream that maintains a history of the values of a single input stream

    """

    values = param.List(constant=True, doc="""
        List containing the historical values of the input stream""")

    def __init__(self, input_stream, **params):
        super().__init__(**params)
        self.input_stream = input_stream
        self._register_input_stream()
        # Trigger event on input stream after registering so that current value is
        # added to our values list
        self.input_stream.event()

    def clone(self):
        return type(self)(self.input_stream.clone(), **self.contents)

    def clear_history(self):
        del self.values[:]

    def _register_input_stream(self):
        """Register callback on input_stream to watch for changes

        """
        def perform_update(**kwargs):
            self.values.append(kwargs)
            self.event()

        self.input_stream.add_subscriber(perform_update)

    def cleanup(self):
        self.input_stream.source = None
        self.input_stream.clear()
        del self.values[:]

    def __del__(self):
        self.cleanup()


class SelectionExpr(Derived):
    selection_expr = param.Parameter(default=None, constant=True)

    bbox = param.Dict(default=None, constant=True)

    region_element = param.Parameter(default=None, constant=True)

    def __init__(self, source, include_region=True, **params):
        from .core.spaces import DynamicMap
        from .element import Element
        from .plotting.util import initialize_dynamic

        self._index_cols = params.pop('index_cols', None)
        self.include_region = include_region

        if isinstance(source, DynamicMap):
            initialize_dynamic(source)

        if not ((isinstance(source, DynamicMap) and issubclass(source.type, Element))
                or isinstance(source, Element)):
            raise ValueError(
                "The source of SelectionExpr must be an instance of an "
                "Element subclass or a DynamicMap that returns such an "
                f"instance. Received value of type {type(source)}: {source}"
            )

        input_streams = self._build_selection_streams(source)
        super().__init__(
            source=source, input_streams=input_streams, exclusive=True, **params
        )

    def clone(self):
        return type(self)(self.source, **self.contents)

    def _build_selection_streams(self, source):
        from holoviews.core.spaces import DynamicMap
        if isinstance(source, DynamicMap):
            element_type = source.type
        else:
            element_type = source
        if element_type:
            input_streams = []
            for stream in element_type._selection_streams:
                kwargs = dict(source=source)
                if isinstance(stream, Selection1D):
                    kwargs['index'] = None
                input_streams.append(stream(**kwargs))
            return input_streams
        else:
            return []

    @property
    def constants(self):
        return {
            "source": self.source,
            "index_cols": self._index_cols,
            "include_region": self.include_region,
        }

    def transform(self):
        # Skip index streams if no index_cols are provided
        for stream in self.input_streams:
            if (isinstance(stream, Selection1D) and stream._triggering
                and not self._index_cols):
                return
        return super().transform()

    @classmethod
    def transform_function(cls, stream_values, constants):
        hvobj = constants["source"]
        include_region = constants["include_region"]
        if hvobj is None:
            # source is None
            return dict(selection_expr=None, bbox=None, region_element=None,)

        from holoviews.core.spaces import DynamicMap
        # Import after checking for hvobj None to avoid "sys.meta_path is None"
        # error on shutdown
        if isinstance(hvobj, DynamicMap):
            element = hvobj.values()[-1]
        else:
            element = hvobj

        selection_expr = None
        bbox = None
        region_element = None
        for stream_value in stream_values:
            params = dict(stream_value, index_cols=constants["index_cols"])
            selection = element._get_selection_expr_for_stream_value(**params)
            if selection is None:
                return

            selection_expr, bbox, region_element = selection

            if selection_expr is not None:
                break

        for expr_transform in element._transforms[::-1]:
            if selection_expr is not None:
                selection_expr = expr_transform(selection_expr)

        return dict(
            selection_expr=selection_expr,
            bbox=bbox,
            region_element=region_element if include_region else None,
        )

    @property
    def source(self):
        return Stream.source.fget(self)

    @source.setter
    def source(self, value):
        # Unregister old selection streams
        self._unregister_input_streams()

        # Set new source
        Stream.source.fset(self, value)

        # Build selection input streams for new source element
        if self.source is not None:
            input_streams = self._build_selection_streams(self.source)
        else:
            input_streams = []

        # Clear current selection expression state
        self.update(
            selection_expr=None,
            bbox=None,
            region_element=None,
        )

        # Register callbacks on input streams
        self._register_streams(input_streams)


class SelectionExprSequence(Derived):
    selection_expr = param.Parameter(default=None, constant=True)
    region_element = param.Parameter(default=None, constant=True)

    def __init__(
            self, source, mode="overwrite",
            include_region=True, **params
    ):
        self.mode = mode
        self.include_region = include_region
        sel_expr = SelectionExpr(
            source, index_cols=params.pop('index_cols'),
            **params
        )
        self.history_stream = History(sel_expr)
        input_streams = [self.history_stream]

        super().__init__(
            source=source, input_streams=input_streams, **params
        )

    @property
    def constants(self):
        return {
            "source": self.source,
            "mode": self.mode,
            "include_region": self.include_region,
        }

    def reset(self):
        if self.input_streams:
            self.input_streams[0].clear_history()
        super().reset()

    @classmethod
    def transform_function(cls, stream_values, constants):
        from .core.spaces import DynamicMap
        mode = constants["mode"]
        source = constants["source"]
        include_region = constants["include_region"]

        combined_selection_expr = None
        combined_region_element = None

        for selection_contents in stream_values[0]["values"]:
            if selection_contents is None:
                continue
            selection_expr = selection_contents['selection_expr']
            if not selection_expr:
                continue
            region_element = selection_contents['region_element']

            # Update combined selection expression
            if combined_selection_expr is None or mode == "overwrite":
                if mode == "inverse":
                    combined_selection_expr = ~selection_expr
                else:
                    combined_selection_expr = selection_expr
            elif mode == "intersect":
                combined_selection_expr &= selection_expr
            elif mode == "union":
                combined_selection_expr |= selection_expr
            else:  # inverse
                combined_selection_expr &= ~selection_expr

            # Update region
            if isinstance(source, DynamicMap):
                el_type = source.type
            else:
                el_type = source

            combined_region_element = el_type._merge_regions(
                combined_region_element, region_element, mode
            )

        return dict(
            selection_expr=combined_selection_expr,
            region_element=combined_region_element if include_region else None
        )

    def cleanup(self):
        super().cleanup()
        self.history_stream.cleanup()


class CrossFilterSet(Derived):
    selection_expr = param.Parameter(default=None, constant=True)

    def __init__(self, selection_streams=(), mode="intersection", index_cols=None, **params):
        self._mode = mode
        self._index_cols = index_cols
        input_streams = list(selection_streams)
        exclusive = mode == "overwrite"
        super().__init__(
            input_streams, exclusive=exclusive, **params
        )

    @property
    def mode(self):
        return self._mode

    @mode.setter
    def mode(self, v):
        if v != self._mode:
            self._mode = v
            self.reset()
            self.exclusive = self._mode == "overwrite"

    @property
    def constants(self):
        return {
            "mode": self.mode,
            "index_cols": self._index_cols
        }

    def reset(self):
        super().reset()
        for stream in self.input_streams:
            stream.reset()

    @classmethod
    def transform_function(cls, stream_values, constants):
        from .util.transform import dim

        index_cols = constants["index_cols"]

        # Get non-none selection expressions
        selection_exprs = [sv["selection_expr"] for sv in stream_values]
        selection_exprs = [expr for expr in selection_exprs if expr is not None]
        selection_expr = None
        if len(selection_exprs) > 0:
            if index_cols:
                if len(selection_exprs) > 1:
                    vals = set.intersection(
                        *(set(expr.ops[2]['args'][0]) for expr in selection_exprs))
                    old = selection_exprs[0]
                    selection_expr = dim('new')
                    selection_expr.dimension = old.dimension
                    selection_expr.ops = list(old.ops)
                    selection_expr.ops[2] = dict(selection_expr.ops[2], args=(list(vals),))
            else:
                selection_expr = selection_exprs[0]
                for expr in selection_exprs[1:]:
                    selection_expr = selection_expr & expr

        return dict(selection_expr=selection_expr)


class LinkedStream(Stream):
    """A LinkedStream indicates is automatically linked to plot interactions
    on a backend via a Renderer. Not all backends may support dynamically
    supplying stream data.

    """

    def __init__(self, linked=True, popup=None, popup_position="top_right", popup_anchor=None, **params):
        if popup_position not in POPUP_POSITIONS:
            raise ValueError(
                f"Invalid popup_position: {popup_position!r}; "
                f"expect one of {POPUP_POSITIONS}"
            )

        super().__init__(linked=linked, **params)
        self.popup = popup
        self.popup_position = popup_position
        self.popup_anchor = popup_anchor


class PointerX(LinkedStream):
    """A pointer position along the x-axis in data coordinates which may be
    a numeric or categorical dimension.

    With the appropriate plotting backend, this corresponds to the
    position of the mouse/trackpad cursor. If the pointer is outside the
    plot bounds, the position is set to None.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")


class PointerY(LinkedStream):
    """A pointer position along the y-axis in data coordinates which may be
    a numeric or categorical dimension.

    With the appropriate plotting backend, this corresponds to the
    position of the mouse/trackpad pointer. If the pointer is outside
    the plot bounds, the position is set to None.

    """

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")


class PointerXY(LinkedStream):
    """A pointer position along the x- and y-axes in data coordinates which
    may numeric or categorical dimensions.

    With the appropriate plotting backend, this corresponds to the
    position of the mouse/trackpad pointer. If the pointer is outside
    the plot bounds, the position values are set to None.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")


class Draw(PointerXY):
    """A series of updating x/y-positions when drawing, together with the
    current stroke count

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")

    stroke_count = param.Integer(default=0, constant=True, doc="""
       The current drawing stroke count. Increments every time a new
       stroke is started.""")

class SingleTap(PointerXY):
    """The x/y-position of a single tap or click in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")

class Tap(PointerXY):
    """The x/y-position of a tap or click in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")


class MultiAxisTap(LinkedStream):
    """The x/y-positions of a tap or click in data coordinates.

    """

    xs = param.Dict(default=None, constant=True, doc="""
           Pointer positions along the x-axes in data coordinates""")

    ys = param.Dict(default=None, constant=True, doc="""
           Pointer positions along the y-axes in data coordinates""")


class DoubleTap(PointerXY):
    """The x/y-position of a double-tap or -click in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")

class PressUp(PointerXY):
    """The x/y position of a mouse pressup event in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")

class PanEnd(PointerXY):
    """The x/y position of a the end of a pan event in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")

class MouseEnter(PointerXY):
    """The x/y-position where the mouse/cursor entered the plot area
    in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")


class MouseLeave(PointerXY):
    """The x/y-position where the mouse/cursor entered the plot area
    in data coordinates.

    """

    x = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the x-axis in data coordinates""")

    y = param.ClassSelector(class_=pointer_types, default=None,
                            constant=True, doc="""
           Pointer position along the y-axis in data coordinates""")


class PlotSize(LinkedStream):
    """Returns the dimensions of a plot once it has been displayed.

    """

    width = param.Integer(default=None, constant=True, doc="The width of the plot in pixels")

    height = param.Integer(default=None, constant=True, doc="The height of the plot in pixels")

    scale = param.Number(default=1.0, constant=True, doc="""
       Scale factor to scale width and height values reported by the stream""")

    def transform(self):
        return {'width':  int(self.width * self.scale) if self.width else None,
                'height': int(self.height * self.scale) if self.height else None}


class SelectMode(LinkedStream):

    mode = param.Selector(default="replace", constant=True, objects=[
        "replace", "append", "intersect", "subtract"], doc="""
        Defines what should happen when a new selection is made. The
        default is to replace the existing selection. Other options
        are to append to theselection, intersect with it or subtract
        from it.""")


class RangeXY(LinkedStream):
    """Axis ranges along x- and y-axis in data coordinates.

    """

    x_range = param.Tuple(default=None, length=2, constant=True, doc="""
      Range of the x-axis of a plot in data coordinates""")

    y_range = param.Tuple(default=None, length=2, constant=True, doc="""
      Range of the y-axis of a plot in data coordinates""")


class RangeX(LinkedStream):
    """Axis range along x-axis in data coordinates.

    """

    x_range = param.Tuple(default=None, length=2, constant=True, doc="""
      Range of the x-axis of a plot in data coordinates""")

    def _set_stream_parameters(self, **kwargs):
        kwargs.pop("y_range", None)
        super()._set_stream_parameters(**kwargs)


class RangeY(LinkedStream):
    """Axis range along y-axis in data coordinates.

    """

    y_range = param.Tuple(default=None, length=2, constant=True, doc="""
      Range of the y-axis of a plot in data coordinates""")

    def _set_stream_parameters(self, **kwargs):
        kwargs.pop("x_range", None)
        super()._set_stream_parameters(**kwargs)


class BoundsXY(LinkedStream):
    """A stream representing the bounds of a box selection as an
    tuple of the left, bottom, right and top coordinates.

    """

    bounds = param.Tuple(default=None, constant=True, length=4,
                         allow_None=True, doc="""
        Bounds defined as (left, bottom, right, top) tuple.""")


class Lasso(LinkedStream):
    """A stream representing a lasso selection in 2D space as a two-column
    array of coordinates.

    """

    geometry = param.Array(constant=True, doc="""
        The coordinates of the lasso geometry as a two-column array.""")


class SelectionXY(BoundsXY):
    """A stream representing the selection along the x-axis and y-axis.
    Unlike a BoundsXY stream, this stream returns range or categorical
    selections.

    """

    bounds = param.Tuple(default=None, constant=True, length=4,
                         allow_None=True, doc="""
        Bounds defined as (left, bottom, right, top) tuple.""")

    x_selection = param.ClassSelector(class_=(tuple, list), allow_None=True,
                                      constant=True, doc="""
      The current selection along the x-axis, either a numerical range
      defined as a tuple or a list of categories.""")

    y_selection = param.ClassSelector(class_=(tuple, list), allow_None=True,
                                      constant=True, doc="""
      The current selection along the y-axis, either a numerical range
      defined as a tuple or a list of categories.""")


class BoundsX(LinkedStream):
    """A stream representing the bounds of a box selection as an
    tuple of the left and right coordinates.

    """

    boundsx = param.Tuple(default=None, constant=True, length=2,
                          allow_None=True, doc="""
        Bounds defined as (left, right) tuple.""")


class BoundsY(LinkedStream):
    """A stream representing the bounds of a box selection as an
    tuple of the bottom and top coordinates.

    """

    boundsy = param.Tuple(default=None, constant=True, length=2,
                          allow_None=True, doc="""
        Bounds defined as (bottom, top) tuple.""")


class Selection1D(LinkedStream):
    """A stream representing a 1D selection of objects by their index.

    """

    index = param.List(default=[], allow_None=True, constant=True, doc="""
        Indices into a 1D datastructure.""")


class PlotReset(LinkedStream):
    """A stream signalling when a plot reset event has been triggered.

    """

    resetting = param.Boolean(default=False, constant=True, doc="""
        Whether a reset event is being signalled.""")

    def __init__(self, *args, **params):
        super().__init__(self, *args, **dict(params, transient=True))


class CDSStream(LinkedStream):
    """A Stream that syncs a bokeh ColumnDataSource with python.

    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")


class PointDraw(CDSStream):
    """Attaches a PointDrawTool and syncs the datasource.

    add : boolean
        Whether to allow adding new Points

    drag : boolean
        Whether to enable dragging of Points

    empty_value : int/float/string/None
        The value to insert on non-position columns when adding a new polygon

    num_objects : int
        The number of polygons that can be drawn before overwriting
        the oldest polygon.

    styles : dict
        A dictionary specifying lists of styles to cycle over whenever
        a new Point glyph is drawn.

    tooltip : str
        An optional tooltip to override the default
    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")

    def __init__(self, empty_value=None, add=True, drag=True, num_objects=0,
                 styles=None, tooltip=None, **params):
        if styles is None:
            styles = {}
        self.add = add
        self.drag = drag
        self.empty_value = empty_value
        self.num_objects = num_objects
        self.styles = styles
        self.tooltip = tooltip
        self.styles = styles
        super().__init__(**params)

    @property
    def element(self):
        source = self.source
        if isinstance(source, UniformNdMapping):
            source = source.last
        if not self.data:
            return source.clone([], id=None)
        return source.clone(self.data, id=None)

    @property
    def dynamic(self):
        from .core.spaces import DynamicMap
        return DynamicMap(lambda *args, **kwargs: self.element, streams=[self])



class CurveEdit(PointDraw):
    """Attaches a PointDraw to the plot which allows editing the Curve when selected.

    style : dict
        A dictionary specifying the style of the vertices.

    tooltip : str
        An optional tooltip to override the default
    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")

    def __init__(self, style=None, tooltip=None, **params):
        if style is None:
            style = {}
        self.style = style or {'size': 10}
        self.tooltip = tooltip
        super(PointDraw, self).__init__(**params)



class PolyDraw(CDSStream):
    """Attaches a PolyDrawTool and syncs the datasource.

    drag : boolean
        Whether to enable dragging of polygons and paths

    empty_value : int/float/string/None
        The value to insert on non-position columns when adding a new polygon

    num_objects : int
        The number of polygons that can be drawn before overwriting
        the oldest polygon.

    show_vertices : boolean
        Whether to show the vertices when a polygon is selected

    styles : dict
        A dictionary specifying lists of styles to cycle over whenever
        a new Poly glyph is drawn.

    tooltip : str
        An optional tooltip to override the default

    vertex_style : dict
        A dictionary specifying the style options for the vertices.
        The usual bokeh style options apply, e.g. fill_color,
        line_alpha, size, etc.
    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")

    def __init__(self, empty_value=None, drag=True, num_objects=0,
                 show_vertices=False, vertex_style=None, styles=None,
                 tooltip=None, **params):
        if styles is None:
            styles = {}
        if vertex_style is None:
            vertex_style = {}
        self.drag = drag
        self.empty_value = empty_value
        self.num_objects = num_objects
        self.show_vertices = show_vertices
        self.vertex_style = vertex_style
        self.styles = styles
        self.tooltip = tooltip
        super().__init__(**params)

    @property
    def element(self):
        source = self.source
        if isinstance(source, UniformNdMapping):
            source = source.last
        data = self.data
        if not data:
            return source.clone([], id=None)
        cols = list(self.data)
        x, y = source.kdims
        lookup = {'xs': x.name, 'ys': y.name}
        data = [{lookup.get(c, c): data[c][i] for c in self.data}
                for i in range(len(data[cols[0]]))]
        datatype = source.datatype if source.interface.multi else ['multitabular']
        return source.clone(data, datatype=datatype, id=None)

    @property
    def dynamic(self):
        from .core.spaces import DynamicMap
        return DynamicMap(lambda *args, **kwargs: self.element, streams=[self])


class FreehandDraw(CDSStream):
    """Attaches a FreehandDrawTool and syncs the datasource.

    empty_value : int/float/string/None
        The value to insert on non-position columns when adding a new polygon

    num_objects : int
        The number of polygons that can be drawn before overwriting
        the oldest polygon.

    styles : dict
        A dictionary specifying lists of styles to cycle over whenever
        a new freehand glyph is drawn.

    tooltip : str
        An optional tooltip to override the default
    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")

    def __init__(self, empty_value=None, num_objects=0, styles=None, tooltip=None, **params):
        if styles is None:
            styles = {}
        self.empty_value = empty_value
        self.num_objects = num_objects
        self.styles = styles
        self.tooltip = tooltip
        super().__init__(**params)

    @property
    def element(self):
        source = self.source
        if isinstance(source, UniformNdMapping):
            source = source.last
        data = self.data
        if not data:
            return source.clone([], id=None)
        cols = list(self.data)
        x, y = source.kdims
        lookup = {'xs': x.name, 'ys': y.name}
        data = [{lookup.get(c, c): data[c][i] for c in self.data}
                for i in range(len(data[cols[0]]))]
        return source.clone(data, id=None)

    @property
    def dynamic(self):
        from .core.spaces import DynamicMap
        return DynamicMap(lambda *args, **kwargs: self.element, streams=[self])



class BoxEdit(CDSStream):
    """Attaches a BoxEditTool and syncs the datasource.

    empty_value : int/float/string/None
        The value to insert on non-position columns when adding a new box

    num_objects : int
        The number of boxes that can be drawn before overwriting the
        oldest drawn box.

    styles : dict
        A dictionary specifying lists of styles to cycle over whenever
        a new box glyph is drawn.

    tooltip : str
        An optional tooltip to override the default
    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")

    def __init__(self, empty_value=None, num_objects=0, styles=None, tooltip=None, **params):
        if styles is None:
            styles = {}
        self.empty_value = empty_value
        self.num_objects = num_objects
        self.styles = styles
        self.tooltip = tooltip
        super().__init__(**params)

    @property
    def element(self):
        from .element import Polygons, Rectangles
        source = self.source
        if isinstance(source, UniformNdMapping):
            source = source.last
        data = self.data
        if not data:
            return source.clone([])

        dims = ['x0', 'y0', 'x1', 'y1']+[vd.name for vd in source.vdims]
        if isinstance(source, Rectangles):
            data = tuple(data[d] for d in dims)
            return source.clone(data, id=None)
        paths = []
        for i, (x0, x1, y0, y1) in enumerate(zip(data['x0'], data['x1'], data['y0'], data['y1'], strict=None)):
            xs = [x0, x0, x1, x1]
            ys = [y0, y1, y1, y0]
            if isinstance(source, Polygons):
                xs.append(x0)
                ys.append(y0)
            vals = [data[vd.name][i] for vd in source.vdims]
            paths.append((xs, ys, *vals))
        datatype = source.datatype if source.interface.multi else ['multitabular']
        return source.clone(paths, datatype=datatype, id=None)

    @property
    def dynamic(self):
        from .core.spaces import DynamicMap
        return DynamicMap(lambda *args, **kwargs: self.element, streams=[self])



class PolyEdit(PolyDraw):
    """Attaches a PolyEditTool and syncs the datasource.

    shared : boolean
        Whether PolyEditTools should be shared between multiple elements

    tooltip : str
        An optional tooltip to override the default

    vertex_style : dict
        A dictionary specifying the style options for the vertices.
        The usual bokeh style options apply, e.g. fill_color,
        line_alpha, size, etc.
    """

    data = param.Dict(constant=True, doc="""
        Data synced from Bokeh ColumnDataSource supplied as a
        dictionary of columns, where each column is a list of values
        (for point-like data) or list of lists of values (for
        path-like data).""")

    def __init__(self, vertex_style=None, shared=True, **params):
        if vertex_style is None:
            vertex_style = {}
        self.shared = shared
        super().__init__(vertex_style=vertex_style, **params)


def _streams_transform(obj):
    if isinstance(obj, Pipe):
        return obj.param.data
    return obj


param.reactive.register_reference_transform(_streams_transform)
