"""Options and OptionTrees allow different classes of options
(e.g. matplotlib-specific styles and plot specific parameters) to be
defined separately from the core data structures and away from
visualization specific code.

There are three classes that form the options system:

Cycle:

   Used to define infinite cycles over a finite set of elements, using
   either an explicit list or some pre-defined collection (e.g. from
   matplotlib rcParams). For instance, a Cycle object can be used loop
   a set of display colors for multiple curves on a single axis.

Options:

   Containers of arbitrary keyword values, including optional keyword
   validation, support for Cycle objects and inheritance.

OptionTree:

   A subclass of AttrTree that is used to define the inheritance
   relationships between a collection of Options objects. Each node
   of the tree supports a group of Options objects and the leaf nodes
   inherit their keyword values from parent nodes up to the root.

Store:

   A singleton class that stores all global and custom options and
   links HoloViews objects, the chosen plotting backend and the IPython
   extension together.

"""
import difflib
import inspect
import pickle
import traceback
from collections import defaultdict
from contextlib import contextmanager

import numpy as np
import param

from .accessors import Opts  # noqa (clean up in 2.0)
from .pprint import InfoPrinter
from .tree import AttrTree
from .util import group_sanitizer, label_sanitizer, sanitize_identifier


def cleanup_custom_options(id, weakref=None):
    """Cleans up unused custom trees if all objects referencing the
    custom id have been garbage collected or tree is otherwise
    unreferenced.

    """
    try:
        if Store._options_context:
            return
        weakrefs = Store._weakrefs.get(id, [])
        if weakref in weakrefs:
            weakrefs.remove(weakref)
        refs = []
        for wr in list(weakrefs):
            r = wr()
            if r is None or r.id != id:
                weakrefs.remove(wr)
            else:
                refs.append(r)
        if not refs:
            for bk in Store.loaded_backends():
                if id in Store._custom_options[bk]:
                    Store._custom_options[bk].pop(id)
        if not weakrefs:
            Store._weakrefs.pop(id, None)
    except Exception as e:
        raise Exception(
            f"Cleanup of custom options tree with id '{id}' failed "
            f"with the following exception: {e}, an unreferenced "
            "orphan tree may persist in memory."
        ) from e


def lookup_options(obj, group, backend):
    """Given a HoloViews object, a plot option group (e.g. 'style') and
    backend, return the corresponding Options object.

    """
    plot_class = None
    try:
        plot_class = Store.renderers[backend].plotting_class(obj)
        style_opts = plot_class.style_opts
    except SkipRendering:
        style_opts = None

    node = Store.lookup_options(backend, obj, group)
    if group == 'style' and style_opts is not None:
        return node.filtered(style_opts)
    elif group == 'plot' and plot_class:
        return node.filtered(list(plot_class.param))
    else:
        return node


class CallbackError(RuntimeError):
    """An error raised during a callback.

    """


class SkipRendering(Exception):
    """A SkipRendering exception in the plotting code will make the display
    hooks fall back to a text repr. Used to skip rendering of
    DynamicMaps with exhausted element generators.

    """

    def __init__(self, message="", warn=True):
        self.warn = warn
        super().__init__(message)


class OptionError(Exception):
    """Custom exception raised when there is an attempt to apply invalid
    options. Stores the necessary information to construct a more
    readable message for the user if caught and processed
    appropriately.

    """

    def __init__(self, invalid_keyword, allowed_keywords,
                 group_name=None, path=None):
        super().__init__(self.message(invalid_keyword,
                                      allowed_keywords,
                                      group_name, path))
        self.invalid_keyword = invalid_keyword
        self.allowed_keywords = allowed_keywords
        self.group_name =group_name
        self.path = path

    def message(self, invalid_keyword, allowed_keywords, group_name, path):
        msg = (
            f"Invalid option {invalid_keyword!r}, valid options are: "
            f"{allowed_keywords}."
        )
        if path and group_name:
            msg = f"Invalid key for group {group_name!r} on path {path};\n{msg}"
        return msg

    def format_options_error(self):
        """Return a fuzzy match message based on the OptionError

        """
        allowed_keywords = self.allowed_keywords
        target = allowed_keywords.target
        matches = allowed_keywords.fuzzy_match(self.invalid_keyword)
        if not matches:
            matches = allowed_keywords.values
            similarity = 'Possible'
        else:
            similarity = 'Similar'

        loaded_backends = Store.loaded_backends()
        target = f'for {target}' if target else ''

        if len(loaded_backends) == 1:
            loaded = f' in loaded backend {loaded_backends[0]!r}'
        else:
            backend_list = ', '.join([repr(b) for b in loaded_backends[:-1]])
            loaded = f' in loaded backends {backend_list} and {loaded_backends[-1]!r}'

        group = f'{self.group_name} option' if self.group_name else 'keyword'
        return (
            f"Unexpected {group} '{self.invalid_keyword}' {target}"
            f"{loaded}.\n\n{similarity} keywords in the currently "
            f"active '{Store.current_backend}' renderer are: "
            f"{matches}\n\nIf you believe this keyword is correct, "
            "please make sure the backend has been imported or loaded "
            "with the hv.extension."
        )


class AbbreviatedException(Exception):
    """Raised by the abbreviate_exception context manager when it is
    appropriate to present an abbreviated the traceback and exception
    message in the notebook.

    Particularly useful when processing style options supplied by the
    user which may not be valid.

    """

    def __init__(self, etype, value, traceback):
        self.etype = etype
        self.value = value
        self.traceback = traceback
        self.msg = str(value)

    def __str__(self):
        return (
            f'{self.etype.__name__}: {self.msg}\n\n'
            'To view the original traceback, catch this exception and '
            'call print_traceback() method.'
        )

    def print_traceback(self):
        """Print the traceback of the exception wrapped by the AbbreviatedException.

        """
        traceback.print_exception(self.etype, self.value, self.traceback)


class abbreviated_exception:
    """Context manager used to to abbreviate tracebacks using an
    AbbreviatedException when a backend may raise an error due to
    incorrect style options.

    """

    def __enter__(self):
        return self

    def __exit__(self, etype, value, traceback):
        if isinstance(value, Exception):
            raise AbbreviatedException(etype, value, traceback)


@contextmanager
def options_policy(skip_invalid, warn_on_skip):
    """Context manager to temporarily set the skip_invalid and warn_on_skip
    class parameters on Options.

    """
    settings = (Options.skip_invalid, Options.warn_on_skip)
    (Options.skip_invalid, Options.warn_on_skip) = (skip_invalid, warn_on_skip)
    try:
        yield
    finally:
        (Options.skip_invalid, Options.warn_on_skip) = settings


class Keywords:
    """A keywords objects represents a set of Python keywords. It is
    list-like and ordered but it is also a set without duplicates. When
    passed as `**kwargs`, Python keywords are not ordered but this class
    always lists keywords in sorted order.

    In addition to containing the list of keywords, Keywords has an
    optional target which describes what the keywords are applicable to.

    This class is for internal use only and should not be in the user
    namespace.

    """

    def __init__(self, values=None, target=None):
        if values is None:
            values = []
        if any(not isinstance(v, str) for v in values):
            raise ValueError(f'All keywords must be strings: {values}')
        self.values = sorted(values)
        if target is not None and not isinstance(target, str):
            raise ValueError("Keywords target must be a string type.")
        self.target = target

    def __add__(self, other):
        if (self.target and other.target) and (self.target != other.target):
            raise Exception('Targets must match to combine Keywords')
        target = self.target or other.target
        return Keywords(sorted(set(self.values + other.values)), target=target)

    def fuzzy_match(self, kw):
        """Given a string, fuzzy match against the Keyword values,
        returning a list of close matches.

        """
        return difflib.get_close_matches(kw, self.values)

    def __repr__(self):
        if self.target:
            msg = 'Keywords({values}, target={target})'
            info = dict(values=self.values, target=self.target)
        else:
            msg = 'Keywords({values})'
            info = dict(values=self.values)
        return msg.format(**info)

    def __str__(self):           return str(self.values)
    def __iter__(self):          return iter(self.values)
    def __bool__(self):          return bool(self.values)
    def __contains__(self, val): return val in self.values



class Cycle(param.Parameterized):
    """A simple container class that specifies cyclic options. A typical
    example would be to cycle the curve colors in an Overlay composed
    of an arbitrary number of curves. The values may be supplied as
    an explicit list or a key to look up in the default cycles
    attribute.

    """

    key = param.String(default='default_colors', allow_None=True, doc="""
       The key in the default_cycles dictionary used to specify the
       color cycle if values is not supplied. """)

    values = param.List(default=[], doc="""
       The values the cycle will iterate over.""")

    default_cycles = {'default_colors': []}

    def __init__(self, cycle=None, **params):
        if cycle is not None:
            if isinstance(cycle, str):
                params['key'] = cycle
            else:
                params['values'] = cycle
                params['key'] = None
        super().__init__(**params)
        self.values = self._get_values()


    def __getitem__(self, num):
        return self(values=self.values[:num])


    def _get_values(self):
        if self.values: return self.values
        elif self.key and self.key in self.default_cycles:
            return list(self.default_cycles[self.key])
        elif self.key:
            current = Store.current_backend
            others = " or ".join({"matplotlib", "bokeh", "plotly"} - {current})
            raise ValueError(
                "The key you have supplied is not available "
                f"for the {current} backend. Try to switch to {others} "
                "with 'hv.extension(backend)'."
            )
        else:
            raise ValueError("Supply either a key or explicit values.")


    def __call__(self, values=None, **params):
        values = values if values else self.values
        return self.__class__(**dict(self.param.values(), values=values, **params))


    def __len__(self):
        return len(self.values)


    def __repr__(self):
        if self.key == self.param.objects(False)['key'].default:
            vrepr = ''
        elif self.key:
            vrepr = repr(self.key)
        else:
            vrepr = [str(el) for el in self.values]
        return f"{type(self).__name__}({vrepr})"



def grayscale(val):
    return (val, val, val, 1.0)


class Palette(Cycle):
    """Palettes allow easy specifying a discrete sampling
    of an existing colormap. Palettes may be supplied a key
    to look up a function function in the colormap class
    attribute. The function should accept a float scalar
    in the specified range and return a RGB(A) tuple.
    The number of samples may also be specified as a
    parameter.

    The range and samples may conveniently be overridden
    with the __getitem__ method.

    """

    key = param.String(default='grayscale', doc="""
       Palettes look up the Palette values based on some key.""")

    range = param.NumericTuple(default=(0, 1), doc="""
        The range from which the Palette values are sampled.""")

    samples = param.Integer(default=32, doc="""
        The number of samples in the given range to supply to
        the sample_fn.""")

    sample_fn = param.Callable(default=np.linspace, doc="""
        The function to generate the samples, by default linear.""")

    reverse = param.Boolean(default=False, doc="""
        Whether to reverse the palette.""")

    # A list of available colormaps
    colormaps = {'grayscale': grayscale}

    def __init__(self, key, **params):
        super(Cycle, self).__init__(key=key, **params)
        self.values = self._get_values()


    def __getitem__(self, slc):
        """Provides a convenient interface to override the
        range and samples parameters of the Cycle.
        Supplying a slice step or index overrides the
        number of samples. Unsupplied slice values will be
        inherited.

        """
        (start, stop), step = self.range, self.samples
        if isinstance(slc, slice):
            if slc.start is not None:
                start = slc.start
            if slc.stop is not None:
                stop = slc.stop
            if slc.step is not None:
                step = slc.step
        else:
            step = slc
        return self(range=(start, stop), samples=step)


    def _get_values(self):
        cmap = self.colormaps[self.key]
        (start, stop), steps = self.range, self.samples
        samples = [cmap(n) for n in self.sample_fn(start, stop, steps)]
        return samples[::-1] if self.reverse else samples



class Options:
    """An Options object holds a collection of keyword options. In
    addition, Options support (optional) keyword validation as well as
    infinite indexing over the set of supplied cyclic values.

    Options support inheritance of setting values via the __call__
    method. By calling an Options object with additional keywords, you
    can create a new Options object inheriting the parent options.

    """

    # Whether all Options instances should skip invalid keywords or
    # raise and exception.
    skip_invalid = True

    # Whether all Options instances should generate warnings when
    # skipping over invalid keywords or not. May only be specified at
    # the class level.
    warn_on_skip = True

    _option_groups = ['style', 'plot', 'norm', 'output']

    _output_allowed_kws = ['backend']

    def __init__(self, key=None, allowed_keywords=None, merge_keywords=True,
                 max_cycles=None, **kwargs):

        if allowed_keywords is None:
            allowed_keywords = []
        invalid_kws = []
        for kwarg in sorted(kwargs.keys()):
            if allowed_keywords and kwarg not in allowed_keywords:
                if self.skip_invalid:
                    invalid_kws.append(kwarg)
                else:
                    raise OptionError(kwarg, allowed_keywords)

        if key and key[0].islower() and key not in self._option_groups:
            raise Exception('Key {} does not start with a capitalized element class name and is not a group in {}'.format(repr(key), ', '.join(repr(el) for el in self._option_groups)))

        for invalid_kw in invalid_kws:
            error = OptionError(invalid_kw, allowed_keywords, group_name=key)
            StoreOptions.record_skipped_option(error)
        if invalid_kws and self.warn_on_skip:
            self.param.warning(f"Invalid options {invalid_kws!r}, valid options are: {allowed_keywords!s}")

        self.kwargs = dict([(k,kwargs[k]) for k in sorted(kwargs.keys()) if k not in invalid_kws])
        self._options = []
        self._max_cycles = max_cycles

        allowed_keywords = (allowed_keywords if isinstance(allowed_keywords, Keywords)
                            else Keywords(allowed_keywords))
        self.allowed_keywords = allowed_keywords
        self.merge_keywords = merge_keywords
        self.key = key

    def keywords_target(self, target):
        """Helper method to easily set the target on the allowed_keywords Keywords.

        """
        self.allowed_keywords.target = target
        return self

    def filtered(self, allowed):
        """Return a new Options object that is filtered by the specified
        list of keys. Mutating self.kwargs to filter is unsafe due to
        the option expansion that occurs on initialization.

        """
        kws = {k:v for k,v in self.kwargs.items() if k in allowed}
        return self.__class__(key=self.key,
                              allowed_keywords=self.allowed_keywords,
                              merge_keywords=self.merge_keywords, **kws)


    def __call__(self, allowed_keywords=None, **kwargs):
        """Create a new Options object that inherits the parent options.

        """
        if 'key' not in kwargs:
            kwargs['key'] = self.key
        allowed_keywords=self.allowed_keywords if allowed_keywords in [None,[]] else allowed_keywords
        inherited_style = dict(allowed_keywords=allowed_keywords, **kwargs)
        return self.__class__(**dict(self.kwargs, **inherited_style))

    def keys(self):
        """The keyword names across the supplied options.

        """
        return sorted(list(self.kwargs.keys()))

    def max_cycles(self, num):
        """Truncates all contained Palette objects to a maximum number
        of samples and returns a new Options object containing the
        truncated or resampled Palettes.

        """
        kwargs = {kw: (arg[num] if isinstance(arg, Palette) else arg)
                  for kw, arg in self.kwargs.items()}
        return self(max_cycles=num, **kwargs)

    @property
    def cyclic(self):
        """Returns True if the options cycle, otherwise False

        """
        return any(isinstance(val, Cycle) for val in self.kwargs.values())

    def __getitem__(self, index):
        """Infinite cyclic indexing of options over the integers,
        looping over the set of defined Cycle objects.

        """
        if len(self.kwargs) == 0:
            return {}

        cycles = {k:v.values for k,v in self.kwargs.items() if isinstance(v, Cycle)}
        options = {}
        for key, values in cycles.items():
            options[key] = values[index % len(values)]

        static = {k:v for k,v in self.kwargs.items() if not isinstance(v, Cycle)}
        return dict(static, **options)

    @property
    def options(self):
        """Access of the options keywords when no cycles are defined.

        """
        if not self.cyclic:
            return self[0]
        else:
            raise Exception("The options property may only be used"
                            " with non-cyclic Options.")

    def __repr__(self):
        kws = ', '.join(f"{k}={self.kwargs[k]!r}" for k in sorted(self.kwargs.keys()))
        cls_name = type(self).__name__
        if self.key and self.key[0].isupper() and kws:
            return f"{cls_name}({self.key!r}, {kws})"
        elif self.key and self.key[0].isupper():
            return f"{cls_name}({self.key!r})"
        else:
            return f"{cls_name}({kws})"

    def __str__(self):
        return repr(self)



class OptionTree(AttrTree):
    """A subclass of AttrTree that is used to define the inheritance
    relationships between a collection of Options objects. Each node
    of the tree supports a group of Options objects and the leaf nodes
    inherit their keyword values from parent nodes up to the root.

    Supports the ability to search the tree for the closest valid path
    using the find method, or compute the appropriate Options value
    given an object and a mode. For a given node of the tree, the
    options method computes a Options object containing the result of
    inheritance for a given group up to the root of the tree.

    When constructing an OptionTree, you can specify the option groups
    as a list (i.e. empty initial option groups at the root) or as a
    dictionary (e.g. groups={'style':Option()}). You can also
    initialize the OptionTree with the options argument together with
    the **kwargs - see StoreOptions.merge_options for more information
    on the options specification syntax.

    You can use the string specifier '.' to refer to the root node in
    the options specification. This acts as an alternative was of
    specifying the options groups of the current node. Note that this
    approach method may only be used with the group lists format.

    """

    def __init__(self, items=None, identifier=None, parent=None,
                 groups=None, options=None, backend=None, **kwargs):

        if groups is None:
            raise ValueError('Please supply groups list or dictionary')
        _groups = {g:Options() for g in groups} if isinstance(groups, list) else groups

        self.__dict__['backend'] = backend
        self.__dict__['groups'] = _groups
        self.__dict__['_instantiated'] = False
        AttrTree.__init__(self, items, identifier, parent)
        self.__dict__['_instantiated'] = True

        options = StoreOptions.merge_options(_groups.keys(), options, **kwargs)
        root_groups = options.pop('.', None)
        if root_groups and isinstance(groups, list):
            self.__dict__['groups'] = {g:Options(**root_groups.get(g,{})) for g in _groups.keys()}
        elif root_groups:
            raise Exception(
                "Group specification as a dictionary only supported if "
                "the root node '.' syntax not used in the options."
            )
        if options:
            StoreOptions.apply_customizations(options, self)

    def _merge_options(self, identifier, group_name, options):
        """Computes a merged Options object for the given group
        name from the existing Options on the node and the
        new Options which are passed in.

        """
        if group_name not in self.groups:
            raise KeyError(f"Group {group_name} not defined on SettingTree.")

        if identifier in self.children:
            current_node = self[identifier]
            group_options = current_node.groups[group_name]
        else:
            # When creating a node (nothing to merge with) ensure it is empty
            group_options = Options(
                group_name, allowed_keywords=self.groups[group_name].allowed_keywords
            )

        override_kwargs = dict(options.kwargs)
        old_allowed = group_options.allowed_keywords
        override_kwargs['allowed_keywords'] = options.allowed_keywords + old_allowed

        try:
            if options.merge_keywords:
                return group_options(**override_kwargs)
            else:
                return Options(group_name, **override_kwargs)
        except OptionError as e:
            raise OptionError(e.invalid_keyword,
                              e.allowed_keywords,
                              group_name=group_name,
                              path = self.path) from e

    def __getitem__(self, item):
        if item in self.groups:
            return self.groups[item]
        return super().__getitem__(item)

    def __getattr__(self, identifier):
        """Allows creating sub OptionTree instances using attribute
        access, inheriting the group options.

        """
        try:
            return super(AttrTree, self).__getattr__(identifier)
        except AttributeError:
            pass

        if identifier.startswith('_'):
            raise AttributeError(str(identifier))
        elif self.fixed==True:
            raise AttributeError(self._fixed_error % identifier)

        valid_id = sanitize_identifier(identifier, escape=False)
        if valid_id in self.children:
            return self.__dict__[valid_id]

        # When creating a intermediate child node, leave kwargs empty
        self.__setattr__(identifier, {k:Options(k, allowed_keywords=v.allowed_keywords)
                                      for k,v in self.groups.items()})
        return self[identifier]


    def __setattr__(self, identifier, val):
        # Invalidate the lookup cache whenever an option is changed
        Store._lookup_cache[self.backend] = {}

        identifier = sanitize_identifier(identifier, escape=False)
        new_groups = {}
        if isinstance(val, dict):
            group_items = val
        elif isinstance(val, Options) and val.key is None:
            raise AttributeError(
                "Options object needs to have a group name specified."
            )
        elif isinstance(val, Options) and val.key[0].isupper():
            groups = ', '.join(repr(el) for el in Options._option_groups)
            raise AttributeError(
                f"OptionTree only accepts Options using keys that are one of {groups}."
            )
        elif isinstance(val, Options):
            group_items = {val.key: val}
        elif isinstance(val, OptionTree):
            group_items = val.groups

        current_node = self[identifier] if identifier in self.children else self
        for group_name in current_node.groups:
            options = group_items.get(group_name, False)
            if options:
                new_groups[group_name] = self._merge_options(identifier, group_name, options)
            else:
                new_groups[group_name] = current_node.groups[group_name]

        if new_groups:
            data = self[identifier].items() if identifier in self.children else None
            new_node = OptionTree(data, identifier=identifier, parent=self, groups=new_groups, backend=self.backend)
        else:
            raise ValueError('OptionTree only accepts a dictionary of Options.')

        super().__setattr__(identifier, new_node)

        if isinstance(val, OptionTree):
            for subtree in val:
                self[identifier].__setattr__(subtree.identifier, subtree)

    def find(self, path, mode='node'):
        """Find the closest node or path to an the arbitrary path that is
        supplied down the tree from the given node. The mode argument
        may be either 'node' or 'path' which determines the return
        type.

        """
        path = path.split('.') if isinstance(path, str) else list(path)
        item = self

        for child in path:
            escaped_child = sanitize_identifier(child, escape=False)
            matching_children = (c for c in item.children
                                 if child.endswith(c) or escaped_child.endswith(c))
            matching_children = sorted(matching_children, key=lambda x: -len(x))
            if matching_children:
                item = item[matching_children[0]]
            else:
                continue
        return item if mode == 'node' else item.path

    def closest(self, obj, group, defaults=True, backend=None):
        """This method is designed to be called from the root of the
        tree. Given any LabelledData object, this method will return
        the most appropriate Options object, including inheritance.

        In addition, closest supports custom options by checking the
        object

        """
        opts_spec = (
            obj.__class__.__name__,
            group_sanitizer(obj.group),
            label_sanitizer(obj.label)
        )
        # Try to get a cache hit in the backend lookup cache
        backend = backend or Store.current_backend
        cache = Store._lookup_cache.get(backend, {})
        cache_key = (*opts_spec, group, defaults, id(self.root))
        if cache_key in cache:
            return cache[cache_key]

        target = '.'.join(c for c in opts_spec if c)
        options = self.find(opts_spec).options(
            group, target=target, defaults=defaults, backend=backend)
        cache[cache_key] = options
        return options

    def options(self, group, target=None, defaults=True, backend=None):
        """Using inheritance up to the root, get the complete Options
        object for the given node and the specified group.

        """
        if target is None:
            target = self.path
        if self.groups.get(group, None) is None:
            return None
        options = Store.options(backend=backend)
        if self.parent is None and target and (self is not options) and defaults:
            root_name = self.__class__.__name__
            replacement = root_name + ('' if len(target) == len(root_name) else '.')
            option_key = target.replace(replacement, '')
            match = options.find(option_key)
            if match is not options:
                return match.options(group)
            else:
                return EMPTY_OPTIONS
        elif self.parent is None:
            return self.groups[group]

        parent_opts = self.parent.options(group, target, defaults, backend=backend)
        return Options(**dict(parent_opts.kwargs, **self.groups[group].kwargs))

    def __repr__(self):
        """Evalable representation of the OptionTree.

        """
        groups = self.__dict__['groups']
        # Tab and group entry separators
        tab, gsep = '   ', ',\n\n'
        # Entry separator and group specifications
        esep, gspecs = (",\n"+(tab*2)), []

        for group in groups.keys():
            especs, accumulator = [], []
            if groups[group].kwargs != {}:
                accumulator.append(('.', groups[group].kwargs))

            for t, v in sorted(self.items()):
                kwargs = v.groups[group].kwargs
                accumulator.append(('.'.join(t), kwargs))

            for (t, kws) in accumulator:
                if group=='norm' and all(kws.get(k, False) is False for k in ['axiswise','framewise']):
                    continue
                elif kws:
                    especs.append((t, kws))

            if especs:
                format_kws = [
                    (t, f"dict({', '.join(f'{k}={v}' for k, v in sorted(kws.items()))})")
                    for t, kws in especs
                ]
                ljust = max(len(t) for t,_ in format_kws)
                sep = (tab*2) if len(format_kws) >1 else ''
                entries = sep + esep.join([f'{sep}{t.ljust(ljust)} : {v}' for t,v in format_kws])
                gspecs.append(('%s%s={\n%s}' if len(format_kws)>1 else '%s%s={%s}') % (tab, group, entries))

        return f'OptionTree(groups={groups.keys()},\n{gsep.join(gspecs)}\n)'


EMPTY_OPTIONS = Options()


class Compositor(param.Parameterized):
    """A Compositor is a way of specifying an operation to be automatically
    applied to Overlays that match a specified pattern upon display.

    Any Operation that takes an Overlay as input may be used to define a
    compositor.

    For instance, a compositor may be defined to automatically display
    three overlaid monochrome matrices as an RGB image as long as the
    values names of those matrices match 'R', 'G' and 'B'.

    """

    mode = param.Selector(default='data',
                                objects=['data', 'display'], doc="""
      The mode of the Compositor object which may be either 'data' or
      'display'.""")

    backends = param.List(default=[], doc="""
      Defines which backends to apply the Compositor for.""")

    operation = param.Parameter(doc="""
       The Operation to apply when collapsing overlays.""")

    pattern = param.String(doc="""
       The overlay pattern to be processed. An overlay pattern is a
       sequence of elements specified by dotted paths separated by * .

       For instance the following pattern specifies three overlaid
       matrices with values of 'RedChannel', 'GreenChannel' and
       'BlueChannel' respectively:

      'Image.RedChannel * Image.GreenChannel * Image.BlueChannel.

      This pattern specification could then be associated with the RGB
      operation that returns a single RGB matrix for display.""")

    group = param.String(allow_None=True, doc="""
       The group identifier for the output of this particular compositor""")

    kwargs = param.Dict(doc="""
       Optional set of parameters to pass to the operation.""")

    transfer_options = param.Boolean(default=False, doc="""
       Whether to transfer the options from the input to the output.""")

    transfer_parameters = param.Boolean(default=False, doc="""
       Whether to transfer plot options which match to the operation.""")

    operations = []  # The operations that can be used to define compositors.
    definitions = [] # The set of all the compositor instances

    @classmethod
    def strongest_match(cls, overlay, mode, backend=None):
        """Returns the single strongest matching compositor operation
        given an overlay. If no matches are found, None is returned.

        The best match is defined as the compositor operation with the
        highest match value as returned by the match_level method.

        """
        match_strength = [
            (op.match_level(overlay), op) for op in cls.definitions
            if op.mode == mode and (not op.backends or backend in op.backends)
        ]
        matches = [
            (match[0], op, match[1]) for (match, op) in match_strength
            if match is not None
        ]
        if matches == []:
            return None
        return sorted(matches)[0]

    @classmethod
    def collapse_element(cls, overlay, ranges=None, mode='data', backend=None):
        """Finds any applicable compositor and applies it.

        """
        from .element import Element
        from .overlay import CompositeOverlay, Overlay
        unpack = False
        if not isinstance(overlay, CompositeOverlay):
            overlay = Overlay([overlay])
            unpack = True

        prev_ids = ()
        processed = defaultdict(list)
        while True:
            match = cls.strongest_match(overlay, mode, backend)
            if match is None:
                if unpack and len(overlay) == 1:
                    return overlay.values()[0]
                return overlay
            (_, applicable_op, (start, stop)) = match
            if isinstance(overlay, Overlay):
                values = overlay.values()
                sliced = Overlay(values[start:stop])
            else:
                values = overlay.items()
                sliced = overlay.clone(values[start:stop])
            items = sliced.traverse(lambda x: x, [Element])
            if applicable_op and all(el in processed[applicable_op] for el in items):
                if unpack and len(overlay) == 1:
                    return overlay.values()[0]
                return overlay
            result = applicable_op.apply(sliced, ranges, backend)
            if applicable_op.group:
                result = result.relabel(group=applicable_op.group)
            if isinstance(overlay, Overlay):
                result = [result]
            else:
                result = list(zip(sliced.keys(), [result], strict=None))
            processed[applicable_op] += [el for r in result for el in r.traverse(lambda x: x, [Element])]
            overlay = overlay.clone(values[:start]+result+values[stop:])

            # Guard against infinite recursion for no-ops
            spec_fn = lambda x: not isinstance(x, CompositeOverlay)
            new_ids = tuple(overlay.traverse(lambda x: id(x), [spec_fn]))
            if new_ids == prev_ids:
                return overlay
            prev_ids = new_ids

    @classmethod
    def collapse(cls, holomap, ranges=None, mode='data'):
        """Given a map of Overlays, apply all applicable compositors.

        """
        # No potential compositors
        if cls.definitions == []:
            return holomap

        # Apply compositors
        clone = holomap.clone(shared_data=False)
        data = zip(ranges[1], holomap.data.values(), strict=None) if ranges else holomap.data.items()
        for key, overlay in data:
            clone[key] = cls.collapse_element(overlay, ranges, mode)
        return clone

    @classmethod
    def map(cls, obj, mode='data', backend=None):
        """Applies compositor operations to any HoloViews element or container
        using the map method.

        """
        from .overlay import CompositeOverlay
        element_compositors = [c for c in cls.definitions if len(c._pattern_spec) == 1]
        overlay_compositors = [c for c in cls.definitions if len(c._pattern_spec) > 1]
        if overlay_compositors:
            obj = obj.map(lambda obj: cls.collapse_element(obj, mode=mode, backend=backend),
                          [CompositeOverlay])
        element_patterns = [c.pattern for c in element_compositors]
        if element_compositors and obj.traverse(lambda x: x, element_patterns):
            obj = obj.map(lambda obj: cls.collapse_element(obj, mode=mode, backend=backend),
                          element_patterns)
        return obj

    @classmethod
    def register(cls, compositor):
        defined_patterns = [op.pattern for op in cls.definitions]
        if compositor.pattern in defined_patterns:
            cls.definitions.pop(defined_patterns.index(compositor.pattern))
        cls.definitions.append(compositor)
        if compositor.operation not in cls.operations:
            cls.operations.append(compositor.operation)

    def __init__(self, pattern, operation, group, mode, transfer_options=False,
                 transfer_parameters=False, output_type=None, backends=None, **kwargs):
        self._pattern_spec, labels = [], []

        for path in pattern.split('*'):
            path_tuple = tuple(el.strip() for el in path.strip().split('.'))
            self._pattern_spec.append(path_tuple)

            if len(path_tuple) == 3:
                labels.append(path_tuple[2])

        if len(labels) > 1 and not all(l==labels[0] for l in labels):
            raise KeyError("Mismatched labels not allowed in compositor patterns")
        elif len(labels) == 1:
            self.label = labels[0]
        else:
            self.label = ''

        self._output_type = output_type
        super().__init__(group=group,
                         pattern=pattern,
                         operation=operation,
                         mode=mode,
                         backends=backends or [],
                         kwargs=kwargs,
                         transfer_options=transfer_options,
                         transfer_parameters=transfer_parameters)

    @property
    def output_type(self):
        """Returns the operation output_type unless explicitly overridden
        in the kwargs.

        """
        return self._output_type or self.operation.output_type

    def _slice_match_level(self, overlay_items):
        """Find the match strength for a list of overlay items that must
        be exactly the same length as the pattern specification.

        """
        level = 0
        for spec, el in zip(self._pattern_spec, overlay_items, strict=None):
            if spec[0] != type(el).__name__:
                return None
            level += 1      # Types match
            if len(spec) == 1: continue

            group = [el.group, group_sanitizer(el.group, escape=False)]
            if spec[1] in group: level += 1  # Values match
            else:                     return None

            if len(spec) == 3:
                group = [el.label, label_sanitizer(el.label, escape=False)]
                if (spec[2] in group):
                    level += 1  # Labels match
                else:
                    return None
        return level

    def match_level(self, overlay):
        """Given an overlay, return the match level and applicable slice
        of the overall overlay. The level an integer if there is a
        match or None if there is no match.

        The level integer is the number of matching components. Higher
        values indicate a stronger match.

        """
        slice_width = len(self._pattern_spec)
        if slice_width > len(overlay): return None

        # Check all the possible slices and return the best matching one
        best_lvl, match_slice = (0, None)
        for i in range(len(overlay)-slice_width+1):
            overlay_slice = overlay.values()[i:i+slice_width]
            lvl = self._slice_match_level(overlay_slice)
            if lvl is None: continue
            if lvl > best_lvl:
                best_lvl = lvl
                match_slice = (i, i+slice_width)

        return (best_lvl, match_slice) if best_lvl != 0 else None

    def apply(self, value, input_ranges, backend=None):
        """Apply the compositor on the input with the given input ranges.

        """
        from .overlay import CompositeOverlay
        if backend is None: backend = Store.current_backend
        kwargs = {k: v for k, v in self.kwargs.items() if k != 'output_type'}
        if isinstance(value, CompositeOverlay) and len(value) == 1:
            value = value.values()[0]
            if self.transfer_parameters:
                plot_opts = Store.lookup_options(backend, value, 'plot').kwargs
                kwargs.update({k: v for k, v in plot_opts.items()
                               if k in self.operation.param})

        transformed = self.operation(value, input_ranges=input_ranges, **kwargs)
        if self.transfer_options and value is not transformed:
            Store.transfer_options(value, transformed, backend)
        return transformed


class Store:
    """The Store is what links up HoloViews objects to their
    corresponding options and to the appropriate classes of the chosen
    backend (e.g. for rendering).

    In addition, Store supports pickle operations that automatically
    pickle and unpickle the corresponding options for a HoloViews
    object.

    """

    renderers = {} # The set of available Renderers across all backends.

    # A mapping from ViewableElement types to their corresponding plot
    # types grouped by the backend. Set using the register method.
    registry = {}

    # A list of formats to be published for display on the frontend (e.g
    # IPython Notebook or a GUI application)
    display_formats = ['html']

    # A mapping from Dimensioned type to display hook
    _display_hooks = defaultdict(dict)

    # Once register_plotting_classes is called, this OptionTree is
    # populated for the given backend.
    _options = {}

    # Weakrefs to record objects per id
    _weakrefs = {}
    _options_context = False

    # Backend option caches
    _lookup_cache = {}

    # A list of hooks to call after registering the plot and style options
    option_setters = []

    # A dictionary of custom OptionTree by custom object id by backend
    _custom_options = {'matplotlib': {}}
    load_counter_offset = None
    save_option_state = False

    current_backend = 'matplotlib'

    _backend_switch_hooks = []

    @classmethod
    def set_current_backend(cls, backend):
        """Use this method to set the backend to run the switch hooks

        """
        for hook in cls._backend_switch_hooks:
            hook(backend)
        cls.current_backend = backend

    @classmethod
    def options(cls, backend=None, val=None):
        backend = cls.current_backend if backend is None else backend
        if val is None:
            return cls._options[backend]
        else:
            cls._lookup_cache[backend] = {}
            cls._options[backend] = val

    @classmethod
    def loaded_backends(cls):
        """Returns a list of the backends that have been loaded, based on
        the available OptionTrees.

        """
        return sorted(cls._options.keys())

    @classmethod
    def custom_options(cls, val=None, backend=None):
        backend = cls.current_backend if backend is None else backend
        if val is None:
            return cls._custom_options[backend]
        else:
            cls._custom_options[backend] = val

    @classmethod
    def load(cls, filename):
        """Equivalent to pickle.load except that the HoloViews trees is
        restored appropriately.

        """
        cls.load_counter_offset = StoreOptions.id_offset()
        val = pickle.load(filename)
        cls.load_counter_offset = None
        return val

    @classmethod
    def loads(cls, pickle_string):
        """Equivalent to pickle.loads except that the HoloViews trees is
        restored appropriately.

        """
        cls.load_counter_offset = StoreOptions.id_offset()
        val = pickle.loads(pickle_string)
        cls.load_counter_offset = None
        return val

    @classmethod
    def dump(cls, obj, file, protocol=0):
        """Equivalent to pickle.dump except that the HoloViews option
        tree is saved appropriately.

        """
        cls.save_option_state = True
        pickle.dump(obj, file, protocol=protocol)
        cls.save_option_state = False

    @classmethod
    def dumps(cls, obj, protocol=0):
        """Equivalent to pickle.dumps except that the HoloViews option
        tree is saved appropriately.

        """
        cls.save_option_state = True
        val = pickle.dumps(obj, protocol=protocol)
        cls.save_option_state = False
        return val

    @classmethod
    def info(cls, obj, ansi=True, backend='matplotlib', visualization=True,
             recursive=False, pattern=None, elements=None):
        """Show information about a particular object or component class
        including the applicable style and plot options. Returns None if
        the object is not parameterized.

        """
        if elements is None:
            elements = []
        parameterized_object = isinstance(obj, param.Parameterized)
        parameterized_class = (isinstance(obj,type)
                               and  issubclass(obj,param.Parameterized))
        info = None
        if InfoPrinter.store and (parameterized_object or parameterized_class):
            info = InfoPrinter.info(obj, ansi=ansi, backend=backend,
                                    visualization=visualization,
                                    pattern=pattern, elements=elements)

        if parameterized_object and recursive:
            hierarchy = obj.traverse(lambda x: type(x))
            listed = []
            for c in hierarchy[1:]:
                if c not in listed:
                    inner_info = InfoPrinter.info(c, ansi=ansi, backend=backend,
                                                  visualization=visualization,
                                                  pattern=pattern)
                    black = '\x1b[1;30m%s\x1b[0m' if ansi else '%s'
                    info +=  '\n\n' + (black % inner_info)
                    listed.append(c)
        return info

    @classmethod
    def lookup_options(cls, backend, obj, group, defaults=True):
        # Current custom_options dict may not have entry for obj.id
        if obj.id in cls._custom_options[backend]:
            return cls._custom_options[backend][obj.id].closest(
                obj, group, defaults, backend=backend)
        elif not defaults:
            return Options()
        else:
            return cls._options[backend].closest(obj, group, defaults, backend=backend)

    @classmethod
    def lookup(cls, backend, obj):
        """Given an object, lookup the corresponding customized option
        tree if a single custom tree is applicable.

        """
        ids = {el for el in obj.traverse(lambda x: x.id) if el is not None}
        if len(ids) == 0:
            raise Exception("Object does not own a custom options tree")
        elif len(ids) != 1:
            idlist = ",".join([str(el) for el in sorted(ids)])
            raise Exception("Object contains elements combined across "
                            f"multiple custom trees (ids {idlist})")
        return cls._custom_options[backend][next(iter(ids))]

    @classmethod
    def transfer_options(cls, obj, new_obj, backend=None, names=None, level=3):
        """Transfers options for all backends from one object to another.
        Drops any options defined in the supplied drop list.

        """
        if obj is new_obj:
            return
        backend = cls.current_backend if backend is None else backend
        type_name = type(new_obj).__name__
        group = type_name if obj.group == type(obj).__name__ else obj.group
        spec = '.'.join([s for s in (type_name, group, obj.label)[:level] if s])
        options = []
        for group in Options._option_groups:
            opts = cls.lookup_options(backend, obj, group)
            if not opts:
                continue
            new_opts = cls.lookup_options(backend, new_obj, group, defaults=False)
            existing = new_opts.kwargs if new_opts else {}
            filtered = {k: v for k, v in opts.kwargs.items()
                        if (names is None or k in names) and k not in existing}
            if filtered:
                options.append(Options(group, **filtered))
        if options:
            StoreOptions.set_options(new_obj, {spec: options}, backend)

    @classmethod
    def add_style_opts(cls, component, new_options, backend=None):
        """Given a component such as an Element (e.g. Image, Curve) or a
        container (e.g. Layout) specify new style options to be
        accepted by the corresponding plotting class.

        Note : This is supplied for advanced users who know which
        additional style keywords are appropriate for the
        corresponding plotting class.

        """
        backend = cls.current_backend if backend is None else backend
        if component not in cls.registry[backend]:
            raise ValueError(
                f"Component {component!r} not registered to a plotting class."
            )

        if not isinstance(new_options, list) or not all(isinstance(el, str) for el in new_options):
            raise ValueError(
                "Please supply a list of style option keyword strings"
            )

        with param.logging_level('CRITICAL'):
            for option in new_options:
                if option not in cls.registry[backend][component].style_opts:
                    plot_class = cls.registry[backend][component]
                    plot_class.style_opts = sorted([*plot_class.style_opts, option])
        cls._options[backend][component.name] = Options(
            'style', merge_keywords=True, allowed_keywords=new_options
        )

    @classmethod
    def register(cls, associations, backend, style_aliases=None):
        """Register the supplied dictionary of associations between
        elements and plotting classes to the specified backend.

        """
        if style_aliases is None:
            style_aliases = {}
        if backend not in cls.registry:
            cls.registry[backend] = {}
        cls.registry[backend].update(associations)

        groups = Options._option_groups
        if backend not in cls._options:
            cls._options[backend] = OptionTree([], groups=groups, backend=backend)
        if backend not in cls._custom_options:
            cls._custom_options[backend] = {}

        for view_class, plot in cls.registry[backend].items():
            expanded_opts = [opt for key in plot.style_opts
                             for opt in style_aliases.get(key, [])]
            style_opts = sorted({opt for opt in (expanded_opts + plot.style_opts)
                                    if opt not in plot._disabled_opts})

            # Special handling for PlotSelector which just proxies parameters
            params = list(plot.param) if hasattr(plot, 'param') else plot.params()
            plot_opts = [k for k in params if k not in ['name']]

            with param.logging_level('CRITICAL'):
                plot.style_opts = style_opts

            plot_opts =  Keywords(plot_opts,  target=view_class.__name__)
            style_opts = Keywords(style_opts, target=view_class.__name__)

            opt_groups = {
                'plot':   Options(allowed_keywords=plot_opts),
                'output': Options(allowed_keywords=Options._output_allowed_kws),
                'style':  Options(allowed_keywords=style_opts),
                'norm':   Options(framewise=False, axiswise=False,
                                  allowed_keywords=['framewise', 'axiswise'])
            }

            name = view_class.__name__
            cls._options[backend][name] = opt_groups

    @classmethod
    def set_display_hook(cls, group, objtype, hook):
        """Specify a display hook that will be applied to objects of type
        objtype. The group specifies the set to which the display hook
        belongs, allowing the Store to compute the precedence within
        each group.

        """
        cls._display_hooks[group][objtype] = hook

    @classmethod
    def render(cls, obj):
        """Using any display hooks that have been registered, render the
        object to a dictionary of MIME types and metadata information.

        """
        class_hierarchy = inspect.getmro(type(obj))
        hooks = []
        for _, type_hooks in cls._display_hooks.items():
            for subcls in class_hierarchy:
                if subcls in type_hooks:
                    hooks.append(type_hooks[subcls])
                    break

        data, metadata = {}, {}
        for hook in hooks:
            ret = hook(obj)
            if ret is None:
                continue
            d, md = ret
            data.update(d)
            metadata.update(md)
        return data, metadata


class StoreOptions:
    """A collection of utilities for advanced users for creating and
    setting customized option trees on the Store. Designed for use by
    either advanced users or the %opts line and cell magics which use
    this machinery.

    This class also holds general classmethods for working with
    OptionTree instances: as OptionTrees are designed for attribute
    access it is best to minimize the number of methods implemented on
    that class and implement the necessary utilities on StoreOptions
    instead.

    Lastly this class offers a means to record all OptionErrors
    generated by an option specification. This is used for validation
    purposes.

    """

    #=======================#
    # OptionError recording #
    #=======================#

    _errors_recorded = None

    @classmethod
    def start_recording_skipped(cls):
        """Start collecting OptionErrors for all skipped options recorded
        with the record_skipped_option method

        """
        cls._errors_recorded = []

    @classmethod
    def stop_recording_skipped(cls):
        """Stop collecting OptionErrors recorded with the
        record_skipped_option method and return them

        """
        if cls._errors_recorded is None:
            raise Exception('Cannot stop recording before it is started')
        recorded = cls._errors_recorded[:]
        cls._errors_recorded = None
        return recorded

    @classmethod
    def record_skipped_option(cls, error):
        """Record the OptionError associated with a skipped option if
        currently recording

        """
        if cls._errors_recorded is not None:
            cls._errors_recorded.append(error)

    #===============#
    # ID management #
    #===============#

    @classmethod
    def get_object_ids(cls, obj):
        return {el for el
                   in obj.traverse(lambda x: getattr(x, 'id', None))}

    @classmethod
    def tree_to_dict(cls, tree):
        """Given an OptionTree, convert it into the equivalent dictionary format.

        """
        specs = {}
        for k in tree.keys():
            spec_key = '.'.join(k)
            specs[spec_key] = {}
            for grp in tree[k].groups:
                kwargs = tree[k].groups[grp].kwargs
                if kwargs:
                    specs[spec_key][grp] = kwargs
        return specs

    @classmethod
    def propagate_ids(cls, obj, match_id, new_id, applied_keys, backend=None):
        """Recursively propagate an id through an object for components
        matching the applied_keys. This method can only be called if
        there is a tree with a matching id in Store.custom_options

        """
        applied = []
        def propagate(o):
            if o.id == match_id or (o.__class__.__name__ == 'DynamicMap'):
                o.id = new_id
                applied.append(o)
        obj.traverse(propagate, specs=set(applied_keys) | {'DynamicMap'})

        # Clean up the custom tree if it was not applied
        if new_id not in Store.custom_options(backend=backend):
            raise AssertionError(f"New option id {new_id} does not match any "
                                 "option trees in Store.custom_options.")
        return applied

    @classmethod
    def capture_ids(cls, obj):
        """Given an object, capture a list of ids that can be
        restored using the restore_ids.

        """
        return obj.traverse(lambda o: o.id)

    @classmethod
    def restore_ids(cls, obj, ids):
        """Given an list of ids as captured with capture_ids, restore the
        ids. Note the structure of an object must not change between
        the calls to capture_ids and restore_ids.

        """
        ids = iter(ids)
        obj.traverse(lambda o: setattr(o, 'id', next(ids)))

    @classmethod
    def apply_customizations(cls, spec, options):
        """Apply the given option specs to the supplied options tree.

        """
        for key in sorted(spec.keys()):
            if isinstance(spec[key], (list, tuple)):
                customization = {v.key: v for v in spec[key]}
            else:
                customization = {k: (Options(**v) if isinstance(v, dict) else v)
                                 for k,v in spec[key].items()}

            # Set the Keywords target on Options from the {type} part of the key.
            customization = {k: v.keywords_target(key.split('.')[0])
                             for k,v in customization.items()}
            options[str(key)] = customization
        return options

    @classmethod
    def validate_spec(cls, spec, backends=None):
        """Given a specification, validated it against the options tree for
        the specified backends by raising OptionError for invalid
        options. If backends is None, validates against all the
        currently loaded backend.

        Only useful when invalid keywords generate exceptions instead of
        skipping, i.e. Options.skip_invalid is False.

        """
        loaded_backends = Store.loaded_backends() if backends is None else backends

        error_info     = {}
        backend_errors = defaultdict(set)
        for backend in loaded_backends:
            cls.start_recording_skipped()
            with options_policy(skip_invalid=True, warn_on_skip=False):
                options = OptionTree(
                    items=Store.options(backend).data.items(),
                    groups=Store.options(backend).groups,
                    backend=backend
                )
                cls.apply_customizations(spec, options)

            for error in cls.stop_recording_skipped():
                error_key = (error.invalid_keyword,
                             error.allowed_keywords.target,
                             error.group_name)
                error_info[(*error_key, backend)] = error.allowed_keywords
                backend_errors[error_key].add(backend)

        for ((keyword, target, group_name), backend_error) in backend_errors.items():
            # If the keyword failed for the target across all loaded backends...
            if set(backend_error) == set(loaded_backends):
                key = (keyword, target, group_name, Store.current_backend)
                raise OptionError(keyword,
                                  group_name=group_name,
                                  allowed_keywords=error_info[key])


    @classmethod
    def validation_error_message(cls, spec, backends=None):
        """Returns an options validation error message if there are any
        invalid keywords. Otherwise returns None.

        """
        try:
            cls.validate_spec(spec, backends=backends)
        except OptionError as e:
            return e.format_options_error()

    @classmethod
    def expand_compositor_keys(cls, spec):
        """Expands compositor definition keys into {type}.{group}
        keys. For instance a compositor operation returning a group
        string 'Image' of element type RGB expands to 'RGB.Image'.

        """
        expanded_spec = {}
        applied_keys = []
        compositor_defs = {
            el.group: el.output_type.__name__ for el in Compositor.definitions
        }
        for key, val in spec.items():
            if key not in compositor_defs:
                expanded_spec[key] = val
            else:
                # Send id to Overlays
                applied_keys = ['Overlay']
                type_name = compositor_defs[key]
                expanded_spec[str(type_name+'.'+key)] = val
        return expanded_spec, applied_keys

    @classmethod
    def create_custom_trees(cls, obj, options=None, backend=None):
        """Returns the appropriate set of customized subtree clones for
        an object, suitable for merging with Store.custom_options (i.e
        with the ids appropriately offset). Note if an object has no
        integer ids a new OptionTree is built.

        The id_mapping return value is a list mapping the ids that
        need to be matched as set to their new values.

        """
        clones, id_mapping = {}, []
        obj_ids = cls.get_object_ids(obj)
        offset = cls.id_offset()
        obj_ids = [None] if len(obj_ids) == 0 else obj_ids

        used_obj_types = [(opt.split('.')[0],) for opt in options]
        backend = backend or Store.current_backend
        available_options = Store.options(backend=backend)
        used_options = {}
        for obj_type in available_options:
            if obj_type in used_obj_types:
                opts_groups = available_options[obj_type].groups
                used_options[obj_type] = {
                    grp: Options(
                        allowed_keywords=opt.allowed_keywords,
                        backend=backend,
                    )
                    for (grp, opt) in opts_groups.items()
                }

        custom_options = Store.custom_options(backend=backend)
        for tree_id in obj_ids:
            if tree_id is not None and tree_id in custom_options:
                original = custom_options[tree_id]
                clone = OptionTree(
                    items=original.items(),
                    groups=original.groups,
                    backend=original.backend
                )
                clones[tree_id + offset + 1] = clone
                id_mapping.append((tree_id, tree_id + offset + 1))
            else:
                clone = OptionTree(groups=available_options.groups,
                                   backend=backend)
                clones[offset] = clone
                id_mapping.append((tree_id, offset))

            # Nodes needed to ensure allowed_keywords is respected
            for obj_type, opts in used_options.items():
                clone[obj_type] = opts

        return {k: cls.apply_customizations(options, t) if options else t
                for k,t in clones.items()}, id_mapping

    @classmethod
    def merge_options(cls, groups, options=None,**kwargs):
        """Given a full options dictionary and options groups specified
        as a keywords, return the full set of merged options:

        >>> options={'Curve':{'style':dict(color='b')}}
        >>> style={'Curve':{'linewidth':10 }}
        >>> merged = StoreOptions.merge_options(['style'], options, style=style)
        >>> sorted(merged['Curve']['style'].items())
        [('color', 'b'), ('linewidth', 10)]
        """
        groups = set(groups)
        if (options is not None and set(options.keys()) <= groups):
            kwargs, options = options, None
        elif (options is not None and any(k in groups for k in options)):
              raise Exception(
                  f"All keys must be a subset of {', '.join(groups)}."
              )

        options = {} if (options is None) else dict(**options)
        all_keys = {k for d in kwargs.values() for k in d}
        for spec_key in all_keys:
            additions = {}
            for k, d in kwargs.items():
                if spec_key in d:
                    kws = d[spec_key]
                    additions.update({k:kws})
            if spec_key not in options:
                options[spec_key] = {}
            for key, value in additions.items():
                if key in options[spec_key]:
                    options[spec_key][key].update(value)
                else:
                    options[spec_key][key] = value
        return options


    @classmethod
    def state(cls, obj, state=None):
        """Method to capture and restore option state. When called
        without any state supplied, the current state is
        returned. Then if this state is supplied back in a later call
        using the same object, the original state is restored.

        """
        if state is None:
            ids = cls.capture_ids(obj)
            original_custom_keys = set(Store.custom_options().keys())
            return (ids, original_custom_keys)
        else:
            (ids, original_custom_keys) = state
            current_custom_keys = set(Store.custom_options().keys())
            for key in current_custom_keys.difference(original_custom_keys):
                del Store.custom_options()[key]
                cls.restore_ids(obj, ids)

    @classmethod
    @contextmanager
    def options(cls, obj, options=None, **kwargs):
        """Context-manager for temporarily setting options on an object
        (if options is None, no options will be set) . Once the
        context manager exits, both the object and the Store will be
        left in exactly the same state they were in before the context
        manager was used.

        See holoviews.core.options.set_options function for more
        information on the options specification format.

        """
        if (options is not None) or kwargs:
            Store._options_context = True
            optstate = cls.state(obj)
            groups = Store.options().groups.keys()
            options = cls.merge_options(groups, options, **kwargs)
            cls.set_options(obj, options)

        try:
            yield
        finally:
            if options is not None:
                Store._options_context = True
                cls.state(obj, state=optstate)

    @classmethod
    def id_offset(cls):
        """Compute an appropriate offset for future id values given the set
        of ids currently defined across backends.

        """
        max_ids = []
        for backend in Store.renderers.keys():
            # Ensure store_ids is immediately cast to list to avoid a
            # race condition (#5533)
            store_ids = list(Store.custom_options(backend=backend).keys())
            max_id = max(store_ids)+1 if len(store_ids) > 0 else 0
            max_ids.append(max_id)
        # If no backends defined (e.g. plotting not imported) return zero
        return max(max_ids) if max_ids else 0

    @classmethod
    def update_backends(cls, id_mapping, custom_trees, backend=None):
        """Given the id_mapping from previous ids to new ids and the new
        custom tree dictionary, update the current backend with the
        supplied trees and update the keys in the remaining backends to
        stay linked with the current object.

        """
        backend = Store.current_backend if backend is None else backend
        # Update the custom option entries for the current backend
        Store.custom_options(backend=backend).update(custom_trees)
        Store._lookup_cache[backend] = {}

        # Propagate option ids for non-selected backends
        for b in Store.loaded_backends():
            if b == backend:
                continue
            backend_trees = Store._custom_options[b]
            for (old_id, new_id) in id_mapping:
                tree = backend_trees.get(old_id, None)
                if tree is not None:
                    backend_trees[new_id] = tree

    @classmethod
    def set_options(cls, obj, options=None, backend=None, **kwargs):
        """Pure Python function for customize HoloViews objects in terms of
        their style, plot and normalization options.

        The options specification is a dictionary containing the target
        for customization as a {type}.{group}.{label} keys. An example of
        such a key is 'Image' which would customize all Image components
        in the object. The key 'Image.Channel' would only customize Images
        in the object that have the group 'Channel'.

        The corresponding value is then a list of Option objects specified
        with an appropriate category ('plot', 'style' or 'norm'). For
        instance, using the keys described above, the specs could be:

        {'Image:[Options('style', cmap='jet')]}

        Or setting two types of option at once:

        {'Image.Channel':[Options('plot', size=50),
                          Options('style', cmap='Blues')]}


        Notes
        -----
        Relationship to the %%opts magic:

        This function matches the functionality supplied by the %%opts
        cell magic in the IPython extension. In fact, you can use the same
        syntax as the IPython cell magic to achieve the same customization
        as shown above:

        from holoviews.util.parser import OptsSpec
        set_options(my_image, OptsSpec.parse("Image (cmap='jet')"))

        Then setting both plot and style options:

        set_options(my_image, OptsSpec.parse("Image [size=50] (cmap='Blues')"))

        """
        # Note that an alternate, more verbose and less recommended
        # syntax can also be used:

        # {'Image.Channel:{'plot':  Options(size=50),
        #                  'style': Options('style', cmap='Blues')]}
        groups = Store.options(backend=backend).groups.keys()
        options = cls.merge_options(groups, options, **kwargs)
        spec, compositor_applied = cls.expand_compositor_keys(options)
        custom_trees, id_mapping = cls.create_custom_trees(obj, spec, backend=backend)
        cls.update_backends(id_mapping, custom_trees, backend=backend)

        # Propagate ids to the objects
        not_used = []
        for (match_id, new_id) in id_mapping:
            key = compositor_applied+list(spec.keys())
            applied = cls.propagate_ids(
                obj, match_id, new_id, key, backend=backend
            )
            if not applied:
                not_used.append(new_id)

        # Clean up unused custom option trees
        for new_id in set(not_used):
            cleanup_custom_options(new_id)

        return obj
