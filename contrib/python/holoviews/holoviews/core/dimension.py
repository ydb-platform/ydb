"""Provides Dimension objects for tracking the properties of a value,
axis or map dimension. Also supplies the Dimensioned abstract
baseclass for classes that accept Dimension values.

"""
from __future__ import annotations

import builtins
import datetime as dt
import re
import weakref
from collections import Counter, defaultdict
from collections.abc import Iterable
from functools import partial
from itertools import chain
from operator import itemgetter

import numpy as np
import param

from . import util
from .accessors import Apply, Opts, Redim
from .options import Options, Store, cleanup_custom_options
from .pprint import PrettyPrinter
from .tree import AttrTree
from .util import bytes_to_unicode

# Alias parameter support for pickle loading

ALIASES = {'key_dimensions': 'kdims', 'value_dimensions': 'vdims',
           'constant_dimensions': 'cdims'}

title_format = "{name}: {val}{unit}"

redim = Redim # pickle compatibility - remove in 2.0

def param_aliases(d):
    """Called from __setstate__ in LabelledData in order to load
    old pickles with outdated parameter names.

    Warnings
    --------
    We want to keep pickle hacking to a minimum!
    """
    for old, new in ALIASES.items():
        old_param = f'_{old}_param_value'
        new_param = f'_{new}_param_value'
        if old_param in d:
            d[new_param] = d.pop(old_param)
    return d


def asdim(dimension):
    """Convert the input to a Dimension.

    Parameters
    ----------
    dimension : tuple, dict or string type to convert to Dimension

    Returns
    -------
    A Dimension object constructed from the dimension spec. No
    copy is performed if the input is already a Dimension.
    """
    return dimension if isinstance(dimension, Dimension) else Dimension(dimension)


def dimension_name(dimension):
    """Return the Dimension.name for a dimension-like object.

    Parameters
    ----------
    dimension : Dimension or dimension string, tuple or dict

    Returns
    -------
    The name of the Dimension or what would be the name if the
    input as converted to a Dimension.
    """
    if isinstance(dimension, Dimension):
        return dimension.name
    elif isinstance(dimension, str):
        return dimension
    elif isinstance(dimension, tuple):
        return dimension[0]
    elif isinstance(dimension, dict):
        return dimension['name']
    elif dimension is None:
        return None
    else:
        raise ValueError(f'{type(dimension).__name__} type could not be interpreted as Dimension. '
                         'Dimensions must be declared as a string, tuple, '
                         'dictionary or Dimension type.')


def process_dimensions(kdims, vdims):
    """Converts kdims and vdims to Dimension objects.

    Parameters
    ----------
    kdims : List or single key dimension(s) specified as strings,
        tuples dicts or Dimension objects.
    vdims : List or single value dimension(s) specified as strings,
        tuples dicts or Dimension objects.

    Returns
    -------
    Dictionary containing kdims and vdims converted to Dimension
    objects
        {'kdims': [Dimension('x')], 'vdims': [Dimension('y')]
    """
    dimensions = {}
    for group, dims in [('kdims', kdims), ('vdims', vdims)]:
        if dims is None:
            continue
        elif isinstance(dims, (tuple, str, Dimension, dict)):
            dims = [dims]
        elif not isinstance(dims, list):
            raise ValueError(
                f"{group} argument expects a Dimension or list of dimensions, "
                "specified as tuples, strings, dictionaries or Dimension "
                f"instances, not a {type(dims).__name__} type. "
                "Ensure you passed the data as the first argument."
            )
        dimensions[group] = [asdim(d) for d in dims]
    return dimensions



class Dimension(param.Parameterized):
    """Dimension objects are used to specify some important general
    features that may be associated with a collection of values.

    For instance, a Dimension may specify that a set of numeric values
    actually correspond to 'Height' (dimension name), in units of
    meters, with a descriptive label 'Height of adult males'.

    All dimensions object have a name that identifies them and a label
    containing a suitable description. If the label is not explicitly
    specified it matches the name.

    These two parameters define the core identity of the dimension
    object and must match if two dimension objects are to be considered
    equivalent. All other parameters are considered optional metadata
    and are not used when testing for equality.

    Unlike all the other parameters, these core parameters can be used
    to construct a Dimension object from a tuple. This format is
    sufficient to define an identical Dimension:

    Dimension('a', label='Dimension A') == Dimension(('a', 'Dimension A'))

    Everything else about a dimension is considered to reflect
    non-semantic preferences. Examples include the default value (which
    may be used in a visualization to set an initial slider position),
    how the value is to rendered as text (which may be used to specify
    the printed floating point precision) or a suitable range of values
    to consider for a particular analysis.

    Notes
    -----
    Full unit support with automated conversions are on the HoloViews
    roadmap. Once rich unit objects are supported, the unit (or more
    specifically the type of unit) will be part of the core dimension
    specification used to establish equality.

    Until this feature is implemented, there are two auxiliary
    parameters that hold some partial information about the unit: the
    name of the unit and whether or not it is cyclic. The name of the
    unit is used as part of the pretty-printed representation and
    knowing whether it is cyclic is important for certain operations.

    """

    name = param.String(doc="""
       Short name associated with the Dimension, such as 'height' or
       'weight'. Valid Python identifiers make good names, because they
       can be used conveniently as a keyword in many contexts.""")

    label = param.String(default=None, doc="""
        Unrestricted label used to describe the dimension. A label
        should succinctly describe the dimension and may contain any
        characters, including Unicode and LaTeX expression.""")

    cyclic = param.Boolean(default=False, doc="""
        Whether the range of this feature is cyclic such that the
        maximum allowed value (defined by the range parameter) is
        continuous with the minimum allowed value.""")

    default = param.Parameter(default=None, doc="""
        Default value of the Dimension which may be useful for widget
        or other situations that require an initial or default value.""")

    nodata = param.Integer(default=None, doc="""
        Optional missing-data value for integer data.
        If non-None, data with this value will be replaced with NaN.""")

    range = param.Tuple(default=(None, None), doc="""
        Specifies the minimum and maximum allowed values for a
        Dimension. None is used to represent an unlimited bound.""")

    soft_range = param.Tuple(default=(None, None), doc="""
        Specifies a minimum and maximum reference value, which
        may be overridden by the data.""")

    step = param.Number(default=None, doc="""
        Optional floating point step specifying how frequently the
        underlying space should be sampled. May be used to define a
        discrete sampling over the range.""")

    type = param.Parameter(default=None, doc="""
        Optional type associated with the Dimension values. The type
        may be an inbuilt constructor (such as int, str, float) or a
        custom class object.""")

    unit = param.String(default=None, allow_None=True, doc="""
        Optional unit string associated with the Dimension. For
        instance, the string 'm' may be used represent units of meters
        and 's' to represent units of seconds.""")

    value_format = param.Callable(default=None, doc="""
        Formatting function applied to each value before display.""")

    values = param.List(default=[], doc="""
        Optional specification of the allowed value set for the
        dimension that may also be used to retain a categorical
        ordering.""")

    # Defines default formatting by type
    type_formatters = {}
    unit_format = ' ({unit})'
    presets = {} # A dictionary-like mapping name, (name,) or
                 # (name, unit) to a preset Dimension object

    def __init__(self, spec, **params):
        """Initializes the Dimension object with the given name.

        """
        if 'name' in params:
            raise KeyError('Dimension name must only be passed as the positional argument')

        all_params = {}
        if isinstance(spec, Dimension):
            all_params.update(spec.param.values())
        elif isinstance(spec, str):
            if (spec, params.get('unit', None)) in self.presets.keys():
                preset = self.presets[(str(spec), str(params['unit']))]
                all_params.update(preset.param.values())
            elif spec in self.presets:
                all_params.update(self.presets[spec].param.values())
            elif (spec,) in self.presets:
                all_params.update(self.presets[(spec,)].param.values())
            all_params['name'] = spec
            all_params['label'] = spec
        elif isinstance(spec, tuple):
            try:
                all_params['name'], all_params['label'] = spec
            except ValueError as exc:
                raise ValueError(
                    "Dimensions specified as a tuple must be a tuple "
                    f"consisting of the name and label not: {spec}"
                ) from exc
            if 'label' in params and params['label'] != all_params['label']:
                self.param.warning(
                    f'Using label as supplied by keyword ({params["label"]!r}), '
                    f'ignoring tuple value {all_params["label"]!r}')
        elif isinstance(spec, dict):
            all_params.update(spec)
            try:
                all_params.setdefault('label', spec['name'])
            except KeyError as exc:
                raise ValueError(
                    'Dimension specified as a dict must contain a "name" key'
                ) from exc
        else:
            raise ValueError(
                f'{type(spec).__name__} type could not be interpreted as Dimension.  Dimensions must be '
                'declared as a string, tuple, dictionary or Dimension type.'
            )
        all_params.update(params)

        if not all_params['name']:
            raise ValueError('Dimension name cannot be empty')
        if not all_params['label']:
            raise ValueError('Dimension label cannot be empty')

        values = params.get('values', [])
        if isinstance(values, str) and values == 'initial':
            self.param.warning("The 'initial' string for dimension values "
                               "is no longer supported.")
            values = []

        all_params['values'] = list(util.unique_array(values))
        super().__init__(**all_params)
        if self.default is not None:
            if self.values and self.default not in values:
                raise ValueError(f'{self!r} default {self.default} not found in declared values: {self.values}')
            elif (self.range != (None, None) and
                  ((self.range[0] is not None and self.default < self.range[0]) or
                   (self.range[0] is not None and self.default > self.range[1]))):
                raise ValueError(f'{self!r} default {self.default} not in declared range: {self.range}')

    @property
    def spec(self):
        """"Returns the Dimensions tuple specification

        Returns
        -------
        tuple : Dimension tuple specification
        """
        return (self.name, self.label)

    def clone(self, spec=None, **overrides):
        """Clones the Dimension with new parameters

        Derive a new Dimension that inherits existing parameters
        except for the supplied, explicit overrides

        Parameters
        ----------
        spec : tuple, optional
            Dimension tuple specification
        **overrides: Dimension parameter overrides

        Returns
        -------
        Cloned Dimension object
        """
        settings = dict(self.param.values(), **overrides)

        if spec is None:
            spec = (self.name, overrides.get('label', self.label))
        if 'label' in overrides and isinstance(spec, str) :
            spec = (spec, overrides['label'])
        elif 'label' in overrides and isinstance(spec, tuple) :
            if overrides['label'] != spec[1]:
                self.param.warning(
                    f'Using label as supplied by keyword ({overrides["label"]!r}), '
                    f'ignoring tuple value {spec[1]!r}')
            spec = (spec[0],  overrides['label'])
        return self.__class__(spec, **{k:v for k,v in settings.items()
                                       if k not in ['name', 'label']})

    def __hash__(self):
        """Hashes object on Dimension spec, i.e. (name, label).
        """
        return hash(self.spec)

    def __setstate__(self, d):
        """Compatibility for pickles before alias attribute was introduced.

        """
        super().__setstate__(d)
        if '_label_param_value' not in d:
            self.label = self.name

    def __eq__(self, other):
        """Implements equals operator including sanitized comparison.

        """
        if isinstance(other, Dimension):
            return self.label == other.label

        # For comparison to strings. Name may be sanitized.
        return other in [self.name, self.label, util.dimension_sanitizer(self.name)]

    def __ne__(self, other):
        """Implements not equal operator including sanitized comparison.

        """
        return not self.__eq__(other)

    def __lt__(self, other):
        """Dimensions are sorted alphanumerically by name

        """
        return self.name < other.name if isinstance(other, Dimension) else self.name < other

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.pprint()

    @property
    def pprint_label(self):
        """The pretty-printed label string for the Dimension

        """
        unit = ('' if self.unit is None
                else type(self.unit)(self.unit_format).format(unit=self.unit))
        return bytes_to_unicode(self.label) + bytes_to_unicode(unit)

    def pprint(self):
        changed = self.param.values(onlychanged=True)
        if len({changed.get(k, k) for k in ['name','label']}) == 1:
            return f'Dimension({self.name!r})'

        params = self.param.objects('existing')
        ordering = sorted(
            sorted(changed.keys()), key=lambda k: (
                -float('inf') if params[k].precedence is None
                else params[k].precedence))
        kws = ", ".join(f'{k}={changed[k]!r}' for k in ordering if k != 'name')
        return f'Dimension({self.name!r}, {kws})'

    def _get_type_formatters(self, own_type):
        """_get_type_formatters returns the formatter for the type of the value.

        It first checks if the type itself is in self.type_formatters and if
        not, it checks if the qualified name of the type type is in
        self.type_formatters.
        """
        if own_type in self.type_formatters:
            return self.type_formatters[own_type]

        own_type_str = f"{own_type.__module__}.{own_type.__qualname__}"
        if own_type_str in self.type_formatters:
            return self.type_formatters[own_type_str]

    def pprint_value(self, value, print_unit=False):
        """Applies the applicable formatter to the value.

        Parameters
        ----------
        value
            Dimension value to format

        Returns
        -------
        Formatted dimension value
        """
        own_type = type(value) if self.type is None else self.type
        formatter = (self.value_format if self.value_format
                     else self._get_type_formatters(own_type))
        if formatter:
            if callable(formatter):
                formatted_value = formatter(value)
            elif isinstance(formatter, str):
                if isinstance(value, (dt.datetime, dt.date)):
                    formatted_value = value.strftime(formatter)
                elif isinstance(value, np.datetime64):
                    formatted_value = util.dt64_to_dt(value).strftime(formatter)
                elif re.findall(r"\{(\w+)\}", formatter):
                    formatted_value = formatter.format(value)
                else:
                    formatted_value = formatter % value
        else:
            formatted_value = str(bytes_to_unicode(value))

        if print_unit and self.unit is not None:
            formatted_value = formatted_value + ' ' + bytes_to_unicode(self.unit)
        return formatted_value

    def pprint_value_string(self, value):
        """Pretty print the dimension value and unit with title_format

        Parameters
        ----------
        value
            Dimension value to format

        Returns
        -------
        Formatted dimension value string with unit
        """
        unit = '' if self.unit is None else ' ' + bytes_to_unicode(self.unit)
        value = self.pprint_value(value)
        return title_format.format(name=bytes_to_unicode(self.label), val=value, unit=unit)


class LabelledData(param.Parameterized):
    """LabelledData is a mix-in class designed to introduce the group and
    label parameters (and corresponding methods) to any class
    containing data. This class assumes that the core data contents
    will be held in the attribute called 'data'.

    Used together, group and label are designed to allow a simple and
    flexible means of addressing data. For instance, if you are
    collecting the heights of people in different demographics, you
    could specify the values of your objects as 'Height' and then use
    the label to specify the (sub)population.

    In this scheme, one object may have the parameters set to
    [group='Height', label='Children'] and another may use
    [group='Height', label='Adults'].

    Notes
    -----
    Another level of specification is implicit in the type (i.e
    class) of the LabelledData object. A full specification of a
    LabelledData object is therefore given by the tuple
    (<type>, <group>, label>). This additional level of specification is
    used in the traverse method.

    Any strings can be used for the group and label, but it can be
    convenient to use a capitalized string of alphanumeric characters,
    in which case the keys used for matching in the matches and
    traverse method will correspond exactly to {type}.{group}.{label}.
    Otherwise the strings provided will be sanitized to be valid
    capitalized Python identifiers, which works fine but can sometimes
    be confusing.

    """

    group = param.String(default='LabelledData', constant=True, doc="""
       A string describing the type of data contained by the object.
       By default this will typically mirror the class name.""")

    label = param.String(default='', constant=True, doc="""
       Optional label describing the data, typically reflecting where
       or how it was measured. The label should allow a specific
       measurement or dataset to be referenced for a given group.""")

    _deep_indexable = False

    def __init__(self, data, id=None, plot_id=None, **params):
        """All LabelledData subclasses must supply data to the
        constructor, which will be held on the .data attribute.
        This class also has an id instance attribute, which
        may be set to associate some custom options with the object.
        """
        self.data = data

        self._id = None
        self.id = id
        self._plot_id = plot_id or builtins.id(self)
        if isinstance(params.get('label',None), tuple):
            (alias, long_name) = params['label']
            util.label_sanitizer.add_aliases(**{alias:long_name})
            params['label'] = long_name

        if isinstance(params.get('group',None), tuple):
            (alias, long_name) = params['group']
            util.group_sanitizer.add_aliases(**{alias:long_name})
            params['group'] = long_name

        super().__init__(**params)
        if not util.group_sanitizer.allowable(self.group):
            raise ValueError(f"Supplied group {self.group!r} contains invalid characters.")
        elif not util.label_sanitizer.allowable(self.label):
            raise ValueError(f"Supplied label {self.label!r} contains invalid characters.")

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, opts_id):
        """Handles tracking and cleanup of custom ids."""
        old_id = self._id
        self._id = opts_id
        if old_id is not None:
            cleanup_custom_options(old_id)
        if opts_id is not None and opts_id != old_id:
            if opts_id not in Store._weakrefs:
                Store._weakrefs[opts_id] = []
            ref = weakref.ref(self, partial(cleanup_custom_options, opts_id))
            Store._weakrefs[opts_id].append(ref)


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
        params = self.param.values()
        if new_type is None:
            clone_type = self.__class__
        else:
            clone_type = new_type
            new_params = new_type.param.objects('existing')
            params = {k: v for k, v in params.items()
                      if k in new_params}
            if params.get('group') == self.param.objects('existing')['group'].default:
                params.pop('group')
        settings = dict(params, **overrides)
        if 'id' not in settings:
            settings['id'] = self.id

        if data is None and shared_data:
            data = self.data
            if link:
                settings['plot_id'] = self._plot_id
        # Apply name mangling for __ attribute
        pos_args = getattr(self, '_' + type(self).__name__ + '__pos_params', [])
        return clone_type(data, *args, **{k:v for k,v in settings.items()
                                          if k not in pos_args})


    def relabel(self, label=None, group=None, depth=0):
        """Clone object and apply new group and/or label.

        Applies relabeling to children up to the supplied depth.

        Parameters
        ----------
        label : str, optional
            New label to apply to returned object
        group : str, optional
            New group to apply to returned object
        depth : int, optional
            Depth to which relabel will be applied.

            If applied to container allows applying relabeling to
            contained objects up to the specified depth

        Returns
        -------
        Returns relabelled object
        """
        new_data = self.data
        if (depth > 0) and getattr(self, '_deep_indexable', False):
            new_data = []
            for k, v in self.data.items():
                relabelled = v.relabel(group=group, label=label, depth=depth-1)
                new_data.append((k, relabelled))
        keywords = [('label', label), ('group', group)]
        kwargs = {k: v for k, v in keywords if v is not None}
        return self.clone(new_data, **kwargs)


    def matches(self, spec):
        """Whether the spec applies to this object.

        Parameters
        ----------
        spec : A function, spec or type to check for a match
            * A 'type[[.group].label]' string which is compared
            against the type, group and label of this object.

            * A function which is given the object and returns a boolean.

            * An object type matched using isinstance.

        Returns
        -------
        bool
            Whether the spec matched this object.
        """
        if callable(spec) and not isinstance(spec, type): return spec(self)
        elif isinstance(spec, type): return isinstance(self, spec)
        specification = (self.__class__.__name__, self.group, self.label)
        split_spec = tuple(spec.split('.')) if not isinstance(spec, tuple) else spec
        split_spec, nocompare = zip(*((None, True) if s == '*' or s is None else (s, False)
                                    for s in split_spec), strict=None)
        if all(nocompare): return True
        match_fn = itemgetter(*(idx for idx, nc in enumerate(nocompare) if not nc))
        self_spec = match_fn(split_spec)
        unescaped_match = match_fn(specification[:len(split_spec)]) == self_spec
        if unescaped_match: return True
        sanitizers = [util.sanitize_identifier, util.group_sanitizer, util.label_sanitizer]
        identifier_specification = tuple(fn(ident, escape=False)
                                         for ident, fn in zip(specification, sanitizers, strict=None))
        identifier_match = match_fn(identifier_specification[:len(split_spec)]) == self_spec
        return identifier_match

    def traverse(self, fn=None, specs=None, full_breadth=True):
        """Traverses object returning matching items
        Traverses the set of children of the object, collecting the
        all objects matching the defined specs. Each object can be
        processed with the supplied function.

        Parameters
        ----------
        fn : function, optional
            Function applied to matched objects
        specs : List of specs to match
            Specs must be types, functions or type[.group][.label]
            specs to select objects to return, by default applies
            to all objects.
        full_breadth : Whether to traverse all objects
            Whether to traverse the full set of objects on each
            container or only the first.

        Returns
        -------
        list
            List of objects that matched
        """
        if fn is None:
            fn = lambda x: x
        if specs is not None and not isinstance(specs, (list, set, tuple)):
            specs = [specs]
        accumulator = []
        matches = specs is None
        if not matches:
            for spec in specs:
                matches = self.matches(spec)
                if matches: break
        if matches:
            accumulator.append(fn(self))

        # Assumes composite objects are iterables
        if self._deep_indexable:
            for el in self:
                if el is None:
                    continue
                accumulator += el.traverse(fn, specs, full_breadth)
                if not full_breadth: break
        return accumulator


    def map(self, map_fn, specs=None, clone=True):
        """Map a function to all objects matching the specs

        Recursively replaces elements using a map function when the
        specs apply, by default applies to all objects, e.g. to apply
        the function to all contained Curve objects:

            dmap.map(fn, hv.Curve)

        Parameters
        ----------
        map_fn
            Function to apply to each object
        specs : List of specs to match
            List of types, functions or type[.group][.label] specs
            to select objects to return, by default applies to all
            objects.
        clone
            Whether to clone the object or transform inplace

        Returns
        -------
        Returns the object after the map_fn has been applied
        """
        if specs is not None and not isinstance(specs, (list, set, tuple)):
            specs = [specs]
        applies = specs is None or any(self.matches(spec) for spec in specs)

        if self._deep_indexable:
            deep_mapped = self.clone(shared_data=False) if clone else self
            for k, v in self.items():
                new_val = v.map(map_fn, specs, clone)
                if new_val is not None:
                    deep_mapped[k] = new_val
            if applies: deep_mapped = map_fn(deep_mapped)
            return deep_mapped
        else:
            return map_fn(self) if applies else self


    def __getstate__(self):
        """Ensures pickles save options applied to this objects.

        """
        obj_dict = self.__dict__.copy()
        try:
            if Store.save_option_state and (obj_dict.get('_id', None) is not None):
                custom_key = '_custom_option_{}'.format(obj_dict['_id'])
                if custom_key not in obj_dict:
                    obj_dict[custom_key] = {backend:s[obj_dict['_id']]
                                            for backend,s in Store._custom_options.items()
                                            if obj_dict['_id'] in s}
            else:
                obj_dict['_id'] = None
        except Exception:
            self.param.warning("Could not pickle custom style information.")
        return obj_dict


    def __setstate__(self, d):
        """Restores options applied to this object.

        """
        d = param_aliases(d)

        load_options = Store.load_counter_offset is not None
        if load_options:
            matches = [k for k in d if k.startswith('_custom_option')]
            for match in matches:
                custom_id = int(match.split('_')[-1])+Store.load_counter_offset
                for backend, info in d[match].items():
                    if backend not in Store._custom_options:
                        Store._custom_options[backend] = {}
                    Store._custom_options[backend][custom_id] = info
                if d[match]:
                    if custom_id not in Store._weakrefs:
                        Store._weakrefs[custom_id] = []
                    ref = weakref.ref(self, partial(cleanup_custom_options, custom_id))
                    Store._weakrefs[d["_id"]].append(ref)
                d.pop(match)

            if d["_id"] is not None:
                d["_id"] += Store.load_counter_offset

        self.__dict__.update(d)
        super().__setstate__(d)


class Dimensioned(LabelledData):
    """Dimensioned is a base class that allows the data contents of a
    class to be associated with dimensions. The contents associated
    with dimensions may be partitioned into one of three types:

    * key dimensions
        These are the dimensions that can be indexed via
        the __getitem__ method. Dimension objects
        supporting key dimensions must support indexing
        over these dimensions and may also support
        slicing. This list ordering of dimensions
        describes the positional components of each
        multi-dimensional indexing operation.

        For instance, if the key dimension names are
        'weight' followed by 'height' for Dimensioned
        object 'obj', then obj[80,175] indexes a weight
        of 80 and height of 175.

        Accessed using either kdims.

    * value dimensions
        These dimensions correspond to any data held
        on the Dimensioned object not in the key
        dimensions. Indexing by value dimension is
        supported by dimension name (when there are
        multiple possible value dimensions); no
        slicing semantics is supported and all the
        data associated with that dimension will be
        returned at once. Note that it is not possible
        to mix value dimensions and deep dimensions.

        Accessed using either vdims.

    * deep dimensions
        These are dynamically computed dimensions that
        belong to other Dimensioned objects that are
        nested in the data. Objects that support this
        should enable the _deep_indexable flag. Note
        that it is not possible to mix value dimensions
        and deep dimensions.

        Accessed using either ddims.

    Dimensioned class support generalized methods for finding the
    range and type of values along a particular Dimension. The range
    method relies on the appropriate implementation of the
    dimension_values methods on subclasses.

    The index of an arbitrary dimension is its positional index in the
    list of all dimensions, starting with the key dimensions, followed
    by the value dimensions and ending with the deep dimensions.
    """

    cdims = param.Dict(default={}, doc="""
       The constant dimensions defined as a dictionary of Dimension:value
       pairs providing additional dimension information about the object.

       Aliased with constant_dimensions.""")

    kdims = param.List(bounds=(0, None), constant=True, doc="""
       The key dimensions defined as list of dimensions that may be
       used in indexing (and potential slicing) semantics. The order
       of the dimensions listed here determines the semantics of each
       component of a multi-dimensional indexing operation.

       Aliased with key_dimensions.""")

    vdims = param.List(bounds=(0, None), constant=True, doc="""
       The value dimensions defined as the list of dimensions used to
       describe the components of the data. If multiple value
       dimensions are supplied, a particular value dimension may be
       indexed by name after the key dimensions.

       Aliased with value_dimensions.""")

    group = param.String(default='Dimensioned', constant=True, doc="""
       A string describing the data wrapped by the object.""")

    __abstract = True
    _dim_groups = ['kdims', 'vdims', 'cdims', 'ddims']
    _dim_aliases = dict(key_dimensions='kdims', value_dimensions='vdims',
                        constant_dimensions='cdims', deep_dimensions='ddims')

    def __init__(self, data, kdims=None, vdims=None, **params):
        params.update(process_dimensions(kdims, vdims))
        if 'cdims' in params:
            params['cdims'] = {d if isinstance(d, Dimension) else Dimension(d): val
                               for d, val in params['cdims'].items()}
        super().__init__(data, **params)
        self.ndims = len(self.kdims)
        cdims = [(d.name, val) for d, val in self.cdims.items()]
        self._cached_constants = dict(cdims)
        self._settings = None

        # Instantiate accessors

    @property
    def apply(self):
        return Apply(self)

    @property
    def opts(self):
        return Opts(self)

    @property
    def redim(self):
        return Redim(self)

    def _valid_dimensions(self, dimensions):
        """Validates key dimension input

        Returns kdims if no dimensions are specified

        """
        if dimensions is None:
            dimensions = self.kdims
        elif not isinstance(dimensions, list):
            dimensions = [dimensions]

        valid_dimensions = []
        for dim in dimensions:
            if isinstance(dim, Dimension): dim = dim.name
            if dim not in self.kdims:
                raise Exception(f"Supplied dimensions {dim} not found.")
            valid_dimensions.append(dim)
        return valid_dimensions


    @property
    def ddims(self):
        """The list of deep dimensions

        """
        if self._deep_indexable and self:
            return self.values()[0].dimensions()
        else:
            return []


    def dimensions(self, selection='all', label=False):
        """Lists the available dimensions on the object

        Provides convenient access to Dimensions on nested Dimensioned
        objects. Dimensions can be selected by their type, i.e. 'key'
        or 'value' dimensions. By default 'all' dimensions are
        returned.

        Parameters
        ----------
        selection : Type of dimensions to return
            The type of dimension, i.e. one of 'key', 'value',
            'constant' or 'all'.
        label : Whether to return the name, label or Dimension
            Whether to return the Dimension objects (False),
            the Dimension names (True/'name') or labels ('label').

        Returns
        -------
        List of Dimension objects or their names or labels
        """
        if label in ['name', True]:
            label = 'short'
        elif label == 'label':
            label = 'long'
        elif label:
            raise ValueError("label needs to be one of True, False, 'name' or 'label'")

        lambdas = {'k': (lambda x: x.kdims, {'full_breadth': False}),
                   'v': (lambda x: x.vdims, {}),
                   'c': (lambda x: x.cdims, {})}
        aliases = {'key': 'k', 'value': 'v', 'constant': 'c'}
        if selection in ['all', 'ranges']:
            groups = [d for d in self._dim_groups if d != 'cdims']
            dims = [dim for group in groups
                    for dim in getattr(self, group)]
        elif isinstance(selection, list):
            dims =  [dim for group in selection
                     for dim in getattr(self, f'{aliases.get(group)}dims')]
        elif aliases.get(selection) in lambdas:
            selection = aliases.get(selection, selection)
            lmbd, kwargs = lambdas[selection]
            key_traversal = self.traverse(lmbd, **kwargs)
            dims = [dim for keydims in key_traversal for dim in keydims]
        else:
            raise KeyError(f"Invalid selection {repr(selection)!r}, valid selections include"
                           "'all', 'value' and 'key' dimensions")
        return [(dim.label if label == 'long' else dim.name)
                if label else dim for dim in dims]


    def get_dimension(self, dimension, default=None, strict=False) -> Dimension | None:
        """Get a Dimension object by name or index.

        Parameters
        ----------
        dimension
            Dimension to look up by name or integer index
        default : optional
            Value returned if Dimension not found
        strict : bool, optional
            Raise a KeyError if not found

        Returns
        -------
        Dimension object for the requested dimension or default
        """
        if dimension is not None and not isinstance(dimension, (int, str, Dimension)):
            raise TypeError('Dimension lookup supports int, string, '
                            'and Dimension instances, cannot lookup '
                            f'Dimensions using {type(dimension).__name__} type.')
        all_dims = self.dimensions()
        if isinstance(dimension, int):
            if 0 <= dimension < len(all_dims):
                return all_dims[dimension]
            elif strict:
                raise KeyError(f"Dimension {dimension!r} not found")
            else:
                return default

        if isinstance(dimension, Dimension):
            dims = [d for d in all_dims if dimension == d]
            if strict and not dims:
                raise KeyError(f"{dimension!r} not found.")
            elif dims:
                return dims[0]
            else:
                return None
        else:
            dimension = dimension_name(dimension)
            name_map = {dim.spec: dim for dim in all_dims}
            name_map.update({dim.name: dim for dim in all_dims})
            name_map.update({dim.label: dim for dim in all_dims})
            name_map.update({util.dimension_sanitizer(dim.name): dim for dim in all_dims})
            if strict and dimension not in name_map:
                raise KeyError(f"Dimension {dimension!r} not found.")
            else:
                return name_map.get(dimension, default)


    def get_dimension_index(self, dimension):
        """Get the index of the requested dimension.

        Parameters
        ----------
        dimension
            Dimension to look up by name or by index

        Returns
        -------
        Integer index of the requested dimension
        """
        if isinstance(dimension, int):
            if (dimension < (self.ndims + len(self.vdims)) or
                dimension < len(self.dimensions())):
                return dimension
            else:
                return IndexError('Dimension index out of bounds')
        dim = dimension_name(dimension)
        try:
            dimensions = self.kdims+self.vdims
            return next(i for i, d in enumerate(dimensions) if d == dim)
        except StopIteration:
            raise Exception(f"Dimension {dim} not found in {self.__class__.__name__}.") from None


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
        dim_vals = [type(v) for v in self.dimension_values(dim)]
        if len(set(dim_vals)) == 1:
            return dim_vals[0]
        else:
            return None


    def __getitem__(self, key):
        """Multi-dimensional indexing semantics is determined by the list
        of key dimensions. For instance, the first indexing component
        will index the first key dimension.

        After the key dimensions are given, *either* a value dimension
        name may follow (if there are multiple value dimensions) *or*
        deep dimensions may then be listed (for applicable deep
        dimensions).
        """
        return self


    def select(self, selection_specs=None, **kwargs):
        """Applies selection by dimension name

        Applies a selection along the dimensions of the object using
        keyword arguments. The selection may be narrowed to certain
        objects using selection_specs. For container objects the
        selection will be applied to all children as well.

        Selections may select a specific value, slice or set of values:

        * value
            Scalar values will select rows along with an exact match, e.g.:

            ds.select(x=3)

        * slice
            Slices may be declared as tuples of the upper and lower bound, e.g.:

            ds.select(x=(0, 3))

        * values
            A list of values may be selected using a list or set, e.g.:

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

        # Apply all indexes applying on this object
        vdims = [*self.vdims, 'value'] if self.vdims else []
        kdims = self.kdims
        local_kwargs = {k: v for k, v in kwargs.items()
                        if k in kdims+vdims}

        # Check selection_spec applies
        if selection_specs is not None:
            if not isinstance(selection_specs, (list, tuple)):
                selection_specs = [selection_specs]
            matches = any(self.matches(spec)
                          for spec in selection_specs)
        else:
            matches = True

        # Apply selection to self
        if local_kwargs and matches:
            ndims = self.ndims
            if any(d in self.vdims for d in kwargs):
                ndims = len(self.kdims+self.vdims)
            select = [slice(None) for _ in range(ndims)]
            for dim, val in local_kwargs.items():
                if dim == 'value':
                    select += [val]
                else:
                    if isinstance(val, tuple): val = slice(*val)
                    select[self.get_dimension_index(dim)] = val
            if self._deep_indexable:
                selection = self.get(tuple(select), None)
                if selection is None:
                    selection = self.clone(shared_data=False)
            else:
                selection = self[tuple(select)]
        else:
            selection = self

        if not isinstance(selection, Dimensioned):
            return selection
        elif type(selection) is not type(self) and isinstance(selection, Dimensioned):
            # Apply the selection on the selected object of a different type
            dimensions = [*selection.dimensions(), 'value']
            if any(kw in dimensions for kw in kwargs):
                selection = selection.select(selection_specs=selection_specs, **kwargs)
        elif isinstance(selection, Dimensioned) and selection._deep_indexable:
            # Apply the deep selection on each item in local selection
            items = []
            for k, v in selection.items():
                dimensions = [*v.dimensions(), 'value']
                if any(kw in dimensions for kw in kwargs):
                    items.append((k, v.select(selection_specs=selection_specs, **kwargs)))
                else:
                    items.append((k, v))
            selection = selection.clone(items)
        return selection


    def dimension_values(self, dimension, expanded=True, flat=True):
        """Return the values along the requested dimension.

        Parameters
        ----------
        dimension : str
            The dimension to return values for.
        expanded : bool, optional
            Whether to return the expanded values. Behavior depends on the type of data:

            * Columnar: If false, returns unique values

            * Geometry: If false, returns scalar values per geometry

            * Gridded: If false, returns 1D coordinates
        flat : bool, optional
            Whether to flatten array.

        Returns
        -------
        np.ndarray
            Array of values along the requested dimension.
        """
        val = self._cached_constants.get(dimension, None)
        if val:
            return np.array([val])
        else:
            raise Exception(f"Dimension {dimension} not found in {self.__class__.__name__}.")


    def range(self, dimension, data_range=True, dimension_range=True):
        """Return the lower and upper bounds of values along dimension.

        Parameters
        ----------
        dimension
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
        dimension = self.get_dimension(dimension)
        if dimension is None or (not data_range and not dimension_range):
            return (None, None)
        elif all(util.isfinite(v) for v in dimension.range) and dimension_range:
            return dimension.range
        elif data_range:
            if dimension in self.kdims+self.vdims:
                dim_vals = self.dimension_values(dimension.name)
                lower, upper = util.find_range(dim_vals)
            else:
                dname = dimension.name
                match_fn = lambda x: dname in x.kdims + x.vdims
                range_fn = lambda x: x.range(dname)
                ranges = self.traverse(range_fn, [match_fn])
                lower, upper = util.max_range(ranges)
        else:
            lower, upper = (np.nan, np.nan)
        if not dimension_range:
            return lower, upper
        return util.dimension_range(lower, upper, dimension.range, dimension.soft_range)

    def __repr__(self):
        return PrettyPrinter.pprint(self)

    def __str__(self):
        return repr(self)

    def options(self, *args, clone=True, **kwargs):
        """Applies simplified option definition returning a new object.

        Applies options on an object or nested group of objects in a
        flat format returning a new object with the options
        applied. If the options are to be set directly on the object a
        simple format may be used, e.g.:

            obj.options(cmap='viridis', show_title=False)

        If the object is nested the options must be qualified using
        a type[.group][.label] specification, e.g.:

            obj.options('Image', cmap='viridis', show_title=False)

        or using:

            obj.options({'Image': dict(cmap='viridis', show_title=False)})

        Identical to the .opts method but returns a clone of the object
        by default.

        Parameters
        ----------
        *args: Sets of options to apply to object
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
        backend = kwargs.get('backend', None)

        if not (args or kwargs):
            options = None
        elif args and isinstance(args[0], str):
            options = {args[0]: kwargs}
        elif args and isinstance(args[0], list):
            if kwargs:
                raise ValueError('Please specify a list of option objects, or kwargs, but not both')
            options = args[0]
        elif args and [k for k in kwargs.keys() if k != 'backend']:
            raise ValueError("Options must be defined in one of two formats. "
                             "Either supply keywords defining the options for "
                             "the current object, e.g. obj.options(cmap='viridis'), "
                             "or explicitly define the type, e.g. "
                             "obj.options({'Image': {'cmap': 'viridis'}}). "
                             "Supplying both formats is not supported.")
        elif args and all(isinstance(el, dict) for el in args):
            if len(args) > 1:
                self.param.warning('Only a single dictionary can be passed '
                                   'as a positional argument. Only processing '
                                   'the first dictionary')
            options = [Options(spec, **kws) for spec,kws in args[0].items()]
        elif args:
            options = list(args)
        elif kwargs:
            options = {type(self).__name__: kwargs}

        from ..util import opts
        if options is None:
            expanded_backends = [(backend, {})]
        elif isinstance(options, list): # assuming a flat list of Options objects
            expanded_backends = opts._expand_by_backend(options, backend)
        else:
            expanded_backends = [(backend, opts._expand_options(options, backend))]

        obj = self
        for backend, expanded in expanded_backends:
            if expanded is not None:
                obj = obj.opts._dispatch_opts(expanded, backend=backend, clone=clone)
        return obj

    def _repr_mimebundle_(self, include=None, exclude=None):
        """Resolves the class hierarchy for the class rendering the
        object using any display hooks registered on Store.display
        hooks.  The output of all registered display_hooks is then
        combined and returned.

        """
        return Store.render(self)



class ViewableElement(Dimensioned):
    """A ViewableElement is a dimensioned datastructure that may be
    associated with a corresponding atomic visualization. An atomic
    visualization will display the data on a single set of axes
    (i.e. excludes multiple subplots that are displayed at once). The
    only new parameter introduced by ViewableElement is the title
    associated with the object for display.

    """

    __abstract = True
    _auxiliary_component = False

    group = param.String(default='ViewableElement', constant=True)



class ViewableTree(AttrTree, Dimensioned):
    """A ViewableTree is an AttrTree with Viewable objects as its leaf
    nodes. It combines the tree like data structure of a tree while
    extending it with the deep indexable properties of Dimensioned
    and LabelledData objects.

    """

    group = param.String(default='ViewableTree', constant=True)

    _deep_indexable = True

    def __init__(self, items=None, identifier=None, parent=None, **kwargs):
        if items and all(isinstance(item, Dimensioned) for item in items):
            items = self._process_items(items)
        params = {p: kwargs.pop(p) for p in [*self.param, 'id', 'plot_id'] if p in kwargs}

        AttrTree.__init__(self, items, identifier, parent, **kwargs)
        Dimensioned.__init__(self, self.data, **params)

    @classmethod
    def _process_items(cls, vals):
        """Processes list of items assigning unique paths to each.

        """
        from .layout import AdjointLayout

        if type(vals) is cls:
            return vals.data
        elif isinstance(vals, (AdjointLayout, str)):
            # `string` vals isn't supported but checked anyway
            # for better exception message.
            vals = [vals]
        elif isinstance(vals, Iterable):
            vals = list(vals)
        items = []
        counts = defaultdict(lambda: 1)
        cls._unpack_paths(vals, items, counts)
        items = cls._deduplicate_items(items)
        return items


    def __setstate__(self, d):
        """Ensure that object does not try to reference its parent during
        unpickling.
        """
        parent = d.pop('parent', None)
        d['parent'] = None
        super(AttrTree, self).__setstate__(d)
        self.__dict__['parent'] = parent


    @classmethod
    def _deduplicate_items(cls, items):
        """Deduplicates assigned paths by incrementing numbering

        """
        counter = Counter([path[:i] for path, _ in items for i in range(1, len(path)+1)])
        if sum(counter.values()) == len(counter):
            return items

        new_items = []
        counts = defaultdict(lambda: 0)
        for path, item in items:
            if counter[path] > 1:
                path = (*path, util.int_to_roman(counts[path] + 1))
            else:
                inc = 1
                while counts[path]:
                    path = (*path[:-1], util.int_to_roman(counts[path] + inc))
                    inc += 1
            new_items.append((path, item))
            counts[path] += 1
        return new_items


    @classmethod
    def _unpack_paths(cls, objs, items, counts):
        """Recursively unpacks lists and ViewableTree-like objects, accumulating
        into the supplied list of items.
        """
        if type(objs) is cls:
            objs = objs.items()
        for item in objs:
            path, obj = item if isinstance(item, tuple) else (None, item)
            if type(obj) is cls:
                cls._unpack_paths(obj, items, counts)
                continue
            new = path is None or len(path) == 1
            path = util.get_path(item) if new else path
            new_path = util.make_path_unique(path, counts, new)
            items.append((new_path, obj))


    @property
    def uniform(self):
        """Whether items in tree have uniform dimensions

        """
        from .traversal import uniform
        return uniform(self)


    def dimension_values(self, dimension, expanded=True, flat=True):
        dimension = self.get_dimension(dimension, strict=True).name
        all_dims = self.traverse(lambda x: [d.name for d in x.dimensions()])
        if dimension in chain.from_iterable(all_dims):
            values = [el.dimension_values(dimension) for el in self
                      if dimension in el.dimensions(label=True)]
            vals = np.concatenate(values)
            return vals if expanded else util.unique_array(vals)
        else:
            return super().dimension_values(
                dimension, expanded, flat)

    def __len__(self):
        return len(self.data)
