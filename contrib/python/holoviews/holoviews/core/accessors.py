"""Module for accessor objects for viewable HoloViews objects.

"""
import copy
from functools import wraps
from types import FunctionType

import param

from . import util
from .pprint import PrettyPrinter


class AccessorPipelineMeta(type):
    def __new__(mcs, classname, bases, classdict):
        if '__call__' in classdict:
            classdict['__call__'] = mcs.pipelined(classdict['__call__'])

        inst = type.__new__(mcs, classname, bases, classdict)
        return inst

    @classmethod
    def pipelined(mcs, __call__):
        @wraps(__call__)
        def pipelined_call(*args, **kwargs):
            from ..operation.element import (
                factory,
                method as method_op,
            )
            from .data import Dataset, MultiDimensionalMapping
            inst = args[0]

            if not hasattr(inst._obj, '_pipeline'):
                # Wrapped object doesn't support the pipeline property
                return __call__(*args, **kwargs)

            inst_pipeline = copy.copy(inst._obj. _pipeline)
            in_method = inst._obj._in_method
            if not in_method:
                inst._obj._in_method = True

            try:
                result = __call__(*args, **kwargs)

                if not in_method:
                    init_op = factory.instance(
                        output_type=type(inst),
                        kwargs={'mode': getattr(inst, 'mode', None)},
                    )
                    call_op = method_op.instance(
                        input_type=type(inst),
                        method_name='__call__',
                        args=list(args[1:]),
                        kwargs=kwargs,
                    )

                    if isinstance(result, Dataset):
                        result._pipeline = inst_pipeline.instance(
                            operations=[*inst_pipeline.operations, init_op, call_op],
                            output_type=type(result),
                        )
                    elif isinstance(result, MultiDimensionalMapping):
                        for key, element in result.items():
                            getitem_op = method_op.instance(
                                input_type=type(result),
                                method_name='__getitem__',
                                args=[key],
                            )
                            element._pipeline = inst_pipeline.instance(
                                operations=[*inst_pipeline.operations, init_op, call_op, getitem_op],
                                output_type=type(result),
                            )
            finally:
                if not in_method:
                    inst._obj._in_method = False

            return result

        return pipelined_call


class Apply(metaclass=AccessorPipelineMeta):
    """Utility to apply a function or operation to all viewable elements inside the object.

    """

    def __init__(self, obj, mode=None):
        self._obj = obj

    def __call__(self, apply_function, streams=None, link_inputs=True,
                 link_dataset=True, dynamic=None, per_element=False, **kwargs):
        """Applies a function to all (Nd)Overlay or Element objects.

        Any keyword arguments are passed through to the function. If
        keyword arguments are instance parameters, or streams are
        supplied the returned object will dynamically update in
        response to changes in those objects.

        Parameters
        ----------
        apply_function : A callable function
            The function will be passed the return value of the
            DynamicMap as the first argument and any supplied
            stream values or keywords as additional keyword
            arguments.
        streams : list, optional
            The Stream objects can dynamically supply values which
            will be passed to the function as keywords.
        link_inputs : bool, optional
            Determines whether Streams and Links attached to
            original object will be inherited.
        link_dataset : bool, optional
            Determines whether the dataset will be inherited.
        dynamic : bool, optional
            By default object is made dynamic if streams are
            supplied, an instance parameter is supplied as a
            keyword argument, or the supplied function is a
            parameterized method.
        per_element : bool, optional
            Whether to apply per element.
            By default apply works on the leaf nodes, which
            includes both elements and overlays. If set it will
            apply directly to elements.
        **kwargs : dict, optional
            Keyword arguments which will be supplied to the function.

        Returns
        -------
            A new object where the function was applied to all
            contained (Nd)Overlay or Element objects.
        """
        from panel.widgets.base import Widget

        from ..util import Dynamic
        from .data import Dataset
        from .dimension import ViewableElement
        from .element import Element
        from .spaces import DynamicMap, HoloMap

        if streams is None:
            streams = []

        if isinstance(self._obj, DynamicMap) and dynamic == False:
            samples = tuple(d.values for d in self._obj.kdims)
            if not all(samples):
                raise ValueError('Applying a function to a DynamicMap '
                                 'and setting dynamic=False is only '
                                 'possible if key dimensions define '
                                 'a discrete parameter space.')
            if not samples:
                return self._obj[samples]
            return HoloMap(self._obj[samples]).apply(
                apply_function, streams, link_inputs, link_dataset,
                dynamic, per_element, **kwargs
            )

        if isinstance(apply_function, str):
            args = kwargs.pop('_method_args', ())
            method_name = apply_function
            def apply_function(object, **kwargs):
                method = getattr(object, method_name, None)
                if method is None:
                    raise AttributeError(f'Applied method {method_name} does not exist.'
                                         'When declaring a method to apply '
                                         'as a string ensure a corresponding '
                                         'method exists on the object.')
                return method(*args, **kwargs)

        kwargs = {
            k: v.param.value if isinstance(v, Widget) else v
            for k, v in kwargs.items()
        }

        spec = Element if per_element else ViewableElement
        applies = isinstance(self._obj, spec)
        params = {p: val for p, val in kwargs.items()
                  if isinstance(val, param.Parameter)
                  and isinstance(val.owner, param.Parameterized)}

        dependent_kws = any(
            (isinstance(val, FunctionType) and hasattr(val, '_dinfo')) or
            util.is_param_method(val, has_deps=True) for val in kwargs.values()
        )

        if dynamic is None:
            is_dynamic = (bool(streams) or isinstance(self._obj, DynamicMap) or
                          util.is_param_method(apply_function, has_deps=True) or
                          params or dependent_kws)
        else:
            is_dynamic = dynamic

        if (applies or isinstance(self._obj, HoloMap)) and is_dynamic:
            return Dynamic(self._obj, operation=apply_function, streams=streams,
                           kwargs=kwargs, link_inputs=link_inputs,
                           link_dataset=link_dataset)
        elif applies:
            inner_kwargs = util.resolve_dependent_kwargs(kwargs)
            if hasattr(apply_function, 'dynamic'):
                inner_kwargs['dynamic'] = False
            new_obj = apply_function(self._obj, **inner_kwargs)
            if (link_dataset and isinstance(self._obj, Dataset) and
                isinstance(new_obj, Dataset) and new_obj._dataset is None):
                new_obj._dataset = self._obj.dataset
            return new_obj
        elif self._obj._deep_indexable:
            mapped = []
            for k, v in self._obj.data.items():
                new_val = v.apply(apply_function, dynamic=dynamic, streams=streams,
                                  link_inputs=link_inputs, link_dataset=link_dataset,
                                  **kwargs)
                if new_val is not None:
                    mapped.append((k, new_val))
            return self._obj.clone(mapped, link=link_inputs)

    def aggregate(self, dimensions=None, function=None, spreadfn=None, **kwargs):
        """Applies a aggregate function to all ViewableElements.

        See Also
        --------
        :py:meth:`Dimensioned.aggregate` and :py:meth:`Apply.__call__`
        for more information.
        """
        kwargs['_method_args'] = (dimensions, function, spreadfn)
        kwargs['per_element'] = True
        return self.__call__('aggregate', **kwargs)

    def opts(self, *args, **kwargs):
        """Applies options to all ViewableElement objects.

        See Also
        --------
        :py:meth:`Dimensioned.opts` and :py:meth:`Apply.__call__`
        for more information.
        """
        from ..streams import Params
        from ..util.transform import dim
        params = {}
        for arg in kwargs.values():
            if isinstance(arg, dim):
                params.update(arg.params)
        streams = Params.from_params(params, watch_only=True)
        kwargs['streams'] = kwargs.get('streams', []) + streams
        kwargs['_method_args'] = args
        return self.__call__('opts', **kwargs)

    def reduce(self, dimensions=None, function=None, spreadfn=None, **kwargs):
        """Applies a reduce function to all ViewableElement objects.

        See Also
        --------
        :py:meth:`Dimensioned.opts` and :py:meth:`Apply.__call__`
        for more information.
        """
        if dimensions is None:
            dimensions = []
        kwargs['_method_args'] = (dimensions, function, spreadfn)
        kwargs['per_element'] = True
        return self.__call__('reduce', **kwargs)

    def sample(self, samples=None, bounds=None, **kwargs):
        """Samples element values at supplied coordinates.

        See Also
        --------
        :py:meth:`Dataset.sample` and :py:meth:`Apply.__call__`
        for more information.
        """
        if samples is None:
            samples = []
        kwargs['_method_args'] = (samples, bounds)
        kwargs['per_element'] = True
        return self.__call__('sample', **kwargs)

    def select(self, **kwargs):
        """Applies a selection to all ViewableElement objects.

        See Also
        --------
        :py:meth:`Dimensioned.opts` and :py:meth:`Apply.__call__`
        for more information.
        """
        return self.__call__('select', **kwargs)

    def transform(self, *args, **kwargs):
        """Applies transforms to all Datasets.

        See Also
        --------
        :py:meth:`Dataset.transform` and :py:meth:`Apply.__call__`
        for more information.
        """
        from ..streams import Params
        from ..util.transform import dim
        params = {}
        for _, arg in list(args)+list(kwargs.items()):
            if isinstance(arg, dim):
                params.update(arg.params)
        streams = Params.from_params(params, watch_only=True)
        kwargs['streams'] = kwargs.get('streams', []) + streams
        kwargs['_method_args'] = args
        kwargs['per_element'] = True
        return self.__call__('transform', **kwargs)


class Redim(metaclass=AccessorPipelineMeta):
    """Utility that supports re-dimensioning any HoloViews object via the
    redim method.

    """

    def __init__(self, obj, mode=None):
        self._obj = obj
        # Can be 'dataset', 'dynamic' or None
        self.mode = mode

    def __str__(self):
        return "<holoviews.core.dimension.redim method>"

    @classmethod
    def replace_dimensions(cls, dimensions, overrides):
        """Replaces dimensions in list with dictionary of overrides.

        Parameters
        ----------
        dimensions : list
                List of dimensions
        overrides : dict
                Dictionary of dimension specs indexed by name

        Returns
        -------
        list
            List of dimensions with replacements applied
        """
        from .dimension import Dimension

        replaced = []
        for d in dimensions:
            if d.name in overrides:
                override = overrides[d.name]
            elif d.label in overrides:
                override = overrides[d.label]
            else:
                override = None

            if override is None:
                replaced.append(d)
            elif isinstance(override, (str, tuple)):
                replaced.append(d.clone(override))
            elif isinstance(override, Dimension):
                replaced.append(override)
            elif isinstance(override, dict):
                replaced.append(d.clone(override.get('name',None),
                                        **{k:v for k,v in override.items() if k != 'name'}))
            else:
                raise ValueError('Dimension can only be overridden '
                                 'with another dimension or a dictionary '
                                 'of attributes')
        return replaced


    def _filter_cache(self, dmap, kdims):
        """Returns a filtered version of the DynamicMap cache leaving only
        keys consistently with the newly specified values

        """
        filtered = []
        for key, value in dmap.data.items():
            if not any(kd.values and v not in kd.values for kd, v in zip(kdims, key, strict=None)):
                filtered.append((key, value))
        return filtered

    def _transform_dimension(self, kdims, vdims, dimension):
        if dimension in kdims:
            idx = kdims.index(dimension)
            dimension = self._obj.kdims[idx]
        elif dimension in vdims:
            idx = vdims.index(dimension)
            dimension = self._obj.vdims[idx]
        return dimension

    def _create_expression_transform(self, kdims, vdims, exclude=None):
        from ..util.transform import dim
        from .dimension import dimension_name

        if exclude is None:
            exclude = []

        def _transform_expression(expression):
            if dimension_name(expression.dimension) in exclude:
                dimension = expression.dimension
            else:
                dimension = self._transform_dimension(
                    kdims, vdims, expression.dimension
                )
            expression = expression.clone(dimension)
            ops = []
            for op in expression.ops:
                new_op = dict(op)
                new_args = []
                for arg in op['args']:
                    if isinstance(arg, dim):
                        arg = _transform_expression(arg)
                    new_args.append(arg)
                new_op['args'] = tuple(new_args)
                new_kwargs = {}
                for kw, kwarg in op['kwargs'].items():
                    if isinstance(kwarg, dim):
                        kwarg = _transform_expression(kwarg)
                    new_kwargs[kw] = kwarg
                new_op['kwargs'] = new_kwargs
                ops.append(new_op)
            expression.ops = ops
            return expression
        return _transform_expression

    def __call__(self, specs=None, **dimensions):
        """Replace dimensions on the dataset and allows renaming
        dimensions in the dataset.

        Dimension mapping should map between the old dimension name
        and a dictionary of the new attributes,
        a completely new dimension or a new string name.

        """
        obj = self._obj
        redimmed = obj
        if obj._deep_indexable and self.mode != 'dataset':
            deep_mapped = [(k, v.redim(specs, **dimensions))
                           for k, v in obj.items()]
            redimmed = obj.clone(deep_mapped)

        if specs is not None:
            if not isinstance(specs, list):
                specs = [specs]
            matches = any(obj.matches(spec) for spec in specs)
            if self.mode != 'dynamic' and not matches:
                return redimmed

        kdims = self.replace_dimensions(obj.kdims, dimensions)
        vdims = self.replace_dimensions(obj.vdims, dimensions)
        zipped_dims = zip(obj.kdims+obj.vdims, kdims+vdims, strict=None)
        renames = {pk.name: nk for pk, nk in zipped_dims if pk.name != nk.name}

        if self.mode == 'dataset':
            data = obj.data
            if renames:
                data = obj.interface.redim(obj, renames)
            transform = self._create_expression_transform(kdims, vdims, list(renames.values()))
            transforms = [*obj._transforms, transform]
            clone = obj.clone(data, kdims=kdims, vdims=vdims, transforms=transforms)
            if self._obj.dimensions(label='name') == clone.dimensions(label='name'):
                # Ensure that plot_id is inherited as long as dimension
                # name does not change
                clone._plot_id = self._obj._plot_id
            return clone

        if self.mode != 'dynamic':
            return redimmed.clone(kdims=kdims, vdims=vdims)

        from ..util import Dynamic
        def dynamic_redim(obj, **dynkwargs):
            return obj.redim(specs, **dimensions)
        dmap = Dynamic(obj, streams=obj.streams, operation=dynamic_redim)
        dmap.data = dict(self._filter_cache(redimmed, kdims))
        with util.disable_constant(dmap):
            dmap.kdims = kdims
            dmap.vdims = vdims
        return dmap


    def _redim(self, name, specs, **dims):
        dimensions = {k:{name:v} for k,v in dims.items()}
        return self(specs, **dimensions)

    def cyclic(self, specs=None, **values):
        return self._redim('cyclic', specs, **values)

    def value_format(self, specs=None, **values):
        return self._redim('value_format', specs, **values)

    def range(self, specs=None, **values):
        return self._redim('range', specs, **values)

    def label(self, specs=None, **values):
        for k, v in values.items():
            dim = self._obj.get_dimension(k)
            if dim and dim.label not in (dim.name, v):
                raise ValueError('Cannot override an existing Dimension label')
        return self._redim('label', specs, **values)

    def soft_range(self, specs=None, **values):
        return self._redim('soft_range', specs, **values)

    def type(self, specs=None, **values):
        return self._redim('type', specs, **values)

    def nodata(self, specs=None, **values):
        return self._redim('nodata', specs, **values)

    def step(self, specs=None, **values):
        return self._redim('step', specs, **values)

    def default(self, specs=None, **values):
        return self._redim('default', specs, **values)

    def unit(self, specs=None, **values):
        return self._redim('unit', specs, **values)

    def values(self, specs=None, **ranges):
        return self._redim('values', specs, **ranges)


class Opts(metaclass=AccessorPipelineMeta):

    def __init__(self, obj, mode=None):
        self._mode = mode
        self._obj = obj


    def get(self, group=None, backend=None, defaults=True):
        """Returns the corresponding Options object.

        Parameters
        ----------
        group : The options group, optional
            Flattens across groups if None.
        backend : optional
            Current backend if None otherwise chosen backend.
        defaults : bool, optional
            Whether to include default option values

        Returns
        -------
        Options object associated with the object containing the
        applied option keywords.
        """
        from .options import Options, Store
        keywords = {}
        groups = Options._option_groups if group is None else [group]
        backend = backend if backend else Store.current_backend
        for group in groups:  # noqa: PLR1704
            optsobj = Store.lookup_options(backend, self._obj, group,
                                           defaults=defaults)
            keywords = dict(keywords, **optsobj.kwargs)
        return Options(**keywords)


    def __call__(self, *args, **kwargs):
        """Applies nested options definition.

        Applies options on an object or nested group of objects in a
        flat format. Unlike the .options method, .opts modifies the
        options in place by default. If the options are to be set
        directly on the object a simple format may be used, e.g.:

            obj.opts(cmap='viridis', show_title=False)

        If the object is nested the options must be qualified using
        a type[.group][.label] specification, e.g.:

            obj.opts('Image', cmap='viridis', show_title=False)

        or using:

            obj.opts({'Image': dict(cmap='viridis', show_title=False)})

        Parameters
        ----------
        *args
            Sets of options to apply to object.
            Supports a number of formats including lists of Options
            objects, a type[.group][.label] followed by a set of
            keyword options to apply and a dictionary indexed by
            type[.group][.label] specs.
        backend : optional
            Backend to apply options to
            Defaults to current selected backend
        clone : bool, optional
            Whether to clone object
            Options can be applied in place with clone=False
        **kwargs : Keywords of options
            Set of options to apply to the object

        Notes
        -----
        For backwards compatibility, this method also supports the
        option group semantics now offered by the hv.opts.apply_groups
        utility. This usage will be deprecated and for more
        information see the apply_options_type docstring.

        Returns
        -------
        Returns the object or a clone with the options applied
        """
        if not(args) and not(kwargs):
            return self._obj
        if self._mode is None:
            apply_groups, _, _ = util.deprecated_opts_signature(args, kwargs)
            if apply_groups:
                msg = ("Calling the .opts method with options broken down by options "
                       "group (i.e. separate plot, style and norm groups) has been removed. "
                       "Use the .options method converting to the simplified format "
                       "instead or use hv.opts.apply_groups for backward compatibility.")
                raise ValueError(msg)

        return self._dispatch_opts( *args, **kwargs)

    def _dispatch_opts(self, *args, **kwargs):
        if self._mode is None:
            return self._base_opts(*args, **kwargs)
        elif self._mode == 'holomap':
            return self._holomap_opts(*args, **kwargs)
        elif self._mode == 'dynamicmap':
            return self._dynamicmap_opts(*args, **kwargs)

    def clear(self, clone=False):
        """Clears any options applied to the object.

        Parameters
        ----------
        clone : bool
            Whether to return a cleared clone or clear inplace

        Returns
        -------
        The object cleared of any options applied to it
        """
        return self._obj.opts(clone=clone)

    def info(self, show_defaults=False):
        """Prints a repr of the object including any applied options.

        Parameters
        ----------
        show_defaults : bool
            Whether to include default options
        """
        pprinter = PrettyPrinter(show_options=True, show_defaults=show_defaults)
        print(pprinter.pprint(self._obj))

    def _holomap_opts(self, *args, clone=None, **kwargs):
        apply_groups, _, _ = util.deprecated_opts_signature(args, kwargs)
        data = dict([(k, v.opts(*args, **kwargs))
                             for k, v in self._obj.data.items()])

        # By default do not clone in .opts method
        if (apply_groups if clone is None else clone):
            return self._obj.clone(data)
        else:
            self._obj.data = data
            return self._obj

    def _dynamicmap_opts(self, *args, **kwargs):
        from ..util import Dynamic

        clone = kwargs.get('clone', None)
        apply_groups, _, _ = util.deprecated_opts_signature(args, kwargs)
        # By default do not clone in .opts method
        clone = (apply_groups if clone is None else clone)

        obj = self._obj if clone else self._obj.clone()
        dmap = Dynamic(obj, operation=lambda obj, **dynkwargs: obj.opts(*args, **kwargs),
                       streams=self._obj.streams, link_inputs=True)
        if not clone:
            with util.disable_constant(self._obj):
                obj.callback = self._obj.callback
                self._obj.callback = dmap.callback
            dmap = self._obj
            dmap.data = dict([(k, v.opts(*args, **kwargs))
                                     for k, v in self._obj.data.items()])
        return dmap


    def _base_opts(self, *args, **kwargs):
        from .options import Options

        new_args = []
        for arg in args:
            if isinstance(arg, Options) and arg.key is None:
                arg = arg(key=type(self._obj).__name__)
            new_args.append(arg)
        apply_groups, options, new_kwargs = util.deprecated_opts_signature(new_args, kwargs)

        # By default do not clone in .opts method
        clone = kwargs.get('clone', None)
        if apply_groups:
            from ..util import opts
            if options is not None:
                kwargs['options'] = options
            return opts.apply_groups(self._obj, **dict(kwargs, **new_kwargs))

        kwargs['clone'] = False if clone is None else clone
        return self._obj.options(*new_args, **kwargs)

    def __getitem__(self, item):
        options = self.get().kwargs
        if item in options:
            return options[item]
        else:
            raise KeyError(
                f"{item!r} is not in opts. Valid items is {', '.join(options)}."
            )

    def __repr__(self):
        options = self.get().kwargs
        kws = ', '.join(f"{k}={options[k]!r}" for k in sorted(options.keys()))
        return f"Opts({kws})"
