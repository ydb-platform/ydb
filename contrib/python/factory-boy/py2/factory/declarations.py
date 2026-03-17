# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.

from __future__ import unicode_literals

import itertools
import logging

from . import enums
from . import compat
from . import errors
from . import utils


logger = logging.getLogger('factory.generate')


class BaseDeclaration(utils.OrderedBase):
    """A factory declaration.

    Declarations mark an attribute as needing lazy evaluation.
    This allows them to refer to attributes defined by other BaseDeclarations
    in the same factory.
    """

    FACTORY_BUILDER_PHASE = enums.BuilderPhase.ATTRIBUTE_RESOLUTION

    #: Whether to unroll the context before evaluating the declaration.
    #: Set to False on declarations that perform their own unrolling.
    UNROLL_CONTEXT_BEFORE_EVALUATION = True

    def unroll_context(self, instance, step, context):
        if not self.UNROLL_CONTEXT_BEFORE_EVALUATION:
            return context
        if not any(enums.get_builder_phase(v) for v in context.values()):
            # Optimization for simple contexts - don't do anything.
            return context

        import factory.base
        subfactory = factory.base.DictFactory
        return step.recurse(subfactory, context, force_sequence=step.sequence)

    def evaluate(self, instance, step, extra):
        """Evaluate this declaration.

        Args:
            instance (builder.Resolver): The object holding currently computed
                attributes
            step: a factory.builder.BuildStep
            extra (dict): additional, call-time added kwargs
                for the step.
        """
        raise NotImplementedError('This is an abstract method')


class OrderedDeclaration(BaseDeclaration):
    """Compatibility"""

    # FIXME(rbarrois)


class LazyFunction(BaseDeclaration):
    """Simplest BaseDeclaration computed by calling the given function.

    Attributes:
        function (function): a function without arguments and
            returning the computed value.
    """

    def __init__(self, function, *args, **kwargs):
        super(LazyFunction, self).__init__(*args, **kwargs)
        self.function = function

    def evaluate(self, instance, step, extra):
        logger.debug("LazyFunction: Evaluating %s on %s", utils.log_repr(self.function), utils.log_repr(step))
        return self.function()


class LazyAttribute(BaseDeclaration):
    """Specific BaseDeclaration computed using a lambda.

    Attributes:
        function (function): a function, expecting the current LazyStub and
            returning the computed value.
    """

    def __init__(self, function, *args, **kwargs):
        super(LazyAttribute, self).__init__(*args, **kwargs)
        self.function = function

    def evaluate(self, instance, step, extra):
        logger.debug("LazyAttribute: Evaluating %s on %s", utils.log_repr(self.function), utils.log_repr(instance))
        return self.function(instance)


class _UNSPECIFIED(object):
    pass


def deepgetattr(obj, name, default=_UNSPECIFIED):
    """Try to retrieve the given attribute of an object, digging on '.'.

    This is an extended getattr, digging deeper if '.' is found.

    Args:
        obj (object): the object of which an attribute should be read
        name (str): the name of an attribute to look up.
        default (object): the default value to use if the attribute wasn't found

    Returns:
        the attribute pointed to by 'name', splitting on '.'.

    Raises:
        AttributeError: if obj has no 'name' attribute.
    """
    try:
        if '.' in name:
            attr, subname = name.split('.', 1)
            return deepgetattr(getattr(obj, attr), subname, default)
        else:
            return getattr(obj, name)
    except AttributeError:
        if default is _UNSPECIFIED:
            raise
        else:
            return default


class SelfAttribute(BaseDeclaration):
    """Specific BaseDeclaration copying values from other fields.

    If the field name starts with two dots or more, the lookup will be anchored
    in the related 'parent'.

    Attributes:
        depth (int): the number of steps to go up in the containers chain
        attribute_name (str): the name of the attribute to copy.
        default (object): the default value to use if the attribute doesn't
            exist.
    """

    def __init__(self, attribute_name, default=_UNSPECIFIED, *args, **kwargs):
        super(SelfAttribute, self).__init__(*args, **kwargs)
        depth = len(attribute_name) - len(attribute_name.lstrip('.'))
        attribute_name = attribute_name[depth:]

        self.depth = depth
        self.attribute_name = attribute_name
        self.default = default

    def evaluate(self, instance, step, extra):
        if self.depth > 1:
            # Fetching from a parent
            target = step.chain[self.depth - 1]
        else:
            target = instance

        logger.debug("SelfAttribute: Picking attribute %r on %s", self.attribute_name, utils.log_repr(target))
        return deepgetattr(target, self.attribute_name, self.default)

    def __repr__(self):
        return '<%s(%r, default=%r)>' % (
            self.__class__.__name__,
            self.attribute_name,
            self.default,
        )


class Iterator(BaseDeclaration):
    """Fill this value using the values returned by an iterator.

    Warning: the iterator should not end !

    Attributes:
        iterator (iterable): the iterator whose value should be used.
        getter (callable or None): a function to parse returned values
    """

    def __init__(self, iterator, cycle=True, getter=None):
        super(Iterator, self).__init__()
        self.getter = getter
        self.iterator = None

        if cycle:
            self.iterator_builder = lambda: utils.ResetableIterator(itertools.cycle(iterator))
        else:
            self.iterator_builder = lambda: utils.ResetableIterator(iterator)

    def evaluate(self, instance, step, extra):
        # Begin unrolling as late as possible.
        # This helps with ResetableIterator(MyModel.objects.all())
        if self.iterator is None:
            self.iterator = self.iterator_builder()

        logger.debug("Iterator: Fetching next value from %s", utils.log_repr(self.iterator))
        value = next(iter(self.iterator))
        if self.getter is None:
            return value
        return self.getter(value)

    def reset(self):
        """Reset the internal iterator."""
        if self.iterator is not None:
            self.iterator.reset()


class Sequence(BaseDeclaration):
    """Specific BaseDeclaration to use for 'sequenced' fields.

    These fields are typically used to generate increasing unique values.

    Attributes:
        function (function): A function, expecting the current sequence counter
            and returning the computed value.
    """
    def __init__(self, function):
        super(Sequence, self).__init__()
        self.function = function

    def evaluate(self, instance, step, extra):
        logger.debug("Sequence: Computing next value of %r for seq=%s", self.function, step.sequence)
        return self.function(int(step.sequence))


class LazyAttributeSequence(Sequence):
    """Composite of a LazyAttribute and a Sequence.

    Attributes:
        function (function): A function, expecting the current LazyStub and the
            current sequence counter.
        type (function): A function converting an integer into the expected kind
            of counter for the 'function' attribute.
    """
    def evaluate(self, instance, step, extra):
        logger.debug(
            "LazyAttributeSequence: Computing next value of %r for seq=%s, obj=%s",
            self.function, step.sequence, utils.log_repr(instance))
        return self.function(instance, int(step.sequence))


class ContainerAttribute(BaseDeclaration):
    """Variant of LazyAttribute, also receives the containers of the object.

    Attributes:
        function (function): A function, expecting the current LazyStub and the
            (optional) object having a subfactory containing this attribute.
        strict (bool): Whether evaluating should fail when the containers are
            not passed in (i.e used outside a SubFactory).
    """
    def __init__(self, function, strict=True, *args, **kwargs):
        super(ContainerAttribute, self).__init__(*args, **kwargs)
        self.function = function
        self.strict = strict

    def evaluate(self, instance, step, extra):
        """Evaluate the current ContainerAttribute.

        Args:
            obj (LazyStub): a lazy stub of the object being constructed, if
                needed.
            containers (list of LazyStub): a list of lazy stubs of factories
                being evaluated in a chain, each item being a future field of
                next one.
        """
        # Strip the current instance from the chain
        chain = step.chain[1:]
        if self.strict and not chain:
            raise TypeError(
                "A ContainerAttribute in 'strict' mode can only be used "
                "within a SubFactory.")

        return self.function(instance, chain)


class ParameteredAttribute(BaseDeclaration):
    """Base class for attributes expecting parameters.

    Attributes:
        defaults (dict): Default values for the parameters.
            May be overridden by call-time parameters.

    Class attributes:
        CONTAINERS_FIELD (str): name of the field, if any, where container
            information (e.g for SubFactory) should be stored. If empty,
            containers data isn't merged into generate() parameters.
    """

    CONTAINERS_FIELD = '__containers'

    # Whether to add the current object to the stack of containers
    EXTEND_CONTAINERS = False

    def __init__(self, **kwargs):
        super(ParameteredAttribute, self).__init__()
        self.defaults = kwargs

    def _prepare_containers(self, obj, containers=()):
        if self.EXTEND_CONTAINERS:
            return (obj,) + tuple(containers)

        return containers

    def evaluate(self, instance, step, extra):
        """Evaluate the current definition and fill its attributes.

        Uses attributes definition in the following order:
        - values defined when defining the ParameteredAttribute
        - additional values defined when instantiating the containing factory

        Args:
            instance (builder.Resolver): The object holding currently computed
                attributes
            step: a factory.builder.BuildStep
            extra (dict): additional, call-time added kwargs
                for the step.
        """
        defaults = dict(self.defaults)
        if extra:
            defaults.update(extra)

        return self.generate(step, defaults)

    def generate(self, step, params):
        """Actually generate the related attribute.

        Args:
            sequence (int): the current sequence number
            obj (LazyStub): the object being constructed
            create (bool): whether the calling factory was in 'create' or
                'build' mode
            params (dict): parameters inherited from init and evaluation-time
                overrides.

        Returns:
            Computed value for the current declaration.
        """
        raise NotImplementedError()


class _FactoryWrapper(object):
    """Handle a 'factory' arg.

    Such args can be either a Factory subclass, or a fully qualified import
    path for that subclass (e.g 'myapp.factories.MyFactory').
    """
    def __init__(self, factory_or_path):
        self.factory = None
        self.module = self.name = ''
        if isinstance(factory_or_path, type):
            self.factory = factory_or_path
        else:
            if not (compat.is_string(factory_or_path) and '.' in factory_or_path):
                raise ValueError(
                    "A factory= argument must receive either a class "
                    "or the fully qualified path to a Factory subclass; got "
                    "%r instead." % factory_or_path)
            self.module, self.name = factory_or_path.rsplit('.', 1)

    def get(self):
        if self.factory is None:
            self.factory = utils.import_object(
                self.module,
                self.name,
            )
        return self.factory

    def __repr__(self):
        if self.factory is None:
            return '<_FactoryImport: %s.%s>' % (self.module, self.name)
        else:
            return '<_FactoryImport: %s>' % self.factory.__class__


class SubFactory(ParameteredAttribute):
    """Base class for attributes based upon a sub-factory.

    Attributes:
        defaults (dict): Overrides to the defaults defined in the wrapped
            factory
        factory (base.Factory): the wrapped factory
    """

    EXTEND_CONTAINERS = True
    FORCE_SEQUENCE = False
    UNROLL_CONTEXT_BEFORE_EVALUATION = False

    def __init__(self, factory, **kwargs):
        super(SubFactory, self).__init__(**kwargs)
        self.factory_wrapper = _FactoryWrapper(factory)

    def get_factory(self):
        """Retrieve the wrapped factory.Factory subclass."""
        return self.factory_wrapper.get()

    def generate(self, step, params):
        """Evaluate the current definition and fill its attributes.

        Args:
            step: a factory.builder.BuildStep
            params (dict): additional, call-time added kwargs
                for the step.
        """
        subfactory = self.get_factory()
        logger.debug(
            "SubFactory: Instantiating %s.%s(%s), create=%r",
            subfactory.__module__, subfactory.__name__,
            utils.log_pprint(kwargs=params),
            step,
        )
        force_sequence = step.sequence if self.FORCE_SEQUENCE else None
        return step.recurse(subfactory, params, force_sequence=force_sequence)


class Dict(SubFactory):
    """Fill a dict with usual declarations."""

    FORCE_SEQUENCE = True

    def __init__(self, params, dict_factory='factory.DictFactory'):
        super(Dict, self).__init__(dict_factory, **dict(params))


class List(SubFactory):
    """Fill a list with standard declarations."""

    FORCE_SEQUENCE = True

    def __init__(self, params, list_factory='factory.ListFactory'):
        params = dict((str(i), v) for i, v in enumerate(params))
        super(List, self).__init__(list_factory, **params)


# Parameters
# ==========


class Skip(object):
    def __bool__(self):
        return False

    # Py2 compatibility
    __nonzero__ = __bool__


SKIP = Skip()


class Maybe(BaseDeclaration):
    def __init__(self, decider, yes_declaration=SKIP, no_declaration=SKIP):
        super(Maybe, self).__init__()
        phases = {
            'yes_declaration': enums.get_builder_phase(yes_declaration),
            'no_declaration': enums.get_builder_phase(no_declaration),
        }
        used_phases = set(phase for phase in phases.values() if phase is not None)

        if len(used_phases) > 1:
            raise TypeError("Inconsistent phases for %r: %r" % (self, phases))

        self.FACTORY_BUILDER_PHASE = used_phases.pop() if used_phases else enums.BuilderPhase.ATTRIBUTE_RESOLUTION

        if enums.get_builder_phase(decider) is None:
            # No builder phase => flat value
            decider = SelfAttribute(decider, default=None)

        self.decider = decider
        self.yes = yes_declaration
        self.no = no_declaration

    def call(self, instance, step, context):
        decider_phase = enums.get_builder_phase(self.decider)
        if decider_phase == enums.BuilderPhase.ATTRIBUTE_RESOLUTION:
            # Note: we work on the *builder stub*, not on the actual instance.
            # This gives us access to all Params-level definitions.
            choice = self.decider.evaluate(instance=step.stub, step=step, extra=context.extra)
        else:
            assert decider_phase == enums.BuilderPhase.POST_INSTANTIATION
            choice = self.decider.call(instance, step, context)

        target = self.yes if choice else self.no
        if enums.get_builder_phase(target) == enums.BuilderPhase.POST_INSTANTIATION:
            return target.call(
                instance=instance,
                step=step,
                context=context,
            )
        else:
            # Flat value (can't be ATTRIBUTE_RESOLUTION, checked in __init__)
            return target

    def evaluate(self, instance, step, extra):
        choice = self.decider.evaluate(instance=instance, step=step, extra={})
        target = self.yes if choice else self.no

        if isinstance(target, BaseDeclaration):
            return target.evaluate(
                instance=instance,
                step=step,
                extra=extra,
            )
        else:
            # Flat value (can't be POST_INSTANTIATION, checked in __init__)
            return target

    def __repr__(self):
        return 'Maybe(%r, yes=%r, no=%r)' % (self.decider, self.yes, self.no)


class Parameter(utils.OrderedBase):
    """A complex parameter, to be used in a Factory.Params section.

    Must implement:
    - A "compute" function, performing the actual declaration override
    - Optionally, a get_revdeps() function (to compute other parameters it may alter)
    """

    def as_declarations(self, field_name, declarations):
        """Compute the overrides for this parameter.

        Args:
        - field_name (str): the field this parameter is installed at
        - declarations (dict): the global factory declarations

        Returns:
            dict: the declarations to override
        """
        raise NotImplementedError()

    def get_revdeps(self, parameters):
        """Retrieve the list of other parameters modified by this one."""
        return []


class SimpleParameter(Parameter):
    def __init__(self, value):
        super(SimpleParameter, self).__init__()
        self.value = value

    def as_declarations(self, field_name, declarations):
        return {
            field_name: self.value,
        }

    @classmethod
    def wrap(cls, value):
        if not isinstance(value, Parameter):
            return cls(value)
        return value


class Trait(Parameter):
    """The simplest complex parameter, it enables a bunch of new declarations based on a boolean flag."""
    def __init__(self, **overrides):
        super(Trait, self).__init__()
        self.overrides = overrides

    def as_declarations(self, field_name, declarations):
        overrides = {}
        for maybe_field, new_value in self.overrides.items():
            overrides[maybe_field] = Maybe(
                decider=SelfAttribute(
                    '%s.%s' % (
                        '.' * maybe_field.count(enums.SPLITTER),
                        field_name,
                    ),
                    default=False,
                ),
                yes_declaration=new_value,
                no_declaration=declarations.get(maybe_field, SKIP),
            )
        return overrides

    def get_revdeps(self, parameters):
        """This might alter fields it's injecting."""
        return [param for param in parameters if param in self.overrides]

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join('%s=%r' % t for t in self.overrides.items())
        )


# Post-generation
# ===============


class PostGenerationDeclaration(BaseDeclaration):
    """Declarations to be called once the model object has been generated."""

    FACTORY_BUILDER_PHASE = enums.BuilderPhase.POST_INSTANTIATION

    def call(self, instance, step, context):  # pragma: no cover
        """Call this hook; no return value is expected.

        Args:
            obj (object): the newly generated object
            create (bool): whether the object was 'built' or 'created'
            context: a builder.PostGenerationContext containing values
                extracted from the containing factory's declaration
        """
        raise NotImplementedError()


class PostGeneration(PostGenerationDeclaration):
    """Calls a given function once the object has been generated."""
    def __init__(self, function):
        super(PostGeneration, self).__init__()
        self.function = function

    def call(self, instance, step, context):
        logger.debug(
            "PostGeneration: Calling %s.%s(%s)",
            self.function.__module__,
            self.function.__name__,
            utils.log_pprint(
                (instance, step),
                context._asdict(),
            ),
        )
        create = step.builder.strategy == enums.CREATE_STRATEGY
        return self.function(
            instance, create, context.value, **context.extra)


class RelatedFactory(PostGenerationDeclaration):
    """Calls a factory once the object has been generated.

    Attributes:
        factory (Factory): the factory to call
        defaults (dict): extra declarations for calling the related factory
        name (str): the name to use to refer to the generated object when
            calling the related factory
    """

    UNROLL_CONTEXT_BEFORE_EVALUATION = False

    def __init__(self, factory, factory_related_name='', **defaults):
        super(RelatedFactory, self).__init__()

        self.name = factory_related_name
        self.defaults = defaults
        self.factory_wrapper = _FactoryWrapper(factory)

    def get_factory(self):
        """Retrieve the wrapped factory.Factory subclass."""
        return self.factory_wrapper.get()

    def call(self, instance, step, context):
        factory = self.get_factory()

        if context.value_provided:
            # The user passed in a custom value
            logger.debug(
                "RelatedFactory: Using provided %s instead of generating %s.%s.",
                utils.log_repr(context.value),
                factory.__module__, factory.__name__,
            )
            return context.value

        passed_kwargs = dict(self.defaults)
        passed_kwargs.update(context.extra)
        if self.name:
            passed_kwargs[self.name] = instance

        logger.debug(
            "RelatedFactory: Generating %s.%s(%s)",
            factory.__module__,
            factory.__name__,
            utils.log_pprint((step,), passed_kwargs),
        )
        return step.recurse(factory, passed_kwargs)


class RelatedFactoryList(RelatedFactory):
    """Calls a factory 'size' times once the object has been generated.

    Attributes:
        factory (Factory): the factory to call "size-times"
        defaults (dict): extra declarations for calling the related factory
        factory_related_name (str): the name to use to refer to the generated
            object when calling the related factory
        size (int|lambda): the number of times 'factory' is called, ultimately
            returning a list of 'factory' objects w/ size 'size'.
    """

    def __init__(self, factory, factory_related_name='', size=2, **defaults):
        self.size = size
        super(RelatedFactoryList, self).__init__(factory, factory_related_name, **defaults)

    def call(self, instance, step, context):
        return [super(RelatedFactoryList, self).call(instance, step, context)
                for i in range(self.size if isinstance(self.size, int)
                               else self.size())]


class NotProvided:
    pass


class PostGenerationMethodCall(PostGenerationDeclaration):
    """Calls a method of the generated object.

    Attributes:
        method_name (str): the method to call
        method_args (list): arguments to pass to the method
        method_kwargs (dict): keyword arguments to pass to the method

    Example:
        class UserFactory(factory.Factory):
            ...
            password = factory.PostGenerationMethodCall('set_pass', password='')
    """
    def __init__(self, method_name, *args, **kwargs):
        super(PostGenerationMethodCall, self).__init__()
        if len(args) > 1:
            raise errors.InvalidDeclarationError(
                "A PostGenerationMethodCall can only handle 1 positional argument; "
                "please provide other parameters through keyword arguments."
            )
        self.method_name = method_name
        self.method_arg = args[0] if args else NotProvided
        self.method_kwargs = kwargs

    def call(self, instance, step, context):
        if not context.value_provided:
            if self.method_arg is NotProvided:
                args = tuple()
            else:
                args = tuple([self.method_arg])
        else:
            args = tuple([context.value])

        kwargs = dict(self.method_kwargs)
        kwargs.update(context.extra)
        method = getattr(instance, self.method_name)
        logger.debug(
            "PostGenerationMethodCall: Calling %s.%s(%s)",
            utils.log_repr(instance),
            self.method_name,
            utils.log_pprint(args, kwargs),
        )
        return method(*args, **kwargs)
