"""Build factory instances."""

import collections

from . import declarations
from . import enums
from . import errors
from . import utils


DeclarationWithContext = collections.namedtuple(
    'DeclarationWithContext',
    ['name', 'declaration', 'context'],
)


PostGenerationContext = collections.namedtuple(
    'PostGenerationContext',
    ['value_provided', 'value', 'extra'],
)


class DeclarationSet(object):
    """A set of declarations, including the recursive parameters.

    Attributes:
        declarations (dict(name => declaration)): the top-level declarations
        contexts (dict(name => dict(subfield => value))): the nested parameters related
            to a given top-level declaration

    This object behaves similarly to a dict mapping a top-level declaration name to a
    DeclarationWithContext, containing field name, declaration object and extra context.
    """

    def __init__(self, initial=None):
        self.declarations = {}
        self.contexts = collections.defaultdict(dict)
        self.update(initial or {})

    @classmethod
    def split(cls, entry):
        """Split a declaration name into a (declaration, subpath) tuple.

        Examples:
        >>> DeclarationSet.split('foo__bar')
        ('foo', 'bar')
        >>> DeclarationSet.split('foo')
        ('foo', None)
        >>> DeclarationSet.split('foo__bar__baz')
        ('foo', 'bar__baz')
        """
        if enums.SPLITTER in entry:
            return entry.split(enums.SPLITTER, 1)
        else:
            return (entry, None)

    @classmethod
    def join(cls, root, subkey):
        """Rebuild a full declaration name from its components.

        for every string x, we have `join(split(x)) == x`.
        """
        if subkey is None:
            return root
        return enums.SPLITTER.join((root, subkey))

    def copy(self):
        return self.__class__(self.as_dict())

    def update(self, values):
        """Add new declarations to this set/

        Args:
            values (dict(name, declaration)): the declarations to ingest.
        """
        for k, v in values.items():
            root, sub = self.split(k)
            if sub is None:
                self.declarations[root] = v
            else:
                self.contexts[root][sub] = v

        extra_context_keys = set(self.contexts) - set(self.declarations)
        if extra_context_keys:
            raise errors.InvalidDeclarationError(
                "Received deep context for unknown fields: %r (known=%r)" % (
                    {
                        self.join(root, sub): v
                        for root in extra_context_keys
                        for sub, v in self.contexts[root].items()
                    },
                    sorted(self.declarations),
                )
            )

    def filter(self, entries):
        """Filter a set of declarations: keep only those related to this object.

        This will keep:
        - Declarations that 'override' the current ones
        - Declarations that are parameters to current ones
        """
        return [
            entry for entry in entries
            if self.split(entry)[0] in self.declarations
        ]

    def sorted(self):
        return utils.sort_ordered_objects(
            self.declarations,
            getter=lambda entry: self.declarations[entry],
        )

    def __contains__(self, key):
        return key in self.declarations

    def __getitem__(self, key):
        return DeclarationWithContext(
            name=key,
            declaration=self.declarations[key],
            context=self.contexts[key],
        )

    def __iter__(self):
        return iter(self.declarations)

    def values(self):
        """Retrieve the list of declarations, with their context."""
        for name in self:
            yield self[name]

    def _items(self):
        """Extract a list of (key, value) pairs, suitable for our __init__."""
        for name in self.declarations:
            yield name, self.declarations[name]
            for subkey, value in self.contexts[name].items():
                yield self.join(name, subkey), value

    def as_dict(self):
        """Return a dict() suitable for our __init__."""
        return dict(self._items())

    def __repr__(self):
        return '<DeclarationSet: %r>' % self.as_dict()


class FakePostGenerationDeclaration(declarations.PostGenerationDeclaration):
    """A fake post-generation declaration, providing simply a hardcoded value.

    Used to disable post-generation when the user has overridden a method.
    """
    def __init__(self, value):
        self.value = value

    def call(self, instance, step, context):
        return self.value


def parse_declarations(decls, base_pre=None, base_post=None):
    pre_declarations = base_pre.copy() if base_pre else DeclarationSet()
    post_declarations = base_post.copy() if base_post else DeclarationSet()

    # Inject extra declarations, splitting between known-to-be-post and undetermined
    extra_post = {}
    extra_maybenonpost = {}
    for k, v in decls.items():
        if enums.get_builder_phase(v) == enums.BuilderPhase.POST_INSTANTIATION:
            if k in pre_declarations:
                # Conflict: PostGenerationDeclaration with the same
                # name as a BaseDeclaration
                raise errors.InvalidDeclarationError(
                    "PostGenerationDeclaration %s=%r shadows declaration %r"
                    % (k, v, pre_declarations[k])
                )
            extra_post[k] = v
        elif k in post_declarations:
            # Passing in a scalar value to a PostGenerationDeclaration
            # Set it as `key__`
            magic_key = post_declarations.join(k, '')
            extra_post[magic_key] = v
        else:
            extra_maybenonpost[k] = v

    # Start with adding new post-declarations
    post_declarations.update(extra_post)

    # Fill in extra post-declaration context
    post_overrides = post_declarations.filter(extra_maybenonpost)
    post_declarations.update({
        k: v
        for k, v in extra_maybenonpost.items()
        if k in post_overrides
    })

    # Anything else is pre_declarations
    pre_declarations.update({
        k: v
        for k, v in extra_maybenonpost.items()
        if k not in post_overrides
    })

    return pre_declarations, post_declarations


class BuildStep(object):
    def __init__(self, builder, sequence, parent_step=None):
        self.builder = builder
        self.sequence = sequence
        self.attributes = {}
        self.parent_step = parent_step
        self.stub = None

    def resolve(self, declarations):
        self.stub = Resolver(
            declarations=declarations,
            step=self,
            sequence=self.sequence,
        )

        for field_name in declarations:
            self.attributes[field_name] = getattr(self.stub, field_name)

    @property
    def chain(self):
        if self.parent_step:
            parent_chain = self.parent_step.chain
        else:
            parent_chain = ()
        return (self.stub,) + parent_chain

    def recurse(self, factory, declarations, force_sequence=None):
        builder = self.builder.recurse(factory._meta, declarations)
        return builder.build(parent_step=self, force_sequence=force_sequence)


class StepBuilder(object):
    """A factory instantiation step.

    Attributes:
    - parent: the parent StepBuilder, or None for the root step
    - extras: the passed-in kwargs for this branch
    - factory: the factory class being built
    - strategy: the strategy to use
    """
    def __init__(self, factory_meta, extras, strategy):
        self.factory_meta = factory_meta
        self.strategy = strategy
        self.extras = extras
        self.force_init_sequence = extras.pop('__sequence', None)

    def build(self, parent_step=None, force_sequence=None):
        """Build a factory instance."""
        # TODO: Handle "batch build" natively
        pre, post = parse_declarations(
            self.extras,
            base_pre=self.factory_meta.pre_declarations,
            base_post=self.factory_meta.post_declarations,
        )

        if force_sequence is not None:
            sequence = force_sequence
        elif self.force_init_sequence is not None:
            sequence = self.force_init_sequence
        else:
            sequence = self.factory_meta.next_sequence()

        step = BuildStep(
            builder=self,
            sequence=sequence,
            parent_step=parent_step,
        )
        step.resolve(pre)

        args, kwargs = self.factory_meta.prepare_arguments(step.attributes)

        instance = self.factory_meta.instantiate(
            step=step,
            args=args,
            kwargs=kwargs,
        )

        postgen_results = {}
        for declaration_name in post.sorted():
            declaration = post[declaration_name]
            unrolled_context = declaration.declaration.unroll_context(
                instance=instance,
                step=step,
                context=declaration.context,
            )

            postgen_context = PostGenerationContext(
                value_provided='' in unrolled_context,
                value=unrolled_context.get(''),
                extra={k: v for k, v in unrolled_context.items() if k != ''},
            )
            postgen_results[declaration_name] = declaration.declaration.call(
                instance=instance,
                step=step,
                context=postgen_context,
            )
        self.factory_meta.use_postgeneration_results(
            instance=instance,
            step=step,
            results=postgen_results,
        )
        return instance

    def recurse(self, factory_meta, extras):
        """Recurse into a sub-factory call."""
        return self.__class__(factory_meta, extras, strategy=self.strategy)


class Resolver(object):
    """Resolve a set of declarations.

    Attributes are set at instantiation time, values are computed lazily.

    Attributes:
        __initialized (bool): whether this object's __init__ as run. If set,
            setting any attribute will be prevented.
        __declarations (dict): maps attribute name to their declaration
        __values (dict): maps attribute name to computed value
        __pending (str list): names of the attributes whose value is being
            computed. This allows to detect cyclic lazy attribute definition.
        __step (BuildStep): the BuildStep related to this resolver.
            This allows to have the value of a field depend on the value of
            another field
    """

    __initialized = False

    def __init__(self, declarations, step, sequence):
        self.__declarations = declarations
        self.__step = step

        self.__values = {}
        self.__pending = []

        self.__initialized = True

    @property
    def factory_parent(self):
        return self.__step.parent_step.stub if self.__step.parent_step else None

    def __repr__(self):
        return '<Resolver for %r>' % self.__step

    def __getattr__(self, name):
        """Retrieve an attribute's value.

        This will compute it if needed, unless it is already on the list of
        attributes being computed.
        """
        if name in self.__pending:
            raise errors.CyclicDefinitionError(
                "Cyclic lazy attribute definition for %r; cycle found in %r." %
                (name, self.__pending))
        elif name in self.__values:
            return self.__values[name]
        elif name in self.__declarations:
            declaration = self.__declarations[name]
            value = declaration.declaration
            if enums.get_builder_phase(value) == enums.BuilderPhase.ATTRIBUTE_RESOLUTION:
                self.__pending.append(name)
                try:
                    context = value.unroll_context(
                        instance=self,
                        step=self.__step,
                        context=declaration.context,
                    )

                    value = value.evaluate(
                        instance=self,
                        step=self.__step,
                        extra=context,
                    )
                finally:
                    last = self.__pending.pop()
                assert name == last

            self.__values[name] = value
            return value
        else:
            raise AttributeError(
                "The parameter %r is unknown. Evaluated attributes are %r, "
                "definitions are %r." % (name, self.__values, self.__declarations))

    def __setattr__(self, name, value):
        """Prevent setting attributes once __init__ is done."""
        if not self.__initialized:
            return super(Resolver, self).__setattr__(name, value)
        else:
            raise AttributeError('Setting of object attributes is not allowed')
