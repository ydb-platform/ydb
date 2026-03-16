from .abstract import BaseMarshmallowSchema


class MetaTemplate(type):

    def __new__(cls, name, bases, nmspc):
        # If user has passed parent documents as implementation, we need
        # to retrieve the original templates
        cooked_bases = []
        for base in bases:
            if issubclass(base, Implementation):
                base = base.opts.template
            cooked_bases.append(base)
        return type.__new__(cls, name, tuple(cooked_bases), nmspc)

    def __repr__(cls):
        return "<Template class '%s.%s'>" % (cls.__module__, cls.__name__)


class Template(metaclass=MetaTemplate):
    """
    Base class to represent a template.
    """
    MA_BASE_SCHEMA_CLS = BaseMarshmallowSchema

    def __init__(self, *args, **kwargs):
        raise NotImplementedError('Cannot instantiate a template, '
                                  'use instance.register result instead.')


class MetaImplementation(MetaTemplate):

    def __new__(cls, name, bases, nmspc):
        # `opts` is only defined by the builder to implement a template.
        # If this field is missing, the user is subclassing an implementation
        # to define a new type of document, thus we should construct a template class.
        if 'opts' not in nmspc:
            # Inheritance to avoid metaclass conflicts
            return super().__new__(cls, name, bases, nmspc)
        return type.__new__(cls, name, bases, nmspc)

    def __repr__(cls):
        return "<Implementation class '%s.%s'>" % (cls.__module__, cls.__name__)


class Implementation(metaclass=MetaImplementation):
    """
    Base class to represent an implementation.
    """
    @property
    def opts(self):
        "An implementation must provide its configuration though this attribute."
        raise NotImplementedError()


def get_template(template_or_implementation):
    if issubclass(template_or_implementation, Implementation):
        return template_or_implementation.opts.template
    assert issubclass(template_or_implementation, Template)
    return template_or_implementation
