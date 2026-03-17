from dictpath.paths import AccessorPath

from openapi_core.spec.accessors import SpecAccessor

SPEC_SEPARATOR = '#'


class SpecPath(AccessorPath):

    @classmethod
    def from_spec(cls, spec_dict, dereferencer=None, *args, **kwargs):
        separator = kwargs.pop('separator', SPEC_SEPARATOR)
        accessor = SpecAccessor(spec_dict, dereferencer)
        return cls(accessor, *args, separator=separator)
