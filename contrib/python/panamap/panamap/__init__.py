import pkg_resources
from panamap.panamap import (  # noqa: F401
    Mapper,
    MappingException,
    MissingMappingException,
    ImproperlyConfiguredException,
    MappingDescriptor,
    UnsupportedFieldException,
    FieldMappingException,
    DuplicateMappingException,
)
from panamap.tools import values_map  # noqa: F401

__version__ = pkg_resources.resource_string(__name__, "panamap.version").decode("utf-8").strip()
