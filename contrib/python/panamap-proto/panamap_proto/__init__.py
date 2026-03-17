import pkg_resources
from panamap_proto.panamap_proto import ProtoMappingDescriptor  # noqa: F401

__version__ = pkg_resources.resource_string(__name__, "panamap_proto.version").decode("utf-8").strip()
