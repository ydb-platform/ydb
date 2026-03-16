from .middleware import OpenTracingMiddleware  # noqa
from .tracing import DjangoTracing  # noqa
from .tracing import DjangoTracing as DjangoTracer  # noqa, deprecated
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
