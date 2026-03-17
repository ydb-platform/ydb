# Provide a nicer error message than failing to import models.Index.

VERSION = (0, 6, 0)
__version__ = '.'.join(str(v) for v in VERSION)


__all__ = ['PartialIndex', 'PQ', 'PF', 'ValidatePartialUniqueMixin', 'PartialUniqueValidationError']


MIN_DJANGO_VERSION = (1, 11)
DJANGO_VERSION_ERROR = 'Django version %s or later is required for django-partial-index.' % '.'.join(str(v) for v in MIN_DJANGO_VERSION)

try:
    import django
except ImportError:
    raise ImportError(DJANGO_VERSION_ERROR)

if tuple(django.VERSION[:2]) < MIN_DJANGO_VERSION:
    raise ImportError(DJANGO_VERSION_ERROR)


from .index import PartialIndex
from .query import PQ, PF
from .mixins import ValidatePartialUniqueMixin, PartialUniqueValidationError
