import importlib.metadata

from .choices import Choices
from .tracker import FieldTracker, ModelTracker

try:
    __version__ = importlib.metadata.version('django-model-utils')
except importlib.metadata.PackageNotFoundError:  # pragma: no cover
    # package is not installed
    __version__ = None

__all__ = ("Choices", "FieldTracker", "ModelTracker")
