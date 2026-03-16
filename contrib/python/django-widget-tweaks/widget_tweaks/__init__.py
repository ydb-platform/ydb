from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("django-widget-tweaks")
except PackageNotFoundError:
    # package is not installed
    __version__ = None
