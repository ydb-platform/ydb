from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("djangorestframework_simplejwt")
except PackageNotFoundError:
    # package is not installed
    __version__ = None
