"""Simple wrapper around importing pykdtree for OpenMP use or not."""

try:
    from . import kdtree
except ImportError as err:
    raise ImportError(
        "Pykdtree failed to import its C extension. This usually means it "
        "was built with OpenMP (C-level parallelization library) support but "
        "could not find it on your system. To enable better performance "
        "OpenMP must be installed (ex. ``brew install omp`` on Mac with "
        "HomeBrew). Otherwise, try installing Pykdtree from source (ex. "
        "``pip install --no-binary pykdtree --force-reinstall pykdtree``)."
    ) from err
