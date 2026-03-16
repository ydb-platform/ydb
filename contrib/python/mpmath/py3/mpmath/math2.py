import warnings

def __getattr__(name):
    warnings.warn("the math2 module is deprecated, use libfp instead",
                  DeprecationWarning)
    from . import libfp
    return getattr(libfp, name)
