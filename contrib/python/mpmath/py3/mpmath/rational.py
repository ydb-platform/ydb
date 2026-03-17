import warnings

def __getattr__(name):
    warnings.warn("the rational private module is deprecated",
                  DeprecationWarning)
    if name == 'mpq':
        from fractions import Fraction
        class mpq(Fraction):
            _mpq_ = property(Fraction.as_integer_ratio)
        return mpq
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
