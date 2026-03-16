def jit_mixin(obj, mixin):
    """Apply mixin to object and return the object"""
    if not isinstance(obj, mixin):
        obj.__class__ = jit_mixin_type(obj.__class__, mixin)
    return obj


def jit_mixin_type(base, *mixins):
    assert not issubclass(base, mixins), (base, mixins)
    mixed = _mixin_cache.get((base, mixins))
    if mixed is None:
        prefix = "".join(m._jit_mixin_prefix for m in mixins)
        name = f"{prefix}{base.__name__}"
        mixed = _mixin_cache[(base, mixins)] = type(name, (*mixins, base), {
            "_jit_mixin_base": getattr(base, "_jit_mixin_base", base),
            "_jit_mixins": mixins + getattr(base, "_jit_mixins", ()),
        })
    return mixed


_mixin_cache = {}


class JITMixin:

    def __reduce__(self):
        # make JITMixin classes pickleable
        return (jit_mixin_type, (self._jit_mixin_base, *self._jit_mixins))
