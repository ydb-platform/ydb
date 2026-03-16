""" decorators.py
"""
import contextlib
import functools
import logging

try:
    from singledispatch import singledispatch

except ImportError:  # pragma: no cover
    from functools import singledispatch


from .errors import PGPError

__all__ = ['classproperty',
           'sdmethod',
           'sdproperty',
           'KeyAction']


def classproperty(fget):
    class ClassProperty(object):
        def __init__(self, fget):
            self.fget = fget
            self.__doc__ = fget.__doc__

        def __get__(self, cls, owner):
            return self.fget(owner)

        def __set__(self, obj, value):  # pragma: no cover
            raise AttributeError("Read-only attribute")

        def __delete__(self, obj):  # pragma: no cover
            raise AttributeError("Read-only attribute")

    return ClassProperty(fget)


def sdmethod(meth):
    """
    This is a hack to monkey patch sdproperty to work as expected with instance methods.
    """
    sd = singledispatch(meth)

    def wrapper(obj, *args, **kwargs):
        return sd.dispatch(args[0].__class__)(obj, *args, **kwargs)

    wrapper.register = sd.register
    wrapper.dispatch = sd.dispatch
    wrapper.registry = sd.registry
    wrapper._clear_cache = sd._clear_cache
    functools.update_wrapper(wrapper, meth)
    return wrapper


def sdproperty(fget):
    def defset(obj, val):  # pragma: no cover
        raise TypeError(str(val.__class__))

    class SDProperty(property):
        def register(self, cls=None, fset=None):
            return self.fset.register(cls, fset)

        def setter(self, fset):
            self.register(object, fset)
            return type(self)(self.fget, self.fset, self.fdel, self.__doc__)

    return SDProperty(fget, sdmethod(defset))


class KeyAction(object):
    def __init__(self, *usage, **conditions):
        super(KeyAction, self).__init__()
        self.flags = set(usage)
        self.conditions = conditions

    @contextlib.contextmanager
    def usage(self, key, user):
        def _preiter(first, iterable):
            yield first
            for item in iterable:
                yield item

        em = {}
        em['keyid'] = key.fingerprint.keyid
        em['flags'] = ', '.join(flag.name for flag in self.flags)

        if len(self.flags):
            for _key in _preiter(key, key.subkeys.values()):
                if self.flags & set(_key._get_key_flags(user)):
                    break

            else:  # pragma: no cover
                warning = "Key {keyid:s} does not have the required usage flag {flags:s}".format(**em)
                if key._require_usage_flags:
                    raise PGPError(warning)
                else:
                    logging.warning(warning)

        else:
            _key = key

        if _key is not key:
            em['subkeyid'] = _key.fingerprint.keyid
            logging.debug("Key {keyid:s} does not have the required usage flag {flags:s}; using subkey {subkeyid:s}"
                          "".format(**em))  # TODO: consider adding stacklevel=4 when we only support Python >= 3.8

        yield _key

    def check_attributes(self, key):
        for attr, expected in self.conditions.items():
            if getattr(key, attr) != expected:
                raise PGPError("Expected: {attr:s} == {eval:s}. Got: {got:s}"
                               "".format(attr=attr, eval=str(expected), got=str(getattr(key, attr))))

    def __call__(self, action):
        @functools.wraps(action)
        def _action(key, *args, **kwargs):
            if key._key is None:
                raise PGPError("No key!")

            # if a key is in the process of being created, it needs to be allowed to certify its own user id
            if len(key._uids) == 0 and key.is_primary and action is not key.certify.__wrapped__:
                raise PGPError("Key is not complete - please add a User ID!")

            with self.usage(key, kwargs.get('user', None)) as _key:
                self.check_attributes(key)

                # do the thing
                return action(_key, *args, **kwargs)

        return _action
