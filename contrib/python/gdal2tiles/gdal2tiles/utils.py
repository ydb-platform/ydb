# -*- coding: utf-8 -*-

import warnings


class AttrDict(object):
    """
    Helper class to provide attribute like access (read and write) to
    dictionaries. Used to provide a convenient way to access both results and
    nested dsl dicts.
    """
    def __init__(self, d={}):
        # assign the inner dict manually to prevent __setattr__ from firing
        super(AttrDict, self).__setattr__('_d_', d)

    def __contains__(self, key):
        return key in self._d_

    def __nonzero__(self):
        return bool(self._d_)
    __bool__ = __nonzero__

    def __dir__(self):
        # introspection for auto-complete in IPython etc
        return list(self._d_.keys())

    def __eq__(self, other):
        if isinstance(other, AttrDict):
            return other._d_ == self._d_
        # make sure we still equal to a dict with the same data
        return other == self._d_

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        r = repr(self._d_)
        if len(r) > 60:
            r = r[:60] + '...}'
        return r

    def __getstate__(self):
        return (self._d_, )

    def __setstate__(self, state):
        super(AttrDict, self).__setattr__('_d_', state[0])

    def __getattr__(self, attr_name):
        try:
            return self.__getitem__(attr_name)
        except KeyError:
            raise AttributeError(
                '%r object has no attribute %r' % (self.__class__.__name__, attr_name))

    def __delattr__(self, attr_name):
        try:
            del self._d_[attr_name]
        except KeyError:
            raise AttributeError(
                '%r object has no attribute %r' % (self.__class__.__name__, attr_name))

    def __getitem__(self, key):
        return self._d_[key]

    def __setitem__(self, key, value):
        self._d_[key] = value

    def __delitem__(self, key):
        del self._d_[key]

    def __setattr__(self, name, value):
        if name in self._d_ or not hasattr(self.__class__, name):
            self._d_[name] = value
        else:
            # there is an attribute on the class (could be property, ..) - don't add it as field
            super(AttrDict, self).__setattr__(name, value)

    def __iter__(self):
        return iter(self._d_)

    def to_dict(self):
        return self._d_


def recursive_attrdict(obj):
    """
    .. deprecated:: version

    Walks a simple data structure, converting dictionary to AttrDict.
    Supports lists, tuples, and dictionaries.
    """
    warnings.warn("deprecated", DeprecationWarning)
    AttrDict(obj)
