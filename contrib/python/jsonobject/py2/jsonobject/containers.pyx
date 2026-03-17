from __future__ import absolute_import
from jsonobject.base_properties import DefaultProperty
from jsonobject.utils import check_type, SimpleDict


class JsonArray(list):
    def __init__(self, _obj=None, wrapper=None, type_config=None):
        super(JsonArray, self).__init__()
        self._obj = check_type(_obj, list,
                               'JsonArray must wrap a list or None')

        assert type_config is not None
        self._type_config = type_config
        self._wrapper = (
            wrapper or
            DefaultProperty(type_config=self._type_config)
        )
        for item in self._obj:
            super(JsonArray, self).append(self._wrapper.wrap(item))

    def validate(self, required=True):
        for obj in self:
            self._wrapper.validate(obj, required=required)

    def append(self, wrapped):
        wrapped, unwrapped = self._wrapper.unwrap(wrapped)
        self._obj.append(unwrapped)
        super(JsonArray, self).append(wrapped)

    def __delitem__(self, i):
        super(JsonArray, self).__delitem__(i)
        del self._obj[i]

    def __setitem__(self, i, wrapped):
        if isinstance(i, slice):
            new_wrapped = []
            unwrapped = []
            for _wrapped in wrapped:
                _wrapped, _unwrapped = self._wrapper.unwrap(_wrapped)
                new_wrapped.append(_wrapped)
                unwrapped.append(_unwrapped)
        else:
            new_wrapped, unwrapped = self._wrapper.unwrap(wrapped)
        self._obj[i] = unwrapped
        super(JsonArray, self).__setitem__(i, new_wrapped)

    def extend(self, wrapped_list):
        if wrapped_list:
            wrapped_list, unwrapped_list = zip(
                *map(self._wrapper.unwrap, wrapped_list)
            )
        else:
            unwrapped_list = []
        self._obj.extend(unwrapped_list)
        super(JsonArray, self).extend(wrapped_list)

    def insert(self, index, wrapped):
        wrapped, unwrapped = self._wrapper.unwrap(wrapped)
        self._obj.insert(index, unwrapped)
        super(JsonArray, self).insert(index, wrapped)

    def remove(self, value):
        i = self.index(value)
        super(JsonArray, self).remove(value)
        self._obj.pop(i)

    def pop(self, index=-1):
        self._obj.pop(index)
        return super(JsonArray, self).pop(index)

    def sort(self, cmp=None, key=None, reverse=False):
        zipped = list(zip(self, self._obj))
        if key:
            new_key = lambda pair: key(pair[0])
            zipped.sort(key=new_key, reverse=reverse)
        elif cmp:
            new_cmp = lambda pair1, pair2: cmp(pair1[0], pair2[0])
            zipped.sort(cmp=new_cmp, reverse=reverse)
        else:
            zipped.sort(reverse=reverse)

        wrapped_list, unwrapped_list = list(zip(*zipped))
        while self:
            self.pop()
        super(JsonArray, self).extend(wrapped_list)
        self._obj.extend(unwrapped_list)

    def reverse(self):
        self._obj.reverse()
        super(JsonArray, self).reverse()

    def __fix_slice(self, i, j):
        length = len(self)
        if j < 0:
            j += length
        if i < 0:
            i += length
        if i > length:
            i = length
        if j > length:
            j = length
        return i, j

    def __setslice__(self, i, j, sequence):
        i, j = self.__fix_slice(i, j)
        for _ in range(j - i):
            self.pop(i)
        for k, wrapped in enumerate(sequence):
            self.insert(i + k, wrapped)

    def __delslice__(self, i, j):
        i, j = self.__fix_slice(i, j)
        for _ in range(j - i):
            self.pop(i)

    def __iadd__(self, b):
        self.extend(b)
        return self


class JsonDict(SimpleDict):

    def __init__(self, _obj=None, wrapper=None, type_config=None):
        super(JsonDict, self).__init__()
        self._obj = check_type(_obj, dict, 'JsonDict must wrap a dict or None')
        assert type_config is not None
        self._type_config = type_config
        self._wrapper = (
            wrapper or
            DefaultProperty(type_config=self._type_config)
        )
        for key, value in self._obj.items():
            self[key] = self.__wrap(key, value)

    def validate(self, required=True):
        for obj in self.values():
            self._wrapper.validate(obj, required=required)

    def __wrap(self, key, unwrapped):
        return self._wrapper.wrap(unwrapped)

    def __unwrap(self, key, wrapped):
        return self._wrapper.unwrap(wrapped)

    def __setitem__(self, key, value):
        if isinstance(key, int):
            key = unicode(key)

        wrapped, unwrapped = self.__unwrap(key, value)
        self._obj[key] = unwrapped
        super(JsonDict, self).__setitem__(key, wrapped)

    def __delitem__(self, key):
        del self._obj[key]
        super(JsonDict, self).__delitem__(key)

    def __getitem__(self, key):
        if isinstance(key, int):
            key = unicode(key)
        return super(JsonDict, self).__getitem__(key)


class JsonSet(set):
    def __init__(self, _obj=None, wrapper=None, type_config=None):
        super(JsonSet, self).__init__()
        if isinstance(_obj, set):
            _obj = list(_obj)
        self._obj = check_type(_obj, list, 'JsonSet must wrap a list or None')
        assert type_config is not None
        self._type_config = type_config
        self._wrapper = (
            wrapper or
            DefaultProperty(type_config=self._type_config)
        )
        for item in self._obj:
            super(JsonSet, self).add(self._wrapper.wrap(item))

    def validate(self, required=True):
        for obj in self:
            self._wrapper.validate(obj, required=required)

    def add(self, wrapped):
        wrapped, unwrapped = self._wrapper.unwrap(wrapped)
        if wrapped not in self:
            self._obj.append(unwrapped)
            super(JsonSet, self).add(wrapped)

    def remove(self, wrapped):
        wrapped, unwrapped = self._wrapper.unwrap(wrapped)
        if wrapped in self:
            self._obj.remove(unwrapped)
            super(JsonSet, self).remove(wrapped)
        else:
            raise KeyError(wrapped)

    def discard(self, wrapped):
        try:
            self.remove(wrapped)
        except KeyError:
            pass

    def pop(self):
        # get first item
        for wrapped in self:
            break
        else:
            raise KeyError()
        wrapped_, unwrapped = self._wrapper.unwrap(wrapped)
        assert wrapped is wrapped_
        self.remove(unwrapped)
        return wrapped

    def clear(self):
        while self:
            self.pop()

    def __ior__(self, other):
        for wrapped in other:
            self.add(wrapped)
        return self

    def update(self, *args):
        for wrapped_list in args:
            self |= set(wrapped_list)

    union_update = update

    def __iand__(self, other):
        for wrapped in list(self):
            if wrapped not in other:
                self.remove(wrapped)
        return self

    def intersection_update(self, *args):
        for wrapped_list in args:
            self &= set(wrapped_list)

    def __isub__(self, other):
        for wrapped in list(self):
            if wrapped in other:
                self.remove(wrapped)
        return self

    def difference_update(self, *args):
        for wrapped_list in args:
            self -= set(wrapped_list)

    def __ixor__(self, other):
        removed = set()
        for wrapped in list(self):
            if wrapped in other:
                self.remove(wrapped)
                removed.add(wrapped)
        self.update(other - removed)
        return self

    def symmetric_difference_update(self, *args):
        for wrapped_list in args:
            self ^= set(wrapped_list)
