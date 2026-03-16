'''
General request and RTNL object data filters.
'''

import abc
import weakref
from collections import ChainMap


class RequestFilter(abc.ABC):
    pass


class RequestProcessor(dict):
    field_filters = tuple()
    mark = tuple()
    context = None
    combined = None
    parameters = None

    def __init__(self, context=None, prime=None, parameters=None):
        self.reset_filters()
        self.reset_mark()
        self.parameters = dict(parameters) if parameters else {}
        prime = {} if prime is None else prime
        self.context = (
            context if isinstance(context, (dict, weakref.ProxyType)) else {}
        )
        self.combined = ChainMap(self, self.context)
        if isinstance(prime, dict):
            self.update(prime)

    def __setitem__(self, key, value):
        if value is None:
            return
        new_data = self.filter(key, value)
        op = 'set'
        #
        if isinstance(new_data, tuple):
            op, new_data = new_data
        elif not isinstance(new_data, dict):
            raise ValueError('invalid new_data type')
        #
        if op == 'set':
            if key in self:
                super().__delitem__(key)
            for nkey, nvalue in new_data.items():
                if nkey in self:
                    super().__delitem__(nkey)
                super().__setitem__(nkey, nvalue)
            return
        #
        if op == 'patch':
            for nkey, nvalue in new_data.items():
                if nkey not in self:
                    self[nkey] = []
                self[nkey].extend(nvalue)
            return
        #
        raise RuntimeError('error applying new values')

    def reset_filters(self):
        self.field_filters = []

    def set_parameter(self, name, value):
        self.parameters[name] = value

    def reset_mark(self):
        self.mark = []

    def items(self):
        for key, value in super().items():
            if key not in self.mark:
                yield key, value

    def get_value(self, key, default=None, mode=None):
        for field_filter in self.field_filters:
            getter = getattr(field_filter, f'get_{key}', None)
            if getter is not None:
                return getter(self, mode)
        self.mark.append(key)
        return self.get(key, default)

    def filter(self, key, value):
        job = {key: value}
        ret = None

        for field_filter in self.field_filters:
            for k, v in tuple(job.items()):
                if hasattr(field_filter, 'key_transform'):
                    k = field_filter.key_transform(k)
                if (
                    hasattr(field_filter, 'allowed')
                    and k not in field_filter.allowed
                ):
                    return {}
                if hasattr(field_filter, 'policy') and not field_filter.policy(
                    k
                ):
                    return {}
                setter = getattr(field_filter, f'set_{k}', None)
                if setter is not None:
                    if ret is None:
                        ret = {}
                    ret.update(setter(ChainMap(self.combined, ret), v))

            if ret is not None:
                job = ret

        return ret if ret is not None else {key: value}

    def update(self, prime):
        for key, value in tuple(prime.items()):
            self[key] = value

    def add_filter(self, field_filter):
        self.field_filters.append(field_filter)
        return self

    def finalize(self):
        self.update(self)
        for field_filter in self.field_filters:
            if hasattr(field_filter, 'finalize'):
                field_filter.finalize(self.combined)
        return self
