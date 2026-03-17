# -*- coding: utf-8 -*-

from __future__ import absolute_import

import numpy as np

import ast
import jsonpickle
from jsonpickle.compat import unicode

__all__ = ['register_handlers', 'unregister_handlers']


class NumpyBaseHandler(jsonpickle.handlers.BaseHandler):
    def restore_dtype(self, data):
        dtype = data['dtype']
        if dtype.startswith(('{', '[')):
            return ast.literal_eval(dtype)
        return np.dtype(dtype)


class NumpyDTypeHandler(NumpyBaseHandler):
    def flatten(self, obj, data):
        data['dtype'] = unicode(obj)
        return data

    def restore(self, data):
        return self.restore_dtype(data)


class NumpyGenericHandler(NumpyBaseHandler):
    def flatten(self, obj, data):
        data['dtype'] = unicode(obj.dtype)
        data['value'] = self.context.flatten(obj.tolist(), reset=False)
        return data

    def restore(self, data):
        value = self.context.restore(data['value'], reset=False)
        return self.restore_dtype(data).type(value)


class NumpyNDArrayHandler(NumpyBaseHandler):
    def flatten(self, obj, data):
        data['dtype'] = unicode(obj.dtype)
        data['values'] = self.context.flatten(obj.tolist(), reset=False)
        return data

    def restore(self, data):
        dtype = self.restore_dtype(data)
        return np.array(self.context.restore(data['values'], reset=False),
                        dtype=dtype)


def register_handlers():
    jsonpickle.handlers.register(np.dtype, NumpyDTypeHandler, base=True)
    jsonpickle.handlers.register(np.generic, NumpyGenericHandler, base=True)
    jsonpickle.handlers.register(np.ndarray, NumpyNDArrayHandler, base=True)


def unregister_handlers():
    jsonpickle.handlers.unregister(np.dtype)
    jsonpickle.handlers.unregister(np.generic)
    jsonpickle.handlers.unregister(np.ndarray)
