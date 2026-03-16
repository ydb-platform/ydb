import six

if six.PY3:
    from contextvars import ContextVar
    from .base import BaseLocalData


    class LocalData(BaseLocalData):
        data = ContextVar('thread_data')

        @classmethod
        def is_exist(cls):
            try:
                cls.data.get()
                return True
            except LookupError:
                return False

        @classmethod
        def get_data(cls):
            return cls.data.get()

        @classmethod
        def set_data(cls, value):
            cls.data.set(value)
