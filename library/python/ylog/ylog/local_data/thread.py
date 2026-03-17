import threading
from .base import BaseLocalData


class LocalData(BaseLocalData):
    data = threading.local()

    @classmethod
    def is_exist(cls):
        return hasattr(cls.data, 'logging_context')

    @classmethod
    def get_data(cls):
        return cls.data.logging_context

    @classmethod
    def set_data(cls, value):
        cls.data.logging_context = value
