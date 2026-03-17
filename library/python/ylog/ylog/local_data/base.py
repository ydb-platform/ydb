import six
from abc import abstractmethod, ABCMeta


@six.add_metaclass(ABCMeta)
class BaseLocalData(object):
    data = None

    @classmethod
    @abstractmethod
    def is_exist(cls):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def get_data(cls):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def set_data(cls, value):
        raise NotImplementedError()
