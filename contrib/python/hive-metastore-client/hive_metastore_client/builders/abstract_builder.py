"""Abstract Builder Class."""
from abc import ABCMeta, abstractmethod


class AbstractBuilder(metaclass=ABCMeta):
    """Abstract Builder class with builder methods."""

    @abstractmethod
    def build(self) -> object:
        """
        Builds the respective Thrift object.

        :return: an instance of the built object
        """
        pass
