#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author: Jialiang Shi
import logging

logger = logging.getLogger(__name__)


class GerritBase:
    """
    This appears to be the base object that all other gerrit objects are
    inherited from
    """

    def __init__(self, pull=True):
        """
        Initialize a gerrit connection
        """
        self._data = None
        if pull:
            self.poll()

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__} {str(self)}>"

    def __str__(self):
        raise NotImplementedError

    def poll(self):
        data = self._poll()
        self._data = data

        if isinstance(self._data, dict):
            for key, value in self._data.items():
                try:
                    if key[0] == "_":
                        key = key[1:]
                    setattr(self, key, value)
                except AttributeError:
                    pass

    def _poll(self):
        res = self.gerrit.get(self.endpoint)  # pylint: disable=no-member

        if isinstance(res, list) and res:
            return res[0]

        return res

    def to_dict(self):
        """
        Print out all the data in this object for debugging.
        """
        return self._data

    def __eq__(self, other):
        """
        Return true if the other object represents a connection to the
        same server
        """
        if not isinstance(other, self.__class__):
            return False
        return other.endpoint == self.endpoint  # pylint: disable=no-member
