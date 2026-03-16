#
# Copyright (C) 2018, 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
# pylint: disable=too-few-public-methods
r"""Singleton class

.. versionadded:: 0.9.8

   - Add to make a kind of manager instancne later to manage plugins.
"""
from __future__ import absolute_import
import threading


class Singleton(object):
    """Singleton utilizes __new__ special method.

    .. note:: Inherited classes are equated with base class inherit this.
    """
    __instance = None
    __lock = threading.RLock()

    def __new__(cls):
        if cls.__instance is None:
            cls.__lock.acquire()
            if cls.__instance is None:
                try:
                    cls.__instance = object.__new__(cls)
                finally:
                    cls.__lock.release()

        return cls.__instance

# vim:sw=4:ts=4:et:
