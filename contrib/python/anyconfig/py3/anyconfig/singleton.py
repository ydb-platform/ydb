#
# Copyright (C) 2018 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
# pylint: disable=too-few-public-methods
r"""Singleton class.

.. versionadded:: 0.9.8

   - Add to make a kind of manager instancne later to manage plugins.
"""
from __future__ import annotations

import threading
import typing

if typing.TYPE_CHECKING:
    try:
        from typing import Self
    except ImportError:
        from typing_extensions import Self


class Singleton:
    """Singleton utilizes __new__ special method.

    .. note:: Inherited classes are equated with base class inherit this.
    """

    __instance = None
    __lock = threading.RLock()

    def __new__(cls) -> Self:
        """Override class constructor to cache class objects."""
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:
                    cls.__instance = object.__new__(cls)

        return cls.__instance
