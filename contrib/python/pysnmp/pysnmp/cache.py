#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
# Limited-size dictionary-like class to use for caches
#
"""
This module defines a limited-size dictionary-like class to use for caches.

Classes:
    Cache: Limited-size dictionary-like class to use for caches.

Usage:
    This module is used to create a cache instance with a limited size.
"""


class Cache:
    """Limited-size dictionary-like class to use for caches."""

    def __init__(self, maxSize=256):
        """Create cache instance."""
        self.__maxSize = maxSize
        self.__size = 0
        self.__chopSize = maxSize // 10
        self.__chopSize = self.__chopSize and self.__chopSize or 1
        self.__cache = {}
        self.__usage = {}

    def __contains__(self, k):
        """Check if key is in cache."""
        return k in self.__cache

    def __getitem__(self, k):
        """Get cache entry."""
        self.__usage[k] += 1
        return self.__cache[k]

    def __len__(self):
        """Return number of entries in cache."""
        return self.__size

    def __setitem__(self, k, v):
        """Set cache entry."""
        if self.__size >= self.__maxSize:
            usageKeys = sorted(self.__usage, key=lambda x, d=self.__usage: d[x])
            for _k in usageKeys[: self.__chopSize]:
                del self.__cache[_k]
                del self.__usage[_k]
            self.__size -= self.__chopSize
        if k not in self.__cache:
            self.__size += 1
            self.__usage[k] = 0
        self.__cache[k] = v

    def __delitem__(self, k):
        """Delete cache entry."""
        del self.__cache[k]
        del self.__usage[k]
        self.__size -= 1
