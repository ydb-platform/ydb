#!/usr/bin/env python
# -*- coding: utf-8 -*-
import string

from enum import unique, IntEnum


@unique
class EStorageChannel(IntEnum):
    MAIN = 0
    EXTRA = 1
    EXTRA2 = 2
    EXTRA3 = 3
    EXTRA4 = 4
    EXTRA5 = 5
    INLINE = 65535

    @staticmethod
    def from_string(s):
        """
        >>> EStorageChannel.from_string('MAIN')
        <EStorageChannel.MAIN: 0>
        >>> EStorageChannel.MAIN.name
        'MAIN'
        """
        s = string.upper(s)
        return EStorageChannel[s]
