# -*- coding: utf-8 -*-
"""
Unit of a dictionary
"""
from __future__ import absolute_import

PRECISION_MASK = 0xFFFFFFFF

OFFSET_MAX = 1 << 21
IS_LEAF_BIT = 1 << 31
HAS_LEAF_BIT = 1 << 8
EXTENSION_BIT = 1 << 9


def has_leaf(base, _mask=HAS_LEAF_BIT):
    """ Check if a unit has a leaf as a child or not. """
    return bool(base & _mask)


def value(base, _mask=~IS_LEAF_BIT & PRECISION_MASK):
    """ Check if a unit corresponds to a leaf or not. """
    return base & _mask


def label(base, _mask=IS_LEAF_BIT | 0xFF):
    """ Read a label with a leaf flag from a non-leaf unit. """
    return base & _mask


def offset(base):
    """ Read an offset to child units from a non-leaf unit. """
    return ((base >> 10) << ((base & EXTENSION_BIT) >> 6)) & PRECISION_MASK
