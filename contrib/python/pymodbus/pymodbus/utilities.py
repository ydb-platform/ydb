"""Modbus Utilities.

A collection of utilities for packing data, unpacking
data computing checksums, and decode checksums.
"""
from __future__ import annotations


# pylint: disable=missing-type-doc


def dict_property(store, index):
    """Create class properties from a dictionary.

    Basically this allows you to remove a lot of possible
    boilerplate code.

    :param store: The store store to pull from
    :param index: The index into the store to close over
    :returns: An initialized property set
    """
    if hasattr(store, "__call__"):
        getter = lambda self: store(  # pylint: disable=unnecessary-lambda-assignment
            self
        )[index]
        setter = lambda self, value: store(  # pylint: disable=unnecessary-lambda-assignment
            self
        ).__setitem__(index, value)
    elif isinstance(store, str):
        getter = lambda self: self.__getattribute__(  # pylint: disable=unnecessary-dunder-call,unnecessary-lambda-assignment
            store
        )[index]
        setter = lambda self, value: self.__getattribute__(  # pylint: disable=unnecessary-dunder-call,unnecessary-lambda-assignment
            store
        ).__setitem__(index, value)
    else:
        getter = (
            lambda self: store[index]  # pylint: disable=unnecessary-lambda-assignment
        )
        setter = lambda self, value: store.__setitem__(  # pylint: disable=unnecessary-lambda-assignment
            index, value
        )

    return property(getter, setter)


# --------------------------------------------------------------------------- #
# Error Detection Functions
# --------------------------------------------------------------------------- #


def hexlify_packets(packet):
    """Return hex representation of bytestring received.

    :param packet:
    :return:
    """
    if not packet:
        return ""
    return " ".join([hex(int(x)) for x in packet])
