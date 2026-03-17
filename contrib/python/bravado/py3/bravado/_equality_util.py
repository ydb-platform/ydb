# -*- coding: utf-8 -*-
import typing
from itertools import chain

from six import iterkeys


def are_objects_equal(obj1, obj2, attributes_to_ignore=None):
    # type: (typing.Any, typing.Any, typing.Optional[typing.Set[typing.Text]]) -> bool
    """
    Helper method that checks if two objects are the same.
    This is very generic and basically ensures that all the attributes
    of both objects are defined and are the same.

    NOTE: Sometimes some attribute do create recursive references to the same objects
          and/or some objects do not define equality checks (ie. missing __eq__ method).
          Those attributes might be ignored via attributes_to_ignore parameter
    """
    if id(obj1) == id(obj2):
        return True

    if not isinstance(obj2, obj1.__class__):
        return False

    if attributes_to_ignore is None:
        attributes_to_ignore = set()

    for attr_name in set(
        chain(
            iterkeys(obj1.__dict__),
            iterkeys(obj2.__dict__),
        ),
    ):
        if attr_name in attributes_to_ignore:
            continue
        try:
            if not (getattr(obj1, attr_name) == getattr(obj2, attr_name)):
                return False
        except AttributeError:
            return False

    return True
