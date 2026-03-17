# -*- coding: utf-8 -*-
from __future__ import absolute_import


def split_data(iterable, pred):
    """
    Split data from ``iterable`` into two lists.
    Each element is passed to function ``pred``; elements
    for which ``pred`` returns True are put into ``yes`` list,
    other elements are put into ``no`` list.

    >>> split_data(["foo", "Bar", "Spam", "egg"], lambda t: t.istitle())
    (['Bar', 'Spam'], ['foo', 'egg'])
    """
    yes, no = [], []
    for d in iterable:
        if pred(d):
            yes.append(d)
        else:
            no.append(d)
    return yes, no
