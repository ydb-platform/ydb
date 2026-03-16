#
# Copyright (C) 2011 - 2018 Satoru SATOH <ssato @ redhat.com>
# Copyright (C) 2019 Satoru SATOH <satoru.satoh@gmail.com>
# License: MIT
#
r"""Common parts for YAML backends:
"""
from __future__ import absolute_import
import anyconfig.backend.base


def filter_from_options(key, options):
    """
    :param key: Key str in options
    :param options: Mapping object
    :return:
        New mapping object from 'options' in which the item with 'key' filtered

    >>> filter_from_options('a', dict(a=1, b=2))
    {'b': 2}
    """
    return anyconfig.utils.filter_options([k for k in options.keys()
                                           if k != key], options)


class Parser(anyconfig.backend.base.StreamParser):
    """
    Parser for YAML files.
    """
    _type = "yaml"
    _extensions = ["yaml", "yml"]
    _ordered = True
    _allow_primitives = True
    _dict_opts = ["ac_dict"]

# vim:sw=4:ts=4:et:
