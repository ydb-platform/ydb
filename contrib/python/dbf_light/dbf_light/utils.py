# -*- encoding: utf-8 -*-
from __future__ import unicode_literals
from os import path

import codecs


try:
    string_types = basestring,

except NameError:
    string_types = str,


def bytes_to_int(val):
    return int(codecs.encode(val, 'hex'), 16)


def pick_name(filename, candidates):
    filedir = path.dirname(filename)
    name_lower = path.basename(filename).lower()
    name_candidate = None

    for name in candidates:

        if name_lower == path.basename(name).lower():
            name_candidate = path.join(filedir, name)
            break

    if name_candidate:
        filename = name_candidate

    return filename
