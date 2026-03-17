#
# This file is part of pysmi software.
#
# Copyright (c) 2015-2019, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysmi/license.html
#

from pysmi.codegen.base import AbstractCodeGen


def capfirst(text):
    if not text:
        return text

    return text[0].upper() + text[1:]


def bitstring(bits):
    mask = sum(1 << bit for bit in bits)

    # The left-most character of the returned string is for bit number zero,
    # so reverse the bits, while also stripping off the "0b" prefix.
    return bin(mask)[:1:-1]


def pythonsym(symbol):
    return AbstractCodeGen.trans_opers(symbol)


def pythonstr(text):
    if "\n" in text or "\r" in text:
        return '"""\\\n' + text.replace("\\", "\\\\") + '"""'
    else:
        return '"' + text.replace("\\", "\\\\") + '"'
