from collections import UserDict
from collections.abc import Mapping

DICT_TYPES = (dict, Mapping, UserDict)


def strencode(instr, encoding="utf-8"):
    try:
        instr = instr.encode(encoding)
    except (UnicodeDecodeError, AttributeError):
        pass
    return instr
