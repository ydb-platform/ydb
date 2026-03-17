from library.python.ctypes import StaticLibrary

from .syms import syms


def Geos():
    return StaticLibrary('geos', syms)
