"""
This module contains common `ctypes` utils.
"""

import ctypes
import logging
import sys
from typing import Any, Callable, Optional, Union

log = logging.getLogger("can.ctypesutil")

__all__ = ["HANDLE", "HRESULT", "PHANDLE", "CLibrary"]

if sys.platform == "win32":
    _LibBase = ctypes.WinDLL
    _FUNCTION_TYPE = ctypes.WINFUNCTYPE
else:
    _LibBase = ctypes.CDLL
    _FUNCTION_TYPE = ctypes.CFUNCTYPE


class CLibrary(_LibBase):
    def __init__(self, library_or_path: Union[str, ctypes.CDLL]) -> None:
        self.func_name: Any

        if isinstance(library_or_path, str):
            super().__init__(library_or_path)
        else:
            super().__init__(library_or_path._name, library_or_path._handle)

    def map_symbol(
        self,
        func_name: str,
        restype: Any = None,
        argtypes: tuple[Any, ...] = (),
        errcheck: Optional[Callable[..., Any]] = None,
    ) -> Any:
        """
        Map and return a symbol (function) from a C library. A reference to the
        mapped symbol is also held in the instance

        :param func_name:
            symbol_name
        :param ctypes.c_* restype:
            function result type (i.e. ctypes.c_ulong...), defaults to void
        :param tuple(ctypes.c_* ... ) argtypes:
            argument types, defaults to no args
        :param callable errcheck:
            optional error checking function, see ctypes docs for _FuncPtr
        """
        if argtypes:
            prototype = _FUNCTION_TYPE(restype, *argtypes)
        else:
            prototype = _FUNCTION_TYPE(restype)
        try:
            func = prototype((func_name, self))
        except AttributeError:
            raise ImportError(
                f'Could not map function "{func_name}" from library {self._name}'
            ) from None

        func._name = func_name  # type: ignore[attr-defined] # pylint: disable=protected-access
        log.debug(
            'Wrapped function "%s", result type: %s, error_check %s',
            func_name,
            type(restype),
            errcheck,
        )

        if errcheck is not None:
            func.errcheck = errcheck

        setattr(self, func_name, func)
        return func


if sys.platform == "win32":
    HRESULT = ctypes.HRESULT

elif sys.platform == "cygwin":

    class HRESULT(ctypes.c_long):
        pass


# Common win32 definitions
class HANDLE(ctypes.c_void_p):
    pass


PHANDLE = ctypes.POINTER(HANDLE)
