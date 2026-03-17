from typing import List
from os import PathLike
from typing import Tuple
from collections.abc import Generator
from typing import Any


def package_modpaths(pkgpath,
                     with_pkg: bool = ...,
                     with_mod: bool = ...,
                     followlinks: bool = ...,
                     recursive: bool = ...,
                     with_libs: bool = ...,
                     check: bool = ...) -> Generator[Any, None, None]:
    ...


def modname_to_modpath(
        modname: str,
        hide_init: bool = True,
        hide_main: bool = False,
        sys_path: None | List[str | PathLike] = None) -> str | None:
    ...


def split_modpath(modpath: str, check: bool = True) -> Tuple[str, str]:
    ...


def normalize_modpath(modpath: str | PathLike,
                      hide_init: bool = True,
                      hide_main: bool = False) -> str | PathLike:
    ...


def modpath_to_modname(modpath: str,
                       hide_init: bool = True,
                       hide_main: bool = False,
                       check: bool = True,
                       relativeto: str | None = None) -> str:
    ...
