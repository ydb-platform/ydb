import pickle
from importlib import import_module
from pathlib import Path
from typing import Union

from multidict import CIMultiDict, MultiDict, istr

TESTS_DIR = Path(__file__).parent.resolve()
_MD_Classes = Union[type[MultiDict[int]], type[CIMultiDict[int]]]


def write(tag: str, cls: _MD_Classes, proto: int) -> None:
    d = cls([("a", 1), ("a", 2)])
    file_basename = f"{cls.__name__.lower()}-{tag}"
    with (TESTS_DIR / f"{file_basename}.pickle.{proto}").open("wb") as f:
        pickle.dump(d, f, proto)


def write_istr(tag: str, cls: type[istr], proto: int) -> None:
    s = cls("str")
    file_basename = f"{cls.__name__.lower()}-{tag}"
    with (TESTS_DIR / f"{file_basename}.pickle.{proto}").open("wb") as f:
        pickle.dump(s, f, proto)


def generate() -> None:
    _impl_map = {
        "c-extension": "_multidict",
        "pure-python": "_multidict_py",
    }
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        for tag, impl_name in _impl_map.items():
            impl = import_module(f"multidict.{impl_name}")
            for cls in impl.CIMultiDict, impl.MultiDict:
                write(tag, cls, proto)
            write_istr(tag, impl.istr, proto)


if __name__ == "__main__":
    generate()
