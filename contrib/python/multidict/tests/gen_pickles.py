import pickle

from multidict._compat import USE_EXTENSIONS
from multidict._multidict_py import CIMultiDict as PyCIMultiDict  # noqa
from multidict._multidict_py import MultiDict as PyMultiDict  # noqa

try:
    from multidict._multidict import (  # type: ignore # noqa
        CIMultiDict,
        MultiDict,
    )
except ImportError:
    pass


def write(name, proto):
    cls = globals()[name]
    d = cls([("a", 1), ("a", 2)])
    with open("{}.pickle.{}".format(name.lower(), proto), "wb") as f:
        pickle.dump(d, f, proto)


def generate():
    if not USE_EXTENSIONS:
        raise RuntimeError("C Extension is required")
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        for name in ("MultiDict", "CIMultiDict", "PyMultiDict", "PyCIMultiDict"):
            write(name, proto)


if __name__ == "__main__":
    generate()
