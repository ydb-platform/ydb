import pickle
from importlib import import_module
from pathlib import Path

TESTS_DIR = Path(__file__).parent.resolve()


def write(tag, cls, proto):
    d = cls([("a", 1), ("a", 2)])
    file_basename = f"{cls.__name__.lower()}-{tag}"
    with (TESTS_DIR / f"{file_basename}.pickle.{proto}").open("wb") as f:
        pickle.dump(d, f, proto)


def generate():
    _impl_map = {
        "c-extension": "_multidict",
        "pure-python": "_multidict_py",
    }
    for proto in range(pickle.HIGHEST_PROTOCOL + 1):
        for tag, impl_name in _impl_map.items():
            impl = import_module(f"multidict.{impl_name}")
            for cls in impl.CIMultiDict, impl.MultiDict:
                write(tag, cls, proto)


if __name__ == "__main__":
    generate()
