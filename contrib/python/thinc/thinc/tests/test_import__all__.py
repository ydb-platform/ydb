import ast
import importlib
from collections import namedtuple
from typing import List, Tuple

import pytest

_Import = namedtuple("_Import", ["module", "name", "alias"])


def get_imports(path: str) -> Tuple[_Import, ...]:
    """Parse Python file at path, retrieve import statements.
    Adapted from https://stackoverflow.com/a/9049549.
    path (str): Path to Python file.
    RETURNS (Tuple[_Import]): All imports found in file at path.
    """
    with open(path) as fh:
        root = ast.parse(fh.read(), path)

    imports: List[_Import] = []
    for node in ast.walk(root):
        if isinstance(node, ast.Import):
            module: List[str] = []
        elif isinstance(node, ast.ImportFrom) and node.module:
            module = node.module.split(".")
        else:
            continue

        assert isinstance(node, (ast.Import, ast.ImportFrom))
        imports.extend(
            [_Import(module, n.name.split("."), n.asname) for n in node.names]
        )

    return tuple(imports)


@pytest.mark.parametrize("module_name", ["thinc.api", "thinc.shims", "thinc.layers"])
def test_import_reexport_equivalency(module_name: str):
    """Tests whether a module's __all__ is equivalent to its imports. This assumes that this module is supposed to
    re-export all imported values.
    module_name (str): Module to load.
    """
    mod = importlib.import_module(module_name)

    assert set(mod.__all__) == {
        k
        for k in set(n for i in get_imports(str(mod.__file__)) for n in i.name)
        if (
            # Ignore all values prefixed with _, as we expect those not to be re-exported.
            # However, __version__ should be reexported in thinc/__init__.py.
            (not k.startswith("_") or module_name == "thinc" and k == "__version__")
        )
    }
