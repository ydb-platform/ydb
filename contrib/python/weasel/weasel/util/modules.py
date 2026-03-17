import importlib
from pathlib import Path
from types import ModuleType
from typing import Union


def import_file(name: str, loc: Union[str, Path]) -> ModuleType:
    """Import module from a file. Used to load models from a directory.

    name (str): Name of module to load.
    loc (str / Path): Path to the file.
    RETURNS: The loaded module.
    """
    spec = importlib.util.spec_from_file_location(name, str(loc))  # type: ignore
    module = importlib.util.module_from_spec(spec)  # type: ignore
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module
