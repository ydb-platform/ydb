import os
import shutil
import stat
import sys
import tempfile
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Iterator, Union

from ..errors import Warnings


@contextmanager
def working_dir(path: Union[str, Path]) -> Iterator[Path]:
    """Change current working directory and returns to previous on exit.

    path (str / Path): The directory to navigate to.
    YIELDS (Path): The absolute path to the current working directory. This
        should be used if the block needs to perform actions within the working
        directory, to prevent mismatches with relative paths.
    """
    prev_cwd = Path.cwd()
    current = Path(path).resolve()
    os.chdir(str(current))
    try:
        yield current
    finally:
        os.chdir(str(prev_cwd))


@contextmanager
def make_tempdir() -> Generator[Path, None, None]:
    """Execute a block in a temporary directory and remove the directory and
    its contents at the end of the with block.

    YIELDS (Path): The path of the temp directory.
    """
    d = Path(tempfile.mkdtemp())
    yield d

    # On Windows, git clones use read-only files, which cause permission errors
    # when being deleted. This forcibly fixes permissions.
    def force_remove(rmfunc, path, ex):
        os.chmod(path, stat.S_IWRITE)
        rmfunc(path)

    try:
        if sys.version_info >= (3, 12):
            shutil.rmtree(str(d), onexc=force_remove)
        else:
            shutil.rmtree(str(d), onerror=force_remove)
    except PermissionError as e:
        warnings.warn(Warnings.W801.format(dir=d, msg=e))


def is_cwd(path: Union[Path, str]) -> bool:
    """Check whether a path is the current working directory.

    path (Union[Path, str]): The directory path.
    RETURNS (bool): Whether the path is the current working directory.
    """
    return str(Path(path).resolve()).lower() == str(Path.cwd().resolve()).lower()


def ensure_path(path: Any) -> Any:
    """Ensure string is converted to a Path.

    path (Any): Anything. If string, it's converted to Path.
    RETURNS: Path or original argument.
    """
    if isinstance(path, str):
        return Path(path)
    else:
        return path


def ensure_pathy(path):
    """Temporary helper to prevent importing cloudpathlib globally (which was
    originally added due to a slow and annoying Google Cloud warning with
    Pathy)"""
    from cloudpathlib import AnyPath  # noqa: F811

    return AnyPath(path)


def is_subpath_of(parent, child):
    """
    Check whether `child` is a path contained within `parent`.
    """
    # Based on https://stackoverflow.com/a/37095733 .

    # In Python 3.9, the `Path.is_relative_to()` method will supplant this, so
    # we can stop using crusty old os.path functions.
    parent_realpath = os.path.realpath(parent)
    child_realpath = os.path.realpath(child)
    return os.path.commonpath([parent_realpath, child_realpath]) == parent_realpath
