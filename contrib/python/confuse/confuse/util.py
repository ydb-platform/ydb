from __future__ import annotations

import argparse
import importlib.util
import optparse
import os
import platform
import sys
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from collections.abc import Iterable


UNIX_DIR_FALLBACK = "~/.config"
WINDOWS_DIR_VAR = "APPDATA"
WINDOWS_DIR_FALLBACK = "~\\AppData\\Roaming"
MAC_DIR = "~/Library/Application Support"

T = TypeVar("T")


def iter_first(sequence: Iterable[T]) -> T:
    """Get the first element from an iterable or raise a ValueError if
    the iterator generates no values.
    """
    it = iter(sequence)
    try:
        return next(it)
    except StopIteration:
        raise ValueError()


def namespace_to_dict(
    obj: argparse.Namespace | optparse.Values | T,
) -> dict[str, Any] | T:
    """If obj is argparse.Namespace or optparse.Values we'll return
      a dict representation of it, else return the original object.

    Redefine this method if using other parsers.

    :param obj: *
    :return:
    :rtype: dict or *
    """
    if isinstance(obj, (argparse.Namespace, optparse.Values)):
        return vars(obj)
    return obj


def build_dict(
    values_obj: argparse.Namespace | optparse.Values | T,
    sep: str = "",
    keep_none: bool = False,
) -> dict[str, Any] | T:
    """Recursively builds a dictionary from an argparse.Namespace,
    optparse.Values, or dict object.

    Additionally, if `sep` is a non-empty string, the keys will be split
    by `sep` and expanded into a nested dict. Keys with a `None` value
    are dropped by default to avoid unsetting options but can be kept
    by setting `keep_none` to `True`.

    :param obj: Namespace, Values, or dict to iterate over. Other
        values will simply be returned.
    :type obj: argparse.Namespace or optparse.Values or dict or *
    :param sep: Separator to use for splitting properties/keys of `obj`
        for expansion into nested dictionaries.
    :type sep: str
    :param keep_none: Whether to keep keys whose value is `None`.
    :type keep_none: bool
    :return: A new dictionary or the value passed if obj was not a
        dict, Namespace, or Values.
    :rtype: dict or *
    """
    # We expect our root object to be a dict, but it may come in as
    # a namespace
    obj = namespace_to_dict(values_obj)
    # We only deal with dictionaries
    if not isinstance(obj, dict):
        return obj

    # Get keys iterator
    keys: Iterable[str] = obj.keys()
    if sep:
        # Splitting keys by `sep` needs sorted keys to prevent parents
        # from clobbering children
        keys = sorted(list(keys))

    output: dict[str, Any] = {}
    for key in keys:
        value = obj[key]
        if value is None and not keep_none:  # Avoid unset options.
            continue

        save_to = output
        result = build_dict(value, sep, keep_none)
        if sep:
            # Split keys by `sep` as this signifies nesting
            split = key.split(sep)
            if len(split) > 1:
                # The last index will be the key we assign result to
                key = split.pop()
                # Build the dict tree if needed and change where
                # we're saving to
                for child_key in split:
                    if child_key in save_to and isinstance(save_to[child_key], dict):
                        save_to = save_to[child_key]
                    else:
                        # Clobber or create
                        save_to[child_key] = {}
                        save_to = save_to[child_key]

        # Save
        if key in save_to:
            save_to[key].update(result)
        else:
            save_to[key] = result
    return output


# Config file paths, including platform-specific paths and in-package
# defaults.


def find_package_path(name: str) -> str | None:
    """Returns the path to the package containing the named module or
    None if the path could not be identified (e.g., if
    ``name == "__main__"``).
    """
    # Based on get_root_path from Flask by Armin Ronacher, cf.
    # https://github.com/pallets/flask/blob/85c5d93cbd049c4bd0679c36fd1ddcae8c37b642/src/flask/helpers.py#L570
    try:
        spec = importlib.util.find_spec(name)
    except (ImportError, ValueError):
        return None

    if not spec or (loader := spec.loader) is None or name == "__main__":
        return None

    filepath: str
    if hasattr(loader, "get_filename"):
        filepath = loader.get_filename(name)
    else:
        # Fall back to importing the specified module.
        __import__(name)
        filepath = sys.modules[name].__file__  # type: ignore[assignment]

    return os.path.dirname(os.path.abspath(filepath))


def xdg_config_dirs() -> list[str]:
    """Returns a list of paths taken from the XDG_CONFIG_DIRS
    and XDG_CONFIG_HOME environment varibables if they exist
    """
    paths = []
    if "XDG_CONFIG_HOME" in os.environ:
        paths.append(os.environ["XDG_CONFIG_HOME"])
    if "XDG_CONFIG_DIRS" in os.environ:
        paths.extend(os.environ["XDG_CONFIG_DIRS"].split(":"))
    else:
        paths.append("/etc/xdg")
    paths.append("/etc")
    return paths


def config_dirs() -> list[str]:
    """Return a platform-specific list of candidates for user
    configuration directories on the system.

    The candidates are in order of priority, from highest to lowest. The
    last element is the "fallback" location to be used when no
    higher-priority config file exists.
    """
    paths = []

    if platform.system() == "Darwin":
        paths.append(UNIX_DIR_FALLBACK)
        paths.append(MAC_DIR)
        paths.extend(xdg_config_dirs())

    elif platform.system() == "Windows":
        paths.append(WINDOWS_DIR_FALLBACK)
        if WINDOWS_DIR_VAR in os.environ:
            paths.append(os.environ[WINDOWS_DIR_VAR])

    else:
        # Assume Unix.
        paths.append(UNIX_DIR_FALLBACK)
        paths.extend(xdg_config_dirs())

    # Expand and deduplicate paths.
    out = []
    for path in paths:
        path = os.path.abspath(os.path.expanduser(path))
        if path not in out:
            out.append(path)
    return out
