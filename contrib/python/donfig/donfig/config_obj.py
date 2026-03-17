#!/usr/bin/env python
#
# Copyright (c) 2018-2019 Donfig Developers
# Copyright (c) 2014-2018, Anaconda, Inc. and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from __future__ import annotations

import ast
import base64
import contextlib
import json
import os
import pprint
import site
import sys
import warnings
from collections.abc import Mapping, Sequence
from contextlib import nullcontext
from copy import deepcopy
from typing import Any, Literal, MutableMapping

import yaml

from ._lock import SerializableLock

no_default = "__no_default__"


def canonical_name(k: str, config: Mapping[str, Any]) -> str:
    """Return the canonical name for a key.

    Handles user choice of '-' or '_' conventions by standardizing on whichever
    version was set first. If a key already exists in either hyphen or
    underscore form, the existing version is the canonical name. If neither
    version exists the original key is used as is.
    """
    try:
        if k in config:
            return k
    except TypeError:
        # config is not a mapping, return the same name as provided
        return k

    altk = k.replace("_", "-") if "_" in k else k.replace("-", "_")

    if altk in config:
        return altk

    return k


def update(
    old: MutableMapping[str, Any],
    new: Mapping[str, Any],
    priority: Literal["old", "new", "new-defaults"] = "new",
    defaults: Mapping | None = None,
) -> Mapping[str, Any]:
    """Update a nested dictionary with values from another

    This is like dict.update except that it smoothly merges nested values

    This operates in-place and modifies old

    Parameters
    ----------
    priority: string {'old', 'new', 'new-defaults'}
        If new (default) then the new dictionary has preference.
        Otherwise the old dictionary does.
        If 'new-defaults', a mapping should be given of the current defaults.
        Only if a value in ``old`` matches the current default, it will be
        updated with ``new``.

    Examples
    --------
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'x': 2, 'y': {'b': 3}}
    >>> update(a, b)  # doctest: +SKIP
    {'x': 2, 'y': {'a': 2, 'b': 3}}

    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'x': 2, 'y': {'b': 3}}
    >>> update(a, b, priority='old')  # doctest: +SKIP
    {'x': 1, 'y': {'a': 2, 'b': 3}}

    >>> d = {'x': 0, 'y': {'a': 2}}
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'x': 2, 'y': {'a': 3, 'b': 3}}
    >>> update(a, b, priority='new-defaults', defaults=d)  # doctest: +SKIP
    {'x': 1, 'y': {'a': 3, 'b': 3}}

    See Also
    --------
    donfig.config_obj.merge

    """
    for k, v in new.items():
        k = canonical_name(k, old)

        if isinstance(v, Mapping):
            if k not in old or old[k] is None:
                old[k] = {}
            update(
                old[k],
                v,
                priority=priority,
                defaults=defaults.get(k) if defaults else None,
            )
        else:
            if (
                priority == "new"
                or k not in old
                or (priority == "new-defaults" and defaults and k in defaults and defaults[k] == old[k])
            ):
                old[k] = v

    return old


def merge(*dicts: Mapping) -> dict:
    """Update a sequence of nested dictionaries

    This prefers the values in the latter dictionaries to those in the former

    Examples
    --------
    >>> a = {'x': 1, 'y': {'a': 2}}
    >>> b = {'y': {'b': 3}}
    >>> merge(a, b)  # doctest: +SKIP
    {'x': 1, 'y': {'a': 2, 'b': 3}}

    See Also
    --------
    donfig.config_obj.update

    """
    result: dict = {}
    for d in dicts:
        update(result, d)
    return result


def collect_yaml(paths: Sequence[str]) -> list[dict]:
    """Collect configuration from yaml files

    This searches through a list of paths, expands to find all yaml or json
    files, and then parses each file.

    """
    # Find all paths
    file_paths = []
    for path in paths:
        if os.path.exists(path):
            if os.path.isdir(path):
                try:
                    file_paths.extend(
                        sorted(
                            os.path.join(path, p)
                            for p in os.listdir(path)
                            if os.path.splitext(p)[1].lower() in (".json", ".yaml", ".yml")
                        )
                    )
                except OSError:
                    # Ignore permission errors
                    pass
            else:
                file_paths.append(path)

    configs = []

    # Parse yaml files
    for path in file_paths:
        config = _load_config_file(path)
        if config is not None:
            configs.append(config)

    return configs


def _load_config_file(path: str) -> dict | None:
    try:
        with open(path) as f:
            config = yaml.safe_load(f.read())
    except OSError:
        # Ignore permission errors
        return None
    except Exception as exc:
        raise ValueError(f"A config file at {path!r} is malformed, original error " f"message:\n\n{exc}") from None
    if config is not None and not isinstance(config, dict):
        raise ValueError(
            f"A config file at {path!r} is malformed - config files must have "
            f"a dict as the top level object, got a {type(config).__name__} instead"
        )
    return config


def collect_env(
    prefix: str, env: Mapping[str, str] | None = None, deprecations: MutableMapping[str, str | None] | None = None
) -> dict:
    """Collect config from environment variables

    This grabs environment variables of the form "DASK_FOO__BAR_BAZ=123" and
    turns these into config variables of the form ``{"foo": {"bar-baz": 123}}``
    It transforms the key and value in the following way:

    -  Lower-cases the key text
    -  Treats ``__`` (double-underscore) as nested access
    -  Calls ``ast.literal_eval`` on the value

    """
    if env is None:
        env = os.environ

    serial_env = f"{prefix}_INTERNAL_INHERIT_CONFIG"
    if serial_env in env:
        d = deserialize(env[serial_env])
    else:
        d = {}

    prefix_len = len(prefix)
    for name, value in env.items():
        if name.startswith(prefix):
            varname = name[prefix_len:].lower().replace("__", ".")
            try:
                d[varname] = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                d[varname] = value

    result: dict = {}
    # fake thread lock to use set functionality
    lock = nullcontext()
    ConfigSet(result, lock, deprecations or {}, d)
    return result


class ConfigSet:
    """Temporarily set configuration values within a context manager

    Note, this class should be used directly from the `Config`
    object via the :meth:`donfig.Config.set` method.

    Examples
    --------
    >>> from donfig.config_obj import ConfigSet
    >>> import mypkg
    >>> with ConfigSet(mypkg.config, {'foo': 123}):
    ...     pass

    See Also
    --------
    donfig.Config.set
    donfig.Config.get

    """

    def __init__(
        self,
        config: MutableMapping,
        lock: SerializableLock | contextlib.AbstractContextManager,
        deprecations: MutableMapping[str, str | None],
        arg: Mapping | None = None,
        **kwargs,
    ):
        with lock:
            self.config = config
            self.deprecations = deprecations
            self._record: list[tuple[str, tuple, Any]] = []

            if arg is not None:
                for key, value in arg.items():
                    key = self._check_deprecations(key)
                    self._assign(key.split("."), value, config)
            if kwargs:
                for key, value in kwargs.items():
                    key = key.replace("__", ".")
                    key = self._check_deprecations(key)
                    self._assign(key.split("."), value, config)

    def _check_deprecations(self, key: str):
        """Check if the provided value has been renamed or removed.

        Parameters
        ----------
        key : str
            The configuration key to check

        Returns
        -------
        new: str
            The proper key, whether the original (if no deprecation) or the aliased
            value

        """
        if key in self.deprecations:
            new = self.deprecations[key]
            if new:
                warnings.warn(f"Configuration key {key!r} has been deprecated. " f"Please use {new!r} instead")
                return new
            else:
                raise ValueError(f"Configuration value {key!r} has been removed")
        else:
            return key

    def __enter__(self):
        return self.config

    def __exit__(self, type, value, traceback):
        for op, path, value in reversed(self._record):
            d = self.config
            if op == "replace":
                for key in path[:-1]:
                    d = d.setdefault(key, {})
                d[path[-1]] = value
            else:  # insert
                for key in path[:-1]:
                    try:
                        d = d[key]
                    except KeyError:
                        break
                else:
                    d.pop(path[-1], None)

    def _assign(
        self,
        keys: Sequence[str],
        value: Any,
        d: MutableMapping,
        path: tuple[str, ...] = (),
        record: bool = True,
    ) -> None:
        """Assign value into a nested configuration dictionary

        Parameters
        ----------
        keys : Sequence[str]
            The nested path of keys to assign the value, similar to toolz.put_in
        value: object
        d : dict
            The part of the nested dictionary into which we want to assign the
            value
        path : tuple[str], optional
            Used internally to hold the path of old values
        record : bool, optional
            Whether this operation needs to be recorded to allow for rollback.

        """
        key = canonical_name(keys[0], d)
        path = path + (key,)

        if len(keys) == 1:
            if record:
                if key in d:
                    self._record.append(("replace", path, d[key]))
                else:
                    self._record.append(("insert", path, None))
            d[key] = value
        else:
            if key not in d:
                if record:
                    self._record.append(("insert", path, None))
                d[key] = {}
                # No need to record subsequent operations after an insert
                record = False
            self._assign(keys[1:], value, d[key], path, record=record)


def expand_environment_variables(config):
    """Expand environment variables in a nested config dictionary

    This function will recursively search through any nested dictionaries
    and/or lists.

    Parameters
    ----------
    config : dict, iterable, or str
        Input object to search for environment variables

    Returns
    -------
    config : same type as input

    Examples
    --------
    >>> expand_environment_variables({'x': [1, 2, '$USER']})  # doctest: +SKIP
    {'x': [1, 2, 'my-username']}

    """
    if isinstance(config, Mapping):
        return {k: expand_environment_variables(v) for k, v in config.items()}
    elif isinstance(config, str):
        return os.path.expandvars(config)
    elif isinstance(config, (list, tuple, set)):
        return type(config)([expand_environment_variables(v) for v in config])
    else:
        return config


class Config:
    def __init__(
        self,
        name: str,
        defaults: list[Mapping[str, Any]] | None = None,
        paths: list[str] | None = None,
        env: Mapping[str, str] | None = None,
        env_var: str | None = None,
        root_env_var: str | None = None,
        env_prefix: str | None = None,
        deprecations: Mapping[str, str | None] | None = None,
    ):
        if root_env_var is None:
            root_env_var = f"{name.upper()}_ROOT_CONFIG"
        if paths is None:
            paths = [
                os.getenv(root_env_var, f"/etc/{name}"),
                os.path.join(sys.prefix, "etc", name),
                *[os.path.join(prefix, "etc", name) for prefix in site.PREFIXES],
                os.path.join(os.path.expanduser("~"), ".config", name),
            ]

        if env_prefix is None:
            env_prefix = f"{name.upper()}_"
        if env is None:
            env = os.environ
        if env_var is None:
            env_var = f"{name.upper()}_CONFIG"
        if env_var in os.environ:
            main_path = os.environ[env_var]
            paths.append(main_path)
        else:
            main_path = os.path.join(os.path.expanduser("~"), ".config", name)
        if deprecations is None:
            deprecations = {}

        # Remove duplicate paths while preserving ordering
        paths = list(reversed(list(dict.fromkeys(reversed(paths)))))

        self.name = name
        self.env_prefix = env_prefix
        self.env = env
        self.main_path = main_path
        self.paths = paths
        self.defaults = defaults or []
        self.deprecations = deprecations

        self.config: MutableMapping[str, Any] = {}
        self.config_lock = SerializableLock()
        self.refresh()

    def __contains__(self, item):
        try:
            self[item]
            return True
        except (TypeError, IndexError, KeyError):
            return False

    def __getitem__(self, item):
        return self.get(item)

    def pprint(self, **kwargs):
        return pprint.pprint(self.config, **kwargs)

    def collect(self, paths: list[str] | None = None, env: Mapping[str, str] | None = None) -> dict:
        """Collect configuration from paths and environment variables

        Parameters
        ----------
        paths : list[str]
            A list of paths to search for yaml config files. Defaults to the
            paths passed when creating this object.

        env : Mapping[str, str]
            The system environment variables to search through. Defaults to
            the environment dictionary passed when creating this object.

        Returns
        -------
        config: dict

        See Also
        --------
        donfig.Config.refresh: collect configuration and update into primary config

        """
        if paths is None:
            paths = self.paths
        if env is None:
            env = self.env
        configs = []

        if yaml:
            configs.extend(collect_yaml(paths=paths))

        configs.append(collect_env(self.env_prefix, env=env))

        return merge(*configs)

    def refresh(self, **kwargs) -> None:
        """Update configuration by re-reading yaml files and env variables.

        This goes through the following stages:

        1.  Clearing out all old configuration
        2.  Updating from the stored defaults from downstream libraries
            (see update_defaults)
        3.  Updating from yaml files and environment variables

        Note that some functionality only checks configuration once at startup and
        may not change behavior, even if configuration changes.  It is recommended
        to restart your python process if convenient to ensure that new
        configuration changes take place.

        See Also
        --------
        donfig.Config.collect: for parameters
        donfig.Config.update_defaults

        """
        self.clear()

        for d in self.defaults:
            update(self.config, d, priority="old")

        update(self.config, self.collect(**kwargs))

    def get(self, key: str, default: Any = no_default) -> Any:
        """Get elements from global config

        Use '.' for nested access

        Examples
        --------
        >>> from donfig import Config
        >>> config = Config('mypkg')
        >>> config.get('foo')  # doctest: +SKIP
        {'x': 1, 'y': 2}

        >>> config.get('foo.x')  # doctest: +SKIP
        1

        >>> config.get('foo.x.y', default=123)  # doctest: +SKIP
        123

        See Also
        --------
        donfig.Config.set

        """
        keys = key.split(".")
        result = self.config
        for k in keys:
            k = canonical_name(k, result)
            try:
                result = result[k]
            except (TypeError, IndexError, KeyError):
                if default is not no_default:
                    return default
                else:
                    raise
        return result

    def update_defaults(self, new: Mapping) -> None:
        """Add a new set of defaults to the configuration

        It does two things:

        1.  Add the defaults to a collection to be used by refresh() later
        2.  Updates the global config with the new configuration.
            Old values are prioritized over new ones, unless the current value
            is the old default, in which case it's updated to the new default.

        """
        current_defaults = merge(*self.defaults)
        self.defaults.append(new)
        update(self.config, new, priority="new-defaults", defaults=current_defaults)

    def to_dict(self):
        """Return dictionary copy of configuration.

        .. warning::

            This will copy all keys and values. This includes values that
            may cause unwanted side effects depending on what values exist
            in the current configuration.

        """
        return deepcopy(self.config)

    def clear(self):
        """Clear all existing configuration."""
        self.config.clear()

    def merge(self, *dicts):
        """Merge this configuration with multiple dictionaries.

        See :func:`~donfig.config_obj.merge` for more information.

        """
        self.config = merge(self.config, dicts)

    def update(self, new, priority="new"):
        """Update the internal configuration dictionary with `new`.

        See :func:`~donfig.config_obj.update` for more information.

        """
        self.config = update(self.config, new, priority=priority)

    def expand_environment_variables(self) -> None:
        """Expand any environment variables in this configuration in-place.

        See :func:`~donfig.config_obj.expand_environment_variables` for more information.

        """
        self.config = expand_environment_variables(self.config)

    def rename(self, aliases: Mapping) -> None:
        """Rename old keys to new keys

        This helps migrate older configuration versions over time

        """
        old = []
        new = {}
        for o, n in aliases.items():
            value = self.get(o, None)
            if value is not None:
                old.append(o)
                new[n] = value

        for k in old:
            del self.config[canonical_name(k, self.config)]  # TODO: support nested keys

        self.set(new)

    def set(self, arg=None, **kwargs):
        """Set configuration values within a context manager.

        Parameters
        ----------
        arg : mapping or None, optional
            A mapping of configuration key-value pairs to set.
        **kwargs :
            Additional key-value pairs to set. If ``arg`` is provided, values set
            in ``arg`` will be applied before those in ``kwargs``.
            Double-underscores (``__``) in keyword arguments will be replaced with
            ``.``, allowing nested values to be easily set.

        Examples
        --------
        >>> from donfig import Config
        >>> config = Config('mypkg')

        Set ``'foo.bar'`` in a context, by providing a mapping.

        >>> with config.set({'foo.bar': 123}):
        ...     pass

        Set ``'foo.bar'`` in a context, by providing a keyword argument.

        >>> with config.set(foo__bar=123):
        ...     pass

        Set ``'foo.bar'`` globally.

        >>> config.set(foo__bar=123)  # doctest: +SKIP

        See Also
        --------
        donfig.Config.get

        """
        return ConfigSet(self.config, self.config_lock, self.deprecations, arg=arg, **kwargs)

    def ensure_file(self, source: str, destination: str | None = None, comment: bool = True) -> None:
        """Copy file to default location if it does not already exist

        This tries to move a default configuration file to a default location if
        if does not already exist.  It also comments out that file by default.

        This is to be used by downstream modules that may
        have default configuration files that they wish to include in the default
        configuration path.

        Parameters
        ----------
        source : string, filename
            Source configuration file, typically within a source directory.
        destination : string, directory
            Destination directory. Configurable by ``<CONFIG NAME>_CONFIG``
            environment variable, falling back to ~/.config/<config name>.
        comment : bool, True by default
            Whether or not to comment out the config file when copying.

        """
        if destination is None:
            destination = self.main_path

        # destination is a file and already exists, never overwrite
        if os.path.isfile(destination):
            return

        # If destination is not an existing file, interpret as a directory,
        # use the source basename as the filename
        directory = destination
        destination = os.path.join(directory, os.path.basename(source))

        try:
            if not os.path.exists(destination):
                os.makedirs(directory, exist_ok=True)

                # Atomically create destination.  Parallel testing discovered
                # a race condition where a process can be busy creating the
                # destination while another process reads an empty config file.
                tmp = "%s.tmp.%d" % (destination, os.getpid())
                with open(source) as f:
                    lines = list(f)

                if comment:
                    lines = ["# " + line if line.strip() and not line.startswith("#") else line for line in lines]

                with open(tmp, "w") as f:
                    f.write("".join(lines))

                try:
                    os.rename(tmp, destination)
                except OSError:
                    os.remove(tmp)
        except OSError:
            pass

    def serialize(self) -> str:
        """Serialize conifg data into a string.

        See :func:`serialize` for more information.

        """
        return serialize(self.config)


def serialize(data: Any) -> str:
    """Serialize config data into a string.

    Typically used to pass config via the ``MYPKG_INTERNAL_INHERIT_CONFIG`` environment variable.

    Parameters
    ----------
    data: json-serializable object
        The data to serialize

    Returns
    -------
    serialized_data: str
        The serialized data as a string

    """
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()


def deserialize(data: str) -> Any:
    """De-serialize config data into the original object.

    Typically used when receiving config via the
    ``MYPKG_INTERNAL_INHERIT_CONFIG`` environment variable. This is
    automatically called when a :class:`Config` is created and environment
    variables are loaded.

    Parameters
    ----------
    data: str
        String serialized by :func:`donfig.serialize`

    Returns
    -------
    deserialized_data: obj
        The de-serialized data

    """
    return json.loads(base64.urlsafe_b64decode(data.encode()).decode())
