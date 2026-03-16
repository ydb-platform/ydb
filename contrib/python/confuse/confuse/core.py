# This file is part of Confuse.
# Copyright 2016, Adrian Sampson.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.

"""Worry-free YAML configuration files."""

from __future__ import annotations

__all__ = [
    "CONFIG_FILENAME",
    "DEFAULT_FILENAME",
    "REDACTED_TOMBSTONE",
    "ROOT_NAME",
    "ConfigView",
    "Configuration",
    "LazyConfig",
    "RootView",
    "Subview",
]

import errno
import os
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, TypeVar, overload

import yaml
from typing_extensions import Self

from . import templates, util, yaml_util
from .exceptions import ConfigError, ConfigTypeError, NotFoundError
from .sources import ConfigSource, EnvSource, YamlSource

if TYPE_CHECKING:
    import builtins
    from argparse import Namespace
    from collections.abc import Iterable, Iterator, Mapping, Sequence
    from optparse import Values
    from pathlib import Path

    from .templates import ConfigKey

CONFIG_FILENAME = "config.yaml"
DEFAULT_FILENAME = "config_default.yaml"
ROOT_NAME = "root"

REDACTED_TOMBSTONE = "REDACTED"

R = TypeVar("R")

# Views and sources.


class ConfigView:
    """A configuration "view" is a query into a program's configuration
    data. A view represents a hypothetical location in the configuration
    tree; to extract the data from the location, a client typically
    calls the ``view.get()`` method. The client can access children in
    the tree (subviews) by subscripting the parent view (i.e.,
    ``view[key]``).
    """

    name: str
    """The name of the view, depicting the path taken through the
    configuration in Python-like syntax (e.g., ``foo['bar'][42]``).
    """

    def resolve(self) -> Iterator[tuple[dict[str, Any] | list[Any], ConfigSource]]:
        """The core (internal) data retrieval method. Generates (value,
        source) pairs for each source that contains a value for this
        view. May raise `ConfigTypeError` if a type error occurs while
        traversing a source.
        """
        raise NotImplementedError

    def first(self) -> tuple[dict[str, Any] | list[Any], ConfigSource]:
        """Return a (value, source) pair for the first object found for
        this view. This amounts to the first element returned by
        `resolve`. If no values are available, a `NotFoundError` is
        raised.
        """
        pairs = self.resolve()
        try:
            return util.iter_first(pairs)
        except ValueError:
            raise NotFoundError(f"{self.name} not found")

    def exists(self) -> bool:
        """Determine whether the view has a setting in any source."""
        try:
            self.first()
        except NotFoundError:
            return False
        return True

    def add(self, value: Any) -> None:
        """Set the *default* value for this configuration view. The
        specified value is added as the lowest-priority configuration
        data source.
        """
        raise NotImplementedError

    def set(self, value: Any) -> None:
        """*Override* the value for this configuration view. The
        specified value is added as the highest-priority configuration
        data source.
        """
        raise NotImplementedError

    def root(self) -> RootView:
        """The RootView object from which this view is descended."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: {self.name}>"

    def __iter__(self) -> Iterator[Subview | str]:
        """Iterate over the keys of a dictionary view or the *subviews*
        of a list view.
        """
        # Try iterating over the keys, if this is a dictionary view.
        try:
            yield from self.keys()

        except ConfigTypeError:
            # Otherwise, try iterating over a list view.
            try:
                yield from self.sequence()

            except ConfigTypeError:
                item, _ = self.first()
                raise ConfigTypeError(
                    f"{self.name} must be a dictionary or a list, not "
                    f"{type(item).__name__}"
                )

    def __getitem__(self, key: ConfigKey) -> Subview:
        """Get a subview of this view."""
        return Subview(self, key)

    def __setitem__(self, key: ConfigKey, value: Any) -> None:
        """Create an overlay source to assign a given key under this
        view.
        """
        self.set({key: value})

    def __contains__(self, key: ConfigKey) -> bool:
        return self[key].exists()

    def set_args(
        self, namespace: dict[str, Any] | Namespace | Values, dots: bool = False
    ) -> None:
        """Overlay parsed command-line arguments, generated by a library
        like argparse or optparse, onto this view's value.

        :param namespace: Dictionary or Namespace to overlay this config with.
            Supports nested Dictionaries and Namespaces.
        :type namespace: dict or Namespace
        :param dots: If True, any properties on namespace that contain dots (.)
            will be broken down into child dictionaries.
            :Example:

            {'foo.bar': 'car'}
            # Will be turned into
            {'foo': {'bar': 'car'}}
        :type dots: bool
        """
        self.set(util.build_dict(namespace, sep="." if dots else ""))

    # Magical conversions. These special methods make it possible to use
    # View objects somewhat transparently in certain circumstances. For
    # example, rather than using ``view.get(bool)``, it's possible to
    # just say ``bool(view)`` or use ``view`` in a conditional.

    def __str__(self) -> str:
        """Get the value for this view as a bytestring."""
        return str(self.get())

    def __bool__(self) -> bool:
        """Gets the value for this view as a bool."""
        return bool(self.get())

    # Dictionary emulation methods.

    def keys(self) -> list[str]:
        """Returns a list containing all the keys available as subviews
        of the current views. This enumerates all the keys in *all*
        dictionaries matching the current view, in contrast to
        ``view.get(dict).keys()``, which gets all the keys for the
        *first* dict matching the view. If the object for this view in
        any source is not a dict, then a `ConfigTypeError` is raised. The
        keys are ordered according to how they appear in each source.
        """
        keys = []

        for dic, _ in self.resolve():
            try:
                cur_keys = dic.keys()  # type: ignore[union-attr]
            except AttributeError:
                raise ConfigTypeError(
                    f"{self.name} must be a dict, not {type(dic).__name__}"
                )

            for key in cur_keys:
                if key not in keys:
                    keys.append(key)

        return keys

    def items(self) -> Iterator[tuple[str, Subview]]:
        """Iterates over (key, subview) pairs contained in dictionaries
        from *all* sources at this view. If the object for this view in
        any source is not a dict, then a `ConfigTypeError` is raised.
        """
        for key in self.keys():
            yield key, self[key]

    def values(self) -> Iterator[Subview]:
        """Iterates over all the subviews contained in dictionaries from
        *all* sources at this view. If the object for this view in any
        source is not a dict, then a `ConfigTypeError` is raised.
        """
        for key in self.keys():
            yield self[key]

    # List/sequence emulation.

    def sequence(self) -> Iterator[Subview]:
        """Iterates over the subviews contained in lists from the *first*
        source at this view. If the object for this view in the first source
        is not a list or tuple, then a `ConfigTypeError` is raised.
        """
        try:
            collection, _ = self.first()
        except NotFoundError:
            return
        if not isinstance(collection, (list, tuple)):
            raise ConfigTypeError(
                f"{self.name} must be a list, not {type(collection).__name__}"
            )

        # Yield all the indices in the sequence.
        for index in range(len(collection)):
            yield self[index]

    def all_contents(self) -> Iterator[str]:
        """Iterates over all subviews from collections at this view from
        *all* sources. If the object for this view in any source is not
        iterable, then a `ConfigTypeError` is raised. This method is
        intended to be used when the view indicates a list; this method
        will concatenate the contents of the list from all sources.
        """
        for collection, _ in self.resolve():
            try:
                it = iter(collection)
            except TypeError:
                raise ConfigTypeError(
                    f"{self.name} must be an iterable, not {type(collection).__name__}"
                )
            yield from it

    # Validation and conversion.

    def flatten(self, redact: bool = False) -> OrderedDict[str, Any]:
        """Create a hierarchy of OrderedDicts containing the data from
        this view, recursively reifying all views to get their
        represented values.

        If `redact` is set, then sensitive values are replaced with
        the string "REDACTED".
        """
        od: OrderedDict[str, Any] = OrderedDict()
        for key, view in self.items():
            if redact and view.redact:
                od[key] = REDACTED_TOMBSTONE
            else:
                try:
                    od[key] = view.flatten(redact=redact)
                except ConfigTypeError:
                    od[key] = view.get()
        return od

    @overload
    def get(self, template: templates.Path) -> Path: ...
    @overload
    def get(self, template: templates.Template[R]) -> R: ...
    @overload
    def get(self, template: type[R]) -> R: ...
    @overload
    def get(self, template: Mapping[str, object]) -> templates.AttrDict[str, Any]: ...
    @overload
    def get(self, template: list[R]) -> R: ...
    @overload
    def get(self, template: templates._Required = ...) -> Any: ...
    def get(self, template: object = templates.REQUIRED) -> Any:
        """Retrieve the value for this view according to the template.

        The `template` against which the values are checked can be
        anything convertible to a `Template` using `as_template`. This
        means you can pass in a default integer or string value, for
        example, or a type to just check that something matches the type
        you expect.

        May raise a `ConfigValueError` (or its subclass,
        `ConfigTypeError`) or a `NotFoundError` when the configuration
        doesn't satisfy the template.
        """
        return templates.as_template(template).value(self, template)

    # Shortcuts for common templates.

    def as_filename(self) -> str:
        """Get the value as a path. Equivalent to `get(Filename())`."""
        return self.get(templates.Filename())

    def as_path(self) -> Path:
        """Get the value as a `pathlib.Path` object. Equivalent to `get(Path())`."""
        return self.get(templates.Path())

    def as_choice(self, choices: Sequence[R] | dict[str, R] | type[R]) -> R:
        """Get the value from a list of choices. Equivalent to
        `get(Choice(choices))`.

        Sequences, dictionaries and :class:`Enum` types are supported,
        see :class:`confuse.templates.Choice` for more details.
        """
        return self.get(templates.Choice(choices))

    def as_number(self) -> int | float:
        """Get the value as any number type: int or float. Equivalent to
        `get(Number())`.
        """
        return self.get(templates.Number())

    def as_str_seq(self, split: bool = True) -> list[str]:
        """Get the value as a sequence of strings. Equivalent to
        `get(StrSeq(split=split))`.
        """
        return self.get(templates.StrSeq(split=split))

    @overload
    def as_pairs(self, default_value: str) -> list[tuple[str, str]]: ...
    @overload
    def as_pairs(self, default_value: None = None) -> list[tuple[str, None]]: ...
    def as_pairs(
        self, default_value: str | None = None
    ) -> list[tuple[str, str]] | list[tuple[str, None]]:
        """Get the value as a sequence of pairs of two strings. Equivalent to
        `get(Pairs(default_value=default_value))`.
        """
        return self.get(templates.Pairs(default_value=default_value))  # type: ignore[return-value]

    def as_str(self) -> str:
        """Get the value as a (Unicode) string. Equivalent to
        `get(unicode)` on Python 2 and `get(str)` on Python 3.
        """
        return self.get(templates.String())

    def as_str_expanded(self) -> str:
        """Get the value as a (Unicode) string, with env vars
        expanded by `os.path.expandvars()`.
        """
        return self.get(templates.String(expand_vars=True))

    # Redaction.

    @property
    def redact(self) -> bool:
        """Whether the view contains sensitive information and should be
        redacted from output.
        """
        return () in self.get_redactions()

    @redact.setter
    def redact(self, flag: bool) -> None:
        self.set_redaction((), flag)

    def set_redaction(self, path: tuple[ConfigKey, ...], flag: bool) -> None:
        """Add or remove a redaction for a key path, which should be an
        iterable of keys.
        """
        raise NotImplementedError()

    def get_redactions(self) -> Iterable[tuple[ConfigKey, ...]]:
        """Get the set of currently-redacted sub-key-paths at this view."""
        raise NotImplementedError()


class RootView(ConfigView):
    """The base of a view hierarchy. This view keeps track of the
    sources that may be accessed by subviews.
    """

    def __init__(self, sources: Iterable[ConfigSource]) -> None:
        """Create a configuration hierarchy for a list of sources. At
        least one source must be provided. The first source in the list
        has the highest priority.
        """
        self.sources: list[ConfigSource] = list(sources)
        self.name = ROOT_NAME
        self.redactions: set[tuple[ConfigKey, ...]] = set()

    def add(self, value: Any) -> None:
        self.sources.append(ConfigSource.of(value))

    def set(self, value: Any) -> None:
        self.sources.insert(0, ConfigSource.of(value))

    def resolve(self) -> Iterator[tuple[dict[str, Any] | list[Any], ConfigSource]]:
        return ((dict(s), s) for s in self.sources)

    def clear(self) -> None:
        """Remove all sources (and redactions) from this
        configuration.
        """
        del self.sources[:]
        self.redactions.clear()

    def root(self) -> Self:
        return self

    def set_redaction(self, path: tuple[ConfigKey, ...], flag: bool) -> None:
        if flag:
            self.redactions.add(path)
        elif path in self.redactions:
            self.redactions.remove(path)

    def get_redactions(self) -> builtins.set[tuple[ConfigKey, ...]]:
        return self.redactions


class Subview(ConfigView):
    """A subview accessed via a subscript of a parent view."""

    def __init__(self, parent: ConfigView, key: ConfigKey) -> None:
        """Make a subview of a parent view for a given subscript key."""
        self.parent = parent
        self.key = key

        # Choose a human-readable name for this view.
        if isinstance(self.parent, RootView):
            self.name = ""
        else:
            self.name = self.parent.name
            if not isinstance(self.key, int):
                self.name += "."
        if isinstance(self.key, int):
            self.name += f"#{self.key}"
        elif isinstance(self.key, bytes):
            self.name += self.key.decode("utf-8")
        elif isinstance(self.key, str):
            self.name += self.key
        else:
            self.name += repr(self.key)

    def resolve(self) -> Iterator[tuple[dict[str, Any] | list[Any], ConfigSource]]:
        for collection, source in self.parent.resolve():
            try:
                value = collection[self.key]  # type: ignore[index]
            except IndexError:
                # List index out of bounds.
                continue
            except KeyError:
                # Dict key does not exist.
                continue
            except TypeError:
                # Not subscriptable.
                raise ConfigTypeError(
                    f"{self.parent.name} must be a collection, not "
                    f"{type(collection).__name__}"
                )
            yield value, source

    def set(self, value: Any) -> None:
        self.parent.set({self.key: value})

    def add(self, value: Any) -> None:
        self.parent.add({self.key: value})

    def root(self) -> RootView:
        return self.parent.root()

    def set_redaction(self, path: tuple[ConfigKey, ...], flag: bool) -> None:
        self.parent.set_redaction((self.key, *path), flag)

    def get_redactions(self) -> Iterable[tuple[ConfigKey, ...]]:
        return (
            kp[1:] for kp in self.parent.get_redactions() if kp and kp[0] == self.key
        )


# Main interface.


class Configuration(RootView):
    def __init__(
        self,
        appname: str,
        modname: str | None = None,
        read: bool = True,
        loader: type[yaml_util.Loader] = yaml_util.Loader,
    ):
        """Create a configuration object by reading the
        automatically-discovered config files for the application for a
        given name. If `modname` is specified, it should be the import
        name of a module whose package will be searched for a default
        config file. (Otherwise, no defaults are used.) Pass `False` for
        `read` to disable automatic reading of all discovered
        configuration files. Use this when creating a configuration
        object at module load time and then call the `read` method
        later. Specify the Loader class as `loader`.
        """
        super().__init__([])
        self.appname = appname
        self.modname = modname
        self.loader = loader

        # Resolve default source location. We do this ahead of time to
        # avoid unexpected problems if the working directory changes.
        if self.modname:
            self._package_path = util.find_package_path(self.modname)
        else:
            self._package_path = None

        self._env_var = f"{self.appname.upper()}DIR"

        if read:
            self.read()

    def user_config_path(self) -> str:
        """Points to the location of the user configuration.

        The file may not exist.
        """
        return os.path.join(self.config_dir(), CONFIG_FILENAME)

    def _add_user_source(self) -> None:
        """Add the configuration options from the YAML file in the
        user's configuration directory (given by `config_dir`) if it
        exists.
        """
        filename = self.user_config_path()
        self.add(YamlSource(filename, loader=self.loader, optional=True))

    def _add_default_source(self) -> None:
        """Add the package's default configuration settings. This looks
        for a YAML file located inside the package for the module
        `modname` if it was given.
        """
        if self.modname:
            if self._package_path:
                filename = os.path.join(self._package_path, DEFAULT_FILENAME)
                self.add(
                    YamlSource(
                        filename, loader=self.loader, optional=True, default=True
                    )
                )

    def read(self, user: bool = True, defaults: bool = True) -> None:
        """Find and read the files for this configuration and set them
        as the sources for this configuration. To disable either
        discovered user configuration files or the in-package defaults,
        set `user` or `defaults` to `False`.
        """
        if user:
            self._add_user_source()
        if defaults:
            self._add_default_source()

    def config_dir(self) -> str:
        """Get the path to the user configuration directory. The
        directory is guaranteed to exist as a postcondition (one may be
        created if none exist).

        If the application's ``...DIR`` environment variable is set, it
        is used as the configuration directory. Otherwise,
        platform-specific standard configuration locations are searched
        for a ``config.yaml`` file. If no configuration file is found, a
        fallback path is used.
        """
        # If environment variable is set, use it.
        if self._env_var in os.environ:
            appdir = os.environ[self._env_var]
            appdir = os.path.abspath(os.path.expanduser(appdir))
            if os.path.isfile(appdir):
                raise ConfigError(f"{self._env_var} must be a directory")

        else:
            # Search platform-specific locations. If no config file is
            # found, fall back to the first directory in the list.
            configdirs = util.config_dirs()
            for confdir in configdirs:
                appdir = os.path.join(confdir, self.appname)
                if os.path.isfile(os.path.join(appdir, CONFIG_FILENAME)):
                    break
            else:
                appdir = os.path.join(configdirs[0], self.appname)

        # Ensure that the directory exists.
        try:
            os.makedirs(appdir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        return appdir

    def set_file(self, filename: str, base_for_paths: bool = False) -> None:
        """Parses the file as YAML and inserts it into the configuration
        sources with highest priority.

        :param filename: Filename of the YAML file to load.
        :param base_for_paths: Indicates whether the directory containing the
            YAML file will be used as the base directory for resolving relative
            path values stored in the YAML file. Otherwise, by default, the
            directory returned by `config_dir()` will be used as the base.
        """
        self.set(
            YamlSource(filename, base_for_paths=base_for_paths, loader=self.loader)
        )

    def set_env(self, prefix: str | None = None, sep: str = "__") -> None:
        """Create a configuration overlay at the highest priority from
        environment variables.

        After prefix matching and removal, environment variable names will be
        converted to lowercase for use as keys within the configuration. If
        there are nested keys, list-like dicts (ie, `{0: 'a', 1: 'b'}`) will
        be converted into corresponding lists (ie, `['a', 'b']`). The values
        of all environment variables will be parsed as YAML scalars using the
        `self.loader` Loader class to ensure type conversion is consistent
        with YAML file sources. Use the `EnvSource` class directly to load
        environment variables using non-default behavior and to enable full
        YAML parsing of values.

        :param prefix: The prefix to identify the environment variables to use.
            Defaults to uppercased `self.appname` followed by an underscore.
        :param sep: Separator within variable names to define nested keys.
        """
        if prefix is None:
            prefix = f"{self.appname.upper()}_"
        self.set(EnvSource(prefix, sep=sep, loader=self.loader))

    def dump(self, full: bool = True, redact: bool = False) -> str:
        """Dump the Configuration object to a YAML file.

        The order of the keys is determined from the default
        configuration file. All keys not in the default configuration
        will be appended to the end of the file.

        :param full:      Dump settings that don't differ from the defaults
                          as well
        :param redact:    Remove sensitive information (views with the `redact`
                          flag set) from the output
        """
        if full:
            out_dict = self.flatten(redact=redact)
        else:
            # Exclude defaults when flattening.
            sources = [s for s in self.sources if not s.default]
            temp_root = RootView(sources)
            temp_root.redactions = self.redactions
            out_dict = temp_root.flatten(redact=redact)

        yaml_out = yaml.dump(
            out_dict,
            Dumper=yaml_util.Dumper,
            default_flow_style=None,
            indent=4,
            width=1000,
        )

        # Restore comments to the YAML text.
        default_source = None
        for source in self.sources:
            if source.default:
                default_source = source
                break
        if default_source and default_source.filename:
            with open(default_source.filename, "rb") as fp:
                default_data = fp.read()
            yaml_out = yaml_util.restore_yaml_comments(
                yaml_out, default_data.decode("utf-8")
            )

        return yaml_out

    def reload(self) -> None:
        """Reload all sources from the file system.

        This only affects sources that come from files (i.e.,
        `YamlSource` objects); other sources, such as dictionaries
        inserted with `add` or `set`, will remain unchanged.
        """
        for source in self.sources:
            if isinstance(source, YamlSource):
                source.load()


class LazyConfig(Configuration):
    """A Configuration at reads files on demand when it is first
    accessed. This is appropriate for using as a global config object at
    the module level.
    """

    def __init__(self, appname: str, modname: str | None = None) -> None:
        super().__init__(appname, modname, False)
        self._materialized = False  # Have we read the files yet?
        self._lazy_prefix: list[
            ConfigSource
        ] = []  # Pre-materialization calls to set().
        self._lazy_suffix: list[ConfigSource] = []  # Calls to add().

    def read(self, user: bool = True, defaults: bool = True) -> None:
        self._materialized = True
        super().read(user, defaults)

    def resolve(self) -> Iterator[tuple[dict[str, Any] | list[Any], ConfigSource]]:
        if not self._materialized:
            # Read files and unspool buffers.
            self.read()
            self.sources += self._lazy_suffix
            self.sources[:0] = self._lazy_prefix
        return super().resolve()

    def add(self, value: Any) -> None:
        super().add(value)
        if not self._materialized:
            # Buffer additions to end.
            self._lazy_suffix += self.sources
            del self.sources[:]

    def set(self, value: Any) -> None:
        super().set(value)
        if not self._materialized:
            # Buffer additions to beginning.
            self._lazy_prefix[:0] = self.sources
            del self.sources[:]

    def clear(self) -> None:
        """Remove all sources from this configuration."""
        super().clear()
        self._lazy_suffix = []
        self._lazy_prefix = []


# "Validated" configuration views: experimental!
