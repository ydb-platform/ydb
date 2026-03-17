# -*- coding:utf-8 -*-

#  ************************** Copyrights and license ***************************
#
# This file is part of gcovr 8.6, a parsing and reporting tool for gcov.
# https://gcovr.com/en/8.6
#
# _____________________________________________________________________________
#
# Copyright (c) 2013-2026 the gcovr authors
# Copyright (c) 2013 Sandia Corporation.
# Under the terms of Contract DE-AC04-94AL85000 with Sandia Corporation,
# the U.S. Government retains certain rights in this software.
#
# This software is distributed under the 3-clause BSD License.
# For more information, see the README.rst file.
#
# ****************************************************************************

from __future__ import annotations
from abc import abstractmethod
from argparse import ArgumentParser, ArgumentTypeError, Namespace
import argparse
import platform
import re
from typing import Any, Type, Callable
import os

from .filter import AbsoluteFilter, Filter, RelativeFilter
from .logging import LOGGER


def check_percentage(value: str) -> float:
    r"""
    Check that the percentage is within a reasonable range and if so return it.
    """

    # strip trailing percent sign if present, useful for config files
    if value.endswith("%"):
        value = value[:-1]

    try:
        x = float(value)
        if not (0.0 <= x <= 100.0):
            raise ValueError()
    except ValueError:
        raise ArgumentTypeError(f"{value} not in range [0.0, 100.0]") from None
    return x


def check_input_file(value: str, basedir: str | None = None) -> str:
    r"""
    Check that the input file is present. Return the full path.
    """
    if basedir is None:
        basedir = os.getcwd()

    if not os.path.isabs(value):
        value = os.path.join(basedir, value)
    value = os.path.normpath(value)

    if not os.path.isfile(value):
        raise ArgumentTypeError(
            f"Should be a file that already exists: {value!r}"
        ) from None

    return os.path.abspath(value)


def relative_path(value: str, basedir: str | None = None) -> str:
    r"""
    Make a absolute path if value is a relative path.
    """
    if not value:
        raise ArgumentTypeError("Should not be set to an empty string.") from None

    if basedir is None:
        basedir = os.getcwd()

    if not os.path.isabs(value):
        value = os.path.join(basedir, value)
    value = os.path.normpath(value)
    return os.path.relpath(value, os.getcwd())


class FilterOption:
    """Argparse type for filter options."""

    def __init__(self, regex: str, path_context: str | None = None) -> None:
        self.regex = regex
        self.path_context = os.getcwd() if path_context is None else path_context

    def build_filter(self) -> Filter:
        """Return the filter object depending on the given RegEx."""
        # Try to detect unintended backslashes and warn.
        # Later, the regex engine may or may not raise a syntax error.
        # An unintended backslash is a literal backslash r"\\",
        # or a regex escape that doesn't exist.
        (suggestion, bs_count) = re.subn(
            r"\\\\|\\(?=[^\WabfnrtuUvx0-9AbBdDsSwWZ])", "/", self.regex
        )
        if bs_count:
            LOGGER.warning(
                "Filters must use forward slashes as path separators, your filter >>%s<<.",
                self.regex,
            )
            LOGGER.warning("Did you mean >>%s<<?", suggestion)

        if self.regex.startswith("/") or (
            (platform.system() == "Windows") and re.match(r"^[A-Za-z]:/", self.regex)
        ):
            return AbsoluteFilter(self.regex)

        return RelativeFilter(self.path_context, self.regex)


class NonEmptyFilterOption(FilterOption):
    """Argparse type to check filters."""

    def __init__(self, regex: str, path_context: str | None = None) -> None:
        if not regex:
            raise ArgumentTypeError("filter cannot be empty")
        super().__init__(regex, path_context)


class OutputOrDefault:
    """An output path that may be empty.

    - ``None``: the option is not set
    - ``OutputOrDefault(None)``: fall back to some default value
    - ``OutputOrDefault(path)``: use that path
    """

    def __init__(self, value: str | None, basedir: str | None = None) -> None:
        self.value = value
        self._check_output_and_make_abspath(os.getcwd() if basedir is None else basedir)

    def __repr__(self) -> str:
        name = self.__class__.__name__
        value = self.value
        return f"{name}({value!r})"

    def _check_output_and_make_abspath(self, basedir: str) -> None:
        r"""
        Check if the output file can be created.
        """

        if self.value in (None, "-"):
            self.abspath = "-"
            self.is_dir = False
        else:
            # Replace / and \ with the os path separator.
            value = str(self.value).replace("\\", os.sep).replace("/", os.sep)
            # Save if it is a directory
            self.is_dir = value.endswith(os.sep)
            value = os.path.normpath(value)
            if self.is_dir:
                value += os.sep

            if not os.path.isabs(value):
                value = os.path.join(basedir, value)
            self.abspath = value

            if self.is_dir:
                # Now normalize and add the trailing slash after creating the directory.
                if not os.path.isdir(value):
                    try:
                        os.mkdir(value)
                    except OSError as e:
                        raise ArgumentTypeError(
                            f"Could not create output directory {self.value!r}: {e.strerror}"
                        ) from None
            else:
                try:
                    with open(value, "w", encoding="utf-8") as _:
                        pass
                except OSError as e:
                    raise ArgumentTypeError(
                        f"Could not create output file {self.value!r}: {e.strerror}"
                    ) from None
                os.unlink(value)

    @classmethod
    def choose(
        cls,
        choices: list[OutputOrDefault | None],
        default: OutputOrDefault | None = None,
    ) -> OutputOrDefault | None:
        """select the first choice that contains a value

        Example: chooses a truthy value over None:
        >>> OutputOrDefault.choose([None, OutputOrDefault(42)])
        OutputOrDefault(42)

        Example: chooses a truthy value over empty value:
        >>> OutputOrDefault.choose([OutputOrDefault(None), OutputOrDefault('x')])
        OutputOrDefault('x')

        Example: chooses default when given empty list
        >>> OutputOrDefault.choose([], default=OutputOrDefault('default'))
        OutputOrDefault('default')

        Example: chooses default when only given false values:
        >>> OutputOrDefault.choose(
        ...     [None, OutputOrDefault(None)],
        ...     default=OutputOrDefault('default'))
        OutputOrDefault('default')

        Example: throws when given other value
        >>> OutputOrDefault.choose([True])
        Traceback (most recent call last):
          ...
        TypeError: ...
        """
        for choice in choices:
            if choice is None:
                continue
            if not isinstance(choice, OutputOrDefault):
                raise TypeError(f"expected OutputOrDefault instance, got: {choice}")
            if choice.value is not None:
                return choice
        return default


class Options:
    """Wrapper for holding the configuration."""

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def get(self, name: str) -> Any:
        """Function to get an option by name."""
        return self.__dict__.get(name)


class GcovrConfigOptionAction(argparse.Action):  # pylint: disable=abstract-method
    """Abstract class to be detect our own actions."""

    @abstractmethod
    def store_config_key(
        self, namespace: dict[str, Any], values: Any, config: str | None
    ) -> None:
        """Method to store a configuration key."""


class GcovrDeprecatedConfigOptionAction(GcovrConfigOptionAction):
    """Argparse action for deprecated options to map on new option with a deprecation warning."""

    def __init__(self, option_strings: list[str], dest: str, **kwargs: Any) -> None:
        super().__init__(option_strings, dest, **kwargs)

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        """Used by argparse to store the values."""
        LOGGER.warning(
            "Deprecated option %s used, please use '%s %s' instead.",
            option_string,
            self.option,
            self.value,
        )
        setattr(namespace, self.dest, self.value)

    def store_config_key(
        self, namespace: dict[str, Any], values: Any, config: str | None
    ) -> None:
        if config is not None:
            LOGGER.warning(
                "Deprecated config key %s used, please use '%s=%s' instead.",
                config,
                self.config,
                self.value,
            )
        namespace[self.dest] = values


class GcovrConfigOption:
    # pylint: disable=too-many-instance-attributes
    # pylint: disable=too-few-public-methods
    # pylint: disable=redefined-builtin
    r"""
    Represents a single setting for a gcovr runtime parameter.

    Gcovr can be extensively configured through a series of options,
    representing these options as a simple class object allows them to be
    portability re-used in multiple configuration schemes. This is implemented
    in a way similar to how options are defined in argparse. The converter
    keyword argument is expected to return a valid conversion of a string
    value or throw an error.

    Arguments:
        name (str):
            Destination (options object field),
            must be valid Python identifier.
        flags (list of str, optional):
            Any command line flags.

    Keyword Arguments:
        action (str, optional):
            What to do when the option is parsed:
            - store (default): store the option argument
            - store_const: store the const value
            - store_true, store_false: shortcuts for store_const
            - append: append the option argument
            (Compare also the *argparse* documentation.)
        choices (list, optional):
            Value must be one of these after conversion.
        config (str or bool, optional):
            Configuration file key.
            If absent, the first ``--flag`` is used without the leading dashes.
            If explicitly set to False,
            the option cannot be set from a config file.
        const (any, optional):
            Assigned by the "store_const" action.
        const_negate (any, optional):
            Generate a "--no-foo" negation flag with the given "const" value.
        default (any, optional):
            Default value if the option is not found, defaults to None.
        group (str, optional):
            Name of the option group in GCOVR_CONFIG_OPTION_GROUPS.
            Only relevant for documentation purposes.
        help (str):
            Help message.
            Must display well on terminal *and* render as Restructured Text.
            Any named curly-brace placeholders
            are filled in from the option attributes via ``str.format()``.
        metavar (str, optional):
            Name of the value in help messages, defaults to the name.
        nargs (int or '+', '*', '?', optional):
            How often the option may occur.
            Special case for "?": if the option exists but has no value,
            the const value is stored.
        positional (bool, optional):
            Whether this is a positional option, defaults to False.
            A positional argument cannot have flags.
        required (bool, optional):
            Whether this option is required, defaults to False.
        type (function, optional):
            Check and convert the option value, may throw exceptions.

    Constraint: an option must be either have a flag or be positional
    or have a config key, or a combination thereof.
    """

    def __init__(
        self,
        name: str,
        flags: list[str] | None = None,
        *,
        help: str,
        action: str | Type[GcovrConfigOptionAction] = "store",
        choices: tuple[int, ...] | tuple[str, ...] | None = None,
        const: Any = None,
        const_negate: Any = None,
        config: str | bool = True,
        default: Any = None,
        group: str | None = None,
        metavar: str | None = None,
        nargs: int | str | None = None,
        positional: bool = False,
        required: bool = False,
        type: Callable[[str], Any] | Type[FilterOption] | None = None,
    ) -> None:
        if flags is None:
            flags = []

        if flags and positional:
            raise AssertionError("Option cannot have flags and be positional")

        config_keys = _derive_configuration_key(config, flags=flags)
        del config

        if not (flags or positional or config_keys):
            raise AssertionError(
                "Option must be named, positional, or config argument."
            )

        negate = list[str]()
        if flags and const_negate is not None:
            negate = ["--no-" + f[2:] for f in flags if f.startswith("--")]
            if not negate:
                raise AssertionError("Cannot autogenerate negation")

        if not help:
            raise AssertionError("help required")
        if negate:
            help += f" Negation: {', '.join(negate)}."
        if (flags or positional) and config_keys:
            config_keys_help = []
            for config_key in config_keys:
                config_keys_help.append(config_key)
            help += f" Config key(s): {', '.join(config_keys_help)}."

        # the store_true and store_false actions have hardcoded boolean
        # constants in their definitions so they need switched to the generic
        # store_const in order for the logic here to work correctly.
        if action == "store_true":
            if const is not None:
                raise AssertionError("action=store_true and const conflict")
            if default is not None:
                raise AssertionError("action=store_true and default conflict")
            action = "store_const"
            const = True
            default = False
        elif action == "store_false":
            if const is not None:
                raise AssertionError("action=store_false and const conflict")
            if default is not None:
                raise AssertionError("action=store_false and default conflict")
            action = "store_const"
            const = False
            default = True

        if not (
            action in ("store", "store_const", "append")
            or issubclass(action, GcovrConfigOptionAction)  # type: ignore [arg-type]
        ):
            raise AssertionError(f"Unknown action {action!r}")

        self.name = name
        self.flags = flags

        self.action = action
        self.choices = choices
        self.config_keys = config_keys
        self.const = const
        self.const_negate = const_negate
        self.default = default
        self.group = group
        self.help = ""  # assigned later
        self.metavar = metavar
        self.nargs = nargs
        self.negate = negate
        self.positional = positional
        self.required = required
        self.type = type

        # format the help
        self.help = help.format(**self.__dict__)

    def __repr__(self) -> str:
        r"""String representation of instance.

        >>> GcovrConfigOption('foo', ['-f', '--foo'], help="foo text.")
        GcovrConfigOption('foo', [-f, --foo], ..., help='foo text. Config key(s): foo.', ...)
        """
        name = self.name
        flags = ", ".join(self.flags)
        kwargs = ", ".join(
            f"{k}={v!r}"
            for k, v in sorted(self.__dict__.items())
            if k not in ("name", "flags")
        )

        return f"GcovrConfigOption({name!r}, [{flags}], {kwargs})"


def _derive_configuration_key(
    config: str | bool,
    *,
    flags: list[str],
) -> list[str] | None:
    if config is True:
        config_keys = []
        for flag in flags:
            if flag.startswith("--"):
                config_keys.append(flag.lstrip("-"))
        if not config_keys:
            raise AssertionError("Could not autogenerate config key from {flags!r}.")
        return config_keys
    if config is False:
        return None
    if isinstance(config, str):
        return [config]

    raise AssertionError(f"Unexpected config entry type {config!r}")
