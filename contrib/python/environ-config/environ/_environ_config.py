# SPDX-License-Identifier: Apache-2.0
#
# Copyright 2017 Hynek Schlawack
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import os

from typing import Any, Callable, TypeVar, overload

import attr

from .exceptions import MissingEnvValueError


CNF_KEY = "environ_config"
log = logging.getLogger(CNF_KEY)


# We define a sentinel for when prefixes are not set and special handling for
# what value to use at the app level when that is the case
class Sentinel:
    def __init__(self, bool_=True):
        self._bool = bool(bool_)

    def __bool__(self):
        return self._bool


PREFIX_NOT_SET = Sentinel(False)
DEFAULT_PREFIX = "APP"


def _get_prefix(obj):
    """
    Get the prefix of an environ object or the default value if it is not set.
    """
    return obj._prefix if obj._prefix is not PREFIX_NOT_SET else DEFAULT_PREFIX


@attr.s
class Raise:
    pass


RAISE = Raise()


T = TypeVar("T")


@overload
def config(
    *,
    prefix: str | Sentinel = PREFIX_NOT_SET,
    from_environ: str = "from_environ",
    generate_help: str = "generate_help",
    frozen: bool = False,
) -> Callable[[type[T]], type[T]]: ...


@overload
def config(maybe_cls: type[T]) -> type[T]: ...


def config(
    maybe_cls: type[T] | None = None,
    *,
    prefix: str | Sentinel = PREFIX_NOT_SET,
    from_environ: str = "from_environ",
    generate_help: str = "generate_help",
    frozen: bool = False,
) -> type[T] | Callable[[type[T]], type[T]]:
    """
    Make a class a configuration class.

    Args:
        prefix:
            The prefix that is used for the env variables.  If you have an
            `var` attribute on the class and you leave the default argument
            value of *PREFIX_NOT_SET*, the *DEFAULT_PREFIX* value of ``APP``
            will be used and *environ-config* will look for an environment
            variable called ``APP_VAR``.

        from_environ:
            If not `None`, attach a config loading method with the name
            *from_environ* to the class.  See `to_config` for more information.

        generate_help:
            If not `None`, attach a config loading method with the name
            *generate_help* to the class.  See `generate_help` for more
            information.

        frozen:
            The configuration will be immutable after instantiation, if `True`.

    .. versionadded:: 19.1.0
       *from_environ*
    .. versionadded:: 19.1.0
       *generate_help*
    .. versionadded:: 20.1.0
       *frozen*
    .. versionchanged:: 21.1.0
       *prefix* now defaults to *PREFIX_NOT_SET* instead of ``APP``.
    """

    def wrap(cls):
        def from_environ_fnc(cls, environ=os.environ):
            return __to_config(cls, environ)

        def generate_help_fnc(cls, **kwargs):
            return __generate_help(cls, **kwargs)

        cls._prefix = prefix
        if from_environ is not None:
            setattr(cls, from_environ, classmethod(from_environ_fnc))
        if generate_help is not None:
            setattr(cls, generate_help, classmethod(generate_help_fnc))
        return attr.s(cls, frozen=frozen, slots=True)

    if maybe_cls is None:
        return wrap

    return wrap(maybe_cls)


@attr.s(slots=True)
class _ConfigEntry:
    name: str | None = attr.ib(default=None)
    default: Any = attr.ib(default=RAISE)
    sub_cls: type | None = attr.ib(default=None)
    callback: Callable | None = attr.ib(default=None)
    help: str | None = attr.ib(default=None)


def var(
    default: Any = RAISE,
    converter: Callable | None = None,
    name: str | None = None,
    validator: Callable | None = None,
    help: str | None = None,
) -> Any:
    """
    Declare a configuration attribute on the body of `config`-decorated class.

    It will be attempted to be filled from an environment variable based on the
    prefix and *name*.

    Args:
        default: Setting this to a value makes the config attribute optional.

        name:
            Overwrite name detection with a string.  If not set, the name of
            the attribute is used.

        converter:
            A callable that is run with the found value and its return value is
            used.  Please not that it is also run for default values.

        validator:
            A callable that is run with the final value. See *attrs*'s `chapter
            on validation
            <https://www.attrs.org/en/stable/init.html#validators>`_ for
            details. You can also use any validator that is `shipped with attrs
            <https://www.attrs.org/en/stable/api.html#validators>`_.

        help: A help string that is used by `generate_help`.
    """
    return attr.ib(
        default=default,
        metadata={CNF_KEY: _ConfigEntry(name, default, None, None, help)},
        converter=converter,
        validator=validator,
    )


def _env_to_bool(val: str | bool) -> bool:
    """
    Convert *val* to a bool if it's not a bool in the first place.
    """
    if isinstance(val, bool):
        return val
    return val.strip().lower() in ("1", "true", "yes")


def bool_var(
    default: Any = RAISE, name: str | None = None, help: str | None = None
) -> Any:
    """
    Like `var`, but converts the value to a `bool`.

    The following values are considered `True`:

    - ``True`` (if you set a *default*)
    - ``"1"``
    - ``"true"``
    - ``"yes"``

    Every other value is interpreted as `False`.  Leading and trailing
    whitespace is ignored.
    """
    return var(default=default, name=name, converter=_env_to_bool, help=help)


def group(cls: type[T], optional: bool = False) -> T:
    """
    A configuration attribute that is another configuration class.

    This way you can nest your configuration hierarchically although the values
    are coming from a flat source.

    The group's name is used to build a namespace::

       @environ.config
       class Cfg:
           @environ.config
           class Sub:
               x = environ.var()

           sub = environ.group(Sub)

    The value of ``x`` is looked up using ``APP_SUB_X``.

    You can nest your configuration as deeply as you wish.

    The *optional* keyword argument can be used to mark a *group* referencing
    a "child" *config* object so that if all variables defined in the child
    (including sub-groups) are not present in the environment being parsed, the
    attribute corresponding to the *optional* *group* will be set to `None`.

    Args:
        optional: Mark this group as *optional*. Defaults to `False`.

    Returns:
        An attribute which will be used as a nested *group* of variables.

    .. versionadded:: 21.1.0 *optional*
    """
    default = None if optional else RAISE
    return attr.ib(
        default=default,
        metadata={CNF_KEY: _ConfigEntry(None, default, cls, True)},
    )


def _default_getter(environ, metadata, prefix, name):
    """
    This default lookup implementation simply gets values from *environ*.
    """
    ce = metadata[CNF_KEY]
    var = ce.name if ce.name is not None else "_".join((*prefix, name)).upper()
    log.debug("looking for env var '%s'.", var)
    try:
        return environ[var]
    except KeyError:
        raise MissingEnvValueError(var) from None


def _to_config_recurse(config_cls, environ, prefixes, default=RAISE):
    """
    Traverse *config_cls* to construct an instance with values from *environ*.

    This function walks through a potential tree of config definition classes
    and uses the specified (via attributes set through class construction) or
    default implementation of config variable lookup to collect values from the
    provided *environ* object. The collected configuration values (including
    sub-config objects, e.g. for groups) are used to instantiate the
    well-structured *config_cls* with those values being accessible via the new
    object's attributes.
    """
    # We keep track of values we actually got from the getter vs those we set
    # from the `ConfigEntry` default value
    got = {}
    defaulted = {}
    missing_vars = set()

    for attr_obj in attr.fields(config_cls):
        try:
            ce = attr_obj.metadata[CNF_KEY]
        except KeyError:
            continue
        name = attr_obj.name

        if ce.sub_cls is not None:
            prefix = ce.sub_cls._prefix or name
            got[name] = _to_config_recurse(
                ce.sub_cls, environ, (*prefixes, prefix), default=ce.default
            )
        else:
            getter = ce.callback or _default_getter
            try:
                got[name] = getter(environ, attr_obj.metadata, prefixes, name)
            except MissingEnvValueError as exc:
                if isinstance(ce.default, Raise):
                    missing_vars |= set(exc.args)
                else:
                    defaulted[name] = (
                        attr.NOTHING
                        if isinstance(ce.default, attr.Factory)
                        else ce.default
                    )

    if missing_vars:
        # If we were told to raise OR if we got *any* values for our attrs, we
        # will raise a `MissingEnvValueError` with all the missing variables
        if isinstance(default, Raise) or got:
            raise MissingEnvValueError(*missing_vars) from None

        # Otherwise we will simply use the default passed into this call.
        # Should be no need to handle `Factory`s here.
        return default

    # Merge the defaulted and actually collected values into the config type
    defaulted.update(got)
    return config_cls(**defaulted)


def to_config(config_cls: type[T], environ: dict[str, str] = os.environ) -> T:
    """
    Load the configuration as declared by *config_cls* from *environ*.

    Args:
        config_cls: The configuration class to fill.

        environ: Source of the configuration.  `os.environ` by default.

    Returns:
        An instance of *config_cls*.

    This is equivalent to calling ``config_cls.from_environ()``.
    """
    # The canonical app prefix might be falsey in which case we'll still set
    # the default prefix for this top level config object
    app_prefix = tuple(p for p in (_get_prefix(config_cls),) if p)
    return _to_config_recurse(config_cls, environ, app_prefix)


def _format_help_dicts(help_dicts, display_defaults=False):
    """
    Format the output of _generate_help_dicts into a str.
    """
    help_strs = []
    for help_dict in help_dicts:
        help_str = "{} ({}".format(
            help_dict["var_name"],
            "Required" if help_dict["required"] else "Optional",
        )
        if help_dict.get("default") and display_defaults:
            help_str += f", Default={help_dict['default']})"
        else:
            help_str += ")"
        if help_dict.get("help_str"):
            help_str += f": {help_dict['help_str']}"
        help_strs.append(help_str)

    return "\n".join(help_strs)


def _generate_var_name(prefix, field_name):
    """
    Generate the environment variable name, given a prefix and the
    configuration field name.

    Examples:

    >>> _generate_var_name("", "some_var")
    "SOME_VAR"
    >>> _generate_var_name("my_app", "some_var")
    "MY_APP_SOME_VAR"

    Args:
        prefix: the prefix to be used, can be empty

        field_name: the name of the field from which the variable is derived
    """
    return f"{prefix}_{field_name}".upper() if prefix else field_name.upper()


def _generate_new_prefix(current_prefix, class_name):
    """
    Generate the new prefix to be used when handling nested configurations.

    Examples:

    >>> _generate_new_prefix("", "config_group_1")
    "CONFIG_GROUP_1"
    >>> _generate_new_prefix("my_app", "another_config_group")
    "MY_APP_ANOTHER_CONFIG_GROUP"

    Args:
        prefix: the prefix to be used, can be empty

        field_name: the name of the field from which the variable is derived
    """
    return (
        f"{current_prefix}_{class_name}".upper()
        if current_prefix
        else class_name.upper()
    )


def _generate_help_dicts(config_cls, _prefix=PREFIX_NOT_SET):
    """
    Generate dictionaries for use in building help strings.

    Every dictionary includes the keys...

    var_name: The env var that should be set to populate the value.
    required: A bool, True if the var is required, False if it's optional.

    Conditionally, the following are included...

    default: Included if an optional variable has a default set
    help_str: Included if the var uses the help kwarg to provide additional
        context for the value.

    Conditional key inclusion is meant to differentiate between exclusion
    vs explicitly setting a value to None.
    """
    help_dicts = []
    _prefix = _prefix if _prefix is not PREFIX_NOT_SET else config_cls._prefix
    for a in attr.fields(config_cls):
        try:
            ce = a.metadata[CNF_KEY]
        except KeyError:
            continue
        if ce.sub_cls is None:  # Base case for "leaves".
            if ce.name is None:
                var_name = _generate_var_name(_prefix, a.name)
            else:
                var_name = ce.name
            req = isinstance(ce.default, Raise)
            help_dict = {"var_name": var_name, "required": req}
            if not req:
                help_dict["default"] = ce.default
            if ce.help is not None:
                help_dict["help_str"] = ce.help
            help_dicts.append(help_dict)
        else:  # Construct the new prefix and recurse.
            help_dicts += _generate_help_dicts(
                ce.sub_cls, _prefix=_generate_new_prefix(_prefix, a.name)
            )
    return help_dicts


def generate_help(
    config_cls: type[T], formatter: Callable | None = None, **kwargs: Any
) -> str:
    """
    Autogenerate a help string for a config class.

    Args:
        formatter:
            A callable that will be called with the help dictionaries as an
            argument and the remaining *kwargs*.  It should return the help
            string.

        display_defaults (bool):
            When using the default formatter, passing `True` for
            *display_defaults* makes the default values part of the output.

    Returns:
        A help string that can be printed to the user.

    This is equivalent to calling ``config_cls.generate_help()``.

    .. versionadded:: 19.1.0
    """
    if formatter is None:
        formatter = _format_help_dicts
    help_dicts = _generate_help_dicts(config_cls)

    return formatter(help_dicts, **kwargs)


# We need these aliases because of a name clash with a function argument.
__generate_help = generate_help
__to_config = to_config
