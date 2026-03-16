"""Inspecting module"""

from __future__ import annotations

import json
import sys
from contextlib import suppress
from functools import partial
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version
from pathlib import PosixPath
from typing import Any
from typing import Callable
from typing import Literal
from typing import Protocol
from typing import TextIO
from typing import TYPE_CHECKING
from typing import Union

from dynaconf.loaders.base import SourceMetadata
from dynaconf.utils.boxing import DynaBox
from dynaconf.utils.functional import empty
from dynaconf.utils.parse_conf import Lazy
from dynaconf.vendor.box.box_list import BoxList
from dynaconf.vendor.ruamel.yaml import YAML

if TYPE_CHECKING:  # pragma: no cover
    from dynaconf.base import LazySettings
    from dynaconf.base import Settings


# Dumpers config

json_pretty = partial(json.dump, indent=2, default=str)
json_compact = partial(json.dump, default=str)


def yaml_dumper_with_defaults(data: dict, text_stream: TextIO) -> None:
    """Easier way to get YAML dumper to handle unseralizable types"""
    yaml = YAML()
    # let JSON handle unserializable types and then load it back
    data = json.loads(json.dumps(data, default=str))
    yaml.default_flow_style = False
    yaml.dump(data, text_stream)


builtin_dumpers = {
    "yaml": yaml_dumper_with_defaults,
    "json": json_pretty,
    "json-compact": json_compact,
}

DumperPreset = Union[Literal["yaml"], Literal["json"], Literal["json-compact"]]


class DumperType(Protocol):
    def __call__(
        self, data: dict, text_stream: TextIO
    ) -> None:  # pragma: no cover
        ...


class ReportBuilderType(Protocol):
    def __call__(
        self,
        *,
        key: str | None,
        env: str | None,
        new_first: bool | None,
        include_internal: bool | None,
        history_limit: int | None,
        current: Any,
        history: list[dict] | None,
    ) -> dict:  # pragma: no cover
        ...


class KeyNotFoundError(Exception):
    pass


class EnvNotFoundError(Exception):
    pass


class OutputFormatError(Exception):
    pass


def inspect_settings(
    settings: Settings | LazySettings,
    key: str | None = None,
    env: str | None = None,
    *,
    new_first: bool = True,
    history_limit: int | None = None,
    include_internal: bool = False,
    to_file: str | PosixPath | None = None,
    print_report: bool = False,
    dumper: DumperPreset | DumperType | None = None,
    report_builder: ReportBuilderType | None = None,
):
    """
    Print and return the loading history of a settings object.

    Optional arguments must be provided as kwargs.

    :param settings: A Dynaconf instance
    :param key: String dotted path. E.g "path.to.key"
    :param env: Filter by this env

    :param new_first: If True, uses newest to oldest loading order
    :param history_limit: Limits how many entries are shown
    :param include_internal: If True, include internal loaders (e.g. defaults).
        This has effect only if key is not provided.
    :param to_file: If specified, write to this filename
    :param print_report: If true, prints the dumped report to stdout
    :param dumper: Accepts preset strings (e.g. "yaml", "json") or custom
        dumper callable ``(dict, TextIO) -> None``. Defaults to "yaml"
    :param report_builder: if provided, it is used to generate the report

    :return: Dict with a dict containing report data
    :rtype: dict
    """
    # choose dumper and report builder
    if dumper is None:
        _dumper = builtin_dumpers["yaml"]
    elif isinstance(dumper, str):
        _dumper = builtin_dumpers.get(dumper)
        if _dumper is None:
            raise OutputFormatError(
                f"The desired format is not available: {dumper!r}"
            )
    else:
        _dumper = dumper

    _report_builder = report_builder or _default_report_builder

    # get history and apply optional arguments
    original_settings = settings

    env_filter = None  # type: ignore
    if env:
        settings = settings.from_env(env)
        registered_envs = {
            src_meta.env for src_meta in settings._loaded_by_loaders.keys()
        }
        if env.lower() not in registered_envs:
            raise EnvNotFoundError(f"The requested env is not valid: {env!r}")

        def env_filter(src: SourceMetadata) -> bool:  # noqa: F811
            return src.env.lower() == env.lower()

    history = get_history(
        original_settings,
        key=key,
        filter_callable=env_filter,
        include_internal=include_internal,
    )

    if new_first:
        history.reverse()

    if history_limit:
        history = history[:history_limit]

    if key:
        current_value = settings.get(key)
    else:
        current_value = settings.as_dict()

    # format output
    dict_report = _report_builder(
        history=history,
        current=current_value,
        key=key,
        env=env,
        new_first=new_first,
        history_limit=history_limit,
        include_internal=include_internal,
    )

    dict_report["current"] = _ensure_serializable(dict_report["current"])

    # write to stdout AND/OR to file AND return
    if to_file is not None:
        _encoding = settings.get("ENCODER_FOR_DYNACONF")
        with open(to_file, "w", encoding=_encoding) as file:
            _dumper(dict_report, file)

    if print_report is True:
        _dumper(dict_report, sys.stdout)

    return dict_report


def _default_report_builder(**kwargs) -> dict:
    """
    Default inspect report builder.

    Accept the kwargs passed inside `inspect_settings` and returns a dict with
    {header, current, history} as top-level keys.
    """
    return {
        "header": {
            "env_filter": str(kwargs.get("env")),
            "key_filter": str(kwargs.get("key")),
            "new_first": str(kwargs.get("new_first")),
            "history_limit": str(kwargs.get("history_limit")),
            "include_internal": str(kwargs.get("include_internal")),
        },
        "current": kwargs.get("current"),
        "history": kwargs.get("history"),
    }


def get_history(
    obj: Settings | LazySettings,
    key: str | None = None,
    *,
    filter_callable: Callable[[SourceMetadata], bool] | None = None,
    include_internal: bool = False,
    history_limit: int | None = None,
) -> list[dict]:
    """
    Gets data from `settings.loaded_by_loaders` in order of loading with
    optional filtering options.

    Returns a list of dict in new-first order, where the dict contains the
    data and it's source metadata.

    :param obj: Setting object which contain the data
    :param key: Key path to desired key. Use all if not provided
    :param filter_callable: Takes SourceMetadata and returns a boolean
    :param include_internal: If True, include internal loaders (e.g. defaults).
        This has effect only if key is not provided.
    history_limit: limits how many entries are shown

    Example:
        >>> settings = Dynaconf(...)
        >>> _get_history(settings)
        [
            {
                "loader": "yaml"
                "identifier": "path/to/file.yml"
                "env": "default"
                "data": {"foo": 123, "spam": "eggs"}
            },
            ...
        ]
    """
    if filter_callable is None:
        filter_callable = lambda x: True  # noqa

    sep = obj.get("NESTED_SEPARATOR_FOR_DYNACONF", "__")

    # trigger key based hooks
    if key:
        obj.get(key)  # noqa

    internal_identifiers = ["default_settings", "_root_path"]
    result = []
    for source_metadata, data in obj._loaded_by_loaders.items():
        # filter by source_metadata
        if filter_callable(source_metadata) is False:
            continue

        # filter by internal identifiers
        if (
            not key
            and include_internal is False
            and source_metadata.identifier in internal_identifiers
        ):
            continue  # skip: internal loaders

        # filter by key path
        try:
            data = _get_data_by_key(data, key, sep=sep) if key else data
        except KeyError:
            continue  # skip: source doesn't contain the requested key

        # Normalize output
        data = _ensure_serializable(data)
        result.append({**source_metadata._asdict(), "value": data})

    if key and not result:
        # Key may be set in obj but history not tracked
        if (data := obj.get(key, empty)) is not empty:
            generic_source_metadata = SourceMetadata(
                loader="undefined",
                identifier="undefined",
            )
            data = _ensure_serializable(data)
            result.append({**generic_source_metadata._asdict(), "value": data})

        # Raise if still not found
        if key and not result:
            raise KeyNotFoundError(f"The requested key was not found: {key!r}")

    return result


def _ensure_serializable(data: BoxList | DynaBox) -> dict | list:
    """
    Converts box dict or list types to regular python dict or list
    Bypasses other values.
    {
        "foo": [1,2,3, {"a": "A", "b": "B"}],
        "bar": {"a": "A", "b": [1,2,3]},
    }
    """
    if isinstance(data, (BoxList, list)):
        return [_ensure_serializable(v) for v in data]
    elif isinstance(data, (DynaBox, dict)):
        return {
            k: _ensure_serializable(v)
            for k, v in data.items()  # type: ignore
        }
    else:
        return data if isinstance(data, (int, bool, float)) else str(data)


def _get_data_by_key(
    data: dict,
    key_dotted_path: str,
    default: Any = None,
    sep="__",
) -> Any:
    """
    Returns value found in data[key] using dot-path str (e.g, "path.to.key").
    Raises KeyError if not found
    """
    if not isinstance(data, DynaBox):
        data = DynaBox(data)  # DynaBox can handle insensitive keys

    if sep in key_dotted_path:
        key_dotted_path = key_dotted_path.replace(sep, ".")

    def handle_repr(value):
        # lazy values shouldnt be evaluated for inspecting
        if isinstance(value, Lazy):
            return value._dynaconf_encode()
        return value

    def traverse_data(data, path):
        # transform `a.b.c` in successive calls to `data['a']['b']['c']`
        path = path.split(".")
        root_key, nested_keys = path[0], path[1:]
        result = data.get(root_key, bypass_eval=True)
        for key in nested_keys:
            result = handle_repr(result.get(key, bypass_eval=True))
        return result

    try:
        return traverse_data(data, key_dotted_path)
    except KeyError:
        if not default:
            raise KeyError(f"Path not found in data: {key_dotted_path!r}")
        return default


def get_debug_info(
    settings: Settings | LazySettings,
    verbosity: int = 0,
    key: str | None = None,
) -> dict:
    """Returns a dict with debug info about the settings object"""

    if key:
        verbosity = 2

    def filter_by_key(data: dict) -> dict:
        """If key is not None, filter dict keeping only the key"""
        if key and data:
            try:
                return {key: _get_data_by_key(data, key)}
            except KeyError:
                return {}
        return data

    def build_loading_history() -> list[dict]:
        _data = []
        for (
            source_metadata,
            source_data,
        ) in settings._loaded_by_loaders.items():
            _real_data = filter_by_key(source_data)
            if verbosity == 0:
                _data.append(
                    {
                        "loader": source_metadata.loader,
                        "identifier": source_metadata.identifier,
                        "data": len(_real_data),
                    }
                )
            elif verbosity == 1:
                _data.append(
                    {
                        "loader": source_metadata.loader,
                        "identifier": source_metadata.identifier,
                        "data": list(_real_data.keys()),
                    }
                )
            else:
                _data.append(
                    {
                        "loader": source_metadata.loader,
                        "identifier": source_metadata.identifier,
                        "data": _real_data,
                    }
                )
        return _data

    def build_loaded_hooks():
        _data = []
        for hook, hook_data in settings._loaded_hooks.items():
            _real_data = filter_by_key(hook_data.get("post", {})) or {}
            if verbosity == 0:
                _data.append(
                    {
                        "hook": str(hook),
                        "data": len(_real_data),
                    }
                )
            elif verbosity == 1:
                _data.append(
                    {
                        "hook": str(hook),
                        "data": list(_real_data.keys()),
                    }
                )
            else:
                _data.append(
                    {
                        "hook": str(hook),
                        "data": _real_data,
                    }
                )
        return _data

    data = {
        "versions": {
            "dynaconf": version("dynaconf"),
        },
        "root_path": settings._root_path,
        "validators": [str(v) for v in settings.validators],
        "core_loaders": settings._loaders,
        "loaded_files": settings._loaded_files,
        "history": build_loading_history(),
        "post_hooks": [str(h) for h in settings._post_hooks],
        "loaded_hooks": build_loaded_hooks(),
    }
    for name in ["django", "flask", "fastapi", "starlette"]:
        with suppress(PackageNotFoundError):
            data["versions"][name] = version(name)

    if settings.get("ENVIRONMENTS_FOR_DYNACONF"):
        environments = settings.get("ENVIRONMENTS_FOR_DYNACONF")
        if isinstance(environments, (list, tuple)):
            data["environments"] = list(environments)
        else:
            data["environments"] = environments
        data["loaded_envs"] = settings._loaded_envs

    if key:
        data["current"] = {key: settings.get(key)}
    return data


def print_debug_info(
    settings: Settings | LazySettings,
    *,
    dumper: DumperPreset | DumperType | None = None,
    verbosity: int = 0,
    key: str | None = None,
):
    """Calls dumper with settings debug info"""
    dumper = dumper or "yaml"
    if isinstance(dumper, str):
        dumper = builtin_dumpers.get(dumper)
        if dumper is None:
            raise OutputFormatError(
                f"The desired format is not available: {dumper!r}"
            )

    data = get_debug_info(settings, verbosity, key)
    dumper(data, sys.stdout)
