from __future__ import annotations

import base64
import importlib.util
import os
import pkgutil
import re
import sys
import zlib
from collections.abc import Awaitable, Callable, Generator
from importlib.machinery import FileFinder
from pathlib import Path
from types import ModuleType
from typing import Any, TypeVar, cast

from anyio import from_thread
from asyncclick import BadOptionUsage, ClickException, Context
from dictdiffer import diff
from tortoise import BaseDBAsyncClient, Tortoise
from tortoise.log import logger

from aerich._compat import tomllib
from aerich.coder import decoder, encoder
from aerich.exceptions import NotInitedError

if sys.version_info >= (3, 11):
    from typing import ParamSpec, TypeVarTuple, Unpack
else:
    from typing_extensions import ParamSpec, TypeVarTuple, Unpack

T_Retval = TypeVar("T_Retval")
PosArgsT = TypeVarTuple("PosArgsT")
P = ParamSpec("P")

CONFIG_DEFAULT_VALUES = {
    "src_folder": ".",
}


def add_src_path(path: str) -> str:
    """
    add a folder to the paths, so we can import from there
    :param path: path to add
    :return: absolute path
    """
    if not os.path.isabs(path):
        # use the absolute path, otherwise some other things (e.g. __file__) won't work properly
        path = os.path.abspath(path)
    if not os.path.isdir(path):
        raise ClickException(f"Specified source folder does not exist: {path}")
    if path not in sys.path:
        sys.path.insert(0, path)
    return path


def get_app_connection_name(config: dict[str, dict[str, Any]], app_name: str) -> str:
    """
    get connection name
    :param config:
    :param app_name:
    :return: the default connection name (Usally it is 'default')
    """
    if app := config["apps"].get(app_name):
        return cast(str, app.get("default_connection", "default"))
    raise BadOptionUsage(option_name="--app", message=f"Can't get app named {app_name!r}")


def get_app_connection(config: dict[str, Any], app: str) -> BaseDBAsyncClient:
    """
    get connection client
    :param config:
    :param app:
    :return: client instance
    """
    return Tortoise.get_connection(get_app_connection_name(config, app))


def get_tortoise_config(tortoise_orm: str, ctx: Context | None = None) -> dict[str, Any]:
    """
    get tortoise config from module
    :param tortoise_orm:
    :param ctx:
    :return:
    """
    if isinstance(ctx, str) and (tortoise_orm is None or isinstance(tortoise_orm, Context)):
        tortoise_orm, ctx = ctx, tortoise_orm  # Leave it here for backwards compatibility
    splits = tortoise_orm.split(".")
    config_path = ".".join(splits[:-1])
    tortoise_config = splits[-1]

    try:
        config_module = importlib.import_module(config_path)
    except ModuleNotFoundError as e:
        if len(splits) < 3:
            raise ClickException(f"Error while importing configuration module: {e}") from None
        module_path = ".".join(splits[:-2])
        try:
            config_module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            raise ClickException(f"Failed to import configuration module: {e}") from None
        tortoise_config = splits[-2] + "." + splits[-1]
        config_class = getattr(config_module, splits[-2], None)
        config = getattr(config_class, splits[-1], None)
    else:
        config = getattr(config_module, tortoise_config, None)
    if not config:
        raise BadOptionUsage(
            option_name="--config",
            message=f'Can\'t get "{tortoise_config}" from module "{config_module}"',
            ctx=ctx,
        )
    return cast(dict[str, Any], config)


def load_tortoise_config(
    tortoise_orm: str = "",
    ctx: Context | None = None,
    config_file: str | Path = "pyproject.toml",
    env_name: str = "TORTOISE_ORM",
) -> dict[str, Any]:
    """
    Load tortoise config from tortoise_orm or config_file or os environ.

    If tortoise_orm is not empty, load config dict by it;
    Otherwise, try to get tortoise_orm string by tool.aerich.tortoise_orm from config_file;
    While failed to get tortoise_orm from config file, try to get it by os.getenv(<env_name>);
    Raises ClickException if failed to get tortoise_orm from config file and os environ.

    :param tortoise_orm: module.value string to load the tortoise config, e.g.: 'settings.TORTOISE_ORM'
    :param ctx: click.Context that will be used when raising BadOptionUsage error
    :param config_file: config filename, must be a toml file
    :param env_name: os environ name to get the tortoise_orm string (Only use when failed to load from config file)

    :return: config dict that can be used in `Tortoise.init(config=config_dict)`
    """
    return _load_tortoise_aerich_config(tortoise_orm, ctx, config_file, env_name)[0]


def _load_tortoise_aerich_config(
    tortoise_orm: str = "",
    ctx: Context | None = None,
    config_file: str | Path = "pyproject.toml",
    env_name: str = "TORTOISE_ORM",
    default_src_folder: str = CONFIG_DEFAULT_VALUES["src_folder"],
) -> tuple[dict[str, Any], dict[str, str]]:
    aerich_config: dict[str, str] = {}
    if tortoise_orm:
        add_src_path(default_src_folder)
        return get_tortoise_config(tortoise_orm, ctx), aerich_config
    if isinstance(config_file, str):
        config_file = Path(config_file)
    if config_file.exists():
        text = config_file.read_text(encoding="utf-8")
        doc = tomllib.loads(text)
        try:
            aerich_config = doc["tool"]["aerich"]
        except KeyError:
            ...
        else:
            if t := aerich_config.get("tortoise_orm", ""):
                add_src_path(aerich_config.get("src", default_src_folder))
                return get_tortoise_config(t, ctx), aerich_config
    if v := os.getenv(env_name):
        add_src_path(os.getenv("TORTOISE_ORM_SRC", default_src_folder))
        return get_tortoise_config(v, ctx), aerich_config
    raise ClickException(
        f"Failed to load tortoise config from config_file({config_file}) and os environ({env_name!r})"
    )


def get_models_describe(app: str) -> dict[str, dict[str, Any]]:
    """
    get app models describe
    :param app:
    :return:
    """
    ret: dict[str, dict[str, Any]] = {}
    try:
        app_config = Tortoise.apps[app]
    except KeyError as e:
        if not Tortoise._inited:
            raise NotInitedError("Tortoise not inited yet.") from e
        logger.debug(f"{Tortoise.apps.keys() = }")
        raise e
    for model in app_config.values():
        managed = getattr(model.Meta, "managed", None)
        describe = model.describe()
        try:
            qualified_model_name = describe["name"]
        except KeyError:
            continue
        else:
            ret[qualified_model_name] = dict(describe, managed=managed)
    return ret


def is_default_function(string: Any) -> re.Match[str] | None:
    return re.match(r"^<function.+>$", str(string or ""))


def file_module_info(path: str | Path, name: str) -> pkgutil.ModuleInfo:
    for module_info in pkgutil.iter_modules([str(path)]):
        if module_info.name == name:
            return module_info
    raise FileNotFoundError(f"Module {name} not found in path {path}")


def import_py_file(file: str | Path) -> ModuleType:
    module_name, file_ext = os.path.splitext(os.path.split(file)[-1])
    spec = importlib.util.spec_from_file_location(module_name, file)
    module = importlib.util.module_from_spec(spec)  # type:ignore[arg-type]
    spec.loader.exec_module(module)  # type:ignore[union-attr]
    return module


def import_py_module(module_info: pkgutil.ModuleInfo) -> ModuleType:
    module_finder: FileFinder
    name: str
    ispkg: bool
    module_finder, name, ispkg = module_info  # type:ignore[assignment]
    module_finder.invalidate_caches()
    spec = module_finder.find_spec(name)
    module = importlib.util.module_from_spec(spec)  # type:ignore[arg-type]
    spec.loader.exec_module(module)  # type:ignore[union-attr]
    return module


def py_module_path(module_info: pkgutil.ModuleInfo) -> Path:
    module_finder: FileFinder
    name: str
    ispkg: bool
    module_finder, name, ispkg = module_info  # type:ignore[assignment]
    module_finder.invalidate_caches()
    spec = module_finder.find_spec(name)
    if not spec or not spec.origin or not Path(spec.origin).is_file():
        raise FileNotFoundError(f"Module {name} not found in {module_finder.path}.")
    return Path(spec.origin)


def get_dict_diff_by_key(
    old_fields: list[dict],
    new_fields: list[dict],
    key: str = "through",
    second_key: str = "forward_key",
) -> Generator[tuple[str, Any, Any]]:
    """
    Compare two list by key instead of by index

    :param old_fields: previous field info list
    :param new_fields: current field info list
    :param key: if two dicts have the same value of this key, action is change; otherwise, is remove/add
    :param second_key: if multi fields with same value of key, use `(field[key], field[second_key])` as ident
    :return: similar to dictdiffer.diff

    Example::

        >>> old = [{'through': 'a'}, {'through': 'b'}, {'through': 'c'}]
        >>> new = [{'through': 'a'}, {'through': 'c'}]  # remove the second element
        >>> list(diff(old, new))
        [('change', [1, 'through'], ('b', 'c')),
         ('remove', '', [(2, {'through': 'c'})])]
        >>> list(get_dict_diff_by_key(old, new))
        [('remove', '', [(0, {'through': 'b'})])]

    """
    length_old, length_new = len(old_fields), len(new_fields)
    if length_old == 0 or length_new == 0 or length_old == length_new == 1:
        yield from diff(old_fields, new_fields)
    else:
        should_use_second_key = len({i[key] for i in old_fields}) < length_old or (
            len({i[key] for i in new_fields}) < length_new
        )
        value_index: dict[str | tuple[str, str], int] = (
            {(f[key], f[second_key]): i for i, f in enumerate(new_fields)}
            if should_use_second_key
            else {f[key]: i for i, f in enumerate(new_fields)}
        )
        additions = set(range(length_new))
        for field in old_fields:
            value = (field[key], field[second_key]) if should_use_second_key else field[key]
            if (index := value_index.get(value)) is not None:
                additions.remove(index)
                yield from diff([field], [new_fields[index]])  # change
            else:
                yield from diff([field], [])  # remove
        if additions:
            for index in sorted(additions):
                yield from diff([], [new_fields[index]])  # add


def compress_dict(dictionary: dict[str, Any]) -> str:
    json_str = encoder(dictionary)
    compressed_bytes = zlib.compress(json_str.encode("utf-8"))
    base64_str = base64.b64encode(compressed_bytes).decode("ascii")

    return base64_str


def get_formatted_compressed_data(dictionary: dict[str, Any], row_length: int = 70) -> str:
    compressed_str = compress_dict(dictionary)
    formatted_parts = [
        '"' + compressed_str[current_cursor : current_cursor + row_length] + '"'
        for current_cursor in range(0, len(compressed_str), row_length)
    ]
    return "\n    ".join(formatted_parts)


def decompress_dict(compressed_str: str) -> dict[str, Any]:
    compressed_bytes = base64.b64decode(compressed_str)
    json_bytes = zlib.decompress(compressed_bytes)
    dictionary = decoder(json_bytes)
    return dictionary


def run_async(
    async_func: Callable[[Unpack[PosArgsT]], Awaitable[T_Retval]],
    *args: Unpack[PosArgsT],
) -> T_Retval:
    """Run async function in worker thread and get the result of it"""
    # `asyncio.run(async_func())` can get the result of async function,
    # but it will close the running loop.
    with from_thread.start_blocking_portal() as portal:
        future = portal.start_task_soon(async_func, *args)
        return_value = future.result()
    return return_value
