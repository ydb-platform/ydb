import asyncio
import os
import shutil
import sys
import threading
from functools import lru_cache
from pathlib import Path
from typing import Awaitable, Optional, TypeVar

import yaml
from contextlog import get_logger

from annet.annlib.lib import (  # pylint: disable=unused-import
    ContextOrderedDict,
    HuaweiNumBlock,
    LMSMatcher,
    add_annotation,
    catch_ctrl_c,
    cisco_collapse_vlandb,
    cisco_expand_vlandb,
    find_exc_in_stack,
    find_modules,
    first,
    flatten,
    huawei_collapse_vlandb,
    huawei_expand_vlandb,
    huawei_iface_ranges,
    is_relative,
    jun_activate,
    jun_is_inactive,
    juniper_fmt_prefix_lists_acl,
    juniper_port_split,
    make_ip4_mask,
    mako_render,
    merge_dicts,
    percentile,
    uniq,
)


_HOMEDIR_PATH: Optional[str] = None  # defaults to ~/.annet
_TEMPLATE_CONTEXT_PATH: Optional[str] = None  # defaults to annet/configs/context.yml
_DEFAULT_CONTEXT_PATH: Optional[str] = None  # defaults to ~/.annet/context.yml


def get_homedir_path() -> str:
    if _HOMEDIR_PATH is None:
        set_homedir_path(os.path.expanduser("~/.annet/"))
    return _HOMEDIR_PATH


def set_homedir_path(path: str) -> None:
    global _HOMEDIR_PATH  # pylint: disable=global-statement
    _HOMEDIR_PATH = path


def get_template_context_path() -> str:
    if _TEMPLATE_CONTEXT_PATH is None:
        set_template_context_path(str(Path(sys.modules["annet"].__file__).parent / "configs/context.yml"))
    return _TEMPLATE_CONTEXT_PATH


def set_template_context_path(path: str) -> None:
    global _TEMPLATE_CONTEXT_PATH  # pylint: disable=global-statement
    _TEMPLATE_CONTEXT_PATH = path


def get_default_context_path() -> str:
    if _DEFAULT_CONTEXT_PATH is None:
        set_default_context_path("~/.annet/context.yml")
    return _DEFAULT_CONTEXT_PATH


def set_default_context_path(path: str) -> None:
    global _DEFAULT_CONTEXT_PATH  # pylint: disable=global-statement
    _DEFAULT_CONTEXT_PATH = path


def get_default_log_dest() -> str:
    homedir = get_homedir_path()
    return os.path.join(homedir, "deploy/")


@lru_cache(maxsize=1)
def _get_template_context():
    with open(get_template_context_path()) as f:
        return yaml.safe_load(f)


def get_context_path(touch: Optional[bool] = False) -> str:
    path = Path(os.getenv("ANN_CONTEXT_CONFIG_PATH", get_default_context_path())).expanduser().absolute()
    if not path.exists():
        src = get_template_context_path()
        if not touch:
            return str(src)
        try:
            # populate path with default configuration
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, path)
        except shutil.SameFileError:
            pass
    return str(path)


@lru_cache(maxsize=1)
def get_context() -> dict:
    with open(get_context_path()) as f:
        raw = yaml.safe_load(f)
    _fill_in_default_generator_modules(raw)
    context_name = os.getenv("ANN_SELECTED_CONTEXT", raw["selected_context"])
    res = {k: raw[k][v] for k, v in raw["context"][context_name].items()}
    if "ANN_GENERATORS_CONTEXT" in os.environ:  # an undocumented hack to maintain backwards compatibility; TODO: remove
        res["generators"] = raw["generators"][os.getenv("ANN_GENERATORS_CONTEXT")]
    return res


@lru_cache(maxsize=1)
def _warn_no_generators_in_context():
    get_logger().warning(
        "Older version of the context configuration found. Getting generators references from the template context"
    )


def _fill_in_default_generator_modules(raw: dict) -> bool:
    """Backwards compatibility hack to add existing generators refs to context"""
    if "generators" not in raw:
        _warn_no_generators_in_context()
        raw["generators"] = _get_template_context()["generators"]
        for dst_context in raw["context"].values():
            dst_context["generators"] = "default"
        return True
    return False


def repair_context_file() -> None:
    path = get_context_path()
    with open(path) as f:
        data = yaml.safe_load(f)
    if _fill_in_default_generator_modules(data):
        with open(path, "w") as f:
            yaml.dump(data, f, sort_keys=False)


ReturnType = TypeVar("ReturnType")


def do_async(coro: Awaitable[ReturnType], new_thread=False) -> ReturnType:
    if new_thread:
        # start the new thread with the new event loop
        res: ReturnType = None

        def wrapper(main):
            nonlocal res
            try:
                res = asyncio.run(main)
            except BaseException as e:
                res = e

        thread = threading.Thread(target=wrapper, args=(coro,))
        thread.start()
        thread.join()
        if isinstance(res, BaseException):
            raise res
        return res
    else:
        return asyncio.run(coro)
