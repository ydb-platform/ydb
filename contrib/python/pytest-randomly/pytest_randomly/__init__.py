from __future__ import annotations

import argparse
import random
import sys
from functools import lru_cache
from itertools import groupby
from types import ModuleType
from typing import Any, Callable, TypeVar
from zlib import crc32

from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.fixtures import SubRequest
from _pytest.nodes import Item
from pytest import Collector, fixture, hookimpl

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points

# factory-boy
try:
    from factory.random import set_random_state as factory_set_random_state

    have_factory_boy = True
except ImportError:  # pragma: no cover
    # old versions
    try:
        from factory.fuzzy import set_random_state as factory_set_random_state

        have_factory_boy = True
    except ImportError:
        have_factory_boy = False

# faker
try:
    from faker.generator import random as faker_random

    have_faker = True
except ImportError:  # pragma: no cover
    have_faker = False

# model_bakery
try:
    from model_bakery.random_gen import baker_random

    have_model_bakery = True
except ImportError:  # pragma: no cover
    have_model_bakery = False

# numpy
try:
    from numpy import random as np_random

    have_numpy = True
except ImportError:  # pragma: no cover
    have_numpy = False


def make_seed() -> int:
    return random.Random().getrandbits(32)


def seed_type(string: str) -> str | int:
    if string in ("default", "last"):
        return string
    try:
        return int(string)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{repr(string)} is not an integer or the string 'last'"
        )


def pytest_addoption(parser: Parser) -> None:
    group = parser.getgroup("randomly", "pytest-randomly")
    group._addoption(
        "--randomly-seed",
        action="store",
        dest="randomly_seed",
        default="default",
        type=seed_type,
        help="""Set the seed that pytest-randomly uses (int), or pass the
                special value 'last' to reuse the seed from the previous run.
                Default behaviour: use random.Random().getrandbits(32), so the seed is
                different on each run.""",
    )
    group._addoption(
        "--randomly-dont-reset-seed",
        action="store_false",
        dest="randomly_reset_seed",
        default=True,
        help="""Stop pytest-randomly from resetting random.seed() at the
                start of every test context (e.g. TestCase) and individual
                test.""",
    )
    group._addoption(
        "--randomly-dont-reorganize",
        action="store_false",
        dest="randomly_reorganize",
        default=True,
        help="Stop pytest-randomly from randomly reorganizing the test order.",
    )


def pytest_configure(config: Config) -> None:
    if config.pluginmanager.hasplugin("xdist"):
        config.pluginmanager.register(XdistHooks())

    seed_value = config.getoption("randomly_seed")
    if seed_value == "last":
        assert hasattr(config, "cache"), (
            "The cacheprovider plugin is required to use 'last'"
        )
        assert config.cache is not None
        seed = config.cache.get("randomly_seed", make_seed())
    elif seed_value == "default":
        if hasattr(config, "workerinput"):  # pragma: no cover
            # pytest-xdist: use seed generated on main.
            seed = config.workerinput["randomly_seed"]
        else:
            seed = make_seed()
    else:
        seed = seed_value
    if hasattr(config, "cache"):
        assert config.cache is not None
        config.cache.set("randomly_seed", seed)
    config.option.randomly_seed = seed


class XdistHooks:
    # Hooks for xdist only, registered when needed in pytest_configure()
    # https://docs.pytest.org/en/latest/writing_plugins.html#optionally-using-hooks-from-3rd-party-plugins  # noqa: E501

    def pytest_configure_node(self, node: Item) -> None:
        seed = node.config.getoption("randomly_seed")
        node.workerinput["randomly_seed"] = seed  # type: ignore [attr-defined]


entrypoint_reseeds: list[Callable[[int], None]] | None = None


def _reseed(config: Config, offset: int = 0) -> int:
    global entrypoint_reseeds
    seed: int = config.getoption("randomly_seed") + offset

    random.seed(seed)
    random_state = random.getstate()

    if have_factory_boy:  # pragma: no branch
        factory_set_random_state(random_state)

    if have_faker:  # pragma: no branch
        faker_random.setstate(random_state)

    if have_model_bakery:  # pragma: no branch
        baker_random.setstate(random_state)

    if have_numpy:  # pragma: no branch
        np_random.seed(seed % 2**32)

    if entrypoint_reseeds is None:
        eps = entry_points(group="pytest_randomly.random_seeder")
        entrypoint_reseeds = [e.load() for e in eps]
    for reseed in entrypoint_reseeds:
        reseed(seed)

    return seed


def pytest_report_header(config: Config) -> str:
    seed = config.getoption("randomly_seed")
    _reseed(config)
    return f"Using --randomly-seed={seed}"


def pytest_runtest_setup(item: Item) -> None:
    if item.config.getoption("randomly_reset_seed"):
        _reseed(item.config, (_crc32(item.nodeid) - 1) % 2**32)


def pytest_runtest_call(item: Item) -> None:
    if item.config.getoption("randomly_reset_seed"):
        _reseed(item.config, _crc32(item.nodeid))


def pytest_runtest_teardown(item: Item) -> None:
    if item.config.getoption("randomly_reset_seed"):
        _reseed(item.config, (_crc32(item.nodeid) + 1) % 2**32)


@hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: Config, items: list[Item]) -> None:
    if not config.getoption("randomly_reorganize"):
        return

    seed = _reseed(config)

    modules_items: list[tuple[ModuleType | None, list[Item]]] = []
    for module, group in groupby(items, _get_module):
        modules_items.append(
            (
                module,
                _shuffle_by_class(list(group), seed),
            )
        )

    def _module_key(module_item: tuple[ModuleType | None, list[Item]]) -> int:
        module, _items = module_item
        if module is None:
            return _crc32(f"{seed}::None")
        return _crc32(f"{seed}::{module.__name__}")

    modules_items.sort(key=_module_key)

    items[:] = reduce_list_of_lists([subitems for module, subitems in modules_items])


def _get_module(item: Item) -> ModuleType | None:
    try:
        return getattr(item, "module", None)
    except (ImportError, Collector.CollectError):
        return None


def _shuffle_by_class(items: list[Item], seed: int) -> list[Item]:
    klasses_items: list[tuple[type[Any] | None, list[Item]]] = []

    def _item_key(item: Item) -> int:
        return _crc32(f"{seed}::{item.nodeid}")

    for klass, group in groupby(items, _get_cls):
        klass_items = list(group)
        klass_items.sort(key=_item_key)
        klasses_items.append((klass, klass_items))

    def _cls_key(klass_items: tuple[type[Any] | None, list[Item]]) -> int:
        klass, items = klass_items
        if klass is None:
            return _crc32(f"{seed}::None")
        return _crc32(f"{seed}::{klass.__module__}.{klass.__qualname__}")

    klasses_items.sort(key=_cls_key)

    return reduce_list_of_lists([subitems for klass, subitems in klasses_items])


def _get_cls(item: Item) -> type[Any] | None:
    return getattr(item, "cls", None)


T = TypeVar("T")


def reduce_list_of_lists(lists: list[list[T]]) -> list[T]:
    new_list = []
    for list_ in lists:
        new_list.extend(list_)
    return new_list


@lru_cache
def _crc32(string: str) -> int:
    return crc32(string.encode())


if have_faker:  # pragma: no branch

    @fixture(autouse=True)
    def faker_seed(pytestconfig: Config, request: SubRequest) -> int:
        result: int = pytestconfig.getoption("randomly_seed") + _crc32(
            request.node.nodeid
        )
        return result
