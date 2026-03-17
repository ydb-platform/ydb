import functools
import importlib

from annet.annlib.rulebook import common  # pylint: disable=unused-import # noqa: F401,F403
from annet.annlib.rulebook.common import *  # pylint: disable=wildcard-import,unused-wildcard-import # noqa: F401,F403


@functools.lru_cache()
def import_rulebook_function(name):
    from . import rulebook_provider_connector

    index = name.rindex(".")
    for root in rulebook_provider_connector.get().get_root_modules():
        try:
            module = importlib.import_module(f"{root}.{name[:index]}", package=__name__.rsplit(".", 1)[0])
            return getattr(module, name[index + 1 :])
        except ImportError:
            pass
    raise ImportError(f"Could not import {name}")
