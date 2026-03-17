import logging
from codecs import open as codecs_open
from typing import Dict, ItemsView, Optional, Union
from urllib.request import urlopen
from os.path import isabs

from .exceptions import TldImproperlyConfigured, TldIOError
from .helpers import project_dir

__author__ = "Artur Barseghyan"
__copyright__ = "2013-2025 Artur Barseghyan"
__license__ = "MPL-1.1 OR GPL-2.0-only OR LGPL-2.1-or-later"
__all__ = (
    "BaseTLDSourceParser",
    "Registry",
)

LOGGER = logging.getLogger(__name__)


class Registry(type):

    REGISTRY: Dict[str, "BaseTLDSourceParser"] = {}

    def __new__(mcs, name, bases, attrs):
        new_cls = type.__new__(mcs, name, bases, attrs)
        # Here the name of the class is used as key but it could be any class
        # parameter.
        if getattr(new_cls, "_uid", None):
            mcs.REGISTRY[new_cls._uid] = new_cls
        return new_cls

    @property
    def _uid(cls) -> str:
        return getattr(cls, "uid", cls.__name__)

    @classmethod
    def reset(mcs) -> None:
        mcs.REGISTRY = {}

    @classmethod
    def get(
        mcs, key: str, default: "BaseTLDSourceParser" = None
    ) -> Union["BaseTLDSourceParser", None]:
        return mcs.REGISTRY.get(key, default)

    @classmethod
    def items(mcs) -> ItemsView[str, "BaseTLDSourceParser"]:
        return mcs.REGISTRY.items()

    # @classmethod
    # def get_registry(mcs) -> Dict[str, Type]:
    #     return dict(mcs.REGISTRY)
    #
    # @classmethod
    # def pop(mcs, uid) -> None:
    #     mcs.REGISTRY.pop(uid)


class BaseTLDSourceParser(metaclass=Registry):
    """Base TLD source parser."""

    uid: Optional[str] = None
    source_url: str
    local_path: str
    include_private: bool = True

    @classmethod
    def validate(cls):
        """Constructor."""
        if not cls.uid:
            raise TldImproperlyConfigured(
                "The `uid` property of the TLD source parser shall be defined."
            )

    @classmethod
    def get_tld_names(cls, fail_silently: bool = False, retry_count: int = 0):
        """Get tld names.

        :param fail_silently:
        :param retry_count:
        :return:
        """
        cls.validate()
        raise NotImplementedError(
            "Your TLD source parser shall implement `get_tld_names` method."
        )

    @classmethod
    def update_tld_names(cls, fail_silently: bool = False) -> bool:
        """Update the local copy of the TLD file.

        :param fail_silently:
        :return:
        """
        try:
            remote_file = urlopen(cls.source_url)

            if isabs(cls.local_path):
                local_path = cls.local_path
                local_file = codecs_open(local_path, "r", encoding="utf8")
            else:
                local_path = project_dir(cls.local_path)
                local_file = local_path.open("r", encoding="utf8")

            local_file.write(remote_file.read().decode("utf8"))
            local_file.close()
            remote_file.close()
            LOGGER.info(
                f"Fetched '{cls.source_url}' as '{local_path}'"
            )
        except Exception as err:
            LOGGER.error(
                f"Failed fetching '{cls.source_url}'. Reason: {str(err)}"
            )
            if fail_silently:
                return False
            raise TldIOError(err)

        return True
