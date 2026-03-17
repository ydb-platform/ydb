import sys
import warnings
from abc import ABC, abstractmethod
from functools import cached_property
from importlib.metadata import entry_points
from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from annet.lib import get_context


T = TypeVar("T")


class Connector(ABC, Generic[T]):
    name: str
    # legacy
    ep_name: str
    ep_group: str = "annet.connectors"
    # right way just to use ep groups
    ep_by_group_only: str = ""
    _classes: Optional[List[Type[T]]] = None

    def _get_default(self) -> Type[T]:
        raise RuntimeError(f"{self.name} is not set")

    @cached_property
    def _entry_point(self) -> List[Type[T]]:
        ep = load_entry_point(self.ep_group, self.ep_name)
        if self.ep_by_group_only:
            ep.extend(load_entry_point_new(self.ep_by_group_only))
        return ep

    def get(self, *args, **kwargs) -> T:
        """
        Returns connector. If more than one is registered returns random and throw warning
        """
        if self._classes is None:
            self._classes = self._entry_point or [self._get_default()]
        if not self._classes:
            raise Exception(f"Not found registered class for group={self.ep_group}")
        if len(self._classes) > 1:
            warnings.warn(
                f"Multiple classes are registered with the group={self.ep_group} but {[cls for cls in self._classes]}",
                UserWarning,
            )
        res = self._classes[0]
        return res(*args, **kwargs)

    def get_all(self) -> List[T]:
        if self._classes is None:
            self._classes = self._entry_point or [self._get_default()]

        return self._classes.copy()

    def set(self, cls: Type[T]):
        if self._classes is not None:
            raise RuntimeError(f"Cannot reinitialize value of {self.name}")
        self._classes = [cls]

    def set_all(self, classes: List[Type[T]]):
        if self._classes is not None:
            raise RuntimeError(f"Cannot reinitialize value of {self.name}")
        self._classes = list(classes)

    def is_default(self) -> bool:
        return self._classes is None and not self._entry_point


class CachedConnector(Connector[T], ABC):
    _cache: Optional[T] = None

    def get(self, *args, **kwargs) -> T:
        assert not (args or kwargs), "Arguments forwarding is not allowed for cached connectors"
        if self._cache is None:
            self._cache = super().get()
        return self._cache

    def set(self, cls: Type[T]):
        super().set(cls)
        self._cache = None


def load_entry_point(group: str, name: str):
    if sys.version_info < (3, 10):
        ep = [item for item in entry_points().get(group, []) if item.name == name]
    else:
        ep = entry_points(group=group, name=name)  # pylint: disable=unexpected-keyword-arg
    if not ep:
        return []
    return [item.load() for item in ep]


def load_entry_point_new(group: str) -> List:
    if sys.version_info < (3, 10):
        ep = [item for item in entry_points().get(group, [])]
    else:
        ep = entry_points(group=group)  # pylint: disable=unexpected-keyword-arg
    if not ep:
        return []
    return [item.load() for item in ep]


class AdapterWithConfig(ABC, Generic[T]):
    @classmethod
    @abstractmethod
    def with_config(cls, **kwargs: Dict[str, Any]) -> T:
        pass


class AdapterWithName(ABC):
    @classmethod
    @abstractmethod
    def name(cls) -> str:
        pass


def get_connector_from_config(config_key: str, connectors: List[Type[Connector]]) -> Tuple[Connector, Dict[str, Any]]:
    if not connectors:
        raise Exception("empty connectors")

    # handle configuration
    connector_params = dict[str, Any]()  # default
    if context_storage := get_context().get(config_key):
        connector_params = context_storage.get("params", {})
        if adapter_name := context_storage.get("adapter", None):
            seen = list[str]()
            for con in connectors:
                if issubclass(con, AdapterWithName):
                    con_name = con.name()
                else:
                    con_name = con.__name__
                seen.append(con_name)
                if adapter_name == con_name:
                    connectors = [con]
                    break
            else:
                raise Exception("unknown %s %s: seen %s" % (config_key, adapter_name, seen))

    if len(connectors) > 1:
        warnings.warn(
            f"Please specify adapter for '{config_key}'. Found more than one classes {connectors}", UserWarning
        )
    connector = connectors[0]
    if issubclass(connector, AdapterWithConfig):
        connector_ins = connector.with_config(**connector_params)
    else:
        connector_ins = connector()
    # return connector_params only for storage
    # TODO: switch storage interface to AdapterWithConfig
    return connector_ins, connector_params
