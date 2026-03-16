import abc
from typing import Type

from annet.connectors import Connector


class _FiltererConnector(Connector["Filterer"]):
    name = "Filterer"
    ep_name = "filterer"
    ep_by_group_only = "annet.connectors.filterer"

    def _get_default(self) -> Type["Filterer"]:
        return NopFilterer


filterer_connector = _FiltererConnector()


class Filterer(abc.ABC):
    @abc.abstractmethod
    def for_ifaces(self, device, ifnames) -> str:
        pass

    @abc.abstractmethod
    def for_peers(self, device, peers_allowed) -> str:
        pass

    @abc.abstractmethod
    def for_policies(self, device, policies_allowed) -> str:
        pass


class NopFilterer(Filterer):
    def for_ifaces(self, device, ifnames) -> str:
        return ""

    def for_peers(self, device, peers_allowed) -> str:
        return ""

    def for_policies(self, device, policies_allowed) -> str:
        return ""
