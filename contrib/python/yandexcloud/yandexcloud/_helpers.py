from typing import TYPE_CHECKING, Optional

from yandex.cloud.iam.v1.service_account_service_pb2 import ListServiceAccountsRequest
from yandex.cloud.iam.v1.service_account_service_pb2_grpc import (
    ServiceAccountServiceStub,
)
from yandex.cloud.vpc.v1.network_service_pb2 import ListNetworksRequest
from yandex.cloud.vpc.v1.network_service_pb2_grpc import NetworkServiceStub
from yandex.cloud.vpc.v1.subnet_service_pb2 import ListSubnetsRequest
from yandex.cloud.vpc.v1.subnet_service_pb2_grpc import SubnetServiceStub

if TYPE_CHECKING:
    from yandexcloud._sdk import SDK


class Helpers:
    def __init__(self, sdk: "SDK"):
        self.sdk = sdk

    def find_service_account_id(self, folder_id: str) -> str:
        """
        Get service account id in case the folder has the only one service account

        :param folder_id: ID of the folder
        :return ID of the service account
        """
        service = self.sdk.client(ServiceAccountServiceStub)
        service_accounts = service.List(ListServiceAccountsRequest(folder_id=folder_id)).service_accounts
        if len(service_accounts) == 1:
            return service_accounts[0].id
        if len(service_accounts) == 0:
            raise RuntimeError(f"There are no service accounts in folder {folder_id}, please create it.")
        raise RuntimeError(f"There are more than one service account in folder {folder_id}, please specify it")

    def find_network_id(self, folder_id: str) -> str:
        """
        Get ID of the first network in folder

        :param folder_id: ID of the folder
        :return ID of the network
        """
        networks = self.sdk.client(NetworkServiceStub).List(ListNetworksRequest(folder_id=folder_id)).networks
        if not networks:
            raise RuntimeError(f"No networks in folder: {folder_id}")
        if len(networks) > 1:
            raise RuntimeError("There are more than one network in folder {folder_id}, please specify it")
        return networks[0].id

    def find_subnet_id(self, folder_id: str, zone_id: str, network_id: Optional[str] = None) -> str:
        """
        Get ID of the subnetwork of specified network in specified availability zone

        :param folder_id: ID of the folder
        :param zone_id: ID of the availability zone
        :param network_id: ID of the network
        :return ID of the subnetwork
        """
        subnet_service = self.sdk.client(SubnetServiceStub)
        subnets = subnet_service.List(ListSubnetsRequest(folder_id=folder_id)).subnets
        if network_id:
            applicable = [s for s in subnets if s.zone_id == zone_id and s.network_id == network_id]
        else:
            applicable = [s for s in subnets if s.zone_id == zone_id]
        if len(applicable) == 1:
            return applicable[0].id
        if len(applicable) == 0:
            raise RuntimeError(f"There are no subnets in {zone_id} zone, please create it.")
        raise RuntimeError(f"There are more than one subnet in {zone_id} zone, please specify it")
