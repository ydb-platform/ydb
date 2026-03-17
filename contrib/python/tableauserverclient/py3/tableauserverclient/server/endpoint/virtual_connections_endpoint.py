from functools import partial
import json
from pathlib import Path
from typing import Optional, TYPE_CHECKING, Union
from collections.abc import Iterable

from tableauserverclient.models.connection_item import ConnectionItem
from tableauserverclient.models.pagination_item import PaginationItem
from tableauserverclient.models.revision_item import RevisionItem
from tableauserverclient.models.virtual_connection_item import VirtualConnectionItem
from tableauserverclient.server.request_factory import RequestFactory
from tableauserverclient.server.request_options import RequestOptions
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api
from tableauserverclient.server.endpoint.permissions_endpoint import _PermissionsEndpoint
from tableauserverclient.server.endpoint.resource_tagger import TaggingMixin
from tableauserverclient.server.pager import Pager

if TYPE_CHECKING:
    from tableauserverclient.server import Server


class VirtualConnections(QuerysetEndpoint[VirtualConnectionItem], TaggingMixin):
    def __init__(self, parent_srv: "Server") -> None:
        super().__init__(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/virtualConnections"

    @api(version="3.18")
    def get(self, req_options: Optional[RequestOptions] = None) -> tuple[list[VirtualConnectionItem], PaginationItem]:
        server_response = self.get_request(self.baseurl, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        virtual_connections = VirtualConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return virtual_connections, pagination_item

    @api(version="3.18")
    def populate_connections(self, virtual_connection: VirtualConnectionItem) -> VirtualConnectionItem:
        def _connection_fetcher():
            return Pager(partial(self._get_virtual_database_connections, virtual_connection))

        virtual_connection._connections = _connection_fetcher
        return virtual_connection

    def _get_virtual_database_connections(
        self, virtual_connection: VirtualConnectionItem, req_options: Optional[RequestOptions] = None
    ) -> tuple[list[ConnectionItem], PaginationItem]:
        server_response = self.get_request(f"{self.baseurl}/{virtual_connection.id}/connections", req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)

        return connections, pagination_item

    @api(version="3.18")
    def update_connection_db_connection(
        self, virtual_connection: Union[str, VirtualConnectionItem], connection: ConnectionItem
    ) -> ConnectionItem:
        vconn_id = getattr(virtual_connection, "id", virtual_connection)
        url = f"{self.baseurl}/{vconn_id}/connections/{connection.id}/modify"
        xml_request = RequestFactory.VirtualConnection.update_db_connection(connection)
        server_response = self.put_request(url, xml_request)
        return ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.23")
    def get_by_id(self, virtual_connection: Union[str, VirtualConnectionItem]) -> VirtualConnectionItem:
        vconn_id = getattr(virtual_connection, "id", virtual_connection)
        url = f"{self.baseurl}/{vconn_id}"
        server_response = self.get_request(url)
        return VirtualConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.23")
    def download(self, virtual_connection: Union[str, VirtualConnectionItem]) -> str:
        v_conn = self.get_by_id(virtual_connection)
        return json.dumps(v_conn.content)

    @api(version="3.23")
    def update(self, virtual_connection: VirtualConnectionItem) -> VirtualConnectionItem:
        url = f"{self.baseurl}/{virtual_connection.id}"
        xml_request = RequestFactory.VirtualConnection.update(virtual_connection)
        server_response = self.put_request(url, xml_request)
        return VirtualConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.23")
    def get_revisions(
        self, virtual_connection: VirtualConnectionItem, req_options: Optional[RequestOptions] = None
    ) -> tuple[list[RevisionItem], PaginationItem]:
        server_response = self.get_request(f"{self.baseurl}/{virtual_connection.id}/revisions", req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        revisions = RevisionItem.from_response(server_response.content, self.parent_srv.namespace, virtual_connection)
        return revisions, pagination_item

    @api(version="3.23")
    def download_revision(self, virtual_connection: VirtualConnectionItem, revision_number: int) -> str:
        url = f"{self.baseurl}/{virtual_connection.id}/revisions/{revision_number}"
        server_response = self.get_request(url)
        virtual_connection = VirtualConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return json.dumps(virtual_connection.content)

    @api(version="3.23")
    def delete(self, virtual_connection: Union[VirtualConnectionItem, str]) -> None:
        vconn_id = getattr(virtual_connection, "id", virtual_connection)
        self.delete_request(f"{self.baseurl}/{vconn_id}")

    @api(version="3.23")
    def publish(
        self,
        virtual_connection: VirtualConnectionItem,
        virtual_connection_content: str,
        mode: str = "CreateNew",
        publish_as_draft: bool = False,
    ) -> VirtualConnectionItem:
        """
        Publish a virtual connection to the server.

        For the virtual_connection object, name, project_id, and owner_id are
        required.

        The virtual_connection_content can be a json string or a file path to a
        json file.

        The mode can be "CreateNew" or "Overwrite". If mode is
        "Overwrite" and the virtual connection already exists, it will be
        overwritten.

        If publish_as_draft is True, the virtual connection will be published
        as a draft, and the id of the draft will be on the response object.
        """
        try:
            json.loads(virtual_connection_content)
        except json.JSONDecodeError:
            file = Path(virtual_connection_content)
            if not file.exists():
                raise RuntimeError(f"{virtual_connection_content} is not valid json nor an existing file path")
            content = file.read_text()
        else:
            content = virtual_connection_content

        if mode not in ["CreateNew", "Overwrite"]:
            raise ValueError(f"Invalid mode: {mode}")
        overwrite = mode == "Overwrite"

        url = f"{self.baseurl}?overwrite={str(overwrite).lower()}&publishAsDraft={str(publish_as_draft).lower()}"
        xml_request = RequestFactory.VirtualConnection.publish(virtual_connection, content)
        server_response = self.post_request(url, xml_request)
        return VirtualConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.22")
    def populate_permissions(self, item: VirtualConnectionItem) -> None:
        self._permissions.populate(item)

    @api(version="3.22")
    def add_permissions(self, resource, rules):
        return self._permissions.update(resource, rules)

    @api(version="3.22")
    def delete_permission(self, item, capability_item):
        return self._permissions.delete(item, capability_item)

    @api(version="3.23")
    def add_tags(
        self, virtual_connection: Union[VirtualConnectionItem, str], tags: Union[Iterable[str], str]
    ) -> set[str]:
        return super().add_tags(virtual_connection, tags)

    @api(version="3.23")
    def delete_tags(
        self, virtual_connection: Union[VirtualConnectionItem, str], tags: Union[Iterable[str], str]
    ) -> None:
        return super().delete_tags(virtual_connection, tags)

    @api(version="3.23")
    def update_tags(self, virtual_connection: VirtualConnectionItem) -> None:
        raise NotImplementedError("Update tags is not implemented for Virtual Connections")
