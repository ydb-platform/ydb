import logging
from typing import Callable, Optional, Protocol, TYPE_CHECKING

from .endpoint import Endpoint
from .exceptions import MissingRequiredFieldError
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import DQWItem

from tableauserverclient.helpers.logging import logger

if TYPE_CHECKING:
    from tableauserverclient.server.request_options import RequestOptions


class HasId(Protocol):
    @property
    def id(self) -> Optional[str]: ...
    def _set_data_quality_warnings(self, dqw: Callable[[], list[DQWItem]]): ...


class _DataQualityWarningEndpoint(Endpoint):
    def __init__(self, parent_srv, resource_type):
        super().__init__(parent_srv)
        self.resource_type = resource_type

    @property
    def baseurl(self) -> str:
        return "{}/sites/{}/dataQualityWarnings/{}".format(
            self.parent_srv.baseurl, self.parent_srv.site_id, self.resource_type
        )

    def add(self, resource: HasId, warning: DQWItem) -> list[DQWItem]:
        url = f"{self.baseurl}/{resource.id}"
        add_req = RequestFactory.DQW.add_req(warning)
        response = self.post_request(url, add_req)
        warnings = DQWItem.from_response(response.content, self.parent_srv.namespace)
        logger.info(f"Added dqw for resource {resource.id}")

        return warnings

    def update(self, resource: HasId, warning: DQWItem) -> list[DQWItem]:
        url = f"{self.baseurl}/{resource.id}"
        add_req = RequestFactory.DQW.update_req(warning)
        response = self.put_request(url, add_req)
        warnings = DQWItem.from_response(response.content, self.parent_srv.namespace)
        logger.info(f"Added dqw for resource {resource.id}")

        return warnings

    def clear(self, resource: HasId) -> None:
        url = f"{self.baseurl}/{resource.id}"
        return self.delete_request(url)

    def populate(self, item: HasId) -> None:
        if not item.id:
            error = "Server item is missing ID. Item must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def dqw_fetcher():
            return self._get_data_quality_warnings(item)

        item._set_data_quality_warnings(dqw_fetcher)
        logger.info(f"Populated permissions for item (ID: {item.id})")

    def _get_data_quality_warnings(self, item: HasId, req_options: Optional["RequestOptions"] = None) -> list[DQWItem]:
        url = f"{self.baseurl}/{item.id}"
        server_response = self.get_request(url, req_options)
        dqws = DQWItem.from_response(server_response.content, self.parent_srv.namespace)

        return dqws
