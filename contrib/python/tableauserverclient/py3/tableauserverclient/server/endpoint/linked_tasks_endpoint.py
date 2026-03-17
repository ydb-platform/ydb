from typing import Optional, Union

from tableauserverclient.helpers.logging import logger
from tableauserverclient.models.linked_tasks_item import LinkedTaskItem, LinkedTaskJobItem
from tableauserverclient.models.pagination_item import PaginationItem
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api
from tableauserverclient.server.request_factory import RequestFactory
from tableauserverclient.server.request_options import RequestOptions


class LinkedTasks(QuerysetEndpoint[LinkedTaskItem]):
    def __init__(self, parent_srv):
        super().__init__(parent_srv)
        self._parent_srv = parent_srv

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/tasks/linked"

    @api(version="3.15")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[LinkedTaskItem], PaginationItem]:
        logger.info("Querying all linked tasks on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_group_items = LinkedTaskItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_group_items, pagination_item

    @api(version="3.15")
    def get_by_id(self, linked_task: Union[LinkedTaskItem, str]) -> LinkedTaskItem:
        task_id = getattr(linked_task, "id", linked_task)
        logger.info("Querying all linked tasks on site")
        url = f"{self.baseurl}/{task_id}"
        server_response = self.get_request(url)
        all_group_items = LinkedTaskItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_group_items[0]

    @api(version="3.15")
    def run_now(self, linked_task: Union[LinkedTaskItem, str]) -> LinkedTaskJobItem:
        task_id = getattr(linked_task, "id", linked_task)
        logger.info(f"Running linked task {task_id} now")
        url = f"{self.baseurl}/{task_id}/runNow"
        empty_req = RequestFactory.Empty.empty_req()
        server_response = self.post_request(url, empty_req)
        return LinkedTaskJobItem.from_response(server_response.content, self.parent_srv.namespace)
