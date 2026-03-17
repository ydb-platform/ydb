from tableauserverclient.server.endpoint.endpoint import Endpoint, api
from requests import Response
from tableauserverclient.helpers.logging import logger
from tableauserverclient.models import (
    DatasourceItem,
    FavoriteItem,
    FlowItem,
    MetricItem,
    ProjectItem,
    Resource,
    TableauItem,
    UserItem,
    ViewItem,
    WorkbookItem,
)
from tableauserverclient.server import RequestFactory, RequestOptions
from typing import Optional


class Favorites(Endpoint):
    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/favorites"

    # Gets all favorites
    @api(version="2.5")
    def get(self, user_item: UserItem, req_options: Optional[RequestOptions] = None) -> None:
        logger.info(f"Querying all favorites for user {user_item.name}")
        url = f"{self.baseurl}/{user_item.id}"
        server_response = self.get_request(url, req_options)
        user_item._favorites = FavoriteItem.from_response(server_response.content, self.parent_srv.namespace)

    # ---------add to favorites

    @api(version="3.15")
    def add_favorite(self, user_item: UserItem, content_type: str, item: TableauItem) -> "Response":
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_request(item.id, content_type, item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited {item.name} for user (ID: {user_item.id})")
        return server_response

    @api(version="2.0")
    def add_favorite_workbook(self, user_item: UserItem, workbook_item: WorkbookItem) -> None:
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_workbook_req(workbook_item.id, workbook_item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited {workbook_item.name} for user (ID: {user_item.id})")

    @api(version="2.0")
    def add_favorite_view(self, user_item: UserItem, view_item: ViewItem) -> None:
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_view_req(view_item.id, view_item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited {view_item.name} for user (ID: {user_item.id})")

    @api(version="2.3")
    def add_favorite_datasource(self, user_item: UserItem, datasource_item: DatasourceItem) -> None:
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_datasource_req(datasource_item.id, datasource_item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited {datasource_item.name} for user (ID: {user_item.id})")

    @api(version="3.1")
    def add_favorite_project(self, user_item: UserItem, project_item: ProjectItem) -> None:
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_project_req(project_item.id, project_item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited {project_item.name} for user (ID: {user_item.id})")

    @api(version="3.3")
    def add_favorite_flow(self, user_item: UserItem, flow_item: FlowItem) -> None:
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_flow_req(flow_item.id, flow_item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited {flow_item.name} for user (ID: {user_item.id})")

    @api(version="3.3")
    def add_favorite_metric(self, user_item: UserItem, metric_item: MetricItem) -> None:
        url = f"{self.baseurl}/{user_item.id}"
        add_req = RequestFactory.Favorite.add_request(metric_item.id, Resource.Metric, metric_item.name)
        server_response = self.put_request(url, add_req)
        logger.info(f"Favorited metric {metric_item.name} for user (ID: {user_item.id})")

    # ------- delete from favorites
    # Response:
    """
    <tsResponse>
      <favorites>
        <favorite label="favorite-label">
      </favorites>
    </tsResponse>
    """

    @api(version="3.15")
    def delete_favorite(self, user_item: UserItem, content_type: Resource, item: TableauItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/{content_type}/{item.id}"
        logger.info(f"Removing favorite {content_type}({item.id}) for user (ID: {user_item.id})")
        self.delete_request(url)

    @api(version="2.0")
    def delete_favorite_workbook(self, user_item: UserItem, workbook_item: WorkbookItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/workbooks/{workbook_item.id}"
        logger.info(f"Removing favorite workbook {workbook_item.id} for user (ID: {user_item.id})")
        self.delete_request(url)

    @api(version="2.0")
    def delete_favorite_view(self, user_item: UserItem, view_item: ViewItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/views/{view_item.id}"
        logger.info(f"Removing favorite view {view_item.id} for user (ID: {user_item.id})")
        self.delete_request(url)

    @api(version="2.3")
    def delete_favorite_datasource(self, user_item: UserItem, datasource_item: DatasourceItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/datasources/{datasource_item.id}"
        logger.info(f"Removing favorite {datasource_item.id} for user (ID: {user_item.id})")
        self.delete_request(url)

    @api(version="3.1")
    def delete_favorite_project(self, user_item: UserItem, project_item: ProjectItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/projects/{project_item.id}"
        logger.info(f"Removing favorite project {project_item.id} for user (ID: {user_item.id})")
        self.delete_request(url)

    @api(version="3.3")
    def delete_favorite_flow(self, user_item: UserItem, flow_item: FlowItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/flows/{flow_item.id}"
        logger.info(f"Removing favorite flow {flow_item.id} for user (ID: {user_item.id})")
        self.delete_request(url)

    @api(version="3.15")
    def delete_favorite_metric(self, user_item: UserItem, metric_item: MetricItem) -> None:
        url = f"{self.baseurl}/{user_item.id}/metrics/{metric_item.id}"
        logger.info(f"Removing favorite metric {metric_item.id} for user (ID: {user_item.id})")
        self.delete_request(url)
