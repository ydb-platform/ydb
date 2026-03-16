from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from .. import RequestFactory
from ...models import FavoriteItem
from ..pager import Pager
import xml.etree.ElementTree as ET
import logging
import copy

logger = logging.getLogger('tableau.endpoint.favorites')


class Favorites(Endpoint):
    @property
    def baseurl(self):
        return "{0}/sites/{1}/favorites".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    # Gets all favorites
    @api(version="2.5")
    def get(self, user_item, req_options=None):
        logger.info('Querying all favorites for user {0}'.format(user_item.name))
        url = '{0}/{1}'.format(self.baseurl, user_item.id)
        server_response = self.get_request(url, req_options)

        user_item._favorites = FavoriteItem.from_response(server_response.content, self.parent_srv.namespace)

    @api(version="2.0")
    def add_favorite_workbook(self, user_item, workbook_item):
        url = '{0}/{1}'.format(self.baseurl, user_item.id)
        add_req = RequestFactory.Favorite.add_workbook_req(workbook_item.id, workbook_item.name)
        server_response = self.put_request(url, add_req)
        logger.info('Favorited {0} for user (ID: {1})'.format(workbook_item.name, user_item.id))

    @api(version="2.0")
    def add_favorite_view(self, user_item, view_item):
        url = '{0}/{1}'.format(self.baseurl, user_item.id)
        add_req = RequestFactory.Favorite.add_view_req(view_item.id, view_item.name)
        server_response = self.put_request(url, add_req)
        logger.info('Favorited {0} for user (ID: {1})'.format(view_item.name, user_item.id))

    @api(version="2.3")
    def add_favorite_datasource(self, user_item, datasource_item):
        url = '{0}/{1}'.format(self.baseurl, user_item.id)
        add_req = RequestFactory.Favorite.add_datasource_req(datasource_item.id, datasource_item.name)
        server_response = self.put_request(url, add_req)
        logger.info('Favorited {0} for user (ID: {1})'.format(datasource_item.name, user_item.id))

    @api(version="3.1")
    def add_favorite_project(self, user_item, project_item):
        url = '{0}/{1}'.format(self.baseurl, user_item.id)
        add_req = RequestFactory.Favorite.add_project_req(project_item.id, project_item.name)
        server_response = self.put_request(url, add_req)
        logger.info('Favorited {0} for user (ID: {1})'.format(project_item.name, user_item.id))

    @api(version="2.0")
    def delete_favorite_workbook(self, user_item, workbook_item):
        url = '{0}/{1}/workbooks/{2}'.format(self.baseurl, user_item.id, workbook_item.id)
        logger.info('Removing favorite {0} for user (ID: {1})'.format(workbook_item.id, user_item.id))
        self.delete_request(url)

    @api(version="2.0")
    def delete_favorite_view(self, user_item, view_item):
        url = '{0}/{1}/views/{2}'.format(self.baseurl, user_item.id, view_item.id)
        logger.info('Removing favorite {0} for user (ID: {1})'.format(view_item.id, user_item.id))
        self.delete_request(url)

    @api(version="2.3")
    def delete_favorite_datasource(self, user_item, datasource_item):
        url = '{0}/{1}/datasources/{2}'.format(self.baseurl, user_item.id, datasource_item.id)
        logger.info('Removing favorite {0} for user (ID: {1})'.format(datasource_item.id, user_item.id))
        self.delete_request(url)

    @api(version="3.1")
    def delete_favorite_project(self, user_item, project_item):
        url = '{0}/{1}/projects/{2}'.format(self.baseurl, user_item.id, project_item.id)
        logger.info('Removing favorite {0} for user (ID: {1})'.format(project_item.id, user_item.id))
        self.delete_request(url)
