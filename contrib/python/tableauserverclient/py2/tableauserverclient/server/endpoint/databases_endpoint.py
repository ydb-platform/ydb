from .endpoint import api, Endpoint
from .exceptions import MissingRequiredFieldError
from .permissions_endpoint import _PermissionsEndpoint
from .default_permissions_endpoint import _DefaultPermissionsEndpoint

from .. import RequestFactory, DatabaseItem, TableItem, PaginationItem, Permission

import logging

logger = logging.getLogger('tableau.endpoint.databases')


class Databases(Endpoint):
    def __init__(self, parent_srv):
        super(Databases, self).__init__(parent_srv)

        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)
        self._default_permissions = _DefaultPermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self):
        return "{0}/sites/{1}/databases".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    @api(version="3.5")
    def get(self, req_options=None):
        logger.info('Querying all databases on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_database_items = DatabaseItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_database_items, pagination_item

    # Get 1 database
    @api(version="3.5")
    def get_by_id(self, database_id):
        if not database_id:
            error = "database ID undefined."
            raise ValueError(error)
        logger.info('Querying single database (ID: {0})'.format(database_id))
        url = "{0}/{1}".format(self.baseurl, database_id)
        server_response = self.get_request(url)
        return DatabaseItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.5")
    def delete(self, database_id):
        if not database_id:
            error = "Database ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, database_id)
        self.delete_request(url)
        logger.info('Deleted single database (ID: {0})'.format(database_id))

    @api(version="3.5")
    def update(self, database_item):
        if not database_item.id:
            error = "Database item missing ID."
            raise MissingRequiredFieldError(error)

        url = "{0}/{1}".format(self.baseurl, database_item.id)
        update_req = RequestFactory.Database.update_req(database_item)
        server_response = self.put_request(url, update_req)
        logger.info('Updated database item (ID: {0})'.format(database_item.id))
        updated_database = DatabaseItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return updated_database

    # Not Implemented Yet
    @api(version="99")
    def populate_tables(self, database_item):
        if not database_item.id:
            error = "database item missing ID. database must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def column_fetcher():
            return self._get_tables_for_database(database_item)

        database_item._set_tables(column_fetcher)
        logger.info('Populated tables for database (ID: {0}'.format(database_item.id))

    def _get_tables_for_database(self, database_item):
        url = "{0}/{1}/tables".format(self.baseurl, database_item.id)
        server_response = self.get_request(url)
        tables = TableItem.from_response(server_response.content,
                                         self.parent_srv.namespace)
        return tables

    @api(version='3.5')
    def populate_permissions(self, item):
        self._permissions.populate(item)

    @api(version='3.5')
    def update_permission(self, item, rules):
        return self._permissions.update(item, rules)

    @api(version='3.5')
    def delete_permission(self, item, rules):
        self._permissions.delete(item, rules)

    @api(version='3.5')
    def populate_table_default_permissions(self, item):
        self._default_permissions.populate_default_permissions(item, Permission.Resource.Table)

    @api(version='3.5')
    def update_table_default_permissions(self, item):
        return self._default_permissions.update_default_permissions(item, Permission.Resource.Table)

    @api(version='3.5')
    def delete_table_default_permissions(self, item):
        self._default_permissions.delete_default_permissions(item, Permission.Resource.Table)
