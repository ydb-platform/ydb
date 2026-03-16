from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from .. import RequestFactory, GroupItem, UserItem, PaginationItem
from ..pager import Pager

import logging

logger = logging.getLogger('tableau.endpoint.groups')

UNLICENSED_USER = UserItem.Roles.Unlicensed


class Groups(Endpoint):
    @property
    def baseurl(self):
        return "{0}/sites/{1}/groups".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    # Gets all groups
    @api(version="2.0")
    def get(self, req_options=None):
        logger.info('Querying all groups on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_group_items = GroupItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_group_items, pagination_item

    # Gets all users in a given group
    @api(version="2.0")
    def populate_users(self, group_item, req_options=None):
        if not group_item.id:
            error = "Group item missing ID. Group must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        # Define an inner function that we bind to the model_item's `.user` property.

        def user_pager():
            return Pager(lambda options: self._get_users_for_group(group_item, options), req_options)

        group_item._set_users(user_pager)

    def _get_users_for_group(self, group_item, req_options=None):
        url = "{0}/{1}/users".format(self.baseurl, group_item.id)
        server_response = self.get_request(url, req_options)
        user_item = UserItem.from_response(server_response.content, self.parent_srv.namespace)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        logger.info('Populated users for group (ID: {0})'.format(group_item.id))
        return user_item, pagination_item

    # Deletes 1 group by id
    @api(version="2.0")
    def delete(self, group_id):
        if not group_id:
            error = "Group ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, group_id)
        self.delete_request(url)
        logger.info('Deleted single group (ID: {0})'.format(group_id))

    @api(version="2.0")
    def update(self, group_item, default_site_role=UNLICENSED_USER):
        if not group_item.id:
            error = "Group item missing ID."
            raise MissingRequiredFieldError(error)
        url = "{0}/{1}".format(self.baseurl, group_item.id)
        update_req = RequestFactory.Group.update_req(group_item, default_site_role)
        server_response = self.put_request(url, update_req)
        logger.info('Updated group item (ID: {0})'.format(group_item.id))
        updated_group = GroupItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return updated_group

    # Create a 'local' Tableau group
    @api(version="2.0")
    def create(self, group_item):
        url = self.baseurl
        create_req = RequestFactory.Group.create_req(group_item)
        server_response = self.post_request(url, create_req)
        return GroupItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Removes 1 user from 1 group
    @api(version="2.0")
    def remove_user(self, group_item, user_id):
        if not group_item.id:
            error = "Group item missing ID."
            raise MissingRequiredFieldError(error)
        if not user_id:
            error = "User ID undefined."
            raise ValueError(error)
        url = "{0}/{1}/users/{2}".format(self.baseurl, group_item.id, user_id)
        self.delete_request(url)
        logger.info('Removed user (id: {0}) from group (ID: {1})'.format(user_id, group_item.id))

    # Adds 1 user to 1 group
    @api(version="2.0")
    def add_user(self, group_item, user_id):
        if not group_item.id:
            error = "Group item missing ID."
            raise MissingRequiredFieldError(error)
        if not user_id:
            error = "User ID undefined."
            raise ValueError(error)
        url = "{0}/{1}/users".format(self.baseurl, group_item.id)
        add_req = RequestFactory.Group.add_user_req(user_id)
        server_response = self.post_request(url, add_req)
        return UserItem.from_response(server_response.content, self.parent_srv.namespace).pop()
        logger.info('Added user (id: {0}) to group (ID: {1})'.format(user_id, group_item.id))
