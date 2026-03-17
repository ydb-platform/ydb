from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from .. import RequestFactory, SiteItem, PaginationItem

import copy
import logging

logger = logging.getLogger('tableau.endpoint.sites')


class Sites(Endpoint):
    @property
    def baseurl(self):
        return "{0}/sites".format(self.parent_srv.baseurl)

    # Gets all sites
    @api(version="2.0")
    def get(self, req_options=None):
        logger.info('Querying all sites on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_site_items = SiteItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_site_items, pagination_item

    # Gets 1 site by id
    @api(version="2.0")
    def get_by_id(self, site_id):
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        logger.info('Querying single site (ID: {0})'.format(site_id))
        url = "{0}/{1}".format(self.baseurl, site_id)
        server_response = self.get_request(url)
        return SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Gets 1 site by name
    @api(version="2.0")
    def get_by_name(self, site_name):
        if not site_name:
            error = "Site Name undefined."
            raise ValueError(error)
        logger.info('Querying single site (Name: {0})'.format(site_name))
        url = "{0}/{1}?key=name".format(self.baseurl, site_name)
        server_response = self.get_request(url)
        return SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Gets 1 site by content url
    @api(version="2.0")
    def get_by_content_url(self, content_url):
        if content_url is None:
            error = "Content URL undefined."
            raise ValueError(error)
        logger.info('Querying single site (Content URL: {0})'.format(content_url))
        url = "{0}/{1}?key=contentUrl".format(self.baseurl, content_url)
        server_response = self.get_request(url)
        return SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Update site
    @api(version="2.0")
    def update(self, site_item):
        if not site_item.id:
            error = "Site item missing ID."
            raise MissingRequiredFieldError(error)
        if site_item.admin_mode:
            if site_item.admin_mode == SiteItem.AdminMode.ContentOnly and site_item.user_quota:
                error = 'You cannot set admin_mode to ContentOnly and also set a user quota'
                raise ValueError(error)

        url = "{0}/{1}".format(self.baseurl, site_item.id)
        update_req = RequestFactory.Site.update_req(site_item)
        server_response = self.put_request(url, update_req)
        logger.info('Updated site item (ID: {0})'.format(site_item.id))
        update_site = copy.copy(site_item)
        return update_site._parse_common_tags(server_response.content, self.parent_srv.namespace)

    # Delete 1 site object
    @api(version="2.0")
    def delete(self, site_id):
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, site_id)
        self.delete_request(url)
        # If we deleted the site we are logged into
        # then we are automatically logged out
        if site_id == self.parent_srv.site_id:
            logger.info('Deleting current site and clearing auth tokens')
            self.parent_srv._clear_auth()
        logger.info('Deleted single site (ID: {0}) and signed out'.format(site_id))

    # Create new site
    @api(version="2.0")
    def create(self, site_item):
        if site_item.admin_mode:
            if site_item.admin_mode == SiteItem.AdminMode.ContentOnly and site_item.user_quota:
                error = 'You cannot set admin_mode to ContentOnly and also set a user quota'
                raise ValueError(error)

        url = self.baseurl
        create_req = RequestFactory.Site.create_req(site_item)
        server_response = self.post_request(url, create_req)
        new_site = SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info('Created new site (ID: {0})'.format(new_site.id))
        return new_site
