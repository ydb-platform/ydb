from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from .resource_tagger import _ResourceTagger
from .permissions_endpoint import _PermissionsEndpoint
from .. import ViewItem, PaginationItem

from contextlib import closing
import logging

logger = logging.getLogger('tableau.endpoint.views')


class Views(Endpoint):
    def __init__(self, parent_srv):
        super(Views, self).__init__(parent_srv)
        self._resource_tagger = _ResourceTagger(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

    # Used because populate_preview_image functionaliy requires workbook endpoint
    @property
    def siteurl(self):
        return "{0}/sites/{1}".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    @property
    def baseurl(self):
        return "{0}/views".format(self.siteurl)

    @api(version="2.2")
    def get(self, req_options=None, usage=False):
        logger.info('Querying all views on site')
        url = self.baseurl
        if usage:
            url += "?includeUsageStatistics=true"
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_view_items = ViewItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_view_items, pagination_item

    @api(version="2.0")
    def populate_preview_image(self, view_item):
        if not view_item.id or not view_item.workbook_id:
            error = "View item missing ID or workbook ID."
            raise MissingRequiredFieldError(error)

        def image_fetcher():
            return self._get_preview_for_view(view_item)

        view_item._set_preview_image(image_fetcher)
        logger.info('Populated preview image for view (ID: {0})'.format(view_item.id))

    def _get_preview_for_view(self, view_item):
        url = "{0}/workbooks/{1}/views/{2}/previewImage".format(self.siteurl,
                                                                view_item.workbook_id,
                                                                view_item.id)
        server_response = self.get_request(url)
        image = server_response.content
        return image

    @api(version="2.5")
    def populate_image(self, view_item, req_options=None):
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def image_fetcher():
            return self._get_view_image(view_item, req_options)

        view_item._set_image(image_fetcher)
        logger.info("Populated image for view (ID: {0})".format(view_item.id))

    def _get_view_image(self, view_item, req_options):
        url = "{0}/{1}/image".format(self.baseurl, view_item.id)
        server_response = self.get_request(url, req_options)
        image = server_response.content
        return image

    @api(version="2.7")
    def populate_pdf(self, view_item, req_options=None):
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def pdf_fetcher():
            return self._get_view_pdf(view_item, req_options)

        view_item._set_pdf(pdf_fetcher)
        logger.info("Populated pdf for view (ID: {0})".format(view_item.id))

    def _get_view_pdf(self, view_item, req_options):
        url = "{0}/{1}/pdf".format(self.baseurl, view_item.id)
        server_response = self.get_request(url, req_options)
        pdf = server_response.content
        return pdf

    @api(version="2.7")
    def populate_csv(self, view_item, req_options=None):
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def csv_fetcher():
            return self._get_view_csv(view_item, req_options)

        view_item._set_csv(csv_fetcher)
        logger.info("Populated csv for view (ID: {0})".format(view_item.id))

    def _get_view_csv(self, view_item, req_options):
        url = "{0}/{1}/data".format(self.baseurl, view_item.id)

        with closing(self.get_request(url, request_object=req_options, parameters={"stream": True})) as server_response:
            csv = server_response.iter_content(1024)
        return csv

    @api(version='3.2')
    def populate_permissions(self, item):
        self._permissions.populate(item)

    @api(version='3.2')
    def update_permissions(self, resource, rules):
        return self._permissions.update(resource, rules)

    @api(version='3.2')
    def delete_permission(self, item, capability_item):
        return self._permissions.delete(item, capability_item)

    # Update view. Currently only tags can be updated
    def update(self, view_item):
        if not view_item.id:
            error = "View item missing ID. View must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        self._resource_tagger.update_tags(self.baseurl, view_item)

        # Returning view item to stay consistent with datasource/view update functions
        return view_item
