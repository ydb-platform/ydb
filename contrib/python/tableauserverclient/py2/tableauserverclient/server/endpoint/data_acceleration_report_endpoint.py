from .endpoint import api, Endpoint
from .permissions_endpoint import _PermissionsEndpoint
from .default_permissions_endpoint import _DefaultPermissionsEndpoint

from ...models.data_acceleration_report_item import DataAccelerationReportItem

import logging

logger = logging.getLogger('tableau.endpoint.data_acceleration_report')


class DataAccelerationReport(Endpoint):
    def __init__(self, parent_srv):
        super(DataAccelerationReport, self).__init__(parent_srv)

        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)
        self._default_permissions = _DefaultPermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self):
        return "{0}/sites/{1}/dataAccelerationReport".format(
            self.parent_srv.baseurl, self.parent_srv.site_id)

    @api(version="3.8")
    def get(self, req_options=None):
        logger.info("Querying data acceleration report")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        data_acceleration_report = DataAccelerationReportItem.from_response(
            server_response.content, self.parent_srv.namespace)
        return data_acceleration_report
