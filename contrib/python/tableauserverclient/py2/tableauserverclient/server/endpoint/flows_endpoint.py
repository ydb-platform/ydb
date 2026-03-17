from .endpoint import Endpoint, api
from .exceptions import InternalServerError, MissingRequiredFieldError
from .permissions_endpoint import _PermissionsEndpoint
from .fileuploads_endpoint import Fileuploads
from .resource_tagger import _ResourceTagger
from .. import RequestFactory, FlowItem, PaginationItem, ConnectionItem
from ...filesys_helpers import to_filename, make_download_path
from ...models.job_item import JobItem

import os
import logging
import copy
import cgi
from contextlib import closing

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB

ALLOWED_FILE_EXTENSIONS = ['tfl', 'tflx']

logger = logging.getLogger('tableau.endpoint.flows')


class Flows(Endpoint):
    def __init__(self, parent_srv):
        super(Flows, self).__init__(parent_srv)
        self._resource_tagger = _ResourceTagger(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self):
        return "{0}/sites/{1}/flows".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    # Get all flows
    @api(version="3.3")
    def get(self, req_options=None):
        logger.info('Querying all flows on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_flow_items = FlowItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_flow_items, pagination_item

    # Get 1 flow by id
    @api(version="3.3")
    def get_by_id(self, flow_id):
        if not flow_id:
            error = "Flow ID undefined."
            raise ValueError(error)
        logger.info('Querying single flow (ID: {0})'.format(flow_id))
        url = "{0}/{1}".format(self.baseurl, flow_id)
        server_response = self.get_request(url)
        return FlowItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Populate flow item's connections
    @api(version="3.3")
    def populate_connections(self, flow_item):
        if not flow_item.id:
            error = 'Flow item missing ID. Flow must be retrieved from server first.'
            raise MissingRequiredFieldError(error)

        def connections_fetcher():
            return self._get_flow_connections(flow_item)

        flow_item._set_connections(connections_fetcher)
        logger.info('Populated connections for flow (ID: {0})'.format(flow_item.id))

    def _get_flow_connections(self, flow_item, req_options=None):
        url = '{0}/{1}/connections'.format(self.baseurl, flow_item.id)
        server_response = self.get_request(url, req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return connections

    # Delete 1 flow by id
    @api(version="3.3")
    def delete(self, flow_id):
        if not flow_id:
            error = "Flow ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, flow_id)
        self.delete_request(url)
        logger.info('Deleted single flow (ID: {0})'.format(flow_id))

    # Download 1 flow by id
    @api(version="3.3")
    def download(self, flow_id, filepath=None):
        if not flow_id:
            error = "Flow ID undefined."
            raise ValueError(error)
        url = "{0}/{1}/content".format(self.baseurl, flow_id)

        with closing(self.get_request(url, parameters={'stream': True})) as server_response:
            _, params = cgi.parse_header(server_response.headers['Content-Disposition'])
            filename = to_filename(os.path.basename(params['filename']))

            download_path = make_download_path(filepath, filename)

            with open(download_path, 'wb') as f:
                for chunk in server_response.iter_content(1024):  # 1KB
                    f.write(chunk)

        logger.info('Downloaded flow to {0} (ID: {1})'.format(download_path, flow_id))
        return os.path.abspath(download_path)

    # Update flow
    @api(version="3.3")
    def update(self, flow_item):
        if not flow_item.id:
            error = 'Flow item missing ID. Flow must be retrieved from server first.'
            raise MissingRequiredFieldError(error)

        self._resource_tagger.update_tags(self.baseurl, flow_item)

        # Update the flow itself
        url = "{0}/{1}".format(self.baseurl, flow_item.id)
        update_req = RequestFactory.Flow.update_req(flow_item)
        server_response = self.put_request(url, update_req)
        logger.info('Updated flow item (ID: {0})'.format(flow_item.id))
        updated_flow = copy.copy(flow_item)
        return updated_flow._parse_common_elements(server_response.content, self.parent_srv.namespace)

    # Update flow connections
    @api(version="3.3")
    def update_connection(self, flow_item, connection_item):
        url = "{0}/{1}/connections/{2}".format(self.baseurl, flow_item.id, connection_item.id)

        update_req = RequestFactory.Connection.update_req(connection_item)
        server_response = self.put_request(url, update_req)
        connection = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info('Updated flow item (ID: {0} & connection item {1}'.format(flow_item.id,
                                                                              connection_item.id))
        return connection

    @api(version="3.3")
    def refresh(self, flow_item):
        url = "{0}/{1}/run".format(self.baseurl, flow_item.id)
        empty_req = RequestFactory.Empty.empty_req()
        server_response = self.post_request(url, empty_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    # Publish flow
    @api(version="3.3")
    def publish(self, flow_item, file_path, mode, connections=None):
        if not os.path.isfile(file_path):
            error = "File path does not lead to an existing file."
            raise IOError(error)
        if not mode or not hasattr(self.parent_srv.PublishMode, mode):
            error = 'Invalid mode defined.'
            raise ValueError(error)

        filename = os.path.basename(file_path)
        file_extension = os.path.splitext(filename)[1][1:]

        # If name is not defined, grab the name from the file to publish
        if not flow_item.name:
            flow_item.name = os.path.splitext(filename)[0]
        if file_extension not in ALLOWED_FILE_EXTENSIONS:
            error = "Only {} files can be published as flows.".format(', '.join(ALLOWED_FILE_EXTENSIONS))
            raise ValueError(error)

        # Construct the url with the defined mode
        url = "{0}?flowType={1}".format(self.baseurl, file_extension)
        if mode == self.parent_srv.PublishMode.Overwrite or mode == self.parent_srv.PublishMode.Append:
            url += '&{0}=true'.format(mode.lower())

        # Determine if chunking is required (64MB is the limit for single upload method)
        if os.path.getsize(file_path) >= FILESIZE_LIMIT:
            logger.info('Publishing {0} to server with chunking method (flow over 64MB)'.format(filename))
            upload_session_id = Fileuploads.upload_chunks(self.parent_srv, file_path)
            url = "{0}&uploadSessionId={1}".format(url, upload_session_id)
            xml_request, content_type = RequestFactory.Flow.publish_req_chunked(flow_item,
                                                                                connections)
        else:
            logger.info('Publishing {0} to server'.format(filename))
            with open(file_path, 'rb') as f:
                file_contents = f.read()
            xml_request, content_type = RequestFactory.Flow.publish_req(flow_item,
                                                                        filename,
                                                                        file_contents,
                                                                        connections)

        # Send the publishing request to server
        try:
            server_response = self.post_request(url, xml_request, content_type)
        except InternalServerError as err:
            if err.code == 504:
                err.content = "Timeout error while publishing. Please use asynchronous publishing to avoid timeouts."
            raise err
        else:
            new_flow = FlowItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info('Published {0} (ID: {1})'.format(filename, new_flow.id))
            return new_flow

        server_response = self.post_request(url, xml_request, content_type)
        new_flow = FlowItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info('Published {0} (ID: {1})'.format(filename, new_flow.id))
        return new_flow

    @api(version='3.3')
    def populate_permissions(self, item):
        self._permissions.populate(item)

    @api(version='3.3')
    def update_permission(self, item, permission_item):
        self._permissions.update(item, permission_item)

    @api(version='3.3')
    def delete_permission(self, item, capability_item):
        self._permissions.delete(item, capability_item)
