from .endpoint import Endpoint, api, parameter_added_in
from .exceptions import InternalServerError, MissingRequiredFieldError
from .permissions_endpoint import _PermissionsEndpoint
from .fileuploads_endpoint import Fileuploads
from .resource_tagger import _ResourceTagger
from .. import RequestFactory, DatasourceItem, PaginationItem, ConnectionItem
from ...filesys_helpers import to_filename, make_download_path
from ...models.job_item import JobItem

import os
import logging
import copy
import cgi
from contextlib import closing

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB

ALLOWED_FILE_EXTENSIONS = ['tds', 'tdsx', 'tde', 'hyper']

logger = logging.getLogger('tableau.endpoint.datasources')


class Datasources(Endpoint):
    def __init__(self, parent_srv):
        super(Datasources, self).__init__(parent_srv)
        self._resource_tagger = _ResourceTagger(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self):
        return "{0}/sites/{1}/datasources".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    # Get all datasources
    @api(version="2.0")
    def get(self, req_options=None):
        logger.info('Querying all datasources on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_datasource_items = DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_datasource_items, pagination_item

    # Get 1 datasource by id
    @api(version="2.0")
    def get_by_id(self, datasource_id):
        if not datasource_id:
            error = "Datasource ID undefined."
            raise ValueError(error)
        logger.info('Querying single datasource (ID: {0})'.format(datasource_id))
        url = "{0}/{1}".format(self.baseurl, datasource_id)
        server_response = self.get_request(url)
        return DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Populate datasource item's connections
    @api(version="2.0")
    def populate_connections(self, datasource_item):
        if not datasource_item.id:
            error = 'Datasource item missing ID. Datasource must be retrieved from server first.'
            raise MissingRequiredFieldError(error)

        def connections_fetcher():
            return self._get_datasource_connections(datasource_item)

        datasource_item._set_connections(connections_fetcher)
        logger.info('Populated connections for datasource (ID: {0})'.format(datasource_item.id))

    def _get_datasource_connections(self, datasource_item, req_options=None):
        url = '{0}/{1}/connections'.format(self.baseurl, datasource_item.id)
        server_response = self.get_request(url, req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return connections

    # Delete 1 datasource by id
    @api(version="2.0")
    def delete(self, datasource_id):
        if not datasource_id:
            error = "Datasource ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, datasource_id)
        self.delete_request(url)
        logger.info('Deleted single datasource (ID: {0})'.format(datasource_id))

    # Download 1 datasource by id
    @api(version="2.0")
    @parameter_added_in(no_extract='2.5')
    @parameter_added_in(include_extract='2.5')
    def download(self, datasource_id, filepath=None, include_extract=True, no_extract=None):
        if not datasource_id:
            error = "Datasource ID undefined."
            raise ValueError(error)
        url = "{0}/{1}/content".format(self.baseurl, datasource_id)

        if no_extract is False or no_extract is True:
            import warnings
            warnings.warn('no_extract is deprecated, use include_extract instead.', DeprecationWarning)
            include_extract = not no_extract

        if not include_extract:
            url += "?includeExtract=False"

        with closing(self.get_request(url, parameters={'stream': True})) as server_response:
            _, params = cgi.parse_header(server_response.headers['Content-Disposition'])
            filename = to_filename(os.path.basename(params['filename']))

            download_path = make_download_path(filepath, filename)

            with open(download_path, 'wb') as f:
                for chunk in server_response.iter_content(1024):  # 1KB
                    f.write(chunk)

        logger.info('Downloaded datasource to {0} (ID: {1})'.format(download_path, datasource_id))
        return os.path.abspath(download_path)

    # Update datasource
    @api(version="2.0")
    def update(self, datasource_item):
        if not datasource_item.id:
            error = 'Datasource item missing ID. Datasource must be retrieved from server first.'
            raise MissingRequiredFieldError(error)

        self._resource_tagger.update_tags(self.baseurl, datasource_item)

        # Update the datasource itself
        url = "{0}/{1}".format(self.baseurl, datasource_item.id)
        update_req = RequestFactory.Datasource.update_req(datasource_item)
        server_response = self.put_request(url, update_req)
        logger.info('Updated datasource item (ID: {0})'.format(datasource_item.id))
        updated_datasource = copy.copy(datasource_item)
        return updated_datasource._parse_common_elements(
            server_response.content, self.parent_srv.namespace)

    # Update datasource connections
    @api(version="2.3")
    def update_connection(self, datasource_item, connection_item):
        url = "{0}/{1}/connections/{2}".format(self.baseurl, datasource_item.id, connection_item.id)

        update_req = RequestFactory.Connection.update_req(connection_item)
        server_response = self.put_request(url, update_req)
        connection = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info('Updated datasource item (ID: {0} & connection item {1}'.format(datasource_item.id,
                                                                                    connection_item.id))
        return connection

    @api(version="2.8")
    def refresh(self, datasource_item):
        id_ = getattr(datasource_item, 'id', datasource_item)
        url = "{0}/{1}/refresh".format(self.baseurl, id_)
        empty_req = RequestFactory.Empty.empty_req()
        server_response = self.post_request(url, empty_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    # Publish datasource
    @api(version="2.0")
    @parameter_added_in(connections="2.8")
    @parameter_added_in(as_job='3.0')
    def publish(self, datasource_item, file_path, mode, connection_credentials=None, connections=None, as_job=False):
        if not os.path.isfile(file_path):
            error = "File path does not lead to an existing file."
            raise IOError(error)
        if not mode or not hasattr(self.parent_srv.PublishMode, mode):
            error = 'Invalid mode defined.'
            raise ValueError(error)

        filename = os.path.basename(file_path)
        file_extension = os.path.splitext(filename)[1][1:]

        # If name is not defined, grab the name from the file to publish
        if not datasource_item.name:
            datasource_item.name = os.path.splitext(filename)[0]
        if file_extension not in ALLOWED_FILE_EXTENSIONS:
            error = "Only {} files can be published as datasources.".format(', '.join(ALLOWED_FILE_EXTENSIONS))
            raise ValueError(error)

        # Construct the url with the defined mode
        url = "{0}?datasourceType={1}".format(self.baseurl, file_extension)
        if mode == self.parent_srv.PublishMode.Overwrite or mode == self.parent_srv.PublishMode.Append:
            url += '&{0}=true'.format(mode.lower())

        if as_job:
            url += '&{0}=true'.format('asJob')

        # Determine if chunking is required (64MB is the limit for single upload method)
        if os.path.getsize(file_path) >= FILESIZE_LIMIT:
            logger.info('Publishing {0} to server with chunking method (datasource over 64MB)'.format(filename))
            upload_session_id = Fileuploads.upload_chunks(self.parent_srv, file_path)
            url = "{0}&uploadSessionId={1}".format(url, upload_session_id)
            xml_request, content_type = RequestFactory.Datasource.publish_req_chunked(datasource_item,
                                                                                      connection_credentials,
                                                                                      connections)
        else:
            logger.info('Publishing {0} to server'.format(filename))
            with open(file_path, 'rb') as f:
                file_contents = f.read()
            xml_request, content_type = RequestFactory.Datasource.publish_req(datasource_item,
                                                                              filename,
                                                                              file_contents,
                                                                              connection_credentials,
                                                                              connections)

        # Send the publishing request to server
        try:
            server_response = self.post_request(url, xml_request, content_type)
        except InternalServerError as err:
            if err.code == 504 and not as_job:
                err.content = "Timeout error while publishing. Please use asynchronous publishing to avoid timeouts."
            raise err

        if as_job:
            new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info('Published {0} (JOB_ID: {1}'.format(filename, new_job.id))
            return new_job
        else:
            new_datasource = DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info('Published {0} (ID: {1})'.format(filename, new_datasource.id))
            return new_datasource
        server_response = self.post_request(url, xml_request, content_type)
        new_datasource = DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info('Published {0} (ID: {1})'.format(filename, new_datasource.id))
        return new_datasource

    @api(version='2.0')
    def populate_permissions(self, item):
        self._permissions.populate(item)

    @api(version='2.0')
    def update_permission(self, item, permission_item):
        self._permissions.update(item, permission_item)

    @api(version='2.0')
    def delete_permission(self, item, capability_item):
        self._permissions.delete(item, capability_item)
