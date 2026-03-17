from .endpoint import Endpoint, api, parameter_added_in
from .exceptions import InternalServerError, MissingRequiredFieldError
from .permissions_endpoint import _PermissionsEndpoint
from .fileuploads_endpoint import Fileuploads
from .resource_tagger import _ResourceTagger
from .. import RequestFactory, WorkbookItem, ConnectionItem, ViewItem, PaginationItem
from ...models.job_item import JobItem
from ...filesys_helpers import to_filename, make_download_path

import os
import logging
import copy
import cgi
from contextlib import closing

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64   # 64MB

ALLOWED_FILE_EXTENSIONS = ['twb', 'twbx']

logger = logging.getLogger('tableau.endpoint.workbooks')


class Workbooks(Endpoint):
    def __init__(self, parent_srv):
        super(Workbooks, self).__init__(parent_srv)
        self._resource_tagger = _ResourceTagger(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self):
        return "{0}/sites/{1}/workbooks".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    # Get all workbooks on site
    @api(version="2.0")
    def get(self, req_options=None):
        logger.info('Querying all workbooks on site')
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_workbook_items = WorkbookItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_workbook_items, pagination_item

    # Get 1 workbook
    @api(version="2.0")
    def get_by_id(self, workbook_id):
        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        logger.info('Querying single workbook (ID: {0})'.format(workbook_id))
        url = "{0}/{1}".format(self.baseurl, workbook_id)
        server_response = self.get_request(url)
        return WorkbookItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.8")
    def refresh(self, workbook_id):
        id_ = getattr(workbook_id, 'id', workbook_id)
        url = "{0}/{1}/refresh".format(self.baseurl, id_)
        empty_req = RequestFactory.Empty.empty_req()
        server_response = self.post_request(url, empty_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    # Delete 1 workbook by id
    @api(version="2.0")
    def delete(self, workbook_id):
        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        url = "{0}/{1}".format(self.baseurl, workbook_id)
        self.delete_request(url)
        logger.info('Deleted single workbook (ID: {0})'.format(workbook_id))

    # Update workbook
    @api(version="2.0")
    def update(self, workbook_item):
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        self._resource_tagger.update_tags(self.baseurl, workbook_item)

        # Update the workbook itself
        url = "{0}/{1}".format(self.baseurl, workbook_item.id)
        update_req = RequestFactory.Workbook.update_req(workbook_item)
        server_response = self.put_request(url, update_req)
        logger.info('Updated workbook item (ID: {0})'.format(workbook_item.id))
        updated_workbook = copy.copy(workbook_item)
        return updated_workbook._parse_common_tags(server_response.content, self.parent_srv.namespace)

    @api(version="2.3")
    def update_conn(self, *args, **kwargs):
        import warnings
        warnings.warn('update_conn is deprecated, please use update_connection instead')
        return self.update_connection(*args, **kwargs)

    # Update workbook_connection
    @api(version="2.3")
    def update_connection(self, workbook_item, connection_item):
        url = "{0}/{1}/connections/{2}".format(self.baseurl, workbook_item.id, connection_item.id)
        update_req = RequestFactory.Connection.update_req(connection_item)
        server_response = self.put_request(url, update_req)
        connection = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info('Updated workbook item (ID: {0} & connection item {1})'.format(workbook_item.id,
                                                                                   connection_item.id))
        return connection

    # Download workbook contents with option of passing in filepath
    @api(version="2.0")
    @parameter_added_in(no_extract='2.5')
    @parameter_added_in(include_extract='2.5')
    def download(self, workbook_id, filepath=None, include_extract=True, no_extract=None):
        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        url = "{0}/{1}/content".format(self.baseurl, workbook_id)

        if no_extract is False or no_extract is True:
            import warnings
            warnings.warn('no_extract is deprecated, use include_extract instead.', DeprecationWarning)
            include_extract = not no_extract

        if not include_extract:
            url += "?includeExtract=False"

        with closing(self.get_request(url, parameters={"stream": True})) as server_response:
            _, params = cgi.parse_header(server_response.headers['Content-Disposition'])
            filename = to_filename(os.path.basename(params['filename']))

            download_path = make_download_path(filepath, filename)

            with open(download_path, 'wb') as f:
                for chunk in server_response.iter_content(1024):  # 1KB
                    f.write(chunk)
        logger.info('Downloaded workbook to {0} (ID: {1})'.format(download_path, workbook_id))
        return os.path.abspath(download_path)

    # Get all views of workbook
    @api(version="2.0")
    def populate_views(self, workbook_item, usage=False):
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def view_fetcher():
            return self._get_views_for_workbook(workbook_item, usage)

        workbook_item._set_views(view_fetcher)
        logger.info('Populated views for workbook (ID: {0})'.format(workbook_item.id))

    def _get_views_for_workbook(self, workbook_item, usage):
        url = "{0}/{1}/views".format(self.baseurl, workbook_item.id)
        if usage:
            url += "?includeUsageStatistics=true"
        server_response = self.get_request(url)
        views = ViewItem.from_response(server_response.content,
                                       self.parent_srv.namespace,
                                       workbook_id=workbook_item.id)
        return views

    # Get all connections of workbook
    @api(version="2.0")
    def populate_connections(self, workbook_item):
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def connection_fetcher():
            return self._get_workbook_connections(workbook_item)

        workbook_item._set_connections(connection_fetcher)
        logger.info('Populated connections for workbook (ID: {0})'.format(workbook_item.id))

    def _get_workbook_connections(self, workbook_item, req_options=None):
        url = "{0}/{1}/connections".format(self.baseurl, workbook_item.id)
        server_response = self.get_request(url, req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return connections

    # Get the pdf of the entire workbook if its tabs are enabled, pdf of the default view if its tabs are disabled
    @api(version="3.4")
    def populate_pdf(self, workbook_item, req_options=None):
        if not workbook_item.id:
            error = "Workbook item missing ID."
            raise MissingRequiredFieldError(error)

        def pdf_fetcher():
            return self._get_wb_pdf(workbook_item, req_options)

        workbook_item._set_pdf(pdf_fetcher)
        logger.info("Populated pdf for workbook (ID: {0})".format(workbook_item.id))

    def _get_wb_pdf(self, workbook_item, req_options):
        url = "{0}/{1}/pdf".format(self.baseurl, workbook_item.id)
        server_response = self.get_request(url, req_options)
        pdf = server_response.content
        return pdf

    # Get preview image of workbook
    @api(version="2.0")
    def populate_preview_image(self, workbook_item):
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def image_fetcher():
            return self._get_wb_preview_image(workbook_item)

        workbook_item._set_preview_image(image_fetcher)
        logger.info('Populated preview image for workbook (ID: {0})'.format(workbook_item.id))

    def _get_wb_preview_image(self, workbook_item):
        url = "{0}/{1}/previewImage".format(self.baseurl, workbook_item.id)
        server_response = self.get_request(url)
        preview_image = server_response.content
        return preview_image

    @api(version='2.0')
    def populate_permissions(self, item):
        self._permissions.populate(item)

    @api(version='2.0')
    def update_permissions(self, resource, rules):
        return self._permissions.update(resource, rules)

    @api(version='2.0')
    def delete_permission(self, item, capability_item):
        return self._permissions.delete(item, capability_item)

    # Publishes workbook. Chunking method if file over 64MB
    @api(version="2.0")
    @parameter_added_in(as_job='3.0')
    @parameter_added_in(connections='2.8')
    def publish(
        self, workbook_item, file_path, mode,
        connection_credentials=None, connections=None, as_job=False,
        hidden_views=None
    ):

        if connection_credentials is not None:
            import warnings
            warnings.warn("connection_credentials is being deprecated. Use connections instead",
                          DeprecationWarning)

        if not os.path.isfile(file_path):
            error = "File path does not lead to an existing file."
            raise IOError(error)
        if not hasattr(self.parent_srv.PublishMode, mode):
            error = 'Invalid mode defined.'
            raise ValueError(error)

        filename = os.path.basename(file_path)
        file_extension = os.path.splitext(filename)[1][1:]

        # If name is not defined, grab the name from the file to publish
        if not workbook_item.name:
            workbook_item.name = os.path.splitext(filename)[0]
        if file_extension not in ALLOWED_FILE_EXTENSIONS:
            error = "Only {} files can be published as workbooks.".format(', '.join(ALLOWED_FILE_EXTENSIONS))
            raise ValueError(error)

        # Construct the url with the defined mode
        url = "{0}?workbookType={1}".format(self.baseurl, file_extension)
        if mode == self.parent_srv.PublishMode.Overwrite:
            url += '&{0}=true'.format(mode.lower())
        elif mode == self.parent_srv.PublishMode.Append:
            error = 'Workbooks cannot be appended.'
            raise ValueError(error)

        if as_job:
            url += '&{0}=true'.format('asJob')

        # Determine if chunking is required (64MB is the limit for single upload method)
        if os.path.getsize(file_path) >= FILESIZE_LIMIT:
            logger.info('Publishing {0} to server with chunking method (workbook over 64MB)'.format(filename))
            upload_session_id = Fileuploads.upload_chunks(self.parent_srv, file_path)
            url = "{0}&uploadSessionId={1}".format(url, upload_session_id)
            conn_creds = connection_credentials
            xml_request, content_type = RequestFactory.Workbook.publish_req_chunked(workbook_item,
                                                                                    connection_credentials=conn_creds,
                                                                                    connections=connections,
                                                                                    hidden_views=hidden_views)
        else:
            logger.info('Publishing {0} to server'.format(filename))
            with open(file_path, 'rb') as f:
                file_contents = f.read()
            conn_creds = connection_credentials
            xml_request, content_type = RequestFactory.Workbook.publish_req(workbook_item,
                                                                            filename,
                                                                            file_contents,
                                                                            connection_credentials=conn_creds,
                                                                            connections=connections,
                                                                            hidden_views=hidden_views)
        logger.debug('Request xml: {0} '.format(xml_request[:1000]))

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
            new_workbook = WorkbookItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info('Published {0} (ID: {1})'.format(filename, new_workbook.id))
            return new_workbook
