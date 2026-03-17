from email.message import Message
import copy
import io
import logging
import os
from contextlib import closing
from pathlib import Path

from tableauserverclient.helpers.headers import fix_filename
from tableauserverclient.models.permissions_item import PermissionsRule
from tableauserverclient.server.query import QuerySet

from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api, parameter_added_in
from tableauserverclient.server.endpoint.exceptions import (
    InternalServerError,
    MissingRequiredFieldError,
    UnsupportedAttributeError,
)
from tableauserverclient.server.endpoint.permissions_endpoint import _PermissionsEndpoint
from tableauserverclient.server.endpoint.resource_tagger import TaggingMixin

from tableauserverclient.filesys_helpers import (
    to_filename,
    make_download_path,
    get_file_type,
    get_file_object_size,
)
from tableauserverclient.helpers import redact_xml
from tableauserverclient.models import WorkbookItem, ConnectionItem, ViewItem, PaginationItem, JobItem, RevisionItem
from tableauserverclient.server import RequestFactory

from typing import (
    Literal,
    Optional,
    TYPE_CHECKING,
    TypeVar,
    Union,
    overload,
)
from collections.abc import Iterable, Sequence

if TYPE_CHECKING:
    from tableauserverclient.server import Server
    from tableauserverclient.server.request_options import RequestOptions, PDFRequestOptions, PPTXRequestOptions
    from tableauserverclient.models import DatasourceItem
    from tableauserverclient.server.endpoint.schedules_endpoint import AddResponse

io_types_r = (io.BytesIO, io.BufferedReader)
io_types_w = (io.BytesIO, io.BufferedWriter)

# The maximum size of a file that can be published in a single request is 64MB
FILESIZE_LIMIT = 1024 * 1024 * 64  # 64MB

ALLOWED_FILE_EXTENSIONS = ["twb", "twbx"]

from tableauserverclient.helpers.logging import logger

FilePath = Union[str, os.PathLike]
FileObject = Union[io.BufferedReader, io.BytesIO]
FileObjectR = Union[io.BufferedReader, io.BytesIO]
FileObjectW = Union[io.BufferedWriter, io.BytesIO]
PathOrFileR = Union[FilePath, FileObjectR]
PathOrFileW = Union[FilePath, FileObjectW]


class Workbooks(QuerysetEndpoint[WorkbookItem], TaggingMixin[WorkbookItem]):
    def __init__(self, parent_srv: "Server") -> None:
        super().__init__(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

        return None

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/workbooks"

    # Get all workbooks on site
    @api(version="2.0")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[WorkbookItem], PaginationItem]:
        """
        Queries the server and returns information about the workbooks the site.

        Parameters
        ----------
        req_options : RequestOptions, optional
             (Optional) You can pass the method a request object that contains
             additional parameters to filter the request. For example, if you
             were searching for a specific workbook, you could specify the name
             of the workbook or the name of the owner.

        Returns
        -------
        Tuple containing one page's worth of workbook items and pagination
        information.
        """
        logger.info("Querying all workbooks on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_workbook_items = WorkbookItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_workbook_items, pagination_item

    # Get 1 workbook
    @api(version="2.0")
    def get_by_id(self, workbook_id: str) -> WorkbookItem:
        """
        Returns information about the specified workbook on the site.

        Parameters
        ----------
        workbook_id : str
            The workbook ID.

        Returns
        -------
        WorkbookItem
            The workbook item.
        """
        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        logger.info(f"Querying single workbook (ID: {workbook_id})")
        url = f"{self.baseurl}/{workbook_id}"
        server_response = self.get_request(url)
        return WorkbookItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.8")
    def refresh(self, workbook_item: Union[WorkbookItem, str], incremental: bool = False) -> JobItem:
        """
        Refreshes the extract of an existing workbook.

        Parameters
        ----------
        workbook_item : WorkbookItem | str
            The workbook item or workbook ID.
        incremental: bool
            Whether to do a full refresh or incremental refresh of the extract data

        Returns
        -------
        JobItem
            The job item.
        """
        id_ = getattr(workbook_item, "id", workbook_item)
        url = f"{self.baseurl}/{id_}/refresh"
        refresh_req = RequestFactory.Task.refresh_req(incremental, self.parent_srv)
        server_response = self.post_request(url, refresh_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    # create one or more extracts on 1 workbook, optionally encrypted
    @api(version="3.5")
    def create_extract(
        self,
        workbook_item: WorkbookItem,
        encrypt: bool = False,
        includeAll: bool = True,
        datasources: Optional[list["DatasourceItem"]] = None,
    ) -> JobItem:
        """
        Create one or more extracts on 1 workbook, optionally encrypted.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#create_extracts_for_workbook

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to create extracts for.

        encrypt : bool, default False
            Set to True to encrypt the extracts.

        includeAll : bool, default True
            If True, all data sources in the workbook will have an extract
            created for them. If False, then a data source must be supplied in
            the request.

        datasources : list[DatasourceItem] | None
            List of DatasourceItem objects for the data sources to create
            extracts for. Only required if includeAll is False.

        Returns
        -------
        JobItem
            The job item for the extract creation.
        """
        id_ = getattr(workbook_item, "id", workbook_item)
        url = f"{self.baseurl}/{id_}/createExtract?encrypt={encrypt}"

        datasource_req = RequestFactory.Workbook.embedded_extract_req(includeAll, datasources)
        server_response = self.post_request(url, datasource_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    # delete all the extracts on 1 workbook
    @api(version="3.3")
    def delete_extract(self, workbook_item: WorkbookItem, includeAll: bool = True, datasources=None) -> JobItem:
        """
        Delete all extracts of embedded datasources on 1 workbook.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#delete_extracts_from_workbook

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to delete extracts from.

        includeAll : bool, default True
            If True, all data sources in the workbook will have their extracts
            deleted. If False, then a data source must be supplied in the
            request.

        datasources : list[DatasourceItem] | None
            List of DatasourceItem objects for the data sources to delete
            extracts from. Only required if includeAll is False.

        Returns
        -------
        JobItem
        """
        id_ = getattr(workbook_item, "id", workbook_item)
        url = f"{self.baseurl}/{id_}/deleteExtract"
        datasource_req = RequestFactory.Workbook.embedded_extract_req(includeAll, datasources)
        server_response = self.post_request(url, datasource_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    # Delete 1 workbook by id
    @api(version="2.0")
    def delete(self, workbook_id: str) -> None:
        """
        Deletes a workbook with the specified ID.

        Parameters
        ----------
        workbook_id : str
            The workbook ID.

        Returns
        -------
        None
        """
        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{workbook_id}"
        self.delete_request(url)
        logger.info(f"Deleted single workbook (ID: {workbook_id})")

    # Update workbook
    @api(version="2.0")
    @parameter_added_in(include_view_acceleration_status="3.22")
    def update(
        self,
        workbook_item: WorkbookItem,
        include_view_acceleration_status: bool = False,
    ) -> WorkbookItem:
        """
        Modifies an existing workbook. Use this method to change the owner or
        the project that the workbook belongs to, or to change whether the
        workbook shows views in tabs. The workbook item must include the
        workbook ID and overrides the existing settings.

        See https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#update_workbook
        for a list of fields that can be updated.

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to update. ID is required. Other fields are
            optional. Any fields that are not specified will not be changed.

        include_view_acceleration_status : bool, default False
            Set to True to include the view acceleration status in the response.

        Returns
        -------
        WorkbookItem
            The updated workbook item.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        self.update_tags(workbook_item)

        # Update the workbook itself
        url = f"{self.baseurl}/{workbook_item.id}"
        if include_view_acceleration_status:
            url += "?includeViewAccelerationStatus=True"

        update_req = RequestFactory.Workbook.update_req(workbook_item, self.parent_srv)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated workbook item (ID: {workbook_item.id})")
        updated_workbook = copy.copy(workbook_item)
        return updated_workbook._parse_common_tags(server_response.content, self.parent_srv.namespace)

    # Update workbook_connection
    @api(version="2.3")
    def update_connection(self, workbook_item: WorkbookItem, connection_item: ConnectionItem) -> ConnectionItem:
        """
        Updates a workbook connection information (server addres, server port,
        user name, and password).

        The workbook connections must be populated before the strings can be
        updated.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#update_workbook_connection

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to update.

        connection_item : ConnectionItem
            The connection item to update.

        Returns
        -------
        ConnectionItem
            The updated connection item.
        """
        url = f"{self.baseurl}/{workbook_item.id}/connections/{connection_item.id}"
        update_req = RequestFactory.Connection.update_req(connection_item)
        server_response = self.put_request(url, update_req)
        connection = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info(f"Updated workbook item (ID: {workbook_item.id} & connection item {connection_item.id})")
        return connection

    # Update workbook_connections
    @api(version="3.26")
    def update_connections(
        self,
        workbook_item: WorkbookItem,
        connection_luids: Iterable[str],
        authentication_type: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        embed_password: Optional[bool] = None,
    ) -> list[ConnectionItem]:
        """
        Bulk updates one or more workbook connections by LUID, including authenticationType, username, password, and embedPassword.

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item containing the connections.

        connection_luids : Iterable of str
            The connection LUIDs to update.

        authentication_type : str
            The authentication type to use (e.g., 'AD Service Principal').

        username : str, optional
            The username to set (e.g., client ID for keypair auth).

        password : str, optional
            The password or secret to set.

        embed_password : bool, optional
            Whether to embed the password.

        Returns
        -------
        Iterable of str
            The connection LUIDs that were updated.
        """

        url = f"{self.baseurl}/{workbook_item.id}/connections"

        request_body = RequestFactory.Workbook.update_connections_req(
            connection_luids,
            authentication_type,
            username=username,
            password=password,
            embed_password=embed_password,
        )

        # Send request
        server_response = self.put_request(url, request_body)
        connection_items = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        updated_ids: list[str] = [conn.id for conn in connection_items]

        logger.info(f"Updated connections for workbook {workbook_item.id}: {', '.join(updated_ids)}")
        return connection_items

    T = TypeVar("T", bound=FileObjectW)

    @overload
    def download(
        self,
        workbook_id: str,
        filepath: T,
        include_extract: bool = True,
    ) -> T: ...

    @overload
    def download(
        self,
        workbook_id: str,
        filepath: Optional[FilePath] = None,
        include_extract: bool = True,
    ) -> str: ...

    # Download workbook contents with option of passing in filepath
    @api(version="2.0")
    @parameter_added_in(no_extract="2.5")
    @parameter_added_in(include_extract="2.5")
    def download(
        self,
        workbook_id,
        filepath=None,
        include_extract=True,
    ):
        """
        Downloads a workbook to the specified directory (optional).

        Parameters
        ----------
        workbook_id : str
            The workbook ID.

        filepath : Path or File object, optional
            Downloads the file to the location you specify. If no location is
            specified, the file is downloaded to the current working directory.
            The default is Filepath=None.

        include_extract : bool, default True
            Set to False to exclude the extract from the download. The default
            is True.

        Returns
        -------
        Path or File object
            The path to the downloaded workbook or the file object.

        Raises
        ------
        ValueError
            If the workbook ID is not defined.
        """

        return self.download_revision(
            workbook_id,
            None,
            filepath,
            include_extract,
        )

    # Get all views of workbook
    @api(version="2.0")
    def populate_views(self, workbook_item: WorkbookItem, usage: bool = False) -> None:
        """
        Populates (or gets) a list of views for a workbook.

        You must first call this method to populate views before you can iterate
        through the views.

        This method retrieves the view information for the specified workbook.
        The REST API is designed to return only the information you ask for
        explicitly. When you query for all the workbooks, the view information
        is not included. Use this method to retrieve the views. The method adds
        the list of views to the workbook item (workbook_item.views). This is a
        list of ViewItem.

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to populate views for.

        usage : bool, default False
            Set to True to include usage statistics for each view.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the workbook item is missing an ID.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def view_fetcher() -> list[ViewItem]:
            return self._get_views_for_workbook(workbook_item, usage)

        workbook_item._set_views(view_fetcher)
        logger.info(f"Populated views for workbook (ID: {workbook_item.id})")

    def _get_views_for_workbook(self, workbook_item: WorkbookItem, usage: bool) -> list[ViewItem]:
        url = f"{self.baseurl}/{workbook_item.id}/views"
        if usage:
            url += "?includeUsageStatistics=true"
        server_response = self.get_request(url)
        views = ViewItem.from_response(
            server_response.content,
            self.parent_srv.namespace,
            workbook_id=workbook_item.id,
        )
        return views

    # Get all connections of workbook
    @api(version="2.0")
    def populate_connections(self, workbook_item: WorkbookItem) -> None:
        """
        Populates a list of data source connections for the specified workbook.

        You must populate connections before you can iterate through the
        connections.

        This method retrieves the data source connection information for the
        specified workbook. The REST API is designed to return only the
        information you ask for explicitly. When you query all the workbooks,
        the data source connection information is not included. Use this method
        to retrieve the connection information for any data sources used by the
        workbook. The method adds the list of data connections to the workbook
        item (workbook_item.connections). This is a list of ConnectionItem.

        REST API docs: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_workbook_connections

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to populate connections for.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the workbook item is missing an ID.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def connection_fetcher():
            return self._get_workbook_connections(workbook_item)

        workbook_item._set_connections(connection_fetcher)
        logger.info(f"Populated connections for workbook (ID: {workbook_item.id})")

    def _get_workbook_connections(
        self, workbook_item: WorkbookItem, req_options: Optional["RequestOptions"] = None
    ) -> list[ConnectionItem]:
        url = f"{self.baseurl}/{workbook_item.id}/connections"
        server_response = self.get_request(url, req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return connections

    @api(version="3.4")
    def populate_pdf(self, workbook_item: WorkbookItem, req_options: Optional["PDFRequestOptions"] = None) -> None:
        """
        Populates the PDF for the specified workbook item. Get the pdf of the
        entire workbook if its tabs are enabled, pdf of the default view if its
        tabs are disabled.

        This method populates a PDF with image(s) of the workbook view(s) you
        specify.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#download_workbook_pdf

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to populate the PDF for.

        req_options : PDFRequestOptions, optional
            (Optional) You can pass in request options to specify the page type
            and orientation of the PDF content, as well as the maximum age of
            the PDF rendered on the server. See PDFRequestOptions class for more
            details.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the workbook item is missing an ID.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID."
            raise MissingRequiredFieldError(error)

        def pdf_fetcher() -> bytes:
            return self._get_wb_pdf(workbook_item, req_options)

        if not self.parent_srv.check_at_least_version("3.23") and req_options is not None:
            if req_options.view_filters or req_options.view_parameters:
                raise UnsupportedAttributeError("view_filters and view_parameters are only supported in 3.23+")

            if req_options.viz_height or req_options.viz_width:
                raise UnsupportedAttributeError("viz_height and viz_width are only supported in 3.23+")

        workbook_item._set_pdf(pdf_fetcher)
        logger.info(f"Populated pdf for workbook (ID: {workbook_item.id})")

    def _get_wb_pdf(self, workbook_item: WorkbookItem, req_options: Optional["PDFRequestOptions"]) -> bytes:
        url = f"{self.baseurl}/{workbook_item.id}/pdf"
        server_response = self.get_request(url, req_options)
        pdf = server_response.content
        return pdf

    @api(version="3.8")
    def populate_powerpoint(
        self, workbook_item: WorkbookItem, req_options: Optional["PPTXRequestOptions"] = None
    ) -> None:
        """
        Populates the PowerPoint for the specified workbook item.

        This method populates a PowerPoint with image(s) of the workbook view(s) you
        specify.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#download_workbook_powerpoint

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to populate the PDF for.

        req_options : RequestOptions, optional
            (Optional) You can pass in request options to specify the maximum
            number of minutes a workbook .pptx will be cached before being
            refreshed. To prevent multiple .pptx requests from overloading the
            server, the shortest interval you can set is one minute. There is no
            maximum value, but the server job enacting the caching action may
            expire before a long cache period is reached.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the workbook item is missing an ID.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID."
            raise MissingRequiredFieldError(error)

        def pptx_fetcher() -> bytes:
            return self._get_wb_pptx(workbook_item, req_options)

        workbook_item._set_powerpoint(pptx_fetcher)
        logger.info(f"Populated powerpoint for workbook (ID: {workbook_item.id})")

    def _get_wb_pptx(self, workbook_item: WorkbookItem, req_options: Optional["PPTXRequestOptions"]) -> bytes:
        url = f"{self.baseurl}/{workbook_item.id}/powerpoint"
        server_response = self.get_request(url, req_options)
        pptx = server_response.content
        return pptx

    # Get preview image of workbook
    @api(version="2.0")
    def populate_preview_image(self, workbook_item: WorkbookItem) -> None:
        """
        This method gets the preview image (thumbnail) for the specified workbook item.

        This method uses the workbook's ID to get the preview image. The method
        adds the preview image to the workbook item (workbook_item.preview_image).

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to populate the preview image for.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the workbook item is missing an ID.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def image_fetcher() -> bytes:
            return self._get_wb_preview_image(workbook_item)

        workbook_item._set_preview_image(image_fetcher)
        logger.info(f"Populated preview image for workbook (ID: {workbook_item.id})")

    def _get_wb_preview_image(self, workbook_item: WorkbookItem) -> bytes:
        url = f"{self.baseurl}/{workbook_item.id}/previewImage"
        server_response = self.get_request(url)
        preview_image = server_response.content
        return preview_image

    @api(version="2.0")
    def populate_permissions(self, item: WorkbookItem) -> None:
        """
        Populates the permissions for the specified workbook item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_workbook_permissions

        Parameters
        ----------
        item : WorkbookItem
            The workbook item to populate permissions for.

        Returns
        -------
        None
        """
        self._permissions.populate(item)

    @api(version="2.0")
    def update_permissions(self, resource: WorkbookItem, rules: list[PermissionsRule]) -> list[PermissionsRule]:
        """
        Updates the permissions for the specified workbook item. The method
        replaces the existing permissions with the new permissions. Any missing
        permissions are removed.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_permissions_for_content

        Parameters
        ----------
        resource : WorkbookItem
            The workbook item to update permissions for.

        rules : list[PermissionsRule]
            A list of permissions rules to apply to the workbook item.

        Returns
        -------
        list[PermissionsRule]
            The updated permissions rules.
        """
        return self._permissions.update(resource, rules)

    @api(version="2.0")
    def delete_permission(self, item: WorkbookItem, capability_item: PermissionsRule) -> None:
        """
        Deletes a single permission rule from the specified workbook item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_workbook_permission

        Parameters
        ----------
        item : WorkbookItem
            The workbook item to delete the permission from.

        capability_item : PermissionsRule
            The permission rule to delete.

        Returns
        -------
        None
        """
        return self._permissions.delete(item, capability_item)

    @overload
    def publish(
        self,
        workbook_item: WorkbookItem,
        file: PathOrFileR,
        mode: str,
        connections: Optional[Sequence[ConnectionItem]],
        as_job: Literal[False],
        skip_connection_check: bool,
        parameters=None,
    ) -> WorkbookItem: ...

    @overload
    def publish(
        self,
        workbook_item: WorkbookItem,
        file: PathOrFileR,
        mode: str,
        connections: Optional[Sequence[ConnectionItem]],
        as_job: Literal[True],
        skip_connection_check: bool,
        parameters=None,
    ) -> JobItem: ...

    @api(version="2.0")
    @parameter_added_in(as_job="3.0")
    @parameter_added_in(connections="2.8")
    def publish(
        self,
        workbook_item: WorkbookItem,
        file: PathOrFileR,
        mode: str,
        connections: Optional[Sequence[ConnectionItem]] = None,
        as_job: bool = False,
        skip_connection_check: bool = False,
        parameters=None,
    ):
        """
        Publish a workbook to the specified site.

        Note: The REST API cannot automatically include extracts or other
        resources that the workbook uses. Therefore, a .twb file that uses data
        from an Excel or csv file on a local computer cannot be published,
        unless you package the data and workbook in a .twbx file, or publish the
        data source separately.

        For workbooks that are larger than 64 MB, the publish method
        automatically takes care of chunking the file in parts for uploading.
        Using this method is considerably more convenient than calling the
        publish REST APIs directly.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#publish_workbook

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook_item specifies the workbook you are publishing. When
            you are adding a workbook, you need to first create a new instance
            of a workbook_item that includes a project_id of an existing
            project. The name of the workbook will be the name of the file,
            unless you also specify a name for the new workbook when you create
            the instance.

        file : Path or File object
            The file path or file object of the workbook to publish. When
            providing a file object, you must also specifiy the name of the
            workbook in your instance of the workbook_itemworkbook_item , as
            the name cannot be derived from the file name.

        mode : str
            Specifies whether you are publishing a new workbook (CreateNew) or
            overwriting an existing workbook (Overwrite). You cannot appending
            workbooks. You can also use the publish mode attributes, for
            example: TSC.Server.PublishMode.Overwrite.

        connections : list[ConnectionItem] | None
            List of ConnectionItems objects for the connections created within
            the workbook.

        as_job : bool, default False
            Set to True to run the upload as a job (asynchronous upload). If set
            to True a job will start to perform the publishing process and a Job
            object is returned. Defaults to False.

        skip_connection_check : bool, default False
            Set to True to skip connection check at time of upload. Publishing
            will succeed but unchecked connection issues may result in a
            non-functioning workbook. Defaults to False.

        Raises
        ------
        OSError
            If the file path does not lead to an existing file.

        ServerResponseError
            If the server response is not successful.

        TypeError
            If the file is not a file path or file object.

        ValueError
            If the file extension is not supported

        ValueError
            If the mode is invalid.

        ValueError
            Workbooks cannot be appended.

        Returns
        -------
        WorkbookItem | JobItem
            The workbook item or job item that was published.
        """
        if isinstance(file, (str, os.PathLike)):
            if not os.path.isfile(file):
                error = "File path does not lead to an existing file."
                raise OSError(error)

            filename = os.path.basename(file)
            file_extension = os.path.splitext(filename)[1][1:]
            file_size = os.path.getsize(file)

            # If name is not defined, grab the name from the file to publish
            if not workbook_item.name:
                workbook_item.name = os.path.splitext(filename)[0]
            if file_extension not in ALLOWED_FILE_EXTENSIONS:
                error = "Only {} files can be published as workbooks.".format(", ".join(ALLOWED_FILE_EXTENSIONS))
                raise ValueError(error)

        elif isinstance(file, io_types_r):
            if not workbook_item.name:
                error = "Workbook item must have a name when passing a file object"
                raise ValueError(error)

            file_type = get_file_type(file)
            if file_type == "zip":
                file_extension = "twbx"
            elif file_type == "xml":
                file_extension = "twb"
            else:
                error = f"Unsupported file type {file_type}!"
                raise ValueError(error)

            # Generate filename for file object.
            # This is needed when publishing the workbook in a single request
            filename = f"{workbook_item.name}.{file_extension}"
            file_size = get_file_object_size(file)

        else:
            raise TypeError("file should be a filepath or file object.")

        if not hasattr(self.parent_srv.PublishMode, mode):
            error = "Invalid mode defined."
            raise ValueError(error)

        # Construct the url with the defined mode
        url = f"{self.baseurl}?workbookType={file_extension}"
        if mode == self.parent_srv.PublishMode.Overwrite:
            url += f"&{mode.lower()}=true"
        elif mode == self.parent_srv.PublishMode.Append:
            error = "Workbooks cannot be appended."
            raise ValueError(error)

        if as_job:
            url += "&{}=true".format("asJob")

        if skip_connection_check:
            url += "&{}=true".format("skipConnectionCheck")

        # Determine if chunking is required (64MB is the limit for single upload method)
        if file_size >= FILESIZE_LIMIT:
            logger.info(f"Publishing {workbook_item.name} to server with chunking method (workbook over 64MB)")
            upload_session_id = self.parent_srv.fileuploads.upload(file)
            url = f"{url}&uploadSessionId={upload_session_id}"
            xml_request, content_type = RequestFactory.Workbook.publish_req_chunked(
                workbook_item,
                connections=connections,
            )
        else:
            logger.info(f"Publishing {filename} to server")

            if isinstance(file, (str, Path)):
                with open(file, "rb") as f:
                    file_contents = f.read()

            elif isinstance(file, io_types_r):
                file_contents = file.read()

            else:
                raise TypeError("file should be a filepath or file object.")

            xml_request, content_type = RequestFactory.Workbook.publish_req(
                workbook_item,
                filename,
                file_contents,
                connections=connections,
            )
        logger.debug(f"Request xml: {redact_xml(xml_request[:1000])} ")

        # Send the publishing request to server
        try:
            server_response = self.post_request(url, xml_request, content_type, parameters)
        except InternalServerError as err:
            if err.code == 504 and not as_job:
                err.content = "Timeout error while publishing. Please use asynchronous publishing to avoid timeouts."
            raise err

        if as_job:
            new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info(f"Published {workbook_item.name} (JOB_ID: {new_job.id}")
            return new_job
        else:
            new_workbook = WorkbookItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info(f"Published {workbook_item.name} (ID: {new_workbook.id})")
            return new_workbook

    # Populate workbook item's revisions
    @api(version="2.3")
    def populate_revisions(self, workbook_item: WorkbookItem) -> None:
        """
        Populates (or gets) a list of revisions for a workbook.

        You must first call this method to populate revisions before you can
        iterate through the revisions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#get_workbook_revisions

        Parameters
        ----------
        workbook_item : WorkbookItem
            The workbook item to populate revisions for.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the workbook item is missing an ID.
        """
        if not workbook_item.id:
            error = "Workbook item missing ID. Workbook must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def revisions_fetcher():
            return self._get_workbook_revisions(workbook_item)

        workbook_item._set_revisions(revisions_fetcher)
        logger.info(f"Populated revisions for workbook (ID: {workbook_item.id})")

    def _get_workbook_revisions(
        self, workbook_item: WorkbookItem, req_options: Optional["RequestOptions"] = None
    ) -> list[RevisionItem]:
        url = f"{self.baseurl}/{workbook_item.id}/revisions"
        server_response = self.get_request(url, req_options)
        revisions = RevisionItem.from_response(server_response.content, self.parent_srv.namespace, workbook_item)
        return revisions

    T = TypeVar("T", bound=FileObjectW)

    @overload
    def download_revision(
        self, workbook_id: str, revision_number: Optional[str], filepath: T, include_extract: bool
    ) -> T: ...

    @overload
    def download_revision(
        self, workbook_id: str, revision_number: Optional[str], filepath: Optional[FilePath], include_extract: bool
    ) -> str: ...

    # Download 1 workbook revision by revision number
    @api(version="2.3")
    def download_revision(
        self,
        workbook_id,
        revision_number,
        filepath,
        include_extract=True,
    ):
        """
        Downloads a workbook revision to the specified directory (optional).

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#download_workbook_revision

        Parameters
        ----------
        workbook_id : str
            The workbook ID.

        revision_number : str | None
            The revision number of the workbook. If None, the latest revision is
            downloaded.

        filepath : Path or File object, optional
            Downloads the file to the location you specify. If no location is
            specified, the file is downloaded to the current working directory.
            The default is Filepath=None.

        include_extract : bool, default True
            Set to False to exclude the extract from the download. The default
            is True.

        Returns
        -------
        Path or File object
            The path to the downloaded workbook or the file object.

        Raises
        ------
        ValueError
            If the workbook ID is not defined.
        """

        if not workbook_id:
            error = "Workbook ID undefined."
            raise ValueError(error)
        if revision_number is None:
            url = f"{self.baseurl}/{workbook_id}/content"
        else:
            url = f"{self.baseurl}/{workbook_id}/revisions/{revision_number}/content"

        if not include_extract:
            url += "?includeExtract=False"

        with closing(self.get_request(url, parameters={"stream": True})) as server_response:
            m = Message()
            m["Content-Disposition"] = server_response.headers["Content-Disposition"]
            params = m.get_filename(failobj="")
            if isinstance(filepath, io_types_w):
                for chunk in server_response.iter_content(1024):  # 1KB
                    filepath.write(chunk)
                return_path = filepath
            else:
                params = fix_filename(params)
                filename = to_filename(os.path.basename(params))
                download_path = make_download_path(filepath, filename)
                with open(download_path, "wb") as f:
                    for chunk in server_response.iter_content(1024):  # 1KB
                        f.write(chunk)
                return_path = os.path.abspath(download_path)

        logger.info(f"Downloaded workbook revision {revision_number} to {return_path} (ID: {workbook_id})")
        return return_path

    @api(version="2.3")
    def delete_revision(self, workbook_id: str, revision_number: str) -> None:
        """
        Deletes a specific revision from a workbook on Tableau Server.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_revisions.htm#remove_workbook_revision

        Parameters
        ----------
        workbook_id : str
            The workbook ID.

        revision_number : str
            The revision number of the workbook to delete.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the workbook ID or revision number is not defined.
        """
        if workbook_id is None or revision_number is None:
            raise ValueError
        url = "/".join([self.baseurl, workbook_id, "revisions", revision_number])

        self.delete_request(url)
        logger.info(f"Deleted single workbook revision (ID: {workbook_id}) (Revision: {revision_number})")

    # a convenience method
    @api(version="2.8")
    def schedule_extract_refresh(
        self, schedule_id: str, item: WorkbookItem
    ) -> list["AddResponse"]:  # actually should return a task
        """
        Adds a workbook to a schedule for extract refresh.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#add_workbook_to_schedule

        Parameters
        ----------
        schedule_id : str
            The schedule ID.

        item : WorkbookItem
            The workbook item to add to the schedule.

        Returns
        -------
        list[AddResponse]
            The response from the server.
        """
        return self.parent_srv.schedules.add_to_schedule(schedule_id, workbook=item)

    @api(version="1.0")
    def add_tags(self, item: Union[WorkbookItem, str], tags: Union[Iterable[str], str]) -> set[str]:
        """
        Adds tags to a workbook. One or more tags may be added at a time. If a
        tag already exists on the workbook, it will not be duplicated.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#add_tags_to_workbook

        Parameters
        ----------
        item : WorkbookItem | str
            The workbook item or workbook ID to add tags to.

        tags : Iterable[str] | str
            The tag or tags to add to the workbook. Tags can be a single tag or
            a list of tags.

        Returns
        -------
        set[str]
            The set of tags added to the workbook.
        """
        return super().add_tags(item, tags)

    @api(version="1.0")
    def delete_tags(self, item: Union[WorkbookItem, str], tags: Union[Iterable[str], str]) -> None:
        """
        Deletes tags from a workbook. One or more tags may be deleted at a time.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#delete_tag_from_workbook

        Parameters
        ----------
        item : WorkbookItem | str
            The workbook item or workbook ID to delete tags from.

        tags : Iterable[str] | str
            The tag or tags to delete from the workbook. Tags can be a single
            tag or a list of tags.

        Returns
        -------
        None
        """
        return super().delete_tags(item, tags)

    @api(version="1.0")
    def update_tags(self, item: WorkbookItem) -> None:
        """
        Updates the tags on a workbook. This method is used to update the tags
        on the server to match the tags on the workbook item. This method is a
        convenience method that calls add_tags and delete_tags to update the
        tags on the server.

        Parameters
        ----------
        item : WorkbookItem
            The workbook item to update the tags for. The tags on the workbook
            item will be used to update the tags on the server.

        Returns
        -------
        None
        """
        return super().update_tags(item)

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> QuerySet[WorkbookItem]:
        """
        Queries the Tableau Server for items using the specified filters. Page
        size can be specified to limit the number of items returned in a single
        request. If not specified, the default page size is 100. Page size can
        be an integer between 1 and 1000.

        No positional arguments are allowed. All filters must be specified as
        keyword arguments. If you use the equality operator, you can specify it
        through <field_name>=<value>. If you want to use a different operator,
        you can specify it through <field_name>__<operator>=<value>. Field
        names can either be in snake_case or camelCase.

        This endpoint supports the following fields and operators:


        created_at=...
        created_at__gt=...
        created_at__gte=...
        created_at__lt=...
        created_at__lte=...
        content_url=...
        content_url__in=...
        display_tabs=...
        favorites_total=...
        favorites_total__gt=...
        favorites_total__gte=...
        favorites_total__lt=...
        favorites_total__lte=...
        has_alerts=...
        has_extracts=...
        name=...
        name__in=...
        owner_domain=...
        owner_domain__in=...
        owner_email=...
        owner_email__in=...
        owner_name=...
        owner_name__in=...
        project_name=...
        project_name__in=...
        sheet_count=...
        sheet_count__gt=...
        sheet_count__gte=...
        sheet_count__lt=...
        sheet_count__lte=...
        size=...
        size__gt=...
        size__gte=...
        size__lt=...
        size__lte=...
        subscriptions_total=...
        subscriptions_total__gt=...
        subscriptions_total__gte=...
        subscriptions_total__lt=...
        subscriptions_total__lte=...
        tags=...
        tags__in=...
        updated_at=...
        updated_at__gt=...
        updated_at__gte=...
        updated_at__lt=...
        updated_at__lte=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
