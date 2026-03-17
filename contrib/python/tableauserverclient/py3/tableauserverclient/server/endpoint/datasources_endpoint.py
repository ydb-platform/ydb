from email.message import Message
import copy
import json
import io
import os

from contextlib import closing
from pathlib import Path
from typing import Literal, Optional, TYPE_CHECKING, TypedDict, TypeVar, Union, overload
from collections.abc import Iterable, Sequence

from tableauserverclient.helpers.headers import fix_filename
from tableauserverclient.models.dqw_item import DQWItem
from tableauserverclient.server.query import QuerySet

if TYPE_CHECKING:
    from tableauserverclient.server import Server
    from tableauserverclient.models import PermissionsRule
    from .schedules_endpoint import AddResponse

from tableauserverclient.server.endpoint.dqw_endpoint import _DataQualityWarningEndpoint
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api, parameter_added_in
from tableauserverclient.server.endpoint.exceptions import InternalServerError, MissingRequiredFieldError
from tableauserverclient.server.endpoint.permissions_endpoint import _PermissionsEndpoint
from tableauserverclient.server.endpoint.resource_tagger import TaggingMixin

from tableauserverclient.config import ALLOWED_FILE_EXTENSIONS, BYTES_PER_MB, config
from tableauserverclient.filesys_helpers import (
    make_download_path,
    get_file_type,
    get_file_object_size,
    to_filename,
)
from tableauserverclient.helpers.logging import logger
from tableauserverclient.models import (
    ConnectionCredentials,
    ConnectionItem,
    DatasourceItem,
    JobItem,
    RevisionItem,
    PaginationItem,
)
from tableauserverclient.server import RequestFactory, RequestOptions

io_types = (io.BytesIO, io.BufferedReader)
io_types_r = (io.BytesIO, io.BufferedReader)
io_types_w = (io.BytesIO, io.BufferedWriter)

FilePath = Union[str, os.PathLike]
FileObject = Union[io.BufferedReader, io.BytesIO]
PathOrFile = Union[FilePath, FileObject]

FileObjectR = Union[io.BufferedReader, io.BytesIO]
FileObjectW = Union[io.BufferedWriter, io.BytesIO]
PathOrFileR = Union[FilePath, FileObjectR]
PathOrFileW = Union[FilePath, FileObjectW]


HyperActionCondition = TypedDict(
    "HyperActionCondition",
    {
        "op": str,
        "target-col": str,
        "source-col": str,
    },
)

HyperActionRow = TypedDict(
    "HyperActionRow",
    {
        "action": Literal[
            "update",
            "upsert",
            "delete",
        ],
        "source-table": str,
        "target-table": str,
        "condition": HyperActionCondition,
    },
)

HyperActionTable = TypedDict(
    "HyperActionTable",
    {
        "action": Literal[
            "insert",
            "replace",
        ],
        "source-table": str,
        "target-table": str,
    },
)

HyperAction = Union[HyperActionTable, HyperActionRow]


class Datasources(QuerysetEndpoint[DatasourceItem], TaggingMixin[DatasourceItem]):
    def __init__(self, parent_srv: "Server") -> None:
        super().__init__(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)
        self._data_quality_warnings = _DataQualityWarningEndpoint(self.parent_srv, "datasource")

        return None

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/datasources"

    # Get all datasources
    @api(version="2.0")
    def get(self, req_options: Optional[RequestOptions] = None) -> tuple[list[DatasourceItem], PaginationItem]:
        """
        Returns a list of published data sources on the specified site, with
        optional parameters for specifying the paging of large results. To get
        a list of data sources embedded in a workbook, use the Query Workbook
        Connections method.

        Endpoint is paginated, and will return one page per call.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#query_data_sources

        Parameters
        ----------
        req_options : Optional[RequestOptions]
            Optional parameters for the request, such as filters, sorting, page
            size, and page number.

        Returns
        -------
        tuple[list[DatasourceItem], PaginationItem]
            A tuple containing the list of datasource items and pagination
            information.
        """
        logger.info("Querying all datasources on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_datasource_items = DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_datasource_items, pagination_item

    # Get 1 datasource by id
    @api(version="2.0")
    def get_by_id(self, datasource_id: str) -> DatasourceItem:
        """
        Returns information about a specific data source.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#query_data_source

        Parameters
        ----------
        datasource_id : str
            The unique ID of the datasource to retrieve.

        Returns
        -------
        DatasourceItem
            An object containing information about the datasource.
        """
        if not datasource_id:
            error = "Datasource ID undefined."
            raise ValueError(error)
        logger.info(f"Querying single datasource (ID: {datasource_id})")
        url = f"{self.baseurl}/{datasource_id}"
        server_response = self.get_request(url)
        return DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Populate datasource item's connections
    @api(version="2.0")
    def populate_connections(self, datasource_item: DatasourceItem) -> None:
        """
        Retrieve connection information for the specificed datasource item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#query_data_source_connections

        Parameters
        ----------
        datasource_item : DatasourceItem
            The datasource item to retrieve connections for.

        Returns
        -------
        None
        """
        if not datasource_item.id:
            error = "Datasource item missing ID. Datasource must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def connections_fetcher():
            return self._get_datasource_connections(datasource_item)

        datasource_item._set_connections(connections_fetcher)
        logger.info(f"Populated connections for datasource (ID: {datasource_item.id})")

    def _get_datasource_connections(
        self, datasource_item: DatasourceItem, req_options: Optional[RequestOptions] = None
    ) -> list[ConnectionItem]:
        url = f"{self.baseurl}/{datasource_item.id}/connections"
        server_response = self.get_request(url, req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        for connection in connections:
            connection._datasource_id = datasource_item.id
            connection._datasource_name = datasource_item.name
        return connections

    # Delete 1 datasource by id
    @api(version="2.0")
    def delete(self, datasource_id: str) -> None:
        """
        Deletes the specified data source from a site. When a data source is
        deleted, its associated data connection is also deleted. Workbooks that
        use the data source are not deleted, but they will no longer work
        properly.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#delete_data_source

        Parameters
        ----------
        datasource_id : str

        Returns
        -------
        None
        """
        if not datasource_id:
            error = "Datasource ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{datasource_id}"
        self.delete_request(url)
        logger.info(f"Deleted single datasource (ID: {datasource_id})")

    T = TypeVar("T", bound=FileObjectW)

    @overload
    def download(
        self,
        datasource_id: str,
        filepath: T,
        include_extract: bool = True,
    ) -> T: ...

    @overload
    def download(
        self,
        datasource_id: str,
        filepath: Optional[FilePath] = None,
        include_extract: bool = True,
    ) -> str: ...

    # Download 1 datasource by id
    @api(version="2.0")
    @parameter_added_in(no_extract="2.5")
    @parameter_added_in(include_extract="2.5")
    def download(
        self,
        datasource_id,
        filepath=None,
        include_extract=True,
    ):
        """
        Downloads the specified data source from a site. The data source is
        downloaded as a .tdsx file.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#download_data_source

        Parameters
        ----------
        datasource_id : str
            The unique ID of the datasource to download.

        filepath : Optional[PathOrFileW]
            The file path to save the downloaded datasource to. If not
            specified, the file will be saved to the current working directory.

        include_extract : bool, default True
            If True, the extract is included in the download. If False, the
            extract is not included.

        Returns
        -------
        filepath : PathOrFileW
        """
        return self.download_revision(
            datasource_id,
            None,
            filepath,
            include_extract,
        )

    # Update datasource
    @api(version="2.0")
    def update(self, datasource_item: DatasourceItem) -> DatasourceItem:
        """
        Updates the owner, project or certification status of the specified
        data source.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#update_data_source

        Parameters
        ----------
        datasource_item : DatasourceItem
            The datasource item to update.

        Returns
        -------
        DatasourceItem
            An object containing information about the updated datasource.

        Raises
        ------
        MissingRequiredFieldError
            If the datasource item is missing an ID.
        """

        if not datasource_item.id:
            error = "Datasource item missing ID. Datasource must be retrieved from server first."
            raise MissingRequiredFieldError(error)
        # bug - before v3.15 you must always include the project id
        if datasource_item.owner_id and not datasource_item.project_id:
            if not self.parent_srv.check_at_least_version("3.15"):
                error = (
                    "Attempting to set new owner but datasource is missing Project ID."
                    "In versions before 3.15 the project id must be included to update the owner."
                )
                raise MissingRequiredFieldError(error)

        self.update_tags(datasource_item)

        # Update the datasource itself
        url = f"{self.baseurl}/{datasource_item.id}"

        update_req = RequestFactory.Datasource.update_req(datasource_item)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated datasource item (ID: {datasource_item.id})")
        updated_datasource = copy.copy(datasource_item)
        return updated_datasource._parse_common_elements(server_response.content, self.parent_srv.namespace)

    # Update datasource connections
    @api(version="2.3")
    def update_connection(
        self, datasource_item: DatasourceItem, connection_item: ConnectionItem
    ) -> Optional[ConnectionItem]:
        """
        Updates the server address, port, username, or password for the
        specified data source connection.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#update_data_source_connection

        Parameters
        ----------
        datasource_item : DatasourceItem
            The datasource item to update.

        connection_item : ConnectionItem
            The connection item to update.

        Returns
        -------
        Optional[ConnectionItem]
            An object containing information about the updated connection.
        """

        url = f"{self.baseurl}/{datasource_item.id}/connections/{connection_item.id}"

        update_req = RequestFactory.Connection.update_req(connection_item)
        server_response = self.put_request(url, update_req)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        if not connections:
            return None

        if len(connections) > 1:
            logger.debug(f"Multiple connections returned ({len(connections)})")
        connection = list(filter(lambda x: x.id == connection_item.id, connections))[0]

        logger.info(f"Updated datasource item (ID: {datasource_item.id} & connection item {connection_item.id}")
        return connection

    @api(version="3.26")
    def update_connections(
        self,
        datasource_item: DatasourceItem,
        connection_luids: Iterable[str],
        authentication_type: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        embed_password: Optional[bool] = None,
    ) -> list[ConnectionItem]:
        """
        Bulk updates one or more datasource connections by LUID.

        Parameters
        ----------
        datasource_item : DatasourceItem
            The datasource item containing the connections.

        connection_luids : Iterable of str
            The connection LUIDs to update.

        authentication_type : str
            The authentication type to use (e.g., 'auth-keypair').

        username : str, optional
            The username to set.

        password : str, optional
            The password or secret to set.

        embed_password : bool, optional
            Whether to embed the password.

        Returns
        -------
        Iterable of str
            The connection LUIDs that were updated.
        """

        url = f"{self.baseurl}/{datasource_item.id}/connections"

        request_body = RequestFactory.Datasource.update_connections_req(
            connection_luids=connection_luids,
            authentication_type=authentication_type,
            username=username,
            password=password,
            embed_password=embed_password,
        )
        server_response = self.put_request(url, request_body)
        connection_items = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        updated_ids: list[str] = [conn.id for conn in connection_items]

        logger.info(f"Updated connections for datasource {datasource_item.id}: {', '.join(updated_ids)}")
        return connection_items

    @api(version="2.8")
    def refresh(self, datasource_item: Union[DatasourceItem, str], incremental: bool = False) -> JobItem:
        """
        Refreshes the extract of an existing workbook.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#run_extract_refresh_task

        Parameters
        ----------
        workbook_item : DatasourceItem | str
            The datasource item or datasource ID.
        incremental: bool
            Whether to do a full refresh or incremental refresh of the extract data

        Returns
        -------
        JobItem
            The job item.
        """
        id_ = getattr(datasource_item, "id", datasource_item)
        url = f"{self.baseurl}/{id_}/refresh"
        refresh_req = RequestFactory.Task.refresh_req(incremental, self.parent_srv)
        server_response = self.post_request(url, refresh_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    @api(version="3.5")
    def create_extract(self, datasource_item: DatasourceItem, encrypt: bool = False) -> JobItem:
        """
        Create an extract for a data source in a site. Optionally, encrypt the
        extract if the site and workbooks using it are configured to allow it.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_extract_and_encryption.htm#create_extract_for_datasource

        Parameters
        ----------
        datasource_item : DatasourceItem | str
            The datasource item or datasource ID.

        encrypt : bool, default False
            Whether to encrypt the extract.

        Returns
        -------
        JobItem
            The job item.
        """
        id_ = getattr(datasource_item, "id", datasource_item)
        url = f"{self.baseurl}/{id_}/createExtract?encrypt={encrypt}"
        empty_req = RequestFactory.Empty.empty_req()
        server_response = self.post_request(url, empty_req)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    @api(version="3.5")
    def delete_extract(self, datasource_item: DatasourceItem) -> None:
        """
        Delete the extract of a data source in a site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_extract_and_encryption.htm#delete_extract_from_datasource

        Parameters
        ----------
        datasource_item : DatasourceItem | str
            The datasource item or datasource ID.

        Returns
        -------
        None
        """
        id_ = getattr(datasource_item, "id", datasource_item)
        url = f"{self.baseurl}/{id_}/deleteExtract"
        empty_req = RequestFactory.Empty.empty_req()
        self.post_request(url, empty_req)

    @overload
    def publish(
        self,
        datasource_item: DatasourceItem,
        file: PathOrFileR,
        mode: str,
        connection_credentials: Optional[ConnectionCredentials] = None,
        connections: Optional[Sequence[ConnectionItem]] = None,
        as_job: Literal[False] = False,
    ) -> DatasourceItem:
        pass

    @overload
    def publish(
        self,
        datasource_item: DatasourceItem,
        file: PathOrFileR,
        mode: str,
        connection_credentials: Optional[ConnectionCredentials] = None,
        connections: Optional[Sequence[ConnectionItem]] = None,
        as_job: Literal[True] = True,
    ) -> JobItem:
        pass

    # Publish datasource
    @api(version="2.0")
    @parameter_added_in(connections="2.8")
    @parameter_added_in(as_job="3.0")
    def publish(
        self,
        datasource_item,
        file,
        mode,
        connection_credentials=None,
        connections=None,
        as_job=False,
    ):
        """
        Publishes a data source to a server, or appends data to an existing
        data source.

        This method checks the size of the data source and automatically
        determines whether the publish the data source in multiple parts or in
        one operation.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#publish_data_source

        Parameters
        ----------
        datasource_item : DatasourceItem
            The datasource item to publish. The fields for name and project_id
            are required.

        file : PathOrFileR
            The file path or file object to publish.

        mode : str
            Specifies whether you are publishing a new datasource (CreateNew),
            overwriting an existing datasource (Overwrite), or add to an
            existing datasource (Append). You can also use the publish mode
            attributes, for example: TSC.Server.PublishMode.Overwrite.

        connection_credentials : Optional[ConnectionCredentials]
            The connection credentials to use when publishing the datasource.
            Mutually exclusive with the connections parameter.

        connections : Optional[Sequence[ConnectionItem]]
            The connections to use when publishing the datasource. Mutually
            exclusive with the connection_credentials parameter.

        as_job : bool, default False
            If True, the publish operation is asynchronous and returns a job
            item. If False, the publish operation is synchronous and returns a
            datasource item.

        Returns
        -------
        Union[DatasourceItem, JobItem]
            The datasource item or job item.

        """
        if isinstance(file, (os.PathLike, str)):
            if not os.path.isfile(file):
                error = "File path does not lead to an existing file."
                raise OSError(error)

            filename = os.path.basename(file)
            file_extension = os.path.splitext(filename)[1][1:]
            file_size = os.path.getsize(file)
            logger.debug(f"Publishing file `{filename}`, size `{file_size}`")
            # If name is not defined, grab the name from the file to publish
            if not datasource_item.name:
                datasource_item.name = os.path.splitext(filename)[0]
            if file_extension not in ALLOWED_FILE_EXTENSIONS:
                error = "Only {} files can be published as datasources.".format(", ".join(ALLOWED_FILE_EXTENSIONS))
                raise ValueError(error)

        elif isinstance(file, io_types_r):
            if not datasource_item.name:
                error = "Datasource item must have a name when passing a file object"
                raise ValueError(error)

            file_type = get_file_type(file)
            if file_type == "zip":
                file_extension = "tdsx"
            elif file_type == "xml":
                file_extension = "tds"
            else:
                error = f"Unsupported file type {file_type}"
                raise ValueError(error)

            filename = f"{datasource_item.name}.{file_extension}"
            file_size = get_file_object_size(file)

        else:
            raise TypeError("file should be a filepath or file object.")

        # Construct the url with the defined mode
        url = f"{self.baseurl}?datasourceType={file_extension}"
        if not mode or not hasattr(self.parent_srv.PublishMode, mode):
            error = f"Invalid mode defined: {mode}"
            raise ValueError(error)
        else:
            url += f"&{mode.lower()}=true"

        if as_job:
            url += "&{}=true".format("asJob")

        # Determine if chunking is required (64MB is the limit for single upload method)
        if file_size >= config.FILESIZE_LIMIT_MB * BYTES_PER_MB:
            logger.info(
                "Publishing {} to server with chunking method (datasource over {}MB, chunk size {}MB)".format(
                    filename, config.FILESIZE_LIMIT_MB, config.CHUNK_SIZE_MB
                )
            )
            upload_session_id = self.parent_srv.fileuploads.upload(file)
            url = f"{url}&uploadSessionId={upload_session_id}"
            xml_request, content_type = RequestFactory.Datasource.publish_req_chunked(
                datasource_item, connection_credentials, connections
            )
        else:
            logger.info(f"Publishing {filename} to server")

            if isinstance(file, (Path, str)):
                with open(file, "rb") as f:
                    file_contents = f.read()
            elif isinstance(file, io_types_r):
                file_contents = file.read()
            else:
                raise TypeError("file should be a filepath or file object.")

            xml_request, content_type = RequestFactory.Datasource.publish_req(
                datasource_item,
                filename,
                file_contents,
                connection_credentials,
                connections,
            )

        # Send the publishing request to server
        try:
            server_response = self.post_request(url, xml_request, content_type)
        except InternalServerError as err:
            if err.code == 504 and not as_job:
                err.content = "Timeout error while publishing. Please use asynchronous publishing to avoid timeouts."
            raise err

        if as_job:
            new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info(f"Published {filename} (JOB_ID: {new_job.id}")
            return new_job
        else:
            new_datasource = DatasourceItem.from_response(server_response.content, self.parent_srv.namespace)[0]
            logger.info(f"Published {filename} (ID: {new_datasource.id})")
            return new_datasource

    @api(version="3.13")
    def update_hyper_data(
        self,
        datasource_or_connection_item: Union[DatasourceItem, ConnectionItem, str],
        *,
        request_id: str,
        actions: Sequence[HyperAction],
        payload: Optional[FilePath] = None,
    ) -> JobItem:
        """
        Incrementally updates data (insert, update, upsert, replace and delete)
        in a published data source from a live-to-Hyper connection, where the
        data source has multiple connections.

        A live-to-Hyper connection has a Hyper or Parquet formatted
        file/database as the origin of its data.

        For all connections to Parquet files, and for any data sources with a
        single connection generally, you can use the Update Data in Hyper Data
        Source method without specifying the connection-id.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#update_data_in_hyper_connection

        Parameters
        ----------
        datasource_or_connection_item : Union[DatasourceItem, ConnectionItem, str]
            The datasource item, connection item, or datasource ID. Either a
            DataSourceItem or a ConnectionItem. If the datasource only contains
            a single connection, the DataSourceItem is sufficient to identify
            which data should be updated. Otherwise, for datasources with
            multiple connections, a ConnectionItem must be provided.

        request_id : str
            User supplied arbitrary string to identify the request. A request
            identified with the same key will only be executed once, even if
            additional requests using the key are made, for instance, due to
            retries when facing network issues.

        actions : Sequence[Mapping]
            A list of actions (insert, update, delete, ...) specifying how to
            modify the data within the published datasource. For more
            information on the actions, see: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_how_to_update_data_to_hyper.htm#action-batch-descriptions

        payload : Optional[FilePath]
            A Hyper file containing tuples to be inserted/deleted/updated or
            other payload data used by the actions. Hyper files can be created
            using the Tableau Hyper API or pantab.

        Returns
        -------
        JobItem
            The job running on the server.

        """
        if isinstance(datasource_or_connection_item, DatasourceItem):
            datasource_id = datasource_or_connection_item.id
            url = f"{self.baseurl}/{datasource_id}/data"
        elif isinstance(datasource_or_connection_item, ConnectionItem):
            datasource_id = datasource_or_connection_item.datasource_id
            connection_id = datasource_or_connection_item.id
            url = f"{self.baseurl}/{datasource_id}/connections/{connection_id}/data"
        else:
            assert isinstance(datasource_or_connection_item, str)
            url = f"{self.baseurl}/{datasource_or_connection_item}/data"

        if payload is not None:
            if not os.path.isfile(payload):
                error = "File path does not lead to an existing file."
                raise OSError(error)

            logger.info(f"Uploading {payload} to server with chunking method for Update job")
            upload_session_id = self.parent_srv.fileuploads.upload(payload)
            url = f"{url}?uploadSessionId={upload_session_id}"

        json_request = json.dumps({"actions": actions})
        parameters = {"headers": {"requestid": request_id}}
        server_response = self.patch_request(url, json_request, "application/json", parameters=parameters)
        new_job = JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return new_job

    @api(version="2.0")
    def populate_permissions(self, item: DatasourceItem) -> None:
        """
        Populates the permissions on the specified datasource item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_data_source_permissions

        Parameters
        ----------
        item : DatasourceItem
            The datasource item to populate permissions for.

        Returns
        -------
        None
        """
        self._permissions.populate(item)

    @api(version="2.0")
    def update_permissions(self, item: DatasourceItem, permission_item: list["PermissionsRule"]) -> None:
        """
        Updates the permissions on the specified datasource item. This method
        overwrites all existing permissions. Any permissions not included in
        the list will be removed.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_permissions_for_content

        Parameters
        ----------
        item : DatasourceItem
            The datasource item to update permissions for.

        permission_item : list[PermissionsRule]
            The permissions to apply to the datasource item.

        Returns
        -------
        None
        """
        self._permissions.update(item, permission_item)

    @api(version="2.0")
    def delete_permission(self, item: DatasourceItem, capability_item: "PermissionsRule") -> None:
        """
        Deletes a single permission rule from the specified datasource item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_data_source_permissionDatasourceItem

        Parameters
        ----------
        item : DatasourceItem
            The datasource item to delete permissions from.

        capability_item : PermissionsRule
            The permission rule to delete.

        Returns
        -------
        None
        """
        self._permissions.delete(item, capability_item)

    @api(version="3.5")
    def populate_dqw(self, item) -> None:
        """
        Get information about the data quality warning for the database, table,
        column, published data source, flow, virtual connection, or virtual
        connection table.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#query_dqws

        Parameters
        ----------
        item : DatasourceItem
            The datasource item to populate data quality warnings for.

        Returns
        -------
        None
        """
        self._data_quality_warnings.populate(item)

    @api(version="3.5")
    def update_dqw(self, item: DatasourceItem, warning: "DQWItem") -> list["DQWItem"]:
        """
        Update the warning type, status, and message of a data quality warning.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#update_dqw

        Parameters
        ----------
        item : DatasourceItem
            The datasource item to update data quality warnings for.

        warning : DQWItem
            The data quality warning to update.

        Returns
        -------
        DQWItem
            The updated data quality warning.
        """
        return self._data_quality_warnings.update(item, warning)

    @api(version="3.5")
    def add_dqw(self, item: DatasourceItem, warning: "DQWItem") -> list["DQWItem"]:
        """
        Add a data quality warning to a datasource.

        The Add Data Quality Warning method adds a data quality warning to an
        asset. (An automatically-generated monitoring warning does not count
        towards this limit.) In Tableau Cloud February 2024 and Tableau Server
        2024.2 and earlier, adding a data quality warning to an asset that
        already has one causes an error.

        This method is available if your Tableau Cloud site or Tableau Server is licensed with Data Management.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#add_dqw

        Parameters
        ----------
        item: DatasourceItem
            The datasource item to add data quality warnings to.

        warning: DQWItem
            The data quality warning to add.

        Returns
        -------
        DQWItem
            The added data quality warning.

        """
        return self._data_quality_warnings.add(item, warning)

    @api(version="3.5")
    def delete_dqw(self, item: DatasourceItem) -> None:
        """
        Delete a data quality warnings from an asset.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#delete_dqws

        Parameters
        ----------
        item: DatasourceItem
            The datasource item to delete data quality warnings from.

        Returns
        -------
        None
        """
        self._data_quality_warnings.clear(item)

    # Populate datasource item's revisions
    @api(version="2.3")
    def populate_revisions(self, datasource_item: DatasourceItem) -> None:
        """
        Retrieve revision information for the specified datasource item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#get_data_source_revisions

        Parameters
        ----------
        datasource_item : DatasourceItem
            The datasource item to retrieve revisions for.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the datasource item is missing an ID.
        """
        if not datasource_item.id:
            error = "Datasource item missing ID. Datasource must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def revisions_fetcher():
            return self._get_datasource_revisions(datasource_item)

        datasource_item._set_revisions(revisions_fetcher)
        logger.info(f"Populated revisions for datasource (ID: {datasource_item.id})")

    def _get_datasource_revisions(
        self, datasource_item: DatasourceItem, req_options: Optional["RequestOptions"] = None
    ) -> list[RevisionItem]:
        url = f"{self.baseurl}/{datasource_item.id}/revisions"
        server_response = self.get_request(url, req_options)
        revisions = RevisionItem.from_response(server_response.content, self.parent_srv.namespace, datasource_item)
        return revisions

    T = TypeVar("T", bound=FileObjectW)

    @overload
    def download_revision(
        self,
        datasource_id: str,
        revision_number: Optional[str],
        filepath: T,
        include_extract: bool = True,
    ) -> T: ...

    @overload
    def download_revision(
        self,
        datasource_id: str,
        revision_number: Optional[str],
        filepath: Optional[FilePath] = None,
        include_extract: bool = True,
    ) -> str: ...

    # Download 1 datasource revision by revision number
    @api(version="2.3")
    def download_revision(
        self,
        datasource_id,
        revision_number,
        filepath=None,
        include_extract=True,
    ):
        """
        Downloads a specific version of a data source prior to the current one
        in .tdsx format. To download the current version of a data source set
        the revision number to None.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#download_data_source_revision

        Parameters
        ----------
        datasource_id : str
            The unique ID of the datasource to download.

        revision_number : Optional[str]
            The revision number of the data source to download. To determine
            what versions are available, call the `populate_revisions` method.
            Pass None to download the current version.

        filepath : Optional[PathOrFileW]
            The file path to save the downloaded datasource to. If not
            specified, the file will be saved to the current working directory.

        include_extract : bool, default True
            If True, the extract is included in the download. If False, the
            extract is not included.

        Returns
        -------
        filepath : PathOrFileW
        """
        if not datasource_id:
            error = "Datasource ID undefined."
            raise ValueError(error)
        if revision_number is None:
            url = f"{self.baseurl}/{datasource_id}/content"
        else:
            url = f"{self.baseurl}/{datasource_id}/revisions/{revision_number}/content"

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

        logger.info(f"Downloaded datasource revision {revision_number} to {return_path} (ID: {datasource_id})")
        return return_path

    @api(version="2.3")
    def delete_revision(self, datasource_id: str, revision_number: str) -> None:
        """
        Removes a specific version of a data source from the specified site.

        The content is removed, and the specified revision can no longer be
        downloaded using Download Data Source Revision. If you call Get Data
        Source Revisions, the revision that's been removed is listed with the
        attribute is_deleted=True.

        REST API:https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#remove_data_source_revision

        Parameters
        ----------
        datasource_id : str
            The unique ID of the datasource to delete.

        revision_number : str
            The revision number of the data source to delete.

        Returns
        -------
        None
        """
        if datasource_id is None or revision_number is None:
            raise ValueError
        url = "/".join([self.baseurl, datasource_id, "revisions", revision_number])

        self.delete_request(url)
        logger.info(f"Deleted single datasource revision (ID: {datasource_id}) (Revision: {revision_number})")

    # a convenience method
    @api(version="2.8")
    def schedule_extract_refresh(
        self, schedule_id: str, item: DatasourceItem
    ) -> list["AddResponse"]:  # actually should return a task
        """
        Adds a task to refresh a data source to an existing server schedule on
        Tableau Server.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#add_data_source_to_schedule

        Parameters
        ----------
        schedule_id : str
            The unique ID of the schedule to add the task to.

        item : DatasourceItem
            The datasource item to add to the schedule.

        Returns
        -------
        list[AddResponse]
        """
        return self.parent_srv.schedules.add_to_schedule(schedule_id, datasource=item)

    @api(version="1.0")
    def add_tags(self, item: Union[DatasourceItem, str], tags: Union[Iterable[str], str]) -> set[str]:
        """
        Adds one or more tags to the specified datasource item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#add_tags_to_data_source

        Parameters
        ----------
        item : Union[DatasourceItem, str]
            The datasource item or ID to add tags to.

        tags : Union[Iterable[str], str]
            The tag or tags to add to the datasource item.

        Returns
        -------
        set[str]
            The updated set of tags on the datasource item.
        """
        return super().add_tags(item, tags)

    @api(version="1.0")
    def delete_tags(self, item: Union[DatasourceItem, str], tags: Union[Iterable[str], str]) -> None:
        """
        Deletes one or more tags from the specified datasource item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data_sources.htm#delete_tag_from_data_source

        Parameters
        ----------
        item : Union[DatasourceItem, str]
            The datasource item or ID to delete tags from.

        tags : Union[Iterable[str], str]
            The tag or tags to delete from the datasource item.

        Returns
        -------
        None
        """
        return super().delete_tags(item, tags)

    @api(version="1.0")
    def update_tags(self, item: DatasourceItem) -> None:
        """
        Updates the tags on the server to match the specified datasource item.

        Parameters
        ----------
        item : DatasourceItem
            The datasource item to update tags for.

        Returns
        -------
        None
        """
        return super().update_tags(item)

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> QuerySet[DatasourceItem]:
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


        authentication_type=...
        authentication_type__in=...
        connected_workbook_type=...
        connected_workbook_type__gt=...
        connected_workbook_type__gte=...
        connected_workbook_type__lt=...
        connected_workbook_type__lte=...
        connection_to=...
        connection_to__in=...
        connection_type=...
        connection_type__in=...
        content_url=...
        content_url__in=...
        created_at=...
        created_at__gt=...
        created_at__gte=...
        created_at__lt=...
        created_at__lte=...
        database_name=...
        database_name__in=...
        database_user_name=...
        database_user_name__in=...
        description=...
        description__in=...
        favorites_total=...
        favorites_total__gt=...
        favorites_total__gte=...
        favorites_total__lt=...
        favorites_total__lte=...
        has_alert=...
        has_embedded_password=...
        has_extracts=...
        is_certified=...
        is_connectable=...
        is_default_port=...
        is_hierarchical=...
        is_published=...
        name=...
        name__in=...
        owner_domain=...
        owner_domain__in=...
        owner_email=...
        owner_name=...
        owner_name__in=...
        project_name=...
        project_name__in=...
        server_name=...
        server_name__in=...
        server_port=...
        size=...
        size__gt=...
        size__gte=...
        size__lt=...
        size__lte=...
        table_name=...
        table_name__in=...
        tags=...
        tags__in=...
        type=...
        updated_at=...
        updated_at__gt=...
        updated_at__gte=...
        updated_at__lt=...
        updated_at__lte=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
