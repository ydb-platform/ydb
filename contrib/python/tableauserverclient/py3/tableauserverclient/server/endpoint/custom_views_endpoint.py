import io
import logging
import os
from contextlib import closing
from pathlib import Path
from typing import Optional, Union, TYPE_CHECKING
from collections.abc import Iterator

from tableauserverclient.config import BYTES_PER_MB, config
from tableauserverclient.filesys_helpers import get_file_object_size
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api
from tableauserverclient.server.endpoint.exceptions import MissingRequiredFieldError
from tableauserverclient.models import CustomViewItem, PaginationItem
from tableauserverclient.server import (
    RequestFactory,
    RequestOptions,
    ImageRequestOptions,
    PDFRequestOptions,
    CSVRequestOptions,
)

from tableauserverclient.helpers.logging import logger

if TYPE_CHECKING:
    from tableauserverclient.server.query import QuerySet

"""
Get a list of custom views on a site
get the details of a custom view
download an image of a custom view.
Delete a custom view 
update the name or owner of a custom view.
"""

FilePath = Union[str, os.PathLike]
FileObject = Union[io.BufferedReader, io.BytesIO]
FileObjectR = Union[io.BufferedReader, io.BytesIO]
FileObjectW = Union[io.BufferedWriter, io.BytesIO]
PathOrFileR = Union[FilePath, FileObjectR]
PathOrFileW = Union[FilePath, FileObjectW]
io_types_r = (io.BufferedReader, io.BytesIO)
io_types_w = (io.BufferedWriter, io.BytesIO)


class CustomViews(QuerysetEndpoint[CustomViewItem]):
    def __init__(self, parent_srv):
        super().__init__(parent_srv)

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/customviews"

    @property
    def expurl(self) -> str:
        return f"{self.parent_srv._server_address}/api/exp/sites/{self.parent_srv.site_id}/customviews"

    @api(version="3.18")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[CustomViewItem], PaginationItem]:
        """
        Get a list of custom views on a site.

        If the request has no filter parameters: Administrators will see all custom views.
            Other users will see only custom views that they own.
        If the filter parameters include ownerId: Users will see only custom views that they own.
        If the filter parameters include viewId and/or workbookId, and don't include ownerId:
            Users will see those custom views that they have Write and WebAuthoring permissions for.
        If site user visibility is not set to Limited, the Users will see those custom views that are "public",
         meaning the value of their shared attribute is true.
        If site user visibility is set to Limited, ????

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#list_custom_views

        Parameters
        ----------
        req_options : RequestOptions, optional
            Filtering options for the request, by default None

        Returns
        -------
        tuple[list[CustomViewItem], PaginationItem]
        """
        logger.info("Querying all custom views on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_view_items = CustomViewItem.list_from_response(server_response.content, self.parent_srv.namespace)
        return all_view_items, pagination_item

    @api(version="3.18")
    def get_by_id(self, view_id: str) -> Optional[CustomViewItem]:
        """
        Get the details of a specific custom view.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#get_custom_view

        Parameters
        ----------
        view_id : str

        Returns
        -------
        Optional[CustomViewItem]
        """
        if not view_id:
            error = "Custom view item missing ID."
            raise MissingRequiredFieldError(error)
        logger.info(f"Querying custom view (ID: {view_id})")
        url = f"{self.baseurl}/{view_id}"
        server_response = self.get_request(url)
        return CustomViewItem.from_response(server_response.content, self.parent_srv.namespace)

    @api(version="3.18")
    def populate_image(self, view_item: CustomViewItem, req_options: Optional["ImageRequestOptions"] = None) -> None:
        """
        Populate the image of a custom view.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#get_custom_view_image

        Parameters
        ----------
        view_item : CustomViewItem

        req_options : ImageRequestOptions, optional
            Options to customize the image returned, by default None

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the view_item is missing an ID
        """
        if not view_item.id:
            error = "Custom View item missing ID."
            raise MissingRequiredFieldError(error)

        def image_fetcher():
            return self._get_view_image(view_item, req_options)

        view_item._set_image(image_fetcher)
        logger.info(f"Populated image for custom view (ID: {view_item.id})")

    def _get_view_image(self, view_item: CustomViewItem, req_options: Optional["ImageRequestOptions"]) -> bytes:
        url = f"{self.baseurl}/{view_item.id}/image"
        server_response = self.get_request(url, req_options)
        image = server_response.content
        return image

    @api(version="3.23")
    def populate_pdf(self, custom_view_item: CustomViewItem, req_options: Optional["PDFRequestOptions"] = None) -> None:
        """
        Populate the PDF of a custom view.

        Parameters
        ----------
        custom_view_item : CustomViewItem
            The custom view item to populate the PDF for.

        req_options : PDFRequestOptions, optional
            Options to customize the PDF returned, by default None

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the custom view item is missing an ID
        """
        if not custom_view_item.id:
            error = "Custom View item missing ID."
            raise MissingRequiredFieldError(error)

        def pdf_fetcher():
            return self._get_custom_view_pdf(custom_view_item, req_options)

        custom_view_item._set_pdf(pdf_fetcher)
        logger.info(f"Populated pdf for custom view (ID: {custom_view_item.id})")

    def _get_custom_view_pdf(
        self, custom_view_item: CustomViewItem, req_options: Optional["PDFRequestOptions"]
    ) -> bytes:
        url = f"{self.baseurl}/{custom_view_item.id}/pdf"
        server_response = self.get_request(url, req_options)
        pdf = server_response.content
        return pdf

    @api(version="3.23")
    def populate_csv(self, custom_view_item: CustomViewItem, req_options: Optional["CSVRequestOptions"] = None) -> None:
        """
        Populate the CSV of a custom view.

        Parameters
        ----------
        custom_view_item : CustomViewItem
            The custom view item to populate the CSV for.

        req_options : CSVRequestOptions, optional
            Options to customize the CSV returned, by default None

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the custom view item is missing an ID
        """
        if not custom_view_item.id:
            error = "Custom View item missing ID."
            raise MissingRequiredFieldError(error)

        def csv_fetcher():
            return self._get_custom_view_csv(custom_view_item, req_options)

        custom_view_item._set_csv(csv_fetcher)
        logger.info(f"Populated csv for custom view (ID: {custom_view_item.id})")

    def _get_custom_view_csv(
        self, custom_view_item: CustomViewItem, req_options: Optional["CSVRequestOptions"]
    ) -> Iterator[bytes]:
        url = f"{self.baseurl}/{custom_view_item.id}/data"

        with closing(self.get_request(url, request_object=req_options, parameters={"stream": True})) as server_response:
            yield from server_response.iter_content(1024)

    @api(version="3.18")
    def update(self, view_item: CustomViewItem) -> Optional[CustomViewItem]:
        """
        Updates the name, owner, or shared status of a custom view.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#update_custom_view

        Parameters
        ----------
        view_item : CustomViewItem
            The custom view item to update.

        Returns
        -------
        Optional[CustomViewItem]
            The updated custom view item.
        """
        if not view_item.id:
            error = "Custom view item missing ID."
            raise MissingRequiredFieldError(error)
        if not (view_item.owner or view_item.name or view_item.shared):
            logger.debug("No changes to make")
            return view_item

        # Update the custom view owner or name
        url = f"{self.baseurl}/{view_item.id}"
        update_req = RequestFactory.CustomView.update_req(view_item)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated custom view (ID: {view_item.id})")
        return CustomViewItem.from_response(server_response.content, self.parent_srv.namespace)

    # Delete 1 view by id
    @api(version="3.19")
    def delete(self, view_id: str) -> None:
        """
        Deletes a single custom view by ID.

        Rest API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#delete_custom_view

        Parameters
        ----------
        view_id : str
            The ID of the custom view to delete.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the view_id is not provided.
        """
        if not view_id:
            error = "Custom View ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{view_id}"
        self.delete_request(url)
        logger.info(f"Deleted single custom view (ID: {view_id})")

    @api(version="3.21")
    def download(self, view_item: CustomViewItem, file: PathOrFileW) -> PathOrFileW:
        """
        Download the definition of a custom view as json. The file parameter can
        be a file path or a file object. If a file path is provided, the file
        will be written to that location. If a file object is provided, the file
        will be written to that object.

        May contain sensitive information.

        Parameters
        ----------
        view_item : CustomViewItem
            The custom view item to download.

        file : PathOrFileW
            The file path or file object to write the custom view to.

        Returns
        -------
        PathOrFileW
            The file path or file object that the custom view was written to.
        """
        url = f"{self.expurl}/{view_item.id}/content"
        server_response = self.get_request(url)
        if isinstance(file, io_types_w):
            file.write(server_response.content)
            return file

        with open(file, "wb") as f:
            f.write(server_response.content)

        return file

    @api(version="3.21")
    def publish(self, view_item: CustomViewItem, file: PathOrFileR) -> Optional[CustomViewItem]:
        """
        Publish a custom view to Tableau Server. The file parameter can be a
        file path or a file object. If a file path is provided, the file will be
        read from that location. If a file object is provided, the file will be
        read from that object.

        Parameters
        ----------
        view_item : CustomViewItem
            The custom view item to publish.

        file : PathOrFileR
            The file path or file object to read the custom view from.

        Returns
        -------
        Optional[CustomViewItem]
            The published custom view item.
        """
        url = self.expurl
        if isinstance(file, io_types_r):
            size = get_file_object_size(file)
        elif isinstance(file, (str, Path)) and (p := Path(file)).is_file():
            size = p.stat().st_size
        else:
            raise ValueError("File path or file object required for publishing custom view.")

        if size >= config.FILESIZE_LIMIT_MB * BYTES_PER_MB:
            upload_session_id = self.parent_srv.fileuploads.upload(file)
            url = f"{url}?uploadSessionId={upload_session_id}"
            xml_request, content_type = RequestFactory.CustomView.publish_req_chunked(view_item)
        else:
            if isinstance(file, io_types_r):
                file.seek(0)
                contents = file.read()
                if view_item.name is None:
                    raise MissingRequiredFieldError("Custom view item missing name.")
                filename = view_item.name
            elif isinstance(file, (str, Path)):
                filename = Path(file).name
                contents = Path(file).read_bytes()

            xml_request, content_type = RequestFactory.CustomView.publish_req(view_item, filename, contents)

        server_response = self.post_request(url, xml_request, content_type)
        return CustomViewItem.from_response(server_response.content, self.parent_srv.namespace)

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> "QuerySet[CustomViewItem]":
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

        view_id=...
        workbook_id=...
        owner_id=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
