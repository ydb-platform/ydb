import logging
from contextlib import closing

from tableauserverclient.models.permissions_item import PermissionsRule
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api
from tableauserverclient.server.endpoint.exceptions import MissingRequiredFieldError, UnsupportedAttributeError
from tableauserverclient.server.endpoint.permissions_endpoint import _PermissionsEndpoint
from tableauserverclient.server.endpoint.resource_tagger import TaggingMixin
from tableauserverclient.server.query import QuerySet

from tableauserverclient.models import ViewItem, PaginationItem

from tableauserverclient.helpers.logging import logger

from typing import Optional, TYPE_CHECKING, Union
from collections.abc import Iterable, Iterator

if TYPE_CHECKING:
    from tableauserverclient.server.request_options import (
        RequestOptions,
        CSVRequestOptions,
        PDFRequestOptions,
        ImageRequestOptions,
        ExcelRequestOptions,
    )


class Views(QuerysetEndpoint[ViewItem], TaggingMixin[ViewItem]):
    """
    The Tableau Server Client provides methods for interacting with view
    resources, or endpoints. These methods correspond to the endpoints for views
    in the Tableau Server REST API.
    """

    def __init__(self, parent_srv):
        super().__init__(parent_srv)
        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)

    # Used because populate_preview_image functionaliy requires workbook endpoint
    @property
    def siteurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}"

    @property
    def baseurl(self) -> str:
        return f"{self.siteurl}/views"

    @api(version="2.2")
    def get(
        self, req_options: Optional["RequestOptions"] = None, usage: bool = False
    ) -> tuple[list[ViewItem], PaginationItem]:
        """
        Returns the list of views on the site. Paginated endpoint.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_views_for_site

        Parameters
        ----------
        req_options: Optional[RequestOptions], default None
            The request options for the request. These options can include
            parameters such as page size and sorting.

        usage: bool, default False
            If True, includes usage statistics in the response.

        Returns
        -------
        views: tuple[list[ViewItem], PaginationItem]
        """
        logger.info("Querying all views on site")
        url = self.baseurl
        if usage:
            url += "?includeUsageStatistics=true"
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_view_items = ViewItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_view_items, pagination_item

    @api(version="3.1")
    def get_by_id(self, view_id: str, usage: bool = False) -> ViewItem:
        """
        Returns the details of a specific view.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#get_view

        Parameters
        ----------
        view_id: str
            The view ID.

        usage: bool, default False
            If True, includes usage statistics in the response.

        Returns
        -------
        view_item: ViewItem
        """
        if not view_id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)
        logger.info(f"Querying single view (ID: {view_id})")
        url = f"{self.baseurl}/{view_id}"
        if usage:
            url += "?includeUsageStatistics=true"
        server_response = self.get_request(url)
        return ViewItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.0")
    def populate_preview_image(self, view_item: ViewItem) -> None:
        """
        Populates a preview image for the specified view.

        This method gets the preview image (thumbnail) for the specified view
        item. The method uses the id and workbook_id fields to query the preview
        image. The method populates the preview_image for the view.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_view_with_preview

        Parameters
        ----------
        view_item: ViewItem
            The view item for which to populate the preview image.

        Returns
        -------
        None
        """
        if not view_item.id or not view_item.workbook_id:
            error = "View item missing ID or workbook ID."
            raise MissingRequiredFieldError(error)

        def image_fetcher():
            return self._get_preview_for_view(view_item)

        view_item._set_preview_image(image_fetcher)
        logger.info(f"Populated preview image for view (ID: {view_item.id})")

    def _get_preview_for_view(self, view_item: ViewItem) -> bytes:
        url = f"{self.siteurl}/workbooks/{view_item.workbook_id}/views/{view_item.id}/previewImage"
        server_response = self.get_request(url)
        image = server_response.content
        return image

    @api(version="2.5")
    def populate_image(self, view_item: ViewItem, req_options: Optional["ImageRequestOptions"] = None) -> None:
        """
        Populates the image of the specified view.

        This method uses the id field to query the image, and populates the
        image content as the image field.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_view_image

        Parameters
        ----------
        view_item: ViewItem
            The view item for which to populate the image.

        req_options: Optional[ImageRequestOptions], default None
            Optional request options for the request. These options can include
            parameters such as image resolution and max age.

        Returns
        -------
        None
        """
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def image_fetcher():
            return self._get_view_image(view_item, req_options)

        if not self.parent_srv.check_at_least_version("3.23") and req_options is not None:
            if req_options.viz_height or req_options.viz_width:
                raise UnsupportedAttributeError("viz_height and viz_width are only supported in 3.23+")

        view_item._set_image(image_fetcher)
        logger.info(f"Populated image for view (ID: {view_item.id})")

    def _get_view_image(self, view_item: ViewItem, req_options: Optional["ImageRequestOptions"]) -> bytes:
        url = f"{self.baseurl}/{view_item.id}/image"
        server_response = self.get_request(url, req_options)
        image = server_response.content
        return image

    @api(version="2.7")
    def populate_pdf(self, view_item: ViewItem, req_options: Optional["PDFRequestOptions"] = None) -> None:
        """
        Populates the PDF content of the specified view.

        This method populates a PDF with image(s) of the view you specify.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_view_pdf

        Parameters
        ----------
        view_item: ViewItem
            The view item for which to populate the PDF.

        req_options: Optional[PDFRequestOptions], default None
            Optional request options for the request. These options can include
            parameters such as orientation and paper size.

        Returns
        -------
        None
        """
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def pdf_fetcher():
            return self._get_view_pdf(view_item, req_options)

        view_item._set_pdf(pdf_fetcher)
        logger.info(f"Populated pdf for view (ID: {view_item.id})")

    def _get_view_pdf(self, view_item: ViewItem, req_options: Optional["PDFRequestOptions"]) -> bytes:
        url = f"{self.baseurl}/{view_item.id}/pdf"
        server_response = self.get_request(url, req_options)
        pdf = server_response.content
        return pdf

    @api(version="2.7")
    def populate_csv(self, view_item: ViewItem, req_options: Optional["CSVRequestOptions"] = None) -> None:
        """
        Populates the CSV data of the specified view.

        This method uses the id field to query the CSV data, and populates the
        data as the csv field.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#query_view_data

        Parameters
        ----------
        view_item: ViewItem
            The view item for which to populate the CSV data.

        req_options: Optional[CSVRequestOptions], default None
            Optional request options for the request. These options can include
            parameters such as view filters and max age.

        Returns
        -------
        None
        """
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def csv_fetcher():
            return self._get_view_csv(view_item, req_options)

        view_item._set_csv(csv_fetcher)
        logger.info(f"Populated csv for view (ID: {view_item.id})")

    def _get_view_csv(self, view_item: ViewItem, req_options: Optional["CSVRequestOptions"]) -> Iterator[bytes]:
        url = f"{self.baseurl}/{view_item.id}/data"

        with closing(self.get_request(url, request_object=req_options, parameters={"stream": True})) as server_response:
            yield from server_response.iter_content(1024)

    @api(version="3.8")
    def populate_excel(self, view_item: ViewItem, req_options: Optional["ExcelRequestOptions"] = None) -> None:
        """
        Populates the Excel data of the specified view.

        This method uses the id field to query the Excel data, and populates the
        data as the Excel field.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#download_view_excel

        Parameters
        ----------
        view_item: ViewItem
            The view item for which to populate the Excel data.

        req_options: Optional[ExcelRequestOptions], default None
            Optional request options for the request. These options can include
            parameters such as view filters and max age.

        Returns
        -------
        None
        """
        if not view_item.id:
            error = "View item missing ID."
            raise MissingRequiredFieldError(error)

        def excel_fetcher():
            return self._get_view_excel(view_item, req_options)

        view_item._set_excel(excel_fetcher)
        logger.info(f"Populated excel for view (ID: {view_item.id})")

    def _get_view_excel(self, view_item: ViewItem, req_options: Optional["ExcelRequestOptions"]) -> Iterator[bytes]:
        url = f"{self.baseurl}/{view_item.id}/crosstab/excel"

        with closing(self.get_request(url, request_object=req_options, parameters={"stream": True})) as server_response:
            yield from server_response.iter_content(1024)

    @api(version="3.2")
    def populate_permissions(self, item: ViewItem) -> None:
        """
        Returns a list of permissions for the specific view.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_view_permissions

        Parameters
        ----------
        item: ViewItem
            The view item for which to populate the permissions.

        Returns
        -------
        None
        """
        self._permissions.populate(item)

    @api(version="3.2")
    def update_permissions(self, resource: ViewItem, rules: list[PermissionsRule]) -> list[PermissionsRule]:
        """ """
        return self._permissions.update(resource, rules)

    @api(version="3.2")
    def delete_permission(self, item: ViewItem, capability_item: PermissionsRule) -> None:
        """
        Deletes permission to the specified view (also known as a sheet) for a
        Tableau Server user or group.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_view_permission

        Parameters
        ----------
        item: ViewItem
            The view item for which to delete the permission.

        capability_item: PermissionsRule
            The permission rule to delete.

        Returns
        -------
        None
        """
        return self._permissions.delete(item, capability_item)

    # Update view. Currently only tags can be updated
    def update(self, view_item: ViewItem) -> ViewItem:
        """
        Updates the tags for the specified view. All other fields are managed
        through the WorkbookItem object.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#add_tags_to_view

        Parameters
        ----------
        view_item: ViewItem
            The view item for which to update tags.

        Returns
        -------
        ViewItem
        """
        if not view_item.id:
            error = "View item missing ID. View must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        self.update_tags(view_item)

        # Returning view item to stay consistent with datasource/view update functions
        return view_item

    @api(version="3.27")
    def delete(self, view: ViewItem | str) -> None:
        """
        Deletes a view in a workbook. If you delete the only view in a workbook,
        the workbook is deleted. Can be used to remove hidden views when
        republishing or migrating to a different environment.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#delete_view

        Parameters
        ----------
        view: ViewItem | str
            The ViewItem or the luid for the view to be deleted.

        Returns
        -------
        None
        """
        id_ = getattr(view, "id", view)
        self.delete_request(f"{self.baseurl}/{id_}")
        logger.info(f"View({id_}) deleted.")
        return None

    @api(version="1.0")
    def add_tags(self, item: Union[ViewItem, str], tags: Union[Iterable[str], str]) -> set[str]:
        """
        Adds tags to the specified view.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#add_tags_to_view

        Parameters
        ----------
        item: Union[ViewItem, str]
            The view item or view ID to which to add tags.

        tags: Union[Iterable[str], str]
            The tags to add to the view.

        Returns
        -------
        set[str]

        """
        return super().add_tags(item, tags)

    @api(version="1.0")
    def delete_tags(self, item: Union[ViewItem, str], tags: Union[Iterable[str], str]) -> None:
        """
        Deletes tags from the specified view.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#delete_tags_from_view

        Parameters
        ----------
        item: Union[ViewItem, str]
            The view item or view ID from which to delete tags.

        tags: Union[Iterable[str], str]
            The tags to delete from the view.

        Returns
        -------
        None
        """
        return super().delete_tags(item, tags)

    @api(version="1.0")
    def update_tags(self, item: ViewItem) -> None:
        """
        Updates the tags for the specified view. Any changes to the tags must
        be made by editing the tags attribute of the view item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm#add_tags_to_view

        Parameters
        ----------
        item: ViewItem
            The view item for which to update tags.

        Returns
        -------
        None
        """
        return super().update_tags(item)

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> QuerySet[ViewItem]:
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


        caption=...
        caption__in=...
        content_url=...
        content_url__in=...
        created_at=...
        created_at__gt=...
        created_at__gte=...
        created_at__lt=...
        created_at__lte=...
        favorites_total=...
        favorites_total__gt=...
        favorites_total__gte=...
        favorites_total__lt=...
        favorites_total__lte=...
        fields=...
        fields__in=...
        hits_total=...
        hits_total__gt=...
        hits_total__gte=...
        hits_total__lt=...
        hits_total__lte=...
        name=...
        name__in=...
        owner_domain=...
        owner_domain__in=...
        owner_email=...
        owner_email__in=...
        owner_name=...
        project_name=...
        project_name__in=...
        sheet_number=...
        sheet_number__gt=...
        sheet_number__gte=...
        sheet_number__lt=...
        sheet_number__lte=...
        sheet_type=...
        sheet_type__in=...
        tags=...
        tags__in=...
        title=...
        title__in=...
        updated_at=...
        updated_at__gt=...
        updated_at__gte=...
        updated_at__lt=...
        updated_at__lte=...
        view_url_name=...
        view_url_name__in=...
        workbook_description=...
        workbook_description__in=...
        workbook_name=...
        workbook_name__in=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
