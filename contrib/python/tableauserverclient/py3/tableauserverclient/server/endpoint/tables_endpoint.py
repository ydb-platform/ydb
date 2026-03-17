import logging
from typing import Optional, Union, TYPE_CHECKING
from collections.abc import Iterable

from tableauserverclient.models.permissions_item import PermissionsRule
from tableauserverclient.server.endpoint.dqw_endpoint import _DataQualityWarningEndpoint
from tableauserverclient.server.endpoint.endpoint import api, Endpoint
from tableauserverclient.server.endpoint.exceptions import MissingRequiredFieldError
from tableauserverclient.server.endpoint.permissions_endpoint import _PermissionsEndpoint
from tableauserverclient.server.endpoint.resource_tagger import TaggingMixin
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import TableItem, ColumnItem, PaginationItem
from tableauserverclient.server.pager import Pager

from tableauserverclient.helpers.logging import logger
from tableauserverclient.server.request_options import RequestOptions

if TYPE_CHECKING:
    from tableauserverclient.models import DQWItem, PermissionsRule


class Tables(Endpoint, TaggingMixin[TableItem]):
    def __init__(self, parent_srv):
        super().__init__(parent_srv)

        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)
        self._data_quality_warnings = _DataQualityWarningEndpoint(self.parent_srv, "table")

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/tables"

    @api(version="3.5")
    def get(self, req_options: Optional[RequestOptions] = None) -> tuple[list[TableItem], PaginationItem]:
        """
        Get information about all tables on the site. Endpoint is paginated, and
        will return a default of 100 items per page. Use the `req_options`
        parameter to customize the request.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#query_tables

        Parameters
        ----------
        req_options : RequestOptions, optional
            Options to customize the request. If not provided, defaults to None.

        Returns
        -------
        tuple[list[TableItem], PaginationItem]
            A tuple containing a list of TableItem objects and a PaginationItem
            object.
        """
        logger.info("Querying all tables on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_table_items = TableItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_table_items, pagination_item

    # Get 1 table
    @api(version="3.5")
    def get_by_id(self, table_id: str) -> TableItem:
        """
        Get information about a single table on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#query_table

        Parameters
        ----------
        table_id : str
            The ID of the table to retrieve.

        Returns
        -------
        TableItem
            A TableItem object representing the table.

        Raises
        ------
        ValueError
            If the table ID is not provided.
        """
        if not table_id:
            error = "table ID undefined."
            raise ValueError(error)
        logger.info(f"Querying single table (ID: {table_id})")
        url = f"{self.baseurl}/{table_id}"
        server_response = self.get_request(url)
        return TableItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="3.5")
    def delete(self, table_id: str) -> None:
        """
        Delete a single table from the server.

        Parameters
        ----------
        table_id : str
            The ID of the table to delete.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the table ID is not provided.
        """
        if not table_id:
            error = "Database ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{table_id}"
        self.delete_request(url)
        logger.info(f"Deleted single table (ID: {table_id})")

    @api(version="3.5")
    def update(self, table_item: TableItem) -> TableItem:
        """
        Update a table on the server.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#update_table

        Parameters
        ----------
        table_item : TableItem
            The TableItem object to update.

        Returns
        -------
        TableItem
            The updated TableItem object.

        Raises
        ------
        MissingRequiredFieldError
            If the table item is missing an ID.
        """
        if not table_item.id:
            error = "table item missing ID."
            raise MissingRequiredFieldError(error)

        url = f"{self.baseurl}/{table_item.id}"
        update_req = RequestFactory.Table.update_req(table_item)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated table item (ID: {table_item.id})")
        updated_table = TableItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return updated_table

    # Get all columns of the table
    @api(version="3.5")
    def populate_columns(self, table_item: TableItem, req_options: Optional[RequestOptions] = None) -> None:
        """
        Populate the columns of a table item. Sets a fetcher function to
        retrieve the columns when needed.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#query_columns

        Parameters
        ----------
        table_item : TableItem
            The TableItem object to populate columns for.

        req_options : RequestOptions, optional
            Options to customize the request. If not provided, defaults to None.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the table item is missing an ID.
        """
        if not table_item.id:
            error = "Table item missing ID. table must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def column_fetcher():
            return Pager(
                lambda options: self._get_columns_for_table(table_item, options),  # type: ignore
                req_options,
            )

        table_item._set_columns(column_fetcher)
        logger.info(f"Populated columns for table (ID: {table_item.id}")

    def _get_columns_for_table(
        self, table_item: TableItem, req_options: Optional[RequestOptions] = None
    ) -> tuple[list[ColumnItem], PaginationItem]:
        url = f"{self.baseurl}/{table_item.id}/columns"
        server_response = self.get_request(url, req_options)
        columns = ColumnItem.from_response(server_response.content, self.parent_srv.namespace)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        return columns, pagination_item

    @api(version="3.5")
    def update_column(self, table_item: TableItem, column_item: ColumnItem) -> ColumnItem:
        """
        Update the description of a column in a table.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_metadata.htm#update_column

        Parameters
        ----------
        table_item : TableItem
            The TableItem object representing the table.

        column_item : ColumnItem
            The ColumnItem object representing the column to update.

        Returns
        -------
        ColumnItem
            The updated ColumnItem object.
        """
        url = f"{self.baseurl}/{table_item.id}/columns/{column_item.id}"
        update_req = RequestFactory.Column.update_req(column_item)
        server_response = self.put_request(url, update_req)
        column = ColumnItem.from_response(server_response.content, self.parent_srv.namespace)[0]

        logger.info(f"Updated table item (ID: {table_item.id} & column item {column_item.id}")
        return column

    @api(version="3.5")
    def populate_permissions(self, item: TableItem) -> None:
        self._permissions.populate(item)

    @api(version="3.5")
    def update_permissions(self, item: TableItem, rules: list[PermissionsRule]) -> list[PermissionsRule]:
        return self._permissions.update(item, rules)

    @api(version="3.5")
    def delete_permission(self, item: TableItem, rules: list[PermissionsRule]) -> None:
        return self._permissions.delete(item, rules)

    @api(version="3.5")
    def populate_dqw(self, item: TableItem) -> None:
        self._data_quality_warnings.populate(item)

    @api(version="3.5")
    def update_dqw(self, item: TableItem, warning: "DQWItem") -> list["DQWItem"]:
        return self._data_quality_warnings.update(item, warning)

    @api(version="3.5")
    def add_dqw(self, item: TableItem, warning: "DQWItem") -> list["DQWItem"]:
        return self._data_quality_warnings.add(item, warning)

    @api(version="3.5")
    def delete_dqw(self, item: TableItem) -> None:
        self._data_quality_warnings.clear(item)

    @api(version="3.9")
    def add_tags(self, item: Union[TableItem, str], tags: Union[Iterable[str], str]) -> set[str]:
        return super().add_tags(item, tags)

    @api(version="3.9")
    def delete_tags(self, item: Union[TableItem, str], tags: Union[Iterable[str], str]) -> None:
        return super().delete_tags(item, tags)

    def update_tags(self, item: TableItem) -> None:  # type: ignore
        raise NotImplementedError("Update tags is not implemented for TableItem")
