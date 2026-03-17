import logging

from tableauserverclient.server.endpoint.default_permissions_endpoint import _DefaultPermissionsEndpoint
from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api, XML_CONTENT_TYPE
from tableauserverclient.server.endpoint.exceptions import MissingRequiredFieldError
from tableauserverclient.server.endpoint.permissions_endpoint import _PermissionsEndpoint
from tableauserverclient.server import RequestFactory, RequestOptions
from tableauserverclient.models.permissions_item import PermissionsRule
from tableauserverclient.models import ProjectItem, PaginationItem, Resource

from typing import Optional, TYPE_CHECKING

from tableauserverclient.server.query import QuerySet

if TYPE_CHECKING:
    from tableauserverclient.server.server import Server
    from tableauserverclient.server.request_options import RequestOptions

from tableauserverclient.helpers.logging import logger


class Projects(QuerysetEndpoint[ProjectItem]):
    """
    The project methods are based upon the endpoints for projects in the REST
    API and operate on the ProjectItem class.
    """

    def __init__(self, parent_srv: "Server") -> None:
        super().__init__(parent_srv)

        self._permissions = _PermissionsEndpoint(parent_srv, lambda: self.baseurl)
        self._default_permissions = _DefaultPermissionsEndpoint(parent_srv, lambda: self.baseurl)

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/projects"

    @api(version="2.0")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[ProjectItem], PaginationItem]:
        """
        Retrieves all projects on the site. The endpoint is paginated and can
        be filtered using the req_options parameter.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_projects.htm#query_projects

        Parameters
        ----------
        req_options : RequestOptions | None, default None
            The request options to filter the projects. The default is None.

        Returns
        -------
        tuple[list[ProjectItem], PaginationItem]
            Returns a tuple containing a list of ProjectItem objects and a
            PaginationItem object.
        """
        logger.info("Querying all projects on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_project_items = ProjectItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_project_items, pagination_item

    @api(version="2.0")
    def delete(self, project_id: str) -> None:
        """
        Deletes a single project on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_projects.htm#delete_project

        Parameters
        ----------
        project_id : str
            The unique identifier for the project.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the project ID is not defined, an error is raised.
        """
        if not project_id:
            error = "Project ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{project_id}"
        self.delete_request(url)
        logger.info(f"Deleted single project (ID: {project_id})")

    @api(version="2.0")
    def get_by_id(self, project_id: str) -> ProjectItem:
        """
        Fetch a project by ID. This is a convenience method making up for a gap in the server API.
        It uses the same endpoint as the update method, but without the ability to update the project.
        """
        if not project_id:
            error = "Project ID undefined."
            raise ValueError(error)
        project = ProjectItem(id=project_id)
        return self.update(project, samples=False)

    @api(version="2.0")
    def update(self, project_item: ProjectItem, samples: bool = False) -> ProjectItem:
        """
        Modify the project settings.

        You can use this method to update the project name, the project
        description, or the project permissions. To specify the site, create a
        TableauAuth instance using the content URL for the site (site_id), and
        sign in to that site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_projects.htm#update_project

        Parameters
        ----------
        project_item : ProjectItem
            The project item object must include the project ID. The values in
            the project item override the current project settings.

        samples : bool
            Set to True to include sample workbooks and data sources in the
            project. The default is False.

        Returns
        -------
        ProjectItem
            Returns the updated project item.

        Raises
        ------
        MissingRequiredFieldError
            If the project item is missing the ID, an error is raised.
        """
        if not project_item.id:
            error = "Project item missing ID."
            raise MissingRequiredFieldError(error)

        params = {"params": {RequestOptions.Field.PublishSamples: samples}}
        url = f"{self.baseurl}/{project_item.id}"
        update_req = RequestFactory.Project.update_req(project_item)
        server_response = self.put_request(url, update_req, XML_CONTENT_TYPE, params)
        logger.info(f"Updated project item (ID: {project_item.id})")
        updated_project = ProjectItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        return updated_project

    @api(version="2.0")
    def create(self, project_item: ProjectItem, samples: bool = False) -> ProjectItem:
        """
        Creates a project on the specified site.

        To create a project, you first create a new instance of a ProjectItem
        and pass it to the create method. To specify the site to create the new
        project, create a TableauAuth instance using the content URL for the
        site (site_id), and sign in to that site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_projects.htm#create_project

        Parameters
        ----------
        project_item : ProjectItem
            Specifies the properties for the project. The project_item is the
            request package. To create the request package, create a new
            instance of ProjectItem.

        samples : bool
            Set to True to include sample workbooks and data sources in the
            project. The default is False.

        Returns
        -------
        ProjectItem
            Returns the new project item.
        """
        params = {"params": {RequestOptions.Field.PublishSamples: samples}}
        url = self.baseurl
        if project_item._samples:
            url = f"{self.baseurl}?publishSamples={project_item._samples}"
        create_req = RequestFactory.Project.create_req(project_item)
        server_response = self.post_request(url, create_req, XML_CONTENT_TYPE, params)
        new_project = ProjectItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info(f"Created new project (ID: {new_project.id})")
        return new_project

    @api(version="2.0")
    def populate_permissions(self, item: ProjectItem) -> None:
        """
        Queries the project permissions, parses and stores the returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_project_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with permissions.

        Returns
        -------
        None
        """
        self._permissions.populate(item)

    @api(version="2.0")
    def update_permissions(self, item: ProjectItem, rules: list[PermissionsRule]) -> list[PermissionsRule]:
        """
        Updates the permissions for the specified project item. The rules
        provided are expected to be a complete list of the permissions for the
        project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """

        return self._permissions.update(item, rules)

    @api(version="2.0")
    def delete_permission(self, item: ProjectItem, rules: list[PermissionsRule]) -> None:
        """
        Deletes the specified permissions from the project item.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_project_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete permissions from.

        rules : list[PermissionsRule]
            The list of permissions rules to delete from the project.

        Returns
        -------
        None
        """
        self._permissions.delete(item, rules)

    @api(version="2.1")
    def populate_workbook_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default workbook permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default workbook permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Workbook)

    @api(version="2.1")
    def populate_datasource_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default datasource permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default datasource permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Datasource)

    @api(version="3.2")
    def populate_metric_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default metric permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default metric permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Metric)

    @api(version="3.4")
    def populate_datarole_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default datarole permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default datarole permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Datarole)

    @api(version="3.4")
    def populate_flow_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default flow permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default flow permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Flow)

    @api(version="3.4")
    def populate_lens_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default lens permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default lens permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Lens)

    @api(version="3.23")
    def populate_virtualconnection_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default virtualconnections permissions, parses and stores
        the returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default virtual connection
            permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.VirtualConnection)

    @api(version="3.23")
    def populate_database_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default database permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default database permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Database)

    @api(version="3.23")
    def populate_table_default_permissions(self, item: ProjectItem) -> None:
        """
        Queries the default table permissions, parses and stores the
        returned the permissions.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#query_default_permissions

        Parameters
        ----------
        item : ProjectItem
            The project item to populate with default table permissions.

        Returns
        -------
        None
        """
        self._default_permissions.populate_default_permissions(item, Resource.Table)

    @api(version="2.1")
    def update_workbook_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default workbook permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default workbook permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Workbook)

    @api(version="2.1")
    def update_datasource_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default datasource permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default datasource permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Datasource)

    @api(version="3.2")
    def update_metric_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default metric permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default metric permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Metric)

    @api(version="3.4")
    def update_datarole_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default datarole permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default datarole permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Datarole)

    @api(version="3.4")
    def update_flow_default_permissions(self, item: ProjectItem, rules: list[PermissionsRule]) -> list[PermissionsRule]:
        """
        Add or updates the default flow permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default flow permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Flow)

    @api(version="3.4")
    def update_lens_default_permissions(self, item: ProjectItem, rules: list[PermissionsRule]) -> list[PermissionsRule]:
        """
        Add or updates the default lens permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default lens permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Lens)

    @api(version="3.23")
    def update_virtualconnection_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default virtualconnection permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default virtualconnection permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.VirtualConnection)

    @api(version="3.23")
    def update_database_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default database permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default database permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Database)

    @api(version="3.23")
    def update_table_default_permissions(
        self, item: ProjectItem, rules: list[PermissionsRule]
    ) -> list[PermissionsRule]:
        """
        Add or updates the default table permissions for the specified.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#replace_default_permissions_for_content

        Parameters
        ----------
        item : ProjectItem
            The project item to update default table permissions for.

        rules : list[PermissionsRule]
            The list of permissions rules to update the project with.

        Returns
        -------
        list[PermissionsRule]
            Returns the updated list of permissions rules.
        """
        return self._default_permissions.update_default_permissions(item, rules, Resource.Table)

    @api(version="2.1")
    def delete_workbook_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default workbook permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Workbook)

    @api(version="2.1")
    def delete_datasource_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default datasource permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Datasource)

    @api(version="3.2")
    def delete_metric_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default workbook permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Metric)

    @api(version="3.4")
    def delete_datarole_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default datarole permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Datarole)

    @api(version="3.4")
    def delete_flow_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default flow permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Flow)

    @api(version="3.4")
    def delete_lens_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default lens permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Lens)

    @api(version="3.23")
    def delete_virtualconnection_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default virtualconnection permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.VirtualConnection)

    @api(version="3.23")
    def delete_database_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default database permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Database)

    @api(version="3.23")
    def delete_table_default_permissions(self, item: ProjectItem, rule: PermissionsRule) -> None:
        """
        Deletes the specified default permission rule from the project.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_permissions.htm#delete_default_permission

        Parameters
        ----------
        item : ProjectItem
            The project item to delete default table permissions from.

        rule : PermissionsRule
            The permissions rule to delete from the project.

        Returns
        -------
        None
        """
        self._default_permissions.delete_default_permission(item, rule, Resource.Table)

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> QuerySet[ProjectItem]:
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
        name=...
        name__in=...
        owner_domain=...
        owner_domain__in=...
        owner_email=...
        owner_email__in=...
        owner_name=...
        owner_name__in=...
        parent_project_id=...
        parent_project_id__in=...
        top_level_project=...
        updated_at=...
        updated_at__gt=...
        updated_at__gte=...
        updated_at__lt=...
        updated_at__lte=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
