import logging

from tableauserverclient.server.endpoint.endpoint import QuerysetEndpoint, api
from tableauserverclient.server.endpoint.exceptions import MissingRequiredFieldError
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import GroupItem, UserItem, PaginationItem, JobItem
from tableauserverclient.server.pager import Pager

from tableauserverclient.helpers.logging import logger

from typing import Literal, Optional, TYPE_CHECKING, Union, overload
from collections.abc import Iterable

from tableauserverclient.server.query import QuerySet

if TYPE_CHECKING:
    from tableauserverclient.server.request_options import RequestOptions


class Groups(QuerysetEndpoint[GroupItem]):
    """
    Groups endpoint for creating, reading, updating, and deleting groups on
    Tableau Server.
    """

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites/{self.parent_srv.site_id}/groups"

    @api(version="2.0")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[GroupItem], PaginationItem]:
        """
        Returns information about the groups on the site.

        To get information about the users in a group, you must first populate
        the GroupItem with user information using the groups.populate_users
        method.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#query_groups

        Parameters
        ----------
        req_options : Optional[RequestOptions]
            (Optional) You can pass the method a request object that contains
            additional parameters to filter the request. For example, if you
            were searching for a specific group, you could specify the name of
            the group or the group id.

        Returns
        -------
        tuple[list[GroupItem], PaginationItem]

        Examples
        --------
        >>> # import tableauserverclient as TSC
        >>> # tableau_auth = TSC.TableauAuth('USERNAME', 'PASSWORD')
        >>> # server = TSC.Server('https://SERVERURL')

        >>> with server.auth.sign_in(tableau_auth):

        >>>     # get the groups on the server
        >>>     all_groups, pagination_item = server.groups.get()

        >>>     # print the names of the first 100 groups
        >>>     for group in all_groups :
        >>>         print(group.name, group.id)



        """
        logger.info("Querying all groups on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_group_items = GroupItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_group_items, pagination_item

    @api(version="2.0")
    def populate_users(self, group_item: GroupItem, req_options: Optional["RequestOptions"] = None) -> None:
        """
        Populates the group_item with the list of users.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#get_users_in_group

        Parameters
        ----------
        group_item : GroupItem
            The group item to populate with user information.

        req_options : Optional[RequestOptions]
            (Optional) You can pass the method a request object that contains
            page size and page number.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the group item does not have an ID, the method raises an error.

        Examples
        --------
        >>> # Get the group item from the server
        >>> groups, pagination_item = server.groups.get()
        >>> group = groups[1]

        >>> # Populate the group with user information
        >>> server.groups.populate_users(group)
        >>> for user in group.users:
        >>>     print(user.name)


        """
        if not group_item.id:
            error = "Group item missing ID. Group must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        # Define an inner function that we bind to the model_item's `.user` property.

        def user_pager():
            return Pager(
                lambda options: self._get_users_for_group(group_item, options),
                req_options,
            )

        group_item._set_users(user_pager)

    def _get_users_for_group(
        self, group_item: GroupItem, req_options: Optional["RequestOptions"] = None
    ) -> tuple[list[UserItem], PaginationItem]:
        url = f"{self.baseurl}/{group_item.id}/users"
        server_response = self.get_request(url, req_options)
        user_item = UserItem.from_response(server_response.content, self.parent_srv.namespace)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        logger.info(f"Populated users for group (ID: {group_item.id})")
        return user_item, pagination_item

    @api(version="2.0")
    def delete(self, group_id: str) -> None:
        """
        Deletes the group on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#delete_group

        Parameters
        ----------
        group_id: str
            The id for the group you want to remove from the server

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the group_id is not provided, the method raises an error.

        Examples
        --------
        >>> groups, pagination_item = server.groups.get()
        >>> group = groups[1]
        >>> server.groups.delete(group.id)

        """
        if not group_id:
            error = "Group ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{group_id}"
        self.delete_request(url)
        logger.info(f"Deleted single group (ID: {group_id})")

    @overload
    def update(self, group_item: GroupItem, as_job: Literal[False]) -> GroupItem: ...

    @overload
    def update(self, group_item: GroupItem, as_job: Literal[True]) -> JobItem: ...

    @api(version="2.0")
    def update(self, group_item, as_job=False):
        """
        Updates a group on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#update_group

        Parameters
        ----------
        group_item : GroupItem
            The group item to update.

        as_job : bool
            (Optional) If this value is set to True, the update operation will
            be asynchronous and return a JobItem. This is only supported for
            Active Directory groups. By default, this value is set to False.

        Returns
        -------
        Union[GroupItem, JobItem]

        Raises
        ------
        MissingRequiredFieldError
            If the group_item does not have an ID, the method raises an error.

        ValueError
            If the group_item is a local group and as_job is set to True, the
            method raises an error.
        """
        url = f"{self.baseurl}/{group_item.id}"

        if not group_item.id:
            error = "Group item missing ID."
            raise MissingRequiredFieldError(error)
        if as_job and (group_item.domain_name is None or group_item.domain_name == "local"):
            error = "Local groups cannot be updated asynchronously."
            raise ValueError(error)
        elif as_job:
            url = "?".join([url, "asJob=True"])

        update_req = RequestFactory.Group.update_req(group_item)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated group item (ID: {group_item.id})")
        if as_job:
            return JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        else:
            return GroupItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.0")
    def create(self, group_item: GroupItem) -> GroupItem:
        """
        Create a 'local' Tableau group

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#create_group

        Parameters
        ----------
        group_item : GroupItem
            The group item to create. The group_item specifies the group to add.
            You first create a new instance of a GroupItem and pass that to this
            method.

        Returns
        -------
        GroupItem

        Examples
        --------
        >>> new_group = TSC.GroupItem('new_group')
        >>> new_group.minimum_site_role = TSC.UserItem.Role.ExplorerCanPublish
        >>> new_group = server.groups.create(new_group)

        """
        url = self.baseurl
        create_req = RequestFactory.Group.create_local_req(group_item)
        server_response = self.post_request(url, create_req)
        return GroupItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @overload
    def create_AD_group(self, group_item: GroupItem, asJob: Literal[False]) -> GroupItem: ...

    @overload
    def create_AD_group(self, group_item: GroupItem, asJob: Literal[True]) -> JobItem: ...

    @api(version="2.0")
    def create_AD_group(self, group_item, asJob=False):
        """
        Create a group based on Active Directory.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#create_group

        Parameters
        ----------
        group_item : GroupItem
            The group item to create. The group_item specifies the group to add.
            You first create a new instance of a GroupItem and pass that to this
            method.

        asJob : bool
            (Optional) If this value is set to True, the create operation will
            be asynchronous and return a JobItem. This is only supported for
            Active Directory groups. By default, this value is set to False.

        Returns
        -------
        Union[GroupItem, JobItem]

        Examples
        --------
        >>> new_ad_group = TSC.GroupItem('new_ad_group')
        >>> new_ad_group.domain_name = 'example.com'
        >>> new_ad_group.minimum_site_role = TSC.UserItem.Role.ExplorerCanPublish
        >>> new_ad_group.license_mode = TSC.GroupItem.LicenseMode.onSync
        >>> new_ad_group = server.groups.create_AD_group(new_ad_group)
        """
        asJobparameter = "?asJob=true" if asJob else ""
        url = self.baseurl + asJobparameter
        create_req = RequestFactory.Group.create_ad_req(group_item)
        server_response = self.post_request(url, create_req)
        if asJob:
            return JobItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        else:
            return GroupItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    @api(version="2.0")
    def remove_user(self, group_item: GroupItem, user_id: str) -> None:
        """
        Removes 1 user from 1 group

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#remove_user_to_group

        Parameters
        ----------
        group_item : GroupItem
            The group item from which to remove the user.

        user_id : str
            The ID of the user to remove from the group.

        Returns
        -------
        None

        Raises
        ------
        MissingRequiredFieldError
            If the group_item does not have an ID, the method raises an error.

        ValueError
            If the user_id is not provided, the method raises an error.

        Examples
        --------
        >>> group = server.groups.get_by_id('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        >>> server.groups.populate_users(group)
        >>> server.groups.remove_user(group, group.users[0].id)
        """
        if not group_item.id:
            error = "Group item missing ID."
            raise MissingRequiredFieldError(error)
        if not user_id:
            error = "User ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{group_item.id}/users/{user_id}"
        self.delete_request(url)
        logger.info(f"Removed user (id: {user_id}) from group (ID: {group_item.id})")

    @api(version="3.21")
    def remove_users(self, group_item: GroupItem, users: Iterable[Union[str, UserItem]]) -> None:
        """
        Removes multiple users from 1 group. This makes a single API call to
        remove the provided users.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#remove_users_to_group

        Parameters
        ----------
        group_item : GroupItem
            The group item from which to remove the user.

        users : Iterable[Union[str, UserItem]]
            The IDs or UserItems with IDs of the users to remove from the group.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the group_item is not a GroupItem or str, the method raises an error.

        Examples
        --------
        >>> group = server.groups.get_by_id('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        >>> server.groups.populate_users(group)
        >>> users = [u for u in group.users if u.domain_name == 'example.com']
        >>> server.groups.remove_users(group, users)

        """
        group_id = group_item.id if hasattr(group_item, "id") else group_item
        if not isinstance(group_id, str):
            raise ValueError(f"Invalid group provided: {group_item}")

        url = f"{self.baseurl}/{group_id}/users/remove"
        add_req = RequestFactory.Group.remove_users_req(users)
        _ = self.put_request(url, add_req)
        logger.info(f"Removed users to group (ID: {group_item.id})")
        return None

    @api(version="2.0")
    def add_user(self, group_item: GroupItem, user_id: str) -> UserItem:
        """
        Adds 1 user to 1 group

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#add_user_to_group

        Parameters
        ----------
        group_item : GroupItem
            The group item to which to add the user.

        user_id : str
            The ID of the user to add to the group.

        Returns
        -------
        UserItem
            UserItem for the user that was added to the group.

        Raises
        ------
        MissingRequiredFieldError
            If the group_item does not have an ID, the method raises an error.

        ValueError
            If the user_id is not provided, the method raises an error.

        Examples
        --------
        >>> group = server.groups.get_by_id('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        >>> server.groups.add_user(group, '1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        """
        if not group_item.id:
            error = "Group item missing ID."
            raise MissingRequiredFieldError(error)
        if not user_id:
            error = "User ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{group_item.id}/users"
        add_req = RequestFactory.Group.add_user_req(user_id)
        server_response = self.post_request(url, add_req)
        user = UserItem.from_response(server_response.content, self.parent_srv.namespace).pop()
        logger.info(f"Added user (id: {user_id}) to group (ID: {group_item.id})")
        return user

    @api(version="3.21")
    def add_users(self, group_item: GroupItem, users: Iterable[Union[str, UserItem]]) -> list[UserItem]:
        """
        Adds 1 or more user to 1 group

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#add_user_to_group

        Parameters
        ----------
        group_item : GroupItem
            The group item to which to add the user.

        user_id : Iterable[Union[str, UserItem]]
            User IDs or UserItems with IDs to add to the group.

        Returns
        -------
        list[UserItem]
            UserItem for the user that was added to the group.

        Raises
        ------
        MissingRequiredFieldError
            If the group_item does not have an ID, the method raises an error.

        ValueError
            If the user_id is not provided, the method raises an error.

        Examples
        --------
        >>> group = server.groups.get_by_id('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        >>> added_users = server.groups.add_users(group, '1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        """
        """Adds multiple users to 1 group"""
        group_id = group_item.id if hasattr(group_item, "id") else group_item
        if not isinstance(group_id, str):
            raise ValueError(f"Invalid group provided: {group_item}")

        url = f"{self.baseurl}/{group_id}/users"
        add_req = RequestFactory.Group.add_users_req(users)
        server_response = self.post_request(url, add_req)
        users = UserItem.from_response(server_response.content, self.parent_srv.namespace)
        logger.info(f"Added users to group (ID: {group_item.id})")
        return users

    def filter(self, *invalid, page_size: Optional[int] = None, **kwargs) -> QuerySet[GroupItem]:
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


        domain_name=...
        domain_name__in=...
        domain_nickname=...
        domain_nickname__in=...
        is_external_user_enabled=...
        is_local=...
        luid=...
        luid__in=...
        minimum_site_role=...
        minimum_site_role__in=...
        name__cieq=...
        name=...
        name__in=...
        name__like=...
        user_count=...
        user_count__gt=...
        user_count__gte=...
        user_count__lt=...
        user_count__lte=...
        """

        return super().filter(*invalid, page_size=page_size, **kwargs)
