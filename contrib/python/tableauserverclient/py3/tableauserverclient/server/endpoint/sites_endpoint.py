import copy
import logging

from .endpoint import Endpoint, api
from .exceptions import MissingRequiredFieldError
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import SiteAuthConfiguration, SiteItem, PaginationItem

from tableauserverclient.helpers.logging import logger

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from ..request_options import RequestOptions


class Sites(Endpoint):
    """
    Using the site methods of the Tableau Server REST API you can:

    List sites on a server or get details of a specific site
    Create, update, or delete a site
    List views in a site
    Encrypt, decrypt, or reencrypt extracts on a site

    """

    @property
    def baseurl(self) -> str:
        return f"{self.parent_srv.baseurl}/sites"

    # Gets all sites
    @api(version="2.0")
    def get(self, req_options: Optional["RequestOptions"] = None) -> tuple[list[SiteItem], PaginationItem]:
        """
        Query all sites on the server. This method requires server admin
        permissions. This endpoint is paginated, meaning that the server will
        only return a subset of the data at a time. The response will contain
        information about the total number of sites and the number of sites
        returned in the current response. Use the PaginationItem object to
        request more data.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_sites

        Parameters
        ----------
        req_options : RequestOptions, optional
            Filtering options for the request.

        Returns
        -------
        tuple[list[SiteItem], PaginationItem]
        """
        logger.info("Querying all sites on site")
        logger.info("Requires Server Admin permissions")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_site_items = SiteItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_site_items, pagination_item

    # Gets 1 site by id
    @api(version="2.0")
    def get_by_id(self, site_id: str) -> SiteItem:
        """
        Query a single site on the server. You can only retrieve the site that
        you are currently authenticated for.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_site

        Parameters
        ----------
        site_id : str
            The site ID.

        Returns
        -------
        SiteItem

        Raises
        ------
        ValueError
            If the site ID is not defined.

        ValueError
            If the site ID does not match the site for which you are currently authenticated.

        Examples
        --------
        >>> site = server.sites.get_by_id('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        """
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        if not site_id == self.parent_srv.site_id:
            error = "You can only retrieve the site for which you are currently authenticated."
            raise ValueError(error)

        logger.info(f"Querying single site (ID: {site_id})")
        url = f"{self.baseurl}/{site_id}"
        server_response = self.get_request(url)
        return SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Gets 1 site by name
    @api(version="2.0")
    def get_by_name(self, site_name: str) -> SiteItem:
        """
        Query a single site on the server. You can only retrieve the site that
        you are currently authenticated for.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_site

        Parameters
        ----------
        site_name : str
            The site name.

        Returns
        -------
        SiteItem

        Raises
        ------
        ValueError
            If the site name is not defined.

        Examples
        --------
        >>> site = server.sites.get_by_name('Tableau')

        """
        if not site_name:
            error = "Site Name undefined."
            raise ValueError(error)
        print("Note: You can only work with the site for which you are currently authenticated")
        logger.info(f"Querying single site (Name: {site_name})")
        url = f"{self.baseurl}/{site_name}?key=name"
        print(self.baseurl, url)
        server_response = self.get_request(url)
        return SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Gets 1 site by content url
    @api(version="2.0")
    def get_by_content_url(self, content_url: str) -> SiteItem:
        """
        Query a single site on the server. You can only retrieve the site that
        you are currently authenticated for.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_site

        Parameters
        ----------
        content_url : str
            The content URL.

        Returns
        -------
        SiteItem

        Raises
        ------
        ValueError
            If the site name is not defined.

        Examples
        --------
        >>> site = server.sites.get_by_name('Tableau')

        """
        if content_url is None:
            error = "Content URL undefined."
            raise ValueError(error)
        if not self.parent_srv.baseurl.index(content_url) > 0:
            error = "You can only work with the site you are currently authenticated for"
            raise ValueError(error)

        logger.info(f"Querying single site (Content URL: {content_url})")
        logger.debug("Querying other sites requires Server Admin permissions")
        url = f"{self.baseurl}/{content_url}?key=contentUrl"
        server_response = self.get_request(url)
        return SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]

    # Update site
    @api(version="2.0")
    def update(self, site_item: SiteItem) -> SiteItem:
        """
        Modifies the settings for site.

        The site item object must include the site ID and overrides all other settings.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#update_site

        Parameters
        ----------
        site_item : SiteItem
            The site item that you want to update. The settings specified in the
            site item override the current site settings.

        Returns
        -------
        SiteItem
            The site item object that was updated.

        Raises
        ------
        MissingRequiredFieldError
            If the site item is missing an ID.

        ValueError
            If the site ID does not match the site for which you are currently authenticated.

        ValueError
            If the site admin mode is set to ContentOnly and a user quota is also set.

        Examples
        --------
        >>> ...
        >>> site_item.name = 'New Name'
        >>> updated_site = server.sites.update(site_item)

        """
        if not site_item.id:
            error = "Site item missing ID."
            raise MissingRequiredFieldError(error)
        print(self.parent_srv.site_id, site_item.id)
        if not site_item.id == self.parent_srv.site_id:
            error = "You can only update the site you are currently authenticated for"
            raise ValueError(error)

        if site_item.admin_mode:
            if site_item.admin_mode == SiteItem.AdminMode.ContentOnly and site_item.user_quota:
                error = "You cannot set admin_mode to ContentOnly and also set a user quota"
                raise ValueError(error)

        url = f"{self.baseurl}/{site_item.id}"
        update_req = RequestFactory.Site.update_req(site_item, self.parent_srv)
        server_response = self.put_request(url, update_req)
        logger.info(f"Updated site item (ID: {site_item.id})")
        update_site = copy.copy(site_item)
        return update_site._parse_common_tags(server_response.content, self.parent_srv.namespace)

    # Delete 1 site object
    @api(version="2.0")
    def delete(self, site_id: str) -> None:
        """
        Deletes the specified site from the server. You can only delete the site
        if you are a Server Admin.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#delete_site

        Parameters
        ----------
        site_id : str
            The site ID.

        Raises
        ------
        ValueError
            If the site ID is not defined.

        ValueError
            If the site ID does not match the site for which you are currently authenticated.

        Examples
        --------
        >>> server.sites.delete('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        """
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{site_id}"
        if not site_id == self.parent_srv.site_id:
            error = "You can only delete the site you are currently authenticated for"
            raise ValueError(error)
        self.delete_request(url)
        self.parent_srv._clear_auth()
        logger.info(f"Deleted single site (ID: {site_id}) and signed out")

    # Create new site
    @api(version="2.0")
    def create(self, site_item: SiteItem) -> SiteItem:
        """
        Creates a new site on the server for the specified site item object.

        Tableau Server only.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#create_site

        Parameters
        ----------
        site_item : SiteItem
            The settings for the site that you want to create. You need to
            create an instance of SiteItem and pass it to the create method.

        Returns
        -------
        SiteItem
            The site item object that was created.

        Raises
        ------
        ValueError
            If the site admin mode is set to ContentOnly and a user quota is also set.

        Examples
        --------
        >>> import tableauserverclient as TSC

        >>> # create an instance of server
        >>> server = TSC.Server('https://MY-SERVER')

        >>> # create shortcut for admin mode
        >>> content_users=TSC.SiteItem.AdminMode.ContentAndUsers

        >>> # create a new SiteItem
        >>> new_site = TSC.SiteItem(name='Tableau', content_url='tableau', admin_mode=content_users, user_quota=15, storage_quota=1000, disable_subscriptions=True)

        >>> # call the sites create method with the SiteItem
        >>> new_site = server.sites.create(new_site)


        """
        if site_item.admin_mode:
            if site_item.admin_mode == SiteItem.AdminMode.ContentOnly and site_item.user_quota:
                error = "You cannot set admin_mode to ContentOnly and also set a user quota"
                raise ValueError(error)

        url = self.baseurl
        create_req = RequestFactory.Site.create_req(site_item, self.parent_srv)
        server_response = self.post_request(url, create_req)
        new_site = SiteItem.from_response(server_response.content, self.parent_srv.namespace)[0]
        logger.info(f"Created new site (ID: {new_site.id})")
        return new_site

    @api(version="3.5")
    def encrypt_extracts(self, site_id: str) -> None:
        """
        Encrypts all extracts on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_extract_and_encryption.htm#encrypt_extracts

        Parameters
        ----------
        site_id : str
            The site ID.

        Raises
        ------
        ValueError
            If the site ID is not defined.

        Examples
        --------
        >>> server.sites.encrypt_extracts('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        """
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{site_id}/encrypt-extracts"
        empty_req = RequestFactory.Empty.empty_req()
        self.post_request(url, empty_req)

    @api(version="3.5")
    def decrypt_extracts(self, site_id: str) -> None:
        """
        Decrypts all extracts on the site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_extract_and_encryption.htm#decrypt_extracts

        Parameters
        ----------
        site_id : str
            The site ID.

        Raises
        ------
        ValueError
            If the site ID is not defined.

        Examples
        --------
        >>> server.sites.decrypt_extracts('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')
        """
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{site_id}/decrypt-extracts"
        empty_req = RequestFactory.Empty.empty_req()
        self.post_request(url, empty_req)

    @api(version="3.5")
    def re_encrypt_extracts(self, site_id: str) -> None:
        """
        Reencrypt all extracts on a site with new encryption keys. If no site is
        specified, extracts on the default site will be reencrypted.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_extract_and_encryption.htm#reencrypt_extracts

        Parameters
        ----------
        site_id : str
            The site ID.

        Raises
        ------
        ValueError
            If the site ID is not defined.

        Examples
        --------
        >>> server.sites.re_encrypt_extracts('1a2b3c4d-5e6f-7g8h-9i0j-1k2l3m4n5o6p')

        """
        if not site_id:
            error = "Site ID undefined."
            raise ValueError(error)
        url = f"{self.baseurl}/{site_id}/reencrypt-extracts"

        empty_req = RequestFactory.Empty.empty_req()
        self.post_request(url, empty_req)

    @api(version="3.24")
    def list_auth_configurations(self) -> list[SiteAuthConfiguration]:
        """
        Lists all authentication configurations on the current site.

        REST API: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#list_authentication_configurations_site

        Returns
        -------
        list[SiteAuthConfiguration]
            A list of authentication configurations on the current site.
        """
        url = f"{self.baseurl}/{self.parent_srv.site_id}/site-auth-configurations"
        server_response = self.get_request(url)
        auth_configurations = SiteAuthConfiguration.from_response(server_response.content, self.parent_srv.namespace)
        return auth_configurations
