from office365.runtime.client_result import ClientResult
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.portal.sites.creation_request import SPSiteCreationRequest
from office365.sharepoint.portal.sites.creation_response import SPSiteCreationResponse
from office365.sharepoint.teams.site_owner_response import (
    GetTeamChannelSiteOwnerResponse,
)
from office365.sharepoint.viva.site_request_info import VivaSiteRequestInfo


class SPSiteManager(Entity):
    """Provides REST methods for creating and managing SharePoint sites."""

    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath("SPSiteManager")
        super(SPSiteManager, self).__init__(context, resource_path)

    def create(self, title, site_url, owner=None):
        """
        When executing this method server MUST create a SharePoint site according to the parameters passed in the
        SPSiteCreationRequest and return the information about the site it created in the format of a
        SPSiteCreationResponse.

        :param str title: Site title
        :param str site_url: Site url
        :param str or office365.sharepoint.principal.user.User owner: Site owner object or principal name
        """
        return_type = ClientResult(self.context, SPSiteCreationResponse())

        def _create(owner_string=None):
            request = SPSiteCreationRequest(title, site_url, owner_string)
            payload = {"request": request}
            qry = ServiceOperationQuery(
                self, "Create", None, payload, None, return_type
            )
            self.context.add_query(qry)

        from office365.sharepoint.principal.users.user import User

        if isinstance(owner, User):

            def _owner_loaded():
                _create(owner.user_principal_name)

            owner.ensure_property("UserPrincipalName", _owner_loaded)
        else:
            _create(owner)
        return return_type

    def delete(self, site_id):
        """When executing this method server MUST put the SharePoint site into recycle bin according to
        the parameter passed in the siteId, if the SharePoint site of giving siteId exists and the site has
        no attached AD group.

        :param str site_id: The GUID to uniquely identify a SharePoint site.
        """
        payload = {"siteId": site_id}
        qry = ServiceOperationQuery(self, "Delete", None, payload)
        self.context.add_query(qry)
        return self

    def get_status(self, site_url):
        """When executing this method server SHOULD return a SharePoint site status in the format
        of a SPSiteCreationResponse according to the parameter passed in the url.

        :param str site_url: URL of the site to return status for
        """
        response = ClientResult(self.context, SPSiteCreationResponse())
        qry = ServiceOperationQuery(
            self, "Status", None, {"url": site_url}, None, response
        )

        def _construct_request(request):
            # type: (RequestOptions) -> None
            request.method = HttpMethod.Get
            request.url += "?url='{0}'".format(site_url)

        self.context.add_query(qry).before_query_execute(_construct_request)
        return response

    def get_site_url(self, site_id):
        """
        :param str site_id: The GUID to uniquely identify a SharePoint site.
        """
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(
            self, "SiteUrl", None, {"siteId": site_id}, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_team_channel_site_owner(self, site_id):
        """
        :param str site_id: The GUID to uniquely identify a SharePoint site.
        """
        return_type = ClientResult(self.context, GetTeamChannelSiteOwnerResponse())
        qry = ServiceOperationQuery(
            self,
            "GetTeamChannelSiteOwner",
            None,
            {"siteId": site_id},
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def viva_backend_site_url_from_name(self, site_name):
        """
        :param str site_name:
        """
        return_type = ClientResult(self.context, VivaSiteRequestInfo())
        payload = {"siteName": site_name}
        qry = ServiceOperationQuery(
            self, "VivaBackendSiteUrlFromName", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type
