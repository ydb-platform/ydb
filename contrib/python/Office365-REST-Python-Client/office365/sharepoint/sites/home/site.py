from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.sites.home.reference import SPHSiteReference


class SPHSite(Entity):
    def __init__(self, context, resource_path=None):
        """
        A home site represents a SharePoint communication site.
        It brings together news, events, embedded video and conversations, and other resources to deliver an engaging
        experience that reflects your organization's voice, priorities, and brand.
        It also allows your users to search for content (such as sites, news, and files) across your organization
        """
        if resource_path is None:
            resource_path = ResourcePath("SP.SPHSite")
        super(SPHSite, self).__init__(context, resource_path)

    def details(self):
        return_type = ClientResult(self.context, SPHSiteReference())
        qry = ServiceOperationQuery(self, "Details", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    @staticmethod
    def is_comm_site(context, site_url, return_value=None):
        """
        Determines whether a site is a communication site

        :param office365.sharepoint.client_context.ClientContext context:
        :param str site_url: URL of the site to return status for
        :param ClientResult return_value:
        """
        if return_value is None:
            return_value = ClientResult(context)
        params = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            SPHSite(context), "IsCommSite", params, None, None, return_value, True
        )
        context.add_query(qry)
        return return_value

    @staticmethod
    def is_modern_site_with_horizontal_nav(context, site_url, return_type=None):
        """
        Determines whether a site is a modern site with horizontal navigation

        :param office365.sharepoint.client_context.ClientContext context:
        :param str site_url: URL of the site to return status for
        :param ClientResult return_type: Return value
        """
        if return_type is None:
            return_type = ClientResult(context)
        params = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            SPHSite(context),
            "IsModernSiteWithHorizontalNav",
            params,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def is_valid_home_site(context, site_url, return_value=None):
        """
        Determines whether a site is landing site for your intranet.

        :param office365.sharepoint.client_context.ClientContext context:
        :param str site_url: URL of the site to return status for
        :param ClientResult return_value:
        """

        if return_value is None:
            return_value = ClientResult(context)
        sph = SPHSite(context)
        params = {"siteUrl": site_url}
        qry = ServiceOperationQuery(
            sph, "IsValidHomeSite", params, None, None, return_value
        )
        qry.static = True
        context.add_query(qry)
        return return_value

    @staticmethod
    def validate_home_site(context, site_url, validation_action_type):
        """

        :param office365.sharepoint.client_context.ClientContext context:
        :param str site_url: URL of the site to return status for
        :param int validation_action_type:
        """
        sph = SPHSite(context)
        params = {"siteUrl": site_url, "validationActionType": validation_action_type}
        qry = ServiceOperationQuery(
            sph, "ValidateHomeSite", params, None, None, None, True
        )
        context.add_query(qry)
        return sph

    @staticmethod
    def set_as_home_site(
        context, site_url, viva_connections_default_start=None, return_value=None
    ):
        """
        Sets a site as a landing site for your intranet.

        :param ClientResult return_value:
        :param office365.sharepoint.client_context.ClientContext context:
        :param str site_url:
        :param bool viva_connections_default_start:
        """

        if return_value is None:
            return_value = ClientResult(context)
        sph = SPHSite(context)
        params = {
            "siteUrl": site_url,
            "vivaConnectionsDefaultStart": viva_connections_default_start,
        }
        qry = ServiceOperationQuery(sph, "SetSPHSite", None, params, None, return_value)
        context.add_query(qry)
        return return_value
