from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.brandcenter.configuration import BrandCenterConfiguration
from office365.sharepoint.entity import Entity
from office365.sharepoint.sites.themes import SiteThemes


class BrandCenter(Entity):
    """
    The SharePoint brand center API offers a centralized branding management application
    that empowers your brand managers or designated brand owners to help your organization to customize
    the look and feel of their experiences.
    """

    def __init__(self, context):
        super(BrandCenter, self).__init__(context, ResourcePath("SP.BrandCenter"))

    def configuration(self):
        """ """
        return_type = ClientResult(self.context, BrandCenterConfiguration())
        qry = ServiceOperationQuery(
            self, "Configuration", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_site_themes(self):
        """ """
        return_type = ClientResult(self.context, SiteThemes())
        qry = ServiceOperationQuery(
            self, "GetSiteThemes", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type
