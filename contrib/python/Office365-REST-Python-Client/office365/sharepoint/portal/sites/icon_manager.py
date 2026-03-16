from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class SiteIconManager(Entity):
    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath("SiteIconManager")
        super(SiteIconManager, self).__init__(context, resource_path)

    def get_site_logo(self, site_url, target=None, _type=None, return_type=None):
        payload = {"siteUrl": site_url, "target": target, "type": _type}
        if return_type is None:
            return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "GetSiteLogo", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_site_logo(
        self,
        relative_logo_url,
        _type=None,
        aspect=None,
        focalx=None,
        focaly=None,
        isFocalPatch=None,
    ):
        payload = {
            "relativeLogoUrl": relative_logo_url,
            "type": _type,
            "aspect": aspect,
            "focalx": focalx,
            "focaly": focaly,
            "isFocalPatch": isFocalPatch,
        }
        qry = ServiceOperationQuery(self, "SetSiteLogo", None, payload)
        self.context.add_query(qry)
        return self

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Portal.SiteIconManager"
