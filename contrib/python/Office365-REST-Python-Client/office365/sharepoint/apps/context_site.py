from office365.onedrive.sites.site import Site
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity import Entity
from office365.sharepoint.webs.web import Web


class AppContextSite(Entity):
    """ """

    def __init__(self, context, site_url):
        # type: (ClientContext, str) -> None
        """"""
        static_path = ServiceOperationPath(
            "SP.AppContextSite",
            {"siteUrl": site_url},
        )
        super(AppContextSite, self).__init__(context, static_path)

    @property
    def site(self):
        """"""
        return self.properties.get(
            "Site",
            Site(
                self.context,
                ResourcePath("Site", self.resource_path),
            ),
        )

    @property
    def web(self):
        """"""
        return self.properties.get(
            "Web",
            Web(
                self.context,
                ResourcePath("Web", self.resource_path),
            ),
        )
