from typing import Optional

from office365.sharepoint.entity import Entity


class SiteDesignRun(Entity):
    @property
    def site_design_id(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("SiteDesignID", None)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteDesignRun"
