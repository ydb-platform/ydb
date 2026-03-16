from office365.sharepoint.sitedesigns.creation_info import SiteDesignCreationInfo


class SiteDesignMetadata(SiteDesignCreationInfo):
    def __init__(self, order=None, version=None):
        super().__init__()
        self.Order = order
        self.Version = version

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteDesignMetadata"
