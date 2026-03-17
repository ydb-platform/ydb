from office365.runtime.client_value import ClientValue


class SiteDesignTask(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteDesignTask"
