from office365.runtime.client_value import ClientValue


class SiteScriptActionStatus(ClientValue):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteScriptActionStatus"
