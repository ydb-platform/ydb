from office365.sharepoint.sitescripts.creation_info import SiteScriptCreationInfo


class SiteScriptUpdateInfo(SiteScriptCreationInfo):
    @property
    def entity_type_name(self):
        return (
            "Microsoft.SharePoint.Utilities.WebTemplateExtensions.SiteScriptUpdateInfo"
        )
