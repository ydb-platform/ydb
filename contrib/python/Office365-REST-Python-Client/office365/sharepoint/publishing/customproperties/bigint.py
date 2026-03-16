from office365.sharepoint.publishing.customproperties.base import BaseCustomProperty


class BigIntCustomProperty(BaseCustomProperty):
    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Publishing.RestOnly.BigIntCustomProperty"
