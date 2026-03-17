from office365.sharepoint.entity import Entity
from office365.sharepoint.ui.applicationpages.peoplepicker.query_settings import (
    PeoplePickerQuerySettings,
)


class PickerSettings(Entity):
    """
    This class contains configuration settings for the client people picker control hosted
    by the SharePoint sharing UI.
    """

    @property
    def allow_email_addresses(self):
        """
        Boolean value indicating whether the picker control will allow the resolution of arbitrary email addresses.
        """
        return self.properties.get("AllowEmailAddresses", None)

    @property
    def allow_only_email_addresses(self):
        """
        Boolean value indicating whether the picker control will only allow the resolution of email addresses.
        """
        return self.properties.get("AllowOnlyEmailAddresses", None)

    @property
    def query_settings(self):
        """
        The query settings to be used by the picker control.
        """
        return self.properties.get("QuerySettings", PeoplePickerQuerySettings())
