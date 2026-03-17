from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.outlook.category import OutlookCategory
from office365.outlook.locale_info import LocaleInfo
from office365.outlook.timezone_information import TimeZoneInformation
from office365.runtime.client_result import ClientResult
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.function import FunctionQuery


class OutlookUser(Entity):
    """Represents the Outlook services available to a user."""

    def supported_languages(self):
        """
        Get the list of locales and languages that are supported for the user, as configured on the user's
        mailbox server. When setting up an Outlook client, the user selects the preferred language from this supported
        list. You can subsequently get the preferred language by getting the user's mailbox settings.
        """
        return_type = ClientResult(self.context, ClientValueCollection(LocaleInfo))
        qry = FunctionQuery(self, "supportedLanguages", None, return_type)
        self.context.add_query(qry)
        return return_type

    def supported_time_zones(self):
        """
        Get the list of time zones that are supported for the user, as configured on the user's mailbox server.
        You can explicitly specify to have time zones returned in the Windows time zone format or
        Internet Assigned Numbers Authority (IANA) time zone (also known as Olson time zone) format.
        The Windows format is the default.
        When setting up an Outlook client, the user selects the preferred time zone from this supported list.
        You can subsequently get the preferred time zone by getting the user's mailbox settings.
        """
        return_type = ClientResult(
            self.context, ClientValueCollection(TimeZoneInformation)
        )
        qry = FunctionQuery(self, "supportedTimeZones", None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def master_categories(self):
        # type: () -> EntityCollection[OutlookCategory]
        """A list of categories defined for the user."""
        return self.properties.get(
            "masterCategories",
            EntityCollection(
                self.context,
                OutlookCategory,
                ResourcePath("masterCategories", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {"masterCategories": self.master_categories}
            default_value = property_mapping.get(name, None)
        return super(OutlookUser, self).get_property(name, default_value)
