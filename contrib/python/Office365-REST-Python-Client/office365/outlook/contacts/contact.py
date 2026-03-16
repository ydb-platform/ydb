from typing import Optional

from office365.directory.extensions.extended_property import (
    MultiValueLegacyExtendedProperty,
    SingleValueLegacyExtendedProperty,
)
from office365.directory.extensions.extension import Extension
from office365.directory.profile_photo import ProfilePhoto
from office365.entity_collection import EntityCollection
from office365.outlook.calendar.email_address import EmailAddress
from office365.outlook.item import OutlookItem
from office365.outlook.mail.physical_address import PhysicalAddress
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.types.collections import StringCollection


class Contact(OutlookItem):
    """User's contact."""

    @property
    def business_phones(self):
        """The contact's business phone numbers."""
        return self.properties.setdefault("businessPhones", StringCollection())

    @property
    def display_name(self):
        # type: () -> Optional[str]
        """
        The contact's display name. You can specify the display name in a create or update operation.
        Note that later updates to other properties may cause an automatically generated value to overwrite the
        displayName value you have specified. To preserve a pre-existing value, always include it as displayName
        in an update operation.
        """
        return self.properties.get("displayName", None)

    @property
    def manager(self):
        # type: () -> Optional[str]
        """
        The name of the contact's manager.
        """
        return self.properties.get("manager", None)

    @manager.setter
    def manager(self, value):
        # type: (str) -> None
        """Sets name of the contact's manager."""
        self.set_property("manager", value)

    @property
    def mobile_phone(self):
        # type: () -> Optional[str]
        """The contact's mobile phone number."""
        return self.properties.get("mobilePhone", None)

    @mobile_phone.setter
    def mobile_phone(self, value):
        # type: (str) -> None
        """Sets contact's mobile phone number."""
        self.set_property("mobilePhone", value)

    @property
    def home_address(self):
        """The contact's home address."""
        return self.properties.get("homeAddress", PhysicalAddress())

    @property
    def email_addresses(self):
        # type: () -> ClientValueCollection[EmailAddress]
        """The contact's email addresses."""
        return self.properties.setdefault(
            "emailAddresses", ClientValueCollection(EmailAddress)
        )

    @property
    def extensions(self):
        # type: () -> EntityCollection[Extension]
        """The collection of open extensions defined for the contact. Nullable."""
        return self.properties.get(
            "extensions",
            EntityCollection(
                self.context, Extension, ResourcePath("extensions", self.resource_path)
            ),
        )

    @property
    def photo(self):
        """Optional contact picture. You can get or set a photo for a contact."""
        return self.properties.get(
            "photo",
            ProfilePhoto(self.context, ResourcePath("photo", self.resource_path)),
        )

    @property
    def multi_value_extended_properties(self):
        # type: () -> EntityCollection[MultiValueLegacyExtendedProperty]
        """The collection of multi-value extended properties defined for the Contact."""
        return self.properties.get(
            "multiValueExtendedProperties",
            EntityCollection(
                self.context,
                MultiValueLegacyExtendedProperty,
                ResourcePath("multiValueExtendedProperties", self.resource_path),
            ),
        )

    @property
    def single_value_extended_properties(self):
        # type: () -> EntityCollection[SingleValueLegacyExtendedProperty]
        """The collection of single-value extended properties defined for the Contact."""
        return self.properties.get(
            "singleValueExtendedProperties",
            EntityCollection(
                self.context,
                SingleValueLegacyExtendedProperty,
                ResourcePath("singleValueExtendedProperties", self.resource_path),
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "businessPhones": self.business_phones,
                "emailAddresses": self.email_addresses,
                "homeAddress": self.home_address,
                "multiValueExtendedProperties": self.multi_value_extended_properties,
                "singleValueExtendedProperties": self.single_value_extended_properties,
            }
            default_value = property_mapping.get(name, None)
        return super(Contact, self).get_property(name, default_value)
