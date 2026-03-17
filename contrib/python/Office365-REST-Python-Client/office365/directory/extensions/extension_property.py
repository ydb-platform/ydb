# coding=utf-8
from typing import Optional

from office365.directory.object import DirectoryObject
from office365.runtime.types.collections import StringCollection


class ExtensionProperty(DirectoryObject):
    """
    Represents a directory extension that can be used to add a custom property to directory objects without
    requiring an external data store. For example, if an organization has a line of business (LOB) application
    that requires a Skype ID for each user in the directory, Microsoft Graph can be used to register a new property
    named skypeId on the directory’s User object, and then write a value to the new property for a specific user.
    """

    @property
    def name(self):
        # type: () -> Optional[str]
        """Name of the extension property"""
        return self.properties.get("name", None)

    @property
    def app_display_name(self):
        # type: () -> Optional[str]
        """Display name of the application object on which this extension property is defined. Read-only"""
        return self.properties.get("appDisplayName", None)

    @property
    def data_type(self):
        # type: () -> Optional[str]
        """
        Specifies the data type of the value the extension property can hold. Following values are supported.
            Binary - 256 bytes maximum
            Boolean
            DateTime - Must be specified in ISO 8601 format. Will be stored in UTC.
            Integer - 32-bit value.
            LargeInteger - 64-bit value.
            String - 256 characters maximum
        """
        return self.properties.get("dataType", None)

    @property
    def target_objects(self):
        """
        Following values are supported. Not nullable.
        User
        Group
        Organization
        Device
        Application
        """
        return self.properties.get("targetObjects", StringCollection())
