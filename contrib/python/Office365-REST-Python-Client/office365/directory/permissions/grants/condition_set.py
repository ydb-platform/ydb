from typing import Optional

from office365.entity import Entity
from office365.runtime.types.collections import StringCollection


class PermissionGrantConditionSet(Entity):
    """
    A permission grant condition set is used to specify a matching rule in a permission grant policy to include
    or exclude a permission grant event.

    A permission grant condition set contains several conditions. For an event to match a permission grant condition
    set, all conditions must be met.
    """

    @property
    def client_application_ids(self):
        """
        A list of appId values for the client applications to match with, or a list with the single value all to
        match any client application. Default is the single value all.
        """
        return self.properties.get("clientApplicationIds", StringCollection())

    @property
    def client_application_publisher_ids(self):
        """
        A list of Microsoft Partner Network (MPN) IDs for verified publishers of the client application,  or a list
        with the single value all to match with client apps from any publisher. Default is the single value all.
        """
        return self.properties.get("clientApplicationPublisherIds", StringCollection())

    @property
    def client_applications_from_verified_publisher_only(self):
        # type: () -> Optional[bool]
        """
        Set to true to only match on client applications with a verified publisher. Set to false to match on any client
        app, even if it does not have a verified publisher. Default is false.
        """
        return self.properties.get("clientApplicationsFromVerifiedPublisherOnly", None)

    @property
    def permissions(self):
        """
        The list of id values for the specific permissions to match with, or a list with the single value all to
        match with any permission. The id of delegated permissions can be found in the oauth2PermissionScopes property
        of the API's servicePrincipal object. The id of application permissions can be found in the appRoles property
        of the API's servicePrincipal object. The id of resource-specific application permissions can be found in
        the resourceSpecificApplicationPermissions property of the API's servicePrincipal object.
        Default is the single value all.
        """
        return self.properties.get("permissions", StringCollection())

    @property
    def resource_application(self):
        # type: () -> Optional[str]
        """
        The appId of the resource application (e.g. the API) for which a permission is being granted, or any to match
        with any resource application or API. Default is any.
        """
        return self.properties.get("resourceApplication", None)
