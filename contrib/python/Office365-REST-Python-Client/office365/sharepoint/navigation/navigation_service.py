from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.navigation.home_site_navigation_settings import (
    HomeSiteNavigationSettings,
)
from office365.sharepoint.navigation.menu_state import MenuState
from office365.sharepoint.navigation.provider_type import NavigationProviderType


class NavigationService(Entity):
    """The entry point for REST-based navigation service operations."""

    def __init__(self, context):
        """The entry point for REST-based navigation service operations."""
        static_path = ResourcePath(
            "Microsoft.SharePoint.Navigation.REST.NavigationServiceRest"
        )
        super(NavigationService, self).__init__(context, static_path)

    def get_publishing_navigation_provider_type(
        self, map_provider_name=NavigationProviderType.SPNavigationProvider
    ):
        """
        Gets a publishing navigation provider type when publishing feature is turned on for the site (2).
        If navigation provider is not found on the site MUST return InvalidSiteMapProvider type.

        :param str map_provider_name: The server will use "SPNavigationProvider" as provider name
            if mapProviderName is not specified.
        """
        return_type = ClientResult(self.context)
        params = {"mapProviderName": map_provider_name}
        qry = ServiceOperationQuery(
            self, "GetPublishingNavigationProviderType", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def global_nav(self):
        """"""
        return_type = ClientResult(self.context, MenuState())
        qry = ServiceOperationQuery(self, "GlobalNav", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def global_nav_enabled(self):
        """ """
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "GlobalNavEnabled", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def set_global_nav_enabled(self, is_enabled):
        """
        :param bool is_enabled:
        """
        qry = ServiceOperationQuery(
            self, "SetGlobalNavEnabled", None, {"isEnabled": is_enabled}
        )
        self.context.add_query(qry)
        return self

    def menu_node_key(self, current_url, map_provider_name=None):
        """
        Returns the unique key for a node within the menu tree. If a key cannot be found, an exception is returned.

        :param str current_url: A URL relative to the site collection identifying the node within the menu tree.
        :param str map_provider_name: The name identifying a provider to use for the lookup
        """
        return_type = ClientResult(self.context, str())
        params = {"currentUrl": current_url, "mapProviderName": map_provider_name}
        qry = ServiceOperationQuery(
            self, "MenuNodeKey", None, params, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def menu_state(
        self, menu_node_key, map_provider_name, depth=None, custom_properties=None
    ):
        """
        Returns the menu tree rooted at the specified root node for a given provider.

        :param str menu_node_key: A unique key identifying the node that will be used as root node in the returned
            result
        :param str map_provider_name: The name identifying a provider to use for the lookup
        :param int depth:  The number of levels to include in the returned site map. If no value is specified,
           a depth of 10 is used.
        :param str custom_properties: A comma separated list of custom properties to request.
            The character "\" is used to escape commas, allowing comma to be part of the property names.
        """
        return_type = ClientResult(self.context, MenuState())
        payload = {
            "menuNodeKey": menu_node_key,
            "mapProviderName": map_provider_name,
            "depth": depth,
            "customProperties": custom_properties,
        }
        qry = ServiceOperationQuery(self, "MenuState", None, payload, None, return_type)
        self.context.add_query(qry)
        return return_type

    def save_menu_state(self, menu_node_key, map_provider_name):
        """Updates the menu tree rooted at the specified root node for a given provider.

        :param str menu_node_key: A unique key identifying the node that will be used as root node in the returned
            result
        :param str map_provider_name: The name identifying a provider to use for the lookup
        """
        return_type = ClientResult(self.context)
        payload = {"menuNodeKey": menu_node_key, "mapProviderName": map_provider_name}
        qry = ServiceOperationQuery(
            self, "SaveMenuState", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def home_site_settings(self):
        return self.properties.get(
            "HomeSiteSettings",
            HomeSiteNavigationSettings(
                self.context, ResourcePath("HomeSiteSettings", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "HomeSiteSettings": self.home_site_settings,
            }
            default_value = property_mapping.get(name, None)
        return super(NavigationService, self).get_property(name, default_value)

    @property
    def entity_type_name(self):
        return "Microsoft.SharePoint.Navigation.REST.NavigationServiceRest"
