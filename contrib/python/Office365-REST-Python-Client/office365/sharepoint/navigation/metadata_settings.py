from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.navigation.configured_metadata_items import (
    ConfiguredMetadataNavigationItemCollection,
)


class MetadataNavigationSettings(Entity):
    """Described and allows changes to the meta-data navigation and filtering settings on an SPList."""

    @staticmethod
    def get_configured_settings(context, url, return_type=None):
        """
        Retrieves the configured metadata navigation settings for the list with the specified url.
        :param office365.sharepoint.client_context.ClientContext context: SharePoint context
        :param str url: Specifies list url
        :param ClientResult return_type: Return type
        """

        if return_type is None:
            return_type = ClientResult(
                context, ConfiguredMetadataNavigationItemCollection()
            )
        payload = {"DecodedUrl": url}
        qry = ServiceOperationQuery(
            MetadataNavigationSettings(context),
            "GetConfiguredSettings",
            payload,
            None,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.MetadataNavigation.MetadataNavigationSettings"
