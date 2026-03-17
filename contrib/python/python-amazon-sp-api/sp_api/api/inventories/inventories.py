import urllib

from sp_api.base import Client, Marketplaces, sp_endpoint, ApiResponse
from sp_api.base.InventoryEnums import InventoryGranularity


class Inventories(Client):
    """
    :link: https://github.com/amzn/selling-partner-api-docs/blob/main/references/fba-inventory-api/fbaInventory.md#getinventorysummaries
    """

    @sp_endpoint('/fba/inventory/v1/summaries')
    def get_inventory_summary_marketplace(self, **kwargs) -> ApiResponse:
        """
        get_inventory_summary_marketplace(self, **kwargs) -> GetInventorySummariesResponse


        Returns a list of inventory summaries. The summaries returned depend on the presence or absence of the startDateTime and sellerSkus parameters:

        - All inventory summaries with available details are returned when the startDateTime and sellerSkus parameters are omitted.
        - When startDateTime is provided, the operation returns inventory summaries that have had changes after the date and time specified. The sellerSkus parameter is ignored.
        - When the sellerSkus parameter is provided, the operation returns inventory summaries for only the specified sellerSkus.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        2                                       2
        ======================================  ==============


        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        All inventory summaries with available details are returned when the startDateTime and sellerSkus parameters are omitted.
        When startDateTime is provided, the operation returns inventory summaries that have had changes after the date and time specified. The sellerSkus parameter is ignored.
        When the sellerSkus parameter is provided, the operation returns inventory summaries for only the specified sellerSkus.
        Usage Plan:

        Examples:
            literal blocks::

                Inventories().get_inventory_summary_marketplace(**{
                        "details": True,
                        "marketplaceIds": ["ATVPDKIKX0DER"]
                    })

        Args:
            key details: bool | true to return inventory summaries with additional summarized inventory details and quantities. Otherwise, returns inventory summaries only (default value).	boolean	"false"
            key granularityType: Granularity Type | The granularity type for the inventory aggregation level.	enum (GranularityType)	-
            key granularityId: str The granularity ID for the inventory aggregation level.	string	-
            key startDateTime: datetime | A start date and time in ISO8601 format. If specified, all inventory summaries that have changed since then are returned. You must specify a date and time that is no earlier than 18 months prior to the date and time when you call the API. Note: Changes in inboundWorkingQuantity, inboundShippedQuantity and inboundReceivingQuantity are not detected.	string (date-time)	-
            key sellerSkus: [str] | A list of seller SKUs for which to return inventory summaries. You may specify up to 50 SKUs.
            key nextToken: str | String token returned in the response of your previous request.	string	-
            key marketplaceIds: str | The marketplace ID for the marketplace for which to return inventory summaries.

        Returns:
            GetInventorySummariesResponse:

        """

        kwargs.update({
            'granularityType': kwargs.get('granularityType', InventoryGranularity.MARKETPLACE.value),
            "granularityId": kwargs.get('granularityId', self.marketplace_id)
        })
        if 'sellerSkus' in kwargs:
            kwargs.update({'sellerSkus': ','.join(kwargs.get('sellerSkus'))})

        return self._request(kwargs.pop('path'), params=kwargs)

