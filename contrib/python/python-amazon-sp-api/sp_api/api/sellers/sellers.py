from sp_api.base.helpers import sp_endpoint
from sp_api.base import Client, Marketplaces, ApiResponse


class Sellers(Client):
    """
    :link: https://github.com/amzn/selling-partner-api-docs/blob/main/references/sellers-api/sellers.md

    """

    @sp_endpoint('/sellers/v1/marketplaceParticipations')
    def get_marketplace_participation(self, **kwargs) -> ApiResponse:
        """
        get_marketplace_participation(self, **kwargs) -> ApiResponse
        Returns a list of marketplaces that the seller submitting the request can sell in and information about the seller's participation in those marketplaces.

        **Usage Plan:**

        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        .016                                    15
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                res = Sellers().get_marketplace_participation()

        Args:
            **kwargs:

        Returns:
            GetMarketplaceParticipationsResponse:

        """
        return self._request(kwargs.pop('path'), add_marketplace=False)
