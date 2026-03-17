from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Moderation(Client):
    """
    Use this interface to request and retrieve store information. This can be used for Sponsored Brands campaign creation, to pull the store URL information, and for asset registration for Stores.
    """

    @sp_endpoint('/sb/moderation/campaigns/{}', method='GET')
    def get_moderation(self, campaignId, **kwargs) -> ApiResponse:
        """
        Gets the moderation result for a campaign specified by identifier.

        Keyword Args
            | path **campaignId** (integer): The campaign identifier. [required]

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)
