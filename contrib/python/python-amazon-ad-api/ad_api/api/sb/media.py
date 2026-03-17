from ad_api.base import Client, sp_endpoint, ApiResponse


class Media(Client):
    """
    Use this interface to request and retrieve store information. This can be used for Sponsored Brands campaign creation, to pull the store URL information, and for asset registration for Stores.
    """

    @sp_endpoint('/media/upload', method='POST')
    def create_media(self, **kwargs) -> ApiResponse:
        """
        Creates an ephemeral resource (upload location) to upload Media for an Ad Program (SponsoredBrands).

        The upload location is short lived and expires in 15 minutes. Once the upload is complete, /media/complete API should be used to notify that the upload is complete.

        The upload location only supports PUT HTTP Method to upload the media content. If the upload location expires, API user will get 403 Forbidden response.

        Request body
            | **programType** (string): [required] [ SponsoredBrands ].
            | **creativeType** (string): [required] [ Video ].

        Returns:
            | ApiResponse

        """
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/media/complete', method='PUT')
    def complete_media(self, **kwargs) -> ApiResponse:
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)

    @sp_endpoint('/media/describe', method='GET')
    def describe_media(self, **kwargs) -> ApiResponse:
        return self._request(kwargs.pop('path'), data=kwargs.pop('body'), params=kwargs)
