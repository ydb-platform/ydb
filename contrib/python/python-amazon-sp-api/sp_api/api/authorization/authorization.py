import urllib.parse

from sp_api.base import Client, sp_endpoint, fill_query_params, ApiResponse


class Authorization(Client):
    """
    Authorization SP-API Client
    :link: 

    The Selling Partner API for Authorization helps developers manage authorizations and check the specific permissions associated with a given authorization.
    """
    grantless_scope = 'sellingpartnerapi::migration'

    @sp_endpoint('/authorization/v1/authorizationCode', method='GET')
    def get_authorization_code(self, **kwargs) -> ApiResponse:
        """
        get_authorization_code(self, **kwargs) -> ApiResponse

        With the getAuthorizationCode operation, you can request a Login With Amazon (LWA) authorization code that will allow you to call a Selling Partner API on behalf of a seller who has already authorized you to call Amazon Marketplace Web Service (Amazon MWS). You specify a developer ID, an MWS auth token, and a seller ID. Taken together, these represent the Amazon MWS authorization that the seller previously granted you. The operation returns an LWA authorization code that can be exchanged for a refresh token and access token representing authorization to call the Selling Partner API on the seller's behalf. By using this API, sellers who have already authorized you for Amazon MWS do not need to re-authorize you for the Selling Partner API.

        **Usage Plan:**


        ======================================  ==============
        Rate (requests per second)               Burst
        ======================================  ==============
        1                                       5
        ======================================  ==============

        For more information, see "Usage Plans and Rate Limits" in the Selling Partner API documentation.

        Examples:
            literal blocks::

                res = Authorization().get_authorization_code(
                    mwsAuthToken='test',
                    developerId='test',
                    sellingPartnerId='test'
                )


        Args:
            key sellingPartnerId:string | * REQUIRED The seller ID of the seller for whom you are requesting Selling Partner API authorization. This must be the seller ID of the seller who authorized your application on the Marketplace Appstore.
            key developerId:string | * REQUIRED Your developer ID. This must be one of the developer ID values that you provided when you registered your application in Developer Central.
            key mwsAuthToken:string | * REQUIRED The MWS Auth Token that was generated when the seller authorized your application on the Marketplace Appstore.

        Returns:
            ApiResponse:
        """
    
        return self._request_grantless_operation(kwargs.pop('path'),  params=kwargs)

