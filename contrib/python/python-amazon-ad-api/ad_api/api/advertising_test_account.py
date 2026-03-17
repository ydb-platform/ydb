from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class AdvertisingTestAccount(Client):
    """
    Create test advertising account for 3P API integrators
    """

    @sp_endpoint('/testAccounts', method='POST')
    def create_test_account(self, **kwargs) -> ApiResponse:
        r"""
        create_test_account(body: dict or str) -> ApiResponse:
        API to create test accounts

        Submit a account creation request. You can create up to 1 test account type per marketplace.

        Request body
            | **countryCode** (string): [required] Country code of the test account. [ US, CA, MX, BR, UK, DE, FR, ES, IT, CN, JP, AU, AE, SA, NL ]
            | **accountMetaData** (string):
                | vendorCode [optional] Vendor code that needs to be associated with the vendor account. example: ABCDE
            | **accountType** (string): [required] Type of test account. [ VENDOR, AUTHOR ]

        Returns:
            ApiResponse

        """
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/testAccounts', method='GET')
    def get_test_account(self, **kwargs) -> ApiResponse:
        r"""
        get_test_account(requestId: str) -> ApiResponse

        Keyword Args
            | query **requestId** (string): [required] request id.

        Returns:
            ApiResponse
        """
        return self._request(kwargs.pop('path'), params=kwargs)
