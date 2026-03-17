from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Account(Client):
    @sp_endpoint('/adsAccounts/list', method='POST')
    def list_accounts(self, **kwargs) -> ApiResponse:
        r"""
        list_accounts(body: (dict, str)) -> ApiResponse

        body: | Optional
            | **max_results** (int) Number of records to include in the paginated response. Defaults to max page size for given API. Minimum 10 and a Maximum of 100 [optional]
            | **next_token** (string) Token value allowing to navigate to the next response page. [optional]

        Returns: List all advertising accounts for the user associated with the access token.
        """
        schema_version = 'application/vnd.listaccountsresource.v1+json'
        headers = {'Accept': schema_version, 'Content-Type': schema_version}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/adsAccounts/{}', method='GET')
    def get_account(self, advertisingAccountId: str, **kwargs) -> ApiResponse:
        r"""
        get_account(advertisingAccountId: str) -> ApiResponse

        Request attributes of a given advertising account.

        path **advertisingAccountId**:string | Required. This is the global advertising account Id from the client.
        """
        schema_version = 'application/vnd.listaccountsresource.v1+json'
        headers = {'Accept': schema_version, 'Content-Type': schema_version}

        return self._request(fill_query_params(kwargs.pop('path'), advertisingAccountId), params=kwargs, headers=headers)

    @sp_endpoint('/adsAccounts', method='POST')
    def create_account(self, **kwargs) -> ApiResponse:
        r"""
        create_account(body: (dict, str)) -> ApiResponse

        Create a new advertising account tied to a specific Amazon vendor, seller or author, or to a business who does not sell on Amazon.

        body: | REQUIRED
            | **associations** (list[dict]) Associations you would like to link to this advertising account, could be Amazon Vendor, Seller, or just a regular business
            | **countryCodes** (list[string]) The countries that you want this account to operate in.
            | **accountName** (string) Account names are typically the name of the company or brand being advertised.
            | **termsToken** (string) An obfuscated identifier of the termsToken, which is activated when an advertisers accepts the Amazon Ads Agreement in relation to the ads account being register.
        """
        schema_version = 'application/vnd.registeradsaccountresource.v1+json'
        headers = {'Accept': schema_version, 'Content-Type': schema_version}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)
