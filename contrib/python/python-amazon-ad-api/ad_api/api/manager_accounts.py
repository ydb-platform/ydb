from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class ManagerAccounts(Client):
    @sp_endpoint('/managerAccounts', method='GET')
    def list_manager_accounts(self, **kwargs) -> ApiResponse:
        r"""

        list_manager_accounts() -> ApiResponse

        Returns all Manager accounts that a given Amazon Advertising user has access to.

        """

        accept = 'application/vnd.getmanageraccountsresponse.v1+json'
        headers = {'Accept': accept}

        return self._request(kwargs.pop('path'), params=kwargs, headers=headers)

    @sp_endpoint('/managerAccounts', method='POST')
    def create_manager_account(self, **kwargs) -> ApiResponse:
        r"""
        create_manager_account(body: (dict, str)) -> ApiResponse

        Creates a new Amazon Advertising Manager account.

        body: | REQUIRED

            | {
            | '**managerAccountName**': *string* Name of the Manager account.
            | '**managerAccountType**': *string* Type of the Manager account, which indicates how the Manager account will be used. Use Advertiser if the Manager account will be used for your own products and services, or Agency if you are managing accounts on behalf of your clients. Enum: [ Advertiser, Agency ]
            | }

        """

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/managerAccounts/{}/associate', method='POST')
    def associate_manager_accounts(self, managerAccountId: str, **kwargs) -> ApiResponse:
        r"""
        associate_manager_accounts(managerAccountId: str, body: (dict, str)) -> ApiResponse

        Link Amazon Advertising accounts or advertisers with a Manager Account.

        path **managerAccountId**:string | Required. Id of the Manager Account.

        body: | REQUIRED

        | {
        |   "accounts": A list of Advertising accounts or advertisers to link/unlink with Manager Account. User can pass a list with a maximum of 20 accounts/advertisers using any mix of identifiers.
        |       [
        |           {
        |               "roles": "list", The types of role that will exist with the Amazon Advertising account. Depending on account type, the default role will be ENTITY_USER or SELLER_USER. Only one role at a time is currently supported [ ENTITY_OWNER, ENTITY_USER, ENTITY_VIEWER, SELLER_USER ]
        |               "id": "string", Id of the Amazon Advertising account.
        |               "type": The type of the Id, Enum: [ ACCOUNT_ID, DSP_ADVERTISER_ID ]
        |           }
        |       ]
        | }

        """

        content_type = 'application/vnd.updateadvertisingaccountsinmanageraccountrequest.v1+json'
        headers = {'Content-Type': content_type}
        # accept_contentType = 'application/vnd.updateadvertisingaccountsinmanageraccountresponse.v1+json'
        # headers = {'Content-Type': request_contentType, 'Accept': accept_contentType}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(fill_query_params(kwargs.pop('path'), managerAccountId), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/managerAccounts/{}/disassociate', method='POST')
    def disassociate_manager_accounts(self, managerAccountId: str, **kwargs) -> ApiResponse:
        r"""
        disassociate_manager_accounts(managerAccountId: str, body: (dict, str)) -> ApiResponse

        Unlink Amazon Advertising accounts or advertisers with a Manager Account.

        path **managerAccountId**:string | Required. Id of the Manager Account.

        body: | REQUIRED

        | {
        |   "accounts": A list of Advertising accounts or advertisers to link/unlink with Manager Account. User can pass a list with a maximum of 20 accounts/advertisers using any mix of identifiers.
        |       [
        |           {
        |               "roles": "list", The types of role that will exist with the Amazon Advertising account. Depending on account type, the default role will be ENTITY_USER or SELLER_USER. Only one role at a time is currently supported [ ENTITY_OWNER, ENTITY_USER, ENTITY_VIEWER, SELLER_USER ]
        |               "id": "string", Id of the Amazon Advertising account.
        |               "type": The type of the Id, Enum: [ ACCOUNT_ID, DSP_ADVERTISER_ID ]
        |           }
        |       ]
        | }

        """

        # request_contentType = 'application/vnd.updateadvertisingaccountsinmanageraccountrequest.v1+json'
        # accept_contentType = 'application/vnd.updateadvertisingaccountsinmanageraccountresponse.v1+json'
        # headers = {'Content-Type': request_contentType, 'Accept': accept_contentType}

        content_type = 'application/vnd.updateadvertisingaccountsinmanageraccountrequest.v1+json'
        headers = {'Content-Type': content_type}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(fill_query_params(kwargs.pop('path'), managerAccountId), data=body, params=kwargs, headers=headers)
