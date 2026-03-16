from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class Billing(Client):
    @sp_endpoint('/billing/statuses', method='POST')
    def list_billing_status(self, **kwargs) -> ApiResponse:
        r"""
        Get the billing status for a list of advertising accounts.

        Request Body
            | Bulk Get Billing Statuses Request Body {
            | The properties needed to get the billing statuses for a set of advertisers.
            | **advertiserMarketplaces**\* (array) minItems: 0 maxItems: 100 [
            |   advertiserMarketplace {
            |       **marketplaceId**\* (string)
            |       **advertiserId**\* (string)
            |       }
            |   ]
            | **locale** (string) Locale. Enum: ['ar_AE', 'cs_CZ', 'de_DE', 'en_AU', 'en_CA', 'en_GB', 'en_IN', 'en_SG', 'es_ES', 'es_MX', 'fr_CA', 'fr_FR', 'he_IL', 'hi_IN', 'it_IT', 'ja_JP', 'ko_KR', 'nl_NL', 'pl_PL', 'pt_BR', 'sv_SE', 'ta_IN', 'tr_TR', 'zh_CN', 'zh_TW']
            | }
        Returns
            ApiResponse
        """

        request_contentType = 'application/vnd.bulkgetbillingstatusrequestbody.v1+json'
        accept_contentType = 'application/vnd.bulkgetbillingstatusresponse.v1+json'
        headers = {'Content-Type': request_contentType, 'Accept': accept_contentType}

        body = Utils.convert_body(kwargs.pop('body'), False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/billing/notifications', method='POST')
    def list_billing_notifications(self, **kwargs) -> ApiResponse:
        r"""
        Get the billing notifications for a list advertising accounts.

        Request Body
            | Bulk Get Billing Notifications Request Body {
            | The properties needed to get the billing notifications for a set of advertisers.
            | **advertiserMarketplaces**\* (array) minItems: 0 maxItems: 100 [
            |   advertiserMarketplace {
            |       **marketplaceId**\* (string)
            |       **advertiserId**\* (string)
            |       }
            |   ]
            | **locale** (string) Locale. Enum: ['ar_AE', 'cs_CZ', 'de_DE', 'en_AU', 'en_CA', 'en_GB', 'en_IN', 'en_SG', 'es_ES', 'es_MX', 'fr_CA', 'fr_FR', 'he_IL', 'hi_IN', 'it_IT', 'ja_JP', 'ko_KR', 'nl_NL', 'pl_PL', 'pt_BR', 'sv_SE', 'ta_IN', 'tr_TR', 'zh_CN', 'zh_TW']
            | }
        Returns
            ApiResponse
        """

        request_contentType = 'application/vnd.billingnotifications.v1+json'
        accept_contentType = 'application/vnd.bulkgetbillingnotificationsresponse.v1+json'
        headers = {'Content-Type': request_contentType, 'Accept': accept_contentType}

        body = Utils.convert_body(kwargs.pop('body'))
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/billing/paymentMethods/list', method='POST')
    def get_customer_payment_methods(self, **kwargs) -> ApiResponse:
        r""" """
        accept_contentType = 'application/vnd.paymentmethods.v1+json'
        headers = {'Accept': accept_contentType}
        return self._request(kwargs.pop('path'), params=kwargs, headers=headers)

    @sp_endpoint('/billing/invoices/pay', method='POST')
    def pay_invoices(self, **kwargs) -> ApiResponse:
        r""" """
        accept_contentType = 'application/vnd.invoices.v1+json'
        headers = {'Accept': accept_contentType}
        body = Utils.convert_body(kwargs.pop('body'), False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/billing/paymentAgreements/list', method='POST')
    def get_payment_agreements(self, **kwargs) -> ApiResponse:
        r""" """
        accept_contentType = 'application/vnd.paymentagreements.v1+json'
        headers = {'Accept': accept_contentType}
        return self._request(kwargs.pop('path'), params=kwargs, headers=headers)

    @sp_endpoint('/billing/paymentAgreements', method='POST')
    def create_payment_agreements(self, **kwargs) -> ApiResponse:
        r""" """
        accept_contentType = 'application/vnd.paymentagreements.v1+json'
        headers = {'Accept': accept_contentType}
        body = Utils.convert_body(kwargs.pop('body'), False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)
