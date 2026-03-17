from ad_api.base import (
    Client,
    sp_endpoint,
    ApiResponse,
    Utils,
    MarketplacesIds,
    Currencies,
)
import json


class Localization(Client):
    """ """

    @sp_endpoint('/currencies/localize', method='POST')
    def get_currency_extended(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        get_currency_extended(body: (dict, str), version: int = 1) -> ApiResponse

        Gets an array of localized currencies extended with currency code and the country_code for better understanding in their target marketplaces, with the advertiser ID and source marketplace ID passed in through the header and body

        '**version**': *int* [optional] Will use content body "application/vnd.currencylocalization.v"+str(version)+"+json"

        body: | REQUIRED
        |   {
        |   '**localizeCurrencyRequests**': *list*
        |   [
        |       **'LocalizationCurrencyRequest'**: *dict*
        |       {
        |           **'currency'**: LocalizationCurrency: *dict*
        |           {
        |               **"amount"**: *int*
        |           }
        |       }
        |   ]
        |   }
        |   '**targetCountryCodes**': *list*, A list of two-letter country codes. When both marketplaceId and countryCode are present, countryCode is ignored. Please refer to the table above for a list of supported country codes.
        |   '**sourceCountryCode**': *string*
        |   '**sourceMarketplaceId**': *string*
        |   '**targetMarketplaces**': *list*
        }

        """
        json_version = "application/vnd.currencylocalization.v" + str(version) + "+json"
        headers = {"Content-Type": json_version}
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        source = json.loads(body)

        source_country_code = source.get("sourceCountryCode")
        source_marketplace_id = source.get("sourceMarketplaceId")
        # source_currency = Currencies[source.get("sourceCountryCode")].value + "(" +CurrencySymbols[Currencies[source.get("sourceCountryCode")].value].value + ")"
        source_currency = Currencies[source.get("sourceCountryCode")].value
        source_currency_requests = source.get("localizeCurrencyRequests")

        api_response = self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

        # "application/vnd.currencylocalization.v1+json"
        if version == 1:
            localized_currency_responses = api_response.payload.get("localizedCurrencyResponses")
            for i in range(len(source_currency_requests)):
                amount = source_currency_requests[i].get("currency")['amount']
                localized_currency_responses[i]['sourceCurrency'] = dict(
                    {
                        'amount': amount,
                        'country_code': source_country_code,
                        'currency': source_currency,
                        'source_marketplace_id': source_marketplace_id,
                    }
                )
                localized_currencies = localized_currency_responses[i].get("localizedCurrencies")
                for key, value in localized_currencies.items():
                    # value.update({'currency': Currencies[MarketplacesIds(key).name].value + "(" + CurrencySymbols[
                    #     Currencies[MarketplacesIds(key).name].value].value + ")"})
                    value.update({'currency': Currencies[MarketplacesIds(key).name].value})
                    value.update({'country_code': MarketplacesIds(key).name})

        # # "application/vnd.currencylocalization.v1+json"
        if version == 2:
            localized_currency_responses = api_response.payload.get("localizedCurrencyResponses")
            for localized_localized_currency_response in localized_currency_responses:
                localized_localized_currency_response.get("sourceCurrency").update({'country_code': source_country_code})
                localized_localized_currency_response.get("sourceCurrency").update({'currency': source_currency})
                localized_localized_currency_response.get("sourceCurrency").update({'source_marketplace_id': source_marketplace_id})

                localized_currencies = localized_localized_currency_response.get("localizedCurrencies")
                for key, value in localized_currencies.items():
                    value.update({'currency': Currencies[MarketplacesIds(key).name].value})
                    value.update({'country_code': MarketplacesIds(key).name})

                localized_currency = localized_localized_currency_response.get("localizedCurrency")
                for key, value in localized_currency.items():
                    value.update({'currency': Currencies[MarketplacesIds(key).name].value})
                    value.update({'country_code': MarketplacesIds(key).name})

                localized_currency_results = localized_localized_currency_response.get("localizedCurrencyResults")
                for key, value in localized_currency_results.items():
                    localized_currency = value.get("localizedCurrency")
                    localized_currency.update({'currency': Currencies[MarketplacesIds(key).name].value})
                    localized_currency.update({'country_code': MarketplacesIds(key).name})

        return api_response

    @sp_endpoint('/currencies/localize', method='POST')
    def get_currency(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        get_currency(body: (dict, str), version: int = 1, **kwargs) -> ApiResponse
        Gets an array of localized currencies in their target marketplaces, with the advertiser ID and source marketplace ID passed in through the header and body

        Returns localized currencies within specified marketplaces.

        **Requires one of these permissions**: [\"advertiser_campaign_edit\",\"advertiser_campaign_view\"]

        '**version**': *int* [optional] Will use content body "application/vnd.currencylocalization.v"+str(version)+"+json"

        body: | REQUIRED
        |   {
        |   '**localizeCurrencyRequests**': *list*
        |   [
        |       **'LocalizationCurrencyRequest'**: *dict*
        |       {
        |           **'currency'**: LocalizationCurrency: *dict*
        |           {
        |               **"amount"**: *int*
        |           }
        |       }
        |   ]
        |   }
        |   '**targetCountryCodes**': *list*, A list of two-letter country codes. When both marketplaceId and countryCode are present, countryCode is ignored. Please refer to the table above for a list of supported country codes.
        |   '**sourceCountryCode**': *string*
        |   '**sourceMarketplaceId**': *string*
        |   '**targetMarketplaces**': *list*
        }

        """

        json_version = "application/vnd.currencylocalization.v" + str(version) + "+json"
        headers = {"Content-Type": json_version}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/products/localize', method='POST')
    def get_products(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        get_products(body: (dict, str), version: int = 1, **kwargs) -> ApiResponse
        Localizes (maps) products from a source marketplace to one or more target marketplaces. The localization process succeeds for a given target marketplace if a product matching the source product can be found there and the advertiser is eligible to advertise it. Seller requests have an additional condition: the SKU of a localized product must match the SKU of the source product.

        **Requires one of these permissions**: [\"advertiser_campaign_edit\",\"advertiser_campaign_view\"]

        '**version**': *int* [optional] Will use content body "application/vnd.productlocalization.v"+str(version)+"+json"

        body: | REQUIRED
        |   {
        |   '**localizeProductRequests**': *list*
        |   [
        |       **'LocalizationProductRequest'**: *dict*
        |       {
        |           **'product'**: LocalizationProduct: *dict*
        |           {
        |               **"asin"**: *string*, The product's Amazon Standard Identification Number. Required for entityType=VENDOR. If caller's entityType is SELLER, this field is optional and can yield better localization results if included.
        |               **"sku"**: *string*, The product's Stock Keeping Unit. Required for entityType=SELLER. If caller's entityType is VENDOR, this field is ignored.
        |           }
        |       }
        |   ]
        |   }
        |   '**adType**': *string*, Used to confirm that the caller is eligible to advertise localized products. Currently, only Sponsored Products advertising is supported. [ SPONSORED_PRODUCTS ]
        |   '**sourceCountryCode**': *string* A two-letter country code. When both marketplaceId and countryCode are present, countryCode is ignored. Please refer to the table above for a list of supported country codes.
        |   '**entityType**': *string* [required] The type of the advertiser accounts for which IDs are specified elsewhere in the request. [ SELLER, VENDOR ]
        |   '**sourceMarketplaceId**': *string* The ID of the source marketplace. Please see the table within the description of the target details object for supported values.
        |   '**targetDetails**': *list*
        |       '**LocalizationProductTargetDetails**': *dict* The target details for the LocalizationProductRequests. There must be only one target details object per marketplace ID. The advertiser ID may be repeated across target details objects. The order of target details objects is irrelevant. The following marketplaces are supported for product localization:
        |           {
        |               **"marketplaceId"**: *string*, The ID of a target marketplace (a marketplace in which the caller wishes to localize the specified products). For example, if the caller is an advertiser based in the UK (marketplace ID A1F83G8C2ARO7P) and wishes to localize a product to Germany (marketplace ID A1PA6795UKMFR9), the target marketplace ID is that of Germany, i.e., A1PA6795UKMFR9. The following marketplaces are supported for product localization:
        |               **"countryCode"**: *string*, A two-letter country code. When both marketplaceId and countryCode are present, countryCode is ignored. Please refer to the table above for a list of supported country codes.
        |               **"advertiserId"**: *string*, [required] The advertiser ID of the caller in the associated target marketplace. The ID of the source advertiser account. This may be either a marketplace-specific obfuscated ID (AD9EUOBWMS33M), an entity ID (ENTITYYXZDK86N86HG), or a global account ID (amzn1.ads-account.g.e0kbzpoe2gkpai1pqeaca59k8). This is the advertiser ID returned by the Profiles API. An entity ID (one starting with "ENTITY"), or a global account advertiser ID may be provided instead.
        |           }
        }
        """

        json_version = "application/vnd.productlocalization.v" + str(version) + "+json"
        headers = {"Content-Type": json_version}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/keywords/localize', method='POST')
    def get_keywords(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        get_keywords(body: (dict, str), version: int = 1, **kwargs) -> ApiResponse

        Returns localized keywords within specified marketplaces or locales.

        **Requires one of these permissions**: [\"advertiser_campaign_edit\",\"advertiser_campaign_view\"]

        '**version**': *int* [optional] Will use content body "application/vnd.keywordlocalization.v"+str(version)+"+json"

        body: | REQUIRED
        |   {
        |   '**localizeKeywordRequests**': *list* List of LocalizationKeywordRequests. The order will be maintained in the response maxItems: 1000
        |   [
        |       **'LocalizationKeywordRequest'**: *dict* A LocalizationKeywordRequest object. Contains information needed about the keyword to be localized.
        |           **'localizationKeyword'**: *dict* An object containing information about a keyword.
        |           {
        |               **'keyword'**: *str* The keyword string.
        |           }
        |   ]
        |   '**sourceDetails**': *dict*
        |   {
        |       **'marketplaceId'**: *string*
        |       **'countryCode'**: *string*
        |       **'locale'**: *string*
        |   }
        |   }
        |   '**targetDetails**': *dict*
        |   {
        |       **'marketplaceIds'**: *string*
        |       **'countryCodes'**: *string*
        |       **'locales'**: *string*
        |   }
        |   }
        |   '**targetCountryCodes**': *list*, A list of two-letter country codes. When both marketplaceId and countryCode are present, countryCode is ignored. Please refer to the table above for a list of supported country codes.
        |   '**sourceCountryCode**': *string*
        |   '**sourceMarketplaceId**': *string*
        |   '**targetMarketplaces**': *list*
        }

        """
        json_version = "application/vnd.keywordlocalization.v" + str(version) + "+json"
        headers = {"Content-Type": json_version}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/targetingExpression/localize', method='POST')
    def get_targeting_expression(self, version: int = 1, **kwargs) -> ApiResponse:
        r"""
        Localizes targeting expressions used for advertising targeting.

        Localizes (maps) targeting expressions from a source marketplace to one or more target marketplaces.

        **Requires one of these permissions**: [\"advertiser_campaign_edit\",\"advertiser_campaign_view\"]

        '**version**': *int* [optional] Will use content body "application/vnd.targetingexpressionlocalization.v"+str(version)+"+json"

        body: | REQUIRED
        |   {
        |   '**targetingExpressionLocalizationRequest**': *list*
        |   [
        |       **'targetDetailsList'**: *dict* LocalizationTargetingTargetDetails
        |       {
        |           **'marketplaceId'**: *string* The ID of the target marketplace. For example, when mapping data from the UK (marketplace ID A1F83G8C2ARO7P) to Germany (marketplace ID A1PA6795UKMFR9), the target marketplace ID is that of the UK, i.e., A1F83G8C2ARO7P.
        |           **'countryCode'**: *string* The ID of the target marketplace. For example, when mapping data from the UK (marketplace ID A1F83G8C2ARO7P) to Germany (marketplace ID A1PA6795UKMFR9), the target marketplace ID is that of the UK, i.e., A1F83G8C2ARO7P.
        |       }
        |   ]
        |   '**requests**': *list* LocalizationTargetingExpressionRequest
        |   [
        |       **'LocalizationTargetingExpressionRequest'**: *dict*
        |       {
        |           **'isForNegativeTargeting'**: *bool* Specifies whether the expression is for positive targeting (false) or negative targeting (true).
        |           **'expression'**: LocalizationTargetingExpressionPredicate: *dict*
        |               {
        |                   **'type'**: LocalizationTargetingExpressionPredicateType: *string* Targeting predicate type. The following predicate types are supported [ asinCategorySameAs, asinBrandSameAs, asinPriceLessThan, asinPriceBetween, asinPriceGreaterThan, asinReviewRatingLessThan, asinReviewRatingBetween, asinReviewRatingGreaterThan, asinSameAs, asinIsPrimeShippingEligible, asinAgeRangeSameAs, asinGenreSameAs ]
        |                   **'value'**: *string* The value of the predicate. Targeting expression syntax, including examples of predicates and the values they support, is documented here (https://advertising.amazon.com/API/docs/en-us/bulksheets/sp/sp-general-info/sp-product-attribute-targeting). Only predicates using the following types of data will be localized:
        |               }
        |       }
        |   ]
        |   '**sourceDetails**': *dict*, LocalizationTargetingSourceDetails
        |       {
        |           **'marketplaceId'**: *string* The ID of the source marketplace. For example, when mapping data from the UK (marketplace ID A1F83G8C2ARO7P) to Germany (marketplace ID A1PA6795UKMFR9), the source marketplace ID is that of the UK, i.e., A1F83G8C2ARO7P.
        |           **'countryCode'**: *string* A two-letter country code. Please refer to the table above for a list of supported country codes.
        |       }
        }
        """
        json_version = "application/vnd.keywordlocalization.v" + str(version) + "+json"
        headers = {"Content-Type": json_version}

        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)
