from ad_api.base import Client, sp_endpoint, ApiResponse, Utils


class ValidationConfigurations(Client):
    """Validation Configurations API Version 3

    Documentation: https://dtrnk0o2zy01c.cloudfront.net/openapi/en-us/dest/ValidationConfigurationsAPI_prod_3p.json

    This API allows users to retrieve campaign validation configuration values per marketplace, entity type, and program type.
    """

    @sp_endpoint('/validationConfigurations/campaigns', method='POST')
    def retrieve_validation_campaigns(self, **kwargs) -> ApiResponse:
        r"""

        Retrieves the campaign configuration values used for campaign validation for the requested marketplace, entityType, and programType context/contexts.

        Request body
            | **countryCodesList** (Array): [optional] The list of countryCode enums defining the marketplaces whose configuration values are requested. When null is selected and no value is attributed to this key, all countryCode options are selected. [ US, CA, MX, BR, UK, DE, FR, ES, IN, IT, NL, AE, SA, SE, TR, PL, EG, JP, AU, SG, CN ]
            | **entityTypesList** (Array): [optional] The list of entityType enums defining the marketplaces whose configuration values are requested. When null is selected and no value is attributed to this key, all countryCode options are selected. [ SELLER, VENDOR ]
            | **programTypesList** (Array): [optional] The list of programType enums defining the marketplaces whose configuration values are requested. When null is selected and no value is attributed to this key, all countryCode options are selected. [ SB, SP, SD ]
        Returns:
            ApiResponse

        """

        accept = 'application/vnd.AdsApiValidationConfigsServiceLambda.CampaignsResource.v1+json'
        contentType = 'application/vnd.AdsApiValidationConfigsServiceLambda.CampaignsResource.v1+json'
        headers = {'Accept': accept, 'Content-Type': contentType}
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)

    @sp_endpoint('/validationConfigurations/targetingClauses', method='POST')
    def retrieve_validation_targeting_clauses(self, **kwargs) -> ApiResponse:
        r"""

        Retrieves the configuration values used in targeting clause validation for the requested inputted marketplace, entityType, and programType context/contexts.

        Request body
            | **countryCodesList** (Array): [optional] The list of countryCode enums defining the marketplaces whose configuration values are requested. When null is selected and no value is attributed to this key, all countryCode options are selected. [ US, CA, MX, BR, UK, DE, FR, ES, IN, IT, NL, AE, SA, SE, TR, PL, EG, JP, AU, SG, CN ]
            | **entityTypesList** (Array): [optional] The list of entityType enums defining the marketplaces whose configuration values are requested. When null is selected and no value is attributed to this key, all countryCode options are selected. [ SELLER, VENDOR ]
            | **programTypesList** (Array): [optional] The list of programType enums defining the marketplaces whose configuration values are requested. When null is selected and no value is attributed to this key, all countryCode options are selected. [ SB, SP, SD ]
        Returns:
            ApiResponse

        """

        accept = 'application/vnd.AdsApiValidationConfigsServiceLambda.TargetingClausesResource.v1+json'
        contentType = 'application/vnd.AdsApiValidationConfigsServiceLambda.TargetingClausesResource.v1+json'
        headers = {'Accept': accept, 'Content-Type': contentType}
        body = Utils.convert_body(kwargs.pop('body'), wrap=False)
        return self._request(kwargs.pop('path'), data=body, params=kwargs, headers=headers)
