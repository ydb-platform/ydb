from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class Portfolios(Client):
    """ """

    @sp_endpoint('/v2/portfolios', method='GET')
    def list_portfolios(self, **kwargs) -> ApiResponse:
        r"""

        list_portfolios(**kwargs) -> ApiResponse


        Retrieves a list of portfolios, optionally filtered by identifier, name, or state. Note that this operation returns a maximum of 100 portfolios.


        query **portfolioIdFilter**:string | Optional. The returned list includes portfolios with identifiers matching those in the specified comma-delimited list. There is a maximum of 100 identifiers allowed


        query **portfolioNameFilter**:string | Optional. The returned list includes portfolios with identifiers matching those in the specified comma-delimited list. There is a maximum of 100 identifiers allowed


        query **portfolioStateFilter**:string | Optional. The returned list includes portfolios with states matching those in the specified comma-delimited list. Available values : enabled, paused, archived


        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/portfolios/extended', method='GET')
    def list_portfolios_extended(self, **kwargs) -> ApiResponse:
        r"""
        list_portfolios_extended(**kwargs) -> ApiResponse


        Retrieves a list of portfolios, optionally filtered by identifier, name, or state. Note that this operation returns a maximum of 100 portfolios.


        query **portfolioIdFilter**:string | Optional. The returned list includes portfolios with identifiers matching those in the specified comma-delimited list. There is a maximum of 100 identifiers allowed


        query **portfolioNameFilter**:string | Optional. The returned list includes portfolios with identifiers matching those in the specified comma-delimited list. There is a maximum of 100 identifiers allowed


        query **portfolioStateFilter**:string | Optional. The returned list includes portfolios with states matching those in the specified comma-delimited list. Available values : enabled, paused, archived

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/v2/portfolios/{}', method='GET')
    def get_portfolio(self, portfolioId, **kwargs) -> ApiResponse:
        r"""
        get_portfolio(portfolioId) -> ApiResponse

        Retrieves a portfolio data with the portfolioId identifier provided

        query **portfolioId**:number | Required. The identifier of an existing portfolio.

        """
        return self._request(fill_query_params(kwargs.pop('path'), portfolioId), params=kwargs)

    @sp_endpoint('/v2/portfolios/extended/{}', method='GET')
    def get_portfolio_extended(self, portfolioId, **kwargs) -> ApiResponse:
        r"""
        get_portfolio_extended(portfolioId) -> ApiResponse

        Gets an extended set of properties for a portfolio specified by identifier.

        query **portfolioId**:number | Required. The identifier of an existing portfolio.

        """
        return self._request(fill_query_params(kwargs.pop('path'), portfolioId), params=kwargs)

    @sp_endpoint('/v2/portfolios', method='POST')
    def create_portfolios(self, **kwargs) -> ApiResponse:
        r"""

        create_portfolios(body: (list, str, dict)) -> ApiResponse

        Creates one or more portfolios.

        body: | REQUIRED {'description': 'A list of portfolio resources with updated values.}'

            | **name** | **string** | The portfolio name.
            | **budget** | **dict** |

                | **amount** | **number** | The budget amount associated with the portfolio. Cannot be null.
                | **currencyCode** | **string** | The currency used for all monetary values for entities under this profile. Cannot be null.
                | **policy** | **string** | The budget policy. Set to dateRange to specify a budget for a specific period of time. Set to monthlyRecurring to specify a budget that is automatically renewed at the beginning of each month. Cannot be null. Enum: [ dateRange, monthlyRecurring ]
                | **startDate** | **string** | The starting date in YYYYMMDD format to which the budget is applied. Required if policy is set to dateRange. Not specified if policy is set to monthlyRecurring. Note that the starting date for monthlyRecurring is the date when the policy is set.
                | **endDate** | **string** | The end date after which the budget is no longer applied. Optional if policy is set to dateRange or monthlyRecurring.

            | **inBudget** | **boolean** | Indicates the current budget status of the portfolio. Set to true if the portfolio is in budget, set to false if the portfolio is out of budget.
            | **state** | **string** | The current state of the portfolio. Enum: [ enabled, paused, archived ]


        """
        body = Utils.convert_body(kwargs.pop('body'))
        return self._request(kwargs.pop('path'), data=body, params=kwargs)

    @sp_endpoint('/v2/portfolios', method='PUT')
    def edit_portfolios(self, **kwargs) -> ApiResponse:
        r"""

        edit_portfolios(body: (list, str, dict)) -> ApiResponse

        Updates one or more portfolios.

        body: | REQUIRED {'description': 'A list of portfolio resources with updated values.}'

            | **portfolioId** | **number** | The portfolio identifier.
            | **name** | **string** | The portfolio name.
            | **budget** | **dict** |

                | **amount** | **number** | The budget amount associated with the portfolio. Cannot be null.
                | **currencyCode** | **string** | The currency used for all monetary values for entities under this profile. Cannot be null.
                | **policy** | **string** | The budget policy. Set to dateRange to specify a budget for a specific period of time. Set to monthlyRecurring to specify a budget that is automatically renewed at the beginning of each month. Cannot be null. Enum: [ dateRange, monthlyRecurring ]
                | **startDate** | **string** | The starting date in YYYYMMDD format to which the budget is applied. Required if policy is set to dateRange. Not specified if policy is set to monthlyRecurring. Note that the starting date for monthlyRecurring is the date when the policy is set.
                | **endDate** | **string** | The end date after which the budget is no longer applied. Optional if policy is set to dateRange or monthlyRecurring.

            | **inBudget** | **boolean** | Indicates the current budget status of the portfolio. Set to true if the portfolio is in budget, set to false if the portfolio is out of budget.
            | **state** | **string** | The current state of the portfolio. Enum: [ enabled, paused, archived ]


        """
        body = Utils.convert_body(kwargs.pop('body'))
        return self._request(kwargs.pop('path'), data=body, params=kwargs)
