from ad_api.base import Client, sp_endpoint, fill_query_params, ApiResponse, Utils


class BudgetRules(Client):
    @sp_endpoint('/sd/campaigns/{}/budgetRules', method='POST')
    def create_campaign_budget_rules(self, campaignId, **kwargs) -> ApiResponse:
        r"""
        Associates one or more budget rules to a campaign specified by identifer.

        Param:
            | path **campaignId**\* (number). The campaign identifier.

        Returns:

            ApiResponse

        """
        return self._request(
            fill_query_params(kwargs.pop('path'), campaignId),
            data=Utils.convert_body(kwargs.pop('body'), False),
            params=kwargs,
        )

    @sp_endpoint('/sd/campaigns/{}/budgetRules', method='GET')
    def get_budget_rules_campaign(self, campaignId, **kwargs) -> ApiResponse:
        r"""
        Gets a list of budget rules associated to a campaign specified by identifier.

        Param:
            | path **campaignId**\* (number). The campaign identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/sd/budgetRules', method='POST')
    def create_budget_rules(self, **kwargs) -> ApiResponse:
        r"""
        Creates one or more budget rules.

        Request Body
            | CreateSPBudgetRulesRequest {
            | **budgetRulesDetails** (array) [
            |   maxItems: 25.
            |   A list of budget rule details.
            |   SPBudgetRuleDetails {
            |   Object representing details of a budget rule for SP campaign
            |   **duration** RuleDuration {
            |       eventTypeRuleDuration EventTypeRuleDuration {
            |           Object representing event type rule duration.
            |           eventId*(string): The event identifier. This value is available from the budget rules recommendation API.
            |           endDate(string): The event end date in YYYYMMDD format. Read-only.
            |           eventName(string): The event name. Read-only.
            |           startDate(string): The event start date in YYYYMMDD format. Read-only. Note that this field is present only for announced events.
            |           }
            |       dateRangeTypeRuleDuration DateRangeTypeRuleDuration {
            |           Object representing date range type rule duration.
            |           endDate(string): The end date of the budget rule in YYYYMMDD format. The end date is inclusive. Required to be equal or greater than `startDate`.
            |           startDate*(string): The start date of the budget rule in YYYYMMDD format. The start date is inclusive. Required to be greater than or equal to current date.
            |           eventName(string): The event name. Read-only.
            |           startDate(string): The event start date in YYYYMMDD format. Read-only. Note that this field is present only for announced events.
            |           }
            |       }
            | **recurrence** Recurrence {
            |       type (string): The frequency of the rule application. Enum: ['DAILY']
            |       daysOfWeek (array). Object representing days of the week for weekly type rule. It is not required for daily recurrence type
            |       DayOfWeek [
            |           DayOfWeek(string): The day of the week. Enum: ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
            |           ]
            |       }
            | **ruleType**\* SPRuleType (string): The type of budget rule. SCHEDULE: A budget rule based on a start and end date. PERFORMANCE: A budget rule based on advertising performance criteria. Enum: ['SCHEDULE', 'PERFORMANCE']
            | **budgetIncreaseBy** budgetIncreaseBy {
            |       type* BudgetChangeType (string): The value by which to update the budget of the budget rule. Enum: ['PERCENT']
            |       value* number($double): The budget value.
            |       }
            | **name** (string): The budget rule name. Required to be unique within a campaign. maxLength: 355
            | **performanceMeasureCondition**\* PerformanceMeasureCondition {
            |       metricName* PerformanceMetrics (string): The advertising performance metric. Enum: [ ACOS, CTR, CVR, ROAS ]
            |       comparisonOperator* ComparisonOperator (string): The comparison operator. Enum: [ GREATER_THAN, LESS_THAN, EQUAL_TO, LESS_THAN_OR_EQUAL_TO, GREATER_THAN_OR_EQUAL_TO ]
            |       threshold* number($double): The performance threshold value.
            |       }
            |    ]
            | }


        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)

    @sp_endpoint('/sd/budgetRules', method='GET')
    def list_budget_rules(self, **kwargs) -> ApiResponse:
        r"""
        Get all budget rules created by an advertiser

        Param:
            | query **nextToken** (string). To retrieve the next page of results, call the same operation and specify this token in the request. If the nextToken field is empty, there are no further results.
            | query **pageSize**\* (number). Sets a limit on the number of results returned. Maximum limit of pageSize is 30.

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), params=kwargs)

    @sp_endpoint('/sd/budgetRules', method='PUT')
    def edit_budget_rules(self, **kwargs) -> ApiResponse:
        r"""
        Updates one or more budget rules.

        Request Body
            | UpdateSPBudgetRulesRequest {
            | **budgetRulesDetails** (array) [
            |   maxItems: 25.
            |   A list of budget rule details.
            |   SPBudgetRule {
            |       **ruleState** state (string): The budget rule state. Enum: ['ACTIVE', 'PAUSED']
            |       **lastUpdatedDate** number($int64): Epoch time of budget rule update. Read-only.
            |       **createdDate** number($int64): Epoch time of budget rule creation. Read-only.
            |       **ruleDetails** SPBudgetRuleDetails {
            |           Object representing details of a budget rule for SP campaign
            |           **duration** RuleDuration {
            |           eventTypeRuleDuration EventTypeRuleDuration {
            |               Object representing event type rule duration.
            |               eventId*(string): The event identifier. This value is available from the budget rules recommendation API.
            |               endDate(string): The event end date in YYYYMMDD format. Read-only.
            |               eventName(string): The event name. Read-only.
            |               startDate(string): The event start date in YYYYMMDD format. Read-only. Note that this field is present only for announced events.
            |               }
            |           dateRangeTypeRuleDuration DateRangeTypeRuleDuration {
            |               Object representing date range type rule duration.
            |               endDate(string): The end date of the budget rule in YYYYMMDD format. The end date is inclusive. Required to be equal or greater than `startDate`.
            |               startDate*(string): The start date of the budget rule in YYYYMMDD format. The start date is inclusive. Required to be greater than or equal to current date.
            |               eventName(string): The event name. Read-only.
            |               startDate(string): The event start date in YYYYMMDD format. Read-only. Note that this field is present only for announced events.
            |               }
            |           }
            |       **recurrence** Recurrence {
            |           type (string): The frequency of the rule application. Enum: ['DAILY']
            |           daysOfWeek (array). Object representing days of the week for weekly type rule. It is not required for daily recurrence type
            |           DayOfWeek [
            |               DayOfWeek(string): The day of the week. Enum: ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
            |               ]
            |           }
            |       **ruleType**\* SPRuleType (string): The type of budget rule. SCHEDULE: A budget rule based on a start and end date. PERFORMANCE: A budget rule based on advertising performance criteria. Enum: ['SCHEDULE', 'PERFORMANCE']
            |       **budgetIncreaseBy** budgetIncreaseBy {
            |           type* BudgetChangeType (string): The value by which to update the budget of the budget rule. Enum: ['PERCENT']
            |           value* number($double): The budget value.
            |           }
            |       **name** (string): The budget rule name. Required to be unique within a campaign. maxLength: 355
            |       **performanceMeasureCondition**\* PerformanceMeasureCondition {
            |           metricName* PerformanceMetrics (string): The advertising performance metric. Enum: [ ACOS, CTR, CVR, ROAS ]
            |           comparisonOperator* ComparisonOperator (string): The comparison operator. Enum: [ GREATER_THAN, LESS_THAN, EQUAL_TO, LESS_THAN_OR_EQUAL_TO, GREATER_THAN_OR_EQUAL_TO ]
            |           threshold* number($double): The performance threshold value.
            |           }
            |       **ruleId**\* (string): The budget rule identifier.
            |       **ruleStatus** (string): he budget rule status. Read-only.
            |       }
            |    ]
            | }

        Returns:

            ApiResponse

        """
        return self._request(kwargs.pop('path'), data=Utils.convert_body(kwargs.pop('body'), False), params=kwargs)

    @sp_endpoint('/sd/budgetRules/{}/campaigns', method='GET')
    def get_campaigns_budget_rule(self, budgetRuleId, **kwargs) -> ApiResponse:
        r"""
        Gets all the campaigns associated with a budget rule

        Param:
            | path **budgetRuleId**\* (string). The budget rule identifier.
            | query **nextToken** (string). To retrieve the next page of results, call the same operation and specify this token in the request. If the nextToken field is empty, there are no further results.
            | query **pageSize**\* (number). Sets a limit on the number of results returned. Maximum limit of pageSize is 30.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), budgetRuleId), params=kwargs)

    @sp_endpoint('/sd/campaigns/{}/budgetRules/{}', method='DELETE')
    def delete_budget_rule_campaign(self, campaignId, budgetRuleId, **kwargs) -> ApiResponse:
        r"""
        Disassociates a budget rule specified by identifier from a campaign specified by identifier.

        Param:
            | path **campaignId**\* (number). The campaign identifier.
            | path **budgetRuleId**\* (string). The budget rule identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId, budgetRuleId), params=kwargs)

    @sp_endpoint('/sd/campaigns/{}/budgetRules/budgetHistory', method='GET')
    def get_budget_history(self, campaignId, **kwargs) -> ApiResponse:
        r"""
        Gets the budget history for a campaign specified by identifier.

        Param:
            | path **campaignId**\* (number). The campaign identifier.
            | query **nextToken** (string). To retrieve the next page of results, call the same operation and specify this token in the request. If the nextToken field is empty, there are no further results.
            | query **pageSize**\* (number). Sets a limit on the number of results returned. Maximum limit of pageSize is 30.
            | query **startDate**\* (string). The start date of the budget history in YYYYMMDD format.
            | query **endDate**\* (string). The end date of the budget history in YYYYMMDD format.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), campaignId), params=kwargs)

    @sp_endpoint('/sd/budgetRules/{}', method='GET')
    def get_budget_rule(self, budgetRuleId, **kwargs) -> ApiResponse:
        r"""
        Gets a budget rule specified by identifier.

        Param:
            | path **budgetRuleId**\* (string). The budget rule identifier.

        Returns:

            ApiResponse

        """
        return self._request(fill_query_params(kwargs.pop('path'), budgetRuleId), params=kwargs)
