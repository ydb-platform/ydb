from office365.runtime.client_value import ClientValue


class AccessReviewScope(ClientValue):
    """
    The accessReviewScope defines what entities are reviewed in an accessReviewScheduleDefinition. It's an abstract type
    that is inherited by accessReviewQueryScope, principalResourceMembershipsScope, and accessReviewReviewerScope.

    For scope property on an accessReviewScheduleDefinition see accessReviewQueryScope and
    principalResourceMembershipsScope.

    For reviewers property on an accessReviewScheduleDefinition see accessReviewReviewerScope.

    Specifying the OData type in the scope is highly recommended for all types but required for
    principalResourceMembershipsScope and accessReviewInactiveUserQueryScope.
    """
