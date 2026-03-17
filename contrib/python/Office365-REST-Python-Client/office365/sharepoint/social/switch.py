from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class SPSocialSwitch(Entity):
    """Provides methods to determine whether certain social features are enabled or disabled."""

    @staticmethod
    def is_following_feature_enabled(context):
        """
        Returns true if the SPSocial follow feature is enabled, taking into account the current context
        as appropriate. Specifically, if there is a SP.Web within the SP.RequestContext, this method will take into
        account whether the FollowingContent feature is activated within the SP.Web as well.
        Regardless of whether there is an SP.Web within the context, it will take into account if SPSocial
        is enabled at the tenant level.

        :type context: office365.sharepoint.client_context.ClientContext
        """
        binding_type = SPSocialSwitch(context)
        return_type = ClientResult(context)
        qry = ServiceOperationQuery(
            binding_type, "IsFollowingFeatureEnabled", None, None, None, return_type
        )
        qry.static = True
        context.add_query(qry)
        return return_type

    @property
    def entity_type_name(self):
        return "SP.Utilities.SPSocialSwitch"
