from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class LicenseAssignmentState(ClientValue):
    """
    The licenseAssignmentStates property of the user entity is a collection of licenseAssignmentState objects.
    It provides details about license assignments to a user. The details include information such as:

        - What plans are disabled for a user
        - Whether the license was assigned to the user directly or inherited from a group
        - The current state of the assignment
        - Error details if the assignment state is Error
    """

    def __init__(
        self,
        assigned_by_group=None,
        disabled_plans=None,
        error=None,
        last_updated_datetime=None,
        sku_id=None,
        state=None,
    ):
        """
        :param str assigned_by_group: ndicates whether the license is directly assigned or inherited from a group.
             If directly assigned, this field is null; if inherited through a group membership, this field contains
             the ID of the group.
        :param list[str] disabled_plans: The service plans that are disabled in this assignment
        :param str error: License assignment failure error. If the license is assigned successfully, this field is Null.
             The possible values are CountViolation, MutuallyExclusiveViolation, DependencyViolation,
             ProhibitedInUsageLocationViolation, UniquenessViolation, and Other.
             For more information on how to identify and resolve license assignment errors, see here.
        :param str sku_id: The unique identifier for the SKU. Read-Only.
        :param str state: Indicate the current state of this assignment. Read-Only.
             The possible values are Active, ActiveWithError, Disabled, and Error.
        """
        self.assignedByGroup = assigned_by_group
        self.disabledPlans = StringCollection(disabled_plans)
        self.error = error
        self.lastUpdatedDateTime = last_updated_datetime
        self.skuId = sku_id
        self.state = state
