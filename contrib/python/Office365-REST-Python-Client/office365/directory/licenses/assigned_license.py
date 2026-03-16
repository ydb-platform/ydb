from office365.runtime.client_value import ClientValue
from office365.runtime.types.collections import StringCollection


class AssignedLicense(ClientValue):
    """
    Represents a license assigned to a user. The assignedLicenses property of the user entity is a collection
    of assignedLicense.
    """

    def __init__(self, sku_id=None, disabled_plans=None):
        """
        :param str sku_id: The unique identifier for the SKU.
        :param list[str] disabled_plans: A collection of the unique identifiers for plans that have been disabled.
        """
        super(AssignedLicense, self).__init__()
        self.skuId = sku_id
        self.disabledPlans = StringCollection(disabled_plans)

    def __repr__(self):
        return self.skuId
