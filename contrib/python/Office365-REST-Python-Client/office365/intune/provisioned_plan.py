from office365.runtime.client_value import ClientValue


class ProvisionedPlan(ClientValue):
    """
    The provisionedPlans property of the user entity and the organization entity is a collection of provisionedPlan.
    """

    def __init__(self, service=None, provisioning_status=None, capability_status=None):
        """
        :param str service:
        :param str provisioning_status:
        :param str capability_status:
        """
        self.service = service
        self.provisioningStatus = provisioning_status
        self.capabilityStatus = capability_status

    def __repr__(self):
        return self.service
