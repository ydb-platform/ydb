from office365.runtime.client_value import ClientValue


class AttackSimulationUser(ClientValue):
    """Represents a user in an attack simulation and training campaign."""

    def __init__(self, display_name=None, email=None, user_id=None):
        self.displayName = display_name
        self.email = email
        self.userId = user_id
