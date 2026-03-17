from office365.directory.security.attacksimulations.user import AttackSimulationUser
from office365.runtime.client_value import ClientValue


class AttackSimulationRepeatOffender(ClientValue):
    """Represents a user in a tenant who has given way to attacks more than once across various attack simulation
    and training campaigns."""

    def __init__(
        self, attack_simulation_user=AttackSimulationUser(), repeat_offence_count=None
    ):
        self.attackSimulationUser = attack_simulation_user
        self.repeatOffenceCount = repeat_offence_count
