from office365.directory.security.attacksimulations.user import AttackSimulationUser
from office365.runtime.client_value import ClientValue


class UserSimulationDetails(ClientValue):
    """Represents a user of a tenant and their online actions in an attack simulation and training campaign."""

    def __init__(
        self, assigned_trainings_count=None, simulation_user=AttackSimulationUser()
    ):
        """
        :param int assigned_trainings_count: Number of trainings assigned to a user in an attack simulation
            and training campaign.
        :param AttackSimulationUser simulation_user:
        """
        self.assignedTrainingsCount = assigned_trainings_count
        self.simulationUser = simulation_user
