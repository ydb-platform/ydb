from office365.directory.security.attacksimulations.report import SimulationReport
from office365.entity import Entity


class Simulation(Entity):
    """
    Represents an attack simulation training campaign in a tenant.

    Attack simulation and training is a service available as part of Microsoft Defender for Office 365.
    This service lets tenant users experience a realistic benign phishing attack and learn from it.
    The service enables tenant administrators to simulate, assign trainings, and read derived insights into online
    behaviors of users in the phishing simulations. The service provides attack simulation reports that help tenants
    identify security knowledge gaps, so that they can further train their users to decrease their susceptibility
    to attacks.

    The attack simulation and training API enables tenant administrators to list launched simulation exercises
    and trainings, and get reports on derived insights into online behaviors of users in the phishing simulations.
    """

    @property
    def report(self):
        """Report of the attack simulation and training campaign."""
        return self.properties.get("report", SimulationReport())
