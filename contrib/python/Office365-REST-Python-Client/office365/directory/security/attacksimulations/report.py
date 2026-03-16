from office365.directory.security.attacksimulations.report_overview import (
    SimulationReportOverview,
)
from office365.directory.security.attacksimulations.users.details import (
    UserSimulationDetails,
)
from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection


class SimulationReport(ClientValue):
    """
    Represents a report of an attack simulation and training campaign, including an overview and users who
    participated in the campaign.
    """

    def __init__(self, overview=SimulationReportOverview(), simulation_users=None):
        """
        :param SimulationReportOverview overview: Overview of an attack simulation and training campaign.
        """
        self.overview = overview
        self.simulationUsers = ClientValueCollection(
            UserSimulationDetails, simulation_users
        )
