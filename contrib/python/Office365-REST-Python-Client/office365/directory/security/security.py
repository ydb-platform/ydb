from office365.directory.security.alerts.alert import Alert
from office365.directory.security.alerts.collection import AlertCollection
from office365.directory.security.attacksimulations.root import AttackSimulationRoot
from office365.directory.security.cases.root import CasesRoot
from office365.directory.security.hunting_query_results import HuntingQueryResults
from office365.directory.security.incidents.incident import Incident
from office365.directory.security.scorecontrol.profile import SecureScoreControlProfile
from office365.directory.security.threatintelligence.threat_intelligence import (
    ThreatIntelligence,
)
from office365.directory.security.triggers.root import TriggersRoot
from office365.entity import Entity
from office365.entity_collection import EntityCollection
from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.queries.service_operation import ServiceOperationQuery


class Security(Entity):
    """The security resource is the entry point for the Security object model. It returns a singleton security resource.
    It doesn't contain any usable properties."""

    def run_hunting_query(self, query):
        """
        Queries a specified set of event, activity, or entity data supported by Microsoft 365 Defender
        to proactively look for specific threats in your environment.
        :param str query: The hunting query in Kusto Query Language (KQL). For more information on KQL syntax,see KQL
            quick reference: https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/kql-quick-reference
        """
        return_type = ClientResult(self.context, HuntingQueryResults())
        payload = {"Query": query}
        qry = ServiceOperationQuery(
            self, "runHuntingQuery", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def alerts(self):
        # type: () -> AlertCollection
        return self.properties.get(
            "alerts",
            AlertCollection(self.context, ResourcePath("alerts", self.resource_path)),
        )

    @property
    def alerts_v2(self):
        # type: () -> EntityCollection[Alert]
        """A collection of alerts in Microsoft 365 Defender."""
        return self.properties.get(
            "alerts_v2",
            EntityCollection(
                self.context, Alert, ResourcePath("alerts_v2", self.resource_path)
            ),
        )

    @property
    def cases(self):
        """"""
        return self.properties.get(
            "cases", CasesRoot(self.context, ResourcePath("cases", self.resource_path))
        )

    @property
    def attack_simulation(self):
        """"""
        return self.properties.get(
            "attackSimulation",
            AttackSimulationRoot(
                self.context, ResourcePath("attackSimulation", self.resource_path)
            ),
        )

    @property
    def incidents(self):
        # type: () -> EntityCollection[Incident]
        """A collection of correlated alert instances and associated metadata that reflects the story of
        an attack in a tenant"""
        return self.properties.get(
            "incidents",
            EntityCollection(
                self.context, Incident, ResourcePath("incidents", self.resource_path)
            ),
        )

    @property
    def secure_score_control_profiles(self):
        """"""
        return self.properties.get(
            "secureScoreControlProfiles",
            EntityCollection(
                self.context,
                SecureScoreControlProfile,
                ResourcePath("secureScoreControlProfiles", self.resource_path),
            ),
        )

    @property
    def triggers(self):
        """"""
        return self.properties.get(
            "triggers",
            TriggersRoot(self.context, ResourcePath("triggers", self.resource_path)),
        )

    @property
    def threat_intelligence(self):
        """"""
        return self.properties.get(
            "threatIntelligence",
            ThreatIntelligence(
                self.context, ResourcePath("threatIntelligence", self.resource_path)
            ),
        )

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "alerts_v2": self.alerts_v2,
                "attackSimulation": self.attack_simulation,
                "secureScoreControlProfiles": self.secure_score_control_profiles,
                "threatIntelligence": self.threat_intelligence,
            }
            default_value = property_mapping.get(name, None)
        return super(Security, self).get_property(name, default_value)

    @property
    def entity_type_name(self):
        # type: () -> str
        return "microsoft.graph.security.alert"
