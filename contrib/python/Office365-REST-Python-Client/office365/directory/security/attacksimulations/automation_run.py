from typing import Optional

from office365.entity import Entity


class SimulationAutomationRun(Entity):
    """Represents a run of an attack simulation automation on a tenant."""

    @property
    def simulation_id(self):
        # type: () -> Optional[str]
        """"""
        return self.properties.get("simulationId", None)
