from dataclasses import dataclass, field
from typing import Optional

from ydb.tools.mnc.viewer.widgets.config_models import ConfigCandidate, SelectedClusterConfig
from ydb.tools.mnc.viewer.widgets.operation_models import OperationState


@dataclass
class AgentHostStatus:
    host: str
    status: str
    message: str = ""
    enabled_features: list[str] = field(default_factory=list)
    last_tasks: list[dict] = field(default_factory=list)
    tasks_error: str = ""
    disks: list[dict] = field(default_factory=list)
    disks_error: str = ""

    def agent_is_running(self) -> bool:
        return self.status in ("Running", "OK")


@dataclass
class AgentsState:
    status: str = "NOT SELECTED"
    hosts: list[AgentHostStatus] = field(default_factory=list)

    def ok_count(self) -> int:
        return sum(1 for host in self.hosts if host.agent_is_running())

    def total_count(self) -> int:
        return len(self.hosts)


@dataclass
class ViewerState:
    mnc_config_ok: bool
    selected_cluster_config: Optional[SelectedClusterConfig] = None
    agents: AgentsState = field(default_factory=AgentsState)
    operation: OperationState = field(default_factory=OperationState)

    def mnc_config_status(self) -> str:
        return "OK" if self.mnc_config_ok else "ERROR"

    def cluster_config_status(self) -> str:
        if self.selected_cluster_config is None:
            return "NOT SELECTED"
        return self.selected_cluster_config.candidate.name

    def cluster_config_status_kind(self) -> str:
        if self.selected_cluster_config is None:
            return "NOT SELECTED"
        return "OK" if self.selected_cluster_config.validation.ok else "FAIL"

    def agents_status(self) -> str:
        if self.agents.status in ("NOT SELECTED", "CHECKING"):
            return self.agents.status
        if self.agents.total_count() == 0:
            return self.agents.status
        return self.agents.status

    def agents_status_kind(self) -> str:
        return self.agents.status

    def agents_details(self) -> str:
        if self.agents.status == "NOT SELECTED":
            return "Select cluster config to inspect hosts"
        if self.agents.status == "CHECKING":
            return "Checking agents on " + ", ".join(host.host for host in self.agents.hosts)
        if not self.agents.hosts:
            return "No hosts in selected cluster config"
        details = [f"Agents: {self.agents.ok_count()}/{self.agents.total_count()}"]
        details.extend(
            f"{host.host}: {host.status}" + (f" ({host.message})" if host.message else "")
            for host in self.agents.hosts
            if not host.agent_is_running() or host.message
        )
        return "\n".join(details)

    def is_selected_cluster_config(self, candidate: ConfigCandidate) -> bool:
        return (
            self.selected_cluster_config is not None
            and self.selected_cluster_config.candidate.path == candidate.path
        )
