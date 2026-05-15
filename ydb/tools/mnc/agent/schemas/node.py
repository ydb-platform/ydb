from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class NodeStatusSchema:
    node: str
    running: bool
    enabled: bool = False
    by_agent: bool = False
    pid: Optional[int] = None
    pid_file: Optional[str] = None
    error: Optional[str] = None


@dataclass
class NodesResponseSchema:
    nodes: List[NodeStatusSchema] = field(default_factory=list)
    error: Optional[str] = None
    message: Optional[str] = None


@dataclass
class NodeServiceOperationSchema:
    node: str
    operation: str
    success: bool
    message: Optional[str] = None
    data: Optional[dict] = None


@dataclass
class NodeServiceOperationBatchSchema:
    operations: List[NodeServiceOperationSchema] = field(default_factory=list)


@dataclass
class StaticNodeParams:
    ic_port: int
    mon_port: int
    grpc_port: int


@dataclass
class DynamicNodeParams(StaticNodeParams):
    tenant: str
    pile_name: str = ""


@dataclass
class InstallNodesRequest:
    yaml_config: str
    node_broker_port: int = 2135
    static_node_params: List[StaticNodeParams] = field(default_factory=list)
    dynamic_node_params: List[DynamicNodeParams] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict):
        static_params = [StaticNodeParams(**item) for item in data.get("static_node_params", []) or []]
        dynamic_params = [DynamicNodeParams(**item) for item in data.get("dynamic_node_params", []) or []]
        return cls(
            yaml_config=data["yaml_config"],
            node_broker_port=data.get("node_broker_port", 2135),
            static_node_params=static_params,
            dynamic_node_params=dynamic_params,
        )


@dataclass
class InstallNodesResponse:
    nodes: List[str] = field(default_factory=list)
    error: Optional[str] = None
