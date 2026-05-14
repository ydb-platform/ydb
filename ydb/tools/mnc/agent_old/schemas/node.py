from pydantic import BaseModel, Field
from typing import Optional, List


class NodeStatusSchema(BaseModel):
    """Schema representing status of a single Kikimr node."""

    node: str = Field(..., description="Node name (folder under mnc_home)")
    running: bool = Field(..., description="Is process running right now")
    enabled: bool = Field(False, description="Is process enabled")
    by_agent: bool = Field(..., description="Process started / stopped by agent itself")
    pid: Optional[int] = Field(None, description="PID of the process if available")
    pid_file: Optional[str] = Field(None, description="Absolute path to pid file")
    error: Optional[str] = Field(None, description="An error message if status retrieval failed")


class NodesResponseSchema(BaseModel):
    """List of node statuses returned by GET /mnc_nodes."""

    nodes: List[NodeStatusSchema]
    error: Optional[str] = None
    message: Optional[str] = None


class NodeServiceOperationSchema(BaseModel):
    """Schema representing operation on a single Kikimr node."""

    node: str = Field(..., description="Node name (folder under mnc_home)")
    operation: str = Field(..., description="Operation to perform")
    success: bool = Field(..., description="Operation result")
    message: Optional[str] = Field(None, description="Operation message")
    data: Optional[dict] = Field(None, description="Additional data for the operation")


class NodeServiceOperationBatchSchema(BaseModel):
    operations: List[NodeServiceOperationSchema] = Field(..., description="List of operations")


class StaticNodeParams(BaseModel):
    ic_port: int = Field(..., description="IC port")
    mon_port: int = Field(..., description="MON port")
    grpc_port: int = Field(..., description="GRPC port")


class DynamicNodeParams(StaticNodeParams):
    tenant: str = Field(..., description="Tenant name")
    pile_name: str = Field(..., description="Pile name")


class InstallNodesRequest(BaseModel):
    yaml_config: str = Field(..., description="YAML config")
    node_broker_port: int = Field(2135, description="Node broker port")
    static_node_params: List[StaticNodeParams] = Field(None, description="Static nodes params")
    dynamic_node_params: List[DynamicNodeParams] = Field(None, description="Dynamic nodes params")


class InstallNodesResponse(BaseModel):
    nodes: List[str] = Field(..., description="List of nodes")
    error: Optional[str] = None
