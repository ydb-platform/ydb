from typing import Dict

from .oauth import TokenSource
from .service.provisioning import Workspace


def add_workspace_id_header(cfg: "Config", headers: Dict[str, str]):
    if cfg.azure_workspace_resource_id:
        headers["X-Databricks-Azure-Workspace-Resource-Id"] = cfg.azure_workspace_resource_id


def add_sp_management_token(token_source: "TokenSource", headers: Dict[str, str]):
    mgmt_token = token_source.token()
    headers["X-Databricks-Azure-SP-Management-Token"] = mgmt_token.access_token


def get_azure_resource_id(workspace: Workspace):
    """
    Returns the Azure Resource ID for the given workspace, if it is an Azure workspace.
    :param workspace:
    :return:
    """
    if workspace.azure_workspace_info is None:
        return None
    return (
        f"/subscriptions/{workspace.azure_workspace_info.subscription_id}"
        f"/resourceGroups/{workspace.azure_workspace_info.resource_group}"
        f"/providers/Microsoft.Databricks/workspaces/{workspace.workspace_name}"
    )
