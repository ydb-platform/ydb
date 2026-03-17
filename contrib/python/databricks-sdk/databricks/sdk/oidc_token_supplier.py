import logging
import os
from typing import Optional

import requests

logger = logging.getLogger("databricks.sdk")


# TODO: Check the required environment variables while creating the instance rather than in the get_oidc_token method to allow early return.
class GitHubOIDCTokenSupplier:
    """
    Supplies OIDC tokens from GitHub Actions.
    """

    def get_oidc_token(self, audience: str) -> Optional[str]:
        if "ACTIONS_ID_TOKEN_REQUEST_TOKEN" not in os.environ or "ACTIONS_ID_TOKEN_REQUEST_URL" not in os.environ:
            # not in GitHub actions
            return None
        # See https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-cloud-providers
        headers = {"Authorization": f"Bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"}
        endpoint = f"{os.environ['ACTIONS_ID_TOKEN_REQUEST_URL']}&audience={audience}"
        response = requests.get(endpoint, headers=headers)
        if not response.ok:
            return None

        # get the ID Token with aud=api://AzureADTokenExchange sub=repo:org/repo:environment:name
        response_json = response.json()
        if "value" not in response_json:
            return None

        return response_json["value"]


class AzureDevOpsOIDCTokenSupplier:
    """
    Supplies OIDC tokens from Azure DevOps pipelines.

    Constructs the OIDC token request URL using official Azure DevOps predefined variables.
    See: https://docs.microsoft.com/en-us/azure/devops/pipelines/build/variables
    """

    def __init__(self):
        """Initialize and validate Azure DevOps environment variables."""
        # Get Azure DevOps environment variables.
        self.access_token = os.environ.get("SYSTEM_ACCESSTOKEN")
        self.collection_uri = os.environ.get("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI")
        self.project_id = os.environ.get("SYSTEM_TEAMPROJECTID")
        self.plan_id = os.environ.get("SYSTEM_PLANID")
        self.job_id = os.environ.get("SYSTEM_JOBID")
        self.hub_name = os.environ.get("SYSTEM_HOSTTYPE")

        # Check for required variables with specific error messages.
        missing_vars = []
        if not self.access_token:
            missing_vars.append("SYSTEM_ACCESSTOKEN")
        if not self.collection_uri:
            missing_vars.append("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI")
        if not self.project_id:
            missing_vars.append("SYSTEM_TEAMPROJECTID")
        if not self.plan_id:
            missing_vars.append("SYSTEM_PLANID")
        if not self.job_id:
            missing_vars.append("SYSTEM_JOBID")
        if not self.hub_name:
            missing_vars.append("SYSTEM_HOSTTYPE")

        if missing_vars:
            if "SYSTEM_ACCESSTOKEN" in missing_vars:
                error_msg = "Azure DevOps OIDC: SYSTEM_ACCESSTOKEN env var not found. If calling from Azure DevOps Pipeline, please set this env var following https://learn.microsoft.com/en-us/azure/devops/pipelines/build/variables?view=azure-devops&tabs=yaml#systemaccesstoken"
            else:
                error_msg = f"Azure DevOps OIDC: missing required environment variables: {', '.join(missing_vars)}"
            raise ValueError(error_msg)

    def get_oidc_token(self, audience: str) -> Optional[str]:
        # Note: Azure DevOps OIDC tokens have a fixed audience of "api://AzureADTokenExchange".
        # The audience parameter is ignored but kept for interface compatibility with other OIDC suppliers.

        try:
            # Construct the OIDC token request URL.
            # Format: {collection_uri}{project_id}/_apis/distributedtask/hubs/{hubName}/plans/{planId}/jobs/{jobId}/oidctoken.
            request_url = f"{self.collection_uri}{self.project_id}/_apis/distributedtask/hubs/{self.hub_name}/plans/{self.plan_id}/jobs/{self.job_id}/oidctoken"

            # Add API version (audience is fixed to "api://AzureADTokenExchange" by Azure DevOps).
            endpoint = f"{request_url}?api-version=7.2-preview.1"
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Content-Length": "0",
            }

            # Azure DevOps OIDC endpoint requires POST request with empty body.
            response = requests.post(endpoint, headers=headers)
            if not response.ok:
                logger.debug(f"Azure DevOps OIDC: token request failed with status {response.status_code}")
                return None

            # Azure DevOps returns the token in 'oidcToken' field.
            response_json = response.json()
            if "oidcToken" not in response_json:
                logger.debug("Azure DevOps OIDC: response missing 'oidcToken' field")
                return None

            logger.debug("Azure DevOps OIDC: successfully obtained token")
            return response_json["oidcToken"]
        except Exception as e:
            logger.debug(f"Azure DevOps OIDC: failed to get token: {e}")
            return None
