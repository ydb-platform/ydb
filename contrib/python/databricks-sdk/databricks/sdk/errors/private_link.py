from dataclasses import dataclass
from urllib import parse

import requests

from ..environments import Cloud, get_environment_for_hostname
from .platform import PermissionDenied


@dataclass
class _PrivateLinkInfo:
    serviceName: str
    endpointName: str
    referencePage: str

    def error_message(self):
        return (
            f"The requested workspace has {self.serviceName} enabled and is not accessible from the current network. "
            f"Ensure that {self.serviceName} is properly configured and that your device has access to the "
            f"{self.endpointName}. For more information, see {self.referencePage}."
        )


_private_link_info_map = {
    Cloud.AWS: _PrivateLinkInfo(
        serviceName="AWS PrivateLink",
        endpointName="AWS VPC endpoint",
        referencePage="https://docs.databricks.com/en/security/network/classic/privatelink.html",
    ),
    Cloud.AZURE: _PrivateLinkInfo(
        serviceName="Azure Private Link",
        endpointName="Azure Private Link endpoint",
        referencePage="https://learn.microsoft.com/en-us/azure/databricks/security/network/classic/private-link-standard#authentication-troubleshooting",
    ),
    Cloud.GCP: _PrivateLinkInfo(
        serviceName="Private Service Connect",
        endpointName="GCP VPC endpoint",
        referencePage="https://docs.gcp.databricks.com/en/security/network/classic/private-service-connect.html",
    ),
}


class PrivateLinkValidationError(PermissionDenied):
    """Raised when a user tries to access a Private Link-enabled workspace, but the user's network does not have access
    to the workspace."""


def _is_private_link_redirect(resp: requests.Response) -> bool:
    parsed = parse.urlparse(resp.url)
    return parsed.path == "/login.html" and "error=private-link-validation-error" in parsed.query


def _get_private_link_validation_error(url: str) -> PrivateLinkValidationError:
    parsed = parse.urlparse(url)
    env = get_environment_for_hostname(parsed.hostname)
    return PrivateLinkValidationError(
        message=_private_link_info_map[env.cloud].error_message(),
        error_code="PRIVATE_LINK_VALIDATION_ERROR",
        status_code=403,
    )
