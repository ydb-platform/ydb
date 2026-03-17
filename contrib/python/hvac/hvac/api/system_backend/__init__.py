"""Collection of Vault system backend API endpoint classes."""
import logging

from hvac.api.system_backend.audit import Audit
from hvac.api.system_backend.auth import Auth
from hvac.api.system_backend.capabilities import Capabilities
from hvac.api.system_backend.health import Health
from hvac.api.system_backend.init import Init
from hvac.api.system_backend.key import Key
from hvac.api.system_backend.leader import Leader
from hvac.api.system_backend.lease import Lease
from hvac.api.system_backend.mount import Mount
from hvac.api.system_backend.namespace import Namespace
from hvac.api.system_backend.policies import Policies
from hvac.api.system_backend.policy import Policy
from hvac.api.system_backend.quota import Quota
from hvac.api.system_backend.raft import Raft
from hvac.api.system_backend.seal import Seal
from hvac.api.system_backend.system_backend_mixin import SystemBackendMixin
from hvac.api.system_backend.wrapping import Wrapping
from hvac.api.vault_api_category import VaultApiCategory

__all__ = (
    "Audit",
    "Auth",
    "Capabilities",
    "Health",
    "Init",
    "Key",
    "Leader",
    "Lease",
    "Mount",
    "Namespace",
    "Policies",
    "Policy",
    "Quota",
    "Raft",
    "Seal",
    "SystemBackend",
    "SystemBackendMixin",
    "Wrapping",
)


logger = logging.getLogger(__name__)


class SystemBackend(
    VaultApiCategory,
    Audit,
    Auth,
    Capabilities,
    Health,
    Init,
    Key,
    Leader,
    Lease,
    Mount,
    Namespace,
    Policies,
    Policy,
    Quota,
    Raft,
    Seal,
    Wrapping,
):
    implemented_classes = [
        Audit,
        Auth,
        Capabilities,
        Health,
        Init,
        Key,
        Leader,
        Lease,
        Mount,
        Namespace,
        Policies,
        Policy,
        Quota,
        Raft,
        Seal,
        Wrapping,
    ]
    unimplemented_classes = []
