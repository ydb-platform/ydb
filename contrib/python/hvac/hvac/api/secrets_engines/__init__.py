"""Vault secrets engines endpoints"""
from hvac.api.secrets_engines.active_directory import ActiveDirectory
from hvac.api.secrets_engines.aws import Aws
from hvac.api.secrets_engines.azure import Azure
from hvac.api.secrets_engines.consul import Consul
from hvac.api.secrets_engines.database import Database
from hvac.api.secrets_engines.gcp import Gcp
from hvac.api.secrets_engines.identity import Identity
from hvac.api.secrets_engines.kv import Kv
from hvac.api.secrets_engines.kv_v1 import KvV1
from hvac.api.secrets_engines.kv_v2 import KvV2
from hvac.api.secrets_engines.ldap import Ldap
from hvac.api.secrets_engines.pki import Pki
from hvac.api.secrets_engines.rabbitmq import RabbitMQ
from hvac.api.secrets_engines.ssh import Ssh
from hvac.api.secrets_engines.transform import Transform
from hvac.api.secrets_engines.transit import Transit
from hvac.api.vault_api_category import VaultApiCategory

__all__ = (
    "Aws",
    "Azure",
    "Gcp",
    "ActiveDirectory",
    "Identity",
    "Kv",
    "KvV1",
    "KvV2",
    "Ldap",
    "Pki",
    "Transform",
    "Transit",
    "SecretsEngines",
    "Database",
    "RabbitMQ",
    "Ssh",
)


class SecretsEngines(VaultApiCategory):
    """Secrets Engines."""

    implemented_classes = [
        Aws,
        Azure,
        Gcp,
        ActiveDirectory,
        Identity,
        Kv,
        Ldap,
        Pki,
        Transform,
        Transit,
        Database,
        Consul,
        RabbitMQ,
        Ssh,
    ]
    unimplemented_classes = [
        "AliCloud",
        "Azure",
        "GcpKms",
        "Nomad",
        "Ssh",
        "TOTP",
        "Cassandra",
        "MongoDb",
        "Mssql",
        "MySql",
        "PostgreSql",
    ]
