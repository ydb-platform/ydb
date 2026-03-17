from enum import Enum


class HostType(Enum):
    """Enum representing the type of Databricks host."""

    ACCOUNTS = "accounts"
    WORKSPACE = "workspace"
    UNIFIED = "unified"


class ClientType(Enum):
    """Enum representing the type of client configuration."""

    ACCOUNT = "account"
    WORKSPACE = "workspace"
