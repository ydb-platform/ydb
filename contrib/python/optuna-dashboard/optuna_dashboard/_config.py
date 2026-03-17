from __future__ import annotations

import argparse
from dataclasses import dataclass
from dataclasses import fields
import sys
from typing import Any
from typing import TYPE_CHECKING
from typing import Literal

if TYPE_CHECKING:
    from optuna.artifacts._protocol import ArtifactStore

    from .llm.provider import LLMProvider


if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


def load_config_from_toml(config_path: str) -> dict[str, Any]:
    """Load configuration from TOML file.

    Args:
        config_path: Path to the TOML configuration file

    Returns:
        Dictionary containing configuration values

    Raises:
        ValueError: If the TOML file is malformed or missing required section
    """
    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
    except Exception as e:
        raise ValueError(f"Failed to read configuration file '{config_path}': {e}")

    return config


@dataclass
class DashboardConfig:
    """Configuration for Optuna Dashboard with priority handling."""

    storage: str | None = None
    storage_class: str | None = None
    port: int = 8080
    host: str = "127.0.0.1"
    server: Literal["auto", "wsgiref", "gunicorn"] = "auto"
    artifact_dir: str | None = None
    quiet: bool = False
    allow_unsafe: bool = False

    @classmethod
    def build_from_sources(
        cls, args: argparse.Namespace, toml_config: dict[str, Any] | None = None
    ) -> "DashboardConfig":
        """Build configuration from multiple sources with proper precedence."""

        toml_dashboard = toml_config.get("optuna_dashboard", {}) if toml_config else {}

        config = cls(
            **{
                field.name: cls._resolve_value(field.name, args, toml_dashboard, field.default)
                for field in fields(cls)
            }
        )

        return config

    @staticmethod
    def _resolve_value(
        field_name: str, args: argparse.Namespace, toml_section: dict[str, Any], default: Any
    ) -> Any:
        """Resolve configuration value using precedence: CLI > TOML > default."""

        # CLI argument takes priority.
        cli_val = getattr(args, field_name, None)
        if cli_val is not None:
            return cli_val

        # TOML configuration second.
        toml_val = toml_section.get(field_name)
        if toml_val is not None:
            return toml_val

        # Default value last.
        return default


def create_llm_provider_from_config(config: dict[str, Any]) -> LLMProvider | None:
    if "llm" not in config:
        return None

    llm_config = config["llm"]

    if "openai" in llm_config:
        import optuna_dashboard.llm.openai
        import openai

        client = openai.OpenAI(**llm_config["openai"]["client"])
        return optuna_dashboard.llm.openai.OpenAI(
            client,
            model=llm_config["openai"]["model"],
            use_chat_completions_api=llm_config["openai"].get("use_chat_completions_api", False),
        )
    elif "azure_openai" in llm_config:
        import optuna_dashboard.llm.openai
        import openai

        client = openai.AzureOpenAI(**llm_config["azure_openai"]["client"])
        return optuna_dashboard.llm.openai.AzureOpenAI(
            client,
            model=llm_config["azure_openai"]["model"],
            use_chat_completions_api=llm_config["azure_openai"].get(
                "use_chat_completions_api", False
            ),
        )
    else:
        raise ValueError(
            "Unsupported LLM provider. Supported providers: 'openai', 'azure_openai'."
        )


def create_artifact_store_from_config(config: dict[str, Any]) -> ArtifactStore | None:
    if "artifact_store" not in config:
        return None

    artifact_store: ArtifactStore | None = None
    if "boto3" in config["artifact_store"]:
        from optuna.artifacts import Boto3ArtifactStore

        artifact_store = Boto3ArtifactStore(**config["artifact_store"]["boto3"])
    elif "gcs" in config["artifact_store"]:
        from optuna.artifacts import GCSArtifactStore

        artifact_store = GCSArtifactStore(**config["artifact_store"]["gcs"])
    elif "filesystem" in config["artifact_store"]:
        from optuna.artifacts import FileSystemArtifactStore

        artifact_store = FileSystemArtifactStore(**config["artifact_store"]["filesystem"])
    else:
        raise ValueError("Unsupported artifact store configuration.")

    return artifact_store
