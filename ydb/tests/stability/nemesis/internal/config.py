from functools import lru_cache
import os
import sys
from typing import Any, TypeVar, Type

T = TypeVar('T', bound='BaseSettings')


class BaseSettings:
    """Base settings class that loads from defaults, environment variables, and allows override."""

    def __init__(self, **kwargs):
        """
        Initialize settings with priority: kwargs > env vars > defaults.

        Args:
            **kwargs: Override values (typically from command-line arguments)
        """
        # Get all class attributes that are not methods or private
        for key, value in self.__class__.__dict__.items():
            if not key.startswith('_') and not callable(value):
                # Start with default value
                default_value = value

                # Try to get from environment variable (convert field name to uppercase)
                env_var_name = key.upper()
                env_value = os.getenv(env_var_name)

                # Set value with priority: kwargs > env > default
                if key in kwargs and kwargs[key] is not None:
                    setattr(self, key, kwargs[key])
                elif env_value is not None:
                    setattr(self, key, self._convert_type(env_value, type(default_value)))
                else:
                    setattr(self, key, default_value)

    def _convert_type(self, value: str, target_type: type) -> Any:
        """Convert string value to target type."""
        if target_type == int:
            return int(value)
        elif target_type == bool:
            return value.lower() in ('true', '1', 'yes', 'on')
        elif target_type == list:
            # Handle list parsing (comma-separated)
            return [item.strip() for item in value.split(',') if item.strip()]
        else:
            return value

    def __repr__(self) -> str:
        """String representation of settings."""
        attrs = []
        for key, value in self.__dict__.items():
            attrs.append(f"{key}={repr(value)}")
        return f"{self.__class__.__name__}({', '.join(attrs)})"

    @classmethod
    def from_args(cls: Type[T], **kwargs) -> T:
        """
        Create Settings instance with argv arguments having highest priority.
        Priority: argv > env > default values
        """
        return cls(**kwargs)


class Settings(BaseSettings):
    app_name: str = "Nemesis"
    nemesis_type: str = 'orchestrator'
    static_location: str = 'static'
    hosts: list[str] = []
    app_host: str = '::'
    app_port: int = 31434
    mon_port: int = 8765
    nemesis_mon_port: int = 8666
    yaml_config_location: str = ''
    database_config_location: str = ''
    # Remote install layout (rsync targets, systemd ExecStart); override via NEMESIS_INSTALL_ROOT.
    install_root: str = '/Berkanavt/nemesis'
    # Log directory on agents for safety wardens (grep kikimr.start, etc.); override via KIKIMR_LOGS_DIRECTORY.
    kikimr_logs_directory: str = '/Berkanavt/kikimr/logs/'
    # When true, the catalog is filtered to runners with supports_local_mode=True.
    # Set by ydb/tests/tools/local_cluster when launching nemesis as a subprocess.
    local_mode: bool = False


class AgentSettings(BaseSettings):
    app_name: str = "Nemesis Agent API"
    nemesis_type: str = 'agent'
    app_host: str = '::'
    app_port: int = 31434
    mon_port: int = 8765
    nemesis_mon_port: int = 8666
    install_root: str = '/Berkanavt/nemesis'
    kikimr_logs_directory: str = '/Berkanavt/kikimr/logs/'
    # Same cluster.yaml as orchestrator when deployed via install (env YAML_CONFIG_LOCATION).
    yaml_config_location: str = ''
    database_config_location: str = ''

    @classmethod
    def from_orchestrator_args(cls: Type['AgentSettings'], settings: Settings) -> 'AgentSettings':
        """Create AgentSettings from orchestrator Settings."""
        root = settings.install_root.rstrip("/")
        yaml_on_agent = f"{root}/cluster.yaml" if settings.yaml_config_location else ""
        database_yaml_on_agent = f"{root}/cluster_database.yaml" if settings.database_config_location else None
        return cls(
            app_host=settings.app_host,
            app_port=settings.app_port,
            mon_port=settings.mon_port,
            nemesis_mon_port=settings.nemesis_mon_port,
            install_root=settings.install_root,
            kikimr_logs_directory=settings.kikimr_logs_directory,
            yaml_config_location=yaml_on_agent,
            database_config_location=database_yaml_on_agent,
        )


@lru_cache
def get_orchestrator_settings(**kwargs) -> Settings:
    """Get orchestrator-mode settings with argv arguments having highest priority."""
    settings = Settings.from_args(**kwargs)
    print(settings, file=sys.stderr)
    return settings
