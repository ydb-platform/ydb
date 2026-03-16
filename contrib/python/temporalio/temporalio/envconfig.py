"""Environment and file-based configuration for Temporal clients.

This module provides utilities to load Temporal client configuration from TOML files
and environment variables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Literal, Mapping, Optional, Union, cast

from typing_extensions import TypeAlias, TypedDict

import temporalio.service
from temporalio.bridge.temporal_sdk_bridge import envconfig as _bridge_envconfig

DataSource: TypeAlias = Union[
    Path, str, bytes
]  # str represents a file contents, bytes represents raw data


# We define typed dictionaries for what these configs look like as TOML.
class ClientConfigTLSDict(TypedDict, total=False):
    """Dictionary representation of TLS config for TOML."""

    disabled: bool
    server_name: str
    server_ca_cert: Mapping[str, str]
    client_cert: Mapping[str, str]
    client_key: Mapping[str, str]


class ClientConfigProfileDict(TypedDict, total=False):
    """Dictionary representation of a client config profile for TOML."""

    address: str
    namespace: str
    api_key: str
    tls: ClientConfigTLSDict
    grpc_meta: Mapping[str, str]


def _from_dict_to_source(d: Optional[Mapping[str, Any]]) -> Optional[DataSource]:
    if not d:
        return None
    if "data" in d:
        return d["data"]
    if "path" in d:
        return Path(d["path"])
    return None


def _source_to_dict(
    source: Optional[DataSource],
) -> Optional[Mapping[str, str]]:
    if isinstance(source, Path):
        return {"path": str(source)}
    if isinstance(source, str):
        return {"data": source}
    if isinstance(source, bytes):
        return {"data": source.decode("utf-8")}
    return None


def _source_to_path_and_data(
    source: Optional[DataSource],
) -> tuple[Optional[str], Optional[bytes]]:
    path: Optional[str] = None
    data: Optional[bytes] = None
    if isinstance(source, Path):
        path = str(source)
    elif isinstance(source, str):
        data = source.encode("utf-8")
    elif isinstance(source, bytes):
        data = source
    elif source is not None:
        raise TypeError(
            "config_source must be one of pathlib.Path, str, bytes, or None, "
            f"but got {type(source).__name__}"
        )
    return path, data


def _read_source(source: Optional[DataSource]) -> Optional[bytes]:
    if source is None:
        return None
    if isinstance(source, Path):
        with open(source, "rb") as f:
            return f.read()
    if isinstance(source, str):
        return source.encode("utf-8")
    if isinstance(source, bytes):
        return source
    raise TypeError(
        f"Source must be one of pathlib.Path, str, or bytes, but got {type(source).__name__}"
    )


@dataclass(frozen=True)
class ClientConfigTLS:
    """TLS configuration as specified as part of client configuration

    .. warning::
        Experimental API.
    """

    disabled: bool = False
    """If true, TLS is explicitly disabled."""
    server_name: Optional[str] = None
    """SNI override."""
    server_root_ca_cert: Optional[DataSource] = None
    """Server CA certificate source."""
    client_cert: Optional[DataSource] = None
    """Client certificate source."""
    client_private_key: Optional[DataSource] = None
    """Client key source."""

    def to_dict(self) -> ClientConfigTLSDict:
        """Convert to a dictionary that can be used for TOML serialization."""
        d: ClientConfigTLSDict = {}
        if self.disabled:
            d["disabled"] = self.disabled
        if self.server_name is not None:
            d["server_name"] = self.server_name

        def set_source(
            key: Literal["server_ca_cert", "client_cert", "client_key"],
            source: Optional[DataSource],
        ):
            if source is not None and (val := _source_to_dict(source)):
                d[key] = val

        set_source("server_ca_cert", self.server_root_ca_cert)
        set_source("client_cert", self.client_cert)
        set_source("client_key", self.client_private_key)
        return d

    def to_connect_tls_config(self) -> Union[bool, temporalio.service.TLSConfig]:
        """Create a `temporalio.service.TLSConfig` from this profile."""
        if self.disabled:
            return False

        return temporalio.service.TLSConfig(
            domain=self.server_name,
            server_root_ca_cert=_read_source(self.server_root_ca_cert),
            client_cert=_read_source(self.client_cert),
            client_private_key=_read_source(self.client_private_key),
        )

    @staticmethod
    def from_dict(d: Optional[ClientConfigTLSDict]) -> Optional[ClientConfigTLS]:
        """Create a ClientConfigTLS from a dictionary."""
        if not d:
            return None
        return ClientConfigTLS(
            disabled=d.get("disabled", False),
            server_name=d.get("server_name"),
            # Note: Bridge uses snake_case, but TOML uses kebab-case which is
            # converted to snake_case. Core has server_ca_cert, client_key.
            server_root_ca_cert=_from_dict_to_source(d.get("server_ca_cert")),
            client_cert=_from_dict_to_source(d.get("client_cert")),
            client_private_key=_from_dict_to_source(d.get("client_key")),
        )


class ClientConnectConfig(TypedDict, total=False):
    """Arguments for `temporalio.client.Client.connect` that are configurable via
    environment configuration.

    .. warning::
        Experimental API.
    """

    target_host: str
    namespace: str
    api_key: str
    tls: Union[bool, temporalio.service.TLSConfig]
    rpc_metadata: Mapping[str, str]


@dataclass(frozen=True)
class ClientConfigProfile:
    """Represents a client configuration profile.

    This class holds the configuration as loaded from a file or environment.
    See `to_connect_config` to transform the profile to `ClientConnectConfig`,
    which can be used to create a client.

    .. warning::
        Experimental API.
    """

    address: Optional[str] = None
    """Client address."""
    namespace: Optional[str] = None
    """Client namespace."""
    api_key: Optional[str] = None
    """Client API key."""
    tls: Optional[ClientConfigTLS] = None
    """TLS configuration."""
    grpc_meta: Mapping[str, str] = field(default_factory=dict)
    """gRPC metadata."""

    @staticmethod
    def from_dict(d: ClientConfigProfileDict) -> ClientConfigProfile:
        """Create a ClientConfigProfile from a dictionary."""
        return ClientConfigProfile(
            address=d.get("address"),
            namespace=d.get("namespace"),
            api_key=d.get("api_key"),
            tls=ClientConfigTLS.from_dict(d.get("tls")),
            grpc_meta=d.get("grpc_meta") or {},
        )

    def to_dict(self) -> ClientConfigProfileDict:
        """Convert to a dictionary that can be used for TOML serialization."""
        d: ClientConfigProfileDict = {}
        if self.address is not None:
            d["address"] = self.address
        if self.namespace is not None:
            d["namespace"] = self.namespace
        if self.api_key is not None:
            d["api_key"] = self.api_key
        if self.tls and (tls_dict := self.tls.to_dict()):
            d["tls"] = tls_dict
        if self.grpc_meta:
            d["grpc_meta"] = self.grpc_meta
        return d

    def to_client_connect_config(self) -> ClientConnectConfig:
        """Create a `ClientConnectConfig` from this profile."""
        # Only include non-None values
        config: Dict[str, Any] = {}
        if self.address:
            config["target_host"] = self.address
        if self.namespace is not None:
            config["namespace"] = self.namespace
        if self.api_key is not None:
            config["api_key"] = self.api_key
        if self.tls is not None:
            config["tls"] = self.tls.to_connect_tls_config()
        if self.grpc_meta:
            config["rpc_metadata"] = self.grpc_meta

        # Cast to ClientConnectConfig - this is safe because we've only included non-None values
        return cast(ClientConnectConfig, config)

    @staticmethod
    def load(
        profile: Optional[str] = None,
        *,
        config_source: Optional[DataSource] = None,
        disable_file: bool = False,
        disable_env: bool = False,
        config_file_strict: bool = False,
        override_env_vars: Optional[Mapping[str, str]] = None,
    ) -> ClientConfigProfile:
        """Load a single client profile from given sources, applying env
        overrides.

        To get a :py:class:`ClientConnectConfig`, use the
        :py:meth:`to_client_connect_config` method on the returned profile.

        Args:
            profile: Profile to load from the config.
            config_source: If present, this is used as the configuration source
                instead of default file locations. This can be a path to the file
                or the string/byte contents of the file.
            disable_file: If true, file loading is disabled. This is only used
                when ``config_source`` is not present.
            disable_env: If true, environment variable loading and overriding
                is disabled. This takes precedence over the ``override_env_vars``
                parameter.
            config_file_strict: If true, will error on unrecognized keys.
            override_env_vars: The environment to use for loading and overrides.
                If not provided, the current process's environment is used. To
                use a specific set of environment variables, provide them here.
                To disable environment variable loading, set ``disable_env`` to
                true.

        Returns:
            The client configuration profile.
        """
        path, data = _source_to_path_and_data(config_source)

        raw_profile = _bridge_envconfig.load_client_connect_config(
            profile=profile,
            path=path,
            data=data,
            disable_file=disable_file,
            disable_env=disable_env,
            config_file_strict=config_file_strict,
            env_vars=override_env_vars,
        )
        return ClientConfigProfile.from_dict(raw_profile)


@dataclass
class ClientConfig:
    """Client configuration loaded from TOML and environment variables.

    This contains a mapping of profile names to client profiles. Use
    `ClientConfigProfile.to_connect_config` to create a `ClientConnectConfig`
    from a profile. See `load_profile` to load an individual profile.

    .. warning::
        Experimental API.
    """

    profiles: Mapping[str, ClientConfigProfile]
    """Map of profile name to its corresponding ClientConfigProfile."""

    def to_dict(self) -> Mapping[str, ClientConfigProfileDict]:
        """Convert to a dictionary that can be used for TOML serialization."""
        return {k: v.to_dict() for k, v in self.profiles.items()}

    @staticmethod
    def from_dict(
        d: Mapping[str, Mapping[str, Any]],
    ) -> ClientConfig:
        """Create a ClientConfig from a dictionary."""
        # We must cast the inner dictionary because the source is often a plain
        # Mapping[str, Any] from the bridge or other sources.
        return ClientConfig(
            profiles={
                k: ClientConfigProfile.from_dict(cast(ClientConfigProfileDict, v))
                for k, v in d.items()
            }
        )

    @staticmethod
    def load(
        *,
        config_source: Optional[DataSource] = None,
        disable_file: bool = False,
        config_file_strict: bool = False,
        override_env_vars: Optional[Mapping[str, str]] = None,
    ) -> ClientConfig:
        """Load all client profiles from given sources.

        This does not apply environment variable overrides to the profiles, it
        only uses an environment variable to find the default config file path
        (``TEMPORAL_CONFIG_FILE``). To get a single profile with environment variables
        applied, use :py:meth:`ClientConfigProfile.load`.

        Args:
            config_source: If present, this is used as the configuration source
                instead of default file locations. This can be a path to the file
                or the string/byte contents of the file.
            disable_file: If true, file loading is disabled. This is only used
                when ``config_source`` is not present.
            config_file_strict: If true, will TOML file parsing will error on
                unrecognized keys.
            override_env_vars: The environment variables to use for locating the
                default config file. If not provided, the current process's
                environment is used to check for ``TEMPORAL_CONFIG_FILE``. To
                use a specific set of environment variables, provide them here.
                To disable environment variable loading, set ``disable_file`` to
                true or pass an empty dictionary for this parameter.
        """
        path, data = _source_to_path_and_data(config_source)

        loaded_profiles = _bridge_envconfig.load_client_config(
            path=path,
            data=data,
            disable_file=disable_file,
            config_file_strict=config_file_strict,
            env_vars=override_env_vars,
        )
        return ClientConfig.from_dict(loaded_profiles)

    @staticmethod
    def load_client_connect_config(
        profile: Optional[str] = None,
        *,
        config_file: Optional[str] = None,
        disable_file: bool = False,
        disable_env: bool = False,
        config_file_strict: bool = False,
        override_env_vars: Optional[Mapping[str, str]] = None,
    ) -> ClientConnectConfig:
        """Load a single client profile and convert to connect config.

        This is a convenience function that combines loading a profile and
        converting it to a connect config dictionary. This will use the current
        process's environment for overrides unless disabled.

        Args:
            profile: The profile to load from the config. Defaults to "default".
            config_file: Path to a specific TOML config file. If not provided,
                default file locations are used. This is ignored if
                ``disable_file`` is true.
            disable_file: If true, file loading is disabled.
            disable_env: If true, environment variable loading and overriding
                is disabled.
            config_file_strict: If true, will error on unrecognized keys in the
                TOML file.
            override_env_vars: A dictionary of environment variables to use for
                loading and overrides. If not provided, the current process's
                environment is used. To use a specific set of environment
                variables, provide them here. To disable environment variable
                loading, set ``disable_env`` to true.

        Returns:
            TypedDict of keyword arguments for
            :py:meth:`temporalio.client.Client.connect`.
        """
        config_source: Optional[DataSource] = None
        if config_file and not disable_file:
            config_source = Path(config_file)

        prof = ClientConfigProfile.load(
            profile=profile,
            config_source=config_source,
            disable_file=disable_file,
            disable_env=disable_env,
            config_file_strict=config_file_strict,
            override_env_vars=override_env_vars,
        )
        return prof.to_client_connect_config()
