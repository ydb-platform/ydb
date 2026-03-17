//! Environment and file-based configuration for Temporal clients.
//!
//! This module provides utilities to load Temporal client configuration from TOML files
//! and environment variables. The configuration supports multiple profiles and various
//! connection settings including TLS, authentication, and codec configuration.
//!
//! ## Environment Variables
//!
//! The following environment variables are supported:
//! - `TEMPORAL_CONFIG_FILE`: Path to the TOML configuration file
//! - `TEMPORAL_PROFILE`: Profile name to use from the configuration file  
//! - `TEMPORAL_ADDRESS`: Temporal server address
//! - `TEMPORAL_NAMESPACE`: Temporal namespace
//! - `TEMPORAL_API_KEY`: API key for authentication
//! - `TEMPORAL_TLS`: A boolean (`true`/`false`) string to enable/disable TLS.
//! - `TEMPORAL_TLS_CLIENT_CERT_PATH`: Path to a client certificate file. Mutually exclusive with TEMPORAL_TLS_CLIENT_CERT_DATA, only supply one.
//! - `TEMPORAL_TLS_CLIENT_CERT_DATA`: The raw client certificate data. Mutually exclusive with TEMPORAL_TLS_CLIENT_CERT_PATH, only supply one.
//! - `TEMPORAL_TLS_CLIENT_KEY_PATH`: Path to a client key file. Mutually exclusive with TEMPORAL_TLS_CLIENT_KEY_DATA, only supply one.
//! - `TEMPORAL_TLS_CLIENT_KEY_DATA`: The raw client key data. Mutually exclusive with TEMPORAL_TLS_CLIENT_KEY_PATH, only supply one.
//! - `TEMPORAL_TLS_SERVER_CA_CERT_PATH`: Path to a server CA certificate file. Mutually exclusive with TEMPORAL_TLS_SERVER_CA_CERT_DATA, only supply one.
//! - `TEMPORAL_TLS_SERVER_CA_CERT_DATA`: The raw server CA certificate data. Mutually exclusive with TEMPORAL_TLS_SERVER_CA_CERT_PATH, only supply one.
//! - `TEMPORAL_TLS_SERVER_NAME`: The server name to use for SNI.
//! - `TEMPORAL_TLS_DISABLE_HOST_VERIFICATION`: A boolean (`true`/`false`) string to disable host verification.
//! - `TEMPORAL_CODEC_ENDPOINT`: The endpoint for a remote data converter.
//! - `TEMPORAL_CODEC_AUTH`: The authorization header value for a remote data converter.
//! - `TEMPORAL_GRPC_META_*`: gRPC metadata headers. Any variables with this prefix will be
//!   converted to gRPC headers. The part of the name after the prefix is converted to the header
//!   name by lowercasing it and replacing underscores with hyphens. For example
//!   `TEMPORAL_GRPC_META_SOME_KEY` becomes `some-key`.
//!
//! ## TOML Configuration Format
//!
//! ```toml
//! [profile.default]
//! address = "localhost:7233"
//! namespace = "default"
//! api_key = "your-api-key"
//!
//! [profile.default.tls]
//! disabled = false
//! client_cert_path = "/path/to/cert.pem"
//! client_key_path = "/path/to/key.pem"
//!
//! [profile.default.codec]
//! endpoint = "http://localhost:8080"
//! auth = "Bearer token"
//!
//! [profile.default.grpc_meta]
//! custom_header = "value"
//! ```

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs, path::Path};
use thiserror::Error;

/// Default profile name when none is specified
pub const DEFAULT_PROFILE: &str = "default";

/// Default configuration file name
pub const DEFAULT_CONFIG_FILE: &str = "temporal.toml";

/// Errors that can occur during configuration loading
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Profile '{0}' not found")]
    ProfileNotFound(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Configuration loading error: {0}")]
    LoadError(Box<dyn std::error::Error>),
}

impl From<std::env::VarError> for ConfigError {
    fn from(e: std::env::VarError) -> Self {
        Self::LoadError(e.into())
    }
}

impl From<std::str::Utf8Error> for ConfigError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self::LoadError(e.into())
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(e: toml::de::Error) -> Self {
        Self::LoadError(e.into())
    }
}

impl From<toml::ser::Error> for ConfigError {
    fn from(e: toml::ser::Error) -> Self {
        Self::LoadError(e.into())
    }
}

/// A source for configuration or a TLS certificate/key, from a path or raw data.
#[derive(Debug, Clone, PartialEq)]
pub enum DataSource {
    Path(String),
    Data(Vec<u8>),
}

/// ClientConfig represents a client config file.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfig {
    /// Profiles, keyed by profile name
    pub profiles: HashMap<String, ClientConfigProfile>,
}

/// ClientConfigProfile is profile-level configuration for a client.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfigProfile {
    /// Client address
    pub address: Option<String>,

    /// Client namespace
    pub namespace: Option<String>,

    /// Client API key. If present and TLS field is None or present and not disabled (i.e. without Disabled as true),
    /// TLS is defaulted to enabled.
    pub api_key: Option<String>,

    /// Optional client TLS config.
    pub tls: Option<ClientConfigTLS>,

    /// Optional client codec config.
    pub codec: Option<ClientConfigCodec>,

    /// Client gRPC metadata (aka headers). When loading from TOML and env var, or writing to TOML, the keys are
    /// lowercased and underscores are replaced with hyphens. This is used for deduplicating/overriding too, so manually
    /// set values that are not normalized may not get overridden with [ClientConfigProfile::apply_env_vars].
    pub grpc_meta: HashMap<String, String>,
}

/// ClientConfigTLS is TLS configuration for a client.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfigTLS {
    /// If true, TLS is explicitly disabled. If false/unset, whether TLS is enabled or not depends on other factors such
    /// as whether this struct is present or None, and whether API key exists (which enables TLS by default).
    pub disabled: bool,

    /// Client certificate source.
    pub client_cert: Option<DataSource>,

    /// Client key source.
    pub client_key: Option<DataSource>,

    /// Server CA certificate source.
    pub server_ca_cert: Option<DataSource>,

    /// SNI override
    pub server_name: Option<String>,

    /// True if host verification should be skipped
    pub disable_host_verification: bool,
}

/// Codec configuration for a client
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ClientConfigCodec {
    /// Remote endpoint for the codec
    pub endpoint: Option<String>,

    /// Auth for the codec
    pub auth: Option<String>,
}

/// Options for loading client configuration
#[derive(Debug, Default)]
pub struct LoadClientConfigOptions {
    /// Where to load config from. If unset, will try env vars then default path.
    pub config_source: Option<DataSource>,

    /// If true, will error if there are unrecognized keys
    pub config_file_strict: bool,
}

/// Options for loading a client configuration profile
#[derive(Debug, Default)]
pub struct LoadClientConfigProfileOptions {
    /// Where to load config from. If unset, will try env vars then default path.
    pub config_source: Option<DataSource>,

    /// Specific profile to use
    pub config_file_profile: Option<String>,

    /// If true, will error if there are unrecognized keys.
    pub config_file_strict: bool,

    /// Disable loading from file
    pub disable_file: bool,

    /// Disable loading from environment variables
    pub disable_env: bool,
}

/// Options for parsing TOML configuration
#[derive(Debug, Default)]
pub struct ClientConfigFromTOMLOptions {
    /// If true, will error if there are unrecognized keys.
    pub strict: bool,
}

/// A source for environment variables, which can be either a provided HashMap or the system's
/// environment. This allows for deferred/lazy reading of system env vars.
enum EnvProvider<'a> {
    Map(&'a HashMap<String, String>),
    System,
}

impl<'a> EnvProvider<'a> {
    fn get(&self, key: &str) -> Result<Option<String>, ConfigError> {
        match self {
            EnvProvider::Map(map) => Ok(map.get(key).cloned()),
            EnvProvider::System => match std::env::var(key) {
                Ok(v) => Ok(Some(v)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn contains_key(&self, key: &str) -> Result<bool, ConfigError> {
        match self {
            EnvProvider::Map(map) => Ok(map.contains_key(key)),
            EnvProvider::System => Ok(std::env::var(key).is_ok()),
        }
    }
}

/// Read bytes from a file path, returning Ok(None) if it doesn't exist
fn read_path_bytes(path: &str) -> Result<Option<Vec<u8>>, ConfigError> {
    if !Path::new(path).exists() {
        return Ok(None);
    }
    match fs::read(path) {
        Ok(data) => Ok(Some(data)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ConfigError::LoadError(e.into())),
    }
}

/// Load client configuration from TOML. This function uses environment variables (which are
/// taken from the system if not provided) to locate the configuration file. It does not apply
/// other environment variable values; that is handled by [load_client_config_profile]. This will
/// not fail if the file does not exist.
pub fn load_client_config(
    options: LoadClientConfigOptions,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<ClientConfig, ConfigError> {
    let env_provider = match env_vars {
        Some(map) => EnvProvider::Map(map),
        None => EnvProvider::System,
    };

    // Get which bytes to load from TOML
    let toml_data = match options.config_source {
        Some(DataSource::Data(d)) => Some(d),
        Some(DataSource::Path(p)) => read_path_bytes(&p)?,
        None => {
            let file_path = if let Some(path) = env_provider
                .get("TEMPORAL_CONFIG_FILE")?
                .filter(|p| !p.is_empty())
            {
                path
            } else {
                get_default_config_file_path()?
            };
            read_path_bytes(&file_path)?
        }
    };

    if let Some(data) = toml_data {
        ClientConfig::from_toml(
            &data,
            ClientConfigFromTOMLOptions {
                strict: options.config_file_strict,
            },
        )
    } else {
        Ok(ClientConfig::default())
    }
}

/// Load a specific client configuration profile.
///
/// This function is the primary entry point for loading client configuration. It orchestrates loading
/// from a TOML file (if not disabled) and then applies overrides from environment variables (if not disabled).
///
/// The resolution order is as follows:
/// 1. A profile is loaded from a TOML file. The file is located by checking `options.config_source`,
///    then the `TEMPORAL_CONFIG_FILE` environment variable, then a default path. The profile within
///    the file is determined by `options.config_file_profile`, then the `TEMPORAL_PROFILE`
///    environment variable, then the "default" profile.
/// 2. Environment variables are applied on top of the loaded profile.
///
/// If `env_vars` is provided as a `HashMap`, it will be used as the source for environment
/// variables. If it is `None`, the function will fall back to using the system's environment variables.
pub fn load_client_config_profile(
    options: LoadClientConfigProfileOptions,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<ClientConfigProfile, ConfigError> {
    if options.disable_file && options.disable_env {
        return Err(ConfigError::InvalidConfig(
            "Cannot disable both file and environment loading".to_string(),
        ));
    }

    let env_provider = if options.disable_env {
        None
    } else {
        Some(match env_vars {
            Some(map) => EnvProvider::Map(map),
            None => EnvProvider::System,
        })
    };

    let mut profile = if options.disable_file {
        ClientConfigProfile::default()
    } else {
        // Load the full config
        let config = load_client_config(
            LoadClientConfigOptions {
                config_source: options.config_source,
                config_file_strict: options.config_file_strict,
            },
            env_vars,
        )?;

        // Determine profile name
        let (profile_name, profile_unset) = if let Some(p) = options.config_file_profile.as_deref()
        {
            (p.to_string(), false)
        } else {
            match env_provider.as_ref() {
                Some(provider) => match provider.get("TEMPORAL_PROFILE")? {
                    Some(p) if !p.is_empty() => (p, false),
                    _ => (DEFAULT_PROFILE.to_string(), true),
                },
                None => (DEFAULT_PROFILE.to_string(), true),
            }
        };

        if let Some(prof) = config.profiles.get(&profile_name) {
            Ok(prof.clone())
        } else if !profile_unset {
            Err(ConfigError::ProfileNotFound(profile_name))
        } else {
            Ok(ClientConfigProfile::default())
        }?
    };

    // Apply environment variables if not disabled
    if !options.disable_env {
        profile.load_from_env(env_vars)?;
    }

    // Apply API key → TLS auto-enabling logic
    profile.apply_api_key_tls_logic();

    Ok(profile)
}

impl ClientConfig {
    /// Load configuration from a TOML string with options. This will replace all profiles within,
    /// it does not do any form of merging.
    pub fn from_toml(
        toml_bytes: &[u8],
        options: ClientConfigFromTOMLOptions,
    ) -> Result<Self, ConfigError> {
        use strict::StrictTomlClientConfig;
        let toml_str = std::str::from_utf8(toml_bytes)?;
        let mut conf = ClientConfig::default();
        if toml_str.trim().is_empty() {
            return Ok(conf);
        }

        if options.strict {
            let toml_conf: StrictTomlClientConfig = toml::from_str(toml_str)?;
            toml_conf.apply_to_client_config(&mut conf)?;
        } else {
            let toml_conf: TomlClientConfig = toml::from_str(toml_str)?;
            toml_conf.apply_to_client_config(&mut conf)?;
        }
        Ok(conf)
    }

    /// Convert configuration to TOML string.
    pub fn to_toml(&self) -> Result<Vec<u8>, ConfigError> {
        let mut toml_conf = TomlClientConfig::new();
        toml_conf.populate_from_client_config(self);
        Ok(toml::to_string_pretty(&toml_conf)?.into_bytes())
    }
}

impl ClientConfigProfile {
    /// Apply environment variable overrides to this profile.
    /// If `env_vars` is `None`, the system's environment variables will be used as the source.
    pub fn load_from_env(
        &mut self,
        env_vars: Option<&HashMap<String, String>>,
    ) -> Result<(), ConfigError> {
        let env_provider = match env_vars {
            Some(map) => EnvProvider::Map(map),
            None => EnvProvider::System,
        };
        // Apply basic settings
        if let Some(address) = env_provider.get("TEMPORAL_ADDRESS")? {
            self.address = Some(address);
        }
        if let Some(namespace) = env_provider.get("TEMPORAL_NAMESPACE")? {
            self.namespace = Some(namespace);
        }
        if let Some(api_key) = env_provider.get("TEMPORAL_API_KEY")? {
            self.api_key = Some(api_key);
        }

        self.apply_tls_env_vars(&env_provider)?;
        self.apply_codec_env_vars(&env_provider)?;
        self.apply_grpc_meta_env_vars(&env_provider)?;

        Ok(())
    }

    fn apply_tls_env_vars(&mut self, env_provider: &EnvProvider) -> Result<(), ConfigError> {
        const TLS_ENV_VARS: &[&str] = &[
            "TEMPORAL_TLS",
            "TEMPORAL_TLS_CLIENT_CERT_PATH",
            "TEMPORAL_TLS_CLIENT_CERT_DATA",
            "TEMPORAL_TLS_CLIENT_KEY_PATH",
            "TEMPORAL_TLS_CLIENT_KEY_DATA",
            "TEMPORAL_TLS_SERVER_CA_CERT_PATH",
            "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
            "TEMPORAL_TLS_SERVER_NAME",
            "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION",
        ];

        if self.tls.is_none() && has_any_env_var(env_provider, TLS_ENV_VARS)? {
            self.tls = Some(ClientConfigTLS::default());
        }

        if let Some(ref mut tls) = self.tls {
            if let Some(disabled_str) = env_provider.get("TEMPORAL_TLS")?
                && let Some(disabled) = env_var_to_bool(&disabled_str)
            {
                tls.disabled = !disabled;
            }

            apply_data_source_env_var(
                env_provider,
                "cert",
                "TEMPORAL_TLS_CLIENT_CERT_PATH",
                "TEMPORAL_TLS_CLIENT_CERT_DATA",
                &mut tls.client_cert,
            )?;
            apply_data_source_env_var(
                env_provider,
                "key",
                "TEMPORAL_TLS_CLIENT_KEY_PATH",
                "TEMPORAL_TLS_CLIENT_KEY_DATA",
                &mut tls.client_key,
            )?;
            apply_data_source_env_var(
                env_provider,
                "server CA cert",
                "TEMPORAL_TLS_SERVER_CA_CERT_PATH",
                "TEMPORAL_TLS_SERVER_CA_CERT_DATA",
                &mut tls.server_ca_cert,
            )?;

            if let Some(v) = env_provider.get("TEMPORAL_TLS_SERVER_NAME")? {
                tls.server_name = Some(v);
            }
            if let Some(v) = env_provider.get("TEMPORAL_TLS_DISABLE_HOST_VERIFICATION")?
                && let Some(b) = env_var_to_bool(&v)
            {
                tls.disable_host_verification = b;
            }
        }
        Ok(())
    }

    fn apply_codec_env_vars(&mut self, env_provider: &EnvProvider) -> Result<(), ConfigError> {
        const CODEC_ENV_VARS: &[&str] = &["TEMPORAL_CODEC_ENDPOINT", "TEMPORAL_CODEC_AUTH"];
        if self.codec.is_none() && has_any_env_var(env_provider, CODEC_ENV_VARS)? {
            self.codec = Some(ClientConfigCodec::default());
        }

        if let Some(ref mut codec) = self.codec {
            if let Some(endpoint) = env_provider.get("TEMPORAL_CODEC_ENDPOINT")? {
                codec.endpoint = Some(endpoint);
            }
            if let Some(auth) = env_provider.get("TEMPORAL_CODEC_AUTH")? {
                codec.auth = Some(auth);
            }
        }
        Ok(())
    }

    fn apply_grpc_meta_env_vars(&mut self, env_provider: &EnvProvider) -> Result<(), ConfigError> {
        let mut handle_meta_var = |header_name: &str, value: &str| {
            let normalized_name = normalize_grpc_meta_key(header_name);
            if value.is_empty() {
                self.grpc_meta.remove(&normalized_name);
            } else {
                self.grpc_meta.insert(normalized_name, value.to_string());
            }
        };

        match env_provider {
            EnvProvider::Map(map) => {
                for (key, value) in map.iter() {
                    if let Some(header_name) = key.strip_prefix("TEMPORAL_GRPC_META_") {
                        handle_meta_var(header_name, value);
                    }
                }
            }
            EnvProvider::System => {
                for (key, value) in std::env::vars() {
                    if let Some(header_name) = key.strip_prefix("TEMPORAL_GRPC_META_") {
                        handle_meta_var(header_name, &value);
                    }
                }
            }
        }
        Ok(())
    }

    /// Apply automatic TLS enabling when API key is present
    pub fn apply_api_key_tls_logic(&mut self) {
        if self.api_key.is_some() && self.tls.is_none() {
            // If API key is present but no TLS config exists, create one with TLS enabled
            self.tls = Some(ClientConfigTLS::default());
        }
    }
}

/// Helper to check if any of the given environment variables are set.
fn has_any_env_var(env_provider: &EnvProvider, keys: &[&str]) -> Result<bool, ConfigError> {
    for &key in keys {
        if env_provider.contains_key(key)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Helper for applying env vars to a data source.
fn apply_data_source_env_var(
    env_provider: &EnvProvider,
    name: &str,
    path_var: &str,
    data_var: &str,
    dest: &mut Option<DataSource>,
) -> Result<(), ConfigError> {
    let path_val = env_provider.get(path_var)?;
    let data_val = env_provider.get(data_var)?;

    match (path_val, data_val) {
        (Some(_), Some(_)) => Err(ConfigError::InvalidConfig(format!(
            "Cannot specify both {path_var} and {data_var}"
        ))),
        (Some(path), None) => {
            if let Some(DataSource::Data(_)) = dest {
                return Err(ConfigError::InvalidConfig(format!(
                    "Cannot specify {name} path via {path_var} when {name} data is already specified"
                )));
            }
            *dest = Some(DataSource::Path(path));
            Ok(())
        }
        (None, Some(data)) => {
            if let Some(DataSource::Path(_)) = dest {
                return Err(ConfigError::InvalidConfig(format!(
                    "Cannot specify {name} data via {data_var} when {name} path is already specified"
                )));
            }
            *dest = Some(DataSource::Data(data.into_bytes()));
            Ok(())
        }
        (None, None) => Ok(()),
    }
}

/// Parse a boolean value from string (supports "true", "false", "1", "0")
fn env_var_to_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Normalize gRPC metadata key (lowercase and replace underscores with hyphens)
fn normalize_grpc_meta_key(key: &str) -> String {
    key.to_lowercase().replace('_', "-")
}

/// Get the default configuration file path
fn get_default_config_file_path() -> Result<String, ConfigError> {
    // Try to get user config directory
    let config_dir = dirs::config_dir()
        .ok_or_else(|| ConfigError::InvalidConfig("failed getting user config dir".to_string()))?;

    let path = config_dir.join("temporalio").join(DEFAULT_CONFIG_FILE);
    Ok(path.to_string_lossy().to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TomlClientConfig {
    #[serde(default, rename = "profile")]
    profiles: HashMap<String, TomlClientConfigProfile>,
}

impl TomlClientConfig {
    fn new() -> Self {
        Self {
            profiles: HashMap::new(),
        }
    }

    fn apply_to_client_config(&self, conf: &mut ClientConfig) -> Result<(), ConfigError> {
        conf.profiles = HashMap::with_capacity(self.profiles.len());
        for (k, v) in &self.profiles {
            conf.profiles.insert(k.clone(), v.to_client_config()?);
        }
        Ok(())
    }

    fn populate_from_client_config(&mut self, conf: &ClientConfig) {
        self.profiles = HashMap::with_capacity(conf.profiles.len());
        for (k, v) in &conf.profiles {
            let mut prof = TomlClientConfigProfile::new();
            prof.populate_from_client_config(v);
            self.profiles.insert(k.clone(), prof);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TomlClientConfigProfile {
    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    tls: Option<TomlClientConfigTLS>,

    #[serde(skip_serializing_if = "Option::is_none")]
    codec: Option<TomlClientConfigCodec>,

    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    grpc_meta: HashMap<String, String>,
}

impl TomlClientConfigProfile {
    fn new() -> Self {
        Self {
            address: None,
            namespace: None,
            api_key: None,
            tls: None,
            codec: None,
            grpc_meta: HashMap::new(),
        }
    }
    fn to_client_config(&self) -> Result<ClientConfigProfile, ConfigError> {
        let mut ret = ClientConfigProfile {
            address: self.address.clone(),
            namespace: self.namespace.clone(),
            api_key: self.api_key.clone(),
            tls: self
                .tls
                .as_ref()
                .map(|tls| tls.to_client_config())
                .transpose()?,
            codec: self.codec.as_ref().map(|codec| codec.to_client_config()),
            grpc_meta: HashMap::new(),
        };

        if !self.grpc_meta.is_empty() {
            ret.grpc_meta = HashMap::with_capacity(self.grpc_meta.len());
            for (k, v) in &self.grpc_meta {
                ret.grpc_meta.insert(normalize_grpc_meta_key(k), v.clone());
            }
        }
        Ok(ret)
    }

    fn populate_from_client_config(&mut self, conf: &ClientConfigProfile) {
        self.address = conf.address.clone();
        self.namespace = conf.namespace.clone();
        self.api_key = conf.api_key.clone();

        if let Some(ref tls_conf) = conf.tls {
            let mut toml_tls = TomlClientConfigTLS::new();
            toml_tls.populate_from_client_config(tls_conf);
            self.tls = Some(toml_tls);
        } else {
            self.tls = None;
        }

        if let Some(ref codec_conf) = conf.codec {
            let mut toml_codec = TomlClientConfigCodec::new();
            toml_codec.populate_from_client_config(codec_conf);
            self.codec = Some(toml_codec);
        } else {
            self.codec = None;
        }

        if !conf.grpc_meta.is_empty() {
            self.grpc_meta = HashMap::with_capacity(conf.grpc_meta.len());
            for (k, v) in &conf.grpc_meta {
                self.grpc_meta.insert(normalize_grpc_meta_key(k), v.clone());
            }
        } else {
            self.grpc_meta.clear();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TomlClientConfigTLS {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    disabled: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    client_cert_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    client_cert_data: Option<String>, // String in TOML, not Vec<u8>

    #[serde(skip_serializing_if = "Option::is_none")]
    client_key_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    client_key_data: Option<String>, // String in TOML, not Vec<u8>

    #[serde(skip_serializing_if = "Option::is_none")]
    server_ca_cert_path: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    server_ca_cert_data: Option<String>, // String in TOML, not Vec<u8>

    #[serde(skip_serializing_if = "Option::is_none")]
    server_name: Option<String>,

    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    disable_host_verification: bool,
}

impl TomlClientConfigTLS {
    fn new() -> Self {
        Self {
            disabled: false,
            client_cert_path: None,
            client_cert_data: None,
            client_key_path: None,
            client_key_data: None,
            server_ca_cert_path: None,
            server_ca_cert_data: None,
            server_name: None,
            disable_host_verification: false,
        }
    }

    fn to_client_config(&self) -> Result<ClientConfigTLS, ConfigError> {
        if self.client_cert_path.is_some() && self.client_cert_data.is_some() {
            return Err(ConfigError::InvalidConfig(
                "Cannot specify both client_cert_path and client_cert_data".to_string(),
            ));
        }
        if self.client_key_path.is_some() && self.client_key_data.is_some() {
            return Err(ConfigError::InvalidConfig(
                "Cannot specify both client_key_path and client_key_data".to_string(),
            ));
        }
        if self.server_ca_cert_path.is_some() && self.server_ca_cert_data.is_some() {
            return Err(ConfigError::InvalidConfig(
                "Cannot specify both server_ca_cert_path and server_ca_cert_data".to_string(),
            ));
        }

        let string_to_bytes = |s: &Option<String>| {
            s.as_ref().and_then(|val| {
                if val.is_empty() {
                    None
                } else {
                    Some(val.as_bytes().to_vec())
                }
            })
        };

        Ok(ClientConfigTLS {
            disabled: self.disabled,
            client_cert: self
                .client_cert_path
                .clone()
                .map(DataSource::Path)
                .or_else(|| string_to_bytes(&self.client_cert_data).map(DataSource::Data)),
            client_key: self
                .client_key_path
                .clone()
                .map(DataSource::Path)
                .or_else(|| string_to_bytes(&self.client_key_data).map(DataSource::Data)),
            server_ca_cert: self
                .server_ca_cert_path
                .clone()
                .map(DataSource::Path)
                .or_else(|| string_to_bytes(&self.server_ca_cert_data).map(DataSource::Data)),
            server_name: self.server_name.clone(),
            disable_host_verification: self.disable_host_verification,
        })
    }

    fn populate_from_client_config(&mut self, conf: &ClientConfigTLS) {
        self.disabled = conf.disabled;
        if let Some(ref cert_source) = conf.client_cert {
            match cert_source {
                DataSource::Path(p) => self.client_cert_path = Some(p.clone()),
                DataSource::Data(d) => {
                    self.client_cert_data = Some(String::from_utf8_lossy(d).into_owned())
                }
            }
        }
        if let Some(ref key_source) = conf.client_key {
            match key_source {
                DataSource::Path(p) => self.client_key_path = Some(p.clone()),
                DataSource::Data(d) => {
                    self.client_key_data = Some(String::from_utf8_lossy(d).into_owned())
                }
            }
        }
        if let Some(ref ca_source) = conf.server_ca_cert {
            match ca_source {
                DataSource::Path(p) => self.server_ca_cert_path = Some(p.clone()),
                DataSource::Data(d) => {
                    self.server_ca_cert_data = Some(String::from_utf8_lossy(d).into_owned())
                }
            }
        }
        self.server_name = conf.server_name.clone();
        self.disable_host_verification = conf.disable_host_verification;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TomlClientConfigCodec {
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    auth: Option<String>,
}

impl TomlClientConfigCodec {
    fn new() -> Self {
        Self {
            endpoint: None,
            auth: None,
        }
    }
    fn to_client_config(&self) -> ClientConfigCodec {
        ClientConfigCodec {
            endpoint: self.endpoint.clone(),
            auth: self.auth.clone(),
        }
    }

    fn populate_from_client_config(&mut self, conf: &ClientConfigCodec) {
        self.endpoint = conf.endpoint.clone();
        self.auth = conf.auth.clone();
    }
}

mod strict {
    use super::{
        ClientConfig, ClientConfigCodec, ClientConfigProfile, ClientConfigTLS, ConfigError,
        DataSource, normalize_grpc_meta_key,
    };
    use serde::Deserialize;
    use std::collections::HashMap;

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub(crate) struct StrictTomlClientConfig {
        #[serde(default, rename = "profile")]
        profiles: HashMap<String, StrictTomlClientConfigProfile>,
    }

    impl StrictTomlClientConfig {
        pub(crate) fn apply_to_client_config(
            self,
            conf: &mut ClientConfig,
        ) -> Result<(), ConfigError> {
            conf.profiles = HashMap::with_capacity(self.profiles.len());
            for (k, v) in self.profiles {
                conf.profiles.insert(k, v.into_client_config()?);
            }
            Ok(())
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct StrictTomlClientConfigProfile {
        #[serde(default)]
        address: Option<String>,
        #[serde(default)]
        namespace: Option<String>,
        #[serde(default)]
        api_key: Option<String>,
        #[serde(default)]
        tls: Option<StrictTomlClientConfigTLS>,
        #[serde(default)]
        codec: Option<StrictTomlClientConfigCodec>,
        #[serde(default)]
        grpc_meta: HashMap<String, String>,
    }

    impl StrictTomlClientConfigProfile {
        fn into_client_config(self) -> Result<ClientConfigProfile, ConfigError> {
            let mut ret = ClientConfigProfile {
                address: self.address,
                namespace: self.namespace,
                api_key: self.api_key,
                tls: self.tls.map(|tls| tls.into_client_config()).transpose()?,
                codec: self.codec.map(|codec| codec.into_client_config()),
                grpc_meta: HashMap::new(),
            };

            if !self.grpc_meta.is_empty() {
                ret.grpc_meta = HashMap::with_capacity(self.grpc_meta.len());
                for (k, v) in self.grpc_meta {
                    ret.grpc_meta.insert(normalize_grpc_meta_key(&k), v);
                }
            }
            Ok(ret)
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct StrictTomlClientConfigTLS {
        #[serde(default)]
        disabled: bool,
        #[serde(default)]
        client_cert_path: Option<String>,
        #[serde(default)]
        client_cert_data: Option<String>,
        #[serde(default)]
        client_key_path: Option<String>,
        #[serde(default)]
        client_key_data: Option<String>,
        #[serde(default)]
        server_ca_cert_path: Option<String>,
        #[serde(default)]
        server_ca_cert_data: Option<String>,
        #[serde(default)]
        server_name: Option<String>,
        #[serde(default)]
        disable_host_verification: bool,
    }

    impl StrictTomlClientConfigTLS {
        fn into_client_config(self) -> Result<ClientConfigTLS, ConfigError> {
            if self.client_cert_path.is_some() && self.client_cert_data.is_some() {
                return Err(ConfigError::InvalidConfig(
                    "Cannot specify both client_cert_path and client_cert_data".to_string(),
                ));
            }
            if self.client_key_path.is_some() && self.client_key_data.is_some() {
                return Err(ConfigError::InvalidConfig(
                    "Cannot specify both client_key_path and client_key_data".to_string(),
                ));
            }
            if self.server_ca_cert_path.is_some() && self.server_ca_cert_data.is_some() {
                return Err(ConfigError::InvalidConfig(
                    "Cannot specify both server_ca_cert_path and server_ca_cert_data".to_string(),
                ));
            }

            let string_to_bytes = |s: Option<String>| {
                s.and_then(|val| {
                    if val.is_empty() {
                        None
                    } else {
                        Some(val.as_bytes().to_vec())
                    }
                })
            };

            Ok(ClientConfigTLS {
                disabled: self.disabled,
                client_cert: self
                    .client_cert_path
                    .map(DataSource::Path)
                    .or_else(|| string_to_bytes(self.client_cert_data).map(DataSource::Data)),
                client_key: self
                    .client_key_path
                    .map(DataSource::Path)
                    .or_else(|| string_to_bytes(self.client_key_data).map(DataSource::Data)),
                server_ca_cert: self
                    .server_ca_cert_path
                    .map(DataSource::Path)
                    .or_else(|| string_to_bytes(self.server_ca_cert_data).map(DataSource::Data)),
                server_name: self.server_name,
                disable_host_verification: self.disable_host_verification,
            })
        }
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct StrictTomlClientConfigCodec {
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        auth: Option<String>,
    }

    impl StrictTomlClientConfigCodec {
        fn into_client_config(self) -> ClientConfigCodec {
            ClientConfigCodec {
                endpoint: self.endpoint,
                auth: self.auth,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_client_config_toml_multiple_profiles() {
        let toml_str = r#"
[profile.default]
address = "localhost:7233"
namespace = "default"
api_key = "test-key"

[profile.default.tls]
disabled = false
client_cert_path = "/path/to/cert"

[profile.prod]
address = "prod.temporal.io:7233"
namespace = "production"
"#;

        let config = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap();

        let default_profile = config.profiles.get("default").unwrap();
        assert_eq!(default_profile.address.as_ref().unwrap(), "localhost:7233");
        assert_eq!(default_profile.namespace.as_ref().unwrap(), "default");
        assert_eq!(default_profile.api_key.as_ref().unwrap(), "test-key");

        let tls = default_profile.tls.as_ref().unwrap();
        assert!(!tls.disabled);
        assert_eq!(
            tls.client_cert,
            Some(DataSource::Path("/path/to/cert".to_string()))
        );

        let prod_profile = config.profiles.get("prod").unwrap();
        assert_eq!(
            prod_profile.address.as_ref().unwrap(),
            "prod.temporal.io:7233"
        );
        assert_eq!(prod_profile.namespace.as_ref().unwrap(), "production");
    }

    #[test]
    fn test_client_config_toml_roundtrip() {
        let mut prof = ClientConfigProfile {
            address: Some("addr".to_string()),
            namespace: Some("ns".to_string()),
            api_key: Some("key".to_string()),
            ..Default::default()
        };
        prof.grpc_meta.insert("k".to_string(), "v".to_string());

        let tls = ClientConfigTLS {
            client_cert: Some(DataSource::Data(b"cert".to_vec())),
            server_ca_cert: Some(DataSource::Data(b"ca".to_vec())),
            ..Default::default()
        };
        prof.tls = Some(tls);

        let mut conf = ClientConfig::default();
        conf.profiles.insert("default".to_string(), prof);

        let toml_bytes = conf.to_toml().unwrap();
        let new_conf = ClientConfig::from_toml(&toml_bytes, Default::default()).unwrap();
        assert_eq!(conf, new_conf);
    }

    #[test]
    fn test_load_client_config_profile_from_file() {
        // Create temporary file with test data
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"
[profile.default]
address = "my-address"
namespace = "my-namespace"
"#
        )
        .unwrap();

        // Test explicit file path
        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Path(
                temp_file.path().to_string_lossy().to_string(),
            )),
            ..Default::default()
        };

        let profile = load_client_config_profile(options, None).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
    }

    #[test]
    fn test_load_client_config_profile_from_env_file_path() {
        // Create temporary file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(
            temp_file,
            r#"
[profile.default]
address = "my-address"
namespace = "my-namespace"
"#
        )
        .unwrap();

        // Test loading via TEMPORAL_CONFIG_FILE env var
        let mut vars = HashMap::new();
        vars.insert(
            "TEMPORAL_CONFIG_FILE".to_string(),
            temp_file.path().to_string_lossy().to_string(),
        );

        let options = LoadClientConfigProfileOptions {
            ..Default::default()
        };

        let profile = load_client_config_profile(options, Some(&vars)).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
    }

    #[test]
    fn test_load_client_config_profile_with_env_overrides() {
        let toml_str = r#"
[profile.default]
address = "my-address"
namespace = "my-namespace"
api_key = "my-api-key"

[profile.default.tls]
disabled = true
client_cert_path = "my-client-cert-path"
client_key_path = "my-client-key-path"
server_name = "my-server-name"
disable_host_verification = true

[profile.default.codec]
endpoint = "my-codec-endpoint"
auth = "my-codec-auth"

[profile.default.grpc_meta]
some-header = "some-value"
some-other-header = "some-value2"
"#;
        let mut vars = HashMap::new();
        vars.insert("TEMPORAL_ADDRESS".to_string(), "my-address-new".to_string());
        vars.insert(
            "TEMPORAL_NAMESPACE".to_string(),
            "my-namespace-new".to_string(),
        );
        vars.insert("TEMPORAL_API_KEY".to_string(), "my-api-key-new".to_string());
        vars.insert("TEMPORAL_TLS".to_string(), "true".to_string());
        vars.insert(
            "TEMPORAL_TLS_CLIENT_CERT_PATH".to_string(),
            "my-client-cert-path-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_TLS_CLIENT_KEY_PATH".to_string(),
            "my-client-key-path-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_TLS_SERVER_NAME".to_string(),
            "my-server-name-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_TLS_DISABLE_HOST_VERIFICATION".to_string(),
            "false".to_string(),
        );
        vars.insert(
            "TEMPORAL_CODEC_ENDPOINT".to_string(),
            "my-codec-endpoint-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_CODEC_AUTH".to_string(),
            "my-codec-auth-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_GRPC_META_SOME_HEADER".to_string(),
            "some-value-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_GRPC_META_SOME_THIRD_HEADER".to_string(),
            "some-value3-new".to_string(),
        );
        vars.insert(
            "TEMPORAL_GRPC_META_some_value4".to_string(),
            "some-value4-new".to_string(),
        );

        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            config_file_profile: Some("default".to_string()),
            ..Default::default()
        };

        let profile = load_client_config_profile(options, Some(&vars)).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "my-address-new");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace-new");
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key-new");

        let tls = profile.tls.as_ref().unwrap();
        assert!(!tls.disabled); // TLS enabled via env var
        assert_eq!(
            tls.client_cert,
            Some(DataSource::Path("my-client-cert-path-new".to_string()))
        );
        assert_eq!(
            tls.client_key,
            Some(DataSource::Path("my-client-key-path-new".to_string()))
        );
        assert_eq!(tls.server_name.as_ref().unwrap(), "my-server-name-new");
        assert!(!tls.disable_host_verification);
        let codec = profile.codec.as_ref().unwrap();
        assert_eq!(codec.endpoint.as_ref().unwrap(), "my-codec-endpoint-new");
        assert_eq!(codec.auth.as_ref().unwrap(), "my-codec-auth-new");
        assert_eq!(
            profile.grpc_meta.get("some-header").unwrap(),
            "some-value-new"
        );
        assert_eq!(
            profile.grpc_meta.get("some-other-header").unwrap(),
            "some-value2"
        );
        assert_eq!(
            profile.grpc_meta.get("some-third-header").unwrap(),
            "some-value3-new"
        );
        assert_eq!(
            profile.grpc_meta.get("some-value4").unwrap(),
            "some-value4-new"
        );
    }

    #[test]
    fn test_client_config_toml_full() {
        let toml_str = r#"
[profile.foo]
address = "my-address"
namespace = "my-namespace"
api_key = "my-api-key"
some_future_key = "some future value not handled"

[profile.foo.tls]
disabled = true
client_cert_path = "my-client-cert-path"
client_key_path = "my-client-key-path"
server_ca_cert_path = "my-server-ca-cert-path"
server_name = "my-server-name"
disable_host_verification = true

[profile.foo.codec]
endpoint = "my-endpoint"
auth = "my-auth"

[profile.foo.grpc_meta]
sOme-hEader_key = "some-value"
"#;

        let config = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap();
        let profile = config.profiles.get("foo").unwrap();

        assert_eq!(profile.address.as_ref().unwrap(), "my-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "my-namespace");
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key");

        let codec = profile.codec.as_ref().unwrap();
        assert_eq!(codec.endpoint.as_ref().unwrap(), "my-endpoint");
        assert_eq!(codec.auth.as_ref().unwrap(), "my-auth");

        let tls = profile.tls.as_ref().unwrap();
        assert!(tls.disabled);
        assert_eq!(
            tls.client_cert,
            Some(DataSource::Path("my-client-cert-path".to_string()))
        );
        assert_eq!(
            tls.client_key,
            Some(DataSource::Path("my-client-key-path".to_string()))
        );
        assert_eq!(
            tls.server_ca_cert,
            Some(DataSource::Path("my-server-ca-cert-path".to_string()))
        );
        assert_eq!(tls.server_name.as_ref().unwrap(), "my-server-name");
        assert!(tls.disable_host_verification);

        // Note: gRPC meta keys get normalized
        assert_eq!(profile.grpc_meta.len(), 1);
        assert_eq!(
            profile.grpc_meta.get("some-header-key").unwrap(),
            "some-value"
        );

        // Test round-trip serialization
        let toml_out = config.to_toml().unwrap();
        let config2 = ClientConfig::from_toml(&toml_out, Default::default()).unwrap();
        assert_eq!(config, config2);
    }

    #[test]
    fn test_client_config_toml_partial() {
        let toml_str = r#"
[profile.foo]
api_key = "my-api-key"

[profile.foo.tls]
"#;

        let config = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap();
        let profile = config.profiles.get("foo").unwrap();

        assert!(profile.address.is_none());
        assert!(profile.namespace.is_none());
        assert_eq!(profile.api_key.as_ref().unwrap(), "my-api-key");
        assert!(profile.codec.is_none());
        assert!(profile.tls.is_some());

        let tls = profile.tls.as_ref().unwrap();
        assert!(!tls.disabled); // default value
        assert!(tls.client_cert.is_none());
    }

    #[test]
    fn test_client_config_toml_empty() {
        let config = ClientConfig::from_toml("".as_bytes(), Default::default()).unwrap();
        assert!(config.profiles.is_empty());

        // Test round-trip
        let toml_out = config.to_toml().unwrap();
        let config2 = ClientConfig::from_toml(&toml_out, Default::default()).unwrap();
        assert_eq!(config, config2);
    }

    #[test]
    fn test_profile_not_found() {
        let toml_str = r#"
[profile.existing]
address = "localhost:7233"
"#;

        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            config_file_profile: Some("nonexistent".to_string()),
            ..Default::default()
        };

        // Should error because we explicitly asked for a profile that doesn't exist
        let result = load_client_config_profile(options, None);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::ProfileNotFound(p) if p == "nonexistent"
        ));
    }

    #[test]
    fn test_client_config_toml_strict_unrecognized_field() {
        let toml_str = r#"
[profile.default]
unrecognized_field = "is-bad"
"#;
        let err = ClientConfig::from_toml(
            toml_str.as_bytes(),
            ClientConfigFromTOMLOptions { strict: true },
        )
        .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("unrecognized_field"));
    }

    #[test]
    fn test_client_config_toml_strict_unrecognized_table() {
        let toml_str = r#"
[unrecognized_table]
foo = "bar"
"#;
        let err = ClientConfig::from_toml(
            toml_str.as_bytes(),
            ClientConfigFromTOMLOptions { strict: true },
        )
        .unwrap_err();
        let err_str = err.to_string();
        assert!(err_str.contains("unrecognized_table"));
    }

    #[test]
    fn test_client_config_both_path_and_data_fails() {
        // Env vars
        let mut vars = HashMap::new();
        vars.insert(
            "TEMPORAL_TLS_CLIENT_CERT_PATH".to_string(),
            "some-path".to_string(),
        );
        vars.insert(
            "TEMPORAL_TLS_CLIENT_CERT_DATA".to_string(),
            "some-data".to_string(),
        );
        let options = LoadClientConfigProfileOptions {
            ..Default::default()
        };
        let err = load_client_config_profile(options, Some(&vars)).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig(_)));
        assert!(err.to_string().contains("Cannot specify both"));

        // TOML
        let toml_str = r#"
[profile.default]
[profile.default.tls]
client_cert_path = "some-path"
client_cert_data = "some-data"
"#;
        let err = ClientConfig::from_toml(toml_str.as_bytes(), Default::default()).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig(_)));
        assert!(err.to_string().contains("Cannot specify both"));
    }

    #[test]
    fn test_client_config_path_data_conflict_across_sources() {
        // Path in TOML, data in env var should fail
        let toml_str = r#"
    [profile.default]
    [profile.default.tls]
    client_cert_path = "some-path"
    "#;
        let mut vars = HashMap::new();
        vars.insert(
            "TEMPORAL_TLS_CLIENT_CERT_DATA".to_string(),
            "some-data".to_string(),
        );
        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            ..Default::default()
        };
        let err = load_client_config_profile(options, Some(&vars)).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig(_)));
        assert!(
            err.to_string()
                .contains("when cert path is already specified")
        );

        // Data in TOML, path in env var should fail
        let toml_str = r#"
    [profile.default]
    [profile.default.tls]
    client_cert_data = "some-data"
    "#;
        let mut vars = HashMap::new();
        vars.insert(
            "TEMPORAL_TLS_CLIENT_CERT_PATH".to_string(),
            "some-path".to_string(),
        );
        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            ..Default::default()
        };
        let err = load_client_config_profile(options, Some(&vars)).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidConfig(_)));
        assert!(
            err.to_string()
                .contains("when cert data is already specified")
        );
    }

    #[test]
    fn test_default_profile_not_found_is_ok() {
        let toml_str = r#"
[profile.existing]
address = "localhost:7233"
"#;

        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            ..Default::default()
        };

        // Should not error, just returns an empty profile
        let profile = load_client_config_profile(options, None).unwrap();
        assert_eq!(profile, ClientConfigProfile::default());
    }

    #[test]
    fn test_normalize_grpc_meta_key() {
        assert_eq!(normalize_grpc_meta_key("SOME_HEADER"), "some-header");
        assert_eq!(normalize_grpc_meta_key("some_header"), "some-header");
        assert_eq!(normalize_grpc_meta_key("Some_Header"), "some-header");
    }

    #[test]
    fn test_env_var_to_bool() {
        assert_eq!(env_var_to_bool("true"), Some(true));
        assert_eq!(env_var_to_bool("TRUE"), Some(true));
        assert_eq!(env_var_to_bool("1"), Some(true));

        assert_eq!(env_var_to_bool("false"), Some(false));
        assert_eq!(env_var_to_bool("FALSE"), Some(false));
        assert_eq!(env_var_to_bool("0"), Some(false));

        assert_eq!(env_var_to_bool("invalid"), None);
        assert_eq!(env_var_to_bool("yes"), None);
        assert_eq!(env_var_to_bool("no"), None);
    }

    #[test]
    fn test_load_client_config_profile_disables_are_an_error() {
        let options = LoadClientConfigProfileOptions {
            disable_file: true,
            disable_env: true,
            ..Default::default()
        };

        let result = load_client_config_profile(options, None);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot disable both file and environment")
        );
    }

    #[test]
    fn test_load_client_config_profile_from_env_only() {
        let mut vars = HashMap::new();
        vars.insert("TEMPORAL_ADDRESS".to_string(), "env-address".to_string());
        vars.insert(
            "TEMPORAL_NAMESPACE".to_string(),
            "env-namespace".to_string(),
        );

        let options = LoadClientConfigProfileOptions {
            disable_file: true,
            ..Default::default()
        };

        let profile = load_client_config_profile(options, Some(&vars)).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "env-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "env-namespace");
    }

    #[test]
    fn test_api_key_tls_auto_enable() {
        // Test 1: When API key is present, TLS should be automatically enabled
        let toml_str = r#"
[profile.default]
api_key = "my-api-key"
"#;

        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            ..Default::default()
        };

        let profile = load_client_config_profile(options, None).unwrap();

        // TLS should be enabled due to API key presence
        assert!(profile.tls.is_some());
        let tls = profile.tls.as_ref().unwrap();
        assert!(!tls.disabled);
    }

    #[test]
    fn test_no_api_key_no_tls_is_none() {
        // Test that if no API key is present and no TLS block exists, TLS config is None
        let toml_str = r#"
[profile.default]
address = "some-address"
"#;

        let options = LoadClientConfigProfileOptions {
            config_source: Some(DataSource::Data(toml_str.as_bytes().to_vec())),
            ..Default::default()
        };

        let profile = load_client_config_profile(options, None).unwrap();

        // TLS should not be enabled
        assert!(profile.tls.is_none());
    }

    #[test]
    fn test_load_client_config_profile_from_system_env() {
        // Set up system env vars. These tests can't be run in parallel.
        unsafe {
            std::env::set_var("TEMPORAL_ADDRESS", "system-address");
            std::env::set_var("TEMPORAL_NAMESPACE", "system-namespace");
        }

        let options = LoadClientConfigProfileOptions {
            disable_file: true, // Don't load from any files
            ..Default::default()
        };

        // Pass None for env_vars to trigger system env var loading
        let profile = load_client_config_profile(options, None).unwrap();
        assert_eq!(profile.address.as_ref().unwrap(), "system-address");
        assert_eq!(profile.namespace.as_ref().unwrap(), "system-namespace");

        // Clean up
        unsafe {
            std::env::remove_var("TEMPORAL_ADDRESS");
            std::env::remove_var("TEMPORAL_NAMESPACE");
        }
    }
}
