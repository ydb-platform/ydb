//! Conversion from [`temporalio_common::envconfig::ClientConfigProfile`] to [`ConnectionOptions`] and [`ClientOptions`].
//!
//! This module bridges the environment/file-based configuration in `temporalio-common` with
//! the client connection types.

use std::{collections::HashMap, fs};
use url::Url;

pub use temporalio_common::envconfig::{
    ClientConfigProfile, ConfigError, DataSource, LoadClientConfigProfileOptions,
};
use temporalio_common::envconfig::{ClientConfigTLS, load_client_config_profile};

use crate::{ClientOptions, ClientTlsOptions, ConnectionOptions, TlsOptions};

const DEFAULT_ADDRESS: &str = "http://localhost:7233";
const DEFAULT_NAMESPACE: &str = "default";

impl ClientOptions {
    /// Load client and connection options from environment variables and/or a TOML config file.
    pub fn load_from_config(
        options: LoadClientConfigProfileOptions,
    ) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
        load_from_config_with_env(options, None)
    }
}

// Separate function allows injecting env vars for testing.
fn load_from_config_with_env(
    options: LoadClientConfigProfileOptions,
    env_vars: Option<&HashMap<String, String>>,
) -> Result<(ConnectionOptions, ClientOptions), ConfigError> {
    let profile = load_client_config_profile(options, env_vars)?;
    let namespace = profile
        .namespace
        .clone()
        .unwrap_or_else(|| DEFAULT_NAMESPACE.to_owned());
    let conn_opts = ConnectionOptions::try_from(profile)?;
    let client_opts = ClientOptions::new(namespace).build();
    Ok((conn_opts, client_opts))
}

/// Parse an address string into a [`Url`], prepending a scheme if none is present.
///
/// Other SDKs pass addresses as bare `host:port` strings. Our [`ConnectionOptions`] requires a
/// [`Url`], so we attempt a direct parse first and fall back to prepending a scheme.
/// When the user omits a scheme, we use `https://` if TLS will be enabled, otherwise `http://`.
fn parse_address(address: &str, use_tls: bool) -> Result<Url, ConfigError> {
    // Try parsing as-is. `Url::parse("localhost:7233")` "succeeds" by treating `localhost` as
    // the scheme, so reject parses that have no host — those need a scheme prefix.
    if let Ok(url) = Url::parse(address)
        && url.host().is_some()
    {
        return Ok(url);
    }
    let scheme = if use_tls { "https" } else { "http" };
    Url::parse(&format!("{scheme}://{address}"))
        .map_err(|e| ConfigError::InvalidConfig(format!("Invalid address: {e}")))
}

/// Build [`TlsOptions`] from a [`ClientConfigTLS`] config, resolving any file-based data sources.
fn build_tls_options(tls: ClientConfigTLS) -> Result<TlsOptions, ConfigError> {
    let client_tls_options = match (tls.client_cert, tls.client_key) {
        (Some(cert), Some(key)) => {
            let cert_bytes =
                resolve_datasource(cert).map_err(|e| ConfigError::LoadError(e.into()))?;
            let key_bytes =
                resolve_datasource(key).map_err(|e| ConfigError::LoadError(e.into()))?;
            Some(
                ClientTlsOptions::builder()
                    .client_cert(cert_bytes)
                    .client_private_key(key_bytes)
                    .build(),
            )
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(ConfigError::InvalidConfig(
                "Both client certificate and client key must be provided together".to_string(),
            ));
        }
        (None, None) => None,
    };

    let server_root_ca_cert = tls
        .server_ca_cert
        .map(resolve_datasource)
        .transpose()
        .map_err(|e| ConfigError::LoadError(e.into()))?;

    Ok(TlsOptions::builder()
        .maybe_server_root_ca_cert(server_root_ca_cert)
        .maybe_domain(tls.server_name)
        .maybe_client_tls_options(client_tls_options)
        .build())
}

/// Determine whether TLS should be enabled based on the profile's TLS config and API key.
///
/// TLS is enabled when:
/// - There is a TLS section that is not explicitly disabled, OR
/// - An API key is set and TLS is not explicitly disabled
fn should_enable_tls(tls: &Option<ClientConfigTLS>, has_api_key: bool) -> bool {
    match tls {
        Some(t) => t.disabled != Some(true),
        None => has_api_key,
    }
}

impl TryFrom<ClientConfigProfile> for ConnectionOptions {
    type Error = ConfigError;

    fn try_from(profile: ClientConfigProfile) -> Result<Self, Self::Error> {
        let ClientConfigProfile {
            address,
            namespace: _,
            api_key,
            tls,
            codec: _,
            grpc_meta,
        } = profile;

        let has_api_key = api_key.is_some();
        let use_tls = should_enable_tls(&tls, has_api_key);
        let target = parse_address(address.as_deref().unwrap_or(DEFAULT_ADDRESS), use_tls)?;

        let tls_options = if use_tls {
            match tls {
                Some(tls_cfg) => Some(build_tls_options(tls_cfg)?),
                None => Some(TlsOptions::default()),
            }
        } else {
            None
        };

        let headers = (!grpc_meta.is_empty()).then_some(grpc_meta);

        Ok(ConnectionOptions::new(target)
            .maybe_api_key(api_key)
            .maybe_tls_options(tls_options)
            .maybe_headers(headers)
            .build())
    }
}

/// Resolve a data source to its raw bytes.
fn resolve_datasource(data_source: DataSource) -> Result<Vec<u8>, std::io::Error> {
    match data_source {
        DataSource::Path(path) => fs::read(path),
        DataSource::Data(data) => Ok(data),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::{fixture, rstest};
    use std::path::PathBuf;
    use tempfile::TempDir;
    use temporalio_common::envconfig::{ClientConfigTLS, DataSource};

    /// Write a TOML config file into a temp directory and return (dir, path).
    /// The `TempDir` handle keeps the directory alive; it is cleaned up on drop.
    #[fixture]
    fn config_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    /// Write `content` to `temporal.toml` inside `dir`, returning the file path.
    fn write_config(dir: &TempDir, content: &str) -> PathBuf {
        let path = dir.path().join("temporal.toml");
        std::fs::write(&path, content).unwrap();
        path
    }

    #[rstest]
    #[case::default(None, false, "http://localhost:7233/")]
    #[case::with_scheme(Some("https://my-server:7233"), false, "https://my-server:7233/")]
    #[case::without_scheme(Some("localhost:7233"), false, "http://localhost:7233/")]
    #[case::without_scheme_tls(Some("localhost:7233"), true, "https://localhost:7233/")]
    #[case::explicit_http_with_tls(Some("http://my-server:7233"), true, "http://my-server:7233/")]
    fn address_parsing(
        #[case] address: Option<&str>,
        #[case] enable_tls: bool,
        #[case] expected: &str,
    ) {
        let tls = enable_tls.then(ClientConfigTLS::default);
        let profile = ClientConfigProfile {
            address: address.map(str::to_string),
            tls,
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.target.as_str(), expected);
    }

    #[test]
    fn invalid_address_errors() {
        let profile = ClientConfigProfile {
            address: Some("://bad".to_string()),
            ..Default::default()
        };
        assert!(ConnectionOptions::try_from(profile).is_err());
    }

    #[test]
    fn empty_profile_defaults() {
        let env = HashMap::new();
        let opts = LoadClientConfigProfileOptions::builder()
            .disable_file(true)
            .build();
        let (conn, client) = load_from_config_with_env(opts, Some(&env)).unwrap();

        assert_eq!(conn.target.as_str(), "http://localhost:7233/");
        assert_eq!(client.namespace, "default");
        assert!(conn.tls_options.is_none());
        assert!(conn.headers.is_none());
        assert!(conn.api_key.is_none());
    }

    #[test]
    fn namespace_override() {
        let mut env = HashMap::new();
        env.insert("TEMPORAL_NAMESPACE".to_string(), "my-namespace".to_string());
        let opts = LoadClientConfigProfileOptions::builder()
            .disable_file(true)
            .build();
        let (_, client) = load_from_config_with_env(opts, Some(&env)).unwrap();
        assert_eq!(client.namespace, "my-namespace");
    }

    #[test]
    fn grpc_metadata_passthrough() {
        let mut meta = HashMap::new();
        meta.insert("x-custom".to_string(), "value".to_string());
        meta.insert("another".to_string(), "header".to_string());
        let profile = ClientConfigProfile {
            grpc_meta: meta.clone(),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.headers.unwrap(), meta);
    }

    #[test]
    fn api_key_populates_field() {
        let profile = ClientConfigProfile {
            api_key: Some("my-key".to_string()),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.api_key.as_deref(), Some("my-key"));
    }

    #[rstest]
    #[case::no_tls_no_key(None, None, false)]
    #[case::no_tls_with_key(None, Some("key"), true)]
    #[case::tls_disabled_false(Some(Some(false)), None, true)]
    #[case::tls_disabled_true(Some(Some(true)), None, false)]
    #[case::tls_disabled_none(Some(None), None, true)]
    #[case::key_with_tls_disabled(Some(Some(true)), Some("key"), false)]
    #[case::key_with_tls_enabled(Some(Some(false)), Some("key"), true)]
    fn tls_enablement(
        #[case] tls_disabled: Option<Option<bool>>,
        #[case] api_key: Option<&str>,
        #[case] expect_tls: bool,
    ) {
        let profile = ClientConfigProfile {
            api_key: api_key.map(str::to_string),
            tls: tls_disabled.map(|disabled| ClientConfigTLS {
                disabled,
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        assert_eq!(conn.tls_options.is_some(), expect_tls);
    }

    #[test]
    fn data_source_certs() {
        let profile = ClientConfigProfile {
            tls: Some(ClientConfigTLS {
                client_cert: Some(DataSource::Data(b"cert-data".to_vec())),
                client_key: Some(DataSource::Data(b"key-data".to_vec())),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        let mtls = tls.client_tls_options.unwrap();
        assert_eq!(mtls.client_cert, b"cert-data");
        assert_eq!(mtls.client_private_key, b"key-data");
    }

    #[rstest]
    fn path_source_certs(config_dir: TempDir) {
        let cert_path = config_dir.path().join("cert.pem");
        let key_path = config_dir.path().join("key.pem");
        std::fs::write(&cert_path, b"file-cert").unwrap();
        std::fs::write(&key_path, b"file-key").unwrap();

        let profile = ClientConfigProfile {
            tls: Some(ClientConfigTLS {
                client_cert: Some(DataSource::Path(cert_path.to_str().unwrap().to_string())),
                client_key: Some(DataSource::Path(key_path.to_str().unwrap().to_string())),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        let mtls = tls.client_tls_options.unwrap();
        assert_eq!(mtls.client_cert, b"file-cert");
        assert_eq!(mtls.client_private_key, b"file-key");
    }

    #[test]
    fn server_ca_cert() {
        let profile = ClientConfigProfile {
            tls: Some(ClientConfigTLS {
                server_ca_cert: Some(DataSource::Data(b"ca-data".to_vec())),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        assert_eq!(tls.server_root_ca_cert.unwrap(), b"ca-data");
    }

    #[test]
    fn server_name_sni() {
        let profile = ClientConfigProfile {
            tls: Some(ClientConfigTLS {
                server_name: Some("my.server.com".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let conn: ConnectionOptions = profile.try_into().unwrap();
        let tls = conn.tls_options.unwrap();
        assert_eq!(tls.domain.as_deref(), Some("my.server.com"));
    }

    #[rstest]
    #[case::cert_without_key(Some(DataSource::Data(b"cert".to_vec())), None)]
    #[case::key_without_cert(None, Some(DataSource::Data(b"key".to_vec())))]
    fn partial_tls_errors(
        #[case] client_cert: Option<DataSource>,
        #[case] client_key: Option<DataSource>,
    ) {
        let profile = ClientConfigProfile {
            tls: Some(ClientConfigTLS {
                client_cert,
                client_key,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(ConnectionOptions::try_from(profile).is_err());
    }

    #[rstest]
    fn load_from_config_from_toml(config_dir: TempDir) {
        let config_path = write_config(
            &config_dir,
            r#"
[profile.default]
address = "toml-server:7233"
namespace = "toml-ns"
api_key = "toml-key"

[profile.default.grpc_meta]
x-custom = "value"

[profile.custom]
address = "custom-server:9090"
namespace = "custom-ns"
"#,
        );

        // Default profile
        let opts = LoadClientConfigProfileOptions::builder()
            .config_source(DataSource::Path(config_path.to_str().unwrap().to_string()))
            .disable_env(true)
            .build();
        let (conn, client) = ClientOptions::load_from_config(opts).unwrap();
        assert_eq!(conn.target.as_str(), "https://toml-server:7233/");
        assert_eq!(client.namespace, "toml-ns");
        assert_eq!(conn.api_key.as_deref(), Some("toml-key"));
        assert!(conn.tls_options.is_some());
        assert_eq!(
            conn.headers.as_ref().unwrap().get("x-custom").unwrap(),
            "value"
        );

        // Custom profile
        let opts = LoadClientConfigProfileOptions::builder()
            .config_source(DataSource::Path(config_path.to_str().unwrap().to_string()))
            .config_file_profile("custom".to_string())
            .disable_env(true)
            .build();
        let (conn, client) = ClientOptions::load_from_config(opts).unwrap();
        assert_eq!(conn.target.as_str(), "http://custom-server:9090/");
        assert_eq!(client.namespace, "custom-ns");
    }
}
