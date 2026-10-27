//! Experimental client plugin APIs.

use crate::{ClientOptions, ConnectionOptions};
use std::{any::Any, error::Error, sync::Arc};

/// An error returned by a plugin configuration hook.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct PluginError(Box<dyn Error + Send + Sync>);

impl PluginError {
    /// Wrap an error returned by a plugin.
    pub fn new(error: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self(error.into())
    }
}

/// The configuration target being modified when a plugin failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq, derive_more::Display)]
#[non_exhaustive]
pub enum PluginTarget {
    /// Connection options.
    #[display("connection options")]
    Connection,
    /// Namespace-bound client options.
    #[display("client options")]
    Client,
    /// Worker options.
    #[display("worker options")]
    Worker,
    /// Workflow replayer options.
    #[display("workflow replayer options")]
    WorkflowReplayer,
}

/// An error applying a named plugin to a configuration target.
#[derive(Debug, thiserror::Error)]
#[error("plugin '{plugin_name}' failed to configure {target}: {source}")]
#[non_exhaustive]
pub struct PluginApplyError {
    /// The plugin name reported by [`ClientPlugin::name`] or its worker equivalent.
    pub plugin_name: String,
    /// The configuration target being modified.
    pub target: PluginTarget,
    /// The error returned by the plugin.
    #[source]
    pub source: PluginError,
}

impl PluginApplyError {
    /// Create an error for a plugin that failed to configure a target.
    ///
    /// **Internal:** This method is intended to be used during worker construction. Arguments can
    /// change or be removed in breaking manners.
    pub fn new(plugin_name: impl Into<String>, target: PluginTarget, source: PluginError) -> Self {
        Self {
            plugin_name: plugin_name.into(),
            target,
            source,
        }
    }
}

/// Configures connection and namespace-bound client options.
///
/// **Experimental:** This API may change or be removed.
pub trait ClientPlugin: Send + Sync + 'static {
    /// Return the stable, unique name used to identify this plugin in diagnostics and worker
    /// heartbeats.
    fn name(&self) -> &str;

    /// Configure options before the connection is established.
    fn configure_connection_options(
        &self,
        _options: &mut ConnectionOptions,
    ) -> Result<(), PluginError> {
        Ok(())
    }

    /// Configure options before the namespace-bound client is created.
    fn configure_client_options(&self, _options: &mut ClientOptions) -> Result<(), PluginError> {
        Ok(())
    }
}

/// Marks opaque worker-plugin propagation data supplied by an SDK integration.
///
/// Implementing this trait will not make a type recognized as plugin. Only SDK known
/// `WorkerPluginExtension` implementers will be used as plugins.
pub trait WorkerPluginData: Any + Send + Sync + 'static {}

/// A type-erased client plugin and worker-plugin propagation data.
///
/// The worker data is intentionally opaque to avoid taking a dependency on `temporalio-sdk`.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct ErasedClientPlugin {
    client: Arc<dyn ClientPlugin>,
    worker_plugins: Vec<Arc<dyn WorkerPluginData>>,
}

impl ErasedClientPlugin {
    /// Type-erase a client plugin for registration on [`ClientOptions`].
    pub fn new<P: ClientPlugin>(plugin: P) -> Self {
        Self {
            client: Arc::new(plugin),
            worker_plugins: Vec::new(),
        }
    }

    /// Attach opaque worker-plugin propagation data.
    ///
    /// This is intended for SDK integrations that define their own worker plugin trait. Values
    /// with types unknown to an SDK are ignored.
    pub fn with_worker_plugin<T: WorkerPluginData>(mut self, plugin: T) -> Self {
        self.worker_plugins.push(Arc::new(plugin));
        self
    }

    /// Iterate over opaque worker-plugin propagation data.
    ///
    /// This is intended for SDK integrations that recognize their own private registration type.
    pub fn worker_plugins(&self) -> impl Iterator<Item = &dyn WorkerPluginData> {
        self.worker_plugins.iter().map(AsRef::as_ref)
    }

    /// Return the stable name so SDK integrations can report client-only plugins in worker
    /// metadata.
    ///
    /// **Experimental:** This API may change or be removed.
    pub fn name(&self) -> &str {
        self.client.name()
    }

    pub(crate) fn plugin(&self) -> &dyn ClientPlugin {
        self.client.as_ref()
    }
}

pub(crate) fn apply_connection_plugins(
    client_options: &ClientOptions,
    connection_options: &mut ConnectionOptions,
) -> Result<(), PluginApplyError> {
    for registration in client_options.plugins() {
        registration
            .plugin()
            .configure_connection_options(connection_options)
            .map_err(|source| {
                PluginApplyError::new(
                    registration.plugin().name(),
                    PluginTarget::Connection,
                    source,
                )
            })?;
    }
    Ok(())
}

pub(crate) fn apply_client_plugins(options: &mut ClientOptions) -> Result<(), PluginApplyError> {
    if options.client_plugins_applied() {
        return Ok(());
    }
    let plugins = options.plugins().to_vec();
    for registration in plugins {
        registration
            .plugin()
            .configure_client_options(options)
            .map_err(|source| {
                PluginApplyError::new(registration.plugin().name(), PluginTarget::Client, source)
            })?;
    }
    options.mark_client_plugins_applied();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use url::Url;

    struct CountingPlugin {
        connection_calls: Arc<AtomicUsize>,
        client_calls: Arc<AtomicUsize>,
    }

    impl ClientPlugin for CountingPlugin {
        fn name(&self) -> &str {
            "counting"
        }

        fn configure_connection_options(
            &self,
            options: &mut ConnectionOptions,
        ) -> Result<(), PluginError> {
            self.connection_calls.fetch_add(1, Ordering::Relaxed);
            options.identity.push_str("-configured");
            Ok(())
        }

        fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
            self.client_calls.fetch_add(1, Ordering::Relaxed);
            options.namespace.push_str("-configured");
            Ok(())
        }
    }

    #[test]
    fn plugins_follow_target_lifecycles() {
        let connection_calls = Arc::new(AtomicUsize::new(0));
        let client_calls = Arc::new(AtomicUsize::new(0));
        let mut client_options = ClientOptions::new("namespace")
            .client_plugin(CountingPlugin {
                connection_calls: connection_calls.clone(),
                client_calls: client_calls.clone(),
            })
            .build();
        let mut first_connection_options =
            ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
                .identity("first")
                .build();
        let mut second_connection_options =
            ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap())
                .identity("second")
                .build();

        apply_connection_plugins(&client_options, &mut first_connection_options).unwrap();
        apply_client_plugins(&mut client_options).unwrap();
        apply_connection_plugins(&client_options, &mut second_connection_options).unwrap();
        apply_client_plugins(&mut client_options).unwrap();

        assert_eq!(connection_calls.load(Ordering::Relaxed), 2);
        assert_eq!(client_calls.load(Ordering::Relaxed), 1);
        assert_eq!(first_connection_options.identity, "first-configured");
        assert_eq!(second_connection_options.identity, "second-configured");
        assert_eq!(client_options.namespace, "namespace-configured");
    }
}
