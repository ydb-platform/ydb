//! Experimental plugin APIs for configuring workers and clients from reusable values.

#[cfg(feature = "wasm-workflows")]
use crate::WasmWorkflowComponent;
pub use crate::workflow_registry::WorkflowDefinitions;
use crate::{
    WorkerOptions,
    activities::ActivityDefinitions,
    interceptors::{ActivityInboundInterceptor, WorkerInterceptor},
    workflow_interceptors::WorkflowInterceptorConstructor,
    workflow_replayer::WorkflowReplayerOptions,
};
use std::{any::Any, sync::Arc};
use temporalio_client::{
    ClientInterceptor, ClientOptions, ClientPlugin, ConnectionOptions, ErasedClientPlugin,
    PluginApplyError, PluginError, PluginTarget, WorkerPluginData,
};
use temporalio_common::data_converters::DataConverter;

/// Configures worker options before the worker is created.
///
/// Use [`ClientAndWorkerPlugin`] for plugins that target both clients and workers.
///
/// **Experimental:** This API may change or be removed.
pub trait WorkerPlugin: Send + Sync + 'static {
    /// Stable, unique name used to identify this plugin.
    fn name(&self) -> &str;

    /// Configure worker options.
    ///
    /// Worker plugin registrations are captured before this method is called. Altering plugins in
    /// this method does not change which plugins are applied.
    fn configure_worker_options(&self, _options: &mut WorkerOptions) -> Result<(), PluginError> {
        Ok(())
    }

    /// Configure workflow replayer options.
    ///
    /// Replay plugin registrations are captured before this method is called. Altering plugins in
    /// this method does not change which plugins are applied.
    fn configure_workflow_replayer_options(
        &self,
        _options: &mut WorkflowReplayerOptions,
    ) -> Result<(), PluginError> {
        Ok(())
    }
}

#[derive(Clone)]
struct PropagatedWorkerPlugin(Arc<dyn WorkerPlugin>);

impl WorkerPluginData for PropagatedWorkerPlugin {}

/// A container for a plugin that implements both [`ClientPlugin`] and
/// [`WorkerPlugin`].
///
/// This wrapper is only needed when the same plugin configures both a client and workers.
/// One-sided plugins can implement [`ClientPlugin`] or [`WorkerPlugin`] directly.
///
/// When registered on [`ClientOptions`], the resulting client carries the worker plugin and
/// automatically applies it to workers created from that client. Do not also register the plugin
/// on [`WorkerOptions`], or its worker configuration will be applied twice.
///
/// ```
/// # use temporalio_client::{ClientOptions, ClientPlugin};
/// # use temporalio_sdk::{ClientAndWorkerPlugin, WorkerPlugin};
/// # struct MyPlugin;
/// # impl ClientPlugin for MyPlugin {
/// #     fn name(&self) -> &str { "my-plugin" }
/// # }
/// # impl WorkerPlugin for MyPlugin {
/// #     fn name(&self) -> &str { "my-plugin" }
/// # }
/// let plugin = ClientAndWorkerPlugin::new(MyPlugin);
/// let client_options = ClientOptions::new("default").plugin(plugin).build();
/// # let _ = client_options;
/// ```
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub struct ClientAndWorkerPlugin {
    client: ErasedClientPlugin,
    worker: Arc<dyn WorkerPlugin>,
}

impl ClientAndWorkerPlugin {
    /// Type-erase one plugin value for client registration, automatic worker propagation, and
    /// optional explicit worker registration.
    pub fn new<P>(plugin: P) -> Self
    where
        P: ClientPlugin + WorkerPlugin,
    {
        let plugin = Arc::new(plugin);
        let mut client = ErasedClientPlugin::new(SharedClientPlugin(plugin.clone()));
        let worker: Arc<dyn WorkerPlugin> = Arc::new(SharedWorkerPlugin(plugin));
        client = client.with_worker_plugin(PropagatedWorkerPlugin(worker.clone()));
        Self { client, worker }
    }
}

impl From<ClientAndWorkerPlugin> for ErasedClientPlugin {
    fn from(plugin: ClientAndWorkerPlugin) -> Self {
        plugin.client
    }
}

impl WorkerPlugin for ClientAndWorkerPlugin {
    fn name(&self) -> &str {
        self.worker.name()
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        self.worker.configure_worker_options(options)
    }

    fn configure_workflow_replayer_options(
        &self,
        options: &mut WorkflowReplayerOptions,
    ) -> Result<(), PluginError> {
        self.worker.configure_workflow_replayer_options(options)
    }
}

/// A simple plugin field that either supplies a value or customizes the existing value.
///
/// Builder methods on [`SimplePluginBuilder`] automatically convert supported values and
/// functions into this type.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone)]
pub enum SimplePluginOption<T> {
    /// A value that replaces a scalar field or appends to a collection field.
    Value(T),
    /// A function that receives the existing field and returns a configured value.
    Function(Arc<dyn Fn(Option<T>) -> T + Send + Sync>),
}

macro_rules! impl_simple_plugin_option_conversions {
    ($type:ty) => {
        impl From<$type> for SimplePluginOption<$type> {
            fn from(value: $type) -> Self {
                Self::Value(value)
            }
        }

        impl<F> From<F> for SimplePluginOption<$type>
        where
            F: Fn(Option<$type>) -> $type + Send + Sync + 'static,
        {
            fn from(function: F) -> Self {
                Self::Function(Arc::new(function))
            }
        }
    };
}

impl_simple_plugin_option_conversions!(DataConverter);
impl_simple_plugin_option_conversions!(Vec<Arc<dyn ClientInterceptor>>);
impl_simple_plugin_option_conversions!(Vec<Arc<dyn WorkerInterceptor>>);
impl_simple_plugin_option_conversions!(Vec<Arc<dyn ActivityInboundInterceptor>>);
impl_simple_plugin_option_conversions!(Vec<WorkflowInterceptorConstructor>);
impl_simple_plugin_option_conversions!(ActivityDefinitions);
impl_simple_plugin_option_conversions!(WorkflowDefinitions);
#[cfg(feature = "wasm-workflows")]
impl_simple_plugin_option_conversions!(Vec<WasmWorkflowComponent>);

fn apply_replacing<T: Clone>(target: &mut T, option: Option<&SimplePluginOption<T>>) {
    let Some(option) = option else {
        return;
    };
    *target = match option {
        SimplePluginOption::Value(value) => value.clone(),
        SimplePluginOption::Function(function) => function(Some(target.clone())),
    };
}

fn apply_appending<T: Clone>(
    target: &mut T,
    option: Option<&SimplePluginOption<T>>,
    append: impl Fn(&mut T, &T),
) {
    let Some(option) = option else {
        return;
    };
    match option {
        SimplePluginOption::Value(value) => append(target, value),
        SimplePluginOption::Function(function) => *target = function(Some(target.clone())),
    }
}

/// A plugin assembled from declarative values or functions of existing values.
///
/// Scalar values replace the corresponding option, while collection values append. Functions
/// receive the existing field and replace it with their return value, except that returned
/// workflow definitions are merged into the existing definitions.
///
/// ```
/// use temporalio_client::ClientOptions;
/// use temporalio_common::data_converters::DataConverter;
/// use temporalio_sdk::SimplePlugin;
///
/// let plugin = SimplePlugin::builder("my-plugin")
///     .data_converter(|existing: Option<DataConverter>| existing.unwrap_or_default())
///     .build();
/// let client_options = ClientOptions::new("default").plugin(plugin).build();
/// # let _ = client_options;
/// ```
///
/// Worker configuration declared by this combined plugin is automatically propagated through the
/// configured client when a worker is created.
///
/// **Experimental:** This API may change or be removed.
#[derive(Clone, bon::Builder)]
#[builder(state_mod(vis = "pub"))]
pub struct SimplePlugin {
    #[builder(start_fn, into)]
    name: String,
    #[builder(into)]
    data_converter: Option<SimplePluginOption<DataConverter>>,
    #[builder(into)]
    client_interceptors: Option<SimplePluginOption<Vec<Arc<dyn ClientInterceptor>>>>,
    #[builder(into)]
    worker_interceptors: Option<SimplePluginOption<Vec<Arc<dyn WorkerInterceptor>>>>,
    #[builder(into)]
    activity_inbound_interceptors:
        Option<SimplePluginOption<Vec<Arc<dyn ActivityInboundInterceptor>>>>,
    #[builder(into)]
    workflow_interceptors: Option<SimplePluginOption<Vec<WorkflowInterceptorConstructor>>>,
    #[builder(into)]
    activities: Option<SimplePluginOption<ActivityDefinitions>>,
    #[builder(into)]
    workflows: Option<SimplePluginOption<WorkflowDefinitions>>,
    #[cfg(feature = "wasm-workflows")]
    #[builder(into)]
    wasm_workflow_components: Option<SimplePluginOption<Vec<WasmWorkflowComponent>>>,
}

impl From<SimplePlugin> for ErasedClientPlugin {
    fn from(plugin: SimplePlugin) -> Self {
        ClientAndWorkerPlugin::new(plugin).into()
    }
}

impl ClientPlugin for SimplePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
        apply_replacing(&mut options.data_converter, self.data_converter.as_ref());
        apply_appending(
            &mut options.client_interceptors,
            self.client_interceptors.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        Ok(())
    }
}

impl WorkerPlugin for SimplePlugin {
    fn name(&self) -> &str {
        &self.name
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        apply_appending(
            &mut options.activities,
            self.activities.as_ref(),
            |existing, value| existing.extend(value),
        );
        if let Some(workflows) = &self.workflows {
            match workflows {
                SimplePluginOption::Value(value) => options.workflows.extend(value),
                SimplePluginOption::Function(function) => {
                    let workflows = function(Some(options.workflows.clone()));
                    options.workflows.extend(&workflows)
                }
            }
            .map_err(PluginError::new)?;
        }
        apply_appending(
            &mut options.worker_interceptors,
            self.worker_interceptors.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        apply_appending(
            &mut options.activity_inbound_interceptors,
            self.activity_inbound_interceptors.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        apply_appending(
            &mut options.workflow_interceptor_constructors,
            self.workflow_interceptors.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        #[cfg(feature = "wasm-workflows")]
        apply_appending(
            &mut options.wasm_workflow_components,
            self.wasm_workflow_components.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        Ok(())
    }

    fn configure_workflow_replayer_options(
        &self,
        options: &mut WorkflowReplayerOptions,
    ) -> Result<(), PluginError> {
        apply_replacing(&mut options.data_converter, self.data_converter.as_ref());
        if let Some(workflows) = &self.workflows {
            match workflows {
                SimplePluginOption::Value(value) => options.workflows.extend(value),
                SimplePluginOption::Function(function) => {
                    let workflows = function(Some(options.workflows.clone()));
                    options.workflows.extend(&workflows)
                }
            }
            .map_err(PluginError::new)?;
        }
        apply_appending(
            &mut options.worker_interceptors,
            self.worker_interceptors.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        apply_appending(
            &mut options.workflow_interceptor_constructors,
            self.workflow_interceptors.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        #[cfg(feature = "wasm-workflows")]
        apply_appending(
            &mut options.wasm_workflow_components,
            self.wasm_workflow_components.as_ref(),
            |existing, value| existing.extend(value.iter().cloned()),
        );
        Ok(())
    }
}

struct SharedClientPlugin<P>(Arc<P>);

impl<P> ClientPlugin for SharedClientPlugin<P>
where
    P: ClientPlugin,
{
    fn name(&self) -> &str {
        ClientPlugin::name(self.0.as_ref())
    }

    fn configure_connection_options(
        &self,
        options: &mut ConnectionOptions,
    ) -> Result<(), PluginError> {
        self.0.configure_connection_options(options)
    }

    fn configure_client_options(&self, options: &mut ClientOptions) -> Result<(), PluginError> {
        self.0.configure_client_options(options)
    }
}

struct SharedWorkerPlugin<P>(Arc<P>);

impl<P> WorkerPlugin for SharedWorkerPlugin<P>
where
    P: ClientPlugin + WorkerPlugin,
{
    fn name(&self) -> &str {
        ClientPlugin::name(self.0.as_ref())
    }

    fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
        self.0.configure_worker_options(options)
    }

    fn configure_workflow_replayer_options(
        &self,
        options: &mut WorkflowReplayerOptions,
    ) -> Result<(), PluginError> {
        self.0.configure_workflow_replayer_options(options)
    }
}

#[derive(Debug, Eq, PartialEq)]
struct WorkerPluginWarning<'a> {
    plugin_name: &'a str,
    message: &'static str,
}

fn worker_plugin_warnings(
    plugins: &[Arc<dyn WorkerPlugin>],
) -> impl Iterator<Item = WorkerPluginWarning<'_>> {
    plugins.iter().enumerate().filter_map(|(index, plugin)| {
        plugins[index + 1..]
            .iter()
            .any(|other| plugin.name() == other.name())
            .then_some(WorkerPluginWarning {
                plugin_name: plugin.name(),
                message: "Multiple worker plugins with the same name were registered",
            })
    })
}

pub(crate) fn apply_worker_plugins(
    client_options: &ClientOptions,
    options: &mut WorkerOptions,
) -> Result<(), PluginApplyError> {
    options.client_plugin_names = client_options
        .plugins()
        .iter()
        .map(|plugin| plugin.name().to_owned())
        .collect();
    let mut plugins = client_options
        .plugins()
        .iter()
        .flat_map(ErasedClientPlugin::worker_plugins)
        .filter_map(|plugin| (plugin as &dyn Any).downcast_ref::<PropagatedWorkerPlugin>())
        .map(|plugin| plugin.0.clone())
        .collect::<Vec<_>>();
    plugins.append(&mut options.worker_plugins);

    for warning in worker_plugin_warnings(&plugins) {
        warn!(plugin = warning.plugin_name, "{}", warning.message);
    }

    for registration in &plugins {
        registration
            .configure_worker_options(options)
            .map_err(|source| {
                PluginApplyError::new(registration.name(), PluginTarget::Worker, source)
            })?;
    }
    options.worker_plugins = plugins;
    Ok(())
}

pub(crate) fn apply_workflow_replayer_plugins(
    options: &mut WorkflowReplayerOptions,
) -> Result<(), PluginApplyError> {
    let plugins = std::mem::take(&mut options.worker_plugins);

    for warning in worker_plugin_warnings(&plugins) {
        warn!(plugin = warning.plugin_name, "{}", warning.message);
    }

    for registration in &plugins {
        registration
            .configure_workflow_replayer_options(options)
            .map_err(|source| {
                PluginApplyError::new(registration.name(), PluginTarget::WorkflowReplayer, source)
            })?;
    }
    options.worker_plugins = plugins;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashSet,
        sync::{
            Mutex,
            atomic::{AtomicU8, Ordering},
        },
    };
    use temporalio_client::ClientOptions;
    use temporalio_common::protos::temporal::api::worker::v1::PluginInfo;

    #[temporalio_macros::workflow]
    #[derive(Default)]
    struct PluginTestWorkflow;

    #[temporalio_macros::workflow_methods]
    impl PluginTestWorkflow {
        #[run]
        async fn run(_ctx: &mut crate::WorkflowContext<Self>) -> crate::WorkflowResult<()> {
            Ok(())
        }
    }

    struct RecordingCombinedPlugin {
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl ClientPlugin for RecordingCombinedPlugin {
        fn name(&self) -> &str {
            "combined"
        }
    }

    impl WorkerPlugin for RecordingCombinedPlugin {
        fn name(&self) -> &str {
            "combined"
        }

        fn configure_worker_options(
            &self,
            _options: &mut WorkerOptions,
        ) -> Result<(), PluginError> {
            self.order.lock().unwrap().push("propagated");
            Ok(())
        }
    }

    struct RecordingWorkerPlugin {
        name: &'static str,
        value: &'static str,
        order: Arc<Mutex<Vec<&'static str>>>,
    }

    impl WorkerPlugin for RecordingWorkerPlugin {
        fn name(&self) -> &str {
            self.name
        }

        fn configure_worker_options(
            &self,
            _options: &mut WorkerOptions,
        ) -> Result<(), PluginError> {
            self.order.lock().unwrap().push(self.value);
            Ok(())
        }

        fn configure_workflow_replayer_options(
            &self,
            _options: &mut WorkflowReplayerOptions,
        ) -> Result<(), PluginError> {
            self.order.lock().unwrap().push(self.value);
            Ok(())
        }
    }

    #[test]
    fn propagated_plugins_run_before_local_plugins() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let combined = ClientAndWorkerPlugin::new(RecordingCombinedPlugin {
            order: order.clone(),
        });
        let client_options = ClientOptions::new("namespace").plugin(combined).build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_plugin(RecordingWorkerPlugin {
                name: "local",
                value: "local",
                order: order.clone(),
            })
            .build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(*order.lock().unwrap(), ["propagated", "local"]);
    }

    #[test]
    fn replay_plugins_configure_in_registration_order() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut options = WorkflowReplayerOptions::new()
            .worker_plugin(RecordingWorkerPlugin {
                name: "first",
                value: "first",
                order: order.clone(),
            })
            .worker_plugin(RecordingWorkerPlugin {
                name: "second",
                value: "second",
                order: order.clone(),
            })
            .build();

        apply_workflow_replayer_plugins(&mut options).unwrap();

        assert_eq!(*order.lock().unwrap(), ["first", "second"]);
        assert_eq!(options.worker_plugins.len(), 2);
    }

    #[test]
    fn explicitly_reusing_a_combined_registration_warns_and_applies_both() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let combined = ClientAndWorkerPlugin::new(RecordingCombinedPlugin {
            order: order.clone(),
        });
        let client_options = ClientOptions::new("namespace")
            .plugin(combined.clone())
            .build();
        let mut worker_options = WorkerOptions::new("queue").worker_plugin(combined).build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(
            worker_plugin_warnings(&worker_options.worker_plugins).collect::<Vec<_>>(),
            [WorkerPluginWarning {
                plugin_name: "combined",
                message: "Multiple worker plugins with the same name were registered",
            }]
        );
        assert_eq!(*order.lock().unwrap(), ["propagated", "propagated"]);
    }

    struct ClientOnlyPlugin;

    impl ClientPlugin for ClientOnlyPlugin {
        fn name(&self) -> &str {
            "client-only"
        }
    }

    struct SameNameClientPlugin;

    impl ClientPlugin for SameNameClientPlugin {
        fn name(&self) -> &str {
            "same-name"
        }
    }

    struct UnrecognizedWorkerPluginExtension {
        _registration: Arc<dyn WorkerPlugin>,
    }

    impl WorkerPluginData for UnrecognizedWorkerPluginExtension {}

    #[test]
    fn arbitrary_opaque_data_cannot_impersonate_propagated_plugin() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let client_registration = ErasedClientPlugin::new(ClientOnlyPlugin).with_worker_plugin(
            UnrecognizedWorkerPluginExtension {
                _registration: Arc::new(RecordingWorkerPlugin {
                    name: "unrecognized",
                    value: "should-not-run",
                    order: order.clone(),
                }),
            },
        );
        let client_options = ClientOptions::new("namespace")
            .plugin(client_registration)
            .build();
        let mut worker_options = WorkerOptions::new("queue").build();

        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert!(order.lock().unwrap().is_empty());
    }

    #[test]
    fn duplicate_worker_plugin_names_warn_and_heartbeat_names_are_deduplicated() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let client_options = ClientOptions::new("namespace")
            .client_plugin(ClientOnlyPlugin)
            .client_plugin(SameNameClientPlugin)
            .build();
        let mut worker_options = WorkerOptions::new("queue")
            .register_workflow::<PluginTestWorkflow>()
            .unwrap()
            .worker_plugin(RecordingWorkerPlugin {
                name: "same-name",
                value: "first",
                order: order.clone(),
            })
            .worker_plugin(RecordingWorkerPlugin {
                name: "same-name",
                value: "second",
                order,
            })
            .build();
        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(
            worker_plugin_warnings(&worker_options.worker_plugins).collect::<Vec<_>>(),
            [WorkerPluginWarning {
                plugin_name: "same-name",
                message: "Multiple worker plugins with the same name were registered",
            }]
        );
        let core_options = worker_options
            .to_core_options("namespace".to_owned(), "identity".to_owned())
            .unwrap();

        let expected_plugins: HashSet<_> = vec![
            PluginInfo {
                name: "client-only".into(),
                version: "".into(),
            },
            PluginInfo {
                name: "same-name".into(),
                version: "".into(),
            },
        ]
        .into_iter()
        .collect();
        assert_eq!(core_options.plugins, expected_plugins);
    }

    struct EmptyClientInterceptor;

    impl ClientInterceptor for EmptyClientInterceptor {}

    struct EmptyWorkerInterceptor;

    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for EmptyWorkerInterceptor {}

    #[test]
    fn simple_plugin_applies_declarative_values() {
        let plugin = SimplePlugin::builder("simple")
            .client_interceptors(vec![
                Arc::new(EmptyClientInterceptor) as Arc<dyn ClientInterceptor>
            ])
            .build();
        let mut client_options = ClientOptions::new("namespace").build();
        client_options
            .client_interceptors
            .push(Arc::new(EmptyClientInterceptor));
        plugin
            .configure_client_options(&mut client_options)
            .unwrap();
        assert_eq!(client_options.client_interceptors.len(), 2);

        let plugin = SimplePlugin::builder("simple")
            .worker_interceptors(vec![
                Arc::new(EmptyWorkerInterceptor) as Arc<dyn WorkerInterceptor>
            ])
            .build();
        let client_options = ClientOptions::new("namespace").build();
        let mut worker_options = WorkerOptions::new("queue")
            .worker_interceptor(EmptyWorkerInterceptor)
            .worker_plugin(plugin)
            .build();
        apply_worker_plugins(&client_options, &mut worker_options).unwrap();

        assert_eq!(worker_options.worker_interceptors.len(), 2);
    }

    #[test]
    fn simple_plugin_options_accept_values_and_functions() {
        let calls = Arc::new(AtomicU8::new(0));
        let plugin = SimplePlugin::builder("simple")
            .data_converter({
                let calls = calls.clone();
                move |existing: Option<DataConverter>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    existing.unwrap()
                }
            })
            .client_interceptors({
                let calls = calls.clone();
                move |existing: Option<Vec<Arc<dyn ClientInterceptor>>>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(existing.unwrap().len(), 1);
                    Vec::new()
                }
            })
            .worker_interceptors({
                let calls = calls.clone();
                move |existing: Option<Vec<Arc<dyn WorkerInterceptor>>>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(existing.unwrap().len(), 1);
                    Vec::new()
                }
            })
            .activity_inbound_interceptors({
                let calls = calls.clone();
                move |existing: Option<Vec<Arc<dyn ActivityInboundInterceptor>>>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    existing.unwrap()
                }
            })
            .workflow_interceptors({
                let calls = calls.clone();
                move |existing: Option<Vec<WorkflowInterceptorConstructor>>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    existing.unwrap()
                }
            })
            .activities({
                let calls = calls.clone();
                move |existing: Option<ActivityDefinitions>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    existing.unwrap()
                }
            })
            .workflows({
                let calls = calls.clone();
                move |existing: Option<WorkflowDefinitions>| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    existing.unwrap()
                }
            })
            .build();

        let mut client_options = ClientOptions::new("namespace").build();
        client_options
            .client_interceptors
            .push(Arc::new(EmptyClientInterceptor));
        plugin
            .configure_client_options(&mut client_options)
            .unwrap();
        assert!(client_options.client_interceptors.is_empty());

        let mut worker_options = WorkerOptions::new("queue")
            .worker_interceptor(EmptyWorkerInterceptor)
            .worker_plugin(plugin)
            .build();
        apply_worker_plugins(
            &ClientOptions::new("namespace").build(),
            &mut worker_options,
        )
        .unwrap();
        assert!(worker_options.worker_interceptors.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 7);
    }

    struct RecursivePlugin(Arc<AtomicU8>);

    impl WorkerPlugin for RecursivePlugin {
        fn name(&self) -> &str {
            "recursive"
        }

        fn configure_worker_options(&self, options: &mut WorkerOptions) -> Result<(), PluginError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            *options = WorkerOptions::new(options.task_queue.clone())
                .worker_plugin(RecursivePlugin(self.0.clone()))
                .worker_plugin(RecursivePlugin(self.0.clone()))
                .build();
            Ok(())
        }
    }

    #[test]
    fn test_plugins_cannot_recurse() {
        let count = Arc::new(AtomicU8::new(0));
        let mut worker_opts = WorkerOptions::new("my-task-queue")
            .worker_plugin(RecursivePlugin(count.clone()))
            .build();
        apply_worker_plugins(&ClientOptions::new("my-ns").build(), &mut worker_opts).unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
