use std::{fs, io, path::PathBuf, rc::Rc, sync::Arc};

use anyhow::{Context, bail};
use prost::Message;
use temporalio_common::protos::{
    coresdk::workflow_commands::WorkflowCommand, temporal::api::failure::v1::Failure,
};
use temporalio_workflow::{
    PatchActivationCaller,
    runtime::{
        guest::WorkflowInstance,
        host::WorkflowHost,
        types::{
            ActivationJobResult, ActivationResult, MainRoutineCompletion, QueryResponse,
            RoutineCompletion, RoutinePendingState, RoutinePollResult, StartedRoutine, TaskFailure,
            TerminalOutcome, UpdateRoutineCompletion, UpdateRoutineKind, WorkflowActivation,
            WorkflowDefinitionDescriptor, WorkflowFailure,
        },
    },
};
use wasmtime::{
    Config, Engine, Store,
    component::{Component, HasSelf, Linker, ResourceAny},
};

use crate::workflow_registry::{
    WorkflowDefinitions, WorkflowExecutionFactory, WorkflowExecutionInput,
};

wasmtime::component::bindgen!({
    path: "../workflow/wit",
    world: "workflow-module",
});

use self::{
    exports::temporal::workflow_runtime::workflow_guest as wit_guest,
    temporal::workflow_runtime::{types as wit_types, workflow_host as wit_host},
};

/// A prebuilt WebAssembly component that exports one or more Temporal workflows.
#[derive(Clone, Debug)]
pub struct WasmWorkflowComponent {
    component_id: String,
    source: WasmWorkflowComponentSource,
}

#[derive(Clone, Debug)]
enum WasmWorkflowComponentSource {
    File(PathBuf),
    Bytes(Arc<[u8]>),
}

impl WasmWorkflowComponent {
    /// Register a workflow component from a component file on disk.
    pub fn from_file(
        component_id: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> io::Result<Self> {
        let path = path.into();
        if !path.try_exists()? {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "WASM workflow component file does not exist: {}",
                    path.display()
                ),
            ));
        }
        Ok(Self {
            component_id: component_id.into(),
            source: WasmWorkflowComponentSource::File(path),
        })
    }

    /// Register a workflow component from in-memory bytes.
    pub fn from_bytes(
        component_id: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> io::Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WASM workflow component bytes must not be empty",
            ));
        }
        Ok(Self {
            component_id: component_id.into(),
            source: WasmWorkflowComponentSource::Bytes(bytes),
        })
    }
}

impl WorkflowDefinitions {
    pub(crate) fn register_wasm_workflows(
        &mut self,
        components: Vec<WasmWorkflowComponent>,
        has_native_workflow_interceptors: bool,
    ) -> Result<(), anyhow::Error> {
        if !components.is_empty() && has_native_workflow_interceptors {
            tracing::warn!(
                "Native workflow interceptors will not be applied to WASM workflow components; \
                 define interceptors in the WASM bundle instead"
            );
        }
        for component in components {
            let module = Arc::new(CompiledWasmWorkflowModule::new(component)?);
            for definition in &module.definitions {
                let module = module.clone();
                let factory: WorkflowExecutionFactory =
                    Arc::new(move |input| module.instantiate(input));
                self.insert_workflow(definition.clone(), factory)?;
            }
        }
        Ok(())
    }
}

struct CompiledWasmWorkflowModule {
    engine: Engine,
    component: Component,
    definitions: Vec<WorkflowDefinitionDescriptor>,
}

impl CompiledWasmWorkflowModule {
    fn new(component: WasmWorkflowComponent) -> Result<Self, anyhow::Error> {
        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;
        let bytes: Arc<[u8]> = match &component.source {
            WasmWorkflowComponentSource::File(path) => fs::read(path)
                .with_context(|| format!("Failed reading WASM component {}", path.display()))?
                .into(),
            WasmWorkflowComponentSource::Bytes(bytes) => bytes.clone(),
        };
        let component = Component::new(&engine, bytes.as_ref()).map_err(|err| {
            anyhow::Error::msg(format!(
                "Failed to compile WASM component {}: {err}",
                component.component_id
            ))
        })?;
        let definitions = Self::read_definitions(&engine, &component)?;
        if definitions.is_empty() {
            bail!("WASM component exports no workflows");
        }
        Ok(Self {
            engine,
            component,
            definitions,
        })
    }

    fn read_definitions(
        engine: &Engine,
        component: &Component,
    ) -> Result<Vec<WorkflowDefinitionDescriptor>, anyhow::Error> {
        let mut linker = Linker::new(engine);
        WorkflowModule::add_to_linker::<_, HasSelf<_>>(&mut linker, |data| data)?;
        let mut store = Store::new(
            engine,
            WasmWorkflowHostState::new(Rc::new(NoopWorkflowHost), None),
        );
        let module = WorkflowModule::instantiate(&mut store, component, &linker)?;
        module
            .temporal_workflow_runtime_workflow_guest()
            .call_list_workflows(&mut store)
            .map(|defs| {
                defs.into_iter()
                    .map(|def| WorkflowDefinitionDescriptor {
                        workflow_type: def.workflow_type,
                        has_init: def.has_init,
                        init_takes_input: def.init_takes_input,
                        signals: def.signals,
                        queries: def.queries,
                        updates: def
                            .updates
                            .into_iter()
                            .map(|u| {
                                temporalio_workflow::runtime::types::UpdateDefinitionDescriptor {
                                    name: u.name,
                                    has_validator: u.has_validator,
                                }
                            })
                            .collect(),
                    })
                    .collect()
            })
            .map_err(|err| {
                anyhow::Error::msg(format!(
                    "Failed to list workflows exported by WASM component: {err}"
                ))
            })
    }

    fn instantiate(
        &self,
        input: WorkflowExecutionInput,
    ) -> Result<Box<dyn WorkflowInstance>, anyhow::Error> {
        let mut linker = Linker::new(&self.engine);
        WorkflowModule::add_to_linker::<_, HasSelf<_>>(&mut linker, |data| data)?;
        let WorkflowExecutionInput {
            namespace,
            task_queue,
            run_id,
            init_workflow_job,
            data_converter,
            host,
            patch_activation_callback,
            ..
        } = input;
        let workflow_init = wit_types::WorkflowInit {
            namespace: namespace.clone(),
            task_queue: task_queue.clone(),
            run_id: run_id.clone(),
            initialize_workflow: init_workflow_job.encode_to_vec(),
        };
        let patch_activation = patch_activation_callback.map(|callback| {
            PatchActivationCaller::new(
                callback,
                namespace,
                task_queue,
                run_id,
                init_workflow_job,
                data_converter.payload_converter().clone(),
            )
        });
        let mut store = Store::new(
            &self.engine,
            WasmWorkflowHostState::new(host, patch_activation),
        );
        let module = WorkflowModule::instantiate(&mut store, &self.component, &linker)?;
        let guest = module.temporal_workflow_runtime_workflow_guest();
        let workflow_instance = guest
            .call_instantiate_workflow(&mut store, &workflow_init)
            .map_err(|err| {
                anyhow::Error::msg(format!("Failed to instantiate WASM workflow: {err}"))
            })?
            .map_err(convert_failure)
            .map_err(|failure| {
                anyhow::Error::msg(format!(
                    "WASM workflow initialization failed: {}",
                    failure.message
                ))
            })?;

        Ok(Box::new(WasmWorkflowInstance {
            store,
            guest: guest.clone(),
            workflow_instance,
        }))
    }
}

struct WasmWorkflowInstance {
    store: Store<WasmWorkflowHostState>,
    guest: wit_guest::Guest,
    workflow_instance: ResourceAny,
}

impl WorkflowInstance for WasmWorkflowInstance {
    fn activate(
        &mut self,
        activation: WorkflowActivation,
        _waker: &std::task::Waker,
    ) -> Result<ActivationResult, WorkflowFailure> {
        let result = self.guest.workflow_instance().call_activate(
            &mut self.store,
            self.workflow_instance,
            &activation.encode_to_vec(),
        );
        trap_to_failure(result, |result| ActivationResult {
            job_results: result
                .job_results
                .into_iter()
                .map(|job_result| match job_result {
                    wit_types::ActivationJobResult::None => ActivationJobResult::None,
                    wit_types::ActivationJobResult::StartedRoutine(routine) => {
                        ActivationJobResult::StartedRoutine(StartedRoutine {
                            routine_id: routine.routine_id,
                            kind: match routine.kind {
                                wit_types::RoutineKind::Main => {
                                    temporalio_workflow::runtime::types::RoutineKind::Main
                                }
                                wit_types::RoutineKind::Signal(name) => {
                                    temporalio_workflow::runtime::types::RoutineKind::Signal(name)
                                }
                                wit_types::RoutineKind::Update(update) => {
                                    temporalio_workflow::runtime::types::RoutineKind::Update(
                                        UpdateRoutineKind {
                                            name: update.name,
                                            update_id: update.update_id,
                                            protocol_instance_id: update.protocol_instance_id,
                                        },
                                    )
                                }
                            },
                        })
                    }
                    wit_types::ActivationJobResult::QueryResponse(response) => {
                        ActivationJobResult::QueryResponse(Box::new(QueryResponse {
                            result: response
                                .response
                                .map(decode_proto)
                                .map_err(|failure| *convert_failure(failure)),
                        }))
                    }
                    wit_types::ActivationJobResult::UpdateRejected(failure) => {
                        ActivationJobResult::UpdateRejected(convert_failure(failure))
                    }
                })
                .collect(),
        })
    }

    fn poll_routine(
        &mut self,
        routine_id: u64,
        _waker: &std::task::Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        let result = self.guest.workflow_instance().call_poll_routine(
            &mut self.store,
            self.workflow_instance,
            routine_id,
        );
        trap_to_failure(result, |result| RoutinePollResult {
            completion: result.completion.map(|completion| match completion {
                wit_types::RoutineCompletion::Main(completion) => {
                    RoutineCompletion::Main(match completion {
                        wit_types::MainRoutineCompletion::Blocked => MainRoutineCompletion::Blocked,
                        wit_types::MainRoutineCompletion::TaskFailed(task_failure) => {
                            MainRoutineCompletion::TaskFailed(TaskFailure {
                                failure: convert_failure(task_failure.failure),
                                force_cause: task_failure.force_cause,
                            })
                        }
                        wit_types::MainRoutineCompletion::Terminal(outcome) => {
                            MainRoutineCompletion::Terminal(Box::new(match outcome {
                                wit_types::TerminalOutcome::Completed(payload) => {
                                    TerminalOutcome::Completed(decode_proto(payload))
                                }
                                wit_types::TerminalOutcome::Failed(failure) => {
                                    TerminalOutcome::Failed(convert_failure(failure))
                                }
                                wit_types::TerminalOutcome::Cancelled => TerminalOutcome::Cancelled,
                                wit_types::TerminalOutcome::ContinueAsNew(req) => {
                                    TerminalOutcome::ContinueAsNew(Box::new(decode_proto(req)))
                                }
                            }))
                        }
                    })
                }
                wit_types::RoutineCompletion::Signal(result) => {
                    RoutineCompletion::Signal(match result {
                        wit_types::SignalRoutineCompletion::Succeeded => Ok(()),
                        wit_types::SignalRoutineCompletion::Failed(failure) => {
                            Err(convert_failure(failure))
                        }
                    })
                }
                wit_types::RoutineCompletion::Update(completion) => {
                    RoutineCompletion::Update(match completion {
                        wit_types::UpdateRoutineCompletion::Completed(success) => {
                            UpdateRoutineCompletion::Completed {
                                protocol_instance_id: success.protocol_instance_id,
                                result: decode_proto(success.value),
                            }
                        }
                        wit_types::UpdateRoutineCompletion::Rejected(rejection) => {
                            UpdateRoutineCompletion::Rejected {
                                protocol_instance_id: rejection.protocol_instance_id,
                                failure: convert_failure(rejection.failure),
                            }
                        }
                    })
                }
            }),
            made_progress: result.made_progress,
            pending_state: result.pending_state.map(|state| match state {
                wit_types::RoutinePendingState::Handler => RoutinePendingState::Handler,
                wit_types::RoutinePendingState::Interceptor => RoutinePendingState::Interceptor,
                wit_types::RoutinePendingState::InterceptorWithActivation => {
                    RoutinePendingState::InterceptorWithActivation
                }
            }),
        })
    }
}

struct WasmWorkflowHostState {
    host: Rc<dyn WorkflowHost>,
    patch_activation: Option<PatchActivationCaller>,
}

impl WasmWorkflowHostState {
    fn new(host: Rc<dyn WorkflowHost>, patch_activation: Option<PatchActivationCaller>) -> Self {
        Self {
            host,
            patch_activation,
        }
    }
}

impl wit_types::Host for WasmWorkflowHostState {}

impl wit_host::Host for WasmWorkflowHostState {
    fn set_current_details(&mut self, details: String) {
        self.host.set_current_details(details);
    }

    fn push_command(&mut self, command: wit_types::WorkflowCommand) {
        self.host.push_command(decode_proto(command));
    }

    fn patch_activation(&mut self, patch_id: String) -> bool {
        let Some(patch_activation) = &self.patch_activation else {
            return true;
        };
        patch_activation.call(patch_id)
    }
}

struct NoopWorkflowHost;

impl WorkflowHost for NoopWorkflowHost {
    fn set_current_details(&self, _details: String) {}
    fn push_command(&self, _command: WorkflowCommand) {}
}

fn trap_to_failure<T, U>(
    result: Result<Result<T, wit_types::Failure>, wasmtime::Error>,
    convert: impl FnOnce(T) -> U,
) -> Result<U, WorkflowFailure> {
    result
        .map_err(|trap| {
            Box::new(Failure {
                message: format!("WASM workflow trapped: {trap}"),
                ..Default::default()
            })
        })?
        .map(convert)
        .map_err(convert_failure)
}

fn convert_failure(failure: wit_types::Failure) -> WorkflowFailure {
    Box::new(decode_proto(failure))
}

fn decode_proto<M: Message + prost::Name + Default>(bytes: Vec<u8>) -> M {
    M::decode(bytes.as_slice()).unwrap_or_else(|err| {
        let n = M::NAME;
        panic!("failed to decode {n} from WASM boundary bytes: {err}")
    })
}
