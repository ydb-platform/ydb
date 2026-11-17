//! Component-model guest export support for workflow crates.
//!
//! Everything in this module is internal SDK/component glue.
use crate::{
    BaseWorkflowContext, PatchActivationCallback,
    runtime::{
        entry::WorkflowImplementation,
        guest::WorkflowInstance as RuntimeWorkflowInstance,
        host::WorkflowHost,
        instance::instantiate_workflow,
        types::{
            ActivationJobResult, MainRoutineCompletion, RoutineCompletion, RoutinePendingState,
            TerminalOutcome, UpdateRoutineCompletion, WorkflowDefinitionDescriptor,
            WorkflowFailure, WorkflowInit,
        },
    },
    workflow_interceptors::WorkflowInterceptorConstructor,
};
use futures_util::task::noop_waker;
use prost::Message;
use std::{cell::RefCell, marker::PhantomData, rc::Rc, sync::Arc};
use temporalio_common_wasm::{
    data_converters::DataConverter,
    protos::{coresdk::workflow_commands::WorkflowCommand, temporal::api::failure::v1::Failure},
};

pub mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "workflow-module",
        pub_export_macro: true,
        with: {
            "temporal:workflow-runtime/types@0.1.0/routine-kind": crate::runtime::types::RoutineKind,
            "temporal:workflow-runtime/types@0.1.0/started-routine": crate::runtime::types::StartedRoutine,
            "temporal:workflow-runtime/types@0.1.0/update-definition": crate::runtime::types::UpdateDefinitionDescriptor,
            "temporal:workflow-runtime/types@0.1.0/update-routine-kind": crate::runtime::types::UpdateRoutineKind,
            "temporal:workflow-runtime/types@0.1.0/workflow-definition": crate::runtime::types::WorkflowDefinitionDescriptor,
        },
    });
}

pub use bindings::export as __wit_export;

use self::bindings::{
    exports::temporal::workflow_runtime::workflow_guest as wit_guest,
    temporal::workflow_runtime::{types as wit_types, workflow_host as wit_host},
};

pub trait StaticWorkflowComponent {
    fn list_workflows() -> Vec<WorkflowDefinitionDescriptor>;
    fn instantiate_workflow(
        workflow_type: &str,
        init: WorkflowInit,
        host: Rc<dyn WorkflowHost>,
    ) -> Result<Box<dyn RuntimeWorkflowInstance>, WorkflowFailure>;
}

pub struct ExportedComponent<T>(PhantomData<T>);

impl<T: StaticWorkflowComponent> wit_guest::Guest for ExportedComponent<T> {
    type WorkflowInstance = ExportedWorkflowInstance;

    fn list_workflows() -> Vec<wit_guest::WorkflowDefinition> {
        T::list_workflows()
    }

    fn instantiate_workflow(
        init: wit_guest::WorkflowInit,
    ) -> Result<wit_guest::WorkflowInstance, wit_guest::Failure> {
        let host: Rc<dyn WorkflowHost> = Rc::new(ImportedWorkflowHost);
        let init = WorkflowInit {
            namespace: init.namespace,
            task_queue: init.task_queue,
            run_id: init.run_id,
            initialize_workflow: decode_proto(init.initialize_workflow),
        };
        let workflow_type = init.initialize_workflow.workflow_type.clone();
        let instance =
            T::instantiate_workflow(&workflow_type, init, host).map_err(|e| e.encode_to_vec())?;
        Ok(wit_guest::WorkflowInstance::new(ExportedWorkflowInstance(
            RefCell::new(instance),
        )))
    }
}

pub struct ExportedWorkflowInstance(RefCell<Box<dyn RuntimeWorkflowInstance>>);

impl wit_guest::GuestWorkflowInstance for ExportedWorkflowInstance {
    fn activate(
        &self,
        activation: wit_guest::WorkflowActivation,
    ) -> Result<wit_guest::ActivationResult, wit_guest::Failure> {
        self.0
            .borrow_mut()
            .activate(decode_proto(activation), &noop_waker())
            .map(|result| wit_types::ActivationResult {
                job_results: result
                    .job_results
                    .into_iter()
                    .map(|result| match result {
                        ActivationJobResult::None => wit_types::ActivationJobResult::None,
                        ActivationJobResult::StartedRoutine(routine) => {
                            wit_types::ActivationJobResult::StartedRoutine(routine)
                        }
                        ActivationJobResult::QueryResponse(response) => {
                            wit_types::ActivationJobResult::QueryResponse(
                                wit_types::QueryResponse {
                                    response: response
                                        .result
                                        .map(|e| e.encode_to_vec())
                                        .map_err(|e| e.encode_to_vec()),
                                },
                            )
                        }
                        ActivationJobResult::UpdateRejected(failure) => {
                            wit_types::ActivationJobResult::UpdateRejected(failure.encode_to_vec())
                        }
                    })
                    .collect(),
            })
            .map_err(|e| e.encode_to_vec())
    }

    fn poll_routine(
        &self,
        routine_id: wit_guest::RoutineId,
    ) -> Result<wit_guest::RoutinePollResult, wit_guest::Failure> {
        let waker = noop_waker();
        self.0
            .borrow_mut()
            .poll_routine(routine_id, &waker)
            .map(|result| wit_types::RoutinePollResult {
                completion: result.completion.map(|completion| match completion {
                    RoutineCompletion::Main(completion) => {
                        wit_types::RoutineCompletion::Main(match completion {
                            MainRoutineCompletion::Blocked => {
                                wit_types::MainRoutineCompletion::Blocked
                            }
                            MainRoutineCompletion::TaskFailed(task_failure) => {
                                wit_types::MainRoutineCompletion::TaskFailed(
                                    wit_types::TaskFailure {
                                        failure: task_failure.failure.encode_to_vec(),
                                        force_cause: task_failure.force_cause,
                                    },
                                )
                            }
                            MainRoutineCompletion::Terminal(outcome) => {
                                wit_types::MainRoutineCompletion::Terminal(match *outcome {
                                    TerminalOutcome::Completed(payload) => {
                                        wit_types::TerminalOutcome::Completed(
                                            payload.encode_to_vec(),
                                        )
                                    }
                                    TerminalOutcome::Failed(failure) => {
                                        wit_types::TerminalOutcome::Failed(failure.encode_to_vec())
                                    }
                                    TerminalOutcome::Cancelled => {
                                        wit_types::TerminalOutcome::Cancelled
                                    }
                                    TerminalOutcome::ContinueAsNew(req) => {
                                        wit_types::TerminalOutcome::ContinueAsNew(
                                            req.encode_to_vec(),
                                        )
                                    }
                                })
                            }
                        })
                    }
                    RoutineCompletion::Signal(result) => {
                        wit_types::RoutineCompletion::Signal(match result {
                            Ok(()) => wit_types::SignalRoutineCompletion::Succeeded,
                            Err(failure) => {
                                wit_types::SignalRoutineCompletion::Failed(failure.encode_to_vec())
                            }
                        })
                    }
                    RoutineCompletion::Update(completion) => {
                        wit_types::RoutineCompletion::Update(match completion {
                            UpdateRoutineCompletion::Completed {
                                protocol_instance_id,
                                result,
                            } => wit_types::UpdateRoutineCompletion::Completed(
                                wit_types::UpdateRoutineSuccess {
                                    protocol_instance_id,
                                    value: result.encode_to_vec(),
                                },
                            ),
                            UpdateRoutineCompletion::Rejected {
                                protocol_instance_id,
                                failure,
                            } => wit_types::UpdateRoutineCompletion::Rejected(
                                wit_types::UpdateRoutineRejection {
                                    protocol_instance_id,
                                    failure: failure.encode_to_vec(),
                                },
                            ),
                        })
                    }
                }),
                made_progress: result.made_progress,
                pending_state: result.pending_state.map(|state| match state {
                    RoutinePendingState::Handler => wit_types::RoutinePendingState::Handler,
                    RoutinePendingState::Interceptor => wit_types::RoutinePendingState::Interceptor,
                    RoutinePendingState::InterceptorWithActivation => {
                        wit_types::RoutinePendingState::InterceptorWithActivation
                    }
                }),
            })
            .map_err(|e| e.encode_to_vec())
    }
}

pub fn instantiate_component_workflow<W: WorkflowImplementation>(
    init: WorkflowInit,
    host: Rc<dyn WorkflowHost>,
) -> Result<Box<dyn RuntimeWorkflowInstance>, WorkflowFailure>
where
    <W::Run as temporalio_common_wasm::WorkflowDefinition>::Input: Send,
{
    instantiate_component_workflow_with_interceptor_constructors::<W>(init, host, Vec::new())
}

pub fn instantiate_component_workflow_with_interceptor_constructors<W: WorkflowImplementation>(
    init: WorkflowInit,
    host: Rc<dyn WorkflowHost>,
    interceptor_constructors: Vec<WorkflowInterceptorConstructor>,
) -> Result<Box<dyn RuntimeWorkflowInstance>, WorkflowFailure>
where
    <W::Run as temporalio_common_wasm::WorkflowDefinition>::Input: Send,
{
    let args = init.initialize_workflow.arguments.clone();
    let data_converter = DataConverter::default();
    let payload_converter = data_converter.payload_converter().clone();
    let patch_activation_callback: PatchActivationCallback =
        Arc::new(|input| wit_host::patch_activation(&input.patch_id));
    let base_ctx = BaseWorkflowContext::from_raw(
        init,
        data_converter,
        host,
        Some(patch_activation_callback),
        interceptor_constructors,
    );
    instantiate_workflow::<W>(args, payload_converter, base_ctx).map_err(|err| {
        Box::new(Failure {
            message: format!("Workflow input deserialization failed: {err}"),
            ..Default::default()
        })
    })
}

struct ImportedWorkflowHost;

impl WorkflowHost for ImportedWorkflowHost {
    fn set_current_details(&self, details: String) {
        wit_host::set_current_details(&details);
    }

    fn push_command(&self, command: WorkflowCommand) {
        wit_host::push_command(&command.encode_to_vec());
    }
}

fn decode_proto<M: Message + prost::Name + Default>(bytes: Vec<u8>) -> M {
    M::decode(bytes.as_slice()).unwrap_or_else(|err| {
        let n = M::NAME;
        panic!("failed to decode {n} from WASM boundary bytes: {err}")
    })
}
