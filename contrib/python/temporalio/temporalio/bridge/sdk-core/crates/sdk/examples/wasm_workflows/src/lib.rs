use temporalio_workflow::{
    WorkflowContext, WorkflowResult,
    common::protos::temporal::api::{
        enums::v1::WorkflowTaskFailedCause,
        failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
    },
    component::{StaticWorkflowComponent, instantiate_component_workflow},
    runtime::{
        guest::WorkflowInstance,
        host::WorkflowHost,
        types::{
            ActivationJobResult, ActivationResult, MAIN_ROUTINE_ID, MainRoutineCompletion,
            RoutineCompletion, RoutinePollResult, TaskFailure, WorkflowDefinitionDescriptor,
            WorkflowFailure, WorkflowInit,
        },
    },
    workflow, workflow_methods,
};

#[workflow]
#[derive(Default)]
pub struct HelloWorkflow;

#[workflow_methods]
impl HelloWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(format!("Hello, {name}!"))
    }
}

struct WasmTaskFailureWorkflow;

impl WorkflowInstance for WasmTaskFailureWorkflow {
    fn activate(
        &mut self,
        activation: temporalio_workflow::runtime::types::WorkflowActivation,
        _waker: &std::task::Waker,
    ) -> Result<ActivationResult, WorkflowFailure> {
        Ok(ActivationResult {
            job_results: activation
                .jobs
                .iter()
                .map(|_| ActivationJobResult::None)
                .collect(),
        })
    }

    fn poll_routine(
        &mut self,
        routine_id: u64,
        _waker: &std::task::Waker,
    ) -> Result<RoutinePollResult, WorkflowFailure> {
        if routine_id != MAIN_ROUTINE_ID {
            return Err(Box::new(Failure {
                message: format!("unexpected routine id {routine_id}"),
                ..Default::default()
            }));
        }

        Ok(RoutinePollResult {
            completion: Some(RoutineCompletion::Main(MainRoutineCompletion::TaskFailed(
                TaskFailure {
                    failure: Box::new(Failure {
                        message: "structured wasm workflow task failure".to_string(),
                        failure_info: Some(FailureInfo::ApplicationFailureInfo(
                            ApplicationFailureInfo {
                                r#type: "WasmTaskFailure".to_string(),
                                non_retryable: true,
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    }),
                    force_cause: Some(WorkflowTaskFailedCause::NonDeterministicError as u32),
                },
            ))),
            made_progress: true,
            pending_state: None,
        })
    }
}

struct WasmTestWorkflowModule;

impl StaticWorkflowComponent for WasmTestWorkflowModule {
    fn list_workflows() -> Vec<WorkflowDefinitionDescriptor> {
        vec![
            <HelloWorkflow as temporalio_workflow::runtime::entry::WorkflowImplementation>::definition(),
            WorkflowDefinitionDescriptor {
                workflow_type: "WasmTaskFailureWorkflow".to_string(),
                has_init: false,
                init_takes_input: false,
                signals: vec![],
                queries: vec![],
                updates: vec![],
            },
        ]
    }

    fn instantiate_workflow(
        workflow_type: &str,
        init: WorkflowInit,
        host: std::rc::Rc<dyn WorkflowHost>,
    ) -> Result<Box<dyn WorkflowInstance>, WorkflowFailure> {
        match workflow_type {
            name if name
                == <HelloWorkflow as temporalio_workflow::runtime::entry::WorkflowImplementation>::name() =>
            {
                instantiate_component_workflow::<HelloWorkflow>(init, host)
            }
            "WasmTaskFailureWorkflow" => Ok(Box::new(WasmTaskFailureWorkflow)),
            _ => Err(Box::new(Failure {
                message: format!("No workflow named '{workflow_type}' exported by this component"),
                ..Default::default()
            })),
        }
    }
}

type WasmTestWorkflowComponentExport =
    temporalio_workflow::component::ExportedComponent<WasmTestWorkflowModule>;

temporalio_workflow::__temporalio_export_workflow_component!(WasmTestWorkflowComponentExport);
