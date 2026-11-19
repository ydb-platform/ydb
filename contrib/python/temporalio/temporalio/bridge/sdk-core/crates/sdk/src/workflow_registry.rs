use std::{collections::HashMap, fmt::Debug, rc::Rc, sync::Arc};

use anyhow::Context;
use temporalio_common::{
    WorkflowDefinition,
    data_converters::{
        DataConverter, GenericPayloadConverter, PayloadConverter, SerializationContext,
        SerializationContextData,
    },
    protos::{
        coresdk::workflow_activation::InitializeWorkflow, temporal::api::common::v1::Payload,
    },
};
use temporalio_workflow::{
    BaseWorkflowContext, PatchActivationCallback,
    runtime::{
        entry::WorkflowImplementation,
        guest::WorkflowInstance,
        host::WorkflowHost,
        instance::{GuestWorkflowInstance, instantiate_workflow},
        types::{WorkflowDefinitionDescriptor, WorkflowInit},
    },
    workflow_interceptors::WorkflowInterceptorConstructor,
};

/// Host-owned execution inputs used to instantiate a single workflow run.
pub(crate) struct WorkflowExecutionInput {
    pub namespace: String,
    pub task_queue: String,
    pub run_id: String,
    pub init_workflow_job: InitializeWorkflow,
    pub data_converter: DataConverter,
    pub host: Rc<dyn WorkflowHost>,
    pub patch_activation_callback: Option<PatchActivationCallback>,
    pub workflow_interceptor_constructors: Vec<WorkflowInterceptorConstructor>,
}

/// Creates workflow execution instances from activation input payloads and context.
pub(crate) type WorkflowExecutionFactory = Arc<
    dyn Fn(WorkflowExecutionInput) -> Result<Box<dyn WorkflowInstance>, anyhow::Error>
        + Send
        + Sync,
>;

#[derive(Clone)]
struct RegisteredWorkflow {
    definition: WorkflowDefinitionDescriptor,
    factory: WorkflowExecutionFactory,
}

/// Error returned when a workflow cannot be registered.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum WorkflowRegistrationError {
    /// The workflow type is already registered.
    #[error("Workflow type {workflow_type} is already registered")]
    DuplicateWorkflowType {
        /// The duplicate workflow type.
        workflow_type: String,
    },

    /// The workflow type has an `#[init]` method and cannot be registered with a factory.
    #[error(
        "Workflow type {workflow_type} must not define an #[init] method when registered with a factory"
    )]
    FactoryRegistrationWithInit {
        /// The workflow type with an `#[init]` method.
        workflow_type: String,
    },
}

/// Contains workflow registrations in a form ready for execution by workers.
#[derive(Default, Clone)]
pub struct WorkflowDefinitions {
    workflows: HashMap<String, RegisteredWorkflow>,
}

impl WorkflowDefinitions {
    pub(crate) fn extend(&mut self, other: &Self) -> Result<(), WorkflowRegistrationError> {
        for workflow in other.workflows.values() {
            self.insert_workflow(workflow.definition.clone(), workflow.factory.clone())?;
        }
        Ok(())
    }

    /// Creates a new empty `WorkflowDefinitions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a workflow implementation.
    ///
    /// Returns an error if a workflow with the same type is already registered.
    pub fn register_workflow<W: WorkflowImplementation>(
        &mut self,
    ) -> Result<&mut Self, WorkflowRegistrationError>
    where
        <W::Run as WorkflowDefinition>::Input: Send,
    {
        let factory = Arc::new(move |input| {
            let (payloads, payload_converter, base_ctx) = workflow_input_parts(input);
            instantiate_workflow::<W>(payloads, payload_converter, base_ctx)
                .context("Failed to instantiate native workflow")
        });
        self.insert_workflow(W::definition(), factory)?;
        Ok(self)
    }

    /// Register a workflow with a custom factory for instance creation.
    ///
    /// Returns an error if a workflow with the same type is already registered, or if the workflow
    /// type defines an `#[init]` method.
    pub fn register_workflow_run_with_factory<W, F>(
        &mut self,
        user_factory: F,
    ) -> Result<&mut Self, WorkflowRegistrationError>
    where
        W: WorkflowImplementation,
        <W::Run as WorkflowDefinition>::Input: Send,
        F: Fn() -> W + Send + Sync + 'static,
    {
        if W::HAS_INIT {
            return Err(WorkflowRegistrationError::FactoryRegistrationWithInit {
                workflow_type: W::definition().workflow_type,
            });
        }

        let factory = Arc::new(move |input| {
            let (payloads, payload_converter, base_ctx) = workflow_input_parts(input);
            let ser_ctx = SerializationContext {
                data: &SerializationContextData::Workflow,
                converter: &payload_converter,
            };
            let input: <W::Run as WorkflowDefinition>::Input =
                payload_converter.from_payloads(&ser_ctx, payloads)?;

            let workflow = user_factory();
            Ok(Box::new(GuestWorkflowInstance::<W>::new_with_workflow(
                workflow,
                base_ctx,
                Some(input),
            )) as Box<dyn WorkflowInstance>)
        });

        self.insert_workflow(W::definition(), factory)?;
        Ok(self)
    }

    /// Check if any workflows are registered.
    pub fn is_empty(&self) -> bool {
        self.workflows.is_empty()
    }

    pub(crate) fn insert_workflow(
        &mut self,
        definition: WorkflowDefinitionDescriptor,
        factory: WorkflowExecutionFactory,
    ) -> Result<(), WorkflowRegistrationError> {
        let workflow_type = definition.workflow_type.clone();
        if self.workflows.contains_key(&workflow_type) {
            return Err(WorkflowRegistrationError::DuplicateWorkflowType { workflow_type });
        }
        self.workflows.insert(
            workflow_type,
            RegisteredWorkflow {
                definition,
                factory,
            },
        );
        Ok(())
    }

    pub(crate) fn get_workflow(&self, workflow_type: &str) -> Option<WorkflowExecutionFactory> {
        self.workflows
            .get(workflow_type)
            .map(|wf| wf.factory.clone())
    }

    /// Returns an iterator over registered workflow definitions.
    pub fn workflow_definitions(&self) -> impl Iterator<Item = &WorkflowDefinitionDescriptor> + '_ {
        self.workflows.values().map(|wf| &wf.definition)
    }
}

fn workflow_input_parts(
    input: WorkflowExecutionInput,
) -> (Vec<Payload>, PayloadConverter, BaseWorkflowContext) {
    let WorkflowExecutionInput {
        namespace,
        task_queue,
        run_id,
        init_workflow_job,
        data_converter,
        host,
        patch_activation_callback,
        workflow_interceptor_constructors,
    } = input;
    let payloads = init_workflow_job.arguments.clone();
    let payload_converter = data_converter.payload_converter().clone();
    let init = WorkflowInit {
        namespace,
        task_queue,
        run_id,
        initialize_workflow: init_workflow_job,
    };
    let base_ctx = BaseWorkflowContext::from_raw(
        init,
        data_converter,
        host,
        patch_activation_callback,
        workflow_interceptor_constructors,
    );
    (payloads, payload_converter, base_ctx)
}

impl Debug for WorkflowDefinitions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowDefinitions")
            .field("workflows", &self.workflows.keys().collect::<Vec<_>>())
            .finish()
    }
}
