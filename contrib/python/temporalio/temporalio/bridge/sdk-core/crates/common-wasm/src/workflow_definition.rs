use crate::data_converters::{RawValue, TemporalDeserializable, TemporalSerializable};

/// Implement on a marker struct to define a workflow.
///
/// Typically, you will want to use the `#[workflow]` and `#[workflow_methods]` macros to define
/// workflows. However, this trait may be implemented manually if desired.
pub trait WorkflowDefinition {
    /// Type of the input argument to the workflow
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the workflow
    type Output: TemporalDeserializable + TemporalSerializable + 'static;
    /// The workflow type name
    fn name(&self) -> &str;
}

/// Indicates that a type is associated with a [`WorkflowDefinition`], enabling typed
/// signal, query, and update methods on `WorkflowHandle`.
///
/// Structs annotated with `#[workflow_methods]` implement this trait automatically.
pub trait HasWorkflowDefinition: WorkflowDefinition {
    /// The [`WorkflowDefinition`] type that [`SignalDefinition`], [`QueryDefinition`], and
    /// [`UpdateDefinition`] are associated with.
    type Run: WorkflowDefinition;
}

/// Marker type for untyped workflow handles. Stores the workflow type name. Uses [`RawValue`]
/// for both input and output.
pub struct UntypedWorkflow {
    name: String,
}
impl UntypedWorkflow {
    /// Create a new `UntypedWorkflow` with the given workflow type name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}
impl WorkflowDefinition for UntypedWorkflow {
    type Input = RawValue;
    type Output = RawValue;
    fn name(&self) -> &str {
        &self.name
    }
}

impl HasWorkflowDefinition for UntypedWorkflow {
    type Run = Self;
}

/// Implement on a marker struct to define a query.
///
/// Typically, you will want to use the `#[query]` attribute inside a `#[workflow_methods]` macro
/// to define updates. However, this trait may be implemented manually if desired.
pub trait QueryDefinition {
    /// The workflow type this query belongs to
    type Workflow: WorkflowDefinition;
    /// Type of the input argument to the query.
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the query.
    type Output: TemporalDeserializable + TemporalSerializable + 'static;

    /// The workflow type name.
    fn name(&self) -> &str;
}

/// Implement on a marker struct to define a signal.
///
/// Typically, you will want to use the `#[signal]` attribute inside a `#[workflow_methods]` macro
/// to define signals. However, this trait may be implemented manually if desired.
pub trait SignalDefinition {
    /// The workflow type this signal belongs to
    type Workflow: WorkflowDefinition;
    /// Type of the input argument to the signal.
    type Input: TemporalDeserializable + TemporalSerializable + 'static;

    /// The workflow type name.
    fn name(&self) -> &str;
}

/// Implement on a marker struct to define an update.
///
/// Typically, you will want to use the `#[update]` attribute inside a `#[workflow_methods]` macro
/// to define updates. However, this trait may be implemented manually if desired.
pub trait UpdateDefinition {
    /// The workflow type this update belongs to
    type Workflow: WorkflowDefinition;
    /// Type of the input argument to the update.
    type Input: TemporalDeserializable + TemporalSerializable + 'static;
    /// Type of the output of the update.
    type Output: TemporalDeserializable + TemporalSerializable + 'static;

    /// The workflow type name.
    fn name(&self) -> &str;
}
