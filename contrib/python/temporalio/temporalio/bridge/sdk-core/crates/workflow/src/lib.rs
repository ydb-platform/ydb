#![warn(missing_docs)]

//! Temporal workflow authoring APIs and runtime glue.

extern crate self as temporalio_workflow;

pub use temporalio_common_wasm as common;
pub use temporalio_macros::{
    init, query, run, signal, update, update_validator, workflow, workflow_methods,
};

#[doc(hidden)]
pub mod __private {
    pub use futures_util;
}

mod cancellation;
#[doc(hidden)]
pub mod component;
mod memo;
#[doc(hidden)]
pub mod runtime;
mod workflow_context;
pub mod workflow_interceptors;
pub mod workflows;

pub use cancellation::{WorkflowCancellationError, WorkflowCancellationToken};
pub use memo::{MemoValue, MemoValues};
#[doc(hidden)]
pub use runtime::model::{CancellableID, UnblockEvent};
pub use runtime::model::{TimerResult, WorkflowResult, WorkflowTermination};
#[doc(hidden)]
pub use runtime::{SdkWakeGuard, is_sdk_wake};
pub use temporalio_common_wasm::{
    ActivityCloseTimeouts, Memo, RetryPolicy,
    error::{
        ActivityExecutionError, ChildWorkflowExecutionError, ChildWorkflowStartError, RetryState,
        TimeoutType, WorkflowSignalError,
    },
};
pub use workflow_context::{
    ActivityCancellationType, ActivityOptions, BaseWorkflowContext, CancellableFuture,
    CancellableFutureWithReason, ChildWorkflowCancellationType, ChildWorkflowOptions,
    ContinueAsNewOptions, ContinueAsNewVersioningBehavior, ExternalWorkflowHandle,
    LocalActivityOptions, NamespacedWorkflowInfo, NexusOperationCancellationType,
    NexusOperationOptions, ParentClosePolicy, SignalWorkflowOptions,
    StartChildWorkflowExecutionFailedCause, StartChildWorkflowOutput, StartedChildWorkflow,
    StartedNexusOperation, SyncWorkflowContext, TimerOptions, VersioningIntent,
    WaitConditionOptions, WorkflowContext, WorkflowContextView, WorkflowIdReusePolicy,
    WorkflowRandomValue,
};
#[doc(hidden)]
pub use workflow_context::{PatchActivationCallback, PatchActivationCaller};
pub use workflows::{join, join_all, select};

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_select {
    ($($tokens:tt)*) => {
        $crate::__private::futures_util::select_biased! { $($tokens)* }
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __temporal_join {
    ($($tokens:tt)*) => {
        $crate::__private::futures_util::join!($($tokens)*)
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __temporalio_export_workflow_component {
    ($export_type:ident) => {
        $crate::component::__wit_export!(
            $export_type with_types_in $crate::component::bindings
        );
    };
}

#[macro_export]
/// Export one or more workflow implementations as a component-model workflow module.
///
/// Component-side workflow interceptor constructors can be supplied with
/// `interceptor_constructors = [constructor]`. Each constructor receives a read-only workflow
/// context and is invoked for every workflow instance.
macro_rules! export_workflow_module {
    ([$($workflow:ty),+ $(,)?]) => {
        ::temporalio_workflow::export_workflow_module!(
            [$($workflow),+],
            interceptor_constructors = [],
        );
    };
    ([$($workflow:ty),+ $(,)?], interceptor_constructors = [$($constructor:expr),* $(,)?] $(,)?) => {
        const _: () = {
            struct __TemporalWorkflowModule;

            fn __temporal_workflow_interceptor_constructors() -> ::std::vec::Vec<
                ::temporalio_workflow::workflow_interceptors::WorkflowInterceptorConstructor,
            > {
                ::std::vec![
                    $(
                        ::temporalio_workflow::workflow_interceptors::WorkflowInterceptorConstructor::new(
                            $constructor,
                        )
                    ),*
                ]
            }

            impl ::temporalio_workflow::component::StaticWorkflowComponent for __TemporalWorkflowModule {
                fn list_workflows(
                ) -> ::std::vec::Vec<::temporalio_workflow::runtime::types::WorkflowDefinitionDescriptor> {
                    ::std::vec![$(<$workflow as ::temporalio_workflow::runtime::entry::WorkflowImplementation>::definition()),*]
                }

                fn instantiate_workflow(
                    workflow_type: &str,
                    init: ::temporalio_workflow::runtime::types::WorkflowInit,
                    host: ::std::rc::Rc<dyn ::temporalio_workflow::runtime::host::WorkflowHost>,
                ) -> ::std::result::Result<
                    ::std::boxed::Box<dyn ::temporalio_workflow::runtime::guest::WorkflowInstance>,
                    ::temporalio_workflow::runtime::types::WorkflowFailure,
                > {
                    match workflow_type {
                        $(
                            name if name == <$workflow as ::temporalio_workflow::runtime::entry::WorkflowImplementation>::name() => {
                                ::temporalio_workflow::component::instantiate_component_workflow_with_interceptor_constructors::<$workflow>(
                                    init,
                                    host,
                                    __temporal_workflow_interceptor_constructors(),
                                )
                            }
                        )*
                        _ => Err(::std::boxed::Box::new(
                            ::temporalio_workflow::common::protos::temporal::api::failure::v1::Failure {
                                message: ::std::format!(
                                    "No workflow named '{}' exported by this component",
                                    workflow_type
                                ),
                                ..::std::default::Default::default()
                            },
                        )),
                    }
                }
            }

            type __TemporalWorkflowComponentExport =
                ::temporalio_workflow::component::ExportedComponent<__TemporalWorkflowModule>;

            ::temporalio_workflow::__temporalio_export_workflow_component!(
                __TemporalWorkflowComponentExport
            );
        };
    };
}
