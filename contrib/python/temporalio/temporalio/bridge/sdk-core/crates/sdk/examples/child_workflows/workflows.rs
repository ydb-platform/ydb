#![allow(unreachable_pub)]
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{ChildWorkflowOptions, WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
pub struct GreetingChildWorkflow;

#[workflow_methods]
impl GreetingChildWorkflow {
    #[run(name = "GreetingChild")]
    pub async fn run(_ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow]
#[derive(Default)]
pub struct ParentWorkflow;

#[workflow_methods]
impl ParentWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        names: Vec<String>,
    ) -> WorkflowResult<Vec<String>> {
        let mut results = Vec::new();

        for (i, name) in names.iter().enumerate() {
            let started = ctx
                .start_child_workflow(
                    GreetingChildWorkflow::run,
                    name.clone(),
                    ChildWorkflowOptions::workflow_id(format!("greeting-child-{i}")),
                )
                .await?;

            let greeting = started.result().await?;
            results.push(greeting);
        }

        Ok(results)
    }
}
