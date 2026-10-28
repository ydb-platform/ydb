#![allow(unreachable_pub)]
use temporalio_common::search_attributes::SearchAttributeKey;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

pub const KEYWORD_FIELD: SearchAttributeKey<String> =
    SearchAttributeKey::keyword("CustomKeywordField");
pub const INT_FIELD: SearchAttributeKey<i64> = SearchAttributeKey::int("CustomIntField");

#[workflow]
#[derive(Default)]
pub struct SearchAttributesWorkflow;

#[workflow_methods]
impl SearchAttributesWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<String> {
        let initial_keyword = ctx
            .search_attributes()
            .get(&KEYWORD_FIELD)
            .unwrap_or_default();

        ctx.upsert_search_attributes([
            KEYWORD_FIELD.value_set("updated-value".into()),
            INT_FIELD.value_set(42),
        ]);

        Ok(format!(
            "initial_keyword={initial_keyword}, upserted CustomIntField=42"
        ))
    }
}
