#include "fetcher.h"
#include "fetching_executor.h"

#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NDataFetcher {

void TFetchingExecutor::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    TMemoryProfileGuard mpg("CS::FETCHER::" + Fetcher->MutableScript().GetScriptClassName());
    YDB_LOG_CREATE_CONTEXT(
        {"event", "on_execution"},
        {"input", Fetcher->GetInput().DebugString()},
        {"consumer", Fetcher->GetInput().GetConsumer()},
        {"taskId", Fetcher->GetInput().GetExternalTaskId()},
        {"script", Fetcher->MutableScript().GetScriptClassName()});
    while (!Fetcher->MutableScript().IsFinished()) {
        switch (Fetcher->MutableScript().GetCurrentStep()->Execute(Fetcher)) {
            case IFetchingStep::EStepResult::Continue:
                Fetcher->MutableScript().Next();
                break;
            case IFetchingStep::EStepResult::Detached:
                return;
            case IFetchingStep::EStepResult::Error:
                return;
        }
    }
    Fetcher->OnFinished();
}

TString TFetchingExecutor::GetTaskClassIdentifier() const {
    return Fetcher->MutableScript().GetScriptClassName();
}

}   // namespace NKikimr::NOlap::NDataFetcher
