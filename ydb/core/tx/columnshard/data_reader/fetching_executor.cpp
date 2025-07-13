#include "fetching_executor.h"
#include "fetcher.h"

namespace NKikimr::NOlap::NDataFetcher {

void TFetchingExecutor::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    NActors::TLogContextGuard lGuard =
        NActors::TLogContextBuilder::Build()("event", "on_execution")("consumer", Fetcher->GetInput().GetConsumer())(
            "task_id", Fetcher->GetInput().GetExternalTaskId())("script", Fetcher->MutableScript().GetScriptClassName());
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
