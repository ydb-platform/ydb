#include "fetcher.h"
#include "fetching_executor.h"

#include <util/generic/scope.h>

#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr::NOlap::NDataFetcher {

void TFetchingExecutor::DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) {
    const auto& capture = Fetcher->GetStatsCapture();
    auto statsOperation = capture.Collector ? capture.Collector->Start(NScanStats::TStep::Reader,
        "DATA_FETCHER_TASK", "Run portion fetch continuation", capture.Operation) : nullptr;
    NScanStats::TScope statsScope(statsOperation, capture.Collector);
    Y_DEFER { if (statsOperation) { statsOperation->Finish(); } };
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
                if (statsOperation) {
                    statsOperation->Finish(NScanStats::TStep::failed);
                }
                return;
        }
    }
    if (statsOperation) {
        statsOperation->Finish();
    }
    Fetcher->OnFinished();
}

TString TFetchingExecutor::GetTaskClassIdentifier() const {
    return Fetcher->MutableScript().GetScriptClassName();
}

}   // namespace NKikimr::NOlap::NDataFetcher
