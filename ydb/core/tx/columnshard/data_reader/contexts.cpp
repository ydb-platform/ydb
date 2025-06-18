#include "contexts.h"
#include "fetcher.h"

namespace NKikimr::NOlap::NDataFetcher {

IFetchingStep::EStepResult IFetchingStep::Execute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const {
    if (fetchingContext->IsAborted()) {
        fetchingContext->OnError("aborted");
        return EStepResult::Error;
    }
    return DoExecute(fetchingContext);
}

}   // namespace NKikimr::NOlap::NDataFetcher
