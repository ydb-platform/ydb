#include "contexts.h"
#include "fetcher.h"

#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>

namespace NKikimr::NOlap::NDataFetcher {

IFetchingStep::EStepResult IFetchingStep::Execute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const {
    if (fetchingContext->IsAborted()) {
        fetchingContext->OnError("aborted");
        return EStepResult::Error;
    }
    return DoExecute(fetchingContext);
}

TRequestInput::TRequestInput(const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<TVersionedIndex>& versions,
    const NBlobOperations::EConsumer consumer, const TString& externalTaskId)
    : Consumer(consumer)
    , ExternalTaskId(externalTaskId) {
    AFL_VERIFY(portions.size());
    ActualSchema = versions->GetLastSchema();
    for (auto&& i : portions) {
        Portions.emplace_back(std::make_shared<TFullPortionInfo>(i, versions->GetSchemaVerified(i->GetSchemaVersionVerified())));
    }
}

}   // namespace NKikimr::NOlap::NDataFetcher
