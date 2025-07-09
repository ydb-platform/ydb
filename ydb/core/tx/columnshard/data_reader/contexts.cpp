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

TRequestInput::TRequestInput(const std::vector<TPortionInfo::TConstPtr>& portions, const std::shared_ptr<const TVersionedIndex>& versions,
    const NBlobOperations::EConsumer consumer, const TString& externalTaskId,
    const std::optional<TFetcherMemoryProcessInfo>& memoryProcessInfo)
    : Consumer(consumer)
    , ExternalTaskId(externalTaskId)
    , MemoryProcessInfo(memoryProcessInfo ? *memoryProcessInfo : TFetcherMemoryProcessInfo())
{
    AFL_VERIFY(portions.size());
    ActualSchema = versions->GetLastSchema();
    for (auto&& i : portions) {
        Portions.emplace_back(std::make_shared<TFullPortionInfo>(i, versions->GetSchemaVerified(i->GetSchemaVersionVerified())));
    }
}

}   // namespace NKikimr::NOlap::NDataFetcher
