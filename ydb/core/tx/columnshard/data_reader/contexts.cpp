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
    const std::shared_ptr<NGroupedMemoryManager::TProcessGuard>& memoryProcessGuard, const ui64 tabletId)
    : Consumer(consumer)
    , ExternalTaskId(externalTaskId)
    , TabletId(tabletId)
    , MemoryProcessGuard(memoryProcessGuard)
{
    AFL_VERIFY(portions.size());
    ActualSchema = versions->GetLastSchema();
    for (auto&& i : portions) {
        Portions.emplace_back(std::make_shared<TFullPortionInfo>(i, versions->GetSchemaVerified(i->GetSchemaVersionVerified())));
    }
}

TString TRequestInput::DebugString() const {
    TStringBuilder sb;
    sb << "tablet_id=" << TabletId << ";";
    sb << "portions(path_id:portion_id)=";
    ui32 count = 0;
    for (auto&& p : Portions) {
        if (count > 0) {
            sb << ",";
        }
        sb << p->GetPortionInfo()->GetPathId().GetRawValue() << ":" << p->GetPortionInfo()->GetPortionId();
        if (++count >= 5) {
            sb << " ...";
            break;
        }
    }
    sb << ";portions_count=" << Portions.size() << ";";
    return sb;
}

}   // namespace NKikimr::NOlap::NDataFetcher
