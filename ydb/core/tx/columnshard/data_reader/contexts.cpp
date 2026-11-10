#include "contexts.h"
#include "fetcher.h"

#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>

namespace NKikimr::NOlap::NDataFetcher {

IFetchingStep::EStepResult IFetchingStep::Execute(const std::shared_ptr<TPortionsDataFetcher>& fetchingContext) const {
    if (fetchingContext->IsAborted()) {
        fetchingContext->FinishStatsOperation(NScanStats::TStep::cancelled);
        fetchingContext->OnError("aborted");
        return EStepResult::Error;
    }
    const auto& capture = fetchingContext->GetStatsCapture();
    auto operation = fetchingContext->GetStatsOperation();
    if (!operation && capture.Collector) {
        operation = capture.Collector->Start(GetStatsKind(),
            TString(NScanStats::TStep::EStepKind_Name(GetStatsKind())), "Fetch portion inputs", NScanStats::Current());
        if (operation && fetchingContext->GetInput().GetConsumer() == NBlobOperations::EConsumer::DUPLICATE_FILTERING) {
            operation->Update([](auto& record) {
                record.SetMetaStep("PKFilter");
                record.SetCanonicalPath("Filter/Merge");
            });
        }
        fetchingContext->SetStatsOperation(operation, this);
    }
    NScanStats::TScope statsScope(operation, capture.Collector);
    const auto result = DoExecute(fetchingContext);
    if (operation) {
        if (result == EStepResult::Detached) {
            operation->Suspend();
        } else {
            fetchingContext->FinishStatsOperation(result == EStepResult::Error
                ? NScanStats::TStep::failed : NScanStats::TStep::completed);
        }
    }
    return result;
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
