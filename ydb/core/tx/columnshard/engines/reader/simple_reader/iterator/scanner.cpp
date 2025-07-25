#include "plain_read_data.h"
#include "scanner.h"

#include "collections/full_scan_sorted.h"
#include "collections/limit_sorted.h"
#include "collections/not_sorted.h"
#include "sync_points/aggr.h"
#include "sync_points/limit.h"
#include "sync_points/result.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

TConclusionStatus TScanHead::Start() {
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::unique_ptr<NCommon::ISourcesConstructor>&& sourcesConstructor, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context) {
    if (auto script = Context->GetSourcesAggregationScript()) {
        SourcesCollection =
            std::make_shared<TNotSortedCollection>(Context, std::move(sourcesConstructor), Context->GetReadMetadata()->GetLimitRobustOptional());
        SyncPoints.emplace_back(std::make_shared<TSyncPointResult>(SyncPoints.size(), context, SourcesCollection));
        SyncPoints.emplace_back(std::make_shared<TSyncPointResultsAggregationControl>(
            SourcesCollection, Context->GetSourcesAggregationScript(), SyncPoints.size(), context));
    } else if (Context->GetReadMetadata()->IsSorted()) {
        if (Context->GetReadMetadata()->HasLimit()) {
            auto collection = std::make_shared<TScanWithLimitCollection>(Context, std::move(sourcesConstructor));
            SourcesCollection = collection;
            SyncPoints.emplace_back(std::make_shared<TSyncPointLimitControl>(
                (ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust(), SyncPoints.size(), context, collection));
        } else {
            SourcesCollection = std::make_shared<TSortedFullScanCollection>(Context, std::move(sourcesConstructor));
        }
        SyncPoints.emplace_back(std::make_shared<TSyncPointResult>(SyncPoints.size(), context, SourcesCollection));
    } else {
        SourcesCollection =
            std::make_shared<TNotSortedCollection>(Context, std::move(sourcesConstructor), Context->GetReadMetadata()->GetLimitRobustOptional());
        SyncPoints.emplace_back(std::make_shared<TSyncPointResult>(SyncPoints.size(), context, SourcesCollection));
    }
    for (ui32 i = 0; i + 1 < SyncPoints.size(); ++i) {
        SyncPoints[i]->SetNext(SyncPoints[i + 1]);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build()("tablet_id", SourcesCollection->GetTabletId());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "build_next_interval");
    bool changed = false;
    while (SourcesCollection->HasData() && SourcesCollection->CheckInFlightLimits()) {
        auto source = SourcesCollection->TryExtractNext();
        if (!source) {
            return changed;
        }
        SyncPoints.front()->AddSource(std::move(source));
        changed = true;
    }
    return changed;
}

const TReadContext& TScanHead::GetContext() const {
    return *Context->GetCommonContext();
}

bool TScanHead::IsReverse() const {
    return GetContext().GetReadMetadata()->IsDescSorted();
}

void TScanHead::Abort() {
    AFL_VERIFY(!Context->IsActive());
    for (auto&& i : SyncPoints) {
        i->Abort();
    }
    SourcesCollection->Abort();
    Y_ABORT_UNLESS(IsFinished());
}

TScanHead::~TScanHead() {
    AFL_VERIFY(IsFinished() || !Context->IsActive());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
