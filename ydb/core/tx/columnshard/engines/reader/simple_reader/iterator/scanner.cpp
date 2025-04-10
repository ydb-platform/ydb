#include "plain_read_data.h"
#include "scanner.h"

#include "collections/full_scan_sorted.h"
#include "collections/limit_sorted.h"
#include "collections/not_sorted.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceCheckLimit(const std::shared_ptr<IDataSource>& source) {
    SourcesCollection->OnSourceCheckLimit(source);
    if (SourcesCollection->IsFinished() && !SourcesCollection->HasWaitingSources()) {
        FetchingSources.clear();
    }
}

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& sourceInput, TPlainReadData& reader) {
    AFL_VERIFY(sourceInput->IsSyncSection());
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, sourceInput->AddEvent("f"));
    AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", FetchingSourcesByIdx.size())(
        "source_id", sourceInput->GetSourceId());
    if (FetchingSources.size()) {
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", sourceInput->GetEventsReport())("count", FetchingSourcesByIdx.size())(
            "source_id", sourceInput->GetSourceId())("first_source_id", FetchingSources.front()->GetSourceId());
    }
    while (FetchingSources.size() && FetchingSources.front()->IsSyncSection() && FetchingSources.front()->HasStageResult() &&
           SourcesCollection->IsSourceReadyForResult(FetchingSources.front())) {
        auto source = FetchingSources.front();
        AFL_WARN(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event", "ready_source")("source_id", source->GetSourceId());
        auto resultChunk = source->MutableStageResult().ExtractResultChunk(false);
        const bool isFinished = source->GetStageResult().IsFinished();
        std::optional<ui32> sourceIdxToContinue;
        if (!isFinished) {
            sourceIdxToContinue = source->GetSourceIdx();
        }
        if (resultChunk && resultChunk->HasData()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "has_result")("source_id", source->GetSourceId())(
                "source_idx", source->GetSourceIdx())("table", resultChunk->GetTable()->num_rows());
            auto cursor = SourcesCollection->BuildCursor(source, resultChunk->GetStartIndex() + resultChunk->GetRecordsCount());
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(source->GetResourceGuards(), source->GetGroupGuard(),
                resultChunk->GetTable(), cursor, Context->GetCommonContext(), sourceIdxToContinue));
        } else if (sourceIdxToContinue) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "continue_source")("source_id", source->GetSourceId())(
                "source_idx", source->GetSourceIdx());
            ContinueSource(*sourceIdxToContinue);
            break;
        }
        if (!isFinished) {
            break;
        }
        AFL_VERIFY(FetchingSourcesByIdx.erase(source->GetSourceIdx()));
        source->ClearResult();
        SourcesCollection->OnSourceFinished(source);
        FetchingSources.pop_front();
        if (SourcesCollection->IsFinished() && !SourcesCollection->HasWaitingSources()) {
            FetchingSources.clear();
        }
    }
}

TConclusionStatus TScanHead::Start() {
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::deque<TSourceConstructor>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context) {
    if (Context->GetReadMetadata()->IsSorted()) {
        if (Context->GetReadMetadata()->HasLimit()) {
            SourcesCollection =
                std::make_unique<TScanWithLimitCollection>(Context, std::move(sources), context->GetCommonContext()->GetScanCursor());
        } else {
            SourcesCollection =
                std::make_unique<TSortedFullScanCollection>(Context, std::move(sources), context->GetCommonContext()->GetScanCursor());
        }
    } else {
        SourcesCollection = std::make_unique<TNotSortedCollection>(
            Context, std::move(sources), context->GetCommonContext()->GetScanCursor(), Context->GetReadMetadata()->GetLimitRobustOptional());
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    bool changed = false;
    while (SourcesCollection->HasData() && SourcesCollection->CheckInFlightLimits()) {
        auto source = SourcesCollection->ExtractNext();
        source->InitFetchingPlan(Context->GetColumnsFetchingPlan(source));
        source->StartProcessing(source);
        FetchingSources.emplace_back(source);
        AFL_VERIFY(FetchingSourcesByIdx.emplace(source->GetSourceIdx(), source).second);
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
    for (auto&& i : FetchingSources) {
        i->Abort();
    }
    FetchingSources.clear();
    SourcesCollection->Abort();
    Y_ABORT_UNLESS(IsFinished());
}

TScanHead::~TScanHead() {
    AFL_VERIFY((FetchingSources.empty() && SourcesCollection->IsFinished()) || !Context->IsActive());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
