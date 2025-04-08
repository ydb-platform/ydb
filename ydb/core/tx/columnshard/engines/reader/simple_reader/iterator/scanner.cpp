#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& tableExt, const ui32 startIndex,
    const ui32 recordsCount, TPlainReadData& reader) {
    FOR_DEBUG_LOG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG, source->AddEvent("f"));
    AFL_DEBUG(NKikimrServices::COLUMNSHARD_SCAN_EVLOG)("event_log", source->GetEventsReport())("count", FetchingSources.size());
    source->MutableResultRecordsCount() += tableExt ? tableExt->num_rows() : 0;
    if (!tableExt || !tableExt->num_rows()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("empty_source", source->DebugJson().GetStringRobust());
    }
    Context->GetCommonContext()->GetCounters().OnSourceFinished(
        source->GetRecordsCount(), source->GetUsedRawBytes(), tableExt ? tableExt->num_rows() : 0);

    source->MutableStageResult().SetResultChunk(std::move(tableExt), startIndex, recordsCount);
    while (FetchingSources.size()) {
        auto frontSource = FetchingSources.front();
        if (!frontSource->HasStageResult()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_no_result")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx());
            break;
        }
        if (!frontSource->GetStageResult().HasResultChunk()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "skip_no_result_chunk")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx());
            break;
        }
        auto table = frontSource->MutableStageResult().ExtractResultChunk();
        const bool isFinished = frontSource->GetStageResult().IsFinished();
        std::optional<ui32> sourceIdxToContinue;
        if (!isFinished) {
            sourceIdxToContinue = frontSource->GetSourceIdx();
        }
        if (table && table->num_rows()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "has_result")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx())("table", table->num_rows());
            auto cursor = SourcesCollection->BuildCursor(frontSource, startIndex + recordsCount);
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(frontSource->GetResourceGuards(), frontSource->GetGroupGuard(), table,
                cursor, Context->GetCommonContext(), sourceIdxToContinue));
            SourcesCollection->OnIntervalResult(table, frontSource);
        } else if (sourceIdxToContinue) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "continue_source")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx());
            ContinueSource(*sourceIdxToContinue);
            break;
        }
        if (!isFinished) {
            break;
        }
        AFL_VERIFY(FetchingSourcesByIdx.erase(frontSource->GetSourceIdx()));
        FetchingSources.pop_front();
        frontSource->ClearResult();
        SourcesCollection->OnSourceFinished(frontSource);
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
    if (!Context->IsActive()) {
        return false;
    }
    bool changed = false;
    while (!SourcesCollection->IsFinished() && SourcesCollection->CheckInFlightLimits() && Context->IsActive()) {
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
    SourcesCollection->Clear();
    Y_ABORT_UNLESS(IsFinished());
}

TScanHead::~TScanHead() {
    AFL_VERIFY((FetchingSources.empty() && SourcesCollection->IsFinished()) || !Context->IsActive());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
