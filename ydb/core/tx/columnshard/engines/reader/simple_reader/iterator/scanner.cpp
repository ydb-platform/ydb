#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& table, const ui32 startIndex,
    const ui32 recordsCount, TPlainReadData& reader) {
    source->MutableResultRecordsCount() += table ? table->num_rows() : 0;
    source->MutableStageResult().SetResultChunk(std::move(table), startIndex, recordsCount);
    if ((!table || !table->num_rows()) && Context->GetCommonContext()->GetReadMetadata()->Limit && InFlightLimit < MaxInFlight) {
        InFlightLimit = 2 * InFlightLimit;
    }
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
            auto cursor =
                std::make_shared<TSimpleScanCursor>(frontSource->GetStartPKRecordBatch(), frontSource->GetSourceId(), startIndex + recordsCount);
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(frontSource->GetResourceGuards(), frontSource->GetGroupGuard(), table,
                cursor, Context->GetCommonContext(), sourceIdxToContinue));
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
        if (Context->GetCommonContext()->GetReadMetadata()->Limit && FetchingSources.size() && frontSource->GetResultRecordsCount()) {
            FinishedSources.emplace(frontSource);
            while (FinishedSources.size() && (*FinishedSources.begin())->GetFinish() < FetchingSources.front()->GetStart()) {
                auto fetchingSource = FetchingSources.front();
                auto finishedSource = *FinishedSources.begin();
                FetchedCount += finishedSource->GetResultRecordsCount();
                FinishedSources.erase(FinishedSources.begin());
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "source_finished")("source_id", finishedSource->GetSourceId())(
                    "source_idx", finishedSource->GetSourceIdx())("limit", Context->GetCommonContext()->GetReadMetadata()->Limit)(
                    "fetched", finishedSource->GetResultRecordsCount());
                if (FetchedCount > Context->GetCommonContext()->GetReadMetadata()->Limit) {
                    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "limit_exhausted")(
                        "limit", Context->GetCommonContext()->GetReadMetadata()->Limit)("fetched", FetchedCount);
                    SortedSources.clear();
                }
            }
        }
    }
}

TConclusionStatus TScanHead::Start() {
    for (auto&& i : SortedSources) {
        i->InitFetchingPlan(Context->GetColumnsFetchingPlan(i));
    }
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context) {
    if (HasAppData()) {
        if (AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
            MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
        }
    }
    if (Context->GetReadMetadata()->Limit) {
        InFlightLimit = 1;
    } else {
        InFlightLimit = MaxInFlight;
    }
    bool started = !context->GetCommonContext()->GetScanCursor()->IsInitialized();
    for (auto&& i : sources) {
        if (!started) {
            bool usage = false;
            if (!context->GetCommonContext()->GetScanCursor()->CheckEntityIsBorder(i, usage)) {
                continue;
            }
            started = true;
            if (!usage) {
                continue;
            }
            i->SetIsStartedByCursor();
        }
        SortedSources.emplace_back(i);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    if (Context->IsAborted()) {
        return false;
    }
    bool changed = false;
    while (SortedSources.size() && FetchingSources.size() < InFlightLimit) {
        SortedSources.front()->StartProcessing(SortedSources.front());
        FetchingSources.emplace_back(SortedSources.front());
        FetchingSourcesByIdx.emplace(SortedSources.front()->GetSourceIdx(), SortedSources.front());
        SortedSources.pop_front();
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
    AFL_VERIFY(Context->IsAborted());
    for (auto&& i : FetchingSources) {
        i->Abort();
    }
    for (auto&& i : SortedSources) {
        i->Abort();
    }
    FetchingSources.clear();
    SortedSources.clear();
    Y_ABORT_UNLESS(IsFinished());
}

}   // namespace NKikimr::NOlap::NReader::NSimple
