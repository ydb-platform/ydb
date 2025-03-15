#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& tableExt, const ui32 startIndex,
    const ui32 recordsCount, TPlainReadData& reader) {
    source->MutableResultRecordsCount() += tableExt ? tableExt->num_rows() : 0;
    if (!tableExt || !tableExt->num_rows()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("empty_source", source->DebugJson().GetStringRobust());
    }
    Context->GetCommonContext()->GetCounters().OnSourceFinished(
        source->GetRecordsCount(), source->GetUsedRawBytes(), tableExt ? tableExt->num_rows() : 0);

    source->MutableStageResult().SetResultChunk(std::move(tableExt), startIndex, recordsCount);
    if (source->GetStageResult().IsFinished()) {
        SourcesInFlightCount.Dec();
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
        if (Context->GetCommonContext()->GetReadMetadata()->HasLimit()) {
            AFL_VERIFY(FetchingInFlightSources.erase(TCompareKeyForScanSequence::FromFinish(frontSource)));
            AFL_VERIFY(FinishedSources.emplace(TCompareKeyForScanSequence::FromFinish(frontSource), frontSource).second);
            while (FinishedSources.size() &&
                   (SortedSources.empty() || FinishedSources.begin()->second->GetFinish() < SortedSources.front()->GetStart())) {
                auto finishedSource = FinishedSources.begin()->second;
                if (!finishedSource->GetResultRecordsCount() && InFlightLimit < MaxInFlight) {
                    InFlightLimit = 2 * InFlightLimit;
                }
                FetchedCount += finishedSource->GetResultRecordsCount();
                FinishedSources.erase(FinishedSources.begin());
                if (Context->IsActive()) {
                    --IntervalsInFlightCount;
                }
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "source_finished")("source_id", finishedSource->GetSourceId())(
                    "source_idx", finishedSource->GetSourceIdx())("limit", Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())(
                    "fetched", finishedSource->GetResultRecordsCount());
                if (FetchedCount > (ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust() && SortedSources.size()) {
                    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("event", "limit_exhausted")(
                        "limit", Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())("fetched", FetchedCount);
                    SortedSources.clear();
                    IntervalsInFlightCount = GetInFlightIntervalsCount();
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
    if (HasAppData() && AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
        MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
    }
    if (Context->GetReadMetadata()->HasLimit()) {
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
    if (!Context->IsActive()) {
        return false;
    }
    if (SortedSources.size() == 0) {
        return false;
    }
    bool changed = false;
    if (!Context->GetCommonContext()->GetReadMetadata()->HasLimit()) {
        while (SortedSources.size() && SourcesInFlightCount.Val() < InFlightLimit && Context->IsActive()) {
            SortedSources.front()->StartProcessing(SortedSources.front());
            FetchingSources.emplace_back(SortedSources.front());
            SourcesInFlightCount.Inc();
            AFL_VERIFY(FetchingSourcesByIdx.emplace(SortedSources.front()->GetSourceIdx(), SortedSources.front()).second);
            SortedSources.pop_front();
            changed = true;
        }
    } else {
        if (InFlightLimit <= IntervalsInFlightCount) {
            return false;
        }
        ui32 inFlightCountLocal = GetInFlightIntervalsCount();
        AFL_VERIFY(IntervalsInFlightCount == inFlightCountLocal)("count_global", IntervalsInFlightCount)("count_local", inFlightCountLocal);
        while (SortedSources.size() && inFlightCountLocal < InFlightLimit && Context->IsActive()) {
            SortedSources.front()->StartProcessing(SortedSources.front());
            FetchingSources.emplace_back(SortedSources.front());
            SourcesInFlightCount.Inc();
            AFL_VERIFY(FetchingSourcesByIdx.emplace(SortedSources.front()->GetSourceIdx(), SortedSources.front()).second);
            AFL_VERIFY(FetchingInFlightSources.emplace(TCompareKeyForScanSequence::FromFinish(SortedSources.front()), SortedSources.front()).second);
            SortedSources.pop_front();
            const ui32 inFlightCountLocalNew = GetInFlightIntervalsCount();
            AFL_VERIFY(inFlightCountLocal <= inFlightCountLocalNew);
            inFlightCountLocal = inFlightCountLocalNew;
            changed = true;
        }
        IntervalsInFlightCount = inFlightCountLocal;
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
    for (auto&& i : SortedSources) {
        i->Abort();
    }
    FetchingSources.clear();
    SortedSources.clear();
    Y_ABORT_UNLESS(IsFinished());
}

TScanHead::~TScanHead() {
    AFL_VERIFY(!IntervalsInFlightCount || !Context->IsActive());
}

ui32 TScanHead::GetInFlightIntervalsCount() const {
    if (SortedSources.empty()) {
        return FetchingInFlightSources.size() + FinishedSources.size();
    }
    ui32 inFlightCountLocal = 0;
    auto itUpperFinished = FinishedSources.upper_bound(TCompareKeyForScanSequence::BorderStart(SortedSources.front()));
    for (auto&& it = FinishedSources.begin(); it != itUpperFinished; ++it) {
        ++inFlightCountLocal;
    }
    auto itUpperFetching = FetchingInFlightSources.upper_bound(TCompareKeyForScanSequence::BorderStart(SortedSources.front()));
    for (auto&& it = FetchingInFlightSources.begin(); it != itUpperFetching; ++it) {
        ++inFlightCountLocal;
    }
    return inFlightCountLocal;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
