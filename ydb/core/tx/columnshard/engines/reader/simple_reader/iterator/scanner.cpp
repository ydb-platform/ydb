#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/result.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

std::vector<std::shared_ptr<IDataSource>> TSourceFetchingSchedulerImpl::ExtractSourcesWithResult() {
    std::vector<std::shared_ptr<IDataSource>> results;
    while (FetchingSources.size()) {
        auto frontSource = FetchingSources.front();
        if (!frontSource->HasStageResult()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_no_result")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx());
            break;
        }
        if (!frontSource->GetStageResult().HasResultChunk()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "skip_no_result_chunk")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx());
            break;
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "has_result")("source_id", frontSource->GetSourceId())(
            "source_idx", frontSource->GetSourceIdx());
        results.emplace_back(frontSource);
        if (!frontSource->GetStageResult().IsFinished()) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "continue_source")("source_id", frontSource->GetSourceId())(
                "source_idx", frontSource->GetSourceIdx());
            Y_UNUSED(ScheduleNextSource());
            break;
        }
        AFL_VERIFY(ActiveSources.erase(frontSource->GetSourceIdx()));
        FetchingSources.pop_front();
    }
    return results;
}

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& tableExt, const ui32 startIndex,
    const ui32 recordsCount, TPlainReadData& reader) {
    source->MutableResultRecordsCount() += tableExt ? tableExt->num_rows() : 0;
    if (!tableExt || !tableExt->num_rows()) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("empty_source", source->DebugJson().GetStringRobust());
    }
    Context->GetCommonContext()->GetCounters().OnSourceFinished(
        source->GetRecordsCount(), source->GetUsedRawBytes(), tableExt ? tableExt->num_rows() : 0);

    source->MutableStageResult().SetResultChunk(std::move(tableExt), startIndex, recordsCount);
    for (const std::shared_ptr<IDataSource>& source : Scheduler->ExtractSourcesWithResult()) {
        auto table = source->MutableStageResult().ExtractResultChunk();
        if (table && table->num_rows()) {
            auto cursor = std::make_shared<TSimpleScanCursor>(source->GetStartPKRecordBatch(), source->GetSourceId(), startIndex + recordsCount);
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(
                source->GetResourceGuards(), source->GetGroupGuard(), table, cursor, Context->GetCommonContext(), std::nullopt));
        }
        if (source->GetStageResult().IsFinished() && Context->GetCommonContext()->GetReadMetadata()->HasLimit()) {
            source->ClearResult();
            AFL_VERIFY(FetchingInFlightSources.erase(source));
            AFL_VERIFY(FinishedSources.emplace(source).second);
            std::shared_ptr<IDataSource> firstNotStarted = Scheduler->GetFirstNotStartedSourceOptional();
            while (FinishedSources.size() && (!firstNotStarted || (*FinishedSources.begin())->GetFinish() < firstNotStarted->GetStart())) {
                auto finishedSource = *FinishedSources.begin();
                const ui64 inFlightLimit = Scheduler->GetInFlightLimit();
                if (!finishedSource->GetResultRecordsCount() && inFlightLimit < MaxInFlight) {
                    Scheduler->SetInFlightLimit(Min(MaxInFlight, 2 * inFlightLimit));
                }
                FetchedCount += finishedSource->GetResultRecordsCount();
                FinishedSources.erase(FinishedSources.begin());
                if (Context->IsActive()) {
                    --IntervalsInFlightCount;
                }
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "source_finished")("source_id", finishedSource->GetSourceId())(
                    "source_idx", finishedSource->GetSourceIdx())("limit", Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())(
                    "fetched", finishedSource->GetResultRecordsCount());
                if (FetchedCount > (ui64)Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust()) {
                    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "limit_exhausted")(
                        "limit", Context->GetCommonContext()->GetReadMetadata()->GetLimitRobust())("fetched", FetchedCount);
                    Scheduler->EraseNotStartedSources();
                    IntervalsInFlightCount = GetInFlightIntervalsCount();
                }
            }
        }
    }
}

TConclusionStatus TScanHead::Start() {
    for (auto&& i : UninitializedSources) {
        i->InitFetchingPlan(Context->GetColumnsFetchingPlan(i));
        Scheduler->AppendSource(i);
    }
    UninitializedSources.clear();
    return TConclusionStatus::Success();
}

TScanHead::TScanHead(std::deque<std::shared_ptr<IDataSource>>&& sources, const std::shared_ptr<TSpecialReadContext>& context)
    : Context(context)
    , Scheduler(context->GetScheduler()) {
    if (HasAppData() && AppDataVerified().ColumnShardConfig.HasMaxInFlightIntervalsOnRequest()) {
        MaxInFlight = AppDataVerified().ColumnShardConfig.GetMaxInFlightIntervalsOnRequest();
    }
    if (Context->GetReadMetadata()->HasLimit()) {
        Scheduler->SetInFlightLimit(1);
    } else {
        Scheduler->SetInFlightLimit(MaxInFlight);
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
        UninitializedSources.emplace_back(i);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    if (!Context->IsActive()) {
        return false;
    }
    if (!Context->GetCommonContext()->GetReadMetadata()->HasLimit()) {
        return !!Scheduler->ScheduleNextSource();
    } else {
        if (Scheduler->GetInFlightLimit() <= IntervalsInFlightCount) {
            return false;
        }
        ui32 inFlightCountLocal = GetInFlightIntervalsCount();
        AFL_VERIFY(IntervalsInFlightCount == inFlightCountLocal)("count_global", IntervalsInFlightCount)("count_local", inFlightCountLocal);
        if (inFlightCountLocal >= Scheduler->GetInFlightLimit()) {
            return false;
        }
        auto scheduledSource = Scheduler->ScheduleNextSource();
        if (!scheduledSource) {
            return false;
        }
        AFL_VERIFY(FetchingInFlightSources.emplace(scheduledSource).second);
        const ui32 inFlightCountLocalNew = GetInFlightIntervalsCount();
        AFL_VERIFY(inFlightCountLocal <= inFlightCountLocalNew);
        IntervalsInFlightCount = inFlightCountLocalNew;
        return true;
    }
}

const TReadContext& TScanHead::GetContext() const {
    return *Context->GetCommonContext();
}

bool TScanHead::IsReverse() const {
    return GetContext().GetReadMetadata()->IsDescSorted();
}

void TScanHead::Abort() {
    AFL_VERIFY(!Context->IsActive());
    Scheduler->Abort();
    Y_ABORT_UNLESS(IsFinished());
}

TScanHead::~TScanHead() {
    AFL_VERIFY(!IntervalsInFlightCount || !Context->IsActive());
}

ui32 TScanHead::GetInFlightIntervalsCount() const {
    std::shared_ptr<IDataSource> firstNotStarted = Scheduler->GetFirstNotStartedSourceOptional();
    if (!firstNotStarted) {
        return FetchingInFlightSources.size() + FinishedSources.size();
    }
    ui32 inFlightCountLocal = 0;
    for (auto it = FinishedSources.begin(); it != FinishedSources.end(); ++it) {
        if ((*it)->GetFinish() < firstNotStarted->GetStart()) {
            ++inFlightCountLocal;
        } else {
            break;
        }
    }
    for (auto it = FetchingInFlightSources.begin(); it != FetchingInFlightSources.end(); ++it) {
        if ((*it)->GetFinish() < firstNotStarted->GetStart()) {
            ++inFlightCountLocal;
        } else {
            break;
        }
    }
    return inFlightCountLocal;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
