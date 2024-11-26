#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& table, const ui32 startIndex,
    const ui32 recordsCount, TPlainReadData& reader) {
    source->MutableStageResult().SetResultChunk(std::move(table), startIndex, recordsCount);
    if ((!table || !table->num_rows()) && Context->GetCommonContext()->GetReadMetadata()->Limit && InFlightLimit < MaxInFlight) {
        InFlightLimit = 2 * InFlightLimit;
    }
    while (FetchingSources.size()) {
        auto frontSource = *FetchingSources.begin();
        if (!frontSource->HasStageResult()) {
            break;
        }
        if (!frontSource->GetStageResult().HasResultChunk()) {
            break;
        }
        auto table = frontSource->MutableStageResult().ExtractResultChunk();
        const bool isFinished = frontSource->GetStageResult().IsFinished();
        std::optional<ui32> sourceIdxToContinue;
        if (!isFinished) {
            sourceIdxToContinue = frontSource->GetSourceIdx();
        }
        if (table && table->num_rows()) {
            auto cursor =
                std::make_shared<TSimpleScanCursor>(frontSource->GetStartPKRecordBatch(), frontSource->GetSourceId(), startIndex + recordsCount);
            reader.OnIntervalResult(std::make_shared<TPartialReadResult>(nullptr, nullptr, table, cursor, sourceIdxToContinue));
        } else if (sourceIdxToContinue) {
            ContinueSource(*sourceIdxToContinue);
            break;
        }
        if (!isFinished) {
            break;
        }
        AFL_VERIFY(FetchingSourcesByIdx.erase(frontSource->GetSourceIdx()));
        if (Context->GetCommonContext()->GetReadMetadata()->Limit) {
            FinishedSources.emplace(*FetchingSources.begin());
        }
        FetchingSources.erase(FetchingSources.begin());
        while (FetchingSources.size() && FinishedSources.size()) {
            auto finishedSource = *FinishedSources.begin();
            auto fetchingSource = *FetchingSources.begin();
            if (finishedSource->GetFinish() < fetchingSource->GetStart()) {
                FetchedCount += finishedSource->GetRecordsCount();
            }
            FinishedSources.erase(FinishedSources.begin());
            if (FetchedCount > Context->GetCommonContext()->GetReadMetadata()->Limit) {
                Context->Abort();
                Abort();
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
        SortedSources.emplace(i);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    if (Context->IsAborted()) {
        return false;
    }
    bool changed = false;
    while (SortedSources.size() && FetchingSources.size() < InFlightLimit) {
        (*SortedSources.begin())->StartProcessing(*SortedSources.begin());
        FetchingSources.emplace(*SortedSources.begin());
        FetchingSourcesByIdx.emplace((*SortedSources.begin())->GetSourceIdx(), *SortedSources.begin());
        SortedSources.erase(SortedSources.begin());
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
