#include "plain_read_data.h"
#include "scanner.h"

#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NReader::NSimple {

void TScanHead::OnSourceReady(const std::shared_ptr<IDataSource>& source, std::shared_ptr<arrow::Table>&& table, const ui32 startIndex,
    const ui32 recordsCount, TPlainReadData& reader) {
    source->MutableStageResult().SetResultChunk(std::move(table), startIndex, recordsCount);
    while (FetchingSources.size()) {
        auto& frontSource = *FetchingSources.begin();
        if (!frontSource->HasStageResult()) {
            break;
        }
        if (!frontSource->GetStageResult().HasResultChunk()) {
            break;
        }
        auto table = (*FetchingSources.begin())->MutableStageResult().ExtractResultChunk();
        auto cursor = std::make_shared<TSimpleScanCursor>(frontSource->GetStart(), frontSource->GetSourceId(), startIndex + recordsCount);
        reader.OnIntervalResult(std::make_shared<TPartialReadResult>(nullptr, nullptr, table, cursor, source->GetSourceIdx()));
        if ((*FetchingSources.begin())->GetStageResult().IsFinished()) {
            FetchingSources.erase(FetchingSources.begin());
        } else {
            break;
        }
    }
}

TConclusionStatus TScanHead::Start() {
    for (auto&& i : FetchingSources) {
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
    for (auto&& i : sources) {
        if (!context->GetCommonContext()->GetScanCursor()->CheckPortionUsage(i)) {
            continue;
        }
        SortedSources.emplace(i);
    }
}

TConclusion<bool> TScanHead::BuildNextInterval() {
    if (Context->IsAborted()) {
        return false;
    }
    while (SortedSources.size() && FetchingSources.size() < InFlightLimit) {
        (*SortedSources.begin())->StartProcessing(*SortedSources.begin());
        FetchingSources.emplace(*SortedSources.begin());
        SortedSources.erase(SortedSources.begin());
        return true;
    }
    return false;
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
