#include "iterator.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NSimple {

TColumnShardScanIterator::TColumnShardScanIterator(const std::shared_ptr<TReadContext>& context, const TReadMetadata::TConstPtr& readMetadata)
    : Context(context)
    , ReadMetadata(readMetadata)
    , ReadyResults(context->GetCounters()) {
    IndexedData = readMetadata->BuildReader(Context);
    Y_ABORT_UNLESS(Context->GetReadMetadata()->IsSorted());
}

TConclusion<std::shared_ptr<TPartialReadResult>> TColumnShardScanIterator::GetBatch() {
    FillReadyResults();
    return ReadyResults.pop_front();
}

void TColumnShardScanIterator::PrepareResults() {
    FillReadyResults();
}

TConclusion<bool> TColumnShardScanIterator::ReadNextInterval() {
    return IndexedData->ReadNextInterval();
}

void TColumnShardScanIterator::DoOnSentDataFromInterval(const ui32 intervalIdx) const {
    return IndexedData->OnSentDataFromInterval(intervalIdx);
}

void TColumnShardScanIterator::FillReadyResults() {
    auto ready = IndexedData->ExtractReadyResults(MaxRowsInBatch);
    const i64 limitLeft = Context->GetReadMetadata()->Limit == 0 ? INT64_MAX : Context->GetReadMetadata()->Limit;
    for (size_t i = 0; i < ready.size(); ++i) {
        auto& batch = ReadyResults.emplace_back(std::move(ready[i]));
        AFL_VERIFY(batch->GetResultBatch().num_rows() <= limitLeft);
        ItemsRead += batch->GetResultBatch().num_rows();
    }
}

TColumnShardScanIterator::~TColumnShardScanIterator() {
    if (!IndexedData->IsFinished()) {
        IndexedData->Abort("iterator destructor");
    }
    ReadMetadata->ReadStats->PrintToLog();
}

void TColumnShardScanIterator::Apply(const std::shared_ptr<IApplyAction>& task) {
    if (!IndexedData->IsFinished()) {
        Y_ABORT_UNLESS(task->Apply(*IndexedData));
    }
}

const TReadStats& TColumnShardScanIterator::GetStats() const {
    return *ReadMetadata->ReadStats;
}

}   // namespace NKikimr::NOlap::NReader::NSimple
