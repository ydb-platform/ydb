#include "iterator.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NCommon {

TColumnShardScanIterator::TColumnShardScanIterator(const std::shared_ptr<TReadContext>& context)
    : Context(context)
    , ReadMetadata(context->GetReadMetadataPtrVerifiedAs<TReadMetadata>())
    , ReadyResults(context->GetCounters()) {
    IndexedData = ReadMetadata->BuildReader(Context);
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

}   // namespace NKikimr::NOlap::NReader::NCommon
