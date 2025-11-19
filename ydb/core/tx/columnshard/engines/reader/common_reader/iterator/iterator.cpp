#include "iterator.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/constructor/read_metadata.h>

namespace NKikimr::NOlap::NReader::NCommon {

TColumnShardScanIterator::TColumnShardScanIterator(const std::shared_ptr<TReadContext>& context)
    : Context(context)
    , ReadMetadata(context->GetReadMetadataPtrVerifiedAs<TReadMetadata>())
    , ReadyResults(context->GetCounters()) {
    IndexedData = ReadMetadata->BuildReader(Context);
}

TConclusion<std::unique_ptr<TPartialReadResult>> TColumnShardScanIterator::GetBatch() {
    FillReadyResults();
    return ReadyResults.pop_front();
}

void TColumnShardScanIterator::PrepareResults() {
    FillReadyResults();
}

TConclusion<bool> TColumnShardScanIterator::ReadNextInterval() {
    return IndexedData->ReadNextInterval();
}

void TColumnShardScanIterator::DoOnSentDataFromInterval(const TPartialSourceAddress& address) {
    return IndexedData->OnSentDataFromInterval(address);
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
