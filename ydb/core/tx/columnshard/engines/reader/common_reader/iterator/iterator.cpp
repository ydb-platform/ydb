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

TString TColumnShardScanIterator::DebugString(const bool verbose) const {
    TStringBuilder perStepCounters;
    NKqp::TPerStepCounters summ;
    summ.StepName = "AllStepsSumm";

    for(auto& counters:  Context->GetCounters().GetCurrentScanCounters()){
        perStepCounters << "[" << counters.DebugString() << "]\n";
        summ.IntegralExecutionDuration += counters.IntegralExecutionDuration;
        summ.IntegralWaitDuration += counters.IntegralWaitDuration;
        summ.IntegralRawBytesRead += counters.IntegralRawBytesRead;
    }
    if (!perStepCounters.empty()) {
        perStepCounters.pop_back(); // last '\n'
    }
    
    
    return TStringBuilder()
        << "ready_results:(" << ReadyResults.DebugString() << ");"
        << "indexed_data:(" << IndexedData->DebugString(verbose) << ");"
        << "per_step_counters:(" << perStepCounters << ");"
        << "counters_summ_across_all_steps:(" << summ.DebugString() << ")"
        ;
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
