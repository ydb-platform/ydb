#include "operations.h"

#include <arrow/type.h>
#include <util/system/yassert.h>

namespace NKikimr::NOlap::NStats::NScan {

NColumnShardStats::TOperation MakeStepStatisticsDefinition(NColumnShardStats::TOperation::DetailsCase kind, TStringBuf key) {
    NColumnShardStats::TOperation result;
    const auto* field = result.GetDescriptor()->FindFieldByNumber(kind);
    Y_ABORT_UNLESS(field && field->containing_oneof(), "invalid operation details");
    result.SetKey(TString(key));
    result.SetName(field->name());
    result.GetReflection()->MutableMessage(&result, field);
    return result;
}

NColumnShardStats::TOperation MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::DetailsCase kind) {
    TStringBuf key;
    switch (kind) {
        case NColumnShardStats::TOperation::kPKFilter:
            key = "PKFilter";
            break;
        case NColumnShardStats::TOperation::kIndexFilter:
            key = "IndexFilter";
            break;
        case NColumnShardStats::TOperation::kFilter:
            key = "Filter";
            break;
        case NColumnShardStats::TOperation::kFetchColumns:
            key = "FetchColumns";
            break;
        case NColumnShardStats::TOperation::kAggregation:
            key = "Aggregation";
            break;
        case NColumnShardStats::TOperation::kProjection:
            key = "Projection";
            break;
        case NColumnShardStats::TOperation::kBuildResult:
            key = "BuildResult";
            break;
        case NColumnShardStats::TOperation::kOutput:
            key = "Output";
            break;
        default:
            Y_ABORT("unsupported source metastep");
    }
    return MakeStepStatisticsDefinition(kind, key);
}

namespace {

NColumnShardStats::TOperation Operation(const TString& key, const TString& name) {
    NColumnShardStats::TOperation operation;
    operation.SetKey(key);
    operation.SetName(name);
    return operation;
}

}   // namespace

NColumnShardStats::TOperation MakePreparation(const TString& tablePath) {
    auto operation = Operation("Preparation", "Preparation");
    operation.MutablePreparation()->SetTablePath(tablePath);
    return operation;
}

NColumnShardStats::TOperation MakeReader() {
    auto operation = Operation("Reader", "Reader");
    operation.MutableReader();
    return operation;
}

NColumnShardStats::TOperation MakePrepareRead() {
    auto operation = Operation("PrepareRead", "PrepareRead");
    operation.MutablePrepareRead();
    return operation;
}

NColumnShardStats::TOperation MakePrepareReadMetadata() {
    auto operation = Operation("PrepareReadMetadata", "PrepareReadMetadata");
    operation.MutablePrepareReadMetadata();
    return operation;
}

NColumnShardStats::TOperation MakeReaderInit() {
    auto operation = Operation("ReaderInit", "ReaderInit");
    operation.MutableReaderInit();
    return operation;
}

NColumnShardStats::TOperation MakeSelectPortions() {
    auto operation = Operation("SelectPortions", "SelectPortions");
    operation.MutableSelectPortions();
    return operation;
}

NColumnShardStats::TOperation MakeParseProgram() {
    auto operation = Operation("ParseProgram", "ParseProgram");
    operation.MutableParseProgram();
    return operation;
}

NColumnShardStats::TOperation MakeOutput() {
    auto operation = Operation("Output", "Output");
    operation.MutableOutput()->MutableIntervalLimitHits()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeSend() {
    auto operation = Operation("Send", "Send");
    operation.MutableSend()->MutableBatches()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeAck() {
    auto operation = Operation("Ack", "Ack");
    operation.MutableAck()->MutableGrantedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableAck()->MutableGrantedBatches()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeStorageRead() {
    auto operation = Operation("StorageRead", "StorageRead");
    operation.MutableStorageRead();
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeReadySources() {
    auto operation = Operation("Ready.sources", "Ready");
    operation.MutableReady()->MutableSources()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableReady()->MutableIntervals()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeReadyIntervals() {
    auto operation = Operation("Ready.intervals", "Ready");
    operation.MutableReady()->MutableIntervals()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableReady()->MutableSources()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeConflictDetection() {
    auto operation = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kConflictDetection, "ConflictDetection");
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeSourceOutput() {
    auto operation = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kOutput, "Output");
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeSourcesAggregation() {
    auto operation = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kAggregation, "Sources");
    operation.MutableAggregation()->SetKind(NColumnShardStats::SOURCES_AGGREGATION);
    operation.MutableAggregation()->MutableSources()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeSourceResult() {
    auto operation = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kBuildResult, "SourcePage");
    operation.MutableBuildResult()->MutableBatches()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakePagePlanning() {
    auto operation = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kPrepareResult, "PagePlanning");
    operation.MutablePrepareResult()->SetAction(NColumnShardStats::PREPARE_RESULT);
    operation.MutablePrepareResult()->MutablePages()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutablePrepareResult()->MutableSkippedPages()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeDuplicateFilter() {
    auto operation = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kFilter, "DUPLICATES");
    operation.MutableFilter()->SetKind(NColumnShardStats::DUPLICATES);
    operation.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    operation.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
    return operation;
}

NColumnShardStats::TOperation MakeDuplicateMerge(const arrow::Schema& schema, bool hasIntervals) {
    auto definition = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kMerge, "Merge.DuplicateMasks");
    definition.MutableMerge()->SetPurpose(NColumnShardStats::DUPLICATE_MASKS);
    for (const auto& field : schema.fields()) {
        definition.MutableMerge()->AddKeyColumns(field->name().c_str());
    }
    definition.MutableMerge()->MutableSources()->SetApplicability(NColumnShardStats::APPLICABLE);
    definition.MutableMerge()->MutableMasks()->SetApplicability(NColumnShardStats::APPLICABLE);
    definition.MutableMerge()->MutableIntervals()->SetApplicability(
        hasIntervals ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
    definition.MutableMerge()->MutableBorders()->SetApplicability(
        hasIntervals ? NColumnShardStats::NOT_APPLICABLE : NColumnShardStats::APPLICABLE);
    definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
    return definition;
}

}   // namespace NKikimr::NOlap::NStats::NScan
