#include "constructor.h"
#include "filter_assembler.h"
#include "column_assembler.h"
#include "committed_assembler.h"
#include <ydb/core/tx/columnshard/engines/reader/read_context.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/conveyor/usage/events.h>
#include <ydb/core/tx/conveyor/usage/service.h>

namespace NKikimr::NOlap::NPlainReader {

TPortionInfo::TPreparedBatchData TAssembleColumnsTaskConstructor::BuildBatchAssembler() {
    auto blobs = ExtractBlobsData();
    THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo> blobsDataAssemble;
    for (auto&& i : blobs) {
        blobsDataAssemble.emplace(i.first, i.second);
    }
    for (auto&& i : NullBlocks) {
        AFL_VERIFY(blobsDataAssemble.emplace(i.first, i.second).second);
    }
    auto blobSchema = ReadMetadata->GetLoadSchema(PortionInfo->GetMinSnapshot());
    auto readSchema = ReadMetadata->GetLoadSchema(ReadMetadata->GetSnapshot());
    ISnapshotSchema::TPtr resultSchema;
    if (ColumnIds.size()) {
        resultSchema = std::make_shared<TFilteredSnapshotSchema>(readSchema, ColumnIds);
    } else {
        resultSchema = readSchema;
    }

    return PortionInfo->PrepareForAssemble(*blobSchema, *resultSchema, blobs);
}

void TEFTaskConstructor::DoOnDataReady() {
    NConveyor::TScanServiceOperator::SendTaskToExecute(std::make_shared<TAssembleFilter>(ScanActorId, BuildBatchAssembler(),
        ReadMetadata, SourceIdx, ColumnIds, UseEarlyFilter));
}

void TFFColumnsTaskConstructor::DoOnDataReady() {
    NConveyor::TScanServiceOperator::SendTaskToExecute(std::make_shared<TAssembleFFBatch>(ScanActorId, BuildBatchAssembler(),
        SourceIdx, AppliedFilter));
}

void TCommittedColumnsTaskConstructor::DoOnDataReady() {
    auto blobs = ExtractBlobsData();
    Y_VERIFY(NullBlocks.size() == 0);
    Y_VERIFY(blobs.size() == 1);
    NConveyor::TScanServiceOperator::SendTaskToExecute(std::make_shared<TCommittedAssembler>(ScanActorId, blobs.begin()->second,
        ReadMetadata, SourceIdx, CommittedBlob));
}

bool IFetchTaskConstructor::DoOnError(const TBlobRange& range) {
    NActors::TActorContext::AsActorContext().Send(ScanActorId, std::make_unique<NConveyor::TEvExecution::TEvTaskProcessedResult>(TConclusionStatus::Fail("cannot read blob range " + range.ToString())));
    return false;
}

}
