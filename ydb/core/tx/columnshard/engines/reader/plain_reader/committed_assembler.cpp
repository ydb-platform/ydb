#include "committed_assembler.h"
#include "plain_read_data.h"

namespace NKikimr::NOlap::NPlainReader {

bool TCommittedAssembler::DoExecute() {
    ResultBatch = NArrow::DeserializeBatch(BlobData, ReadMetadata->GetBlobSchema(SchemaVersion));
    Y_ABORT_UNLESS(ResultBatch);
    ResultBatch = ReadMetadata->GetIndexInfo().AddSpecialColumns(ResultBatch, DataSnapshot);
    Y_ABORT_UNLESS(ResultBatch);
    ReadMetadata->GetPKRangesFilter().BuildFilter(ResultBatch).Apply(ResultBatch);
    EarlyFilter = ReadMetadata->GetProgram().BuildEarlyFilter(ResultBatch);
    return true;
}

bool TCommittedAssembler::DoApply(IDataReader& owner) const {
    auto& source = owner.GetMeAs<TPlainReadData>().GetSourceByIdxVerified(SourceIdx);
    source.InitFilterStageData(nullptr, EarlyFilter, NArrow::ExtractColumnsValidate(ResultBatch, source.GetFetchingPlan().GetFilterStage()->GetColumnNamesVector()));
    source.InitFetchStageData(NArrow::ExtractColumnsValidate(ResultBatch, source.GetFetchingPlan().GetFetchingStage()->GetColumnNamesVector()));
    return true;
}

TCommittedAssembler::TCommittedAssembler(const NActors::TActorId& scanActorId, const TString& blobData, const TReadMetadata::TConstPtr& readMetadata, const ui32 sourceIdx,
    const TCommittedBlob& cBlob, NColumnShard::TCounterGuard&& taskGuard)
    : TBase(scanActorId)
    , BlobData(blobData)
    , ReadMetadata(readMetadata)
    , SourceIdx(sourceIdx)
    , SchemaVersion(cBlob.GetSchemaVersion())
    , DataSnapshot(cBlob.GetSnapshot())
    , TaskGuard(std::move(taskGuard))
{
}

}
