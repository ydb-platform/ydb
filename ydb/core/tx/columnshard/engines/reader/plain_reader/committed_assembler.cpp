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
    if (source.GetFetchingPlan().GetFilterStage()->GetSchema()) {
        source.InitFilterStageData(nullptr, EarlyFilter, NArrow::ExtractColumns(ResultBatch, source.GetFetchingPlan().GetFilterStage()->GetSchema(), true));
    } else {
        source.InitFilterStageData(nullptr, EarlyFilter, nullptr);
    }
    if (source.GetFetchingPlan().GetFetchingStage()->GetSchema()) {
        source.InitFetchStageData(NArrow::ExtractColumns(ResultBatch, source.GetFetchingPlan().GetFetchingStage()->GetSchema(), true));
    } else {
        source.InitFetchStageData(nullptr);
    }
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
