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

bool TCommittedAssembler::DoApply(IDataReader& /*owner*/) const {
    if (Source->GetFetchingPlan().GetFilterStage()->GetSchema()) {
        Source->InitFilterStageData(nullptr, EarlyFilter, NArrow::ExtractColumns(ResultBatch, Source->GetFetchingPlan().GetFilterStage()->GetSchema(), true), Source);
    } else {
        Source->InitFilterStageData(nullptr, EarlyFilter, nullptr, Source);
    }
    if (Source->GetFetchingPlan().GetFetchingStage()->GetSchema()) {
        Source->InitFetchStageData(NArrow::ExtractColumns(ResultBatch, Source->GetFetchingPlan().GetFetchingStage()->GetSchema(), true));
    } else {
        Source->InitFetchStageData(nullptr);
    }
    return true;
}

TCommittedAssembler::TCommittedAssembler(const NActors::TActorId& scanActorId, const TString& blobData, const TReadMetadata::TConstPtr& readMetadata, const std::shared_ptr<IDataSource>& source,
    const TCommittedBlob& cBlob, NColumnShard::TCounterGuard&& taskGuard)
    : TBase(scanActorId)
    , BlobData(blobData)
    , ReadMetadata(readMetadata)
    , Source(source)
    , SchemaVersion(cBlob.GetSchemaVersion())
    , DataSnapshot(cBlob.GetSnapshot())
    , TaskGuard(std::move(taskGuard))
{
}

}
