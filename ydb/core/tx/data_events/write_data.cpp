#include "write_data.h"

#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/defs.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NEvWrite {

TWriteData::TWriteData(const std::shared_ptr<TWriteMeta>& writeMeta, IDataContainer::TPtr data, const std::shared_ptr<arrow::Schema>& primaryKeySchema,
    const std::shared_ptr<NOlap::IBlobsWritingAction>& blobsAction)
    : WriteMeta(writeMeta)
    , Data(data)
    , PrimaryKeySchema(primaryKeySchema)
    , BlobsAction(blobsAction) {
    AFL_VERIFY(WriteMeta);
    Y_ABORT_UNLESS(Data);
    Y_ABORT_UNLESS(PrimaryKeySchema);
    Y_ABORT_UNLESS(BlobsAction);
}

void TWriteMeta::OnStage(const EWriteStage stage) const {
    if (stage == CurrentStage) {
        return;
    }
    AFL_VERIFY((ui32)stage > (ui32)CurrentStage)("from", CurrentStage)("to", stage);
    const TMonotonic nextStageInstant = TMonotonic::Now();
    Counters->OnStageMove(CurrentStage, stage, nextStageInstant - LastStageInstant);
    CurrentStage = stage;
    LastStageInstant = nextStageInstant;
    if (stage == EWriteStage::Finished) {
        Counters->OnWriteFinished(nextStageInstant - WriteStartInstant);
    } else if (stage == EWriteStage::Aborted) {
        Counters->OnWriteAborted(nextStageInstant - WriteStartInstant);
    }
}

}   // namespace NKikimr::NEvWrite
