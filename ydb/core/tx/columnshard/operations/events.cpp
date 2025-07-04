#include "events.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

void TInsertedPortion::Finalize(TColumnShard* shard, NTabletFlatExecutor::TTransactionContext& txc) {
    AFL_VERIFY(PortionInfoConstructor);
    auto* lastPortionId = shard->MutableIndexAs<NOlap::TColumnEngineForLogs>().GetLastPortionPointer();
    PortionInfoConstructor->MutablePortionConstructor().SetPortionId(++*lastPortionId);
    NOlap::TDbWrapper wrapper(txc.DB, nullptr);
    wrapper.WriteCounter(NOlap::TColumnEngineForLogs::LAST_PORTION, *lastPortionId);
    PortionInfo = PortionInfoConstructor->Build(true);
    PortionInfoConstructor = nullptr;
}

TWriteResult::TWriteResult(const std::shared_ptr<NEvWrite::TWriteMeta>& writeMeta, const ui64 dataSize,
    const std::shared_ptr<arrow::RecordBatch>& pkBatch,
    const bool noDataToWrite, const ui32 recordsCount)
    : WriteMeta(writeMeta)
    , DataSize(dataSize)
    , NoDataToWrite(noDataToWrite)
    , PKBatch(pkBatch)
    , RecordsCount(recordsCount) {
    AFL_VERIFY(WriteMeta);
}

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NColumnShard::NPrivateEvents::NWrite {
TEvWritePortionResult::TEvWritePortionResult(const NKikimrProto::EReplyStatus writeStatus,
    const std::shared_ptr<NOlap::IBlobsWritingAction>& writeAction, TInsertedPortions&& insertedData)
    : WriteStatus(writeStatus)
    , WriteAction(writeAction)
    , InsertedData(std::move(insertedData)) {
}

}   // namespace NKikimr::NColumnShard::NPrivateEvents::NWrite
