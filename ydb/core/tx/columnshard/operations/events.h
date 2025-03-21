#pragma once
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <util/generic/hash.h>

namespace NKikimr::NColumnShard {

class TInsertedPortion {
private:
    YDB_READONLY_DEF(std::shared_ptr<NOlap::TPortionAccessorConstructor>, PortionInfoConstructor);
    std::optional<NOlap::TPortionDataAccessor> PortionInfo;

public:
    const NOlap::TPortionDataAccessor& GetPortionInfo() const {
        AFL_VERIFY(PortionInfo);
        return *PortionInfo;
    }
    TInsertedPortion(NOlap::TWritePortionInfoWithBlobsResult&& portion)
        : PortionInfoConstructor(portion.DetachPortionConstructor()) {
    }

    void Finalize(TColumnShard* shard, NTabletFlatExecutor::TTransactionContext& txc);
};

class TWriteResult {
private:
    std::shared_ptr<NEvWrite::TWriteMeta> WriteMeta;
    YDB_READONLY(ui64, DataSize, 0);
    YDB_READONLY(bool, NoDataToWrite, false);
    std::shared_ptr<arrow::RecordBatch> PKBatch;
    ui32 RecordsCount;

public:
    const std::shared_ptr<arrow::RecordBatch>& GetPKBatchVerified() const {
        AFL_VERIFY(PKBatch);
        return PKBatch;
    }

    ui32 GetRecordsCount() const {
        return RecordsCount;
    }

    const NEvWrite::TWriteMeta& GetWriteMeta() const {
        return *WriteMeta;
    }

    NEvWrite::TWriteMeta& MutableWriteMeta() const {
        return *WriteMeta;
    }

    const std::shared_ptr<NEvWrite::TWriteMeta>& GetWriteMetaPtr() const {
        return WriteMeta;
    }

    TWriteResult(const std::shared_ptr<NEvWrite::TWriteMeta>& writeMeta, const ui64 dataSize, const std::shared_ptr<arrow::RecordBatch>& pkBatch,
        const bool noDataToWrite, const ui32 recordsCount);
};

class TInsertedPortions {
private:
    YDB_ACCESSOR_DEF(std::vector<TWriteResult>, WriteResults);
    YDB_ACCESSOR_DEF(std::vector<TInsertedPortion>, Portions);
    YDB_READONLY(NColumnShard::TInternalPathId, PathId, NColumnShard::TInternalPathId{});

public:
    TInsertedPortions(std::vector<TWriteResult>&& writeResults, std::vector<TInsertedPortion>&& portions)
        : WriteResults(std::move(writeResults))
        , Portions(std::move(portions)) {
        AFL_VERIFY(WriteResults.size());
        std::optional<NColumnShard::TInternalPathId> pathId;
        for (auto&& i : WriteResults) {
            i.GetWriteMeta().OnStage(NEvWrite::EWriteStage::Finished);
            AFL_VERIFY(!i.GetWriteMeta().HasLongTxId());
            if (!pathId) {
                pathId = i.GetWriteMeta().GetPathId().InternalPathId;
            } else {
                AFL_VERIFY(pathId == i.GetWriteMeta().GetPathId().InternalPathId);
            }
        }
        AFL_VERIFY(pathId);
        PathId = *pathId;
    }
};

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NColumnShard::NPrivateEvents::NWrite {

class TEvWritePortionResult: public TEventLocal<TEvWritePortionResult, TEvPrivate::EvWritePortionResult> {
private:
    YDB_READONLY_DEF(NKikimrProto::EReplyStatus, WriteStatus);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, WriteAction);
    bool Detached = false;
    TInsertedPortions InsertedData;

public:
    const TInsertedPortions& DetachInsertedData() {
        AFL_VERIFY(!Detached);
        Detached = true;
        return std::move(InsertedData);
    }

    TEvWritePortionResult(const NKikimrProto::EReplyStatus writeStatus, const std::shared_ptr<NOlap::IBlobsWritingAction>& writeAction,
        TInsertedPortions&& insertedData);
};

}   // namespace NKikimr::NColumnShard::NPrivateEvents::NWrite
