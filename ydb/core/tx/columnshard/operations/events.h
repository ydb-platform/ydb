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
    NEvWrite::TWriteMeta WriteMeta;
    YDB_READONLY(ui64, DataSize, 0);
    YDB_READONLY(bool, NoDataToWrite, false);
    std::shared_ptr<arrow::RecordBatch> PKBatch;

public:
    const std::shared_ptr<arrow::RecordBatch>& GetPKBatchVerified() const {
        AFL_VERIFY(PKBatch);
        return PKBatch;
    }

    const NEvWrite::TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    TWriteResult(const NEvWrite::TWriteMeta& writeMeta, const ui64 dataSize, const std::shared_ptr<arrow::RecordBatch>& pkBatch, const bool noDataToWrite)
        : WriteMeta(writeMeta)
        , DataSize(dataSize)
        , NoDataToWrite(noDataToWrite)
        , PKBatch(pkBatch)
    {

    }
};

class TInsertedPortions {
private:
    YDB_ACCESSOR_DEF(std::vector<TWriteResult>, WriteResults);
    YDB_ACCESSOR_DEF(std::vector<TInsertedPortion>, Portions);
    std::optional<EOperationBehaviour> Behaviour;

public:
    EOperationBehaviour GetBehaviour() const {
        AFL_VERIFY(!!Behaviour);
        return *Behaviour;
    }

    TInsertedPortions(std::vector<TWriteResult>&& writeResults, std::vector<TInsertedPortion>&& portions)
        : WriteResults(std::move(writeResults))
        , Portions(std::move(portions)) {
        for (auto&& i : WriteResults) {
            AFL_VERIFY(!i.GetWriteMeta().HasLongTxId());
            if (!Behaviour) {
                Behaviour = i.GetWriteMeta().GetBehaviour();
            } else {
                AFL_VERIFY(Behaviour == i.GetWriteMeta().GetBehaviour());
            }
        }
        AFL_VERIFY(Behaviour);
        if (Behaviour == EOperationBehaviour::NoTxWrite) {

        } else {
            AFL_VERIFY(Behaviour == EOperationBehaviour::WriteWithLock)("behaviour", Behaviour);
            AFL_VERIFY(WriteResults.size() == 1)("size", WriteResults.size());
        }
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
        TInsertedPortions&& insertedData)
        : WriteStatus(writeStatus)
        , WriteAction(writeAction)
        , InsertedData(std::move(insertedData)) {
    }
};

}   // namespace NKikimr::NColumnShard::NPrivateEvents::NWrite
