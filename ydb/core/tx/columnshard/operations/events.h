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
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, PKBatch);

public:
    const NOlap::TPortionDataAccessor& GetPortionInfo() const {
        AFL_VERIFY(PortionInfo);
        return *PortionInfo;
    }
    TInsertedPortion(NOlap::TWritePortionInfoWithBlobsResult&& portion, const std::shared_ptr<arrow::RecordBatch>& pkBatch)
        : PortionInfoConstructor(portion.DetachPortionConstructor())
        , PKBatch(pkBatch) {
        AFL_VERIFY(PKBatch);
    }

    void Finalize(TColumnShard* shard, NTabletFlatExecutor::TTransactionContext& txc);
};

class TInsertedPortions {
private:
    NEvWrite::TWriteMeta WriteMeta;
    YDB_ACCESSOR_DEF(std::vector<TInsertedPortion>, Portions);
    YDB_READONLY(ui64, DataSize, 0);
    YDB_READONLY_DEF(std::vector<NOlap::TInsertWriteId>, InsertWriteIds);

public:
    ui64 GetRecordsCount() const {
        ui64 result = 0;
        for (auto&& i : Portions) {
            result += i.GetPKBatch()->num_rows();
        }
        return result;
    }

    const NEvWrite::TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    void AddInsertWriteId(const NOlap::TInsertWriteId id) {
        InsertWriteIds.emplace_back(id);
    }

    void Finalize(TColumnShard* shard, NTabletFlatExecutor::TTransactionContext& txc);

    TInsertedPortions(const NEvWrite::TWriteMeta& writeMeta, std::vector<TInsertedPortion>&& portions, const ui64 dataSize)
        : WriteMeta(writeMeta)
        , Portions(std::move(portions))
        , DataSize(dataSize) {
        AFL_VERIFY(!WriteMeta.HasLongTxId());
        for (auto&& i : Portions) {
            AFL_VERIFY(i.GetPKBatch());
        }
    }
};

class TNoDataWrite {
private:
    NEvWrite::TWriteMeta WriteMeta;
    YDB_READONLY(ui64, DataSize, 0);

public:
    const NEvWrite::TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    TNoDataWrite(const NEvWrite::TWriteMeta& writeMeta, const ui64 dataSize)
        : WriteMeta(writeMeta)
        , DataSize(dataSize) {
        AFL_VERIFY(!WriteMeta.HasLongTxId());
    }
};

}   // namespace NKikimr::NColumnShard

namespace NKikimr::NColumnShard::NPrivateEvents::NWrite {

class TEvWritePortionResult: public TEventLocal<TEvWritePortionResult, TEvPrivate::EvWritePortionResult> {
private:
    YDB_READONLY_DEF(NKikimrProto::EReplyStatus, WriteStatus);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, WriteAction);
    std::vector<TInsertedPortions> InsertedPacks;
    std::vector<TNoDataWrite> NoData;

public:
    std::vector<TInsertedPortions>&& DetachInsertedPacks() {
        return std::move(InsertedPacks);
    }
    std::vector<TNoDataWrite>&& DetachNoDataWrites() {
        return std::move(NoData);
    }

    TEvWritePortionResult(const NKikimrProto::EReplyStatus writeStatus, const std::shared_ptr<NOlap::IBlobsWritingAction>& writeAction,
        std::vector<TInsertedPortions>&& portions, std::vector<TNoDataWrite>&& noData)
        : WriteStatus(writeStatus)
        , WriteAction(writeAction)
        , InsertedPacks(portions)
        , NoData(noData) {
    }
};

}   // namespace NKikimr::NColumnShard::NPrivateEvents::NWrite
