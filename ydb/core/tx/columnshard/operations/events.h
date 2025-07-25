#pragma once
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/common/blob.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/portions/write_with_blobs.h>
#include <util/generic/hash.h>

namespace NKikimr::NColumnShard {

class TInsertedPortion {
private:
    YDB_READONLY_DEF(std::shared_ptr<NOlap::TPortionAccessorConstructor>, PortionInfoConstructor);
    std::optional<std::shared_ptr<NOlap::TPortionDataAccessor>> PortionInfo;

public:
    const NOlap::TPortionDataAccessor& GetPortionInfo() const {
        AFL_VERIFY(PortionInfo);
        return **PortionInfo;
    }
    const std::shared_ptr<NOlap::TPortionDataAccessor>& GetPortionInfoPtr() const {
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
    TString ErrorMessage;
    std::optional<bool> IsInternalErrorFlag;
    std::shared_ptr<arrow::RecordBatch> PKBatch;
    ui32 RecordsCount;

public:
    TWriteResult& SetErrorMessage(const TString& value, const bool isInternal) {
        AFL_VERIFY(!ErrorMessage);
        IsInternalErrorFlag = isInternal;
        ErrorMessage = value;
        return *this;
    }

    bool IsInternalError() const {
        AFL_VERIFY_DEBUG(!!IsInternalErrorFlag);
        if (!IsInternalErrorFlag) {
            return true;
        }
        return *IsInternalErrorFlag;
    }

    const TString& GetErrorMessage() const {
        static TString undefinedMessage = "UNKNOWN_WRITE_RESULT_MESSAGE";
        AFL_VERIFY_DEBUG(!!ErrorMessage);
        if (!ErrorMessage) {
            return undefinedMessage;
        }
        return ErrorMessage;
    }

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
    YDB_READONLY_DEF(TInternalPathId, PathId);

public:
    TInsertedPortions(std::vector<TWriteResult>&& writeResults, std::vector<TInsertedPortion>&& portions)
        : WriteResults(std::move(writeResults))
        , Portions(std::move(portions)) {
        AFL_VERIFY(WriteResults.size());
        std::optional<TInternalPathId> pathId;
        for (auto&& i : WriteResults) {
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
    std::optional<std::shared_ptr<NOlap::IBlobsWritingAction>> WriteAction;
    bool Detached = false;
    TInsertedPortions InsertedData;

public:
    const std::shared_ptr<NOlap::IBlobsWritingAction>& GetWriteAction() const {
        AFL_VERIFY(!!WriteAction);
        return *WriteAction;
    }

    const TInsertedPortions& DetachInsertedData() {
        AFL_VERIFY(!Detached);
        Detached = true;
        return std::move(InsertedData);
    }

    TEvWritePortionResult(const NKikimrProto::EReplyStatus writeStatus, const std::shared_ptr<NOlap::IBlobsWritingAction>& writeAction,
        TInsertedPortions&& insertedData);
};

}   // namespace NKikimr::NColumnShard::NPrivateEvents::NWrite
