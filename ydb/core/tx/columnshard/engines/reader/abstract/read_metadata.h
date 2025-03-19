#pragma once
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/insert_table/insert_table.h>
#include <ydb/core/tx/columnshard/engines/reader/common/description.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>

namespace NKikimr::NOlap {
class TPortionInfo;
}

namespace NKikimr::NKqp::NInternalImplementation {
struct TEvScanData;
}

namespace NKikimr::NOlap::NReader {

class TScanIteratorBase;
class TReadContext;

class TDataStorageAccessor {
private:
    const std::unique_ptr<NOlap::TInsertTable>& InsertTable;
    const std::unique_ptr<NOlap::IColumnEngine>& Index;

public:
    TDataStorageAccessor(const std::unique_ptr<TInsertTable>& insertTable, const std::unique_ptr<IColumnEngine>& index);
    std::shared_ptr<NOlap::TSelectInfo> Select(const TReadDescription& readDescription, const bool withUncommitted) const;
    std::vector<NOlap::TCommittedBlob> GetCommitedBlobs(const TReadDescription& readDescription, const std::shared_ptr<arrow::Schema>& pkSchema,
        const std::optional<ui64> lockId, const TSnapshot& reqSnapshot) const;
};

// Holds all metadata that is needed to perform read/scan
class TReadMetadataBase {
public:
    enum class ESorting {
        NONE = 0 /* "not_sorted" */,
        ASC /* "ascending" */,
        DESC /* "descending" */,
    };

private:
    YDB_ACCESSOR_DEF(TString, ScanIdentifier);
    std::optional<ui64> FilteredCountLimit;
    std::optional<ui64> RequestedLimit;
    const ESorting Sorting = ESorting::ASC;   // Sorting inside returned batches
    std::shared_ptr<TPKRangesFilter> PKRangesFilter;
    TProgramContainer Program;
    std::shared_ptr<TVersionedIndex> IndexVersionsPointer;
    TSnapshot RequestSnapshot;
    std::optional<TGranuleShardingInfo> RequestShardingInfo;
    std::shared_ptr<IScanCursor> ScanCursor;
    virtual void DoOnReadFinished(NColumnShard::TColumnShard& /*owner*/) const {
    }
    virtual void DoOnBeforeStartReading(NColumnShard::TColumnShard& /*owner*/) const {
    }
    virtual void DoOnReplyConstruction(const ui64 /*tabletId*/, NKqp::NInternalImplementation::TEvScanData& /*scanData*/) const {
    }

protected:
    std::shared_ptr<ISnapshotSchema> ResultIndexSchema;
    ui64 TxId = 0;
    std::optional<ui64> LockId;

public:
    using TConstPtr = std::shared_ptr<const TReadMetadataBase>;

    void SetRequestedLimit(const ui64 value) { 
        AFL_VERIFY(!RequestedLimit);
        if (value == 0 || value >= Max<i64>()) {
            return;
        }
        RequestedLimit = value;
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("requested_limit_detected", RequestedLimit);
    }

    i64 GetLimitRobust() const {
        return std::min<i64>(FilteredCountLimit.value_or(Max<i64>()), RequestedLimit.value_or(Max<i64>()));
    }

    std::optional<i64> GetLimitRobustOptional() const {
        if (HasLimit()) {
            return GetLimitRobust();
        } else {
            return std::nullopt;
        }
    }

    bool HasLimit() const {
        return !!FilteredCountLimit || !!RequestedLimit;
    }

    void OnReplyConstruction(const ui64 tabletId, NKqp::NInternalImplementation::TEvScanData& scanData) const {
        DoOnReplyConstruction(tabletId, scanData);
    }

    ui64 GetTxId() const {
        return TxId;
    }

    const std::shared_ptr<IScanCursor>& GetScanCursor() const {
        return ScanCursor;
    }

    std::optional<ui64> GetLockId() const {
        return LockId;
    }

    void OnReadFinished(NColumnShard::TColumnShard& owner) const {
        DoOnReadFinished(owner);
    }

    void OnBeforeStartReading(NColumnShard::TColumnShard& owner) const {
        DoOnBeforeStartReading(owner);
    }

    const TVersionedIndex& GetIndexVersions() const {
        AFL_VERIFY(IndexVersionsPointer);
        return *IndexVersionsPointer;
    }

    const std::optional<TGranuleShardingInfo>& GetRequestShardingInfo() const {
        return RequestShardingInfo;
    }

    void SetPKRangesFilter(const std::shared_ptr<TPKRangesFilter>& value) {
        AFL_VERIFY(value);
        Y_ABORT_UNLESS(IsSorted() && value->IsReverse() == IsDescSorted());
        Y_ABORT_UNLESS(!PKRangesFilter);
        PKRangesFilter = value;
        if (ResultIndexSchema) {
            FilteredCountLimit = PKRangesFilter->GetFilteredCountLimit(ResultIndexSchema->GetIndexInfo().GetReplaceKey());
            if (FilteredCountLimit) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("filter_limit_detected", FilteredCountLimit);
            } else {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("filter_limit_not_detected", PKRangesFilter->DebugString());
            }
        }
    }

    const TPKRangesFilter& GetPKRangesFilter() const {
        Y_ABORT_UNLESS(!!PKRangesFilter);
        return *PKRangesFilter;
    }

    const std::shared_ptr<TPKRangesFilter>& GetPKRangesFilterPtr() const {
        Y_ABORT_UNLESS(!!PKRangesFilter);
        return PKRangesFilter;
    }

    ISnapshotSchema::TPtr GetResultSchema() const {
        AFL_VERIFY(ResultIndexSchema);
        return ResultIndexSchema;
    }

    bool HasResultSchema() const {
        return !!ResultIndexSchema;
    }

    ISnapshotSchema::TPtr GetLoadSchemaVerified(const TPortionInfo& porition) const;

    NArrow::TSchemaLiteView GetBlobSchema(const ui64 version) const {
        return GetIndexVersions().GetSchemaVerified(version)->GetIndexInfo().ArrowSchema();
    }

    const TIndexInfo& GetIndexInfo(const std::optional<TSnapshot>& version = {}) const {
        AFL_VERIFY(ResultIndexSchema);
        if (version && version < RequestSnapshot) {
            return GetIndexVersions().GetSchemaVerified(*version)->GetIndexInfo();
        }
        return ResultIndexSchema->GetIndexInfo();
    }

    void InitShardingInfo(const ui64 pathId) {
        AFL_VERIFY(!RequestShardingInfo);
        RequestShardingInfo = IndexVersionsPointer->GetShardingInfoOptional(pathId, RequestSnapshot);
    }

    TReadMetadataBase(const std::shared_ptr<TVersionedIndex> index, const ESorting sorting, const TProgramContainer& ssaProgram,
        const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot, const std::shared_ptr<IScanCursor>& scanCursor)
        : Sorting(sorting)
        , Program(ssaProgram)
        , IndexVersionsPointer(index)
        , RequestSnapshot(requestSnapshot)
        , ScanCursor(scanCursor)
        , ResultIndexSchema(schema)
    {
    }
    virtual ~TReadMetadataBase() = default;

    virtual TString DebugString() const {
        return TStringBuilder() << " predicate{" << (PKRangesFilter ? PKRangesFilter->DebugString() : "no_initialized") << "}"
            << " " << Sorting << " sorted";
    }

    std::set<ui32> GetProcessingColumnIds() const {
        AFL_VERIFY(ResultIndexSchema);
        std::set<ui32> result(GetProgram().GetProcessingColumns().begin(), GetProgram().GetProcessingColumns().end());
        return result;
    }
    bool IsAscSorted() const {
        return Sorting == ESorting::ASC;
    }
    bool IsDescSorted() const {
        return Sorting == ESorting::DESC;
    }
    bool IsSorted() const {
        return IsAscSorted() || IsDescSorted();
    }

    virtual std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const = 0;
    virtual std::vector<TNameTypeInfo> GetKeyYqlSchema() const = 0;

    const TProgramContainer& GetProgram() const {
        return Program;
    }

    const TSnapshot& GetRequestSnapshot() const {
        return RequestSnapshot;
    }

    std::shared_ptr<arrow::Schema> GetReplaceKey() const {
        AFL_VERIFY(ResultIndexSchema);
        return ResultIndexSchema->GetIndexInfo().GetReplaceKey();
    }

    std::optional<std::string> GetColumnNameDef(const ui32 columnId) const {
        if (!ResultIndexSchema) {
            return {};
        }
        auto f = ResultIndexSchema->GetFieldByColumnIdOptional(columnId);
        if (!f) {
            return {};
        }
        return f->name();
    }

    std::optional<std::string> GetEntityName(const ui32 entityId) const {
        if (!ResultIndexSchema) {
            return {};
        }
        auto result = ResultIndexSchema->GetIndexInfo().GetColumnNameOptional(entityId);
        if (!!result) {
            return result;
        }
        return ResultIndexSchema->GetIndexInfo().GetIndexNameOptional(entityId);
    }
};

}   // namespace NKikimr::NOlap::NReader
