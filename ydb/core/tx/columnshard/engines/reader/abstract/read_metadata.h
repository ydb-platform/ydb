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
struct TReadMetadataBase {
public:
    enum class ESorting {
        NONE = 0 /* "not_sorted" */,
        ASC /* "ascending" */,
        DESC /* "descending" */,
    };

private:
    const ESorting Sorting = ESorting::ASC;   // Sorting inside returned batches
    std::shared_ptr<TPKRangesFilter> PKRangesFilter;
    TProgramContainer Program;
    std::shared_ptr<TVersionedIndex> IndexVersionsPointer;
    TSnapshot RequestSnapshot;
    std::optional<TGranuleShardingInfo> RequestShardingInfo;
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

    void OnReplyConstruction(const ui64 tabletId, NKqp::NInternalImplementation::TEvScanData& scanData) const {
        DoOnReplyConstruction(tabletId, scanData);
    }

    ui64 GetTxId() const {
        return TxId;
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
        return ResultIndexSchema;
    }

    bool HasGuaranteeExclusivePK() const {
        return GetIndexInfo().GetExternalGuaranteeExclusivePK();
    }

    ISnapshotSchema::TPtr GetLoadSchemaVerified(const TPortionInfo& porition) const;

    const std::shared_ptr<NArrow::TSchemaLite>& GetBlobSchema(const ui64 version) const {
        return GetIndexVersions().GetSchema(version)->GetIndexInfo().ArrowSchema();
    }

    const TIndexInfo& GetIndexInfo(const std::optional<TSnapshot>& version = {}) const {
        if (version && version < RequestSnapshot) {
            return GetIndexVersions().GetSchema(*version)->GetIndexInfo();
        }
        return ResultIndexSchema->GetIndexInfo();
    }

    void InitShardingInfo(const ui64 pathId) {
        AFL_VERIFY(!RequestShardingInfo);
        RequestShardingInfo = IndexVersionsPointer->GetShardingInfoOptional(pathId, RequestSnapshot);
    }

    TReadMetadataBase(const std::shared_ptr<TVersionedIndex> index, const ESorting sorting, const TProgramContainer& ssaProgram,
        const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot)
        : Sorting(sorting)
        , Program(ssaProgram)
        , IndexVersionsPointer(index)
        , RequestSnapshot(requestSnapshot)
        , ResultIndexSchema(schema) {
    }
    virtual ~TReadMetadataBase() = default;

    ui64 Limit = 0;

    virtual void Dump(IOutputStream& out) const {
        out << " predicate{" << (PKRangesFilter ? PKRangesFilter->DebugString() : "no_initialized") << "}"
            << " " << Sorting << " sorted";
    }

    std::set<ui32> GetProcessingColumnIds() const {
        std::set<ui32> result;
        for (auto&& i : GetProgram().GetProcessingColumns()) {
            result.emplace(ResultIndexSchema->GetIndexInfo().GetColumnIdVerified(i));
        }
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

    // TODO:  can this only be done for base class?
    friend IOutputStream& operator<<(IOutputStream& out, const TReadMetadataBase& meta) {
        meta.Dump(out);
        return out;
    }

    const TProgramContainer& GetProgram() const {
        return Program;
    }

    const TSnapshot& GetRequestSnapshot() const {
        return RequestSnapshot;
    }

    std::shared_ptr<arrow::Schema> GetReplaceKey() const {
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
