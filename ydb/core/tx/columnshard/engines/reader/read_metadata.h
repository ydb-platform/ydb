#pragma once
#include "conveyor_task.h"
#include "description.h"
#include "read_filter_merger.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/counters.h>
#include <ydb/core/tx/columnshard/columnshard__scan.h>
#include <ydb/core/tx/columnshard/columnshard_common.h>
#include <ydb/core/tx/columnshard/engines/insert_table/insert_table.h>
#include <ydb/core/tx/columnshard/engines/predicate/predicate.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

namespace NKikimr::NColumnShard {
class TScanIteratorBase;
}

namespace NKikimr::NOlap {

class TReadContext;

struct TReadStats {
    TInstant BeginTimestamp;
    ui64 IndexGranules{0};
    ui64 IndexPortions{0};
    ui64 IndexBatches{0};
    ui64 CommittedBatches{0};
    ui64 CommittedPortionsBytes = 0;
    ui64 InsertedPortionsBytes = 0;
    ui64 CompactedPortionsBytes = 0;
    ui64 DataFilterBytes{ 0 };
    ui64 DataAdditionalBytes{ 0 };

    ui32 SchemaColumns = 0;
    ui32 FilterColumns = 0;
    ui32 AdditionalColumns = 0;

    ui32 SelectedRows = 0;

    TReadStats()
        : BeginTimestamp(TInstant::Now())
    {}

    void PrintToLog();

    ui64 GetReadBytes() const {
        return CompactedPortionsBytes + InsertedPortionsBytes + CompactedPortionsBytes;
    }

    TDuration Duration() {
        return TInstant::Now() - BeginTimestamp;
    }
};

class TDataStorageAccessor {
private:
    const std::unique_ptr<NOlap::TInsertTable>& InsertTable;
    const std::unique_ptr<NOlap::IColumnEngine>& Index;

public:
    TDataStorageAccessor(const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                 const std::unique_ptr<NOlap::IColumnEngine>& index);
    std::shared_ptr<NOlap::TSelectInfo> Select(const NOlap::TReadDescription& readDescription) const;
    std::vector<NOlap::TCommittedBlob> GetCommitedBlobs(const NOlap::TReadDescription& readDescription, const std::shared_ptr<arrow::Schema>& pkSchema) const;
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
    const ESorting Sorting = ESorting::ASC; // Sorting inside returned batches
    std::optional<TPKRangesFilter> PKRangesFilter;
    TProgramContainer Program;
    std::shared_ptr<TVersionedIndex> IndexVersionsPointer;
    TSnapshot RequestSnapshot;
protected:
    std::shared_ptr<ISnapshotSchema> ResultIndexSchema;
    const TVersionedIndex& GetIndexVersions() const {
        AFL_VERIFY(IndexVersionsPointer);
        return *IndexVersionsPointer;
    }
public:
    using TConstPtr = std::shared_ptr<const TReadMetadataBase>;

    void SetPKRangesFilter(const TPKRangesFilter& value) {
        Y_ABORT_UNLESS(IsSorted() && value.IsReverse() == IsDescSorted());
        Y_ABORT_UNLESS(!PKRangesFilter);
        PKRangesFilter = value;
    }

    const TPKRangesFilter& GetPKRangesFilter() const {
        Y_ABORT_UNLESS(!!PKRangesFilter);
        return *PKRangesFilter;
    }

    ISnapshotSchema::TPtr GetSnapshotSchema(const TSnapshot& version) const {
        if (version >= RequestSnapshot) {
            return ResultIndexSchema;
        }
        return GetIndexVersions().GetSchema(version);
    }

    ISnapshotSchema::TPtr GetLoadSchema(const std::optional<TSnapshot>& version = {}) const {
        if (!version) {
            return ResultIndexSchema;
        }
        return GetIndexVersions().GetSchema(*version);
    }

    std::shared_ptr<arrow::Schema> GetBlobSchema(const ui64 version) const {
        return GetIndexVersions().GetSchema(version)->GetIndexInfo().ArrowSchema();
    }

    const TIndexInfo& GetIndexInfo(const std::optional<TSnapshot>& version = {}) const {
        if (version && version < RequestSnapshot) {
            return GetIndexVersions().GetSchema(*version)->GetIndexInfo();
        }
        return ResultIndexSchema->GetIndexInfo();
    }

    TReadMetadataBase(const std::shared_ptr<TVersionedIndex> index, const ESorting sorting, const TProgramContainer& ssaProgram, const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot)
        : Sorting(sorting)
        , Program(ssaProgram)
        , IndexVersionsPointer(index)
        , RequestSnapshot(requestSnapshot)
        , ResultIndexSchema(schema)
    {
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
            result.emplace(ResultIndexSchema->GetIndexInfo().GetColumnId(i));
        }
        return result;
    }
    bool IsAscSorted() const { return Sorting == ESorting::ASC; }
    bool IsDescSorted() const { return Sorting == ESorting::DESC; }
    bool IsSorted() const { return IsAscSorted() || IsDescSorted(); }

    virtual std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(const std::shared_ptr<NOlap::TReadContext>& readContext) const = 0;
    virtual std::vector<TNameTypeInfo> GetKeyYqlSchema() const = 0;

    // TODO:  can this only be done for base class?
    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadataBase& meta) {
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

// Holds all metadata that is needed to perform read/scan
struct TReadMetadata : public TReadMetadataBase, public std::enable_shared_from_this<TReadMetadata> {
    using TBase = TReadMetadataBase;
public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    NIndexedReader::TSortableBatchPosition BuildSortedPosition(const NArrow::TReplaceKey& key) const;
    std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<NOlap::TReadContext>& context) const;

    bool HasProcessingColumnIds() const {
        return GetProgram().HasProcessingColumnIds();
    }

    std::shared_ptr<TSelectInfo> SelectInfo;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    std::vector<TCommittedBlob> CommittedBlobs;
    std::shared_ptr<TReadStats> ReadStats;

    TReadMetadata(const std::shared_ptr<TVersionedIndex> info, const TSnapshot& snapshot, const ESorting sorting, const TProgramContainer& ssaProgram)
        : TBase(info, sorting, ssaProgram, info->GetSchema(snapshot), snapshot)
        , ReadStats(std::make_shared<TReadStats>())
    {
    }

    virtual std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return GetLoadSchema()->GetIndexInfo().GetPrimaryKeyColumns();
    }

    bool Init(const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor, std::string& error);

    std::vector<std::string> GetColumnsOrder() const {
        auto loadSchema = GetLoadSchema(GetRequestSnapshot());
        std::vector<std::string> result;
        for (auto&& i : loadSchema->GetSchema()->fields()) {
            result.emplace_back(i->name());
        }
        return result;
    }

    std::set<ui32> GetEarlyFilterColumnIds() const;
    std::set<ui32> GetPKColumnIds() const;

    bool Empty() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->PortionsOrderedPK.empty() && CommittedBlobs.empty();
    }

    size_t NumIndexedChunks() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->NumChunks();
    }

    size_t NumIndexedBlobs() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->Stats().Blobs;
    }

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(const std::shared_ptr<NOlap::TReadContext>& readContext) const override;

    void Dump(IOutputStream& out) const override {
        out << " index chunks: " << NumIndexedChunks()
            << " index blobs: " << NumIndexedBlobs()
            << " committed blobs: " << CommittedBlobs.size()
      //      << " with program steps: " << (Program ? Program->Steps.size() : 0)
            << " at snapshot: " << GetRequestSnapshot().DebugString();
        TBase::Dump(out);
        if (SelectInfo) {
            out << ", " << *SelectInfo;
        }
    }

    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadata& meta) {
        meta.Dump(out);
        return out;
    }
};

struct TReadStatsMetadata : public TReadMetadataBase, public std::enable_shared_from_this<TReadStatsMetadata> {
private:
    using TBase = TReadMetadataBase;
public:
    using TConstPtr = std::shared_ptr<const TReadStatsMetadata>;

    const ui64 TabletId;
    std::vector<ui32> ReadColumnIds;
    std::vector<ui32> ResultColumnIds;
    std::deque<std::shared_ptr<NOlap::TPortionInfo>> IndexPortions;

    explicit TReadStatsMetadata(const std::shared_ptr<TVersionedIndex>& info, ui64 tabletId, const ESorting sorting, const TProgramContainer& ssaProgram, const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot)
        : TBase(info, sorting, ssaProgram, schema, requestSnapshot)
        , TabletId(tabletId)
    {
    }

    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(const std::shared_ptr<NOlap::TReadContext>& readContext) const override;
};

}
