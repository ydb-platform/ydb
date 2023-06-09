#pragma once
#include "conveyor_task.h"
#include "description.h"
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

namespace NKikimr::NColumnShard {
class TScanIteratorBase;
}

namespace NKikimr::NOlap {

namespace NIndexedReader {
class IOrderPolicy;
}

struct TReadStats {
    TInstant BeginTimestamp;
    ui32 SelectedIndex{0};
    ui64 IndexGranules{0};
    ui64 IndexPortions{0};
    ui64 IndexBatches{0};
    ui64 CommittedBatches{0};
    ui64 PortionsBytes{ 0 };
    ui64 DataFilterBytes{ 0 };
    ui64 DataAdditionalBytes{ 0 };

    ui32 SchemaColumns = 0;
    ui32 FilterColumns = 0;
    ui32 AdditionalColumns = 0;

    ui32 SelectedRows = 0;

    TReadStats(ui32 indexNo)
        : BeginTimestamp(TInstant::Now())
        , SelectedIndex(indexNo)
    {}

    void PrintToLog();

    TDuration Duration() {
        return TInstant::Now() - BeginTimestamp;
    }
};

class TDataStorageAccessor {
private:
    const std::unique_ptr<NOlap::TInsertTable>& InsertTable;
    const std::unique_ptr<NOlap::IColumnEngine>& Index;
    const NColumnShard::TBatchCache& BatchCache;

public:
    TDataStorageAccessor(const std::unique_ptr<NOlap::TInsertTable>& insertTable,
                                 const std::unique_ptr<NOlap::IColumnEngine>& index,
                                 const NColumnShard::TBatchCache& batchCache);
    std::shared_ptr<NOlap::TSelectInfo> Select(const NOlap::TReadDescription& readDescription, const THashSet<ui32>& columnIds) const;
    std::vector<NOlap::TCommittedBlob> GetCommitedBlobs(const NOlap::TReadDescription& readDescription) const;
    std::shared_ptr<arrow::RecordBatch> GetCachedBatch(const TUnifiedBlobId& blobId) const;
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
public:
    using TConstPtr = std::shared_ptr<const TReadMetadataBase>;

    void SetPKRangesFilter(const TPKRangesFilter& value) {
        Y_VERIFY(IsSorted() && value.IsReverse() == IsDescSorted());
        Y_VERIFY(!PKRangesFilter);
        PKRangesFilter = value;
    }

    const TPKRangesFilter& GetPKRangesFilter() const {
        Y_VERIFY(!!PKRangesFilter);
        return *PKRangesFilter;
    }

    TReadMetadataBase(const ESorting sorting, const TProgramContainer& ssaProgram)
        : Sorting(sorting)
        , Program(ssaProgram)
    {
    }
    virtual ~TReadMetadataBase() = default;

    std::shared_ptr<NOlap::TPredicate> LessPredicate;
    std::shared_ptr<NOlap::TPredicate> GreaterPredicate;
    std::shared_ptr<const THashSet<TUnifiedBlobId>> ExternBlobs;
    ui64 Limit{0}; // TODO

    virtual void Dump(IOutputStream& out) const {
        out << " predicate{" << (PKRangesFilter ? PKRangesFilter->DebugString() : "no_initialized") << "}"
            << " " << Sorting << " sorted";
    }

    bool IsAscSorted() const { return Sorting == ESorting::ASC; }
    bool IsDescSorted() const { return Sorting == ESorting::DESC; }
    bool IsSorted() const { return IsAscSorted() || IsDescSorted(); }

    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetResultYqlSchema() const = 0;
    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const = 0;
    virtual std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const = 0;

    // TODO:  can this only be done for base class?
    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadataBase& meta) {
        meta.Dump(out);
        return out;
    }

    const TProgramContainer& GetProgram() const {
        return Program;
    }
};

// Holds all metadata that is needed to perform read/scan
struct TReadMetadata : public TReadMetadataBase, public std::enable_shared_from_this<TReadMetadata> {
    using TBase = TReadMetadataBase;
private:
    TVersionedIndex IndexVersions;
    TSnapshot Snapshot;
    std::shared_ptr<ISnapshotSchema> ResultIndexSchema;
    std::vector<ui32> AllColumns;
    std::vector<ui32> ResultColumnsIds;
    std::shared_ptr<NIndexedReader::IOrderPolicy> DoBuildSortingPolicy() const;
public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    std::shared_ptr<TSelectInfo> SelectInfo;
    std::vector<TCommittedBlob> CommittedBlobs;
    THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CommittedBatches;
    std::shared_ptr<TReadStats> ReadStats;

    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }

    std::shared_ptr<NIndexedReader::IOrderPolicy> BuildSortingPolicy() const;

    TReadMetadata(const TVersionedIndex& info, const TSnapshot& snapshot, const ESorting sorting, const TProgramContainer& ssaProgram)
        : TBase(sorting, ssaProgram)
        , IndexVersions(info)
        , Snapshot(snapshot)
        , ResultIndexSchema(info.GetSchema(Snapshot))
        , ReadStats(std::make_shared<TReadStats>(info.GetLastSchema()->GetIndexInfo().GetId()))
    {
    }

    bool Init(const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor, std::string& error);

    ui64 GetSchemaColumnsCount() const {
        return AllColumns.size();
    }

    ISnapshotSchema::TPtr GetSnapshotSchema(const TSnapshot& version) const {
        if (version >= Snapshot){
            return ResultIndexSchema;
        }
        return IndexVersions.GetSchema(version);
    }
    
    ISnapshotSchema::TPtr GetLoadSchema(const std::optional<TSnapshot>& version = {}) const {
        if (!version) {
            return make_shared<TFilteredSnapshotSchema>(ResultIndexSchema, AllColumns);
        }
        return make_shared<TFilteredSnapshotSchema>(IndexVersions.GetSchema(*version), AllColumns);
    }

    std::shared_ptr<arrow::Schema> GetBlobSchema(const TSnapshot& version) const {
        return IndexVersions.GetSchema(version)->GetIndexInfo().ArrowSchema();
    }

    std::shared_ptr<arrow::Schema> GetResultSchema() const {
        return ResultIndexSchema->GetIndexInfo().ArrowSchema(ResultColumnsIds);
    }

    const TIndexInfo& GetIndexInfo(const std::optional<TSnapshot>& version = {}) const {
        if (version && version < Snapshot) {
            return IndexVersions.GetSchema(*version)->GetIndexInfo();
        }
        return ResultIndexSchema->GetIndexInfo();
    }

    std::vector<std::string> GetColumnsOrder() const {
        auto loadSchema = GetLoadSchema(Snapshot);
        std::vector<std::string> result;
        for (auto&& i : loadSchema->GetSchema()->fields()) {
            result.emplace_back(i->name());
        }
        return result;
    }

    std::set<ui32> GetEarlyFilterColumnIds() const;
    std::set<ui32> GetUsedColumnIds() const;
    std::set<ui32> GetPKColumnIds() const;

    bool Empty() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->Portions.empty() && CommittedBlobs.empty();
    }

    std::shared_ptr<arrow::Schema> GetSortingKey() const {
        return ResultIndexSchema->GetIndexInfo().GetSortingKey();
    }

    std::shared_ptr<arrow::Schema> GetReplaceKey() const {
        return ResultIndexSchema->GetIndexInfo().GetReplaceKey();
    }

    std::vector<TNameTypeInfo> GetResultYqlSchema() const override {
        auto& indexInfo = ResultIndexSchema->GetIndexInfo();
        auto resultSchema = GetResultSchema();
        Y_VERIFY(resultSchema);
        std::vector<NTable::TTag> columnIds;
        columnIds.reserve(resultSchema->num_fields());
        for (const auto& field: resultSchema->fields()) {
            TString name = TStringBuilder() << field->name();
            columnIds.emplace_back(indexInfo.GetColumnId(name));
        }
        return indexInfo.GetColumns(columnIds);
    }

    std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return ResultIndexSchema->GetIndexInfo().GetPrimaryKey();
    }

    size_t NumIndexedRecords() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->NumRecords();
    }

    size_t NumIndexedBlobs() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->Stats().Blobs;
    }

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const override;

    void Dump(IOutputStream& out) const override {
        out << "columns: " << GetSchemaColumnsCount()
            << " index records: " << NumIndexedRecords()
            << " index blobs: " << NumIndexedBlobs()
            << " committed blobs: " << CommittedBlobs.size()
      //      << " with program steps: " << (Program ? Program->Steps.size() : 0)
            << " at snapshot: " << Snapshot.GetPlanStep() << ":" << Snapshot.GetTxId();
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
    THashMap<ui64, std::shared_ptr<NOlap::TColumnEngineStats>> IndexStats;

    explicit TReadStatsMetadata(ui64 tabletId, const ESorting sorting, const TProgramContainer& ssaProgram)
        : TBase(sorting, ssaProgram)
        , TabletId(tabletId)
    {}

    std::vector<std::pair<TString, NScheme::TTypeInfo>> GetResultYqlSchema() const override;

    std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const override;
};

}
