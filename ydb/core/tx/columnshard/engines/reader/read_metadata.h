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

    TReadMetadataBase(const ESorting sorting, const TProgramContainer& ssaProgram)
        : Sorting(sorting)
        , Program(ssaProgram)
    {
    }
    virtual ~TReadMetadataBase() = default;

    ui64 Limit{0}; // TODO

    virtual void Dump(IOutputStream& out) const {
        out << " predicate{" << (PKRangesFilter ? PKRangesFilter->DebugString() : "no_initialized") << "}"
            << " " << Sorting << " sorted";
    }

    bool IsAscSorted() const { return Sorting == ESorting::ASC; }
    bool IsDescSorted() const { return Sorting == ESorting::DESC; }
    bool IsSorted() const { return IsAscSorted() || IsDescSorted(); }

    virtual std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const = 0;
    virtual std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(const std::shared_ptr<NOlap::TReadContext>& readContext) const = 0;

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
public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    NIndexedReader::TSortableBatchPosition BuildSortedPosition(const NArrow::TReplaceKey& key) const;
    std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<NOlap::TReadContext>& context) const;

    bool HasProcessingColumnIds() const {
        return GetProgram().HasProcessingColumnIds();
    }

    std::set<ui32> GetProcessingColumnIds() const {
        std::set<ui32> result;
        for (auto&& i : GetProgram().GetProcessingColumns()) {
            result.emplace(ResultIndexSchema->GetIndexInfo().GetColumnId(i));
        }
        return result;
    }
    std::shared_ptr<TSelectInfo> SelectInfo;
    std::vector<TCommittedBlob> CommittedBlobs;
    std::shared_ptr<TReadStats> ReadStats;

    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }

    TReadMetadata(const TVersionedIndex& info, const TSnapshot& snapshot, const ESorting sorting, const TProgramContainer& ssaProgram)
        : TBase(sorting, ssaProgram)
        , IndexVersions(info)
        , Snapshot(snapshot)
        , ResultIndexSchema(info.GetSchema(Snapshot))
        , ReadStats(std::make_shared<TReadStats>(info.GetLastSchema()->GetIndexInfo().GetId()))
    {
    }

    bool Init(const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor, std::string& error);

    ISnapshotSchema::TPtr GetSnapshotSchema(const TSnapshot& version) const {
        if (version >= Snapshot){
            return ResultIndexSchema;
        }
        return IndexVersions.GetSchema(version);
    }

    ISnapshotSchema::TPtr GetLoadSchema(const std::optional<TSnapshot>& version = {}) const {
        if (!version) {
            return ResultIndexSchema;
        }
        return IndexVersions.GetSchema(*version);
    }

    std::shared_ptr<arrow::Schema> GetBlobSchema(const ui64 version) const {
        return IndexVersions.GetSchema(version)->GetIndexInfo().ArrowSchema();
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
    std::set<ui32> GetPKColumnIds() const;

    bool Empty() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->PortionsOrderedPK.empty() && CommittedBlobs.empty();
    }

    std::shared_ptr<arrow::Schema> GetSortingKey() const {
        return ResultIndexSchema->GetIndexInfo().GetSortingKey();
    }

    std::shared_ptr<arrow::Schema> GetReplaceKey() const {
        return ResultIndexSchema->GetIndexInfo().GetReplaceKey();
    }

    std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return ResultIndexSchema->GetIndexInfo().GetPrimaryKey();
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

    std::vector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(const std::shared_ptr<NOlap::TReadContext>& readContext) const override;
};

}
