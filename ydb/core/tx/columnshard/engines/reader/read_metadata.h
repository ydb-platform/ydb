#pragma once
#include "conveyor_task.h"
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/counters.h>
#include <ydb/core/tx/columnshard/columnshard__scan.h>
#include <ydb/core/tx/columnshard/engines/predicate.h>
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

// Holds all metadata that is needed to perform read/scan
struct TReadMetadataBase {
    using TConstPtr = std::shared_ptr<const TReadMetadataBase>;

    enum class ESorting {
        NONE = 0,
        ASC,
        DESC,
    };

    virtual ~TReadMetadataBase() = default;

    std::shared_ptr<NOlap::TPredicate> LessPredicate;
    std::shared_ptr<NOlap::TPredicate> GreaterPredicate;
    std::shared_ptr<arrow::Schema> BlobSchema;
    std::shared_ptr<arrow::Schema> LoadSchema; // ResultSchema + required for intermediate operations
    std::shared_ptr<arrow::Schema> ResultSchema; // TODO: add Program modifications
    std::shared_ptr<NSsa::TProgram> Program;
    std::shared_ptr<const THashSet<TUnifiedBlobId>> ExternBlobs;
    ESorting Sorting{ESorting::ASC}; // Sorting inside returned batches
    ui64 Limit{0}; // TODO

    bool IsAscSorted() const { return Sorting == ESorting::ASC; }
    bool IsDescSorted() const { return Sorting == ESorting::DESC; }
    bool IsSorted() const { return IsAscSorted() || IsDescSorted(); }
    void SetDescSorting() { Sorting = ESorting::DESC; }

    virtual TVector<std::pair<TString, NScheme::TTypeInfo>> GetResultYqlSchema() const = 0;
    virtual TVector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const = 0;
    virtual std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const = 0;
    virtual void Dump(IOutputStream& out) const { Y_UNUSED(out); };

    // TODO:  can this only be done for base class?
    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadataBase& meta) {
        meta.Dump(out);
        return out;
    }
};

// Holds all metadata that is needed to perform read/scan
struct TReadMetadata : public TReadMetadataBase, public std::enable_shared_from_this<TReadMetadata> {
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    TIndexInfo IndexInfo;
    ui64 PlanStep = 0;
    ui64 TxId = 0;
    std::shared_ptr<TSelectInfo> SelectInfo;
    std::vector<TCommittedBlob> CommittedBlobs;
    THashMap<TUnifiedBlobId, std::shared_ptr<arrow::RecordBatch>> CommittedBatches;
    std::shared_ptr<TReadStats> ReadStats;

    std::shared_ptr<NIndexedReader::IOrderPolicy> BuildSortingPolicy() const;

    TReadMetadata(const TIndexInfo& info)
        : IndexInfo(info)
        , ReadStats(std::make_shared<TReadStats>(info.GetId()))
    {}

    std::vector<std::string> GetColumnsOrder() const {
        std::vector<std::string> result;
        for (auto&& i : LoadSchema->fields()) {
            result.emplace_back(i->name());
        }
        return result;
    }

    std::set<ui32> GetEarlyFilterColumnIds() const;
    std::set<ui32> GetUsedColumnIds() const;

    bool Empty() const {
        Y_VERIFY(SelectInfo);
        return SelectInfo->Portions.empty() && CommittedBlobs.empty();
    }

    std::shared_ptr<arrow::Schema> GetSortingKey() const {
        return IndexInfo.GetSortingKey();
    }

    std::shared_ptr<arrow::Schema> GetReplaceKey() const {
        return IndexInfo.GetReplaceKey();
    }

    TVector<TNameTypeInfo> GetResultYqlSchema() const override {
        TVector<NTable::TTag> columnIds;
        columnIds.reserve(ResultSchema->num_fields());
        for (const auto& field: ResultSchema->fields()) {
            TString name = TStringBuilder() << field->name();
            columnIds.emplace_back(IndexInfo.GetColumnId(name));
        }
        return IndexInfo.GetColumns(columnIds);
    }

    TVector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return IndexInfo.GetPrimaryKey();
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
        out << "columns: " << (LoadSchema ? LoadSchema->num_fields() : 0)
            << " index records: " << NumIndexedRecords()
            << " index blobs: " << NumIndexedBlobs()
            << " committed blobs: " << CommittedBlobs.size()
            << " with program steps: " << (Program ? Program->Steps.size() : 0)
            << (Sorting == ESorting::NONE ? " not" : (Sorting == ESorting::ASC ? " asc" : " desc"))
            << " sorted, at snapshot: " << PlanStep << ":" << TxId;
        if (GreaterPredicate) {
            out << " from{" << *GreaterPredicate << "}";
        }
        if (LessPredicate) {
            out << " to{" << *LessPredicate << "}";
        }
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
    using TConstPtr = std::shared_ptr<const TReadStatsMetadata>;

    const ui64 TabletId;
    TVector<ui32> ReadColumnIds;
    TVector<ui32> ResultColumnIds;
    THashMap<ui64, std::shared_ptr<NOlap::TColumnEngineStats>> IndexStats;

    explicit TReadStatsMetadata(ui64 tabletId)
        : TabletId(tabletId)
    {}

    TVector<std::pair<TString, NScheme::TTypeInfo>> GetResultYqlSchema() const override;

    TVector<std::pair<TString, NScheme::TTypeInfo>> GetKeyYqlSchema() const override;

    std::unique_ptr<NColumnShard::TScanIteratorBase> StartScan(NColumnShard::TDataTasksProcessorContainer tasksProcessor, const NColumnShard::TScanCounters& scanCounters) const override;
};

}
