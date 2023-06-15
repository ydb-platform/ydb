#pragma once

#include "columnshard__scan.h"
#include "columnshard_common.h"
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/formats/custom_registry.h>

namespace NKikimr::NColumnShard {

static const NTable::TScheme::TTableSchema PrimaryIndexStatsSchema = []() {
    NTable::TScheme::TTableSchema schema;
    NIceDb::NHelpers::TStaticSchemaFiller<NKikimr::NSysView::Schema::PrimaryIndexStats>::Fill(schema);
    return schema;
}();


class TStatsColumnResolver : public IColumnResolver {
public:
    TString GetColumnName(ui32 id, bool required) const override {
        auto it = PrimaryIndexStatsSchema.Columns.find(id);
        if (it == PrimaryIndexStatsSchema.Columns.end()) {
            Y_VERIFY(!required, "No column '%" PRIu32 "' in primary_index_stats", id);
            return {};
        }
        return it->second.Name;
    }

    const NTable::TScheme::TTableSchema& GetSchema() const override {
        return PrimaryIndexStatsSchema;
    }
};


class TStatsIterator : public TScanIteratorBase {
public:
    TStatsIterator(const NOlap::TReadStatsMetadata::TConstPtr& readMetadata)
        : ReadMetadata(readMetadata)
        , Reverse(ReadMetadata->IsDescSorted())
        , KeySchema(NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, PrimaryIndexStatsSchema.KeyColumns))
        , ResultSchema(NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, ReadMetadata->ResultColumnIds))
        , IndexStats(ReadMetadata->IndexStats.begin(), ReadMetadata->IndexStats.end())
    {
    }

    bool Finished() const override {
        return IndexStats.empty();
    }

    NOlap::TPartialReadResult GetBatch() override {
        // Take next raw batch
        auto batch = FillStatsBatch();

        // Extract the last row's PK
        auto keyBatch = NArrow::ExtractColumns(batch, KeySchema);
        auto lastKey = keyBatch->Slice(keyBatch->num_rows()-1, 1);

        ApplyRangePredicates(batch);

        // Leave only requested columns
        auto resultBatch = NArrow::ExtractColumns(batch, ResultSchema);

        NOlap::TPartialReadResult out{
            .ResultBatch = std::move(resultBatch),
            .LastReadKey = std::move(lastKey)
        };

        if (ReadMetadata->Program) {
            auto status = ApplyProgram(out.ResultBatch, *ReadMetadata->Program, NArrow::GetCustomExecContext());
            if (!status.ok()) {
                out.ErrorString = status.message();
            }
        }
        return out;
    }

    size_t ReadyResultsCount() const override {
        return IndexStats.empty() ? 0 : 1;
    }

private:
    NOlap::TReadStatsMetadata::TConstPtr ReadMetadata;
    bool Reverse{false};
    std::shared_ptr<arrow::Schema> KeySchema;
    std::shared_ptr<arrow::Schema> ResultSchema;

    TMap<ui64, std::shared_ptr<NOlap::TColumnEngineStats>> IndexStats;

    static constexpr const ui64 NUM_KINDS = 5;
    static_assert(NUM_KINDS == NOlap::TPortionMeta::EVICTED, "NUM_KINDS must match NOlap::TPortionMeta::EProduced enum");

    std::shared_ptr<arrow::RecordBatch> FillStatsBatch() {

        ui64 numRows = 0;
        numRows += NUM_KINDS * IndexStats.size();

        TVector<ui32> allColumnIds;
        for (const auto& c : PrimaryIndexStatsSchema.Columns) {
            allColumnIds.push_back(c.second.Id);
        }
        std::sort(allColumnIds.begin(), allColumnIds.end());
        auto schema = NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, allColumnIds);
        auto builders = NArrow::MakeBuilders(schema, numRows);

        while (!IndexStats.empty()) {
            auto it = Reverse ? std::prev(IndexStats.end()) : IndexStats.begin();
            const auto& stats = it->second;
            Y_VERIFY(stats);
            AppendStats(builders, it->first, *stats);
            IndexStats.erase(it);
        }

        auto columns = NArrow::Finish(std::move(builders));
        return arrow::RecordBatch::Make(schema, numRows, columns);
    }

    void ApplyRangePredicates(std::shared_ptr<arrow::RecordBatch>& batch) {
        std::vector<bool> less;
        if (ReadMetadata->LessPredicate) {
            auto cmpType = ReadMetadata->LessPredicate->Inclusive ?
                NArrow::ECompareType::LESS_OR_EQUAL : NArrow::ECompareType::LESS;
            less = NArrow::MakePredicateFilter(batch, ReadMetadata->LessPredicate->Batch, cmpType);
        }

        std::vector<bool> greater;
        if (ReadMetadata->GreaterPredicate) {
            auto cmpType = ReadMetadata->GreaterPredicate->Inclusive ?
                NArrow::ECompareType::GREATER_OR_EQUAL : NArrow::ECompareType::GREATER;
            greater = NArrow::MakePredicateFilter(batch, ReadMetadata->GreaterPredicate->Batch, cmpType);
        }

        std::vector<bool> bits = NArrow::CombineFilters(std::move(less), std::move(greater));
        if (bits.size()) {
            auto res = arrow::compute::Filter(batch, NArrow::MakeFilter(bits));
            Y_VERIFY_S(res.ok(), res.status().message());
            Y_VERIFY((*res).kind() == arrow::Datum::RECORD_BATCH);
            batch = (*res).record_batch();
        }
    }

    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders,
                     ui64 pathId, const NOlap::TColumnEngineStats& stats) {
        using TUInt64 = arrow::UInt64Type::c_type;
        using TUInt32 = arrow::UInt32Type::c_type;

        TUInt64 pathIds[NUM_KINDS] = {pathId, pathId, pathId, pathId, pathId};
        /// It's in sync with TPortionMeta::EProduced
        TUInt32 kinds[NUM_KINDS] = {
            (ui32)NOlap::TPortionMeta::INSERTED,
            (ui32)NOlap::TPortionMeta::COMPACTED,
            (ui32)NOlap::TPortionMeta::SPLIT_COMPACTED,
            (ui32)NOlap::TPortionMeta::INACTIVE,
            (ui32)NOlap::TPortionMeta::EVICTED
        };
        ui64 tabletId = ReadMetadata->TabletId;
        TUInt64 tabletIds[NUM_KINDS] = {tabletId, tabletId, tabletId, tabletId, tabletId};
        TUInt64 rows[NUM_KINDS] = {
            (ui64)stats.Inserted.Rows,
            (ui64)stats.Compacted.Rows,
            (ui64)stats.SplitCompacted.Rows,
            (ui64)stats.Inactive.Rows,
            (ui64)stats.Evicted.Rows
        };
        TUInt64 bytes[NUM_KINDS] = {
            (ui64)stats.Inserted.Bytes,
            (ui64)stats.Compacted.Bytes,
            (ui64)stats.SplitCompacted.Bytes,
            (ui64)stats.Inactive.Bytes,
            (ui64)stats.Evicted.Bytes
        };
        TUInt64 rawBytes[NUM_KINDS] = {
            (ui64)stats.Inserted.RawBytes,
            (ui64)stats.Compacted.RawBytes,
            (ui64)stats.SplitCompacted.RawBytes,
            (ui64)stats.Inactive.RawBytes,
            (ui64)stats.Evicted.RawBytes
        };
        TUInt64 portions[NUM_KINDS] = {
            (ui64)stats.Inserted.Portions,
            (ui64)stats.Compacted.Portions,
            (ui64)stats.SplitCompacted.Portions,
            (ui64)stats.Inactive.Portions,
            (ui64)stats.Evicted.Portions
        };
        TUInt64 blobs[NUM_KINDS] = {
            (ui64)stats.Inserted.Blobs,
            (ui64)stats.Compacted.Blobs,
            (ui64)stats.SplitCompacted.Blobs,
            (ui64)stats.Inactive.Blobs,
            (ui64)stats.Evicted.Blobs
        };

        if (Reverse) {
            std::reverse(std::begin(pathIds),   std::end(pathIds));
            std::reverse(std::begin(kinds),     std::end(kinds));
            std::reverse(std::begin(tabletIds), std::end(tabletIds));
            std::reverse(std::begin(rows),      std::end(rows));
            std::reverse(std::begin(bytes),     std::end(bytes));
            std::reverse(std::begin(rawBytes),  std::end(rawBytes));
            std::reverse(std::begin(portions),  std::end(portions));
            std::reverse(std::begin(blobs),     std::end(blobs));
        }

        NArrow::Append<arrow::UInt64Type>(*builders[0], pathIds,    NUM_KINDS);
        NArrow::Append<arrow::UInt32Type>(*builders[1], kinds,      NUM_KINDS);
        NArrow::Append<arrow::UInt64Type>(*builders[2], tabletIds,  NUM_KINDS);
        NArrow::Append<arrow::UInt64Type>(*builders[3], rows,       NUM_KINDS);
        NArrow::Append<arrow::UInt64Type>(*builders[4], bytes,      NUM_KINDS);
        NArrow::Append<arrow::UInt64Type>(*builders[5], rawBytes,   NUM_KINDS);
        NArrow::Append<arrow::UInt64Type>(*builders[6], portions,   NUM_KINDS);
        NArrow::Append<arrow::UInt64Type>(*builders[7], blobs,      NUM_KINDS);
    }
};

}
