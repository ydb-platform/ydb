#pragma once

#include "columnshard__scan.h"
#include "columnshard_common.h"
#include "engines/reader/read_metadata.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/core/formats/arrow/custom_registry.h>

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
            Y_ABORT_UNLESS(!required, "No column '%" PRIu32 "' in primary_index_stats", id);
            return {};
        }
        return it->second.Name;
    }

    std::optional<ui32> GetColumnIdOptional(const TString& name) const override {
        auto it = PrimaryIndexStatsSchema.ColumnNames.find(name);
        if (it == PrimaryIndexStatsSchema.ColumnNames.end()) {
            return {};
        } else {
            return it->second;
        }
    }

    const NTable::TScheme::TTableSchema& GetSchema() const override {
        return PrimaryIndexStatsSchema;
    }

    NSsa::TColumnInfo GetDefaultColumn() const override {
        return NSsa::TColumnInfo::Original(1, "PathId");
    }
};


class TStatsIterator : public TScanIteratorBase {
public:
    TStatsIterator(const NOlap::TReadStatsMetadata::TConstPtr& readMetadata)
        : ReadMetadata(readMetadata)
        , Reverse(ReadMetadata->IsDescSorted())
        , KeySchema(NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, PrimaryIndexStatsSchema.KeyColumns))
        , ResultSchema(NOlap::MakeArrowSchema(PrimaryIndexStatsSchema.Columns, ReadMetadata->ResultColumnIds))
        , IndexPortions(ReadMetadata->IndexPortions)
    {
        if (ResultSchema->num_fields() == 0) {
            ResultSchema = KeySchema;
        }
        if (Reverse) {
            std::reverse(IndexPortions.begin(), IndexPortions.end());
        }
    }

    bool Finished() const override {
        return IndexPortions.empty();
    }

    std::optional<NOlap::TPartialReadResult> GetBatch() override;

private:
    NOlap::TReadStatsMetadata::TConstPtr ReadMetadata;
    bool Reverse{false};
    std::shared_ptr<arrow::Schema> KeySchema;
    std::shared_ptr<arrow::Schema> ResultSchema;

    std::deque<std::shared_ptr<NOlap::TPortionInfo>> IndexPortions;

    std::shared_ptr<arrow::RecordBatch> FillStatsBatch();

    void ApplyRangePredicates(std::shared_ptr<arrow::RecordBatch>& batch);

    void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const NOlap::TPortionInfo& portion);
};

}
