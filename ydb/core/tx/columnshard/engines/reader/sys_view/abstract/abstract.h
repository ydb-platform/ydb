#pragma once
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

struct TReadStatsMetadata: public TReadMetadataBase {
private:
    using TBase = TReadMetadataBase;
public:
    using TConstPtr = std::shared_ptr<const TReadStatsMetadata>;

    const ui64 TabletId;
    std::vector<ui32> ReadColumnIds;
    std::vector<ui32> ResultColumnIds;
    std::deque<std::shared_ptr<TPortionInfo>> IndexPortions;

    explicit TReadStatsMetadata(const std::shared_ptr<TVersionedIndex>& info, ui64 tabletId, const ESorting sorting, const TProgramContainer& ssaProgram, const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot)
        : TBase(info, sorting, ssaProgram, schema, requestSnapshot)
        , TabletId(tabletId) {
    }
};

template <class TSysViewSchema>
class TStatsIterator : public TScanIteratorBase {
public:
    static inline const NTable::TScheme::TTableSchema StatsSchema = []() {
        NTable::TScheme::TTableSchema schema;
        NIceDb::NHelpers::TStaticSchemaFiller<TSysViewSchema>::Fill(schema);
        return schema;
    }();

    class TStatsColumnResolver: public IColumnResolver {
    public:
        TString GetColumnName(ui32 id, bool required) const override {
            auto it = StatsSchema.Columns.find(id);
            if (it == StatsSchema.Columns.end()) {
                Y_ABORT_UNLESS(!required, "No column '%" PRIu32 "' in primary_index_stats", id);
                return {};
            }
            return it->second.Name;
        }

        std::optional<ui32> GetColumnIdOptional(const TString& name) const override {
            auto it = StatsSchema.ColumnNames.find(name);
            if (it == StatsSchema.ColumnNames.end()) {
                return {};
            } else {
                return it->second;
            }
        }

        const NTable::TScheme::TTableSchema& GetSchema() const override {
            return StatsSchema;
        }

        NSsa::TColumnInfo GetDefaultColumn() const override {
            return NSsa::TColumnInfo::Original(1, "PathId");
        }
    };

    TStatsIterator(const NAbstract::TReadStatsMetadata::TConstPtr& readMetadata)
        : ReadMetadata(readMetadata)
        , Reverse(ReadMetadata->IsDescSorted())
        , KeySchema(MakeArrowSchema(StatsSchema.Columns, StatsSchema.KeyColumns))
        , ResultSchema(MakeArrowSchema(StatsSchema.Columns, ReadMetadata->ResultColumnIds))
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
protected:
    virtual void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, const TPortionInfo& portion) const = 0;
    virtual ui32 GetConstructionRecordsCount(const TPortionInfo& portion) const = 0;
    TReadStatsMetadata::TConstPtr ReadMetadata;
    const bool Reverse = false;
    std::shared_ptr<arrow::Schema> KeySchema;
    std::shared_ptr<arrow::Schema> ResultSchema;

    std::deque<std::shared_ptr<TPortionInfo>> IndexPortions;

    virtual TConclusion<std::optional<TPartialReadResult>> GetBatch() override {
        // Take next raw batch
        auto batch = FillStatsBatch();

        // Extract the last row's PK
        auto keyBatch = NArrow::ExtractColumns(batch, KeySchema);
        auto lastKey = keyBatch->Slice(keyBatch->num_rows() - 1, 1);

        ApplyRangePredicates(batch);
        if (!batch->num_rows()) {
            return std::nullopt;
        }
        // Leave only requested columns
        auto resultBatch = NArrow::ExtractColumns(batch, ResultSchema);
        NArrow::TStatusValidator::Validate(ReadMetadata->GetProgram().ApplyProgram(resultBatch));
        if (!resultBatch->num_rows()) {
            return std::nullopt;
        }
        TPartialReadResult out(resultBatch, lastKey);

        return std::move(out);
    }

    std::shared_ptr<arrow::RecordBatch> FillStatsBatch() {
        std::vector<std::shared_ptr<TPortionInfo>> portions;
        ui32 recordsCount = 0;
        while (IndexPortions.size()) {
            auto& i = IndexPortions.front();
            recordsCount += GetConstructionRecordsCount(*i);
            portions.emplace_back(i);
            IndexPortions.pop_front();
            if (recordsCount > 10000) {
                break;
            }
        }
        std::vector<ui32> allColumnIds;
        for (const auto& c : StatsSchema.Columns) {
            allColumnIds.push_back(c.second.Id);
        }
        std::sort(allColumnIds.begin(), allColumnIds.end());
        auto schema = MakeArrowSchema(StatsSchema.Columns, allColumnIds);
        auto builders = NArrow::MakeBuilders(schema, recordsCount);

        for (auto&& p : portions) {
            AppendStats(builders, *p);
        }

        auto columns = NArrow::Finish(std::move(builders));
        return arrow::RecordBatch::Make(schema, recordsCount, columns);
    }

    void ApplyRangePredicates(std::shared_ptr<arrow::RecordBatch>& batch) {
        NArrow::TColumnFilter filter = ReadMetadata->GetPKRangesFilter().BuildFilter(batch);
        filter.Apply(batch);
    }
};

}
