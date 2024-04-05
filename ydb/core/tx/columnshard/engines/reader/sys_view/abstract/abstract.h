#pragma once
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

class TGranuleMetaView {
private:
    using TPortions = std::deque<std::shared_ptr<TPortionInfo>>;
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY_DEF(TPortions, Portions);
public:
    TGranuleMetaView(const TGranuleMeta& granule, const bool reverse)
        : PathId(granule.GetPathId())
    {
        for (auto&& i : granule.GetPortions()) {
            Portions.emplace_back(i.second);
        }

        const auto predSort = [](const std::shared_ptr<TPortionInfo>& l, const std::shared_ptr<TPortionInfo>& r) {
            return l->GetPortionId() < r->GetPortionId();
        };

        std::sort(Portions.begin(), Portions.end(), predSort);
        if (reverse) {
            std::reverse(Portions.begin(), Portions.end());
        }
    }

    bool operator<(const TGranuleMetaView& item) const {
        return PathId < item.PathId;
    }

    std::shared_ptr<TPortionInfo> PopFrontPortion() {
        if (Portions.empty()) {
            return nullptr;
        }
        auto result = Portions.front();
        Portions.pop_front();
        return result;
    }
};

struct TReadStatsMetadata: public TReadMetadataBase {
private:
    using TBase = TReadMetadataBase;
public:
    using TConstPtr = std::shared_ptr<const TReadStatsMetadata>;

    const ui64 TabletId;
    std::vector<ui32> ReadColumnIds;
    std::vector<ui32> ResultColumnIds;
    std::deque<TGranuleMetaView> IndexGranules;

    explicit TReadStatsMetadata(const std::shared_ptr<TVersionedIndex>& info, ui64 tabletId, const ESorting sorting,
        const TProgramContainer& ssaProgram, const std::shared_ptr<ISnapshotSchema>& schema, const TSnapshot& requestSnapshot)
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
        , KeySchema(MakeArrowSchema(StatsSchema.Columns, StatsSchema.KeyColumns))
        , ResultSchema(MakeArrowSchema(StatsSchema.Columns, ReadMetadata->ResultColumnIds))
        , IndexGranules(ReadMetadata->IndexGranules)
    {
        if (ResultSchema->num_fields() == 0) {
            ResultSchema = KeySchema;
        }
    }

    bool Finished() const override {
        return IndexGranules.empty();
    }
protected:
    virtual void AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, TGranuleMetaView& granule) const = 0;
    virtual ui32 PredictRecordsCount(const TGranuleMetaView& granule) const = 0;
    TReadStatsMetadata::TConstPtr ReadMetadata;
    const bool Reverse = false;
    std::shared_ptr<arrow::Schema> KeySchema;
    std::shared_ptr<arrow::Schema> ResultSchema;

    std::deque<TGranuleMetaView> IndexGranules;

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
        if (!resultBatch->num_rows()) {
            return std::nullopt;
        }
        auto table = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({resultBatch}));
        NArrow::TStatusValidator::Validate(ReadMetadata->GetProgram().ApplyProgram(table));
        if (!table->num_rows()) {
            return std::nullopt;
        }
        TPartialReadResult out(table, lastKey, std::nullopt);

        return std::move(out);
    }

    std::shared_ptr<arrow::RecordBatch> FillStatsBatch() {
        std::vector<ui32> allColumnIds;
        for (const auto& c : StatsSchema.Columns) {
            allColumnIds.push_back(c.second.Id);
        }
        std::sort(allColumnIds.begin(), allColumnIds.end());
        auto schema = MakeArrowSchema(StatsSchema.Columns, allColumnIds);

        std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
        if (IndexGranules.size()) {
            builders = NArrow::MakeBuilders(schema, PredictRecordsCount(IndexGranules.front()));
            AppendStats(builders, IndexGranules.front());
            if (IndexGranules.front().GetPortions().empty()) {
                IndexGranules.pop_front();
            }
        } else {
            builders = NArrow::MakeBuilders(schema);
        }
        auto columns = NArrow::Finish(std::move(builders));
        AFL_VERIFY(columns.size());
        std::optional<ui32> count;
        for (auto&& i : columns) {
            if (!count) {
                count = i->length();
            } else {
                AFL_VERIFY(*count == i->length());
            }
        }
        return arrow::RecordBatch::Make(schema, columns.front()->length(), columns);
    }

    void ApplyRangePredicates(std::shared_ptr<arrow::RecordBatch>& batch) {
        NArrow::TColumnFilter filter = ReadMetadata->GetPKRangesFilter().BuildFilter(batch);
        filter.Apply(batch);
    }
};

}
