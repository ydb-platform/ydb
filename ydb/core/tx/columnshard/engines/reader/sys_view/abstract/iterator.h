#pragma once
#include "granule_view.h"
#include "metadata.h"

#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/abstract.h>

namespace NKikimr::NOlap::NReader::NSysView::NAbstract {

class TStatsIteratorBase: public TScanIteratorBase {
private:
    const NTable::TScheme::TTableSchema StatsSchema;
    std::shared_ptr<arrow::Schema> DataSchema;

protected:
    virtual bool AppendStats(const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders, TGranuleMetaView& granule) const = 0;
    virtual ui32 PredictRecordsCount(const TGranuleMetaView& granule) const = 0;
    std::shared_ptr<NReader::TReadContext> Context;
    TReadStatsMetadata::TConstPtr ReadMetadata;
    const bool Reverse = false;
    std::shared_ptr<arrow::Schema> KeySchema;
    std::shared_ptr<arrow::Schema> ResultSchema;

    std::deque<TGranuleMetaView> IndexGranules;
    mutable THashMap<ui64, TPortionDataAccessor> FetchedAccessors;

public:
    virtual bool IsReadyForBatch() const {
        return true;
    }

    virtual TConclusionStatus Start() override {
        return TConclusionStatus::Success();
    }

    virtual bool Finished() const override {
        return IndexGranules.empty();
    }

    virtual TConclusion<std::shared_ptr<TPartialReadResult>> GetBatch() override;

    std::optional<std::shared_ptr<arrow::RecordBatch>> ExtractStatsBatch() {
        while (IndexGranules.size()) {
            auto builders = NArrow::MakeBuilders(DataSchema, PredictRecordsCount(IndexGranules.front()));
            if (!AppendStats(builders, IndexGranules.front())) {
                IndexGranules.pop_front();
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
            return arrow::RecordBatch::Make(DataSchema, columns.front()->length(), columns);
        }
        return std::nullopt;
    }

    TStatsIteratorBase(const std::shared_ptr<NReader::TReadContext>& context, const NTable::TScheme::TTableSchema& statsSchema);
};

template <class TSysViewSchema>
class TStatsIterator: public TStatsIteratorBase {
private:
    using TBase = TStatsIteratorBase;

public:
    static inline const NTable::TScheme::TTableSchema StatsSchema = []() {
        NTable::TScheme::TTableSchema schema;
        NIceDb::NHelpers::TStaticSchemaFiller<TSysViewSchema>::Fill(schema);
        return schema;
    }();

    class TStatsColumnResolver: public NArrow::NSSA::IColumnResolver {
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

        NArrow::NSSA::TColumnInfo GetDefaultColumn() const override {
            return NArrow::NSSA::TColumnInfo::Original(1, "PathId");
        }
    };

    TStatsIterator(const std::shared_ptr<NReader::TReadContext>& context)
        : TBase(context, StatsSchema) {
    }
};

}   // namespace NKikimr::NOlap::NReader::NSysView::NAbstract
