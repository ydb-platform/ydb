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
    TReadStatsMetadata::TConstPtr ReadMetadata;
    const bool Reverse = false;
    std::shared_ptr<arrow::Schema> KeySchema;
    std::shared_ptr<arrow::Schema> ResultSchema;

    std::deque<TGranuleMetaView> IndexGranules;
public:
    virtual TConclusionStatus Start() override {
        return TConclusionStatus::Success();
    }

    virtual bool Finished() const override {
        return IndexGranules.empty();
    }

    virtual TConclusion<std::shared_ptr<TPartialReadResult>> GetBatch() override {
        while (!Finished()) {
            auto batchOpt = ExtractStatsBatch();
            if (!batchOpt) {
                AFL_VERIFY(Finished());
                return std::shared_ptr<TPartialReadResult>();
            }
            auto originalBatch = *batchOpt;
            if (originalBatch->num_rows() == 0) {
                continue;
            }
            auto keyBatch = NArrow::TColumnOperator().VerifyIfAbsent().Adapt(originalBatch, KeySchema).DetachResult();
            auto lastKey = keyBatch->Slice(keyBatch->num_rows() - 1, 1);

            {
                NArrow::TColumnFilter filter = ReadMetadata->GetPKRangesFilter().BuildFilter(originalBatch);
                filter.Apply(originalBatch);
            }

            // Leave only requested columns
            auto resultBatch = NArrow::TColumnOperator().Adapt(originalBatch, ResultSchema).DetachResult();
            NArrow::TStatusValidator::Validate(ReadMetadata->GetProgram().ApplyProgram(resultBatch));
            if (resultBatch->num_rows() == 0) {
                continue;
            }
            auto table = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({resultBatch}));
            return std::make_shared<TPartialReadResult>(table, lastKey, std::nullopt);
        }
        return std::shared_ptr<TPartialReadResult>();
    }

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
            auto result = arrow::RecordBatch::Make(DataSchema, columns.front()->length(), columns);
            if (result->num_rows()) {
                return result;
            }
        }
        return std::nullopt;
    }


    TStatsIteratorBase(const NAbstract::TReadStatsMetadata::TConstPtr& readMetadata, const NTable::TScheme::TTableSchema& statsSchema)
        : StatsSchema(statsSchema)
        , ReadMetadata(readMetadata)
        , KeySchema(MakeArrowSchema(StatsSchema.Columns, StatsSchema.KeyColumns))
        , ResultSchema(MakeArrowSchema(StatsSchema.Columns, ReadMetadata->ResultColumnIds))
        , IndexGranules(ReadMetadata->IndexGranules)
    {
        if (ResultSchema->num_fields() == 0) {
            ResultSchema = KeySchema;
        }
        std::vector<ui32> allColumnIds;
        for (const auto& c : StatsSchema.Columns) {
            allColumnIds.push_back(c.second.Id);
        }
        std::sort(allColumnIds.begin(), allColumnIds.end());
        DataSchema = MakeArrowSchema(StatsSchema.Columns, allColumnIds);
    }
};

template <class TSysViewSchema>
class TStatsIterator : public TStatsIteratorBase {
private:
    using TBase = TStatsIteratorBase;
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
        : TBase(readMetadata, StatsSchema)
    {
    }

};

}
