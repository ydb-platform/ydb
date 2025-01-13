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

    virtual TConclusion<std::shared_ptr<TPartialReadResult>> GetBatch() override {
        while (!Finished()) {
            if (!IsReadyForBatch()) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "batch_not_ready");
                return std::shared_ptr<TPartialReadResult>();
            }
            auto batchOpt = ExtractStatsBatch();
            if (!batchOpt) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "no_batch_on_finished");
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
            auto applyConclusion = ReadMetadata->GetProgram().ApplyProgram(resultBatch);
            if (!applyConclusion.ok()) {
                return TConclusionStatus::Fail(applyConclusion.ToString());
            }
            if (resultBatch->num_rows() == 0) {
                continue;
            }
            auto table = NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({resultBatch}));
            return std::make_shared<TPartialReadResult>(table, std::make_shared<TPlainScanCursor>(lastKey), Context, std::nullopt);
        }
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_SCAN)("event", "finished_iterator");
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
            return arrow::RecordBatch::Make(DataSchema, columns.front()->length(), columns);
        }
        return std::nullopt;
    }


    TStatsIteratorBase(const std::shared_ptr<NReader::TReadContext>& context, const NTable::TScheme::TTableSchema& statsSchema);
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

        NSsa::TColumnInfo GetDefaultColumn() const override {
            return NSsa::TColumnInfo::Original(1, "PathId");
        }
    };

    TStatsIterator(const std::shared_ptr<NReader::TReadContext>& context)
        : TBase(context, StatsSchema)
    {
    }

};

}
