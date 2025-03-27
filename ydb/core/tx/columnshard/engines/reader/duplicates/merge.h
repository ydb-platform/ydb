#pragma once

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/duplicates/subscriber.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NOlap::NReader {

class TBuildDuplicateFilters: public NConveyor::ITask {
    class TFiltersBuilder: public NArrow::NMerger::IMergeResultBuilder {
    private:
        THashMap<ui64, NArrow::TColumnFilter> Filters;
        YDB_READONLY(ui64, RowsAdded, 0);
        YDB_READONLY(ui64, RowsSkipped, 0);

        void AddImpl(const ui64 sourceId, const bool value) {
            auto* findFilter = Filters.FindPtr(sourceId);
            AFL_VERIFY(findFilter);
            findFilter->Add(value);
        }

    private:
        virtual void AddRecord(const NArrow::NMerger::TBatchIterator& cursor) override {
            AddImpl(cursor.GetSourceId(), true);
            ++RowsAdded;
        }

        virtual void SkipRecord(const NArrow::NMerger::TBatchIterator& cursor) override {
            AddImpl(cursor.GetSourceId(), false);
            ++RowsSkipped;
        }

        virtual void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const override {
        }

        virtual bool IsBufferExhausted() const override {
            return false;
        }

    public:
        THashMap<ui64, NArrow::TColumnFilter>&& ExtractFilters() && {
            return std::move(Filters);
        }

        void AddSource(const ui64 sourceId) {
            AFL_VERIFY(Filters.emplace(sourceId, NArrow::TColumnFilter::BuildAllowFilter()).second);
        }
    };

    class TSourceMergingInfo {
    private:
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Data);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);

    public:
        TSourceMergingInfo(const std::shared_ptr<NArrow::TGeneralContainer>& data, const std::shared_ptr<NArrow::TColumnFilter>& filter)
            : Data(data)
            , Filter(filter) {
        }
    };

private:
    THashMap<ui64, TSourceMergingInfo> SourcesById;
    std::shared_ptr<arrow::Schema> PKSchema;
    std::vector<std::string> VersionColumnNames;
    ui32 IntervalIdx;
    TActorId Owner;
    std::shared_ptr<NColumnShard::TScanCounters> Counters;

private:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(const std::shared_ptr<arrow::Schema>& pkSchema, const std::vector<std::string>& versionColumnNames,
        const ui32 intervalIdx, const TActorId& owner, const std::shared_ptr<NColumnShard::TScanCounters>& counters)
        : PKSchema(pkSchema)
        , VersionColumnNames(versionColumnNames)
        , IntervalIdx(intervalIdx)
        , Owner(owner), Counters(counters) {
    }

    void AddSource(
        const std::shared_ptr<NArrow::TGeneralContainer>& source, const std::shared_ptr<NArrow::TColumnFilter>& filter, const ui64 sourceId) {
        AFL_VERIFY(SourcesById.emplace(sourceId, TSourceMergingInfo(source, filter)).second);
    }
};

}   // namespace NKikimr::NOlap::NReader
