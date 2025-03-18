#pragma once

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/columnshard/engines/reader/duplicates/subscriber.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NOlap::NReader {

class TBuildDuplicateFilters: public NConveyor::ITask {
    class TFiltersBuilder: public NArrow::NMerger::IMergeResultBuilder {
    private:
        std::vector<NArrow::TColumnFilter> Filters;

    private:
        virtual void AddRecord(const NArrow::NMerger::TBatchIterator& cursor) override {
            AFL_VERIFY(cursor.GetSourceId() < Filters.size())("id", cursor.GetSourceId())("size", Filters.size());
            Filters[cursor.GetSourceId()].Add(true);
        }

        virtual void SkipRecord(const NArrow::NMerger::TBatchIterator& cursor) override {
            AFL_VERIFY(cursor.GetSourceId() < Filters.size())("id", cursor.GetSourceId())("size", Filters.size());
            Filters[cursor.GetSourceId()].Add(false);
        }

        virtual void ValidateDataSchema(const std::shared_ptr<arrow::Schema>& /*schema*/) const override {
        }

        virtual bool IsBufferExhausted() const override {
            return false;
        }

    public:
        TFiltersBuilder(const ui32 numSources) {
            Filters.reserve(numSources);
            for (ui32 i = 0; i < numSources; ++i) {
                Filters.emplace_back(NArrow::TColumnFilter::BuildAllowFilter());
            }
        }

        std::vector<NArrow::TColumnFilter>&& ExtractFilters() && {
            return std::move(Filters);
        }
    };

    class TSourceMergingInfo {
    private:
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Data);
        YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
        YDB_READONLY_DEF(ui32, SourceIdx);

    public:
        TSourceMergingInfo(
            const std::shared_ptr<NArrow::TGeneralContainer>& data, const std::shared_ptr<NArrow::TColumnFilter>& filter, const ui32 sourceIdx)
            : Data(data)
            , Filter(filter)
            , SourceIdx(sourceIdx) {
        }
    };

private:
    std::vector<TSourceMergingInfo> Sources;
    std::shared_ptr<arrow::Schema> PKSchema;
    std::vector<std::string> VersionColumnNames;
    ui32 IntervalIdx;
    TActorId Owner;

private:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(const std::shared_ptr<arrow::Schema>& pkSchema, const std::vector<std::string>& versionColumnNames,
        const ui32 intervalIdx, const TActorId& owner)
        : PKSchema(pkSchema)
        , VersionColumnNames(versionColumnNames)
        , IntervalIdx(intervalIdx)
        , Owner(owner) {
    }

    void AddSource(
        const std::shared_ptr<NArrow::TGeneralContainer>& source, const std::shared_ptr<NArrow::TColumnFilter>& filter, const ui32 sourceIdx) {
        Sources.emplace_back(source, filter, sourceIdx);
    }
};

}   // namespace NKikimr::NOlap::NReader
