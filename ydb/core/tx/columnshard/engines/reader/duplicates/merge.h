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
        YDB_READONLY_DEF(std::shared_ptr<IFilterSubscriber>, Callback);

    public:
        TSourceMergingInfo(const std::shared_ptr<NArrow::TGeneralContainer>& data, const std::shared_ptr<NArrow::TColumnFilter>& filter,
            const std::shared_ptr<IFilterSubscriber>& callback)
            : Data(data)
            , Filter(filter)
            , Callback(callback) {
        }
    };

private:
    std::vector<TSourceMergingInfo> Sources;
    std::shared_ptr<arrow::Schema> PKSchema;
    std::vector<std::string> VersionColumnNames;

private:
    virtual TConclusionStatus DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override {
        NArrow::NMerger::TMergePartialStream merger(PKSchema, nullptr, false, VersionColumnNames);
        for (ui64 i = 0; i < Sources.size(); ++i) {
            const auto& source = Sources[i];
            merger.AddSource(source.GetData(), source.GetFilter(), i);
        }
        TFiltersBuilder filtersBuilder(Sources.size());
        merger.DrainAll(filtersBuilder);
        std::vector<NArrow::TColumnFilter> filters = std::move(filtersBuilder).ExtractFilters();
        AFL_VERIFY(filters.size() == Sources.size());
        for (ui64 i = 0; i < filters.size(); ++i) {
            Sources[i].GetCallback()->OnFilterReady(filters[i]);
        }
        return TConclusionStatus::Success();
    }

    virtual void DoOnCannotExecute(const TString& reason) override {
        for (auto& source : Sources) {
            source.GetCallback()->OnFailure(reason);
        }
    }

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(const std::shared_ptr<arrow::Schema>& pkSchema, const std::vector<std::string>& versionColumnNames)
        : PKSchema(pkSchema)
        , VersionColumnNames(versionColumnNames) {
    }

    void AddSource(const std::shared_ptr<NArrow::TGeneralContainer>& source, const std::shared_ptr<NArrow::TColumnFilter>& filter,
        const std::shared_ptr<IFilterSubscriber>& callback) {
        Sources.emplace_back(source, filter, callback);
    }
};

}   // namespace NKikimr::NOlap::NReader
