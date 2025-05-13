#pragma once

#include "events.h"

#include <ydb/core/formats/arrow/reader/merger.h>
#include <ydb/core/tx/columnshard/engines/reader/common_reader/iterator/source.h>
#include <ydb/core/tx/conveyor/usage/abstract.h>

namespace NKikimr::NOlap::NReader::NSimple {

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

public:
    class ISubscriber {
    public:
        virtual void OnResult(THashMap<ui64, NArrow::TColumnFilter>&& result) = 0;
        virtual void OnFailure(const TString& error) = 0;
        virtual ~ISubscriber() = default;
    };

private:
    THashMap<ui64, TSourceMergingInfo> SourcesById;
    std::shared_ptr<arrow::Schema> PKSchema;
    std::vector<std::string> VersionColumnNames;
    TActorId Owner;
    NColumnShard::TScanCounters Counters;
    std::optional<NArrow::NMerger::TCursor> MaxVersion;
    std::unique_ptr<ISubscriber> Callback;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(const std::shared_ptr<arrow::Schema>& pkSchema, const std::vector<std::string>& versionColumnNames,
        const NColumnShard::TScanCounters& counters, const std::optional<NArrow::NMerger::TCursor>& maxVersion,
        std::unique_ptr<ISubscriber>&& callback)
        : PKSchema(pkSchema)
        , VersionColumnNames(versionColumnNames)
        , Counters(counters)
        , MaxVersion(maxVersion)
        , Callback(std::move(callback)) {
        AFL_VERIFY(Callback);
    }

    void AddSource(
        const std::shared_ptr<NArrow::TGeneralContainer>& source, const std::shared_ptr<NArrow::TColumnFilter>& filter, const ui64 sourceId) {
        AFL_VERIFY(SourcesById.emplace(sourceId, TSourceMergingInfo(source, filter)).second);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
