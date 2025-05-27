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
        YDB_READONLY_DEF(std::shared_ptr<TColumnsData>, Batch);
        YDB_READONLY_DEF(ui64, Offset);

    public:
        TSourceMergingInfo(const std::shared_ptr<TColumnsData>& batch, const ui64 offset)
            : Batch(batch)
            , Offset(offset) {
            AFL_VERIFY(batch);
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
    NArrow::TSimpleRow Finish;
    bool IncludeFinish;
    std::unique_ptr<ISubscriber> Callback;

private:
    virtual void DoExecute(const std::shared_ptr<ITask>& /*taskPtr*/) override;
    virtual void DoOnCannotExecute(const TString& reason) override;

    virtual TString GetTaskClassIdentifier() const override {
        return "BUILD_DUPLICATE_FILTERS";
    }

public:
    TBuildDuplicateFilters(const std::shared_ptr<NCommon::TSpecialReadContext>& context,
        const std::optional<NArrow::NMerger::TCursor>& maxVersion, const NArrow::TSimpleRow& finish, const bool includeFinish,
        std::unique_ptr<ISubscriber>&& callback)
        : PKSchema(context->GetReadMetadata()->GetReplaceKey())
        , VersionColumnNames(IIndexInfo::GetSnapshotColumnNames())
        , Counters(context->GetCommonContext()->GetCounters())
        , MaxVersion(maxVersion)
        , Finish(finish)
        , IncludeFinish(includeFinish)
        , Callback(std::move(callback)) {
        AFL_VERIFY(Callback);
    }

    void AddSource(const std::shared_ptr<TColumnsData>& batch, const ui64 offset, const ui64 sourceId) {
        AFL_VERIFY(SourcesById.emplace(sourceId, TSourceMergingInfo(batch, offset)).second);
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple
