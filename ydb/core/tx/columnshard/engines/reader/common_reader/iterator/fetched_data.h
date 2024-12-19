#pragma once
#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TFetchedData {
private:
    using TBlobs = THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>;
    YDB_ACCESSOR_DEF(TBlobs, Blobs);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Table);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    YDB_READONLY(bool, UseFilter, false);

    std::shared_ptr<NGroupedMemoryManager::TAllocationGuard> AccessorsGuard;
    std::optional<TPortionDataAccessor> PortionAccessor;
    bool DataAdded = false;

public:
    TString DebugString() const {
        return TStringBuilder() << DataAdded;
    }

    TFetchedData(const bool useFilter)
        : UseFilter(useFilter) {
    }

    void SetAccessorsGuard(std::shared_ptr<NGroupedMemoryManager::TAllocationGuard>&& guard) {
        AFL_VERIFY(!AccessorsGuard);
        AFL_VERIFY(!!guard);
        AccessorsGuard = std::move(guard);
    }

    void SetUseFilter(const bool value) {
        if (UseFilter == value) {
            return;
        }
        AFL_VERIFY(!DataAdded);
        UseFilter = value;
    }

    bool HasPortionAccessor() const {
        return !!PortionAccessor;
    }

    void SetPortionAccessor(TPortionDataAccessor&& accessor) {
        AFL_VERIFY(!PortionAccessor);
        PortionAccessor = std::move(accessor);
    }

    const TPortionDataAccessor& GetPortionAccessor() const {
        AFL_VERIFY(!!PortionAccessor);
        return *PortionAccessor;
    }

    ui32 GetFilteredCount(const ui32 recordsCount, const ui32 defLimit) const {
        if (!Filter) {
            return std::min(defLimit, recordsCount);
        }
        return Filter->GetFilteredCount().value_or(recordsCount);
    }

    void SyncTableColumns(const std::vector<std::shared_ptr<arrow::Field>>& fields, const ISnapshotSchema& schema);

    std::shared_ptr<NArrow::TColumnFilter> GetAppliedFilter() const {
        return UseFilter ? Filter : nullptr;
    }

    std::shared_ptr<NArrow::TColumnFilter> GetNotAppliedFilter() const {
        return UseFilter ? nullptr : Filter;
    }

    TString ExtractBlob(const TChunkAddress& address) {
        auto it = Blobs.find(address);
        AFL_VERIFY(it != Blobs.end());
        AFL_VERIFY(it->second.IsBlob());
        auto result = it->second.GetData();
        Blobs.erase(it);
        return result;
    }

    void AddBlobs(THashMap<TChunkAddress, TString>&& blobData) {
        for (auto&& i : blobData) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    void AddDefaults(THashMap<TChunkAddress, TPortionDataAccessor::TAssembleBlobInfo>&& blobs) {
        for (auto&& i : blobs) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    bool IsEmpty() const {
        return (Filter && Filter->IsTotalDenyFilter()) || (Table && !Table->num_rows());
    }

    void Clear() {
        Filter = std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildDenyFilter());
        Table = nullptr;
    }

    void AddFilter(const std::shared_ptr<NArrow::TColumnFilter>& filter) {
        DataAdded = true;
        if (!filter) {
            return;
        }
        return AddFilter(*filter);
    }

    void CutFilter(const ui32 recordsCount, const ui32 limit, const bool reverse) {
        auto filter = std::make_shared<NArrow::TColumnFilter>(NArrow::TColumnFilter::BuildAllowFilter());
        ui32 recordsCountImpl = Filter ? Filter->GetFilteredCount().value_or(recordsCount) : recordsCount;
        if (recordsCountImpl < limit) {
            return;
        }
        if (reverse) {
            filter->Add(false, recordsCountImpl - limit);
            filter->Add(true, limit);
        } else {
            filter->Add(true, limit);
            filter->Add(false, recordsCountImpl - limit);
        }
        if (Filter) {
            if (UseFilter) {
                AddFilter(*filter);
            } else {
                AddFilter(Filter->CombineSequentialAnd(*filter));
            }
        } else {
            AddFilter(*filter);
        }
    }

    void AddFilter(const NArrow::TColumnFilter& filter) {
        if (UseFilter && Table) {
            AFL_VERIFY(filter.Apply(Table, 
                NArrow::TColumnFilter::TApplyContext().SetTrySlices(!HasAppData() || AppDataVerified().ColumnShardConfig.GetUseSlicesFilter())));
        }
        if (!Filter) {
            Filter = std::make_shared<NArrow::TColumnFilter>(filter);
        } else if (UseFilter) {
            *Filter = Filter->CombineSequentialAnd(filter);
        } else {
            *Filter = Filter->And(filter);
        }
    }

    void AddBatch(const std::shared_ptr<NArrow::TGeneralContainer>& table) {
        DataAdded = true;
        AFL_VERIFY(table);
        if (UseFilter) {
            AddBatch(table->BuildTableVerified());
        } else {
            if (!Table) {
                Table = table;
            } else {
                auto mergeResult = Table->MergeColumnsStrictly(*table);
                AFL_VERIFY(mergeResult.IsSuccess())("error", mergeResult.GetErrorMessage());
            }
        }
    }

    void AddBatch(const std::shared_ptr<arrow::Table>& table) {
        DataAdded = true;
        auto tableLocal = table;
        if (Filter && UseFilter) {
            AFL_VERIFY(Filter->Apply(tableLocal, 
                NArrow::TColumnFilter::TApplyContext().SetTrySlices(!HasAppData() || AppDataVerified().ColumnShardConfig.GetUseSlicesFilter())));
        }
        if (!Table) {
            Table = std::make_shared<NArrow::TGeneralContainer>(tableLocal);
        } else {
            auto mergeResult = Table->MergeColumnsStrictly(NArrow::TGeneralContainer(tableLocal));
            AFL_VERIFY(mergeResult.IsSuccess())("error", mergeResult.GetErrorMessage());
        }
    }
};

class TFetchedResult {
private:
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Batch);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, NotAppliedFilter);
    std::optional<std::deque<TPortionDataAccessor::TReadPage>> PagesToResult;
    std::optional<std::shared_ptr<arrow::Table>> ChunkToReply;

public:
    TFetchedResult(std::unique_ptr<TFetchedData>&& data)
        : Batch(data->GetTable())
        , NotAppliedFilter(data->GetNotAppliedFilter()) {
    }

    TPortionDataAccessor::TReadPage ExtractPageForResult() {
        AFL_VERIFY(PagesToResult);
        AFL_VERIFY(PagesToResult->size());
        auto result = PagesToResult->front();
        PagesToResult->pop_front();
        return result;
    }

    const std::deque<TPortionDataAccessor::TReadPage>& GetPagesToResultVerified() const {
        AFL_VERIFY(PagesToResult);
        return *PagesToResult;
    }

    void SetPages(std::vector<TPortionDataAccessor::TReadPage>&& pages) {
        AFL_VERIFY(!PagesToResult);
        PagesToResult = std::deque<TPortionDataAccessor::TReadPage>(pages.begin(), pages.end());
    }

    void SetResultChunk(std::shared_ptr<arrow::Table>&& table, const ui32 indexStart, const ui32 recordsCount) {
        auto page = ExtractPageForResult();
        AFL_VERIFY(page.GetIndexStart() == indexStart)("real", page.GetIndexStart())("expected", indexStart);
        AFL_VERIFY(page.GetRecordsCount() == recordsCount)("real", page.GetRecordsCount())("expected", recordsCount);
        AFL_VERIFY(!ChunkToReply);
        ChunkToReply = std::move(table);
    }

    bool IsFinished() const {
        return GetPagesToResultVerified().empty();
    }

    bool HasResultChunk() const {
        return !!ChunkToReply;
    }

    std::shared_ptr<arrow::Table> ExtractResultChunk() {
        AFL_VERIFY(!!ChunkToReply);
        auto result = std::move(*ChunkToReply);
        ChunkToReply.reset();
        return result;
    }

    bool IsEmpty() const {
        return !Batch || Batch->num_rows() == 0 || (NotAppliedFilter && NotAppliedFilter->IsTotalDenyFilter());
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
