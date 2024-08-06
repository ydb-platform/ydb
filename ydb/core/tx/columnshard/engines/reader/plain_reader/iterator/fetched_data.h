#pragma once
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blobs_reader/task.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NOlap {

class TFetchedData {
protected:
    using TBlobs = THashMap<TChunkAddress, TPortionInfo::TAssembleBlobInfo>;
    YDB_ACCESSOR_DEF(TBlobs, Blobs);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TGeneralContainer>, Table);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    YDB_READONLY(bool, UseFilter, false);

public:
    TFetchedData(const bool useFilter)
        : UseFilter(useFilter) {
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

    void AddDefaults(THashMap<TChunkAddress, TPortionInfo::TAssembleBlobInfo>&& blobs) {
        for (auto&& i : blobs) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    bool IsEmpty() const {
        return (Filter && Filter->IsTotalDenyFilter()) || (Table && !Table->num_rows());
    }

    void AddFilter(const std::shared_ptr<NArrow::TColumnFilter>& filter) {
        if (!filter) {
            return;
        }
        return AddFilter(*filter);
    }

    void AddFilter(const NArrow::TColumnFilter& filter) {
        if (UseFilter && Table) {
            AFL_VERIFY(filter.Apply(Table));
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
        auto tableLocal = table;
        if (Filter && UseFilter) {
            AFL_VERIFY(Filter->Apply(tableLocal));
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

public:
    TFetchedResult(std::unique_ptr<TFetchedData>&& data)
        : Batch(data->GetTable())
        , NotAppliedFilter(data->GetNotAppliedFilter()) {
    }

    bool IsEmpty() const {
        return !Batch || Batch->num_rows() == 0 || (NotAppliedFilter && NotAppliedFilter->IsTotalDenyFilter());
    }
};

}   // namespace NKikimr::NOlap
