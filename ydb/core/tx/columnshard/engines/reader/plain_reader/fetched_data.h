#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

class TFetchedData {
protected:
    using TBlobs = THashMap<TBlobRange, TPortionInfo::TAssembleBlobInfo>;
    YDB_ACCESSOR_DEF(TBlobs, Blobs);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Table>, Table);
    YDB_READONLY_DEF(std::shared_ptr<NArrow::TColumnFilter>, Filter);
    YDB_READONLY(bool, UseFilter, false);
public:
    TFetchedData(const bool useFilter)
        : UseFilter(useFilter)
    {

    }

    void SyncTableColumns(const std::vector<std::shared_ptr<arrow::Field>>& fields);

    std::shared_ptr<NArrow::TColumnFilter> GetAppliedFilter() const {
        return UseFilter ? Filter : nullptr;
    }

    std::shared_ptr<NArrow::TColumnFilter> GetNotAppliedFilter() const {
        return UseFilter ? nullptr : Filter;
    }

    TString ExtractBlob(const TBlobRange& bRange) {
        auto it = Blobs.find(bRange);
        AFL_VERIFY(it != Blobs.end());
        AFL_VERIFY(it->second.IsBlob());
        auto result = it->second.GetData();
        Blobs.erase(it);
        return result;
    }

    void AddBlobs(THashMap<TBlobRange, TString>&& blobs) {
        for (auto&& i : blobs) {
            AFL_VERIFY(Blobs.emplace(i.first, std::move(i.second)).second);
        }
    }

    std::shared_ptr<arrow::RecordBatch> GetBatch() const {
        if (!Table) {
            return nullptr;
        }
        return NArrow::ToBatch(Table, true);
    }

    void AddNulls(THashMap<TBlobRange, ui32>&& blobs) {
        for (auto&& i : blobs) {
            AFL_VERIFY(Blobs.emplace(i.first, i.second).second);
        }
    }

    bool IsEmptyFilter() const {
        return Filter && Filter->IsTotalDenyFilter();
    }

    bool IsEmpty() const {
        return IsEmptyFilter() || (Table && !Table->num_rows());
    }

    void AddFilter(const std::shared_ptr<NArrow::TColumnFilter>& filter) {
        if (UseFilter && Table && filter) {
            AFL_VERIFY(filter->Apply(Table));
        }
        if (!Filter) {
            Filter = filter;
        } else if (filter) {
            *Filter = Filter->CombineSequentialAnd(*filter);
        }
    }

    void AddFilter(const NArrow::TColumnFilter& filter) {
        if (UseFilter && Table) {
            AFL_VERIFY(filter.Apply(Table));
        }
        if (!Filter) {
            Filter = std::make_shared<NArrow::TColumnFilter>(filter);
        } else {
            *Filter = Filter->CombineSequentialAnd(filter);
        }
    }

    void AddBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
        return AddBatch(arrow::Table::Make(batch->schema(), batch->columns(), batch->num_rows()));
    }

    void AddBatch(const std::shared_ptr<arrow::Table>& table) {
        auto tableLocal = table;
        if (Filter && UseFilter) {
            AFL_VERIFY(Filter->Apply(tableLocal));
        }
        if (!Table) {
            Table = tableLocal;
        } else {
            AFL_VERIFY(NArrow::MergeBatchColumns({Table, tableLocal}, Table));
        }
    }

};

}
