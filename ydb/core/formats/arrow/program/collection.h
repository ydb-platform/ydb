#pragma once

#include "abstract.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/formats/arrow/validation/validation.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/datum.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NAccessor {

class TAccessorCollectedContainer {
private:
    std::shared_ptr<NArrow::NAccessor::IChunkedArray> Data;
    YDB_READONLY(bool, ItWasScalar, false);

public:
    TAccessorCollectedContainer(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& data)
        : Data(data) {
        AFL_VERIFY(Data);
    }

    TAccessorCollectedContainer(const arrow::Datum& data);

    const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& GetData() const {
        return Data;
    }

    const NArrow::NAccessor::IChunkedArray* operator->() const {
        return Data.get();
    }
};

class TAccessorsCollection {
private:
    THashMap<ui32, TAccessorCollectedContainer> Accessors;
    THashMap<ui32, std::shared_ptr<arrow::Scalar>> Constants;
    std::vector<ui32> ColumnIdsSequence;
    std::shared_ptr<TColumnFilter> Filter = std::make_shared<TColumnFilter>(TColumnFilter::BuildAllowFilter());
    bool UseFilter = true;
    std::optional<ui32> RecordsCountActual;
    THashSet<i64> Markers;

public:
    bool HasMarker(const i64 marker) const {
        return Markers.contains(marker);
    }

    void AddMarker(const i64 marker) {
        AFL_VERIFY(Markers.emplace(marker).second);
    }

    void RemoveMarker(const i64 marker) {
        AFL_VERIFY(Markers.erase(marker));
    }

    bool IsEmptyFiltered() const {
        return Filter->IsTotalDenyFilter();
    }

    bool HasAccessors() const {
        return Accessors.size();
    }

    std::optional<ui32> GetRecordsCountActualOptional() const {
        return RecordsCountActual;
    }

    ui32 GetRecordsCountActualVerified() const {
        AFL_VERIFY(!!RecordsCountActual);
        return *RecordsCountActual;
    }

    TAccessorsCollection() = default;
    TAccessorsCollection(const ui32 baseRecordsCount)
        : RecordsCountActual(baseRecordsCount) {
    }

    std::optional<TAccessorsCollection> SelectOptional(const std::vector<ui32>& indexes, const bool withFilters) const;

    bool GetFilterUsage() const {
        return UseFilter;
    }

    const TColumnFilter& GetFilter() const {
        return *Filter;
    }

    void SetFilterUsage(const bool value) {
        if (UseFilter == value) {
            return;
        }
        AFL_VERIFY(Filter->IsTotalAllowFilter());
        UseFilter = value;
    }

    void AddBatch(const std::shared_ptr<TGeneralContainer>& container, const NSSA::IColumnResolver& resolver, const bool withFilter);

    TAccessorsCollection(const std::shared_ptr<arrow::RecordBatch>& data, const NSSA::IColumnResolver& resolver);
    TAccessorsCollection(const std::shared_ptr<arrow::Table>& data, const NSSA::IColumnResolver& resolver);

    std::shared_ptr<TGeneralContainer> ToGeneralContainer(const NSSA::IColumnResolver* resolver = nullptr,
        const std::optional<std::set<ui32>>& columnIds = std::nullopt, const bool strictResolver = true) const;

    std::shared_ptr<arrow::RecordBatch> ToBatch(const NSSA::IColumnResolver* resolver = nullptr, const bool strictResolver = true) const;
    std::shared_ptr<arrow::Table> ToTable(const std::optional<std::set<ui32>>& columnIds = std::nullopt,
        const NSSA::IColumnResolver* resolver = nullptr, const bool strictResolver = true) const;

    std::shared_ptr<IChunkedArray> GetConstantVerified(const ui32 columnId, const ui32 recordsCount) const;
    std::shared_ptr<arrow::Scalar> GetConstantScalarVerified(const ui32 columnId) const;
    std::shared_ptr<arrow::Scalar> GetConstantScalarOptional(const ui32 columnId) const;

    void Clear() {
        Accessors.clear();
        Filter = std::make_shared<TColumnFilter>(TColumnFilter::BuildAllowFilter());
        RecordsCountActual = std::nullopt;
    }

    std::optional<ui32> GetRecordsCountOptional() const {
        std::optional<ui32> result;
        for (auto&& i : Accessors) {
            if (!result) {
                result = i.second->GetRecordsCount();
            } else {
                AFL_VERIFY(*result == i.second->GetRecordsCount());
            }
        }
        return result;
    }

    ui32 GetRecordsCountVerified() const {
        const auto result = GetRecordsCountOptional();
        AFL_VERIFY(!!result);
        return *result;
    }

    ui32 GetColumnsCount() const {
        return Accessors.size() + Constants.size();
    }

    bool HasColumn(const ui32 id) const {
        return Accessors.contains(id) || Constants.contains(id);
    }

    void AddVerified(const ui32 columnId, const arrow::Datum& data, const bool withFilter);
    void AddVerified(const ui32 columnId, const std::shared_ptr<IChunkedArray>& data, const bool withFilter);
    void AddVerified(const ui32 columnId, const TAccessorCollectedContainer& data, const bool withFilter);

    void AddConstantVerified(const ui32 columnId, const std::shared_ptr<arrow::Scalar>& scalar) {
        AFL_VERIFY(columnId);
        AFL_VERIFY(Constants.emplace(columnId, scalar).second);
    }

    class TChunksMerger {
    private:
        std::vector<arrow::Datum> Chunks;
        bool Finished = false;
        bool IsScalar = false;

    public:
        void AddChunk(const arrow::Datum& datum) {
            AFL_VERIFY(!Finished);
            Chunks.emplace_back(datum);
            if (datum.is_scalar()) {
                IsScalar = true;
            }
        }

        [[nodiscard]] TConclusion<arrow::Datum> Execute() {
            AFL_VERIFY(!Finished);
            Finished = true;
            if (IsScalar) {
                if (Chunks.size() == 1) {
                    return Chunks.front();
                } else {
                    return TConclusionStatus::Fail("cannot merge datum as scalars");
                }
            }
            std::vector<std::shared_ptr<arrow::Array>> chunks;
            for (auto&& i : Chunks) {
                if (i.is_array()) {
                    chunks.emplace_back(i.make_array());
                } else if (i.is_arraylike()) {
                    for (auto&& c : i.chunked_array()->chunks()) {
                        chunks.emplace_back(c);
                    }
                } else {
                    return TConclusionStatus::Fail("cannot merge datum with type: " + ::ToString((ui32)i.kind()));
                }
            }
            if (chunks.size() == 1) {
                return chunks.front();
            } else {
                auto result = arrow::ChunkedArray::Make(chunks);
                if (!result.ok()) {
                    return TConclusionStatus::Fail(result.status().message());
                } else {
                    return *result;
                }
            }
        }
    };

    class TChunkedArguments: public TMoveOnly {
    private:
        std::vector<std::shared_ptr<IChunkedArray>> ArraysOriginal;
        std::vector<std::shared_ptr<arrow::ChunkedArray>> Arrays;
        std::vector<arrow::Datum> Scalars;

        std::shared_ptr<arrow::Table> Table;
        std::vector<std::shared_ptr<arrow::Field>> Fields;
        class TArrayAddress {
        private:
            YDB_READONLY_DEF(std::optional<ui32>, ArrayIndex);
            YDB_READONLY_DEF(std::optional<ui32>, ScalarIndex);

        public:
            static TArrayAddress Array(const ui32 index) {
                TArrayAddress result;
                result.ArrayIndex = index;
                return result;
            }
            static TArrayAddress Scalar(const ui32 index) {
                TArrayAddress result;
                result.ScalarIndex = index;
                return result;
            }

            arrow::Datum GetDatum(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<arrow::Datum>& scalars) const {
                if (ArrayIndex) {
                    AFL_VERIFY(*ArrayIndex < (ui32)batch->num_columns());
                    return batch->column_data(*ArrayIndex);
                } else {
                    AFL_VERIFY(ScalarIndex);
                    AFL_VERIFY(*ScalarIndex < scalars.size());
                    return scalars[*ScalarIndex];
                }
            }
        };

        std::vector<TArrayAddress> Addresses;
        std::optional<arrow::TableBatchReader> TableReader;
        bool Started = false;
        bool Finished = false;
        bool ConstantsRead = false;

    public:
        void AddArray(const std::shared_ptr<IChunkedArray>& arr) {
            AFL_VERIFY(!Started);
            if (Arrays.size()) {
                AFL_VERIFY(ArraysOriginal.back()->GetRecordsCount() == arr->GetRecordsCount())("last", ArraysOriginal.back()->GetRecordsCount())(
                                                                         "new", arr->GetRecordsCount())("last_type",
                                                                         ArraysOriginal.back()->GetType())("current_type", arr->GetType());
            }
            ArraysOriginal.emplace_back(arr);
            Arrays.emplace_back(arr->GetChunkedArray());
            Addresses.emplace_back(TArrayAddress::Array(Arrays.size() - 1));
            Fields.emplace_back(std::make_shared<arrow::Field>(::ToString(Fields.size() + 1), arr->GetDataType()));
        }

        void AddScalar(const std::shared_ptr<arrow::Scalar>& scalar) {
            AFL_VERIFY(!Started);
            Scalars.emplace_back(scalar);
            Addresses.emplace_back(TArrayAddress::Scalar(Scalars.size() - 1));
        }

        void StartRead(const bool concatenate) {
            Started = true;
            AFL_VERIFY(!Table);
            AFL_VERIFY(Arrays.size() || Scalars.size());
            if (Arrays.size()) {
                Table = arrow::Table::Make(std::make_shared<arrow::Schema>(Fields), Arrays);
                if (concatenate) {
                    Table = TStatusValidator::GetValid(Table->CombineChunks());
                }
                TableReader.emplace(*Table);
            }
        }

        static TChunkedArguments Empty() {
            TChunkedArguments result;
            result.Started = true;
            return result;
        }

        std::optional<std::vector<arrow::Datum>> ReadNext() {
            AFL_VERIFY(Started);
            AFL_VERIFY(!Finished);
            if (Arrays.empty() && Scalars.empty()) {
                Finished = true;
                return {};
            }
            if (Arrays.empty() && Scalars.size()) {
                if (ConstantsRead) {
                    Finished = true;
                    return {};
                }
                ConstantsRead = true;
                return Scalars;
            } else {
                AFL_VERIFY(Table);
                std::shared_ptr<arrow::RecordBatch> chunk;
                TStatusValidator::Validate(TableReader->ReadNext(&chunk));
                if (!chunk) {
                    Finished = true;
                    return {};
                }
                std::vector<arrow::Datum> columns;
                for (auto&& i : Addresses) {
                    columns.emplace_back(i.GetDatum(chunk, Scalars));
                }
                return columns;
            }
        }

        TChunkedArguments() = default;
    };

    TChunkedArguments GetArguments(const std::vector<ui32>& columnIds, const bool concatenate) const;
    std::vector<std::shared_ptr<IChunkedArray>> GetAccessors(const std::vector<ui32>& columnIds) const;
    std::vector<std::shared_ptr<IChunkedArray>> ExtractAccessors(const std::vector<ui32>& columnIds);
    std::shared_ptr<IChunkedArray> ExtractAccessorOptional(const ui32 columnId);


    std::shared_ptr<arrow::Table> GetTable(const std::vector<ui32>& columnIds) const;

    void Remove(const std::vector<ui32>& columnIds, const bool optional = false) {
        for (auto&& i : columnIds) {
            Remove(i, optional);
        }
    }

    void Remove(const ui32 columnId, const bool optional = false) {
        auto it = Accessors.find(columnId);
        if (it != Accessors.end()) {
            Accessors.erase(it);
        } else {
            auto itConst = Constants.find(columnId);
            if (!optional) {
                AFL_VERIFY(itConst != Constants.end());
            } else {
                if (itConst == Constants.end()) {
                    return;
                }
            }
            Constants.erase(itConst);
        }
    }

    template <class TColumnIdOwner>
    void Remove(const std::vector<TColumnIdOwner>& columns, const bool optional = false) {
        for (auto&& i : columns) {
            Remove(i.GetColumnId(), optional);
        }
    }

    void CutFilter(const ui32 recordsCount, const ui32 limit, const bool reverse) {
        const ui32 recordsCountImpl = Filter->GetFilteredCount().value_or(recordsCount);
        if (recordsCountImpl < limit) {
            return;
        }
        if (UseFilter) {
            auto filter = NArrow::TColumnFilter::BuildAllowFilter().Cut(recordsCountImpl, limit, reverse);
            AddFilter(filter);
        } else {
            *Filter = Filter->Cut(recordsCountImpl, limit, reverse);
        }
    }

    void RemainOnly(const std::vector<ui32>& columns, const bool useAsSequence);

    arrow::Datum GetDatumVerified(const ui32 columnId) const {
        auto chunked = GetAccessorVerified(columnId)->GetChunkedArray();
        if (chunked->num_chunks() == 1) {
            return chunked->chunk(0);
        }
        return chunked;
    }

    std::optional<arrow::Datum> GetDatumOptional(const ui32 columnId) const {
        auto acc = GetAccessorOptional(columnId);
        if (!acc) {
            return std::nullopt;
        }
        auto chunked = acc->GetChunkedArray();
        if (chunked->num_chunks() == 1) {
            return chunked->chunk(0);
        }
        return chunked;
    }

    std::shared_ptr<arrow::ChunkedArray> GetChunkedArrayVerified(const ui32 columnId) const {
        return GetAccessorVerified(columnId)->GetChunkedArray();
    }

    const std::shared_ptr<IChunkedArray>& GetAccessorVerified(const ui32 columnId) const {
        auto it = Accessors.find(columnId);
        AFL_VERIFY(it != Accessors.end())("id", columnId);
        return it->second.GetData();
    }

    const std::shared_ptr<IChunkedArray>& GetAccessorOptional(const ui32 columnId) const {
        auto it = Accessors.find(columnId);
        if (it != Accessors.end()) {
            return it->second.GetData();
        } else {
            return Default<std::shared_ptr<IChunkedArray>>();
        }
    }

    std::shared_ptr<arrow::Array> GetArrayVerified(const ui32 columnId) const;

    std::shared_ptr<arrow::Field> GetFieldVerified(const ui32 columnId) const {
        auto it = Accessors.find(columnId);
        AFL_VERIFY(it != Accessors.end());
        return std::make_shared<arrow::Field>(::ToString(columnId), it->second->GetDataType());
    }

    ui32 GetFilteredCount(const ui32 recordsCount, const ui32 defLimit) const {
        return std::min(Filter->GetFilteredCount().value_or(recordsCount), defLimit);
    }

    std::shared_ptr<NArrow::TColumnFilter> GetAppliedFilter() const {
        return UseFilter ? Filter : nullptr;
    }

    std::shared_ptr<NArrow::TColumnFilter> GetNotAppliedFilter() const {
        return UseFilter ? nullptr : Filter;
    }

    void AddFilter(const TColumnFilter& filter) {
        if (!UseFilter) {
            *Filter = Filter->And(filter);
        } else {
            *Filter = Filter->CombineSequentialAnd(filter);
            for (auto&& i : Accessors) {
                i.second = TAccessorCollectedContainer(i.second.GetData()->ApplyFilter(filter, i.second.GetData()));
            }
        }
        RecordsCountActual = Filter->GetFilteredCount();
    }
};

}   // namespace NKikimr::NArrow::NAccessor
