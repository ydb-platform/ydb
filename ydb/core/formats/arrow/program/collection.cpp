#include "collection.h"

#include <ydb/core/formats/arrow/accessor/plain/accessor.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>
#include <util/string/join.h>

namespace NKikimr::NArrow::NAccessor {

void TAccessorsCollection::AddVerified(const ui32 columnId, const arrow::Datum& data, const bool withFilter) {
    AddVerified(columnId, TAccessorCollectedContainer(data), withFilter);
}

void TAccessorsCollection::AddVerified(const ui32 columnId, const std::shared_ptr<IChunkedArray>& data, const bool withFilter) {
    AddVerified(columnId, TAccessorCollectedContainer(data), withFilter);
}

void TAccessorsCollection::AddVerified(const ui32 columnId, const TAccessorCollectedContainer& data, const bool withFilter) {
    AFL_VERIFY(columnId);
    if (UseFilter && withFilter && !Filter->IsTotalAllowFilter()) {
        auto filtered = Filter->Apply(data.GetData());
        RecordsCountActual = filtered->GetRecordsCount();
        AFL_VERIFY(Accessors.emplace(columnId, filtered).second)("id", columnId);
    } else {
        if (Filter->IsTotalAllowFilter()) {
            if (!data.GetItWasScalar()) {
                RecordsCountActual = data->GetRecordsCount();
            }
        } else {
            RecordsCountActual = Filter->GetFilteredCount();
        }
        AFL_VERIFY(Accessors.emplace(columnId, data).second);
    }
}

std::shared_ptr<arrow::Array> TAccessorsCollection::GetArrayVerified(const ui32 columnId) const {
    auto chunked = GetAccessorVerified(columnId)->GetChunkedArray();
    arrow::FieldVector fields = { GetFieldVerified(columnId) };
    auto schema = std::make_shared<arrow::Schema>(fields);
    return NArrow::ToBatch(arrow::Table::Make(schema, { chunked }))->column(0);
}

std::shared_ptr<arrow::Table> TAccessorsCollection::GetTable(const std::vector<ui32>& columnIds) const {
    AFL_VERIFY(columnIds.size());
    auto accessors = GetAccessors(columnIds);
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> arrays;
    std::optional<ui32> recordsCount;
    ui32 idx = 0;
    for (auto&& arr : accessors) {
        fields.emplace_back(std::make_shared<arrow::Field>(::ToString(columnIds[idx]), arr->GetDataType()));
        arrays.emplace_back(arr->GetChunkedArray());
        if (!recordsCount) {
            recordsCount = arr->GetRecordsCount();
        } else {
            AFL_VERIFY(*recordsCount == arr->GetRecordsCount());
        }
        ++idx;
    }
    AFL_VERIFY(recordsCount);
    return arrow::Table::Make(std::make_shared<arrow::Schema>(std::move(fields)), std::move(arrays), *recordsCount);
}

std::shared_ptr<IChunkedArray> TAccessorsCollection::ExtractAccessorOptional(const ui32 columnId) {
    auto result = GetAccessorOptional(columnId);
    if (!!result) {
        Remove(columnId);
    }
    return result;
}

std::vector<std::shared_ptr<IChunkedArray>> TAccessorsCollection::ExtractAccessors(const std::vector<ui32>& columnIds) {
    auto result = GetAccessors(columnIds);
    Remove(columnIds);
    return result;
}

std::vector<std::shared_ptr<IChunkedArray>> TAccessorsCollection::GetAccessors(const std::vector<ui32>& columnIds) const {
    if (columnIds.empty()) {
        return {};
    }
    std::vector<std::shared_ptr<IChunkedArray>> result;
    std::optional<ui32> recordsCount;
    for (auto&& i : columnIds) {
        auto accessor = GetAccessorVerified(i);
        if (!recordsCount) {
            recordsCount = accessor->GetRecordsCount();
        } else {
            AFL_VERIFY(*recordsCount == accessor->GetRecordsCount())("rc", recordsCount)("accessor", accessor->GetRecordsCount());
        }
        result.emplace_back(accessor);
    }
    AFL_VERIFY(recordsCount);
    return result;
}

TAccessorsCollection::TChunkedArguments TAccessorsCollection::GetArguments(const std::vector<ui32>& columnIds, const bool concatenate) const {
    if (columnIds.empty()) {
        return TChunkedArguments::Empty();
    }
    TChunkedArguments result;
    //    NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("ids", JoinSeq(",", columnIds))("records_count", RecordsCountActual)(
    //        "use_filter", UseFilter)("filter", Filter->DebugString());
    for (auto&& i : columnIds) {
        auto it = Accessors.find(i);
        if (it == Accessors.end()) {
            result.AddScalar(GetConstantScalarVerified(i));
        } else if (it->second.GetItWasScalar()) {
            result.AddScalar(it->second->GetScalar(0));
        } else {
            result.AddArray(it->second.GetData());
        }
    }
    result.StartRead(concatenate);
    return result;
}

std::shared_ptr<IChunkedArray> TAccessorsCollection::GetConstantVerified(const ui32 columnId, const ui32 recordsCount) const {
    auto it = Constants.find(columnId);
    AFL_VERIFY(it != Constants.end());
    return std::make_shared<TTrivialArray>(NArrow::TStatusValidator::GetValid(arrow::MakeArrayFromScalar(*it->second, recordsCount)));
}

std::shared_ptr<arrow::Scalar> TAccessorsCollection::GetConstantScalarVerified(const ui32 columnId) const {
    auto it = Constants.find(columnId);
    AFL_VERIFY(it != Constants.end())("id", columnId);
    return it->second;
}

std::shared_ptr<arrow::Scalar> TAccessorsCollection::GetConstantScalarOptional(const ui32 columnId) const {
    auto it = Constants.find(columnId);
    if (it != Constants.end()) {
        return it->second;
    } else {
        return nullptr;
    }
}

TAccessorsCollection::TAccessorsCollection(const std::shared_ptr<arrow::RecordBatch>& data, const NSSA::IColumnResolver& resolver) {
    ui32 idx = 0;
    for (auto&& i : data->columns()) {
        const std::string arrName = data->schema()->field(idx)->name();
        TString name(arrName.data(), arrName.size());
        AddVerified(resolver.GetColumnIdVerified(name), std::make_shared<TTrivialArray>(i), false);
        ++idx;
    }
}

TAccessorsCollection::TAccessorsCollection(const std::shared_ptr<arrow::Table>& data, const NSSA::IColumnResolver& resolver) {
    ui32 idx = 0;
    for (auto&& i : data->columns()) {
        const std::string arrName = data->schema()->field(idx)->name();
        TString name(arrName.data(), arrName.size());
        AddVerified(resolver.GetColumnIdVerified(name), std::make_shared<TTrivialChunkedArray>(i), false);
        ++idx;
    }
}

std::shared_ptr<arrow::RecordBatch> TAccessorsCollection::ToBatch(const NSSA::IColumnResolver* resolver, const bool strictResolver) const {
    auto table = ToGeneralContainer(resolver, {}, strictResolver)->BuildTableVerified();
    return NArrow::ToBatch(table);
}

std::shared_ptr<arrow::Table> TAccessorsCollection::ToTable(
    const std::optional<std::set<ui32>>& columnIds, const NSSA::IColumnResolver* resolver, const bool strictResolver) const {
    return ToGeneralContainer(resolver, columnIds, strictResolver)->BuildTableVerified();
}

std::shared_ptr<NKikimr::NArrow::TGeneralContainer> TAccessorsCollection::ToGeneralContainer(
    const NSSA::IColumnResolver* resolver, const std::optional<std::set<ui32>>& columnIds, const bool strictResolver) const {
    const auto predColumnName = [&](const ui32 colId) {
        TString colName;
        if (resolver) {
            if (strictResolver) {
                colName = resolver->GetColumnName(colId);
            } else {
                colName = resolver->GetColumnName(colId, false);
            }
        }
        if (!colName) {
            colName = ::ToString(colId);
        }
        return colName;
    };
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<IChunkedArray>> arrays;
    if (ColumnIdsSequence.size()) {
        for (auto&& i : ColumnIdsSequence) {
            if (columnIds && !columnIds->contains(i)) {
                continue;
            }
            auto accessor = GetAccessorVerified(i);
            fields.emplace_back(std::make_shared<arrow::Field>(predColumnName(i), accessor->GetDataType()));
            arrays.emplace_back(accessor);
        }
    } else {
        for (auto&& i : Accessors) {
            if (columnIds && !columnIds->contains(i.first)) {
                continue;
            }
            fields.emplace_back(std::make_shared<arrow::Field>(predColumnName(i.first), i.second->GetDataType()));
            arrays.emplace_back(i.second.GetData());
        }
    }
    return std::make_shared<TGeneralContainer>(std::move(fields), std::move(arrays));
}

std::optional<TAccessorsCollection> TAccessorsCollection::SelectOptional(const std::vector<ui32>& indexes, const bool withFilters) const {
    TAccessorsCollection result;
    for (auto&& i : indexes) {
        auto it = Accessors.find(i);
        if (it == Accessors.end()) {
            auto itConst = Constants.find(i);
            if (itConst == Constants.end()) {
                return std::nullopt;
            } else {
                result.AddConstantVerified(i, itConst->second);
            }
        } else {
            result.AddVerified(i, it->second, false);
        }
    }
    if (withFilters) {
        result.UseFilter = UseFilter;
        result.AddFilter(*Filter);
    }
    return result;
}

void TAccessorsCollection::RemainOnly(const std::vector<ui32>& columns, const bool useAsSequence) {
    THashSet<ui32> columnIds;
    for (auto&& i : columns) {
        columnIds.emplace(i);
    }
    THashSet<ui32> toRemove;
    for (auto&& [i, _] : Accessors) {
        if (!columnIds.contains(i)) {
            toRemove.emplace(i);
        } else {
            columnIds.erase(i);
        }
    }
    for (auto&& [i, _] : Constants) {
        if (!columnIds.contains(i)) {
            toRemove.emplace(i);
        } else {
            columnIds.erase(i);
        }
    }
    AFL_VERIFY(columnIds.empty());
    for (auto&& i : toRemove) {
        Remove(i);
    }
    if (useAsSequence) {
        ColumnIdsSequence = columns;
    }
}

void TAccessorsCollection::AddBatch(
    const std::shared_ptr<TGeneralContainer>& container, const NSSA::IColumnResolver& resolver, const bool withFilter) {
    for (ui32 i = 0; i < container->GetColumnsCount(); ++i) {
        AddVerified(
            resolver.GetColumnIdVerified(container->GetSchema()->GetFieldVerified(i)->name()), container->GetColumnVerified(i), withFilter);
    }
}

TAccessorCollectedContainer::TAccessorCollectedContainer(const arrow::Datum& data)
    : ItWasScalar(data.is_scalar()) {
    if (data.is_array()) {
        Data = std::make_shared<TTrivialArray>(data.make_array());
    } else if (data.is_arraylike()) {
        if (data.chunked_array()->num_chunks() == 1) {
            Data = std::make_shared<TTrivialArray>(data.chunked_array()->chunk(0));
        } else {
            Data = std::make_shared<TTrivialChunkedArray>(data.chunked_array());
        }
    } else if (data.is_scalar()) {
        Data = std::make_shared<TTrivialArray>(data.scalar());
    } else {
        AFL_VERIFY(false);
    }
}

}   // namespace NKikimr::NArrow::NAccessor
