#include "settings.h"
#include "stats.h"

#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/constructor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

TDictStats TDictStats::SelectSeparatedColumns(const TSettings& settings, const ui32 recordsCount) const {
    std::map<ui64, std::vector<TRTStats>> bySize;
    ui64 sumSize = 0;
    for (ui32 i = 0; i < GetColumnsCount(); ++i) {
        bySize[GetColumnSize(i)].emplace_back(GetRTStats(i));
        sumSize += GetColumnSize(i);
    }
    std::vector<TRTStats> columnStats;
    TSettings::TColumnsDistributor distributor = settings.BuildDistributor(sumSize, recordsCount);
    for (auto it = bySize.rbegin(); it != bySize.rend(); ++it) {
        for (auto&& i : it->second) {
            // Keys not taken as separated fall into the Others store, whose stats are built elsewhere.
            if (distributor.TakeAndDetect(it->first, i.GetRecordsCount()) == TSettings::TColumnsDistributor::EColumnType::Separated) {
                columnStats.emplace_back(std::move(i));
            }
        }
    }
    std::sort(columnStats.begin(), columnStats.end());
    auto columnsBuilder = MakeBuilder();
    for (auto&& i : columnStats) {
        columnsBuilder.Add(i.GetKeyName(), i.GetRecordsCount(), i.GetDataSize(), i.GetAccessorType(settings, recordsCount), i.GetValueType());
    }
    return columnsBuilder.Finish();
}

TDictStats TDictStats::Merge(const std::vector<TDictStats>& stats, const TSettings& settings, const ui32 recordsCount) {
    std::map<std::string_view, TRTStats> resultMap;
    for (auto&& i : stats) {
        for (ui32 idx = 0; idx < i.GetColumnsCount(); ++idx) {
            auto it = resultMap.find(i.GetColumnName(idx));
            if (it == resultMap.end()) {
                it = resultMap.emplace(i.GetColumnName(idx), TRTStats(i.GetColumnName(idx))).first;
            }
            it->second.Add(i, idx);
        }
    }
    auto builder = MakeBuilder();
    for (auto&& i : resultMap) {
        builder.Add(i.second.GetKeyName(), i.second.GetRecordsCount(), i.second.GetDataSize(),
            i.second.GetAccessorType(settings, recordsCount), i.second.GetValueType());
    }
    return builder.Finish();
}

ui32 TDictStats::GetColumnRecordsCount(const ui32 index) const {
    AFL_VERIFY(index < DataRecordsCount->length());
    return DataRecordsCount->Value(index);
}

ui32 TDictStats::GetColumnSize(const ui32 index) const {
    AFL_VERIFY(index < DataSize->length());
    return DataSize->Value(index);
}

std::string_view TDictStats::GetColumnName(const ui32 index) const {
    AFL_VERIFY(index < DataNames->length());
    auto view = DataNames->GetView(index);
    return std::string_view(view.data(), view.size());
}

TDictStats::TDictStats(const std::shared_ptr<arrow::RecordBatch>& original)
    : Original(original) {
    AFL_VERIFY(Original->num_columns() == 4 || Original->num_columns() == 5)("count", Original->num_columns());
    AFL_VERIFY(Original->column(0)->type()->id() == arrow::binary()->id());
    AFL_VERIFY(Original->column(1)->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Original->column(2)->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Original->column(3)->type()->id() == arrow::uint8()->id());
    if (Original->num_columns() == 4) {
        // Legacy stats (pre native scalar columns): synthesize an all-BinaryJson value_type column.
        auto valueTypeArray = NArrow::TThreadSimpleArraysCache::Get(
            arrow::uint8(), std::make_shared<arrow::UInt8Scalar>((ui8)EValueType::BinaryJson), Original->num_rows());
        Original = arrow::RecordBatch::Make(GetStatsSchema(), Original->num_rows(),
            { Original->column(0), Original->column(1), Original->column(2), Original->column(3), valueTypeArray });
    }
    AFL_VERIFY(Original->column(4)->type()->id() == arrow::uint8()->id());
    DataNames = std::static_pointer_cast<arrow::BinaryArray>(Original->column(0));
    DataRecordsCount = std::static_pointer_cast<arrow::UInt32Array>(Original->column(1));
    DataSize = std::static_pointer_cast<arrow::UInt32Array>(Original->column(2));
    AccessorType = std::static_pointer_cast<arrow::UInt8Array>(Original->column(3));
    ValueType = std::static_pointer_cast<arrow::UInt8Array>(Original->column(4));
}

TConstructorContainer TDictStats::GetAccessorConstructor(const ui32 columnIndex) const {
    switch (GetAccessorType(columnIndex)) {
        case IChunkedArray::EType::Array:
            return std::make_shared<NAccessor::NPlain::TConstructor>();
        case IChunkedArray::EType::SparsedArray:
            return std::make_shared<NAccessor::NSparsed::TConstructor>();
        case IChunkedArray::EType::Dictionary:
            return std::make_shared<NAccessor::NDictionary::TConstructor>();
        case IChunkedArray::EType::Undefined:
        case IChunkedArray::EType::SerializedChunkedArray:
        case IChunkedArray::EType::CompositeChunkedArray:
        case IChunkedArray::EType::SubColumnsArray:
        case IChunkedArray::EType::SubColumnsPartialArray:
        case IChunkedArray::EType::ChunkedArray:
            AFL_VERIFY(false)("type", GetAccessorType(columnIndex));
            return TConstructorContainer();
    }
}

TDictStats TDictStats::BuildEmpty() {
    static const TDictStats result(MakeEmptyBatch(GetStatsSchema()));
    return result;
}

TDictStats TDictStats::DeserializeFromBlob(const TString& blob) {
    NSerialization::TNativeSerializer serializer;
    auto result = serializer.Deserialize(blob, GetStatsSchema());
    if (result.ok()) {
        return TDictStats(*result);
    }
    auto legacy = serializer.Deserialize(blob, GetStatsSchemaLegacy());
    AFL_VERIFY(legacy.ok())("error", legacy.status().ToString());
    return TDictStats(*legacy);
}

TString TDictStats::SerializeAsString(const std::shared_ptr<NSerialization::ISerializer>& serializer) const {
    if (serializer) {
        AFL_VERIFY(serializer);
        return serializer->SerializePayload(Original);
    } else {
        return NArrow::SerializeBatchNoCompression(Original);
    }
}

IChunkedArray::EType TDictStats::GetAccessorType(const ui32 columnIndex) const {
    AFL_VERIFY(columnIndex < AccessorType->length());
    return (IChunkedArray::EType)AccessorType->Value(columnIndex);
}

EValueType TDictStats::GetValueType(const ui32 columnIndex) const {
    AFL_VERIFY(columnIndex < ValueType->length());
    return (EValueType)ValueType->Value(columnIndex);
}

TDictStats::TBuilder::TBuilder() {
    Builders = NArrow::MakeBuilders(GetStatsSchema());
    AFL_VERIFY(Builders.size() == 5);
    AFL_VERIFY(Builders[0]->type()->id() == arrow::binary()->id());
    AFL_VERIFY(Builders[1]->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Builders[2]->type()->id() == arrow::uint32()->id());
    AFL_VERIFY(Builders[3]->type()->id() == arrow::uint8()->id());
    AFL_VERIFY(Builders[4]->type()->id() == arrow::uint8()->id());
    Names = static_cast<arrow::BinaryBuilder*>(Builders[0].get());
    Records = static_cast<arrow::UInt32Builder*>(Builders[1].get());
    DataSize = static_cast<arrow::UInt32Builder*>(Builders[2].get());
    AccessorType = static_cast<arrow::UInt8Builder*>(Builders[3].get());
    ValueType = static_cast<arrow::UInt8Builder*>(Builders[4].get());
}

void TDictStats::TBuilder::Add(const TString& name, const ui32 recordsCount, const ui32 dataSize, const IChunkedArray::EType accessorType,
    const EValueType valueType) {
    AFL_VERIFY(Builders.size());
    if (!LastKeyName) {
        LastKeyName = name;
    } else {
        AFL_VERIFY(*LastKeyName < name)("last", LastKeyName)("name", name);
    }
    AFL_VERIFY(recordsCount);
    AFL_VERIFY(accessorType == IChunkedArray::EType::Array || accessorType == IChunkedArray::EType::SparsedArray ||
               accessorType == IChunkedArray::EType::Dictionary)("type", accessorType);
    TStatusValidator::Validate(Names->Append(name.data(), name.size()));
    TStatusValidator::Validate(Records->Append(recordsCount));
    TStatusValidator::Validate(DataSize->Append(dataSize));
    TStatusValidator::Validate(AccessorType->Append((ui8)accessorType));
    TStatusValidator::Validate(ValueType->Append((ui8)valueType));
    ++RecordsCount;
}

void TDictStats::TBuilder::Add(
    const std::string_view name, const ui32 recordsCount, const ui32 dataSize, const IChunkedArray::EType accessorType, const EValueType valueType) {
    Add(TString(name.data(), name.size()), recordsCount, dataSize, accessorType, valueType);
}

TDictStats TDictStats::TBuilder::Finish() {
    AFL_VERIFY(Builders.size());
    auto arrays = NArrow::Finish(std::move(Builders));
    return TDictStats(arrow::RecordBatch::Make(GetStatsSchema(), RecordsCount, std::move(arrays)));
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
