#include "accessor.h"

#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>

#include <ydb/library/formats/arrow/simple_arrays_cache.h>

namespace NKikimr::NArrow::NAccessor {

std::shared_ptr<TSparsedArray> TSparsedArray::Make(const IChunkedArray& defaultArray, const std::shared_ptr<arrow::Scalar>& defaultValue) {
    if (defaultValue) {
        AFL_VERIFY(defaultValue->type->id() == defaultArray.GetDataType()->id());
    }
    std::optional<TFullDataAddress> current;
    std::shared_ptr<arrow::RecordBatch> records;
    ui32 sparsedRecordsCount = 0;
    AFL_VERIFY(SwitchType(defaultArray.GetDataType()->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;
        auto builderValue = NArrow::MakeBuilder(defaultArray.GetDataType());
        TBuilder* builderValueImpl = (TBuilder*)builderValue.get();
        auto builderIndex = NArrow::MakeBuilder(arrow::uint32());
        arrow::UInt32Builder* builderIndexImpl = (arrow::UInt32Builder*)builderIndex.get();
        auto scalar = static_pointer_cast<TScalar>(defaultValue);
        for (ui32 pos = 0; pos < defaultArray.GetRecordsCount();) {
            current = defaultArray.GetChunk(current, pos);
            auto typedArray = static_pointer_cast<TArray>(current->GetArray());
            for (ui32 i = 0; i < typedArray->length(); ++i) {
                std::optional<bool> isDefault;
                if (scalar) {
                    if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                        isDefault = arrow::util::string_view((char*)scalar->value->data(), scalar->value->size()) == typedArray->GetView(i);
                    } else if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                        isDefault = scalar->value == typedArray->Value(i);
                    } else {
                        AFL_VERIFY(false)("type", defaultArray.GetDataType()->ToString());
                    }
                } else {
                    isDefault = typedArray->IsNull(i);
                }
                if (!*isDefault) {
                    if constexpr (arrow::has_string_view<typename TWrap::T>()) {
                        NArrow::TStatusValidator::Validate(builderValueImpl->Append(typedArray->GetView(i)));
                        NArrow::TStatusValidator::Validate(builderIndexImpl->Append(pos + i));
                        ++sparsedRecordsCount;
                    } else if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                        NArrow::TStatusValidator::Validate(builderValueImpl->Append(typedArray->Value(i)));
                        NArrow::TStatusValidator::Validate(builderIndexImpl->Append(pos + i));
                        ++sparsedRecordsCount;
                    } else {
                        AFL_VERIFY(false)("type", defaultArray.GetDataType()->ToString());
                    }
                }
            }
            pos = current->GetAddress().GetGlobalFinishPosition();
            AFL_VERIFY(pos <= defaultArray.GetRecordsCount());
        }
        std::vector<std::shared_ptr<arrow::Array>> columns = { NArrow::TStatusValidator::GetValid(builderIndex->Finish()),
            NArrow::TStatusValidator::GetValid(builderValue->Finish()) };
        records = arrow::RecordBatch::Make(BuildSchema(defaultArray.GetDataType()), sparsedRecordsCount, columns);
        AFL_VERIFY_DEBUG(records->ValidateFull().ok());
        return true;
    }));
    TSparsedArrayChunk chunk(defaultArray.GetRecordsCount(), records, defaultValue);
    return std::shared_ptr<TSparsedArray>(new TSparsedArray(std::move(chunk), defaultValue, defaultArray.GetDataType()));
}

std::shared_ptr<arrow::Scalar> TSparsedArray::DoGetMaxScalar() const {
    return Record.GetMaxScalar();
}

ui32 TSparsedArray::GetLastIndex(const std::shared_ptr<arrow::RecordBatch>& batch) {
    AFL_VERIFY(batch);
    AFL_VERIFY(batch->num_rows());
    auto c = batch->GetColumnByName("index");
    AFL_VERIFY(c)("schema", batch->schema()->ToString());
    AFL_VERIFY(c->type_id() == arrow::uint32()->id())("type", c->type()->ToString());
    auto ui32Column = static_pointer_cast<arrow::UInt32Array>(c);
    return ui32Column->Value(ui32Column->length() - 1);
}

namespace {
static thread_local THashMap<TString, std::shared_ptr<arrow::RecordBatch>> SimpleBatchesCache;
}

TSparsedArrayChunk TSparsedArray::MakeDefaultChunk(
    const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount) {
    auto it = SimpleBatchesCache.find(type->ToString());
    if (it == SimpleBatchesCache.end()) {
        it = SimpleBatchesCache.emplace(type->ToString(), NArrow::MakeEmptyBatch(BuildSchema(type))).first;
        AFL_VERIFY(it->second->ValidateFull().ok());
    }
    return TSparsedArrayChunk(recordsCount, it->second, defaultValue);
}

void TSparsedArray::Reallocate() {
    Record = TSparsedArrayChunk(GetRecordsCount(), NArrow::ReallocateBatch(Record.GetRecords()), DefaultValue);
}

IChunkedArray::TLocalDataAddress TSparsedArrayChunk::GetChunk(
    const std::optional<IChunkedArray::TCommonChunkAddress>& /*chunkCurrent*/, const ui64 position) const {
    const auto predCompare = [](const ui32 position, const TInternalChunkInfo& item) {
        return position < item.GetStartExt();
    };
    auto it = std::upper_bound(RemapExternalToInternal.begin(), RemapExternalToInternal.end(), position, predCompare);
    AFL_VERIFY(it != RemapExternalToInternal.begin());
    --it;
    if (it->GetIsDefault()) {
        return IChunkedArray::TLocalDataAddress(
            NArrow::TThreadSimpleArraysCache::Get(ColValue->type(), DefaultValue, it->GetSize()), it->GetStartExt(), 0);
    } else {
        return IChunkedArray::TLocalDataAddress(ColValue->Slice(it->GetStartInt(), it->GetSize()), it->GetStartExt(), 0);
    }
}

TSparsedArrayChunk::TSparsedArrayChunk(
    const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& records, const std::shared_ptr<arrow::Scalar>& defaultValue)
    : RecordsCount(recordsCount)
    , Records(records)
    , DefaultValue(defaultValue) {
    AFL_VERIFY(Records);
    AFL_VERIFY(records->num_columns() == 2);
    ColIndex = Records->GetColumnByName("index");
    AFL_VERIFY(ColIndex);
    AFL_VERIFY(ColIndex->type_id() == arrow::uint32()->id());
    UI32ColIndex = static_pointer_cast<arrow::UInt32Array>(ColIndex);
    if (UI32ColIndex->length()) {
        AFL_VERIFY(UI32ColIndex->Value(UI32ColIndex->length() - 1) < recordsCount)("val", UI32ColIndex->Value(UI32ColIndex->length() - 1))(
            "count", recordsCount);
    }
    NotDefaultRecordsCount = UI32ColIndex->length();
    RawValues = UI32ColIndex->raw_values();
    ColValue = Records->GetColumnByName("value");
    if (DefaultValue) {
        AFL_VERIFY(DefaultValue->type->id() == ColValue->type_id());
    }
    DefaultsArray = TTrivialArray::BuildArrayFromOptionalScalar(DefaultValue, ColValue->type());
    ui32 nextIndex = 0;
    ui32 startIndexExt = 0;
    ui32 startIndexInt = 0;
    for (ui32 idx = 0; idx < UI32ColIndex->length(); ++idx) {
        if (nextIndex != UI32ColIndex->Value(idx)) {
            if (idx - startIndexInt) {
                RemapExternalToInternal.emplace_back(startIndexExt, startIndexInt, idx - startIndexInt, false);
            }
            RemapExternalToInternal.emplace_back(nextIndex, 0, UI32ColIndex->Value(idx) - nextIndex, true);
            startIndexExt = UI32ColIndex->Value(idx);
            startIndexInt = idx;
        }
        nextIndex = UI32ColIndex->Value(idx) + 1;
    }
    if (UI32ColIndex->length() > startIndexInt) {
        RemapExternalToInternal.emplace_back(startIndexExt, startIndexInt, UI32ColIndex->length() - startIndexInt, false);
    }
    if (nextIndex != RecordsCount) {
        RemapExternalToInternal.emplace_back(nextIndex, 0, RecordsCount - nextIndex, true);
    }
    ui32 count = 0;
    for (auto&& i : RemapExternalToInternal) {
        count += i.GetSize();
    }
    for (ui32 i = 0; i + 1 < RemapExternalToInternal.size(); ++i) {
        AFL_VERIFY(RemapExternalToInternal[i + 1].GetStartExt() == RemapExternalToInternal[i].GetStartExt() + RemapExternalToInternal[i].GetSize());
    }
    AFL_VERIFY(count == RecordsCount)("count", count)("records_count", RecordsCount);
    AFL_VERIFY(ColValue);
}

ui64 TSparsedArrayChunk::GetRawSize() const {
    return std::max<ui64>(NArrow::GetBatchDataSize(Records), 8);
}

std::shared_ptr<arrow::Scalar> TSparsedArrayChunk::GetScalar(const ui32 index) const {
    AFL_VERIFY(index < RecordsCount);
    for (ui32 idx = 0; idx < UI32ColIndex->length(); ++idx) {
        if (UI32ColIndex->Value(idx) == index) {
            return NArrow::TStatusValidator::GetValid(ColValue->GetScalar(idx));
        }
    }
    return DefaultValue;
}

ui32 TSparsedArrayChunk::GetFirstIndexNotDefault() const {
    if (UI32ColIndex->length()) {
        return GetUI32ColIndex()->Value(0);
    } else {
        return GetRecordsCount();
    }
}

std::shared_ptr<arrow::Scalar> TSparsedArrayChunk::GetMaxScalar() const {
    if (!ColValue->length()) {
        return DefaultValue;
    }
    auto minMax = NArrow::FindMinMaxPosition(ColValue);
    auto currentScalar = NArrow::TStatusValidator::GetValid(ColValue->GetScalar(minMax.second));
    if (!DefaultValue || ScalarCompare(DefaultValue, currentScalar) < 0) {
        return currentScalar;
    }
    return DefaultValue;
}

TSparsedArrayChunk TSparsedArrayChunk::ApplyFilter(const TColumnFilter& filter) const {
    AFL_VERIFY(!filter.IsTotalAllowFilter());
    AFL_VERIFY(!filter.IsTotalDenyFilter());
    if (UI32ColIndex->length() == 0) {
        return TSparsedArrayChunk(filter.GetFilteredCountVerified(), Records, DefaultValue);
    }
    AFL_VERIFY(filter.GetRecordsCountVerified() == RecordsCount)("filter", filter.GetRecordsCountVerified())("chunk", RecordsCount);
    ui32 recordIndex = 0;
    ui32 filterIntervalStart = 0;
    ui32 skippedCount = 0;
    ui32 filteredCount = 0;
    bool currentAcceptance = filter.GetStartValue();
    TColumnFilter filterNew = TColumnFilter::BuildAllowFilter();
    auto indexesBuilder = NArrow::MakeBuilder(arrow::uint32());
    auto valuesBuilder = NArrow::MakeBuilder(ColValue->type());
    for (auto it = filter.GetFilter().begin(); it != filter.GetFilter().end(); ++it) {
        for (; recordIndex < UI32ColIndex->length(); ++recordIndex) {
            if (UI32ColIndex->Value(recordIndex) < filterIntervalStart) {
                Y_ABORT_UNLESS(false);
            } else if (UI32ColIndex->Value(recordIndex) < filterIntervalStart + *it) {
                if (currentAcceptance) {
                    AFL_VERIFY(NArrow::Append<arrow::UInt32Type>(*indexesBuilder, UI32ColIndex->Value(recordIndex) - skippedCount));
                    AFL_VERIFY(NArrow::Append(*valuesBuilder, *ColValue, recordIndex));
                    ++filteredCount;
                }
            } else {
                break;
            }
        }
        if (!currentAcceptance) {
            skippedCount += *it;
        }
        currentAcceptance = !currentAcceptance;
        filterIntervalStart += *it;
    }
    AFL_VERIFY(filteredCount <= filter.GetFilteredCountVerified())("count", filteredCount)("filtered", filter.GetFilteredCountVerified());
    AFL_VERIFY(recordIndex == UI32ColIndex->length());
    auto indexesArr = NArrow::FinishBuilder(std::move(indexesBuilder));
    auto valuesArr = NArrow::FinishBuilder(std::move(valuesBuilder));
    std::shared_ptr<arrow::RecordBatch> result =
        arrow::RecordBatch::Make(TSparsedArray::BuildSchema(ColValue->type()), filteredCount, { indexesArr, valuesArr });
    return TSparsedArrayChunk(filter.GetFilteredCountVerified(), result, DefaultValue);
}

TSparsedArrayChunk TSparsedArrayChunk::Slice(const ui32 offset, const ui32 count) const {
    AFL_VERIFY(offset + count <= RecordsCount)("offset", offset)("count", count)("records", RecordsCount);
    std::optional<ui32> startPosition = NArrow::FindUpperOrEqualPosition(*UI32ColIndex, offset);
    std::optional<ui32> finishPosition = NArrow::FindUpperOrEqualPosition(*UI32ColIndex, offset + count);
    if (!startPosition || startPosition == finishPosition) {
        return TSparsedArrayChunk(count, NArrow::MakeEmptyBatch(Records->schema(), 0), DefaultValue);
    } else {
        AFL_VERIFY(startPosition);
        auto builder = NArrow::MakeBuilder(arrow::uint32());
        for (ui32 i = *startPosition; i < finishPosition.value_or(Records->num_rows()); ++i) {
            NArrow::Append<arrow::UInt32Type>(*builder, UI32ColIndex->Value(i) - offset);
        }
        auto arrIndexes = NArrow::FinishBuilder(std::move(builder));
        auto arrValue = ColValue->Slice(*startPosition, finishPosition.value_or(Records->num_rows()) - *startPosition);
        auto sliceRecords = arrow::RecordBatch::Make(Records->schema(), arrValue->length(), { arrIndexes, arrValue });
        return TSparsedArrayChunk(count, sliceRecords, DefaultValue);
    }
}

void TSparsedArray::TBuilder::AddChunk(const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& data) {
    AFL_VERIFY(data);
    AFL_VERIFY(recordsCount);
    AFL_VERIFY(data->num_rows() <= recordsCount)("rows", data->num_rows())("count", recordsCount);
    AFL_VERIFY(data->num_columns() == 2)("count", data->num_columns());
    AFL_VERIFY(data->column(0)->type_id() == arrow::uint32()->id())("type", data->column(0)->type()->ToString());
    AFL_VERIFY(data->column(1)->type_id() == Type->id())("type", Type->ToString())("ext_type", data->column(0)->type()->ToString());
    AFL_VERIFY_DEBUG(data->schema()->field(0)->name() == "index")("name", data->schema()->field(0)->name());
    if (data->num_rows()) {
        auto* arr = static_cast<const arrow::UInt32Array*>(data->column(0).get());
        AFL_VERIFY(arr->Value(arr->length() - 1) < recordsCount)("val", arr->Value(arr->length() - 1))("count", recordsCount);
    }
    Chunks.emplace_back(recordsCount, data, DefaultValue);
    RecordsCount += recordsCount;
    AFL_VERIFY(Chunks.size() == 1);
}

void TSparsedArray::TBuilder::AddChunk(
    const ui32 recordsCount, const std::shared_ptr<arrow::Array>& indexes, const std::shared_ptr<arrow::Array>& values) {
    AFL_VERIFY(indexes);
    AFL_VERIFY(values);
    AFL_VERIFY(recordsCount);
    AFL_VERIFY(indexes->length() == values->length())("indexes", indexes->length())("values", values->length());
    AFL_VERIFY(indexes->length() <= recordsCount)("indexes", indexes->length())("count", recordsCount);
    AFL_VERIFY(indexes->type_id() == arrow::uint32()->id())("type", indexes->type()->ToString());
    AFL_VERIFY(values->type_id() == Type->id())("type", Type->ToString())("ext_type", values->type()->ToString());
    if (indexes->length()) {
        auto* arr = static_cast<const arrow::UInt32Array*>(indexes.get());
        AFL_VERIFY(arr->Value(arr->length() - 1) < recordsCount)("val", arr->Value(arr->length() - 1))("count", recordsCount);
    }
    Chunks.emplace_back(
        recordsCount, arrow::RecordBatch::Make(BuildSchema(Type), indexes->length(), { indexes, values }), DefaultValue);
    RecordsCount += recordsCount;
    AFL_VERIFY(Chunks.size() == 1);
}

}   // namespace NKikimr::NArrow::NAccessor
