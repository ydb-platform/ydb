#include "accessor.h"

#include <ydb/core/formats/arrow/simple_arrays_cache.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/formats/arrow/splitter/simple.h>
#include <ydb/core/formats/arrow/save_load/saver.h>

namespace NKikimr::NArrow::NAccessor {

TSparsedArray::TSparsedArray(const IChunkedArray& defaultArray, const std::shared_ptr<arrow::Scalar>& defaultValue)
    : TBase(defaultArray.GetRecordsCount(), EType::SparsedArray, defaultArray.GetDataType())
    , DefaultValue(defaultValue) {
    if (DefaultValue) {
        AFL_VERIFY(DefaultValue->type->id() == defaultArray.GetDataType()->id());
    }
    std::optional<TFullDataAddress> current;
    std::shared_ptr<arrow::RecordBatch> records;
    ui32 sparsedRecordsCount = 0;
    AFL_VERIFY(SwitchType(GetDataType()->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TScalar = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;
        auto builderValue = NArrow::MakeBuilder(GetDataType());
        TBuilder* builderValueImpl = (TBuilder*)builderValue.get();
        auto builderIndex = NArrow::MakeBuilder(arrow::uint32());
        arrow::UInt32Builder* builderIndexImpl = (arrow::UInt32Builder*)builderIndex.get();
        auto scalar = static_pointer_cast<TScalar>(DefaultValue);
        for (ui32 pos = 0; pos < GetRecordsCount();) {
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
                        AFL_VERIFY(false)("type", GetDataType()->ToString());
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
                        AFL_VERIFY(false)("type", GetDataType()->ToString());
                    }
                }
            }
            pos = current->GetAddress().GetGlobalFinishPosition();
            AFL_VERIFY(pos <= GetRecordsCount());
        }
        std::vector<std::shared_ptr<arrow::Array>> columns = { NArrow::TStatusValidator::GetValid(builderIndex->Finish()),
            NArrow::TStatusValidator::GetValid(builderValue->Finish()) };
        records = arrow::RecordBatch::Make(BuildSchema(GetDataType()), sparsedRecordsCount, columns);
        AFL_VERIFY_DEBUG(records->ValidateFull().ok());
        return true;
    }));
    AFL_VERIFY(records);
    Records.emplace_back(0, GetRecordsCount(), records, DefaultValue);
}

std::vector<NKikimr::NArrow::NAccessor::TChunkedArraySerialized> TSparsedArray::DoSplitBySizes(
    const TColumnSaver& saver, const TString& fullSerializedData, const std::vector<ui64>& splitSizes) {
    AFL_VERIFY(Records.size() == 1)("size", Records.size());
    auto chunks = NArrow::NSplitter::TSimpleSplitter(saver).SplitBySizes(Records.front().GetRecords(), fullSerializedData, splitSizes);

    std::vector<TChunkedArraySerialized> result;
    ui32 idx = 0;
    ui32 startIdx = 0;
    for (auto&& i : chunks) {
        AFL_VERIFY(i.GetSlicedBatch()->num_columns() == 2);
        AFL_VERIFY(i.GetSlicedBatch()->column(0)->type()->id() == arrow::uint32()->id());
        auto UI32Column = static_pointer_cast<arrow::UInt32Array>(i.GetSlicedBatch()->column(0));
        ui32 nextStartIdx = NArrow::NAccessor::TSparsedArray::GetLastIndex(i.GetSlicedBatch()) + 1;
        if (idx + 1 == chunks.size()) {
            nextStartIdx = GetRecordsCount();
        }
        std::shared_ptr<arrow::RecordBatch> batch;
        {
            std::unique_ptr<arrow::ArrayBuilder> builder = NArrow::MakeBuilder(arrow::uint32());
            arrow::UInt32Builder* builderImpl = (arrow::UInt32Builder*)builder.get();
            for (ui32 rowIdx = 0; rowIdx < UI32Column->length(); ++rowIdx) {
                TStatusValidator::Validate(builderImpl->Append(UI32Column->Value(rowIdx) - startIdx));
            }
            auto colIndex = TStatusValidator::GetValid(builder->Finish());
            batch = arrow::RecordBatch::Make(
                i.GetSlicedBatch()->schema(), i.GetSlicedBatch()->num_rows(), { colIndex, i.GetSlicedBatch()->column(1) });
        }

        ++idx;
        {
            TBuilder builder(DefaultValue, GetDataType());
            builder.AddChunk(nextStartIdx - startIdx, batch);
            result.emplace_back(builder.Finish(), saver.Apply(batch));
        }
        startIdx = nextStartIdx;
    }

    return result;
}

std::shared_ptr<arrow::Scalar> TSparsedArray::DoGetMaxScalar() const {
    std::shared_ptr<arrow::Scalar> result;
    for (auto&& i : Records) {
        auto scalarCurrent = i.GetMaxScalar();
        if (!scalarCurrent) {
            continue;
        }
        if (!result || ScalarCompare(result, scalarCurrent) < 0) {
            result = scalarCurrent;
        }
    }
    return result;
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

NKikimr::NArrow::NAccessor::TSparsedArrayChunk TSparsedArray::MakeDefaultChunk(
    const std::shared_ptr<arrow::Scalar>& defaultValue, const std::shared_ptr<arrow::DataType>& type, const ui32 recordsCount) {
    auto it = SimpleBatchesCache.find(type->ToString());
    if (it == SimpleBatchesCache.end()) {
        it = SimpleBatchesCache.emplace(type->ToString(), NArrow::MakeEmptyBatch(BuildSchema(type))).first;
        AFL_VERIFY(it->second->ValidateFull().ok());
    }
    return TSparsedArrayChunk(0, recordsCount, it->second, defaultValue);
}

IChunkedArray::TLocalDataAddress TSparsedArrayChunk::GetChunk(
    const std::optional<IChunkedArray::TCommonChunkAddress>& /*chunkCurrent*/, const ui64 position, const ui32 chunkIdx) const {
    const auto predCompare = [](const ui32 position, const TInternalChunkInfo& item) {
        return position < item.GetStartExt();
    };
    auto it = std::upper_bound(RemapExternalToInternal.begin(), RemapExternalToInternal.end(), position, predCompare);
    AFL_VERIFY(it != RemapExternalToInternal.begin());
    --it;
    if (it->GetIsDefault()) {
        return IChunkedArray::TLocalDataAddress(
            NArrow::TThreadSimpleArraysCache::Get(ColValue->type(), DefaultValue, it->GetSize()), StartPosition + it->GetStartExt(), chunkIdx);
    } else {
        return IChunkedArray::TLocalDataAddress(
            ColValue->Slice(it->GetStartInt(), it->GetSize()), StartPosition + it->GetStartExt(), chunkIdx);
    }
}

std::vector<std::shared_ptr<arrow::Array>> TSparsedArrayChunk::GetChunkedArray() const {
    std::vector<std::shared_ptr<arrow::Array>> chunks;
    for (auto&& i : RemapExternalToInternal) {
        if (i.GetIsDefault()) {
            chunks.emplace_back(NArrow::TThreadSimpleArraysCache::Get(ColValue->type(), DefaultValue, i.GetSize()));
        } else {
            chunks.emplace_back(ColValue->Slice(i.GetStartInt(), i.GetSize()));
        }
    }
    return chunks;
}

TSparsedArrayChunk::TSparsedArrayChunk(const ui32 posStart, const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& records,
    const std::shared_ptr<arrow::Scalar>& defaultValue)
    : RecordsCount(recordsCount)
    , StartPosition(posStart)
    , Records(records)
    , DefaultValue(defaultValue) {
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
        return StartPosition + GetUI32ColIndex()->Value(0);
    } else {
        return StartPosition + GetRecordsCount();
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

void TSparsedArray::TBuilder::AddChunk(const ui32 recordsCount, const std::shared_ptr<arrow::RecordBatch>& data) {
    AFL_VERIFY(data);
    AFL_VERIFY(recordsCount);
    AFL_VERIFY(data->num_rows() <= recordsCount)("rows", data->num_rows())("count", recordsCount);
    AFL_VERIFY(data->num_columns() == 2)("count", data->num_columns());
    AFL_VERIFY(data->column(0)->type_id() == arrow::uint32()->id())("type", data->column(0)->type()->ToString());
    AFL_VERIFY_DEBUG(data->schema()->field(0)->name() == "index")("name", data->schema()->field(0)->name());
    if (data->num_rows()) {
        auto* arr = static_cast<const arrow::UInt32Array*>(data->column(0).get());
        AFL_VERIFY(arr->Value(arr->length() - 1) < recordsCount)("val", arr->Value(arr->length() - 1))("count", recordsCount);
    }
    Chunks.emplace_back(RecordsCount, recordsCount, data, DefaultValue);
    RecordsCount += recordsCount;
}

}   // namespace NKikimr::NArrow::NAccessor
