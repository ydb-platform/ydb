#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/dictionary/additional_data.h>
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NAccessor::NDictionary {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    if (!externalInfo.HasAdditionalAccessorData()) {
        return TConclusionStatus::Fail("dictionary blob requires additional accessor data in chunk metadata");
    }
    const TDictionaryAccessorData* dictData = dynamic_cast<const TDictionaryAccessorData*>(externalInfo.GetAdditionalAccessorData().get());
    if (!dictData) {
        return TConclusionStatus::Fail("dictionary blob requires TDictionaryAccessorData in chunk metadata");
    }
    const ui32 dictionaryBlobSize = dictData->DictionaryBlobSize;
    const ui32 positionsBlobSize = dictData->PositionsBlobSize;
    if (!dictionaryBlobSize || !positionsBlobSize) {
        return TConclusionStatus::Fail("dictionary blob requires non-zero DictionaryBlobSize and PositionsBlobSize in chunk metadata");
    }
    TStringBuf sbOriginal(originalData.data(), originalData.size());

    if (sbOriginal.size() < dictionaryBlobSize) {
        return TConclusionStatus::Fail("cannot read dictionary blob for dictionary accessor");
    }
    const TStringBuf blobDictionary(sbOriginal.data(), dictionaryBlobSize);
    sbOriginal.Skip(dictionaryBlobSize);

    if (sbOriginal.size() < positionsBlobSize) {
        return TConclusionStatus::Fail("cannot read positions blob for dictionary accessor");
    }
    const TStringBuf blobPositions(sbOriginal.data(), positionsBlobSize);
    sbOriginal.Skip(positionsBlobSize);

    auto schemaDictionary =
        std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    auto resultDictionary = externalInfo.GetDefaultSerializer()->Deserialize(TString(blobDictionary.data(), blobDictionary.size()), schemaDictionary);
    if (!resultDictionary.ok()) {
        return TConclusionStatus::Fail(TStringBuilder{}
            << "Internal deserialization error. type: dictionary (schema dictionary), schema: " << schemaDictionary->ToString()
            << " records count: " << externalInfo.GetRecordsCount()
            << " not null records count: " << (externalInfo.GetNotNullRecordsCount() ? ToString(*externalInfo.GetNotNullRecordsCount()) :  TString{"unknown"})
            << " reason: " << resultDictionary.status().ToString()
            << " original data: " << Base64Encode(TString(blobDictionary.data(), blobDictionary.size())));
    }
    auto rbDictionary = TStatusValidator::GetValid(resultDictionary);
    AFL_VERIFY(rbDictionary->num_columns() == 1);

    const std::shared_ptr<arrow::DataType> type = GetTypeByVariantsCount(rbDictionary->num_rows());
    auto schemaPositions = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", type) }));
    auto resultPositions = externalInfo.GetDefaultSerializer()->Deserialize(TString(blobPositions.data(), blobPositions.size()), schemaPositions);
    if (!resultPositions.ok()) {
        return TConclusionStatus::Fail(TStringBuilder{}
            << "Internal deserialization error. type: dictionary (schema positions), schema: " << schemaPositions->ToString()
            << " records count: " << externalInfo.GetRecordsCount()
            << " not null records count: " << (externalInfo.GetNotNullRecordsCount() ? ToString(*externalInfo.GetNotNullRecordsCount()) :  TString{"unknown"})
            << " dictionary count: " << rbDictionary->num_rows()
            << " reason: " << resultPositions.status().ToString()
            << " original data: " << Base64Encode(TString(blobPositions.data(), blobPositions.size())));
    }
    auto rbPositions = TStatusValidator::GetValid(resultPositions);
    AFL_VERIFY(rbPositions->num_columns() == 1);

    return std::make_shared<NArrow::NAccessor::TDictionaryArray>(rbDictionary->column(0), rbPositions->column(0));
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    return std::make_shared<NArrow::NAccessor::TDictionaryArray>(
        NArrow::TThreadSimpleArraysCache::Get(externalInfo.GetColumnType(), externalInfo.GetDefaultValue(), 1),
        NArrow::TThreadSimpleArraysCache::Get(arrow::uint8(), std::make_shared<arrow::UInt8Scalar>(0), externalInfo.GetRecordsCount()));
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    NKikimrArrowAccessorProto::TConstructor result;
    result.MutableDictionary();
    return result;
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& /*proto*/) {
    return true;
}

TBlobWithAdditionalAccessorData TConstructor::SerializeToBlobAndMeta(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) {
    const TDictionaryArray* arr = static_cast<const TDictionaryArray*>(columnData.get());
    auto arrDictionary = arr->GetDictionary();
    std::shared_ptr<arrow::Array> arrPositions = arr->GetPositions();
    const std::shared_ptr<arrow::DataType> requiredPositionsType = GetTypeByVariantsCount(arrDictionary->length());
    if (!arrPositions->type()->Equals(*requiredPositionsType)) {
        auto castResult = arrow::compute::Cast(*arrPositions, requiredPositionsType);
        AFL_VERIFY(castResult.ok())("error", castResult.status().ToString());
        arrPositions = std::move(*castResult);
    }
    auto schemaDictionary = NArrow::BuildFakeSchema({ arrDictionary });
    auto schemaPositions = NArrow::BuildFakeSchema({ arrPositions });
    const TString blobDictionary =
        externalInfo.GetDefaultSerializer()->SerializePayload(arrow::RecordBatch::Make(schemaDictionary, arrDictionary->length(), { arrDictionary }));
    const TString blobPositions =
        externalInfo.GetDefaultSerializer()->SerializePayload(arrow::RecordBatch::Make(schemaPositions, arrPositions->length(), { arrPositions }));

    auto meta = std::make_shared<TDictionaryAccessorData>(blobDictionary.size(), blobPositions.size());
    TString blob;
    blob.reserve(blobDictionary.size() + blobPositions.size());
    blob.append(blobDictionary);
    blob.append(blobPositions);
    return {std::move(blob), std::move(meta)};
}

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    return SerializeToBlobAndMeta(columnData, externalInfo).Blob;
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstruct(
    const std::shared_ptr<IChunkedArray>& originalArray, const TChunkConstructionData& externalInfo) const {
    if (!originalArray->GetDataType()->Equals(externalInfo.GetColumnType())) {
        return TConclusionStatus::Fail("dictionary accessor cannot convert types for transfer: " + originalArray->GetDataType()->ToString() +
                                       " to " + externalInfo.GetColumnType()->ToString());
    }
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    auto chunked = originalArray->GetChunkedArray();
    AFL_VERIFY(chunked->type()->id() == originalArray->GetDataType()->id());
    std::unique_ptr<arrow::ArrayBuilder> builderRecords;
    std::unique_ptr<arrow::ArrayBuilder> builderVariants = NArrow::MakeBuilder(originalArray->GetDataType());
    std::vector<i32> records;
    std::vector<i32> remap;
    AFL_VERIFY(SwitchType(chunked->type()->id(), [&](const auto type) {
        if constexpr (type.IsAppropriate) {
            bool hasNulls = false;
            std::map<typename decltype(type)::ValueType, ui32> indexByValue;
            for (ui32 chunk = 0; chunk < (ui32)chunked->num_chunks(); ++chunk) {
                auto chunkArr = chunked->chunk(chunk);
                auto typedArray = type.CastArray(chunkArr);
                for (ui32 pos = 0; pos < typedArray->length(); ++pos) {
                    if (typedArray->IsNull(pos)) {
                        records.emplace_back(-1);
                        hasNulls = true;
                        continue;
                    }
                    auto sv = type.GetValue(*typedArray, pos);
                    auto it = indexByValue.find(sv);
                    if (it == indexByValue.end()) {
                        it = indexByValue.emplace(sv, indexByValue.size()).first;
                    }
                    records.emplace_back(it->second);
                }
            }
            {
                auto* builder = type.CastBuilder(builderVariants.get());
                for (auto&& i : indexByValue) {
                    TStatusValidator::Validate(builder->Append(i.first));
                }
                if (hasNulls) {
                    TStatusValidator::Validate(builder->AppendNull());
                }
            }
            const ui32 variantsCount = indexByValue.size() + (hasNulls ? 1 : 0);
            auto recordsType = GetTypeByVariantsCount(variantsCount);
            builderRecords = NArrow::MakeBuilder(recordsType);
            remap.resize(indexByValue.size(), -1);
            ui32 idx = 0;
            for (auto&& i : indexByValue) {
                remap[i.second] = idx++;
            }
            return true;
        }
        AFL_VERIFY(false)("type", originalArray->GetDataType()->ToString());
        return false;
    }));
    AFL_VERIFY(records.size() == originalArray->GetRecordsCount());
    AFL_VERIFY(SwitchType(builderRecords->type()->id(), [&](const auto type) {
        auto* builder = type.CastBuilder(builderRecords.get());
        if constexpr (type.IsIndexType()) {
            using CType = typename decltype(type)::ValueType;
            for (auto&& r : records) {
                if (r < 0) {
                    TStatusValidator::Validate(builder->AppendNull());
                } else {
                    AFL_VERIFY((ui32)r < remap.size());
                    AFL_VERIFY(remap[r] >= 0);
                    TStatusValidator::Validate(builder->Append((CType)remap[r]));
                }
            }
            return true;
        }
        return false;
    }))("type", builderRecords->type()->ToString());
    auto arrVariants = NArrow::FinishBuilder(std::move(builderVariants));
    auto arrRecords = NArrow::FinishBuilder(std::move(builderRecords));
    return std::make_shared<TDictionaryArray>(arrVariants, arrRecords);
}

TConclusion<std::shared_ptr<arrow::Array>> TConstructor::BuildDictionaryOnlyReader(
    const TString& dictionaryBlob, const TChunkConstructionData& externalInfo) {
    if (!externalInfo.HasAdditionalAccessorData()) {
        return TConclusionStatus::Fail("dictionary-only reader requires additional accessor data in chunk metadata");
    }
    const TDictionaryAccessorData* dictData = dynamic_cast<const TDictionaryAccessorData*>(externalInfo.GetAdditionalAccessorData().get());
    if (!dictData) {
        return TConclusionStatus::Fail("dictionary-only reader requires TDictionaryAccessorData in chunk metadata");
    }
    if (dictionaryBlob.size() < dictData->DictionaryBlobSize) {
        return TConclusionStatus::Fail(TStringBuilder{}
            << "dictionary blob too small: need at least " << dictData->DictionaryBlobSize << ", got " << dictionaryBlob.size());
    }
    // Use only the dictionary prefix; blob may be full (dictionary + positions) when e.g. cached or read for other purposes.
    TStringBuf dictionaryPrefix(dictionaryBlob.data(), dictData->DictionaryBlobSize);
    auto schemaDictionary = std::make_shared<arrow::Schema>(arrow::FieldVector(
        {std::make_shared<arrow::Field>("val", externalInfo.GetColumnType(), true)}));
    auto result = externalInfo.GetDefaultSerializer()->Deserialize(TString(dictionaryPrefix), schemaDictionary);
    if (!result.ok()) {
        return TConclusionStatus::Fail(TStringBuilder{}
            << "dictionary-only deserialization failed: " << result.status().ToString());
    }
    auto rb = TStatusValidator::GetValid(result);
    AFL_VERIFY(rb->num_columns() == 1);
    return rb->column(0);
}

std::shared_ptr<arrow::DataType> TConstructor::GetTypeByVariantsCount(const ui32 count) {
    if (count <= Max<ui8>()) {
        return arrow::uint8();
    }
    if (count <= Max<ui16>()) {
        return arrow::uint16();
    }
    if (count <= Max<ui32>()) {
        return arrow::uint32();
    }
    AFL_VERIFY(false);
    return nullptr;
}

}   // namespace NKikimr::NArrow::NAccessor::NDictionary
