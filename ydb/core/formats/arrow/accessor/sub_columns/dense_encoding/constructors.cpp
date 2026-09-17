#include "constructors.h"
#include "encoding.h"

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

namespace {

std::shared_ptr<arrow::util::Codec> GetCompressionCodec(const TChunkConstructionData& externalInfo) {
    return externalInfo.GetDefaultSerializer()->GetCompressionCodec();
}

}   // namespace

TBlobWithAdditionalAccessorData TBinaryDenseConstructor::DoSerializeToBlobAndMeta(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(columnData->GetType() == IChunkedArray::EType::Array)("array_type", columnData->GetType());
    const auto* trivial = static_cast<const TTrivialArray*>(columnData.get());
    AFL_VERIFY(arrow::is_binary_like(trivial->GetArray()->type_id()))("element_type", trivial->GetArray()->type()->ToString());
    const auto& binary = static_cast<const arrow::BinaryArray&>(*trivial->GetArray());
    return { SerializeBinaryLikeArray(binary, GetCompressionCodec(externalInfo)),
        std::make_shared<TEmptyAdditionalData>() };
}

TConclusion<std::shared_ptr<IChunkedArray>> TBinaryDenseConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    auto array = DeserializeBinaryLikeArray(
        originalData, externalInfo.GetRecordsCount(), externalInfo.GetColumnType(), GetCompressionCodec(externalInfo));
    return std::make_shared<TTrivialArray>(array);
}

TBlobWithAdditionalAccessorData TDictionaryDenseConstructor::DoSerializeToBlobAndMeta(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(columnData->GetType() == IChunkedArray::EType::Dictionary)("type", columnData->GetType());
    const auto* dict = static_cast<const TDictionaryArray*>(columnData.get());
    const auto& dictionary = dict->GetDictionary();
    AFL_VERIFY(arrow::is_binary_like(dictionary->type_id()))("element_type", dictionary->type()->ToString());
    const auto& dictBinary = static_cast<const arrow::BinaryArray&>(*dictionary);
    const auto codec = GetCompressionCodec(externalInfo);
    const TString dictBlob = SerializeBinaryLikeArray(dictBinary, codec);

    AFL_VERIFY(dictBinary.length() <= Max<ui32>())("length", dictBinary.length());
    const ui32 dictLength = static_cast<ui32>(dictBinary.length());
    const TString positionsBlob = SerializeIndices(
        dict->GetPositions(), NDictionary::TConstructor::GetTypeByVariantsCount(dictLength), codec);

    // [dictionary length][dictionary blob][positions blob]. The metadata stores the dictionary boundary.

    AFL_VERIFY(sizeof(ui32) + dictBlob.size() <= Max<ui32>())("size", dictBlob.size());
    AFL_VERIFY(positionsBlob.size() <= Max<ui32>())("size", positionsBlob.size());
    const ui32 dictionaryBlobSize = static_cast<ui32>(sizeof(ui32) + dictBlob.size());
    TString result;
    result.reserve(sizeof(ui32) + dictBlob.size() + positionsBlob.size());
    result.append((const char*)&dictLength, sizeof(dictLength));
    result.append(dictBlob);
    result.append(positionsBlob);
    auto meta = std::make_shared<TDictionaryAccessorData>(dictionaryBlobSize, static_cast<ui32>(positionsBlob.size()));
    return { std::move(result), std::move(meta) };
}

TConclusion<std::shared_ptr<IChunkedArray>> TDictionaryDenseConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(externalInfo.HasAdditionalAccessorData());
    const auto* meta = dynamic_cast<const TDictionaryAccessorData*>(externalInfo.GetAdditionalAccessorData().get());
    AFL_VERIFY(meta);
    const ui32 dictionaryBlobSize = meta->DictionaryBlobSize;
    AFL_VERIFY(dictionaryBlobSize >= sizeof(ui32) && dictionaryBlobSize <= originalData.size());
    AFL_VERIFY(originalData.size() - dictionaryBlobSize == meta->PositionsBlobSize)
        ("computed", originalData.size() - dictionaryBlobSize)("meta", meta->PositionsBlobSize);

    ui32 dictLength;
    memcpy(&dictLength, originalData.data(), sizeof(dictLength));
    const TStringBuf dictBlob(originalData.data() + sizeof(ui32), dictionaryBlobSize - sizeof(ui32));
    const TStringBuf positionsBlob(originalData.data() + dictionaryBlobSize, originalData.size() - dictionaryBlobSize);
    const auto codec = GetCompressionCodec(externalInfo);

    auto dictionary = DeserializeBinaryLikeArray(dictBlob, dictLength, externalInfo.GetColumnType(), codec);
    std::shared_ptr<arrow::Array> positions = DeserializeIndices(
        positionsBlob, externalInfo.GetRecordsCount(), NDictionary::TConstructor::GetTypeByVariantsCount(dictLength), codec);
    return std::make_shared<TDictionaryArray>(dictionary, positions);
}

TConclusion<std::shared_ptr<arrow::Array>> TDictionaryDenseConstructor::DeserializeDictionaryOnly(
    const TString& dictionaryBlob, const TChunkConstructionData& externalInfo) {
    if (!externalInfo.HasAdditionalAccessorData()) {
        return TConclusionStatus::Fail("dense dictionary-only reader requires additional accessor data in chunk metadata");
    }
    const auto* meta = dynamic_cast<const TDictionaryAccessorData*>(externalInfo.GetAdditionalAccessorData().get());
    if (!meta) {
        return TConclusionStatus::Fail("dense dictionary-only reader requires TDictionaryAccessorData in chunk metadata");
    }
    const ui32 dictionaryBlobSize = meta->DictionaryBlobSize;
    if (dictionaryBlobSize < sizeof(ui32) || dictionaryBlob.size() < dictionaryBlobSize) {
        return TConclusionStatus::Fail(TStringBuilder{} << "dense dictionary blob too small: need at least " << dictionaryBlobSize
                                                        << ", got " << dictionaryBlob.size());
    }
    ui32 dictLength;
    memcpy(&dictLength, dictionaryBlob.data(), sizeof(dictLength));
    const TStringBuf dictBlob(dictionaryBlob.data() + sizeof(ui32), dictionaryBlobSize - sizeof(ui32));
    return DeserializeBinaryArray(dictBlob, dictLength, GetCompressionCodec(externalInfo));
}

TConclusion<std::shared_ptr<arrow::Array>> BuildDictionaryOnlyValues(
    const TConstructorContainer& constructor, const TString& dictionaryBlob, const TChunkConstructionData& externalInfo) {
    if (!constructor || constructor->GetType() != IChunkedArray::EType::Dictionary) {
        return std::shared_ptr<arrow::Array>();
    }
    if (dynamic_cast<const TDictionaryDenseConstructor*>(constructor.GetObjectPtr().get())) {
        return TDictionaryDenseConstructor::DeserializeDictionaryOnly(dictionaryBlob, externalInfo);
    }
    return NDictionary::TConstructor::BuildDictionaryOnlyReader(dictionaryBlob, externalInfo);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
