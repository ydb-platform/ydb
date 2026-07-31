#include "constructors.h"
#include "encoding.h"

#include <ydb/core/formats/arrow/accessor/common/additional_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>

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
    AFL_VERIFY(trivial->GetArray()->type_id() == arrow::Type::BINARY)("element_type", trivial->GetArray()->type()->ToString());
    const auto& binary = static_cast<const arrow::BinaryArray&>(*trivial->GetArray());
    return { SerializeBinaryArray(binary, GetCompressionCodec(externalInfo)),
        std::make_shared<TEmptyAdditionalData>() };
}

TConclusion<std::shared_ptr<IChunkedArray>> TBinaryDenseConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    auto array = DeserializeBinaryArray(originalData, externalInfo.GetRecordsCount(), GetCompressionCodec(externalInfo));
    return std::make_shared<TTrivialArray>(array);
}

TBlobWithAdditionalAccessorData TDictionaryDenseConstructor::DoSerializeToBlobAndMeta(
    const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(columnData->GetType() == IChunkedArray::EType::Dictionary)("type", columnData->GetType());
    const auto* dict = static_cast<const TDictionaryArray*>(columnData.get());
    const auto& dictBinary = static_cast<const arrow::BinaryArray&>(*dict->GetDictionary());
    const auto codec = GetCompressionCodec(externalInfo);
    const TString dictBlob = SerializeBinaryArray(dictBinary, codec);
    const TString positionsBlob = SerializeIndices(
        dict->GetPositions(), NDictionary::TConstructor::GetTypeByVariantsCount(dictBinary.length()), codec);

    // [dictionary length][dictionary blob][positions blob]. The metadata splits dictionary and positions blobs.
    const ui32 dictLength = dictBinary.length();
    TString result;
    result.reserve(sizeof(ui32) + dictBlob.size() + positionsBlob.size());
    result.append((const char*)&dictLength, sizeof(dictLength));
    result.append(dictBlob);
    result.append(positionsBlob);
    auto meta = std::make_shared<TDictionaryAccessorData>(sizeof(ui32) + dictBlob.size(), positionsBlob.size());
    return { std::move(result), std::move(meta) };
}

TConclusion<std::shared_ptr<IChunkedArray>> TDictionaryDenseConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    AFL_VERIFY(externalInfo.HasAdditionalAccessorData());
    const auto* meta = dynamic_cast<const TDictionaryAccessorData*>(externalInfo.GetAdditionalAccessorData().get());
    AFL_VERIFY(meta);
    AFL_VERIFY(meta->DictionaryBlobSize >= sizeof(ui32));
    AFL_VERIFY(meta->DictionaryBlobSize + meta->PositionsBlobSize == originalData.size());

    ui32 dictLength;
    memcpy(&dictLength, originalData.data(), sizeof(dictLength));
    const TStringBuf dictBlob(originalData.data() + sizeof(ui32), meta->DictionaryBlobSize - sizeof(ui32));
    const TStringBuf positionsBlob(originalData.data() + meta->DictionaryBlobSize, meta->PositionsBlobSize);
    const auto codec = GetCompressionCodec(externalInfo);

    auto dictionary = DeserializeBinaryArray(dictBlob, dictLength, codec);
    std::shared_ptr<arrow::Array> positions = DeserializeIndices(
        positionsBlob, externalInfo.GetRecordsCount(), NDictionary::TConstructor::GetTypeByVariantsCount(dictLength), codec);
    return std::make_shared<TDictionaryArray>(dictionary, positions);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
