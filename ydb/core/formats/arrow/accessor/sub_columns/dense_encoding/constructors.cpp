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

struct TParsedDenseDictionaryPrefix {
    ui32 DictLength = 0;
    TStringBuf DictBlob;
    const TDictionaryAccessorData* Meta = nullptr;
};

TConclusion<TParsedDenseDictionaryPrefix> ParseDenseDictionaryPrefix(
    const TString& data, const TChunkConstructionData& externalInfo) {
    if (!externalInfo.HasAdditionalAccessorData()) {
        return TConclusionStatus::Fail("dense dictionary-only reader requires additional accessor data in chunk metadata");
    }
    const auto* meta = dynamic_cast<const TDictionaryAccessorData*>(externalInfo.GetAdditionalAccessorData().get());
    if (!meta) {
        return TConclusionStatus::Fail("dense dictionary-only reader requires TDictionaryAccessorData in chunk metadata");
    }
    const ui32 dictionaryBlobSize = meta->DictionaryBlobSize;
    if (dictionaryBlobSize < sizeof(ui32) || data.size() < dictionaryBlobSize) {
        return TConclusionStatus::Fail(TStringBuilder{} << "dense dictionary blob too small: need at least " << dictionaryBlobSize
                                                        << ", got " << data.size());
    }
    TParsedDenseDictionaryPrefix parsed;
    parsed.Meta = meta;
    memcpy(&parsed.DictLength, data.data(), sizeof(parsed.DictLength));
    parsed.DictBlob = TStringBuf(data.data() + sizeof(ui32), dictionaryBlobSize - sizeof(ui32));
    return parsed;
}

std::shared_ptr<arrow::Array> DecodeDenseDictionaryValues(
    const TParsedDenseDictionaryPrefix& parsed, const TChunkConstructionData& externalInfo) {
    return DeserializeBinaryLikeArray(
        parsed.DictBlob, parsed.DictLength, externalInfo.GetColumnType(), GetCompressionCodec(externalInfo));
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
    auto parsedConclusion = ParseDenseDictionaryPrefix(originalData, externalInfo);
    AFL_VERIFY(parsedConclusion.IsSuccess())("error", parsedConclusion.GetErrorMessage());
    const auto& parsed = parsedConclusion.GetResult();
    AFL_VERIFY(originalData.size() - parsed.Meta->DictionaryBlobSize == parsed.Meta->PositionsBlobSize)
        ("computed", originalData.size() - parsed.Meta->DictionaryBlobSize)("meta", parsed.Meta->PositionsBlobSize);

    const TStringBuf positionsBlob(
        originalData.data() + parsed.Meta->DictionaryBlobSize, originalData.size() - parsed.Meta->DictionaryBlobSize);
    auto dictionary = DecodeDenseDictionaryValues(parsed, externalInfo);
    std::shared_ptr<arrow::Array> positions = DeserializeIndices(
        positionsBlob, externalInfo.GetRecordsCount(), NDictionary::TConstructor::GetTypeByVariantsCount(parsed.DictLength),
        GetCompressionCodec(externalInfo));
    return std::make_shared<TDictionaryArray>(dictionary, positions);
}

TConclusion<std::shared_ptr<arrow::Array>> BuildDictionaryOnlyValues(
    const TConstructorContainer& constructor, const TString& dictionaryBlob, const TChunkConstructionData& externalInfo) {
    if (!constructor || constructor->GetType() != IChunkedArray::EType::Dictionary) {
        return std::shared_ptr<arrow::Array>();
    }
    if (dynamic_cast<const TDictionaryDenseConstructor*>(constructor.GetObjectPtr().get())) {
        auto parsed = ParseDenseDictionaryPrefix(dictionaryBlob, externalInfo);
        if (parsed.IsFail()) {
            return TConclusionStatus::Fail(parsed.GetErrorMessage());
        }
        return DecodeDenseDictionaryValues(parsed.GetResult(), externalInfo);
    }
    return NDictionary::TConstructor::BuildDictionaryOnlyReader(dictionaryBlob, externalInfo);
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns
