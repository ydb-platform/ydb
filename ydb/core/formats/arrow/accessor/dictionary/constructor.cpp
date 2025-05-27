#include "accessor.h"
#include "constructor.h"

#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>
#include <ydb/library/formats/arrow/simple_arrays_cache.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/table.h>

namespace NKikimr::NArrow::NAccessor::NDictionary {

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoDeserializeFromString(
    const TString& originalData, const TChunkConstructionData& externalInfo) const {
    TStringBuf sbOriginal(originalData.data(), originalData.size());
    if (sbOriginal.size() < sizeof(ui32)) {
        return TConclusionStatus::Fail("cannot READ protobuf blob size for dictionary accessor");
    }
    const ui32 protoSize = *(ui32*)(sbOriginal.data());
    sbOriginal.Skip(sizeof(protoSize));
    if (sbOriginal.size() < protoSize) {
        return TConclusionStatus::Fail("cannot READ protobuf blob for dictionary accessor");
    }
    NKikimrArrowAccessorProto::TDictionaryAccessor proto;
    if (!proto.ParseFromArray(sbOriginal.data(), protoSize)) {
        return TConclusionStatus::Fail("cannot PARSE protobuf blob for dictionary accessor");
    }
    sbOriginal.Skip(protoSize);

    if (sbOriginal.size() < proto.GetVariantsBlobSize()) {
        return TConclusionStatus::Fail("cannot READ variants blob for dictionary accessor");
    }
    const TStringBuf blobVariants(sbOriginal.data(), proto.GetVariantsBlobSize());
    sbOriginal.Skip(proto.GetVariantsBlobSize());

    if (sbOriginal.size() < proto.GetRecordsBlobSize()) {
        return TConclusionStatus::Fail("cannot READ records blob for dictionary accessor");
    }
    const TStringBuf blobRecords(sbOriginal.data(), proto.GetRecordsBlobSize());
    sbOriginal.Skip(proto.GetRecordsBlobSize());

    auto schemaVariants =
        std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", externalInfo.GetColumnType()) }));
    auto resultVariants = externalInfo.GetDefaultSerializer()->Deserialize(TString(blobVariants.data(), blobVariants.size()), schemaVariants);
    if (!resultVariants.ok()) {
        return TConclusionStatus::Fail(
            "cannot parse dictionary variants: " + resultVariants.status().ToString() + " as " + externalInfo.GetColumnType()->ToString());
    }
    auto rbVariants = TStatusValidator::GetValid(resultVariants);
    AFL_VERIFY(rbVariants->num_columns() == 1);

    const std::shared_ptr<arrow::DataType> type = GetTypeByVariantsCount(rbVariants->num_rows());
    auto schemaRecords = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", type) }));
    auto resultRecords = externalInfo.GetDefaultSerializer()->Deserialize(TString(blobRecords.data(), blobRecords.size()), schemaRecords);
    if (!resultRecords.ok()) {
        return TConclusionStatus::Fail(resultRecords.status().ToString());
    }
    auto rbRecords = TStatusValidator::GetValid(resultRecords);
    AFL_VERIFY(rbRecords->num_columns() == 1);

    return std::make_shared<NArrow::NAccessor::TDictionaryArray>(rbVariants->column(0), rbRecords->column(0));
}

TConclusion<std::shared_ptr<IChunkedArray>> TConstructor::DoConstructDefault(const TChunkConstructionData& externalInfo) const {
    return std::make_shared<NArrow::NAccessor::TDictionaryArray>(
        NArrow::TThreadSimpleArraysCache::Get(externalInfo.GetColumnType(), externalInfo.GetDefaultValue(), 1),
        NArrow::TThreadSimpleArraysCache::Get(arrow::uint8(), std::make_shared<arrow::UInt8Scalar>(0), externalInfo.GetRecordsCount()));
}

NKikimrArrowAccessorProto::TConstructor TConstructor::DoSerializeToProto() const {
    return NKikimrArrowAccessorProto::TConstructor();
}

bool TConstructor::DoDeserializeFromProto(const NKikimrArrowAccessorProto::TConstructor& /*proto*/) {
    return true;
}

TString TConstructor::DoSerializeToString(const std::shared_ptr<IChunkedArray>& columnData, const TChunkConstructionData& externalInfo) const {
    const TDictionaryArray* arr = static_cast<const TDictionaryArray*>(columnData.get());
    auto arrVariants = arr->GetVariants();
    auto arrRecords = arr->GetRecords();
    auto schemaVariants = NArrow::BuildFakeSchema({ arrVariants });
    auto schemaRecords = NArrow::BuildFakeSchema({ arrRecords });
    const TString blobVariants =
        externalInfo.GetDefaultSerializer()->SerializePayload(arrow::RecordBatch::Make(schemaVariants, arrVariants->length(), { arrVariants }));
    const TString blobRecords =
        externalInfo.GetDefaultSerializer()->SerializePayload(arrow::RecordBatch::Make(schemaRecords, arrRecords->length(), { arrRecords }));

    NKikimrArrowAccessorProto::TDictionaryAccessor proto;
    proto.SetVariantsBlobSize(blobVariants.size());
    proto.SetRecordsBlobSize(blobRecords.size());
    const TString blobProto = proto.SerializeAsString();

    TString result;
    result.reserve(sizeof(ui32) + blobProto.size() + blobVariants.size() + blobRecords.size());
    {
        TStringOutput sb(result);
        const ui32 protoSize = blobProto.size();
        sb.Write(&protoSize, sizeof(protoSize));
        sb.Write(blobProto.data(), blobProto.size());
        sb.Write(blobVariants.data(), blobVariants.size());
        sb.Write(blobRecords.data(), blobRecords.size());
    }
    return result;
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
            std::map<typename decltype(type)::ValueType, ui32> indexByValue;
            for (ui32 chunk = 0; chunk < (ui32)chunked->num_chunks(); ++chunk) {
                auto chunkArr = chunked->chunk(chunk);
                auto typedArray = type.CastArray(chunkArr);
                for (ui32 pos = 0; pos < typedArray->length(); ++pos) {
                    if (typedArray->IsNull(pos)) {
                        records.emplace_back(-1);
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
            }
            auto recordsType = GetTypeByVariantsCount(indexByValue.size());
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

std::shared_ptr<arrow::DataType> TConstructor::GetTypeByVariantsCount(const ui32 count) {
    if (count < Max<ui8>()) {
        return arrow::uint8();
    }
    if (count < Max<ui16>()) {
        return arrow::uint16();
    }
    if (count < Max<ui32>()) {
        return arrow::uint32();
    }
    AFL_VERIFY(false);
    return nullptr;
}

}   // namespace NKikimr::NArrow::NAccessor::NDictionary
