#include "meta.h"

#include <ydb/core/formats/arrow/program/functions.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/formats/arrow/scalar/serialization.h>

#include <cstring>
#define AFL_VERIFY_UNREACHABLE(...) AFL_VERIFY(false)("error", "unreachable")

#define VALUE_OR_VERIFY(result) NKikimr::NArrow::TStatusValidator::GetValid(result)

namespace NKikimr::NOlap::NIndexes::NMinMax {
inline bool cmp(NKikimr::NKernels::EOperation op, const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    arrow::Datum res =
        VALUE_OR_VERIFY(arrow::compute::CallFunction(NKikimr::NArrow::NSSA::TSimpleFunction::GetFunctionName(op), { left, right }));
    return res.scalar_as<arrow::BooleanScalar>().value;
}

inline bool operator<(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::Less, left, right);
}

inline bool operator>(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::Greater, left, right);
}

inline bool operator<=(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::LessEqual, left, right);
}

inline bool operator>=(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return cmp(NKernels::EOperation::GreaterEqual, left, right);
}

namespace NSerializePair {
constexpr static const char* MaxFieldName = "Max";
constexpr static const char* MinFieldName = "Min";
using TSizeType = ui32;
inline TString Serialize(const NArrow::NAccessor::TMinMax& typedPair) {
    TString minSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(typedPair.Min).DetachResult();
    TString maxSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(typedPair.Max).DetachResult();
    TString res;
    auto writeNext = [&](TStringBuf data) {
        TSizeType dataSize = data.size();
        ui64 resSize = res.size();
        res.resize(resSize + sizeof(TSizeType));
        memcpy(res.MutRef().data() + resSize, &dataSize, sizeof(TSizeType));
        res.append(data);
    };
    writeNext(minSerialized);
    writeNext(maxSerialized);
    return res;
}
inline NArrow::NAccessor::TMinMax Deserialize(TStringBuf data, const std::shared_ptr<arrow::DataType>& type) {
    NArrow::NAccessor::TMinMax typed;
    ui64 offset = 0;
    auto readNext = [&] {
        TSizeType size = 0;
        AFL_VERIFY(offset <= data.size() && sizeof(TSizeType) <= data.size() - offset )("details",
                                                               Sprintf("out of bounds read, data.size(): %i, current_offset: %i, read size: %i",
                                                                   data.size(), offset, sizeof(TSizeType)));
        memcpy(&size, data.data() + offset, sizeof(size));
        offset += sizeof(TSizeType);
        AFL_VERIFY(offset <= data.size() && size <= data.size() - offset)(
                                                  "details", Sprintf("out of bounds read, data.size(): %i, current_offset: %i, read size: %i",
                                                                 data.size(), offset, size));
        auto res =
            NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(TStringBuf{ data.data() + offset, data.data() + offset + size }, type)
                .DetachResult();
        offset += size;
        return res;
    };
    typed.Min = readNext();
    typed.Max = readNext();

    return typed;
}

}   // namespace NSerializePair

bool TIndexMeta::DoIsAppropriateFor(const NArrow::NSSA::TIndexCheckOperation& op) const {
    switch (op.GetOperation()) {
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Equals:
            return true;
        case NArrow::NSSA::TIndexCheckOperation::EOperation::StartsWith:
        case NArrow::NSSA::TIndexCheckOperation::EOperation::EndsWith:
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Contains:
            return false;
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Less:
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Greater:
        case NArrow::NSSA::TIndexCheckOperation::EOperation::LessOrEqual:
        case NArrow::NSSA::TIndexCheckOperation::EOperation::GreaterOrEqual:
            return true;
        default:
            Y_VERIFY(false, "unhandled enum case");
    }
}

TConclusionStatus TIndexMeta::DoCheckModificationCompatibility(const IIndexMeta& newMeta) const {
    Y_UNUSED(newMeta);
    return TConclusionStatus::Fail("minmax index is not modifiable");
}
std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::DoBuildIndexImpl(
    TChunkedBatchReader& reader, const ui32 recordsCount) const {
    NArrow::NAccessor::TMinMax thisChunkIndex;
    AFL_VERIFY(reader.GetColumnsCount() == 1)("got_count", reader.GetColumnsCount());
    {
        TChunkedColumnReader cReader = *reader.begin();
        for (reader.Start(); cReader.IsCorrect(); cReader.ReadNextChunk()) {
            NArrow::NAccessor::TMinMax currentScalar = cReader.GetCurrentChunk()->GetMinMaxScalars();
            AFL_VERIFY(currentScalar.Max);
            if (!thisChunkIndex.Max || thisChunkIndex.Max < currentScalar.Max) {
                thisChunkIndex.Max = currentScalar.Max;
            }
            AFL_VERIFY(currentScalar.Min);
            if (!thisChunkIndex.Min || thisChunkIndex.Min > currentScalar.Min) {
                thisChunkIndex.Min = currentScalar.Min;
            }
        }
    }
    AFL_VERIFY(thisChunkIndex.Max->type->Equals(thisChunkIndex.Min->type));

    auto serializedIndex = NSerializePair::Serialize(thisChunkIndex);
    return { std::make_shared<NChunks::TPortionIndexChunk>(
        TChunkAddress(GetIndexId(), 0), recordsCount, serializedIndex.size(), serializedIndex) };
}

bool TIndexMeta::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
    AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
    AFL_VERIFY(proto.HasMinMaxIndex());
    auto& minMax = proto.GetMinMaxIndex();
    AddColumnId(minMax.GetColumnId());
    if (!MutableDataExtractor().DeserializeFromProto(minMax.GetDataExtractor())) {
        return false;
    }
    return true;
}
bool TIndexMeta::DoCheckValue(const TString& data, const std::optional<ui64> cat,
    const std::shared_ptr<arrow::Scalar>& requestValue, const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo& info) const {
    AFL_VERIFY(!cat.has_value())("error", "category shouldn't be passed to minmax index");
    NArrow::NAccessor::TMinMax chunkValue =
        NSerializePair::Deserialize(data, info.GetColumnFeaturesVerified(GetColumnId()).GetArrowField()->type());
    return !Skip(chunkValue, requestValue, op);
}

bool TIndexMeta::Skip(NArrow::NAccessor::TMinMax chunkValue, const std::shared_ptr<arrow::Scalar>& requestValue,
    const NArrow::NSSA::TIndexCheckOperation& op) const {
    switch (op.GetOperation()) {
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Equals:
            return requestValue < chunkValue.Min || requestValue > chunkValue.Max;
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Less:
            return requestValue <= chunkValue.Min;
        case NArrow::NSSA::TIndexCheckOperation::EOperation::Greater:
            return requestValue >= chunkValue.Max;
        case NArrow::NSSA::TIndexCheckOperation::EOperation::LessOrEqual:
            return requestValue < chunkValue.Min;
        case NArrow::NSSA::TIndexCheckOperation::EOperation::GreaterOrEqual:
            return requestValue > chunkValue.Max;
        default:
            AFL_VERIFY_UNREACHABLE();
    }
}
NJson::TJsonValue TIndexMeta::DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const {
    auto gotType = indexInfo.GetColumnFeaturesVerified(GetColumnId()).GetArrowField()->type();
    NArrow::NAccessor::TMinMax pair = NSerializePair::Deserialize(data, gotType);
    NJson::TJsonValue json;
    json.InsertValue(NSerializePair::MinFieldName, pair.Min ? pair.Min->ToString() : std::string{});
    json.InsertValue(NSerializePair::MaxFieldName, pair.Max ? pair.Max->ToString() : std::string{});
    return json;
}
void TIndexMeta::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
    auto* filterProto = proto.MutableMinMaxIndex();
    filterProto->SetColumnId(GetColumnId());
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NMinMax
