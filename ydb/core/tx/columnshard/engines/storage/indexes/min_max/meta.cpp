#include "meta.h"

#include <ydb/core/formats/arrow/program/functions.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/min_max/misc/misc.h>

#include <ydb/library/arrow_kernels/operations.h>
#include <ydb/library/formats/arrow/scalar/serialization.h>

#include <cstring>

namespace NKikimr::NOlap::NIndexes::NMinMax {


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
            Y_ABORT("unhandled enum case");
    }
}

TConclusionStatus TIndexMeta::DoCheckModificationCompatibility(const IIndexMeta& newMeta) const {
    Y_UNUSED(newMeta);
    return TConclusionStatus::Fail("min_max index is not modifiable");
}

std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> TIndexMeta::DoBuildIndexImpl(
    TChunkedBatchReader& reader, const ui32 recordsCount) const {
    reader.Start();
    TChunkedColumnReader cReader = *reader.begin();
    auto thisChunkIndex = NArrow::NAccessor::TMinMax::MakeNull(cReader.GetCurrentChunk()->GetDataType());
    AFL_VERIFY(reader.GetColumnsCount() == 1)("got_count", reader.GetColumnsCount());
    {
        for (; cReader.IsCorrect(); cReader.ReadNextChunk()) {
            NArrow::NAccessor::TMinMax currentScalar = cReader.GetCurrentChunk()->GetMinMaxScalars();
            thisChunkIndex.UniteWith(currentScalar);
        }
    }

    TString serializedIndex = thisChunkIndex.ToBinaryString();
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
    AFL_VERIFY(!cat.has_value())("error", "category shouldn't be passed to min_max index");
    auto chunkValue = NArrow::NAccessor::TMinMax::FromBinaryString(data, info.GetColumnFeaturesVerified(GetColumnId()).GetArrowField()->type());
    if (chunkValue.ElementType()->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
        chunkValue.Min() = chunkValue.Min()->CastTo(arrow::uint64()).ValueOrDie();
        chunkValue.Max() = chunkValue.Max()->CastTo(arrow::uint64()).ValueOrDie();
    }
    return !Skip(chunkValue, requestValue, op);
}

bool TIndexMeta::Skip(NArrow::NAccessor::TMinMax chunkValue, const std::shared_ptr<arrow::Scalar>& requestValue,
    const NArrow::NSSA::TIndexCheckOperation& op) const {
    if (!requestValue->is_valid) { // predicate is of form "where col = null"; 
        return false; // cant do much in this case 
    }
    
    if (!chunkValue.Min()->is_valid) {
        return true;
    } else {
        switch (op.GetOperation()) {
            case NArrow::NSSA::TIndexCheckOperation::EOperation::Equals:
                return NArrow::NAccessor::NArrowCompare::Less(requestValue, chunkValue.Min()) || NArrow::NAccessor::NArrowCompare::Greater(requestValue, chunkValue.Max());
            case NArrow::NSSA::TIndexCheckOperation::EOperation::Less:
                return NArrow::NAccessor::NArrowCompare::LessOrEqual(requestValue, chunkValue.Min());
            case NArrow::NSSA::TIndexCheckOperation::EOperation::Greater:
                return NArrow::NAccessor::NArrowCompare::GreaterOrEqual(requestValue, chunkValue.Max());
            case NArrow::NSSA::TIndexCheckOperation::EOperation::LessOrEqual:
                return NArrow::NAccessor::NArrowCompare::Less(requestValue, chunkValue.Min());
            case NArrow::NSSA::TIndexCheckOperation::EOperation::GreaterOrEqual:
                return NArrow::NAccessor::NArrowCompare::Greater(requestValue, chunkValue.Max());
            default:
                Y_ABORT("unexpected operation for min_max index");
        }
    }
}

bool TIndexMeta::IsAvailableType(const NScheme::TTypeInfo type) {
    auto dataTypeResult = NArrow::GetArrowType(type);
    if (!dataTypeResult.ok()) {
        return false;
    }
    auto typedId = (*dataTypeResult)->id();
    return arrow::is_primitive(typedId) || arrow::is_base_binary_like(typedId);
}

NJson::TJsonValue TIndexMeta::DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const {
    const auto& colFeatures = indexInfo.GetColumnFeaturesVerified(GetColumnId());
    const auto minmax = NArrow::NAccessor::TMinMax::FromBinaryString(data, colFeatures.GetArrowField()->type());
    const auto typeId = colFeatures.GetTypeInfo().GetTypeId();

    NJson::TJsonValue json;

    if (minmax.IsNull()) {
        json.InsertValue("min", NJson::JSON_NULL);
        json.InsertValue("max", NJson::JSON_NULL);
        return json;
    }

    switch (typeId) {
        case NScheme::NTypeIds::Date:
            json.InsertValue("min", TInstant::Days(static_cast<const arrow::UInt16Scalar*>(minmax.Min().get())->value).ToString().substr(0, 10));
            json.InsertValue("max", TInstant::Days(static_cast<const arrow::UInt16Scalar*>(minmax.Max().get())->value).ToString().substr(0, 10));
            break;
        case NScheme::NTypeIds::Date32: {
            const i32 minDays = static_cast<const arrow::Int32Scalar*>(minmax.Min().get())->value;
            const i32 maxDays = static_cast<const arrow::Int32Scalar*>(minmax.Max().get())->value;
            json.InsertValue("min", minDays >= 0 ? TInstant::Days(minDays).ToString().substr(0, 10) : TString(minmax.Min()->ToString()));
            json.InsertValue("max", maxDays >= 0 ? TInstant::Days(maxDays).ToString().substr(0, 10) : TString(minmax.Max()->ToString()));
            break;
        }
        case NScheme::NTypeIds::Datetime:
            json.InsertValue("min", TInstant::Seconds(static_cast<const arrow::UInt32Scalar*>(minmax.Min().get())->value).ToString());
            json.InsertValue("max", TInstant::Seconds(static_cast<const arrow::UInt32Scalar*>(minmax.Max().get())->value).ToString());
            break;
        case NScheme::NTypeIds::Datetime64: {
            const i64 minSecs = static_cast<const arrow::Int64Scalar*>(minmax.Min().get())->value;
            const i64 maxSecs = static_cast<const arrow::Int64Scalar*>(minmax.Max().get())->value;
            json.InsertValue("min", minSecs >= 0 ? TInstant::Seconds(minSecs).ToString() : TString(minmax.Min()->ToString()));
            json.InsertValue("max", maxSecs >= 0 ? TInstant::Seconds(maxSecs).ToString() : TString(minmax.Max()->ToString()));
            break;
        }
        case NScheme::NTypeIds::Timestamp: {
            const i64 minMicros = static_cast<const arrow::TimestampScalar*>(minmax.Min().get())->value;
            const i64 maxMicros = static_cast<const arrow::TimestampScalar*>(minmax.Max().get())->value;
            json.InsertValue("min", minMicros >= 0 ? TInstant::MicroSeconds(minMicros).ToString() : TString(minmax.Min()->ToString()));
            json.InsertValue("max", maxMicros >= 0 ? TInstant::MicroSeconds(maxMicros).ToString() : TString(minmax.Max()->ToString()));
            break;
        }
        case NScheme::NTypeIds::Timestamp64: {
            const i64 minMicros = static_cast<const arrow::Int64Scalar*>(minmax.Min().get())->value;
            const i64 maxMicros = static_cast<const arrow::Int64Scalar*>(minmax.Max().get())->value;
            json.InsertValue("min", minMicros >= 0 ? TInstant::MicroSeconds(minMicros).ToString() : TString(minmax.Min()->ToString()));
            json.InsertValue("max", maxMicros >= 0 ? TInstant::MicroSeconds(maxMicros).ToString() : TString(minmax.Max()->ToString()));
            break;
        }
        default:
            json.InsertValue("min", TString(minmax.Min()->ToString()));
            json.InsertValue("max", TString(minmax.Max()->ToString()));
            break;
    }

    return json;
}

void TIndexMeta::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
    auto* filterProto = proto.MutableMinMaxIndex();
    filterProto->SetColumnId(GetColumnId());
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NMinMax
