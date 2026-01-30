#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

#include <ydb/library/formats/arrow/scalar/serialization.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {
struct TKeyPair {
    std::shared_ptr<arrow::Scalar> Min;
    std::shared_ptr<arrow::Scalar> Max;
};

namespace NArrowProtocol {
constexpr static const char* MaxFieldName = "Max";
constexpr static const char* MinFieldName = "Min";
std::shared_ptr<arrow::Scalar> Serialize(TKeyPair typedDair) {
    auto res = arrow::StructScalar::Make({ typedDair.Max, typedDair.Min }, { "Max", "Min" });
    AFL_VERIFY(res.ok())("arrow error", Sprintf("MinMax struct creation error: %s", res.status().ToString().c_str()));
    return *res;
}
TKeyPair Deserialize(std::shared_ptr<arrow::Scalar> untypedPair) {
    auto struct_value = std::dynamic_pointer_cast<arrow::StructScalar>(untypedPair);
    TString typeErrorMessage = Sprintf("MinMax struct type error: expected arrow struct type with 2 field: [%s,%s], got: %s", MinFieldName,
        MaxFieldName, untypedPair->type->ToString().c_str());
    AFL_VERIFY(!struct_value)("arrow error", typeErrorMessage);
    arrow::Result<std::shared_ptr<arrow::Scalar>> min = struct_value->field(MinFieldName);
    arrow::Result<std::shared_ptr<arrow::Scalar>> max = struct_value->field(MaxFieldName);
    AFL_VERIFY(min.ok() && max.ok())("arrow error", typeErrorMessage);

    return TKeyPair(min.ValueOrDie(), max.ValueOrDie());
}

}   // namespace NArrowProtocol

inline bool operator<(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return NArrow::ScalarLess(left, right);
}

inline bool operator>(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return NArrow::ScalarGreater(left, right);
}

inline bool operator<=(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return NArrow::ScalarLessOrEqual(left, right);
}

inline bool operator>=(const std::shared_ptr<arrow::Scalar>& left, const std::shared_ptr<arrow::Scalar>& right) {
    return NArrow::ScalarGreaterOrEqual(left, right);
}

#define AFL_VERIFY_UNREACHABLE(...) AFL_VERIFY(false)("error", "unreachable")

class TIndexMeta: public TSkipIndex {
public:
    static TString GetClassNameStatic() {
        return "MINMAX";
    }

private:
    mutable std::shared_ptr<arrow::StructType> MinMaxStructType;
    using TBase = TSkipIndex;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    bool DoIsAppropriateFor(const NArrow::NSSA::TIndexCheckOperation& op) const override {
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
            case NArrow::NSSA::TIndexCheckOperation::EOperation::OpenInterval:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::ClosedInterval:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::LeftOnlyOpenInterval:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::RightOnlyOpenInterval:
                return true;
            default:
                Y_VERIFY(false, "unhandled enum case");
        }
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        Y_UNUSED(newMeta);
        return TConclusionStatus::Fail("minmax index is not modifiable");
    }
    virtual std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> DoBuildIndexImpl(
        TChunkedBatchReader& reader, const ui32 recordsCount) const override {
        TKeyPair thisChunkIndex;
        AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
        {
            TChunkedColumnReader cReader = *reader.begin();
            for (reader.Start(); cReader.IsCorrect(); cReader.ReadNextChunk()) {
                std::shared_ptr<arrow::Scalar> currentMaxScalar = cReader.GetCurrentChunk()->GetMaxScalar();
                std::shared_ptr<arrow::Scalar> currentMinScalar = cReader.GetCurrentChunk()->GetMinScalar();
                AFL_VERIFY(currentMaxScalar);
                if (!thisChunkIndex.Max || thisChunkIndex.Max < currentMaxScalar) {
                    thisChunkIndex.Max = currentMaxScalar;
                }
                AFL_VERIFY(currentMinScalar);
                if (!thisChunkIndex.Min || thisChunkIndex.Min > currentMinScalar) {
                    thisChunkIndex.Min = currentMinScalar;
                }
            }
        }

        auto serializedIndex = NArrowProtocol::Serialize(thisChunkIndex);
        const TString indexData = NArrow::NScalar::TSerializer::SerializePayloadToString(serializedIndex).DetachResult();
        return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, indexData.size(), indexData) };
    }

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasMinMaxIndex());
        auto& bFilter = proto.GetMinMaxIndex();
        if (!bFilter.HasColumnId()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", "incorrect column id");
            return false;
        };
        AddColumnId(bFilter.GetColumnId());
        return true;
    }
    enum class ValueShape {
        SingleValue,
        PairOfValues
    };
    ValueShape InputValueShape(const NArrow::NSSA::TIndexCheckOperation& op) const {
        AFL_VERIFY(DoIsAppropriateFor(op));
        switch (op.GetOperation()) {
            case NArrow::NSSA::TIndexCheckOperation::EOperation::Equals:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::Less:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::Greater:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::LessOrEqual:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::GreaterOrEqual:
                return ValueShape::SingleValue;
            case NArrow::NSSA::TIndexCheckOperation::EOperation::OpenInterval:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::ClosedInterval:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::LeftOnlyOpenInterval:
            case NArrow::NSSA::TIndexCheckOperation::EOperation::RightOnlyOpenInterval:
                return ValueShape::PairOfValues;
            default:
                AFL_VERIFY_UNREACHABLE();
        }
    }
    bool Skip(TKeyPair chunkValue, const std::shared_ptr<arrow::Scalar>& requestValue, const NArrow::NSSA::TIndexCheckOperation& op) const {
        if (InputValueShape(op) == ValueShape::SingleValue) {
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

        } else {
            TKeyPair requestInterval = NArrowProtocol::Deserialize(requestValue);
            switch (op.GetOperation()) {
                case NArrow::NSSA::TIndexCheckOperation::EOperation::OpenInterval:
                    return requestInterval.Max <= chunkValue.Min || requestInterval.Min >= chunkValue.Max;

                case NArrow::NSSA::TIndexCheckOperation::EOperation::ClosedInterval:
                    return requestInterval.Max < chunkValue.Min || requestInterval.Min > chunkValue.Max;

                case NArrow::NSSA::TIndexCheckOperation::EOperation::LeftOnlyOpenInterval:
                    return requestInterval.Max < chunkValue.Min || requestInterval.Min >= chunkValue.Max;

                case NArrow::NSSA::TIndexCheckOperation::EOperation::RightOnlyOpenInterval:
                    return requestInterval.Max <= chunkValue.Min || requestInterval.Min > chunkValue.Max;

                default:
                    AFL_VERIFY_UNREACHABLE();
            }
        }
    }
    virtual bool DoCheckValue(const TString& data, [[maybe_unused]] const std::optional<ui64> cat,
        const std::shared_ptr<arrow::Scalar>& requestValue, const NArrow::NSSA::TIndexCheckOperation& op) const override {
        AFL_VERIFY(!cat.has_value())("error", "category shouldn't be passed to minmax index");
        TKeyPair chunkValue = [&] {
            TConclusion<std::shared_ptr<arrow::Scalar>> value =
                NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(data, MinMaxStructType);
            AFL_VERIFY(value.IsSuccess())("arrow error", Sprintf("MinMax struct parsing error: %s", value.GetErrorMessage().c_str()));
            return NArrowProtocol::Deserialize(*value);
        }();

        return !Skip(chunkValue, requestValue, op);
    }

    NJson::TJsonValue DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const override {
        auto gotType = indexInfo.GetColumnFeaturesVerified(GetColumnId()).GetArrowField()->type();
        AFL_VERIFY(MinMaxStructType->GetFieldByName(NArrowProtocol::MaxFieldName)->type()->Equals(gotType))(
            "arrow error", "inconsistent type in Max field");
        AFL_VERIFY(MinMaxStructType->GetFieldByName(NArrowProtocol::MinFieldName)->type()->Equals(gotType))(
            "arrow error", "inconsistent type in Min field");
        return NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(data, MinMaxStructType).DetachResult()->ToString();
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableMinMaxIndex();
        filterProto->SetColumnId(GetColumnId());
    }

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionStorage, const ui32& columnId)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionStorage, std::make_shared<TDefaultDataExtractor>())
    {
    }

    static bool IsAvailableType(const NScheme::TTypeInfo type) {
        auto dataTypeResult = NArrow::GetArrowType(type);
        if (!dataTypeResult.ok()) {
            return false;
        }
        auto typedId = (*dataTypeResult)->id();
        return arrow::is_primitive(typedId) || arrow::is_base_binary_like(typedId);
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NMinMax
