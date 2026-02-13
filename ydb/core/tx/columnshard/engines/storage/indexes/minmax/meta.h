#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

#include <ydb/library/formats/arrow/scalar/serialization.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/common/print_debug.h>
namespace NKikimr::NOlap::NIndexes::NMinMax {
struct TKeyPair {
    std::shared_ptr<arrow::Scalar> Min;
    std::shared_ptr<arrow::Scalar> Max;
};

namespace NArrowProtocol {
constexpr static const char* MaxFieldName = "Max";
constexpr static const char* MinFieldName = "Min";
inline TString Serialize(TKeyPair typedPair) {
    TString minSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(typedPair.Min).DetachResult();
    TString maxSerialized = NArrow::NScalar::TSerializer::SerializePayloadToString(typedPair.Max).DetachResult();
    TString res;
    auto writeSingle = [&](TStringBuf data) {
        ui64 dataSize = data.size();
        ui64 resSize = res.size();
        res.resize(resSize + sizeof(dataSize));
        memcpy(res.MutRef().data() + resSize, &dataSize, sizeof(dataSize));
        res.append(data);
    };
    writeSingle(minSerialized);
    writeSingle(maxSerialized);
    return res;
}
inline TKeyPair Deserialize(TStringBuf data, const std::shared_ptr<arrow::DataType>& type) {
    TKeyPair typed;
    ui64 offset = 0;
    auto readNext = [&] {
        ui64 size = 0;
        AFL_VERIFY(offset + sizeof(size) <= data.size())("details", Sprintf("out of bounds read, data.size(): %i, read: %i", data.size(), offset + sizeof(size)));
        memcpy(&size, data.data() + offset, sizeof(size));
        offset += sizeof(size);
        AFL_VERIFY(offset + size <= data.size())("details", Sprintf("out of bounds read, data.size(): %i, read: %i", data.size(), offset + size));
        auto res =  NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(TStringBuf{data.data() + offset, data.data() + offset + size}, type).DetachResult();
        offset += size;
        return res;
    };
    typed.Min = readNext();
    typed.Max = readNext();

    return typed;
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
    mutable std::shared_ptr<arrow::DataType> MinMaxType;
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
        AFL_VERIFY(reader.GetColumnsCount() == 1)("got_count", reader.GetColumnsCount());
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
        MinMaxType = thisChunkIndex.Max->type;
        AFL_VERIFY(thisChunkIndex.Max->type->Equals(thisChunkIndex.Min->type));

        auto serializedIndex = NArrowProtocol::Serialize(thisChunkIndex);
        // const TString indexData = NArrow::NScalar::TSerializer::SerializePayloadToString(serializedIndex).DetachResult();
        return { std::make_shared<NChunks::TPortionIndexChunk>(TChunkAddress(GetIndexId(), 0), recordsCount, serializedIndex.size(), serializedIndex) };
    }

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasMinMaxIndex());
        auto& minMax = proto.GetMinMaxIndex();
        AddColumnId(minMax.GetColumnId());
        if (!MutableDataExtractor().DeserializeFromProto(minMax.GetDataExtractor())){
            return false;
        }
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
            // auto requestValue = NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(requestValueBuf, MinMaxStructType->field(0)->type()).DetachResult();
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
            // TKeyPair requestInterval = NArrowProtocol::Deserialize(requestValueBuf, MinMaxStructType.get());
            auto* structValue = dynamic_cast<arrow::StructScalar*>(requestValue.get());
            AFL_VERIFY(structValue)("details", MySprintf("mismatched type of request value, expected arrow::StructType with Min and Max fields for interval operation: %s", op.DebugString()));
            TKeyPair requestInterval;
            requestInterval.Max = structValue->field(NArrowProtocol::MaxFieldName).ValueOrDie();
            requestInterval.Min = structValue->field(NArrowProtocol::MinFieldName).ValueOrDie();
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
        TKeyPair chunkValue = NArrowProtocol::Deserialize(data, MinMaxType);

        return !Skip(chunkValue, requestValue, op);
    }

    NJson::TJsonValue DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const override {
        auto gotType = indexInfo.GetColumnFeaturesVerified(GetColumnId()).GetArrowField()->type();
        AFL_VERIFY(MinMaxType->Equals(gotType))(
            "arrow error", MySprintf("inconsistent type field in TIndexInfo: TIndexInfo: %s, *this: %s", gotType->ToString(), MinMaxType->ToString()));
        return NArrow::NScalar::TSerializer::DeserializeFromStringWithPayload(data, MinMaxType).DetachResult()->ToString();
    }

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableMinMaxIndex();
        filterProto->SetColumnId(GetColumnId());
        *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
    }

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionStorage, const ui32& columnId, TReadDataExtractorContainer dataExtractor)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionStorage, dataExtractor)
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
