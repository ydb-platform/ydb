#pragma once
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/default.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/library/formats/arrow/scalar/serialization.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {

class TIndexMeta: public TSkipIndex {
public:
    static TString GetClassNameStatic() {
        return "MINMAX";
    }

private:
    std::shared_ptr<class T>
    using TBase = TSkipIndex;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    bool DoIsAppropriateFor(const NArrow::NSSA::TIndexCheckOperation& op) const override{
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
        case NArrow::NSSA::TIndexCheckOperation::EOperation::
            LeftOnlyOpenInterval:
        case NArrow::NSSA::TIndexCheckOperation::EOperation::
            RightOnlyOpenInterval:
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
        TChunkedBatchReader& reader, const ui32 recordsCount) const override{
        std::shared_ptr<arrow::Scalar> portionMax;
        std::shared_ptr<arrow::Scalar> portionMin;
        AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
        {
            TChunkedColumnReader cReader = *reader.begin();
            for (reader.Start(); cReader.IsCorrect(); cReader.ReadNextChunk()) {
                std::shared_ptr<arrow::Scalar> currentMaxScalar = cReader.GetCurrentChunk()->GetMaxScalar();
                std::shared_ptr<arrow::Scalar> currentMinScalar = cReader.GetCurrentChunk()->GetMinScalar();
                AFL_VERIFY(currentMaxScalar);
                if (!portionMax || NArrow::ScalarCompare(*portionMax, *currentMaxScalar) == -1){
                    portionMax = currentMaxScalar;
                }
                AFL_VERIFY(currentMinScalar);
                if (!portionMin || NArrow::ScalarCompare(portionMin, currentMinScalar) == 1) {
                    portionMin = currentMinScalar;
                }
            }
        }

        auto type = std::dynamic_pointer_cast<arrow::StructType>(arrow::struct_({std::make_shared<arrow::Field>("1", portionMax->type), std::make_shared<arrow::Field>("2", portionMax->type)}));
        auto ss = arrow::StructScalar::Make({portionMax, portionMin}, {"Max", "Min"});
        
        if (!ss.ok()) {
            AFL_VERIFY(false)("arrow error:", Sprintf("MinMax struct creation error: %s", ss.status().ToString().c_str()));
        }
        const TString indexData = NArrow::NScalar::TSerializer::SerializePayloadToString(portionMax).DetachResult();
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
    virtual bool DoCheckValue(const TString& data, const std::optional<ui64> cat, const std::shared_ptr<arrow::Scalar>& value,
        const NArrow::NSSA::TIndexCheckOperation& op) const override;

    virtual NJson::TJsonValue DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const override;

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
        return NArrow::SwitchType((*dataTypeResult)->id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                    return true;
                }
                return false;
            });
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    std::shared_ptr<arrow::Scalar> GetMaxScalarVerified(const std::vector<TString>& data, const std::shared_ptr<arrow::DataType>& type) const;
};

}   // namespace NKikimr::NOlap::NIndexes::NMax
