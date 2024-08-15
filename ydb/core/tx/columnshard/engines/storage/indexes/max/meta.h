#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
namespace NKikimr::NOlap::NIndexes::NMax {

class TIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "MAX";
    }
private:
    using TBase = TIndexByColumns;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        Y_UNUSED(newMeta);
        return TConclusionStatus::Fail("max index not modifiable");
    }
    virtual void DoFillIndexCheckers(
        const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const override;

    virtual TString DoBuildIndexImpl(std::vector<TChunkedColumnReader>&& columnReaders) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasMaxIndex());
        auto& bFilter = proto.GetMaxIndex();
        if (!bFilter.GetColumnId()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("problem", "incorrect column id");
            return false;
        };
        ColumnIds.emplace(bFilter.GetColumnId());
        return true;
    }

    virtual NJson::TJsonValue DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const override;

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        AFL_VERIFY(ColumnIds.size() == 1);
        auto* filterProto = proto.MutableMaxIndex();
        filterProto->SetColumnId(*ColumnIds.begin());
    }

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32& columnId)
        : TBase(indexId, indexName, { columnId }, storageId) {
    }

    ui32 GetColumnId() const {
        AFL_VERIFY(ColumnIds.size() == 1);
        return *ColumnIds.begin();
    }

    static bool IsAvailableType(const NScheme::TTypeInfo type) {
        auto dataTypeResult = NArrow::GetArrowType(type);
        if (!dataTypeResult.ok()) {
            return false;
        }
        if (!NArrow::SwitchType((*dataTypeResult)->id(), [&](const auto& type) {
                using TWrap = std::decay_t<decltype(type)>;
                if constexpr (arrow::has_c_type<typename TWrap::T>()) {
                    return true;
                }
                return false;
            })) {
            return false;
        }

        return true;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    std::shared_ptr<arrow::Scalar> GetMaxScalarVerified(const std::vector<TString>& data, const std::shared_ptr<arrow::DataType>& type) const;
};

}   // namespace NKikimr::NOlap::NIndexes
