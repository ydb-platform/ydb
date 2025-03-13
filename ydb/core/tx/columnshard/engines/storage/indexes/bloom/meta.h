#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
namespace NKikimr::NOlap::NIndexes {

class TBloomIndexMeta: public TSkipIndex {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }

private:
    using TBase = TSkipIndex;
    std::shared_ptr<arrow::Schema> ResultSchema;
    double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(!ResultSchema);
        std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>(
            "", arrow::TypeTraits<arrow::BooleanType>::type_singleton()) };
        ResultSchema = std::make_shared<arrow::Schema>(fields);
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
    }

    virtual std::optional<ui64> DoCalcCategory(const TString& subColumnName) const override;

    virtual bool DoIsAppropriateFor(const TString& /*subColumnName*/, const EOperation op) const override {
        return op == EOperation::Equals;
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TBloomIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail(
                "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        return TBase::CheckSameColumnsForModification(newMeta);
    }
    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasBloomFilter());
        auto& bFilter = proto.GetBloomFilter();
        FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
        for (auto&& i : bFilter.GetColumnIds()) {
            AddColumnId(i);
        }
        if (!MutableDataExtractor().DeserializeFromProto(bFilter.GetDataExtractor())) {
            return false;
        }
        Initialize();
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomFilter();
        filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
        for (auto&& i : GetColumnIds()) {
            filterProto->AddColumnIds(i);
        }
        *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
    }

    virtual bool DoCheckValue(
        const TString& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const override;

public:
    TBloomIndexMeta() = default;
    TBloomIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId, const double fpProbability,
        const TReadDataExtractorContainer& dataExtractor)
        : TBase(indexId, indexName, columnId, storageId, dataExtractor)
        , FalsePositiveProbability(fpProbability) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
