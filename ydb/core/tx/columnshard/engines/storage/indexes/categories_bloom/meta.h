#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TIndexMeta: public TSkipBitmapIndex {
public:
    static TString GetClassNameStatic() {
        return "CATEGORY_BLOOM_FILTER";
    }

private:
    using TBase = TSkipBitmapIndex;
    double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
    }

    virtual bool DoCheckValueImpl(const IBitsStorage& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const EOperation op) const override;

    virtual TConclusion<std::shared_ptr<IIndexHeader>> DoBuildHeader(const TChunkOriginalData& data) const override;

    virtual bool DoIsAppropriateFor(const TString& subColumnName, const EOperation op) const override {
        if (!subColumnName) {
            return false;
        }
        if (op != EOperation::Equals) {
            return false;
        }
        return true;
    }

    virtual std::optional<ui64> DoCalcCategory(const TString& subColumnName) const override;

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail(
                "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        return TBase::CheckSameColumnsForModification(newMeta);
    }
    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override;

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId, const double fpProbability,
        const TReadDataExtractorContainer& dataExtractor, const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor)
        : TBase(indexId, indexName, columnId, storageId, dataExtractor, bitsStorageConstructor)
        , FalsePositiveProbability(fpProbability) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom
