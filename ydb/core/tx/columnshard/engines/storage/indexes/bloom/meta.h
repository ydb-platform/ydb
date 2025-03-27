#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

namespace NKikimr::NOlap::NIndexes {

class TBloomIndexMeta: public TSkipBitmapIndex {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }

private:
    using TBase = TSkipBitmapIndex;
    std::shared_ptr<arrow::Schema> ResultSchema;
    double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexMeta>(GetClassNameStatic());
    void Initialize();

    virtual std::optional<ui64> DoCalcCategory(const TString& subColumnName) const override;

    virtual bool DoIsAppropriateFor(const TString& /*subColumnName*/, const EOperation op) const override {
        return op == EOperation::Equals;
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override;
    virtual TString DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override;

    virtual bool DoCheckValueImpl(const IBitsStorage& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const EOperation op) const override;

public:
    TBloomIndexMeta() = default;
    TBloomIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId, const double fpProbability,
        const TReadDataExtractorContainer& dataExtractor, const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor)
        : TBase(indexId, indexName, columnId, storageId, dataExtractor, bitsStorageConstructor)
        , FalsePositiveProbability(fpProbability) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
