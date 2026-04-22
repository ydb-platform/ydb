#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_defaults.h>

namespace NKikimr::NOlap::NIndexes {

class TBloomIndexMeta: public TSkipBitmapIndex {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }

private:
    using TBase = TSkipBitmapIndex;
    std::shared_ptr<arrow::Schema> ResultSchema;
    double FalsePositiveProbability = NDefaults::FalsePositiveProbability;
    ui32 HashesCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexMeta>(GetClassNameStatic());
    [[nodiscard]] bool Initialize();

    virtual std::optional<ui64> DoCalcCategory(const TString& subColumnName) const override;

    virtual bool DoIsAppropriateFor(const NArrow::NSSA::TIndexCheckOperation& op) const override {
        return op.GetOperation() == EOperation::Equals && op.GetCaseSensitive();
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override;
    virtual std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> DoBuildIndexImpl(
        TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override;

    virtual bool DoCheckValueImpl(const IBitsStorageViewer& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
        const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo&) const override;

public:
    TBloomIndexMeta() = default;
    TBloomIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionStorage,
        const ui32 columnId, const double fpProbability, const TReadDataExtractorContainer& dataExtractor,
        const std::shared_ptr<IBitsStorageConstructor>& bitsStorageConstructor)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionStorage, dataExtractor, bitsStorageConstructor)
        , FalsePositiveProbability(fpProbability)
    {
        AFL_VERIFY(Initialize());
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
