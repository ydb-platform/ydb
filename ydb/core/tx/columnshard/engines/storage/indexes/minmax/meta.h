#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {

class TIndexMeta: public TSkipIndex {
public:
    static TString GetClassNameStatic() {
        return "MINMAX";
    }

private:
    using TBase = TSkipIndex;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    bool DoIsAppropriateFor(const NArrow::NSSA::TIndexCheckOperation& op) const override;

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override;
    virtual std::vector<std::shared_ptr<NChunks::TPortionIndexChunk>> DoBuildIndexImpl(
        TChunkedBatchReader& reader, const ui32 recordsCount) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override;

    bool Skip(NArrow::NAccessor::TMinMax chunkValue, const std::shared_ptr<arrow::Scalar>& requestValue,
        const NArrow::NSSA::TIndexCheckOperation& op) const;

    virtual bool DoCheckValue(const TString& data, const std::optional<ui64> cat,
        const std::shared_ptr<arrow::Scalar>& requestValue, const NArrow::NSSA::TIndexCheckOperation& op, const TIndexInfo& info) const override;

    NJson::TJsonValue DoSerializeDataToJson(const TString& data, const TIndexInfo& indexInfo) const override;

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override;

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const bool inheritPortionStorage, const ui32& columnId,
        TReadDataExtractorContainer dataExtractor)
        : TBase(indexId, indexName, columnId, storageId, inheritPortionStorage, dataExtractor)
    {
    }

    static bool IsAvailableType(const NScheme::TTypeInfo type);

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NMinMax
