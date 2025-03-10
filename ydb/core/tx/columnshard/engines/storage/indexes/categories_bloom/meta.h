#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "CATEGORY_BLOOM_FILTER";
    }

private:
    using TBase = TIndexByColumns;
    double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
    }

    virtual TConclusion<std::shared_ptr<IIndexHeader>> DoBuildHeader(const TChunkOriginalData& data) const override;

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
    virtual void DoFillIndexCheckers(
        const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const override;

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

public:
    TIndexMeta() = default;
    TIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, const ui32 columnId, const double fpProbability,
        const TReadDataExtractorContainer& dataExtractor)
        : TBase(indexId, indexName, columnId, storageId, dataExtractor)
        , FalsePositiveProbability(fpProbability) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom
