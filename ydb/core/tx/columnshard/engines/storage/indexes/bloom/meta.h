#pragma once
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/meta.h>
namespace NKikimr::NOlap::NIndexes {

class TBloomIndexMeta: public TIndexByColumns {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_FILTER";
    }
private:
    using TBase = TIndexByColumns;
    std::shared_ptr<arrow::Schema> ResultSchema;
    double FalsePositiveProbability = 0.1;
    ui32 HashesCount = 0;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexMeta>(GetClassNameStatic());
    void Initialize() {
        AFL_VERIFY(!ResultSchema);
        std::vector<std::shared_ptr<arrow::Field>> fields = {std::make_shared<arrow::Field>("", arrow::TypeTraits<arrow::BooleanType>::type_singleton())};
        ResultSchema = std::make_shared<arrow::Schema>(fields);
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
    }

    static const ui64 HashesConstructorP = ((ui64)2 << 31) - 1;
    static const ui64 HashesConstructorA = (ui64)2 << 16;

    template <class TActor>
    void BuildHashesSet(const ui64 originalHash, const TActor& actor) const {
        AFL_VERIFY(HashesCount < HashesConstructorP);
        for (ui32 b = 1; b < HashesCount; ++b) {
            const ui64 hash = (HashesConstructorA * originalHash + b) % HashesConstructorP;
            actor(hash);
        }
    }

    template <class TContainer, class TActor>
    void BuildHashesSet(const TContainer& originalHashes, const TActor& actor) const {
        AFL_VERIFY(HashesCount < HashesConstructorP);
        for (auto&& hOriginal : originalHashes) {
            BuildHashesSet(hOriginal, actor);
        }
    }

protected:
    virtual TConclusionStatus DoCheckModificationCompatibility(const IIndexMeta& newMeta) const override {
        const auto* bMeta = dynamic_cast<const TBloomIndexMeta*>(&newMeta);
        if (!bMeta) {
            return TConclusionStatus::Fail("cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
        }
        AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
        return TBase::CheckSameColumnsForModification(newMeta);
    }
    virtual void DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const override;

    virtual TString DoBuildIndexImpl(std::vector<TChunkedColumnReader>&& columnReaders) const override;

    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) override {
        AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
        AFL_VERIFY(proto.HasBloomFilter());
        auto& bFilter = proto.GetBloomFilter();
        FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
        for (auto&& i : bFilter.GetColumnIds()) {
            ColumnIds.emplace(i);
        }
        Initialize();
        return true;
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const override {
        auto* filterProto = proto.MutableBloomFilter();
        filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
        for (auto&& i : ColumnIds) {
            filterProto->AddColumnIds(i);
        }
    }

public:
    TBloomIndexMeta() = default;
    TBloomIndexMeta(const ui32 indexId, const TString& indexName, const TString& storageId, std::set<ui32>& columnIds, const double fpProbability)
        : TBase(indexId, indexName, columnIds, storageId)
        , FalsePositiveProbability(fpProbability) {
        Initialize();
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
