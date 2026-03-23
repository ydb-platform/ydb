#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/helper/index_defaults.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TIndexConstructor: public TSkipBitmapIndexConstructor {
private:
    using TBase = TSkipBitmapIndexConstructor;

public:
    static TString GetClassNameStatic() {
        return "BLOOM_NGRAMM_FILTER";
    }

private:
    ui32 NGrammSize = NDefaults::NGrammSize;
    double FalsePositiveProbability = NDefaults::FalsePositiveProbability;
    bool CaseSensitive = NDefaults::CaseSensitive;
    static inline auto Registrator = TFactory::TRegistrator<TIndexConstructor>(GetClassNameStatic());

protected:
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const ui32 indexId, const TString& indexName,
        const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const override;

    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) override;

private:
    TConclusionStatus ValidateValues() const;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const override;

public:
    TIndexConstructor() = default;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
