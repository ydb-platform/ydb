#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/abstract.h>
namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TBloomIndexConstructor: public IIndexMetaConstructor {
public:
    static TString GetClassNameStatic() {
        return "CATEGORY_BLOOM_FILTER";
    }

private:
    std::set<TString> ColumnNames;
    double FalsePositiveProbability = 0.1;
    TReadDataExtractorContainer DataExtractor;
    static inline auto Registrator = TFactory::TRegistrator<TBloomIndexConstructor>(GetClassNameStatic());

protected:
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const ui32 indexId, const TString& indexName,
        const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const override;

    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const override;

public:
    TBloomIndexConstructor() = default;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes
