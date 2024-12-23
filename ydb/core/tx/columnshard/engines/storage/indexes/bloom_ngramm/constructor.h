#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>
namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TIndexConstructor: public IIndexMetaConstructor {
public:
    static TString GetClassNameStatic() {
        return "BLOOM_NGRAMM_FILTER";
    }

private:
    TString ColumnName;
    ui32 NGrammSize = 3;
    ui32 FilterSizeBytes = 512;
    ui32 HashesCount = 2;
    static inline auto Registrator = TFactory::TRegistrator<TIndexConstructor>(GetClassNameStatic());

protected:
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const ui32 indexId, const TString& indexName,
        const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const override;

    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const override;

public:
    TIndexConstructor() = default;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm
