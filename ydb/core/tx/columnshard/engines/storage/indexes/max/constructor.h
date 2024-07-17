#pragma once
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>
namespace NKikimr::NOlap::NIndexes::NMax {

class TIndexConstructor: public IIndexMetaConstructor {
public:
    static TString GetClassNameStatic() {
        return "MAX";
    }
private:
    TString ColumnName;
    static inline auto Registrator = TFactory::TRegistrator<TIndexConstructor>(GetClassNameStatic());

protected:
    virtual std::shared_ptr<IIndexMeta> DoCreateIndexMeta(const ui32 indexId, const TString& indexName, const NSchemeShard::TOlapSchema& currentSchema, NSchemeShard::IErrorCollector& errors) const override;

    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& jsonInfo) override;

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& proto) const override;

public:
    TIndexConstructor() = default;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NIndexes