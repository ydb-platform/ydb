#pragma once
#include "constructor.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TEmptySelectorConstructor: public ISelectorConstructor {
public:
    static TString GetClassNameStatic() {
        return "Empty";
    }

private:
    virtual std::shared_ptr<IPortionsSelector> DoBuildSelector() const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) override;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionSelectorConstructorContainer& proto) const override;

    const static inline auto Registrator = TFactory::TRegistrator<TEmptySelectorConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
