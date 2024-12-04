#pragma once
#include "constructor.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TZeroLevelConstructor: public ILevelConstructor {
public:
    static TString GetClassNameStatic() {
        return "Zero";
    }

private:
    std::optional<TDuration> PortionsLiveDuration;
    std::optional<ui64> ExpectedBlobsSize;

    virtual std::shared_ptr<IPortionsLevel> DoBuildLevel(
        const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel, const TLevelCounters& counters) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) override;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const override;

    static const inline TFactory::TRegistrator<TZeroLevelConstructor> Registrator = TFactory::TRegistrator<TZeroLevelConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
