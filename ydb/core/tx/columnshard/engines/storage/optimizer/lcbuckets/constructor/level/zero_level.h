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
    std::optional<ui64> PortionsCountAvailable;
    std::optional<ui64> PortionsCountLimit;
    std::optional<ui64> PortionsSizeLimit;

    virtual std::shared_ptr<IPortionsLevel> DoBuildLevel(const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel,
        const std::shared_ptr<TSimplePortionsGroupInfo>& portionsInfo, const TLevelCounters& counters,
        const std::vector<std::shared_ptr<IPortionsSelector>>& selectors) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) override;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const override;

    static const inline TFactory::TRegistrator<TZeroLevelConstructor> Registrator =
        TFactory::TRegistrator<TZeroLevelConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
