#pragma once
#include "constructor.h"

namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets {

class TOneLayerConstructor: public ILevelConstructor {
public:
    static TString GetClassNameStatic() {
        return "OneLayer";
    }

private:
    std::optional<double> BytesLimitFraction;
    std::optional<ui64> ExpectedPortionSize;

    virtual std::shared_ptr<IPortionsLevel> DoBuildLevel(const std::shared_ptr<IPortionsLevel>& nextLevel, const ui32 indexLevel,
        const std::shared_ptr<TSimplePortionsGroupInfo>& portionsInfo, const TLevelCounters& counters) const override;
    virtual TConclusionStatus DoDeserializeFromJson(const NJson::TJsonValue& json) override;
    virtual bool DoDeserializeFromProto(const NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) override;
    virtual void DoSerializeToProto(NKikimrSchemeOp::TCompactionLevelConstructorContainer& proto) const override;
    virtual bool IsEqualToSameClass(const ILevelConstructor& item) const override {
        const auto& itemCast = dynamic_cast<const TOneLayerConstructor&>(item);
        return BytesLimitFraction == itemCast.BytesLimitFraction && ExpectedPortionSize == itemCast.ExpectedPortionSize;
    }

    static const inline TFactory::TRegistrator<TOneLayerConstructor> Registrator =
        TFactory::TRegistrator<TOneLayerConstructor>(GetClassNameStatic());

public:
    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NLCBuckets
