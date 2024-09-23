#pragma once
#include "sharding.h"

namespace NKikimr::NSharding {

class TRandomSharding: public IShardingBase {
private:
    using TBase = IShardingBase;
    static TString GetClassNameStatic() {
        return "RANDOM";
    }
protected:
    virtual std::shared_ptr<IGranuleShardingLogic> DoGetTabletShardingInfoOptional(const ui64 /*tabletId*/) const override {
        return nullptr;
    }

    virtual TConclusionStatus DoOnAfterModification() override {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoOnBeforeModification() override {
        return TConclusionStatus::Success();
    }

    virtual std::set<ui64> DoGetModifiedShardIds(const NKikimrSchemeOp::TShardingModification& /*proto*/) const override {
        return {};
    }

    virtual TConclusionStatus DoApplyModification(const NKikimrSchemeOp::TShardingModification& /*proto*/) override {
        return TConclusionStatus::Fail("its impossible to modify random sharding");
    }
    virtual void DoSerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const override {
        proto.MutableRandomSharding();
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) override {
        if (!proto.HasRandomSharding()) {
            return TConclusionStatus::Fail("no random sharding data");
        }
        return TConclusionStatus::Success();
    }
public:
    using TBase::TBase;

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
};

}
