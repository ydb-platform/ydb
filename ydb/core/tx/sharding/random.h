#pragma once
#include "sharding.h"

namespace NKikimr::NSharding {

class TRandomSharding: public TShardingBase {
private:
    using TBase = TShardingBase;
protected:
    virtual TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> DoBuildSplitShardsModifiers(const std::vector<ui64>& /*newTabletIds*/) const override {
        return TConclusionStatus::Fail("cannot split shards for random sharding");
    }

    virtual TConclusionStatus DoOnAfterModification() override {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoOnBeforeModification() override {
        return TConclusionStatus::Success();
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

};

}
