#pragma once
#include "hash_sharding.h"

namespace NKikimr::NSharding {

class TLogsSharding : public THashShardingImpl {
public:
    static constexpr ui32 DEFAULT_ACITVE_SHARDS = 10;
    static constexpr TDuration DEFAULT_CHANGE_PERIOD = TDuration::Minutes(5);
    static TString GetClassNameStatic() {
        return "LOGS_SHARDING";
    }
private:
    using TBase = THashShardingImpl;
    ui32 NumActive = DEFAULT_ACITVE_SHARDS;
    ui64 TsMin = 0;
    ui64 ChangePeriod = DEFAULT_CHANGE_PERIOD.MicroSeconds();

    virtual void DoSerializeToProto(NKikimrSchemeOp::TColumnTableSharding& proto) const override {
        TBase::DoSerializeToProto(proto);
        proto.MutableHashSharding()->SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CLOUD_LOGS);
        proto.MutableHashSharding()->SetActiveShardsCount(NumActive);
    }

    virtual std::shared_ptr<IGranuleShardingLogic> DoGetTabletShardingInfoOptional(const ui64 /*tabletId*/) const override {
        return nullptr;
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TColumnTableSharding& proto) override;
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
        return TConclusionStatus::Fail("its impossible to modify logs sharding");
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }
public:
    TLogsSharding() = default;

    TLogsSharding(const std::vector<ui64>& shardIds, const std::vector<TString>& columnNames, ui32 shardsCountActive, TDuration changePeriod = DEFAULT_CHANGE_PERIOD)
        : TBase(shardIds, columnNames)
        , NumActive(Min<ui32>(shardsCountActive, GetShardsCount()))
        , TsMin(0)
        , ChangePeriod(changePeriod.MicroSeconds())
    {
        AFL_VERIFY(NumActive);
    }

    // tsMin = GetTsMin(tabletIdsMap, timestamp);
    // tabletIds = GetTableIdsByTs(tabletIdsMap, timestamp);
    // numIntervals = tabletIds.size() / nActive;
    // tsInterval = (timestamp - tsMin) / changePeriod;
    // shardNo = (hash(uid) % nActive) + (tsInterval % numIntervals) * nActive;
    // tabletId = tabletIds[shardNo];
    ui32 ShardNo(ui64 timestamp, const ui64 uidHash) const {
        const ui32 tsInterval = (timestamp - TsMin) / ChangePeriod;
        const ui32 numIntervals = std::max<ui32>(1, GetShardsCount() / NumActive);
        return ((uidHash % NumActive) + (tsInterval % numIntervals) * NumActive) % GetShardsCount();
    }

    virtual THashMap<ui64, std::vector<ui32>> MakeSharding(const std::shared_ptr<arrow::RecordBatch>& batch) const override;

};

}
