#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

class TTxAddShardingInfo : public NTabletFlatExecutor::TTransactionBase<TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<TColumnShard>;
    NSharding::TGranuleShardingLogicContainer GranuleShardingLogic;
    const ui64 PathId;
    const ui64 ShardingVersion;
    std::optional<NOlap::TSnapshot> SnapshotVersion;

public:
    void SetSnapshotVersion(const NOlap::TSnapshot& ss) {
        SnapshotVersion = ss;
    }

    TTxAddShardingInfo(TColumnShard& owner, const NSharding::TGranuleShardingLogicContainer& granuleShardingLogic, const ui64 pathId, const ui64 version)
        : TBase(&owner)
        , GranuleShardingLogic(granuleShardingLogic)
        , PathId(pathId)
        , ShardingVersion(version)
    {
        AFL_VERIFY(!!GranuleShardingLogic);
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
};

}
