#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreShardsUpdate: public TInStoreTableUpdate {
private:
    using TBase = TInStoreTableUpdate;
    NKikimrSchemeOp::TAlterShards Alter;
    std::shared_ptr<TInStoreTable> TargetInStoreTable;
    std::set<ui64> ShardIds;
    std::set<ui64> NewShardIds;
    std::set<ui64> DeleteShardIds;
    std::set<ui64> ModifiedShardIds;
    std::shared_ptr<NSharding::IShardingBase> Sharding;
    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;
    void FillToShardTx(NKikimrTxColumnShard::TCreateTable& shardAlter) const;

    virtual TConclusionStatus DoFinishImpl(const TUpdateFinishContext& context) override;
    virtual std::shared_ptr<TColumnTableInfo> GetTargetTableInfo() const override {
        return TargetInStoreTable->GetTableInfoPtrVerified();
    }
    virtual std::shared_ptr<ISSEntity> GetTargetSSEntity() const override {
        return TargetInStoreTable;
    }

    virtual TString DoGetShardTxBodyString(const ui64 tabletId, const TMessageSeqNo& seqNo) const override {
        NKikimrTxColumnShard::TSchemaTxBody result;
        result.MutableSeqNo()->SetGeneration(seqNo.Generation);
        result.MutableSeqNo()->SetRound(seqNo.Round);
        AFL_VERIFY(NewShardIds.contains(tabletId) || ModifiedShardIds.contains(tabletId) || DeleteShardIds.contains(tabletId));
        if (NewShardIds.contains(tabletId)) {
            auto& alter = *result.MutableEnsureTables();
            auto& create = *alter.AddTables();
            FillToShardTx(create);
            create.SetPathId(TargetInStoreTable->GetPathId().LocalPathId);
        }
        if (DeleteShardIds.contains(tabletId)) {
            result.MutableDropTable()->SetPathId(TargetInStoreTable->GetPathId().LocalPathId);
        } else {
            auto container = Sharding->GetTabletShardingInfoOptional(tabletId);
            if (!!container) {
                auto& shardingInfo = *result.MutableGranuleShardingInfo();
                shardingInfo.SetPathId(TargetInStoreTable->GetPathId().LocalPathId);
                shardingInfo.SetVersionId(Sharding->GetShardInfoVerified(tabletId).GetShardingVersion());
                *shardingInfo.MutableContainer() = container.SerializeToProto();
                AFL_VERIFY(ModifiedShardIds.contains(tabletId));
            }
        }
        return result.SerializeAsString();
    }

    virtual std::set<ui64> DoGetShardIds() const override {
        return ShardIds;
    }

public:

};

}