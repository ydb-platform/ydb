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
    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;
    void FillToShardTx(NKikimrTxColumnShard::TCreateTable& shardAlter) const;

    virtual std::shared_ptr<TColumnTableInfo> GetTargetTableInfo() const override {
        return TargetInStoreTable->GetTableInfoPtrVerified();
    }
    virtual std::shared_ptr<ISSEntity> GetTargetSSEntity() const override {
        return TargetInStoreTable;
    }

    virtual TString DoGetShardTxBodyString(const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const override {
        NKikimrTxColumnShard::TSchemaTxBody result;
        result.MutableSeqNo()->SetGeneration(seqNo.Generation);
        result.MutableSeqNo()->SetRound(seqNo.Round);
        auto& alter = *result.MutableEnsureTables();
        auto& create = *alter.AddTables();
        FillToShardTx(create);
        create.SetPathId(TargetInStoreTable->GetPathId().LocalPathId);
        return result.SerializeAsString();
    }

    virtual std::set<ui64> DoGetShardIds() const override {
        return ShardIds;
    }

public:

};

}