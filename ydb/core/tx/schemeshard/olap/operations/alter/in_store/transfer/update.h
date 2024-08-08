#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/update.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/columnshard/data_sharing/protos/sessions.pb.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreShardsTransfer: public TInStoreTableUpdate {
private:
    using TBase = TInStoreTableUpdate;
    THashMap<ui64, NKikimrColumnShardDataSharingProto::TDestinationSession> DestinationSessions;
    std::shared_ptr<TInStoreTable> TargetInStoreTable;
    std::set<ui64> ShardIdsUsage;

    virtual std::shared_ptr<TColumnTableInfo> GetTargetTableInfo() const override {
        AFL_VERIFY(TargetInStoreTable);
        return TargetInStoreTable->GetTableInfoPtrVerified();
    }
    virtual std::shared_ptr<ISSEntity> GetTargetSSEntity() const override {
        return TargetInStoreTable;
    }

    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SHARING;
    }

    virtual TConclusionStatus DoInitializeImpl(const TUpdateInitializationContext& context) override;

    virtual TString DoGetShardTxBodyString(const ui64 tabletId, const TMessageSeqNo& /*seqNo*/) const override {
        auto it = DestinationSessions.find(tabletId);
        AFL_VERIFY(it != DestinationSessions.end());
        return it->second.SerializeAsString();
    }

    virtual std::set<ui64> DoGetShardIds() const override {
        return ShardIdsUsage;
    }

public:
};

}