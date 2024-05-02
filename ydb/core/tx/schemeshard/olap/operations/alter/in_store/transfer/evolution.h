#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/evolution.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/evolution.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreShardsTransferEvolution: public TInStoreCommonEvolution {
public:
    static TString GetClassNameStatic() {
        return "InStore.Table.Resharding.Transfer";
    }
private:
    using TBase = TInStoreCommonEvolution;
    std::set<ui64> FromShardIds;
    ui64 ToShardId = 0;
    static const inline TFactory::TRegistrator<TInStoreShardsTransferEvolution> Registrator = TFactory::TRegistrator<TInStoreShardsTransferEvolution>(GetClassNameStatic());
protected:
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SHARING;
    }

    virtual TString DoGetShardTxBody(const TPathId& pathId, const ui64 tabletId, const TMessageSeqNo& seqNo) const override;

    virtual TConclusionStatus DoStartEvolution(const TEvolutionStartContext& /*context*/) override {
        return TConclusionStatus::Success();
    }
    virtual TConclusionStatus DoFinishEvolution(const TEvolutionFinishContext& /*context*/) override {
        return TConclusionStatus::Success();
    }
    virtual void DoSerializeToProto(NKikimrSchemeshardOlap::TEntityEvolution& proto) const override;
    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeshardOlap::TEntityEvolution& proto) override;
public:

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TString GetSessionId() const {
        return TStringBuilder() << ToShardId << ":" << JoinSeq(",", FromShardIds);
    }

    TInStoreShardsTransferEvolution() = default;

    TInStoreShardsTransferEvolution(const std::set<ui64>& fromShardIds, const ui64 toShardId)
        : TBase({ toShardId })
        , FromShardIds(fromShardIds)
        , ToShardId(toShardId)
    {

    }
};

}