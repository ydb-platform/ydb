#pragma once
#include "update.h"

#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/common/evolution.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreSchemaEvolution: public TInStoreCommonEvolution {
public:
    static TString GetClassNameStatic() {
        return "Store.Table.Schema";
    }
private:
    using TBase = TInStoreCommonEvolution;
    NKikimrTxColumnShard::TAlterTable AlterToShard;
    static const inline TFactory::TRegistrator<TInStoreSchemaEvolution> Registrator = TFactory::TRegistrator<TInStoreSchemaEvolution>(GetClassNameStatic());
protected:
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    virtual TString DoGetShardTxBody(const TPathId& pathId, const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const override;

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

    TInStoreSchemaEvolution() = default;

    TInStoreSchemaEvolution(const NKikimrTxColumnShard::TAlterTable& updatedTableInfo, const std::set<ui64>& shardIds)
        : TBase(shardIds)
        , AlterToShard(updatedTableInfo)
    {

    }
};

}