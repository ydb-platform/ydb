#pragma once
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/evolution.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneSchemaEvolution: public ISSEntityEvolution {
public:
    static TString GetClassNameStatic() {
        return "StandaloneTable.Schema";
    }
private:
    using TBase = ISSEntityEvolution;
    NKikimrTxColumnShard::TAlterTable AlterToShard;
    static const inline TFactory::TRegistrator<TStandaloneSchemaEvolution> Registrator = TFactory::TRegistrator<TStandaloneSchemaEvolution>(GetClassNameStatic());
protected:
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    virtual TString DoGetShardTxBody(const TPathId& pathId, const ui64 /*tabletId*/, const TMessageSeqNo& seqNo) const override {
        NKikimrTxColumnShard::TSchemaTxBody result;
        
        result.MutableSeqNo()->SetGeneration(seqNo.Generation);
        result.MutableSeqNo()->SetRound(seqNo.Round);

        *result.MutableAlterTable() = AlterToShard;
        result.MutableAlterTable()->SetPathId(pathId.LocalPathId);
        return result.SerializeAsString();
    }

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

    TStandaloneSchemaEvolution() = default;

    TStandaloneSchemaEvolution(const NKikimrTxColumnShard::TAlterTable& alter, const std::set<ui64>& shardIds)
        : TBase(shardIds)
        , AlterToShard(alter)
    {

    }
};

}