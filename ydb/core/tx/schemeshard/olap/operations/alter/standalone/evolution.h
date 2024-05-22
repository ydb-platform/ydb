#pragma once
#include "object.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/evolution.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneSchemaEvolution: public ISSEntityEvolution {
private:
    using TBase = ISSEntityEvolution;
    std::shared_ptr<TStandaloneTable> Original;
    std::shared_ptr<TStandaloneTable> Target;
    std::shared_ptr<TStandaloneSchemaUpdate> Update;
protected:
    virtual NKikimrTxColumnShard::ETransactionKind GetShardTransactionKind() const override {
        return NKikimrTxColumnShard::ETransactionKind::TX_KIND_SCHEMA;
    }

    virtual NKikimrTxColumnShard::TSchemaTxBody DoGetShardTxBody(const NKikimrTxColumnShard::TSchemaTxBody& original, const ui64 /*tabletId*/) const override {
        NKikimrTxColumnShard::TSchemaTxBody copy = original;
        
        auto& alter = *copy.MutableAlterTable();
        Update->FillToShardTx(alter, Target);
        alter.SetPathId(GetPathId().LocalPathId);
        return copy;
    }

    virtual TConclusionStatus DoInitialize(const TEvolutionInitializationContext& context) override;

    virtual TConclusionStatus DoStartEvolution(const TEvolutionStartContext& context) override;
public:
    TStandaloneSchemaEvolution(const TPathId& pathId)
        : TBase(pathId)
    {

    }
    TStandaloneSchemaEvolution(const std::shared_ptr<TStandaloneTable>& original, const std::shared_ptr<TStandaloneTable>& target, const std::shared_ptr<TStandaloneSchemaUpdate>& update)
        : TBase(original ? original->GetPathId() : target->GetPathId())
        , Original(original)
        , Target(target)
        , Update(update)
    {
        AFL_VERIFY(!!Original);
        AFL_VERIFY(!!Target);
        AFL_VERIFY(!!Update);
    }
};

}