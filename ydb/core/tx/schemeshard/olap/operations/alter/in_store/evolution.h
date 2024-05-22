#pragma once
#include "object.h"
#include "update.h"

#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/evolution.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreSchemaEvolution: public ISSEntityEvolution {
private:
    using TBase = ISSEntityEvolution;
    std::shared_ptr<TInStoreTable> Original;
    std::shared_ptr<TInStoreTable> Target;
    std::shared_ptr<TInStoreSchemaUpdate> Update;
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
    TInStoreSchemaEvolution(const TPathId& pathId)
        : TBase(pathId)
    {

    }
    TInStoreSchemaEvolution(const std::shared_ptr<TInStoreTable>& original, const std::shared_ptr<TInStoreTable>& target, const std::shared_ptr<TInStoreSchemaUpdate>& update)
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