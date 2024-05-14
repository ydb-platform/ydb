#pragma once
#include "object.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TInStoreSchemaUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;
    std::optional<TOlapTTLUpdate> AlterTTL;
    std::shared_ptr<TInStoreTable> OriginalInStoreTable;
    std::shared_ptr<TInStoreTable> TargetInStoreTable;
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;
    TConclusionStatus InitializeDiff(const NKikimrSchemeOp::TAlterColumnTable& alterCS);
    TConclusion<TOlapTTL> GetTtlPatched() const;
public:
    TInStoreSchemaUpdate(const std::shared_ptr<ISSEntity>& originalEntity)
        : TBase(originalEntity) {
        OriginalInStoreTable = dynamic_pointer_cast<TInStoreTable>(originalEntity);
        AFL_VERIFY(OriginalInStoreTable);
    }

    TInStoreSchemaUpdate(const std::shared_ptr<ISSEntity>& originalEntity, const std::shared_ptr<ISSEntity>& targetEntity)
        : TBase(originalEntity) {
        OriginalInStoreTable = dynamic_pointer_cast<TInStoreTable>(originalEntity);
        AFL_VERIFY(OriginalInStoreTable);
        TargetInStoreTable = dynamic_pointer_cast<TInStoreTable>(targetEntity);
        AFL_VERIFY(TargetInStoreTable);
        AFL_VERIFY(!!TargetInStoreTable->GetTableInfoVerified().AlterBody);
        InitializeDiff(*TargetInStoreTable->GetTableInfoVerified().AlterBody).Validate();
    }

    void FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter, const std::shared_ptr<TInStoreTable>& target) const;

    const TOlapTTLUpdate* GetAlterTTLOptional() const {
        return AlterTTL ? &*AlterTTL : nullptr;
    }

    virtual TConclusion<std::shared_ptr<ISSEntityEvolution>> DoBuildEvolution(const std::shared_ptr<ISSEntityUpdate>& updateSelfPtr) const override;
};

}