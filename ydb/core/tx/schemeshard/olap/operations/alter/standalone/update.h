#pragma once
#include "object.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/update.h>
#include <ydb/core/tx/schemeshard/olap/schema/update.h>
#include <ydb/core/tx/schemeshard/olap/ttl/update.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

class TStandaloneSchemaUpdate: public ISSEntityUpdate {
private:
    using TBase = ISSEntityUpdate;
    std::optional<TOlapSchemaUpdate> AlterSchema;
    std::optional<TOlapTTLUpdate> AlterTTL;
    std::shared_ptr<TStandaloneTable> TargetStandalone;
    std::shared_ptr<TStandaloneTable> OriginalStandalone;
    virtual TConclusionStatus DoInitialize(const TUpdateInitializationContext& context) override;

public:
    TStandaloneSchemaUpdate(const std::shared_ptr<ISSEntity>& original)
        : TBase(original)
        , OriginalStandalone(dynamic_pointer_cast<TStandaloneTable>(GetOriginalEntity()))
    {
        AFL_VERIFY(!!OriginalStandalone);
    }

    void FillToShardTx(NKikimrTxColumnShard::TAlterTable& shardAlter, const std::shared_ptr<TStandaloneTable>& schema) const {
        if (AlterSchema) {
            *shardAlter.MutableSchema() = schema->GetTableSchemaProto();
        }
        if (AlterTTL) {
            *shardAlter.MutableTtlSettings() = schema->GetTableTTLProto();
        }
    }

    const TOlapSchemaUpdate* GetAlterSchemaOptional() const {
        return AlterSchema ? &*AlterSchema : nullptr;
    }

    const TOlapTTLUpdate* GetAlterTTLOptional() const {
        return AlterTTL ? &*AlterTTL : nullptr;
    }

    virtual TConclusion<std::shared_ptr<ISSEntityEvolution>> DoBuildEvolution(const std::shared_ptr<ISSEntityUpdate>& updateSelfPtr) const override;
};

}