#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
//#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NColumnShard {
class TTxSetCompatibleSchemaVersions: public TTransactionBase<TColumnShard> {
public:
    TTxSetCompatibleSchemaVersions(TColumnShard* self)
        : TBase(self)
    {
    }

    ~TTxSetCompatibleSchemaVersions() {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_SET_COMPATIBLE_SCHEMA_VERSIONS; }
};

}
