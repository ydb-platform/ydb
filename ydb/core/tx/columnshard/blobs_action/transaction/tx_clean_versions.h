#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NColumnShard {
class TTxSchemaVersionsCleanup: public TTransactionBase<TColumnShard> {
public:
    TTxSchemaVersionsCleanup(TColumnShard* self, THashSet<ui64>&& versionsToRemove)
        : TBase(self)
        , VersionsToRemove(versionsToRemove)
    {
    }

    ~TTxSchemaVersionsCleanup() {
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return TXTYPE_CLEANUP_SCHEMA_VERSIONS; }
    std::vector<std::pair<ui64, ui64>> GetPrevNextSchemas() const;

private:
    THashSet<ui64> VersionsToRemove;
};

}
