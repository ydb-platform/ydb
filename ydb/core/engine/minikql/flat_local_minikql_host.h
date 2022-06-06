#pragma once

#include "flat_local_tx_factory.h"
#include "minikql_engine_host.h"

namespace NKikimr {
namespace NMiniKQL {

class TLocalMiniKQLHost : public TEngineHost {
public:
    TLocalMiniKQLHost(
            NTable::TDatabase &db,
            TEngineHostCounters& counters,
            const TEngineHostSettings& settings,
            const TMiniKQLFactory* factory)
        : TEngineHost(db, counters, settings)
        , Factory(factory)
    {}

private:
    bool IsMyKey(const TTableId& tableId, const TArrayRef<const TCell>& row) const override
    {
        Y_UNUSED(row);
        return (tableId.PathId.OwnerId == GetShardId());
    }

    TRowVersion GetWriteVersion(const TTableId& tableId) const override
    {
        return Factory->GetWriteVersion(tableId);
    }

    TRowVersion GetReadVersion(const TTableId& tableId) const override
    {
        return Factory->GetReadVersion(tableId);
    }

    IChangeCollector* GetChangeCollector(const TTableId& tableId) const override
    {
        return Factory->GetChangeCollector(tableId);
    }

    ui64 GetWriteTxId(const TTableId&) const override
    {
        return 0;
    }

    NTable::ITransactionMapPtr GetReadTxMap(const TTableId&) const override
    {
        return nullptr;
    }

    NTable::ITransactionObserverPtr GetReadTxObserver(const TTableId&) const override
    {
        return nullptr;
    }

private:
    const TMiniKQLFactory* const Factory;
};

}}
