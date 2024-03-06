#pragma once

#include "datashard_impl.h"
#include <ydb/core/tx/locks/locks.h>
#include "operation.h"

#include <ydb/core/engine/minikql/change_collector_iface.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NDataShard {

struct TDirectTxResult {
    TActorId Target;
    THolder<IEventBase> Event;
    ui64 Cookie;
};

class IDirectTx {
public:
    virtual ~IDirectTx() = default;
    virtual bool Execute(TDataShard* self, TTransactionContext& txc,
        const TRowVersion& readVersion, const TRowVersion& writeVersion,
        ui64 globalTxId, absl::flat_hash_set<ui64>& volatileReadDependencies) = 0;
    virtual TDirectTxResult GetResult(TDataShard* self) = 0;
    virtual TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const = 0;
};

class TDirectTransaction : public TOperation {
public:
    TDirectTransaction(TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvUploadRowsRequest::TPtr& ev);
    TDirectTransaction(TInstant receivedAt, ui64 tieBreakerIndex, TEvDataShard::TEvEraseRowsRequest::TPtr& ev);

    void BuildExecutionPlan(bool) override;

private:
    bool Execute(TDataShard* self, TTransactionContext& txc);
    void SendResult(TDataShard* self, const TActorContext& ctx);
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;

    friend class TDirectOpUnit;

private:
    THolder<IDirectTx> Impl;
    static constexpr ui32 Flags = NTxDataShard::TTxFlags::Immediate | NTxDataShard::TTxFlags::GlobalWriter;
};

} // NDataShard
} // NKikimr
