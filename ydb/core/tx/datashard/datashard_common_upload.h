#pragma once

#include "datashard_impl.h"

#include <ydb/core/engine/minikql/change_collector_iface.h>

namespace NKikimr {
namespace NDataShard {

template <typename TEvRequest, typename TEvResponse>
class TCommonUploadOps {
    using IChangeCollector = NMiniKQL::IChangeCollector;

    const bool BreakLocks;
    const bool CollectChanges;

    typename TEvRequest::TPtr Ev;
    THolder<TEvResponse> Result;
    THolder<IChangeCollector> ChangeCollector;

public:
    explicit TCommonUploadOps(typename TEvRequest::TPtr& ev, bool breakLocks, bool collectChanges);

protected:
    bool Execute(TDataShard* self, TTransactionContext& txc, const TRowVersion& readVersion, const TRowVersion& writeVersion);
    void SendResult(TDataShard* self, const TActorContext& ctx);
    TVector<IChangeCollector::TChange> GetCollectedChanges() const;

private:
    void SetError(ui32 status, const TString& descr);
};

} // NDataShard
} // NKikimr
