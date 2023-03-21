#pragma once

#include "change_collector.h"
#include "datashard_impl.h"

#include <ydb/core/engine/minikql/change_collector_iface.h>

namespace NKikimr {
namespace NDataShard {

template <typename TEvRequest, typename TEvResponse>
class TCommonUploadOps {
    typename TEvRequest::TPtr Ev;
    const bool BreakLocks;
    const bool CollectChanges;

    THolder<TEvResponse> Result;
    THolder<IDataShardChangeCollector> ChangeCollector;

public:
    explicit TCommonUploadOps(typename TEvRequest::TPtr& ev, bool breakLocks, bool collectChanges);

protected:
    bool Execute(TDataShard* self, TTransactionContext& txc, const TRowVersion& readVersion,
        const TRowVersion& writeVersion, ui64 globalTxId,
        absl::flat_hash_set<ui64>* volatileReadDependencies);
    void GetResult(TDataShard* self, TActorId& target, THolder<IEventBase>& event, ui64& cookie);
    const TEvRequest* GetRequest() const;
    TEvResponse* GetResult();
    TVector<IDataShardChangeCollector::TChange> GetCollectedChanges() const;

private:
    void SetError(ui32 status, const TString& descr);
};

} // NDataShard
} // NKikimr
