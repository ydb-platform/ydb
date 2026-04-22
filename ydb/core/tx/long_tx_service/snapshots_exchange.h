#pragma once

#include "snapshots_storage.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NLongTxService {

struct TSnapshotExchangeCounters {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    TCounterPtr TimeSinceLastRemoteSnapshotsUpdateMs = nullptr;
    TCounterPtr SnapshotsCollectionTimeMs = nullptr;
    TCounterPtr SnapshotsPropagationTimeMs = nullptr;
};

IActor* CreateSnapshotExchangeActor(
    TConstLocalSnapshotsStoragePtr localSnapshotsStorage,
    TRemoteSnapshotsStoragePtr remoteSnapshotsStorage,
    TSnapshotExchangeCounters counters);

}
}
