#pragma once

#include <ydb/apps/bridge_skipper/runtime/skipper.h>
#include <util/generic/strbuf.h>

namespace NBridgeSkipper {

struct THttpResponse {
    unsigned Status = 200;
    TString ContentType = "application/json; charset=utf-8";
    TString Body;
};

TString RenderStateJson(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow);
TString RenderMetrics(const TSkipperSnapshot& snapshot, TMonotonic now);
THttpResponse RouteHttpRequest(
    TStringBuf method,
    TStringBuf target,
    const TSkipperSnapshot& snapshot,
    TMonotonic now,
    TInstant wallNow);

// Registers a catch-all handler. Every response is read-only and no-store.
NActors::IActor* CreateHttpActor(
    std::shared_ptr<TSnapshotStore> snapshots,
    NActors::TActorId httpProxy);

} // namespace NBridgeSkipper
