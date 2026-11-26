#pragma once

#include <ydb/apps/bridge_skipper/runtime/skipper.h>
#include <util/generic/strbuf.h>

namespace NBridgeSkipper {

struct THttpResponse {
    unsigned Status = 200;
    TString ContentType = "application/json; charset=utf-8";
    TString Body;
};

// Main opens admission only after bind and the private controller-ready handshake.
class TCommandAdmission {
public:
    void Open(NActors::TActorId controller);
    void Close();
    std::optional<NActors::TActorId> Read() const;

private:
    mutable std::mutex Mutex;
    std::optional<NActors::TActorId> Controller;
};

// Syntax/size/content type only. Controller owns every semantic check.
std::optional<TControllerCommand> ParseCommand(
    TStringBuf path, TStringBuf contentType, TStringBuf body, THttpResponse& error);

TString RenderStateJson(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow);
TString RenderMetrics(const TSkipperSnapshot& snapshot, TMonotonic now);
THttpResponse RouteHttpRequest(
    TStringBuf method,
    TStringBuf target,
    const TSkipperSnapshot& snapshot,
    TMonotonic now,
    TInstant wallNow);

// Registers a catch-all handler. Host and optional Origin checks precede routing;
// every response is no-store and denies framing.
NActors::IActor* CreateHttpActor(
    std::shared_ptr<TSnapshotStore> snapshots,
    NActors::TActorId httpProxy,
    const THttpConfig& config,
    std::shared_ptr<TCommandAdmission> admission = {});

} // namespace NBridgeSkipper
