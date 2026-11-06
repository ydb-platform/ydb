#include "http.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <util/string/cast.h>

namespace NBridgeSkipper {
namespace {

class THttpActor final : public NActors::TActorBootstrapped<THttpActor> {
public:
    THttpActor(std::shared_ptr<TSnapshotStore> snapshots, NActors::TActorId proxy)
        : Snapshots(std::move(snapshots))
        , Proxy(proxy) {
    }

    void Bootstrap() {
        Send(Proxy, new NHttp::TEvHttpProxy::TEvRegisterHandler("/", SelfId()));
        Become(&TThis::StateWork);
    }

private:
    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        // main publishes the seed snapshot before it starts this actor.
        const auto snapshot = Snapshots->Read();
        Y_ENSURE(snapshot, "HTTP request before the seed snapshot");
        const TMonotonic now = NActors::TActivationContext::Monotonic();
        const TInstant wallNow = NActors::TActivationContext::Now();
        const auto& request = event->Get()->Request;
        const auto response = RouteHttpRequest(request->Method, request->URL, *snapshot, now, wallNow);
        NHttp::THeadersBuilder headers;
        headers.Set("Content-Type", response.ContentType);
        headers.Set("Cache-Control", "no-store");
        if (response.Status == 405) {
            headers.Set("Allow", "GET, HEAD");
        }
        auto outgoing = request->CreateResponse(::ToString(response.Status),
            response.Status == 200 ? "OK" : response.Status == 405 ? "Method Not Allowed" : "Not Found",
            headers, response.Body);
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(std::move(outgoing)), 0, event->Cookie);
    }

    STRICT_STFUNC(StateWork,
        hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        cFunc(NActors::TEvents::TEvPoison::EventType, PassAway);
    )

    std::shared_ptr<TSnapshotStore> Snapshots;
    NActors::TActorId Proxy;
};

} // namespace

THttpResponse RouteHttpRequest(
    TStringBuf method,
    TStringBuf target,
    const TSkipperSnapshot& snapshot,
    TMonotonic now,
    TInstant wallNow)
{
    const bool head = method == "HEAD";
    if (method != "GET" && !head) {
        return {405, "application/json; charset=utf-8", R"({"error":"METHOD_NOT_ALLOWED"})"};
    }
    const TStringBuf path = target.Before('?');
    THttpResponse response;
    if (path == "/api/state") {
        response = {200, "application/json; charset=utf-8", RenderStateJson(snapshot, now, wallNow)};
    } else if (path == "/metrics") {
        response = {200, "text/plain; version=0.0.4; charset=utf-8", RenderMetrics(snapshot, now)};
    } else {
        response = {404, "application/json; charset=utf-8", R"({"error":"NOT_FOUND"})"};
    }
    if (head) {
        response.Body.clear();
    }
    return response;
}

NActors::IActor* CreateHttpActor(
    std::shared_ptr<TSnapshotStore> snapshots,
    NActors::TActorId httpProxy)
{
    return new THttpActor(std::move(snapshots), httpProxy);
}

} // namespace NBridgeSkipper
