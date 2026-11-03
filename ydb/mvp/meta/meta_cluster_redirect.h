#pragma once

#include "meta_cluster_balancer.h"

#include <library/cpp/string_utils/quote/quote.h>

namespace NMVP {

class TMetaClusterRedirectActor : private THandlerActorYdb, public NActors::TActorBootstrapped<TMetaClusterRedirectActor> {
protected:
    const TYdbLocation& Location;
    TRequest Request;
    TString ClusterName;

private:
    const NActors::TActorId BalancerCache;
    TStringBuf Target;

    static bool IsValidPathSegment(TStringBuf value) {
        const TString decoded = UrlUnescapeRet(value);
        if (decoded == "." || decoded == "..") {
            return false;
        }
        for (const unsigned char c : decoded) {
            if (c < ' ' || c == 0x7f || c == '/' || c == '\\') {
                return false;
            }
        }
        return true;
    }

    bool ParseRequest() {
        TStringBuf url = Request.Request->URL;
        if (!NClusterRedirect::IsValidUrlText(url) || !url.SkipPrefix("/cluster/")) {
            return false;
        }
        TStringBuf path = url.Before('?');
        const size_t slash = path.find('/');
        if (slash == TStringBuf::npos || slash == 0) {
            return false;
        }
        const TStringBuf name = path.substr(0, slash);
        if (!IsValidPathSegment(name)) {
            return false;
        }
        ClusterName = UrlUnescapeRet(name);
        Target = url.substr(slash);
        path.Skip(slash + 1);
        while (!path.empty()) {
            if (!IsValidPathSegment(path.NextTok('/'))) {
                return false;
            }
        }
        return true;
    }

    void ReplyAndPassAway(const NHttp::THttpOutgoingResponsePtr& response) {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

public:
    TMetaClusterRedirectActor(const TYdbLocation& location, const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request, NActors::TActorId balancerCache = {})
        : Location(location)
        , Request(sender, request)
        , BalancerCache(balancerCache)
    {}

    void Bootstrap() {
        if (!ParseRequest()) {
            ReplyAndPassAway(Request.Request->CreateResponseBadRequest("Expected /cluster/<cluster_name>/<path>", "text/plain"));
            return;
        }
        Become(&TMetaClusterRedirectActor::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
        RequestClusterInfo();
    }

    virtual void RequestClusterInfo() {
        TString userToken = TMVP::MetaDatabaseTokenName.empty() ? Request.GetAuthToken() : TString();
        Send(BalancerCache, new NClusterRedirect::TEvBalancer::TEvGet({ClusterName, std::move(userToken)}));
    }

    void Handle(NClusterRedirect::TEvBalancer::TEvResult::TPtr event) {
        const auto& result = event->Get()->Result;
        if (!result.Status.IsSuccess()) {
            ReplyAndPassAway(CreateStatusResponse(Request.Request, result.Status));
            return;
        }
        if (!result.Found) {
            ReplyAndPassAway(Request.Request->CreateResponseNotFound("Cluster not found", "text/plain"));
            return;
        }
        NHttp::THeadersBuilder headers;
        headers.Set("Location", TStringBuilder() << result.Balancer << Target);
        headers.Set("Cache-Control", "no-store");
        ReplyAndPassAway(Request.Request->CreateResponse("307", "Temporary Redirect", headers));
    }

    void HandleTimeout() {
        ReplyAndPassAway(Request.Request->CreateResponseGatewayTimeout());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NClusterRedirect::TEvBalancer::TEvResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaClusterRedirect : public NActors::TActorBootstrapped<THandlerActorMetaClusterRedirect> {
    const TYdbLocation& Location;
    NActors::TActorId BalancerCache;

public:
    explicit THandlerActorMetaClusterRedirect(const TYdbLocation& location)
        : Location(location)
    {}

    void Bootstrap() {
        BalancerCache = Register(new NClusterRedirect::TBalancerCacheActor(Location));
        Become(&THandlerActorMetaClusterRedirect::StateWork);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        Register(new TMetaClusterRedirectActor(Location, event->Sender, event->Get()->Request, BalancerCache));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
