#pragma once

#include "meta_cluster_info.h"
#include "mvp.h"

#include <ydb/mvp/core/core_ydb_impl.h>

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/uri/uri.h>

#include <util/string/ascii.h>

namespace NMVP {

class TMetaClusterRedirectActor : private THandlerActorYdb, public NActors::TActorBootstrapped<TMetaClusterRedirectActor> {
protected:
    const TYdbLocation& Location;
    TRequest Request;
    TString ClusterName;

private:
    TStringBuf Target;
    TMaybe<NYdb::NTable::TSession> Session;

    static bool IsValidUrlText(TStringBuf value) {
        for (size_t i = 0; i < value.size(); ++i) {
            const unsigned char c = value[i];
            if (c <= ' ' || c == 0x7f || c == '\\' || c == '#') {
                return false;
            }
            if (c == '%' && (i + 2 >= value.size() || !IsAsciiHex(value[i + 1]) || !IsAsciiHex(value[i + 2]))) {
                return false;
            }
        }
        return true;
    }

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
        if (!IsValidUrlText(url) || !url.SkipPrefix("/clusters/")) {
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

    static TString BuildRedirectLocation(TStringBuf balancer, TStringBuf target) {
        if (!IsValidUrlText(balancer)) {
            return {};
        }
        NUri::TUri uri;
        if (uri.ParseUri(balancer, NUri::TFeature::FeaturesDefaultOrSchemeKnown) != NUri::TState::ParsedOK
                || (uri.GetScheme() != NUri::TScheme::SchemeHTTP && uri.GetScheme() != NUri::TScheme::SchemeHTTPS)
                || uri.GetField(NUri::TField::FieldHost).empty()
                || (uri.GetFieldMask() & (NUri::TField::FlagAuth | NUri::TField::FlagQuery | NUri::TField::FlagFragment))) {
            return {};
        }
        balancer.ChopSuffix("/");
        // The stored balancer points to the viewer API; the request contains its own full path.
        if (!balancer.ChopSuffix("/viewer/json")) {
            balancer.ChopSuffix("/viewer");
        }
        return TStringBuilder() << balancer << target;
    }

    void ReplyAndPassAway(const NHttp::THttpOutgoingResponsePtr& response) {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

public:
    TMetaClusterRedirectActor(const TYdbLocation& location, const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
        , Request(sender, request)
    {}

    void Bootstrap() {
        if (!ParseRequest()) {
            ReplyAndPassAway(Request.Request->CreateResponseBadRequest("Expected /clusters/<cluster_name>/<path>", "text/plain"));
            return;
        }
        Become(&TMetaClusterRedirectActor::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
        RequestClusterInfo();
    }

    virtual void RequestClusterInfo() {
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        auto actorId = SelfId();
        // Query parameters belong to the target cluster, including `database` and `timeout`.
        Location.GetTableClient(TMVP::GetStrictMetaDatabaseClientSettings(Request, Location))
            .CreateSession().Subscribe([actorId, actorSystem](NYdb::NTable::TAsyncCreateSessionResult result) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(result.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event) {
        const auto& result = event->Get()->Result;
        if (!result.IsSuccess()) {
            ReplyAndPassAway(CreateStatusResponse(Request.Request, result));
            return;
        }
        Session = result.GetSession();
        auto* actorSystem = NActors::TActivationContext::ActorSystem();
        auto actorId = SelfId();
        Session->ExecuteDataQuery(
            BuildClusterInfoQuery(Location.RootDomain),
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::OnlineRO()).CommitTx(),
            BuildClusterInfoQueryParams(ClusterName))
            .Subscribe([actorId, actorSystem, session = Session](NYdb::NTable::TAsyncDataQueryResult result) {
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(result.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event) {
        const auto& result = event->Get()->Result;
        if (!result.IsSuccess()) {
            ReplyAndPassAway(CreateStatusResponse(Request.Request, result));
            return;
        }
        THashMap<TString, TString> clusterInfo;
        if (!TryExtractClusterInfo(result.GetResultSet(0), clusterInfo)) {
            ReplyAndPassAway(Request.Request->CreateResponseNotFound("Cluster not found", "text/plain"));
            return;
        }
        const TString location = BuildRedirectLocation(clusterInfo.Value("balancer", TString()), Target);
        if (location.empty()) {
            ReplyAndPassAway(Request.Request->CreateResponseServiceUnavailable("Cluster balancer is not configured as an HTTP(S) URL", "text/plain"));
            return;
        }
        NHttp::THeadersBuilder headers;
        headers.Set("Location", location);
        headers.Set("Cache-Control", "no-store");
        ReplyAndPassAway(Request.Request->CreateResponse("307", "Temporary Redirect", headers));
    }

    void HandleTimeout() {
        ReplyAndPassAway(Request.Request->CreateResponseGatewayTimeout());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            hFunc(TEvPrivate::TEvDataQueryResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorMetaClusterRedirect : public NActors::TActor<THandlerActorMetaClusterRedirect> {
    const TYdbLocation& Location;

public:
    explicit THandlerActorMetaClusterRedirect(const TYdbLocation& location)
        : TActor(&THandlerActorMetaClusterRedirect::StateWork)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        Register(new TMetaClusterRedirectActor(Location, event->Sender, event->Get()->Request));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
