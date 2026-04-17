#pragma once

#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/meta/meta_cluster_info.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
#include <ydb/mvp/meta/support_links/response.h>
#include <ydb/mvp/meta/support_links/source.h>
#include <ydb/mvp/meta/support_links/support_links_resolver.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <memory>

namespace NMVP {

inline constexpr TStringBuf SOURCE_META = "meta";
inline constexpr TDuration SUPPORT_LINKS_REQUEST_TIMEOUT = TDuration::Seconds(10);

class TMetaSupportLinksGetHandlerActor : private THandlerActorYdb, public NActors::TActorBootstrapped<TMetaSupportLinksGetHandlerActor> {
public:
    using TBase = NActors::TActorBootstrapped<TMetaSupportLinksGetHandlerActor>;
    using EEntityType = TSupportLinksResolver::EEntityType;

protected:
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    const TMetaSettings Settings;
    TRequest Request;
    EEntityType EntityType = EEntityType::Cluster;
    THashMap<TString, TString> ClusterInfo;

private:
    TMaybe<NYdb::NTable::TSession> Session;
    std::unique_ptr<TSupportLinksResolver> SupportLinksResolver;
    TVector<NSupportLinks::TSupportError> PendingErrors;

public:
    TMetaSupportLinksGetHandlerActor(
        const NActors::TActorId& httpProxyId,
        const TYdbLocation& location,
        const TMetaSettings& settings,
        const NActors::TActorId& sender,
        const NHttp::THttpIncomingRequestPtr& request)
        : HttpProxyId(httpProxyId)
        , Location(location)
        , Settings(settings)
        , Request(sender, request)
    {}

    void Bootstrap() {
        Become(&TMetaSupportLinksGetHandlerActor::StateWork, GetTimeout(Request, SUPPORT_LINKS_REQUEST_TIMEOUT), new NActors::TEvents::TEvWakeup());

        if (!ValidateAndInitEntityType()) {
            ReplyBadRequestAndDie();
            return;
        }

        RequestClusterInfo();
    }

    bool ValidateAndInitEntityType() {
        const TString cluster = Request.Parameters["cluster"];
        const TString database = Request.Parameters["database"];

        if (cluster.empty()) {
            AddCommonError("Invalid identity parameters. Supported entities: cluster requires 'cluster'; database requires 'cluster' and 'database'.");
            return false;
        }
        EntityType = database.empty() ? EEntityType::Cluster : EEntityType::Database;
        return true;
    }

    virtual void RequestClusterInfo() {
        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        NActors::TActorId actorId = SelfId();
        Location.GetTableClient(TMVP::GetStrictMetaDatabaseClientSettings(Request, Location))
            .CreateSession()
            .Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                SendCreateSessionResult(actorId, actorSystem, result);
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            ReplyStatusAndDie(result);
            return;
        }

        Session = result.GetSession();
        RequestClusterInfoRows();
    }

    void RequestClusterInfoRows() {
        TString query = BuildClusterInfoQuery(Location.RootDomain);
        NYdb::TParams params = BuildClusterInfoQueryParams(Request.Parameters["cluster"]);

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        NActors::TActorId actorId = SelfId();
        Session->ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx(
                NYdb::NTable::TTxSettings::OnlineRO(
                    NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx(),
            params)
            .Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncDataQueryResult& result) {
                SendDataQueryResult(actorId, actorSystem, result);
            });
    }

    static void SendCreateSessionResult(
        const NActors::TActorId& actorId,
        NActors::TActorSystem* actorSystem,
        const NYdb::NTable::TAsyncCreateSessionResult& result)
    {
        NYdb::NTable::TAsyncCreateSessionResult res(result);
        actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
    }

    static void SendDataQueryResult(
        const NActors::TActorId& actorId,
        NActors::TActorSystem* actorSystem,
        const NYdb::NTable::TAsyncDataQueryResult& result)
    {
        NYdb::NTable::TAsyncDataQueryResult res(result);
        actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(res.ExtractValue()));
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            ReplyStatusAndDie(result);
            return;
        }

        if (!TryExtractClusterInfo(result.GetResultSet(0), ClusterInfo)) {
            AddCommonError(TStringBuilder() << "Cluster '" << Request.Parameters["cluster"] << "' is not found in MasterClusterExt.db");
        }
        ResolveSupportLinks();
    }

    void ResolveSupportLinks() {
        SupportLinksResolver = CreateSupportLinksResolver();
        SupportLinksResolver->Start();
        if (TryReplyResolvedSupportLinks()) {
            return;
        }
        Become(&TMetaSupportLinksGetHandlerActor::StateResolveSources);
    }

    TSupportLinksResolver::TParams BuildSupportLinksResolverParams() const {
        return TSupportLinksResolver::TParams{
            .EntityType = EntityType,
            .Settings = &Settings,
            .ClusterInfo = ClusterInfo,
            .UrlParameters = Request.Parameters.UrlParameters,
            .Owner = SelfId(),
            .HttpProxyId = HttpProxyId,
        };
    }

    virtual std::unique_ptr<TSupportLinksResolver> CreateSupportLinksResolver() {
        return std::make_unique<TSupportLinksResolver>(BuildSupportLinksResolverParams());
    }

    bool TryReplyResolvedSupportLinks() {
        if (!SupportLinksResolver->IsFinished()) {
            return false;
        }
        ReplyOkAndDie();
        return true;
    }

    void HandleSourceResponse(NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr event) {
        SupportLinksResolver->OnSourceResponse(event);
        TryReplyResolvedSupportLinks();
    }

    void HandleTimeout() {
        if (SupportLinksResolver) {
            SupportLinksResolver->HandleTimeout();
            ReplyOkAndDie();
            return;
        }
        AddCommonError("Timeout while querying cluster info from MasterClusterExt.db");
        ReplyOkAndDie();
    }

    void AddCommonError(TString message) {
        PendingErrors.emplace_back(NSupportLinks::TSupportError{
            .Source = TString(SOURCE_META),
            .Message = std::move(message),
        });
    }

    void ReplyStatusAndDie(const NYdb::NTable::TCreateSessionResult& result) {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateStatusResponse(Request.Request, result)));
        PassAway();
    }

    void ReplyStatusAndDie(const NYdb::NTable::TDataQueryResult& result) {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateStatusResponse(Request.Request, result)));
        PassAway();
    }

    void SendJsonResponseAndDie(const NHttp::THttpOutgoingResponsePtr& response) {
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void ReplyOkAndDie() {
        SendJsonResponseAndDie(CreateResponseOK(Request.Request, BuildResponseBody(), "application/json; charset=utf-8"));
    }

    void ReplyBadRequestAndDie() {
        SendJsonResponseAndDie(CreateResponseBadRequest(Request.Request, BuildResponseBody(), "application/json; charset=utf-8"));
    }

    TString BuildResponseBody() const {
        const auto* sourceOutputs = SupportLinksResolver ? &SupportLinksResolver->GetSourceOutput() : nullptr;
        return NSupportLinks::BuildResponseBody(sourceOutputs, PendingErrors);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            hFunc(TEvPrivate::TEvDataQueryResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    // Separate state to avoid mixing event IDs from THandlerActorYdb::TEvPrivate and NSupportLinks::TEvPrivate.
    STFUNC(StateResolveSources) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSupportLinks::TEvPrivate::TEvSourceResponse, HandleSourceResponse);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

};

class TMetaSupportLinksHandlerActor : private THandlerActorYdb, public NActors::TActor<TMetaSupportLinksHandlerActor> {
public:
    using TBase = NActors::TActor<TMetaSupportLinksHandlerActor>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    const TMetaSettings Settings;

    TMetaSupportLinksHandlerActor(const NActors::TActorId& httpProxyId, const TYdbLocation& location, const TMetaSettings& settings)
        : TBase(&TMetaSupportLinksHandlerActor::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
        , Settings(settings)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            Register(new TMetaSupportLinksGetHandlerActor(HttpProxyId, Location, Settings, event->Sender, request));
            return;
        }

        NJson::TJsonValue root;
        NJson::TJsonValue errorsJson;
        errorsJson.SetType(NJson::JSON_ARRAY);
        NJson::TJsonValue& item = errorsJson.AppendValue(NJson::TJsonValue());
        item["source"] = TString(SOURCE_META);
        item["message"] = "Only GET method is supported";
        root["errors"] = std::move(errorsJson);

        auto response = CreateResponse(
            request,
            "405",
            "Method Not Allowed",
            "application/json; charset=utf-8",
            NJson::WriteJson(root, false));
        Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
