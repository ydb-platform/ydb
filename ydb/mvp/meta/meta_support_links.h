#pragma once

#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/meta/mvp.h>
#include <ydb/mvp/meta/support_links/events.h>
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

using namespace NKikimr;

inline constexpr TStringBuf SOURCE_META = "meta";

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
    THashMap<TString, TString> ClusterColumns;

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
        Become(&TMetaSupportLinksGetHandlerActor::StateWork, GetTimeout(Request, TDuration::Seconds(60)), new NActors::TEvents::TEvWakeup());

        if (!InitEntityType()) {
            ReplyBadRequestAndDie();
            return;
        }

        RequestClusterInfo();
    }

    bool InitEntityType() {
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
        Location.GetTableClient(TMVP::GetMetaDatabaseClientSettings(Request, Location))
            .CreateSession()
            .Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                NYdb::NTable::TAsyncCreateSessionResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateStatusResponse(Request.Request, result)));
            PassAway();
            return;
        }

        Session = result.GetSession();
        TString query = TStringBuilder() << "DECLARE $name AS Utf8; SELECT * FROM `" << Location.RootDomain << "/ydb/MasterClusterExt.db` WHERE name=$name";
        NYdb::TParamsBuilder params;
        params.AddParam("$name", NYdb::TValueBuilder().Utf8(Request.Parameters["cluster"]).Build());

        NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
        NActors::TActorId actorId = SelfId();
        Session->ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx(
                NYdb::NTable::TTxSettings::OnlineRO(
                    NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx(),
            params.Build())
            .Subscribe([actorId, actorSystem, session = Session](const NYdb::NTable::TAsyncDataQueryResult& result) {
                NYdb::NTable::TAsyncDataQueryResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(res.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateStatusResponse(Request.Request, result)));
            PassAway();
            return;
        }

        auto resultSet = result.GetResultSet(0);
        const auto& columnsMeta = resultSet.GetColumnsMeta();
        NYdb::TResultSetParser rsParser(resultSet);
        if (!rsParser.TryNextRow()) {
            AddCommonError(TStringBuilder() << "Cluster '" << Request.Parameters["cluster"] << "' is not found in MasterClusterExt.db");
        } else {
            for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                ClusterColumns[columnMeta.Name] = ColumnValueToString(rsParser.ColumnParser(columnNum));
            }
        }

        ResolveSupportLinks();
    }

    void ResolveSupportLinks() {
        SupportLinksResolver = CreateSupportLinksResolver();
        SupportLinksResolver->Start();
        if (SupportLinksResolver->IsFinished()) {
            ReplyOkAndDie();
            return;
        }
        Become(&TMetaSupportLinksGetHandlerActor::StateResolveSources);
    }

    virtual std::unique_ptr<TSupportLinksResolver> CreateSupportLinksResolver() {
        return std::make_unique<TSupportLinksResolver>(TSupportLinksResolver::TParams{
            .EntityType = EntityType,
            .Settings = &Settings,
            .ClusterColumns = ClusterColumns,
            .UrlParameters = Request.Parameters.UrlParameters,
            .Parent = SelfId(),
            .HttpProxyId = HttpProxyId,
        });
    }

    void Handle(NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr event) {
        SupportLinksResolver->OnSourceResponse(event);
        if (SupportLinksResolver->IsFinished()) {
            ReplyOkAndDie();
        }
    }

    void HandleTimeout() {
        if (SupportLinksResolver) {
            SupportLinksResolver->HandleTimeout();
        } else {
            AddCommonError("Timeout while querying cluster info from MasterClusterExt.db");
        }
        ReplyOkAndDie();
    }

    void AddCommonError(TString message) {
        PendingErrors.emplace_back(NSupportLinks::TSupportError{
            .Source = TString(SOURCE_META),
            .Message = std::move(message),
        });
    }

    void ReplyOkAndDie() {
        auto response = CreateResponseOK(Request.Request, BuildResponseBody(), "application/json; charset=utf-8");
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    void ReplyBadRequestAndDie() {
        auto response = CreateResponseBadRequest(Request.Request, BuildResponseBody(), "application/json; charset=utf-8");
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        PassAway();
    }

    static void AppendErrorJson(NJson::TJsonValue& errorsJson, const NSupportLinks::TSupportError& error) {
        NJson::TJsonValue& item = errorsJson.AppendValue(NJson::TJsonValue());
        item["source"] = error.Source;
        if (error.Status) {
            item["status"] = *error.Status;
        }
        if (!error.Reason.empty()) {
            item["reason"] = error.Reason;
        }
        if (!error.Message.empty()) {
            item["message"] = error.Message;
        }
    }

    TString BuildResponseBody() const {
        NJson::TJsonValue root;
        NJson::TJsonValue& linksJson = root["links"];
        linksJson.SetType(NJson::JSON_ARRAY);

        NJson::TJsonValue errorsJson;
        errorsJson.SetType(NJson::JSON_ARRAY);

        if (SupportLinksResolver) {
            for (const auto& sourceOutput : SupportLinksResolver->GetSourceOutput()) {
                for (const auto& link : sourceOutput.Links) {
                    NJson::TJsonValue& linkItem = linksJson.AppendValue(NJson::TJsonValue());
                    if (!link.Title.empty()) {
                        linkItem["title"] = link.Title;
                    }
                    linkItem["url"] = link.Url;
                }
                for (const auto& error : sourceOutput.Errors) {
                    AppendErrorJson(errorsJson, error);
                }
            }
        }

        for (const auto& error : PendingErrors) {
            AppendErrorJson(errorsJson, error);
        }

        if (!errorsJson.GetArray().empty()) {
            root["errors"] = std::move(errorsJson);
        }
        return NJson::WriteJson(root, false);
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
            hFunc(NSupportLinks::TEvPrivate::TEvSourceResponse, Handle);
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
