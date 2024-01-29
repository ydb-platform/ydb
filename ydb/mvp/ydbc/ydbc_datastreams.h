#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcDatastreamsListRequest : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatastreamsListRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatastreamsListRequest>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;

    std::map<TString, TEvPrivate::TEvDescribeTopicResult::TPtr> StreamsDescribe;

    ui32 RequestsInflight;
    TString Database;

    THandlerActorYdbcDatastreamsListRequest(
            const TYdbcLocation& location,
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
        , RequestsInflight(0)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString databaseId = Request.Parameters["databaseId"];
        if (IsValidDatabaseId(databaseId)) {
            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequestGet(
                        TMVP::GetAppropriateEndpoint(Request.Request)
                        + "/ydbc/" + Location.Name + "/database" + "?databaseId=" + databaseId);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcDatastreamsListRequest::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            if (event->Get()->Request->URL.StartsWith("/ydbc/" + Location.Name + "/database" + "?")) {
                NJson::TJsonValue responseData;
                bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);

                if (success) {
                    TString kinesisEndpoint = responseData["kinesisApiEndpoint"].GetStringRobust();
                    TString endpoint = responseData["endpoint"].GetStringRobust();
                    TStringBuf scheme;
                    TStringBuf host;
                    TStringBuf uri;
                    NHttp::CrackURL(endpoint, scheme, host, uri);
                    NHttp::TUrlParameters urlParams(uri);
                    TString database = urlParams["database"];
                    Database = database;
                    bool recurse = true;
                    recurse = FromStringWithDefault(urlParams["recurse"], recurse);
                    TString token = Request.GetAuthToken();
                    if (responseData.Has("serverlessDatabase")) {
                        endpoint = responseData["serverlessInternals"]["sharedEndpoint"].GetStringRobust();
                        NHttp::CrackURL(endpoint, scheme, host, uri);
                    }
                    if (!database.empty()) {
                        TopicClient = Location.GetTopicClientPtr(host, scheme, NYdb::NTopic::TTopicClientSettings().Database(database).AuthToken(token));

                        ui32 limit = 1000;
                        TString startStreamName = "";

                        NHttp::THttpOutgoingRequestPtr httpRequest =
                        NHttp::THttpOutgoingRequest::CreateRequest("GET", kinesisEndpoint);
                        Request.ForwardHeadersOnlyForIAM(httpRequest);

                        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
                        httpRequest->Set("x-amz-target", "Kinesis_20131202.ListStreams");

                        httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"Limit\": "
                                                                                      << limit << ", \"ExclusiveStartStreamName\": \"" << startStreamName << "\" }");

                        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

                        return;
                    } else {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
                } else {
                    message = "Unable to parse database information";
                    status = "500";
                }
            } else { //list streams response
                //TODO: process stream names

                NJson::TJsonValue responseData;
                bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);

                if (success) {

                    for (auto& stream : responseData["StreamNames"].GetArraySafe()) {
                        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                        NActors::TActorId actorId = ctx.SelfID;

                        TopicClient->DescribeTopic(stream.GetStringRobust(), NYdb::NTopic::TDescribeTopicSettings().ClientTimeout(GetClientTimeout()))
                                .Subscribe([actorSystem, actorId, streamName = stream.GetStringRobust()](const NYdb::NTopic::TAsyncDescribeTopicResult& result) mutable {
                                    NYdb::NTopic::TAsyncDescribeTopicResult res(result);
                                    actorSystem->Send(actorId, new TEvPrivate::TEvDescribeTopicResult(res.ExtractValue(), streamName));
                            });

                        ++RequestsInflight;
                    }
                    if (RequestsInflight == 0) {
                        NJson::TJsonValue root;
                        root.SetType(NJson::JSON_ARRAY);
                        NHttp::THttpOutgoingResponsePtr response;

                        response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");

                        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
                        TBase::Die(ctx);
                    }
                    return;
                } else {
                    message = "Invalid datastreams response body";
                    status = "500";
                }
            }
        } else {
            message = event->Get()->Response->Message;
            contentType = event->Get()->Response->ContentType;
            body = event->Get()->Response->Body;
        }
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponse(status, message, contentType, body);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }


    void Handle(TEvPrivate::TEvDescribeTopicResult::TPtr event, const NActors::TActorContext& ctx) {
        auto& result(event->Get()->Result);

        if (result.IsSuccess()) {
            StreamsDescribe[event->Get()->Name] = std::move(event);
        }

        if (--RequestsInflight)
            return;

        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        NHttp::THttpOutgoingResponsePtr response;

        for (auto& [name, descr] : StreamsDescribe) {
            auto& description = descr->Get()->Result.GetTopicDescription();
            auto& item = root.AppendValue(NJson::TJsonValue());
            item.SetType(NJson::JSON_MAP);
            item["name"] = EscapeStreamName(name);
            item["shards"] = description.GetPartitions().size();
            item["retentionPeriodHours"] = description.GetRetentionPeriod().Hours();
            item["writeQuotaKbPerSec"] = description.GetPartitionWriteSpeedBytesPerSecond() / 1024;
            item["status"] = "ACTIVE";
            item["owner"] = TrimAtAs(description.GetOwner());
            item["totalWriteQuotaKbPerSec"] = description.GetPartitionWriteSpeedBytesPerSecond() / 1024 * description.GetPartitions().size();
            item["streamCreationTimestamp"] = description.GetCreationTimestamp().PlanStep / 1000;
            if (description.GetRetentionStorageMb()) {
                item["storageLimitMb"] = *description.GetRetentionStorageMb();
            }
        }

        response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");

        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvDescribeTopicResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};


class THandlerActorYdbcDatastreamsListAllRequest : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatastreamsListAllRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatastreamsListAllRequest>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    ui32 InflyRequests;
    std::map<TString, NJson::TJsonValue> DatabaseResponses;
    struct TDBInfo {
        TString Name;
        TString FolderId;
        TString CloudId;
        TString KinesisEndpoint;
        TString Host;
        TString DatabasePath;
    };
    std::map<TString, TDBInfo> DatabaseNames;


    THandlerActorYdbcDatastreamsListAllRequest(
            const TYdbcLocation& location,
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
        , InflyRequests(0)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString folderId = Request.Parameters["folderId"];
        NHttp::THttpOutgoingRequestPtr httpRequest =
                NHttp::THttpOutgoingRequest::CreateRequestGet(
                    TMVP::GetAppropriateEndpoint(Request.Request)
                    + "/ydbc/" + Location.Name + "/databases" + "?folderId=" + folderId);
        Request.ForwardHeadersOnlyForIAM(httpRequest);
        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        ++InflyRequests;
        Become(&THandlerActorYdbcDatastreamsListAllRequest::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {

        --InflyRequests;
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::TJsonValue responseData;
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
            if (success) {
                if (!DatabaseNames.size()) {
                    for (NJson::TJsonValue& jsonDatabase : responseData["databases"].GetArraySafe()) {
                        TString status = jsonDatabase["status"].GetStringRobust();
                        if (status != "RUNNING" && status != "UPDATING")
                            continue;

                        TString databaseId = jsonDatabase["id"].GetStringRobust();
                        TString endpoint = jsonDatabase["endpoint"].GetStringRobust();
                        TStringBuf scheme;
                        TStringBuf host;
                        TStringBuf uri;
                        NHttp::CrackURL(endpoint, scheme, host, uri);
                        NHttp::TUrlParameters urlParams(uri);
                        TString database = urlParams["database"];

                        NHttp::THttpOutgoingRequestPtr httpRequest =
                                NHttp::THttpOutgoingRequest::CreateRequestGet(
                                    TMVP::GetAppropriateEndpoint(Request.Request)
                                    + "/ydbc/" + Location.Name + "/datastreams" + "?databaseId=" + databaseId);
                        Request.ForwardHeadersOnlyForIAM(httpRequest);

                        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
                        TString host1 = endpoint;
                        TString kinesisEndpoint = endpoint;
                        if (jsonDatabase.Has("documentApiEndpoint")) { // switch to kinesis_api_endpoint when ydb cp ready https://docapi
                            kinesisEndpoint = "https://yds" + jsonDatabase["documentApiEndpoint"].GetStringRobust().substr(14);
                            host1 = kinesisEndpoint.substr(0, kinesisEndpoint.find(database));
                        }
                        DatabaseNames[databaseId] = {jsonDatabase["name"].GetStringRobust(), jsonDatabase["folderId"].GetStringRobust(),
                                                     jsonDatabase["cloudId"].GetStringRobust(), kinesisEndpoint, host1, database};
                        ++InflyRequests;
                    }
                    if (InflyRequests == 0)
                        ReplyAndDie(ctx);
                    return;
                } else {
                    auto url = event->Get()->Request->URL;
                    TStringBuf scheme;
                    TStringBuf host;
                    TStringBuf uri;
                    NHttp::CrackURL(url, scheme, host, uri);
                    NHttp::TUrlParameters urlParams(uri);
                    TString databaseId = urlParams["databaseId"];
                    NJson::TJsonValue responseData;
                    bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
                    if (success) {
                        DatabaseResponses[databaseId] = responseData;
                        if (!InflyRequests) {
                            ReplyAndDie(ctx);
                        }
                        return;
                    } else {
                        message = "Unable to parse list information";
                        status = "500";
                    }
                }
            } else {
                message = "Unable to parse database information";
                status = "500";
            }
        } else {
            message = event->Get()->Response->Message;
            contentType = event->Get()->Response->ContentType;
            body = event->Get()->Response->Body;
        }
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponse(status, message, contentType, body);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }


    void ReplyAndDie(const NActors::TActorContext& ctx) {

        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        NHttp::THttpOutgoingResponsePtr response;

        for (auto& db : DatabaseNames) {
            auto& dbId = db.first;
            auto& name = db.second.Name;
            auto& folderId = db.second.FolderId;
            auto& cloudId = db.second.CloudId;
            auto& kinesisEndpoint = db.second.KinesisEndpoint;
            auto& host = db.second.Host;
            auto& databasePath = db.second.DatabasePath;
            const auto& response = DatabaseResponses[dbId];
            NJson::TJsonValue item;
            item.SetType(NJson::JSON_MAP);
            item.InsertValue("databaseId", dbId);
            item.InsertValue("databaseName", name);
            item.InsertValue("streams", response);
            item.InsertValue("folderId", folderId);
            item.InsertValue("cloudId", cloudId);

            item["endpoint"] = kinesisEndpoint;
            item["alternativeEndpoint"] = host;
            item["alternativeRegion"] = "ru-central-1";
            item["region"] = "ru-central1";
            for (auto& stream : item["streams"].GetArraySafe()) {
                stream["alternativeStreamName"] = databasePath + "/" + UnescapeStreamName(stream["name"].GetStringRobust());
            }
            root.AppendValue(item);
        }

        response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");

        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};





class THandlerActorYdbcDatastreams : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDatastreams> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDatastreams>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcDatastreams(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcDatastreams::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            TParameters params(request);
            if (!params["folderId"].empty()) {
                ctx.Register(new THandlerActorYdbcDatastreamsListAllRequest(Location, HttpProxyId, event->Sender, request));
                return;
            }
            ctx.Register(new THandlerActorYdbcDatastreamsListRequest(Location, HttpProxyId, event->Sender, request));
            return;
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};


} // namespace NMVP

