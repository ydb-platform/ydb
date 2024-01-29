#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <ydb/core/protos/grpc.grpc.pb.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcMetaCollector : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcMetaCollector> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcMetaCollector>;
    const TYdbcLocation& Location;
    TRequest Request;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;
    std::unique_ptr<NYdb::NTable::TTableClient> TableClient;
    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    NActors::TActorId HttpProxyId;
    TString Scheme;
    TString Host;
    TString Database;
    TString Path;
    TString FullPath;
    TString Token;
    ui32 Requests = 0;
    NJson::TJsonValue DatabaseInfo;
    TInstant Deadline;
    static constexpr TDuration MAX_RETRY_TIME = TDuration::Seconds(10);
    THolder<TEvPrivate::TEvCreateSessionResult> CreateSessionResult;
    THolder<TEvPrivate::TEvDescribePathResult> DescribePathResult;
    THolder<TEvPrivate::TEvDescribeTableResult> DescribeTableResult;
    THolder<TEvPrivate::TEvDescribeTopicResult> DescribeTopicResult;

    THandlerActorYdbcMetaCollector(
            const TYdbcLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
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

        Become(&THandlerActorYdbcMetaCollector::StateWorkDatabase, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &DatabaseInfo);
            if (success) {
                TString endpoint = DatabaseInfo["endpoint"].GetStringRobust();
                TStringBuf scheme;
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                Scheme = scheme;
                Host = host;
                NHttp::TUrlParameters urlParams(uri);
                Database = urlParams["database"];
                Path = Request.Parameters["path"];
                Token = Request.GetAuthTokenForIAM();

                if (!Database.empty() && !Path.empty()) {
                    // TODO: validation
                    FullPath = Database + Path;
                    Deadline = ctx.Now() + MAX_RETRY_TIME;
                    RequestDescribePath(ctx);
                    return;
                } else {
                    if (Database.empty()) {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
                    if (Path.empty()) {
                        message = "Invalid path argument";
                        status = "400";
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

    void RequestDescribePath(const NActors::TActorContext& ctx) {
        SchemeClient = Location.GetSchemeClientPtr(Host, Scheme, NYdb::TCommonClientSettings().Database(Database).AuthToken(Token));

        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;

        SchemeClient->DescribePath(FullPath, NYdb::NScheme::TDescribePathSettings().ClientTimeout(GetClientTimeout()))
                .Subscribe([actorSystem, actorId](const NYdb::NScheme::TAsyncDescribePathResult& result) mutable {
            NYdb::NScheme::TAsyncDescribePathResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvDescribePathResult(res.ExtractValue()));
        });
        ++Requests;
        Become(&THandlerActorYdbcMetaCollector::StateWorkDescribePath);
    }

    void RequestCreateSession(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        NYdb::NTable::TClientSettings settings;
        settings.Database(Database);
        settings.AuthToken(Token);
        TableClient = Location.GetTableClientPtr(Host, Scheme, settings);
        TableClient->CreateSession(NYdb::NTable::TCreateSessionSettings().ClientTimeout(GetClientTimeout()))
                .Subscribe([actorSystem, actorId](const NYdb::NTable::TAsyncCreateSessionResult& result) {
            NYdb::NTable::TAsyncCreateSessionResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
        });
        ++Requests;
        Become(&THandlerActorYdbcMetaCollector::StateWorkCreateSession);
    }

    void RequestDescribeTopic(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        NYdb::NTopic::TTopicClientSettings settings;
        settings.Database(Database);
        settings.AuthToken(Token);
        TopicClient = Location.GetTopicClientPtr(Host, Scheme, settings);
        TopicClient->DescribeTopic(FullPath, NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true).ClientTimeout(GetClientTimeout()))
            .Subscribe([actorSystem, actorId](const NYdb::NTopic::TAsyncDescribeTopicResult& result) mutable {
                NYdb::NTopic::TAsyncDescribeTopicResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDescribeTopicResult(res.ExtractValue()));
            });
        ++Requests;
        Become(&THandlerActorYdbcMetaCollector::StateWorkDescribeTopic);
    }

    void RequestDescribeTable(const NActors::TActorContext& ctx) {
        NYdb::NTable::TSession session = CreateSessionResult->Result.GetSession();
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        session.DescribeTable(FullPath, NYdb::NTable::TDescribeTableSettings()
                              .WithKeyShardBoundary(true)
                              .WithTableStatistics(true)
                              .WithPartitionStatistics(true)
                              .ClientTimeout(GetClientTimeout()))
                .Subscribe([actorSystem, actorId](const NYdb::NTable::TAsyncDescribeTableResult& result) mutable {
            NYdb::NTable::TAsyncDescribeTableResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvDescribeTableResult(res.ExtractValue()));
        });
        ++Requests;
        Become(&THandlerActorYdbcMetaCollector::StateWorkDescribeTable);
    }

    void HandleDescribePath(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        RequestDescribePath(ctx);
    }

    void HandleCreateSession(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        RequestCreateSession(ctx);
    }

    void HandleDescribeTable(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        RequestDescribeTable(ctx);
    }

    void HandleDescribeTopic(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        RequestDescribeTopic(ctx);
    }

    void Handle(TEvPrivate::TEvDescribePathResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        DescribePathResult = event->Release();
        const auto& result(DescribePathResult->Result);
        if (result.IsSuccess()) {
            if (result.GetEntry().Type == NYdb::NScheme::ESchemeEntryType::Table) {
                RequestCreateSession(ctx);
            }
            if (result.GetEntry().Type == NYdb::NScheme::ESchemeEntryType::ColumnTable) {
                RequestCreateSession(ctx);
            }
            if (result.GetEntry().Type == NYdb::NScheme::ESchemeEntryType::Topic) {
                RequestDescribeTopic(ctx);
            }
        } else if (IsRetryableError(result) && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
            return;
        }
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        CreateSessionResult = event->Release();
        const NYdb::NTable::TCreateSessionResult& result(CreateSessionResult->Result);
        if (result.IsSuccess()) {
            RequestDescribeTable(ctx);
        } else if (IsRetryableError(result) && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
            return;
        }
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvDescribeTableResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        DescribeTableResult = event->Release();
        const NYdb::NTable::TDescribeTableResult& result(DescribeTableResult->Result);
        if (result.IsSuccess()) {

        } else if (IsRetryableError(result) && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
            return;
        }
        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void Handle(TEvPrivate::TEvDescribeTopicResult::TPtr event, const NActors::TActorContext& ctx) {
        --Requests;
        DescribeTopicResult = event->Release();
        const NYdb::NTopic::TDescribeTopicResult& result(DescribeTopicResult->Result);
        if (result.IsSuccess()) {
        } else if (IsRetryableError(result) && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
        }

        if (Requests == 0) {
            ReplyAndDie(ctx);
        }
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        NHttp::THttpOutgoingResponsePtr response;
        if (DescribePathResult != nullptr) {
            const auto& describePathResult(DescribePathResult->Result);
            if (describePathResult.IsSuccess()) {
                WriteSchemeEntry(root, describePathResult.GetEntry());
                if (Path == "/") {
                    root["name"] = DatabaseInfo["name"];
                }
                if (DescribeTableResult != nullptr) {
                    const NYdb::NTable::TDescribeTableResult& describeTableResult(DescribeTableResult->Result);
                    if (describeTableResult.IsSuccess()) {
                        NYdb::NTable::TTableDescription tableDescription = describeTableResult.GetTableDescription();

                        NJson::TJsonValue& columns = root["columns"];
                        WriteColumns(columns, tableDescription.GetColumns(), tableDescription.GetPrimaryKeyColumns());
                        NJson::TJsonValue& indexes = root["indexes"];
                        WriteIndexes(indexes, tableDescription.GetIndexDescriptions());
                        NJson::TJsonValue& shards = root["shards"];
                        WriteShards(shards, tableDescription);

                        NJson::TJsonValue& config = root["tableDescription"];
                        NProtobufJson::Proto2Json(NYdb::TProtoAccessor::GetProto(tableDescription), config, Proto2JsonConfig);
                    }
                } else if (DescribeTopicResult != nullptr) {
                    const NYdb::NTopic::TDescribeTopicResult& describeTopicResult(DescribeTopicResult->Result);
                    if (describeTopicResult.IsSuccess()) {
                        NYdb::NTopic::TTopicDescription topicDescription = describeTopicResult.GetTopicDescription();
                        NJson::TJsonValue& config = root["topicDescription"];
                        if (Request.Parameters["use_proto_description"] == "1") {
                            NProtobufJson::Proto2Json(NYdb::TProtoAccessor::GetProto(topicDescription), config, Proto2JsonConfig);
                        } else {

                            auto& consumers = config["consumers"];
                            WriteTopicConsumers(consumers, topicDescription.GetConsumers());

                            auto& supportedCodecs = config["supportedCodecs"];
                            supportedCodecs.SetType(NJson::JSON_ARRAY);
                            for (const auto codec : topicDescription.GetSupportedCodecs()) {
                                supportedCodecs.AppendValue(TStringBuilder() << codec);
                            }

                            config["retentionPeriodSeconds"] = topicDescription.GetRetentionPeriod().Seconds();
                            if (topicDescription.GetRetentionStorageMb().Defined()) {
                                config["retentionStorageMB"] = *topicDescription.GetRetentionStorageMb();
                            } else {
                                config["retentionStorageMB"] = 0;
                            }

                            auto& partitionSettings = config["partitioningSettings"];
                            WriteTopicPartitioningSettings(partitionSettings, topicDescription.GetPartitioningSettings());

                            config["partitionWriteSpeedKbps"] = topicDescription.GetPartitionWriteSpeedBytesPerSecond() / 1_KB;
                            auto& partitions = config["partitions"];
                            WriteTopicPartitions(partitions, topicDescription.GetPartitions());
                            config["partitionsTotal"] = topicDescription.GetTotalPartitionsCount();
                            WriteTopicMeteringMode(config["meteringMode"], topicDescription.GetMeteringMode());
                        }
                    }
                }
                response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
            } else {
                response = CreateStatusResponse(Request.Request, describePathResult);
            }
        } else {
            response = Request.Request->CreateResponseServiceUnavailable("No DescribePathResult");
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWorkDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateWorkDescribePath) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvDescribePathResult, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, HandleDescribePath);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateWorkCreateSession) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, HandleCreateSession);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateWorkDescribeTable) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvDescribeTableResult, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, HandleDescribeTable);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateWorkDescribeTopic) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvDescribeTopicResult, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, HandleDescribeTopic);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcMeta : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcMeta> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcMeta>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcMeta(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcMeta::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        ctx.Register(new THandlerActorYdbcMetaCollector(Location, event->Sender, event->Get()->Request, HttpProxyId));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
