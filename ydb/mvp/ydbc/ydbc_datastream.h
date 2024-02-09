#pragma once
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/core/persqueue/metering_sink.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;


class THandlerActorYdbcDatastreamRequest : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDatastreamRequest> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDatastreamRequest>;
    static constexpr TDuration MAX_RETRY_TIME = TDuration::Seconds(1);
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    TString Method;
    TString DatabaseId;
    TString DatabaseName;
    TString DatabasePath;
    TString KinesisEndpoint;
    TString Host;
    mutable TString StreamName;
    TString FolderId;
    TString CloudId;
    TInstant Deadline;
    bool IsServerless = false;

    mutable ui32 RetriesCount = 3;
    mutable TString Phase;
    mutable TString GetShardIteratorBody;
    mutable TString Shard;

    THandlerActorYdbcDatastreamRequest(
            const TYdbcLocation& location,
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const TString& method
            )
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
        , Method(method)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        if (Method.StartsWith("simulate")) {
            NHttp::THttpOutgoingResponsePtr response = ReplyOnSimulate();
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        const TString databaseId = Request.Parameters["databaseId"];
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
        Deadline = ctx.Now() + MAX_RETRY_TIME;
        Become(&THandlerActorYdbcDatastreamRequest::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        using namespace NYdb::NDataStreams::V1;
        if (event->Get()->Error) {
            NHttp::THttpOutgoingResponsePtr response =
                Request.Request->CreateResponse("500", event->Get()->Error, "", "");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }
        TString status{event->Get()->Response->Status};
        TString message(event->Get()->Error);
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            if (event->Get()->Request->URL.StartsWith("/ydbc/" + Location.Name + "/database" + "?")) {
                NJson::TJsonValue responseData;
                bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
                if (success) {
                    TString endpoint = responseData["endpoint"].GetStringRobust();
                    TStringBuf scheme;
                    TStringBuf host;
                    TStringBuf uri;
                    NHttp::CrackURL(endpoint, scheme, host, uri);
                    NHttp::TUrlParameters urlParams(uri);
                    TString database = urlParams["database"];
                    TString token = Request.GetAuthToken();
                    DatabaseName = responseData["name"].GetStringRobust();
                    DatabaseId = Request.Parameters["databaseId"];
                    DatabasePath = database;
                    FolderId = responseData["folderId"].GetStringRobust();
                    CloudId = responseData["cloudId"].GetStringRobust();
                    //TODO: take here kinesis_api_endpoint
                    if (!responseData.Has("kinesisApiEndpoint")) {
                        message = "no kinesis endpoint on database";
                        status = "400";
                    } else {
                        KinesisEndpoint = responseData["kinesisApiEndpoint"].GetStringRobust();
                        Host = KinesisEndpoint.substr(0, KinesisEndpoint.find(database));

                        if (responseData.Has("serverlessDatabase")) {
                            IsServerless = true;
                            endpoint = responseData["serverlessInternals"]["sharedEndpoint"].GetStringRobust();
                            NHttp::CrackURL(endpoint, scheme, host, uri);
                        }

                        if (!database.empty()) {
                            std::tie(message, status) = ReplyOnDatastreamsRequest(ctx);
                            if (message.empty() && status.empty()) return;
                        } else {
                            message = "Invalid database endpoint";
                            status = "400";
                        }
                    }
                }
            } else {
                if (Method == "delete" || Method == "delete_consumer" || Method == "create_consumer" || Method == "create" || Method == "update" || Method == "put_records") {
                    message = event->Get()->Response->Message;
                    contentType = event->Get()->Response->ContentType;
                    body = event->Get()->Response->Body;
                } else if (Method == "describe") {
                    ProcessDescribe(event, ctx);
                    return;
                } else if (Method == "list_shards") {
                    ProcessListShards(event, ctx);
                    return;
                } else if (Method == "get_records") {
                    //TODO: check response code and if it is request for iterator; found out iterator and use it or retry
                    //TODO: if it is response for GetRecords then retry it;
                    if (status == "200" && Phase == "get_shard_iterator") {
                        TString iterator;
                        NJson::TJsonValue responseData;
                        bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
                        if (success && responseData.Has("ShardIterator")) {
                            iterator = responseData["ShardIterator"].GetStringRobust();
                            return CallGetRecords(iterator, ctx);
                        } else {
                            status = "500";
                        }
                    }
                    if (status == "200" && Phase == "get_records") {
                        ProcessGetRecords(event, ctx);
                        return;
                    }
                    if (--RetriesCount > 0) {
                        CallGetShardIterator(StreamName, "", "", ctx);
                        return;
                    }
                    message = event->Get()->Response->Message;
                    contentType = event->Get()->Response->ContentType;
                    body = event->Get()->Response->Body;
                } else {
                    message = TStringBuilder() << "Unknown event in processing cookie " << event->Cookie;
                    status = "500";
                }
            }
        } else {
            message = event->Get()->Response->Message;
            contentType = event->Get()->Response->ContentType;
            body = event->Get()->Response->Body;
        }
        NHttp::THttpOutgoingResponsePtr response =
            Request.Request->CreateResponse(status, message, contentType, body);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }


    void Handle(TEvPrivate::TEvDataStreamsUpdateResponse::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        const NYdb::NDataStreams::V1::TUpdateStreamResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            NProtobufJson::Proto2Json(result.GetResult(),root, Proto2JsonConfig);
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void ProcessDescribe(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root, json;
        root.SetType(NJson::JSON_ARRAY);

        NHttp::THttpOutgoingResponsePtr response;

        TStringBuf status =  event->Get()->Response->Status;
        TStringBuf message = event->Get()->Response->Message;
        TStringBuf contentType = event->Get()->Response->ContentType;
        TStringBuf body = event->Get()->Response->Body;

        bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &json);
        if (json.Has("StreamDescription") && json["StreamDescription"].Has("StreamCreationTimestamp")) {
            double val = json["StreamDescription"]["StreamCreationTimestamp"].GetDoubleRobust();
            if (val < 300000) val *= 1000;
            val *= 1000;

            json["StreamDescription"]["StreamCreationTimestamp"]  = (ui64)val;
        }
        if (success && status == "200") {
            try {
                Ydb::DataStreams::V1::DescribeStreamResult proto;
                NProtobufJson::Json2Proto(json, proto, Json2ProtoConfig2);
                NProtobufJson::Proto2Json(proto, root, Proto2JsonConfig);

            } catch (const yexception& e) {
                Cerr <<  "convertation failed " << e.what();
            }

            root["databaseId"] = DatabaseId;
            root["databaseName"] = DatabaseName;
            root["folderId"] = FolderId;
            root["cloudId"] = CloudId;
            if (root.Has("streamDescription")) {
                root["endpoint"] = KinesisEndpoint;
                root["alternativeEndpoint"] = Host;
                root["alternativeRegion"] = "ru-central-1";
                root["region"] = "ru-central1";
                root["streamDescription"]["alternativeStreamName"] = DatabasePath + "/" + StreamName;
                root["streamDescription"]["streamName"] = EscapeStreamName(StreamName);
                root["streamDescription"]["owner"] = TrimAtAs(root["streamDescription"]["owner"].GetStringRobust());
                root["streamDescription"]["totalWriteQuotaKbPerSec"] = ((ui64)root["streamDescription"]["writeQuotaKbPerSec"].GetIntegerRobust())
                                                                            * root["streamDescription"]["shards"].GetArray().size();
            }
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            NHttp::THttpOutgoingResponsePtr response =
                Request.Request->CreateResponse(status, message, contentType, body);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return;
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void ProcessListShards(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root, json;
        root.SetType(NJson::JSON_ARRAY);

        NHttp::THttpOutgoingResponsePtr response;

        TStringBuf status =  event->Get()->Response->Status;
        TStringBuf message = event->Get()->Response->Message;
        TStringBuf contentType = event->Get()->Response->ContentType;
        TStringBuf body = event->Get()->Response->Body;

        bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &json);

        if (success && status == "200") {
            try {
                Ydb::DataStreams::V1::ListShardsResult proto;
                NProtobufJson::Json2Proto(json, proto, Json2ProtoConfig2);
                NProtobufJson::Proto2Json(proto, root, Proto2JsonConfig);

            } catch (const yexception& e) {
                Cerr <<  "convertation failed " << e.what();
            }

            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            NHttp::THttpOutgoingResponsePtr response =
                Request.Request->CreateResponse(status, message, contentType, body);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return;
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void ProcessGetRecords(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root, json;
        root.SetType(NJson::JSON_ARRAY);

        NHttp::THttpOutgoingResponsePtr response;

        TStringBuf status =  event->Get()->Response->Status;
        TStringBuf message = event->Get()->Response->Message;
        TStringBuf contentType = event->Get()->Response->ContentType;
        TStringBuf body = event->Get()->Response->Body;

        bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &json);

        ui32 limit{20};
        if (Request.Parameters.PostData.Has("limit")) {
            limit = FromString<ui32>(Request.Parameters["limit"]);
        }

        ui64 seqNo;
        TString previousCookie, nextCookie;
        if (success && status == "200") {

            if (json["Records"].GetArraySafe().size() > 0) {
                seqNo = FromString<ui32>(json["Records"].GetArraySafe()[0]["SequenceNumber"].GetStringRobust());
                THandlerActorYdb::TEvPrivate::TEvDataStreamsGetRecordsCustomResponse::TCookie prev(Shard, TStringBuilder() << (seqNo > limit ? (seqNo - limit) : 0));
                previousCookie = prev.ToString();
                THandlerActorYdb::TEvPrivate::TEvDataStreamsGetRecordsCustomResponse::TCookie next(Shard, TStringBuilder() << (seqNo + limit));
                nextCookie = next.ToString();

                root["cookies"]["next"] = nextCookie;
                root["cookies"]["previous"] = previousCookie;
                root["shard"]["id"] = Shard;
                for (const auto& r : json["Records"].GetArraySafe()) {
                    auto& record = root["shard"]["records"].AppendValue(NJson::TJsonValue());
                    record["data"] = Base64Decode(r["Data"].GetStringRobust());
                    record["timestamp"] = ((ui64)r["ApproximateArrivalTimestamp"].GetIntegerRobust()) * 1000;
                    record["sequenceNumber"] = r["SequenceNumber"];
                    record["partitionKey"] = r["PartitionKey"];
                }
            }

            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            NHttp::THttpOutgoingResponsePtr response =
                Request.Request->CreateResponse(status, message, contentType, body);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            return;
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvDataStreamsPutRecordsResponse::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        const NYdb::NDataStreams::V1::TPutRecordsResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            NProtobufJson::Proto2Json(result.GetResult(), root, Proto2JsonConfig);
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvDataStreamsGetRecordsCustomResponse::TPtr event, const NActors::TActorContext& ctx) {
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        const auto& result = event->Get();

        if (result->Status.GetStatus() == NYdb::EStatus::OVERLOADED && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
            return;
        }

        NHttp::THttpOutgoingResponsePtr response;
        if (result->Status.IsSuccess()) {
            root["cookies"]["next"] = result->NextCookie;
            root["cookies"]["previous"] = result->PreviousCookie;
            root["shard"]["id"] = result->Shard.Id;
            for (const auto& r : result->Shard.Records) {
                auto& record = root["shard"]["records"].AppendValue(NJson::TJsonValue());
                record["data"] = r.Data;
                record["timestamp"] = r.Timestamp;
                record["sequenceNumber"] = r.SequenceNumber;
                record["partitionKey"] = r.PartitionKey;
            }
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false),
                                                         "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result->Status);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        ReplyOnDatastreamsRequest(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvDataStreamsUpdateResponse, Handle);
            HFunc(TEvPrivate::TEvDataStreamsPutRecordsResponse, Handle);
            HFunc(TEvPrivate::TEvDataStreamsGetRecordsCustomResponse, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    std::pair<TString, TString> ReplyOnDatastreamsRequest(const NActors::TActorContext& ctx) const noexcept {
        using namespace NYdb::NDataStreams::V1;

        TString message, status;

        if (Method == "describe") {
            ui32 limit = 1000;
            StreamName = UnescapeStreamName(Request.Parameters["name"]);
            TString shard = "";

            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequest("GET", KinesisEndpoint);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
            httpRequest->Set("x-amz-target", "Kinesis_20131202.DescribeStream");

            httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"StreamName\": \"" << StreamName << "\", \"Limit\": "
                                                                          << limit << ", \"ExclusiveStartShardId\": \"" << shard << "\" }");

            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

            return {message, status};
        } else if (Method == "delete") {
            TString name = UnescapeStreamName(Request.Parameters["name"]);

            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
            httpRequest->Set("x-amz-target", "Kinesis_20131202.DeleteStream");

            httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"StreamName\": \"" << name << "\" }");

            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

            return {message, status};
        } else if (Method == "delete_consumer") {

            TString name = UnescapeStreamName(Request.Parameters["name"]);
            TString consumer = UnescapeStreamName(Request.Parameters["consumer"]);


            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
            httpRequest->Set("x-amz-target", "Kinesis_20131202.DeregisterStreamConsumer");

            httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"StreamArn\": \"" << name
                                                                 << "\", \"ConsumerName\": \"" << consumer <<  "\" }");

            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

            return {message, status};
        } else if (Method == "create_consumer") {
            TString name = UnescapeStreamName(Request.Parameters["name"]);
            TString consumer = UnescapeStreamName(Request.Parameters["consumer"]);

            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
            httpRequest->Set("x-amz-target", "Kinesis_20131202.RegisterStreamConsumer");

            httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"StreamArn\": \"" << name
                                                                 << "\", \"ConsumerName\": \"" << consumer <<  "\" }");

            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

            return {message, status};
        } else if (Method == "create") {
            ui32 shardCount = FromStringWithDefault<ui32>(Request.Parameters["shards"], 0);
            ui32 writeQuota = FromStringWithDefault<ui32>(Request.Parameters["writeQuota"], 0);
            ui32 retention = FromStringWithDefault<ui32>(Request.Parameters["retentionHours"], 0);
            ui32 storageLimit = FromStringWithDefault<ui32>(Request.Parameters["storageLimitMb"], 0);
            TString meteringMode = Request.Parameters["meterMode"] == "request_units" ? "ON_DEMAND" : "PROVISIONED";
            TString name = UnescapeStreamName(Request.Parameters["name"]);

            if (shardCount) {
                if (retention ^ storageLimit) {

                    TString name = UnescapeStreamName(Request.Parameters["name"]);

                    TStringBuilder body;

                    body << "{ \"StreamName\": \"" << name << "\" "
                         << ", \"ShardCount\": " << shardCount << " "
                         << ", \"WriteQuotaKbPerSec\": " << writeQuota << " ";

                    if (storageLimit > 0) {
                        body << ", \"RetentionStorageMegabytes\": " << storageLimit << " ";
                    } else {
                        body << ", \"RetentionPeriodHours\": " << retention << " ";
                    }
                    if (IsServerless) {
                        body << ", \"StreamModeDetails\": { \"StreamMode\" : \"" << meteringMode << "\" } ";
                    }
                    body << " }";

                    NHttp::THttpOutgoingRequestPtr httpRequest =
                        NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
                    Request.ForwardHeadersOnlyForIAM(httpRequest);
                    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
                    httpRequest->Set("x-amz-target", "Kinesis_20131202.CreateStream");

                    httpRequest->Set<&NHttp::THttpRequest::Body>(body);

                    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

                    return {message, status};
                } else {
                    message = "either storage limit or retention period must be specified";
                    status = "400";
                }
            } else {
                message = "shards must be specified and greter than zero";
                status = "400";
            }
        } else if (Method == "update") {
            ui32 shardCount = FromStringWithDefault<ui32>(Request.Parameters["shards"], 0);
            ui32 writeQuota = FromStringWithDefault<ui32>(Request.Parameters["writeQuota"], 0);
            ui32 retention = FromStringWithDefault<ui32>(Request.Parameters["retentionHours"], 0);
            ui32 storageLimit = FromStringWithDefault<ui32>(Request.Parameters["storageLimitMb"], 0);
            TMaybe<TString> meteringMode;
            if (!Request.Parameters["meterMode"].empty()) {
                meteringMode = Request.Parameters["meterMode"] == "request_units" ? "ON_DEMAND"
                                                                                : "PROVISIONED";
            }

            TString name = UnescapeStreamName(Request.Parameters["name"]);

            if (shardCount) {
                if (retention ^ storageLimit) {

                    TStringBuilder body;

                    body << "{ \"StreamName\": \"" << name << "\" "
                         << ", \"TargetShardCount\": " << shardCount << " "
                         << ", \"WriteQuotaKbPerSec\": " << writeQuota << " ";

                    if (storageLimit > 0) {
                        body << ", \"RetentionStorageMegabytes\": " << storageLimit << " ";
                    } else {
                        body << ", \"RetentionPeriodHours\": " << retention << " ";
                    }
                    if (IsServerless && meteringMode) {
                        body << ", \"StreamModeDetails\": { \"StreamMode\" : \"" << *meteringMode << "\" } ";
                    }
                    body << " }";

                    NHttp::THttpOutgoingRequestPtr httpRequest =
                        NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
                    Request.ForwardHeadersOnlyForIAM(httpRequest);
                    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
                    httpRequest->Set("x-amz-target", "Kinesis_20131202.UpdateStream");

                    httpRequest->Set<&NHttp::THttpRequest::Body>(body);

                    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

                    return {message, status};
                } else {
                    message = "either storage limit or retention period must be specified";
                    status = "400";
                }
            } else {
                message = "shards must be specified and greter than zero";
                status = "400";
            }
        } else if (Method == "put_records") {
            if (Request.Parameters.PostData.Has("records") &&
                Request.Parameters.PostData["records"].GetArray().size() > 0 &&
                Request.Parameters.PostData["records"].GetArray().size() <= 500) {

                TStringBuilder body;

                TString name = UnescapeStreamName(Request.Parameters["name"]);

                body << "{ \"StreamName\": \"" << name << "\", \"Records\": [ ";

                bool first = true;
                for (auto& jRecord : Request.Parameters.PostData["records"].GetArray()) {
                    if (!first) {
                        body << ", ";
                    }
                    first = false;
                    body << " { ";
                    body << " \"Data\": \"" << Base64Encode(jRecord["data"].GetStringRobust()) <<  "\"";
                    body << ", \"PartitionKey\": \"" << jRecord["partitionKey"].GetStringRobust() <<  "\"";
                    if (jRecord.Has("explicitHashKey")) {
                        body << ", \"ExplicitHashKey\": \"" << jRecord["explicitHashKey"].GetStringRobust() <<  "\"";
                    }

                    body << " }\n";
                }

                body << "] }";

                NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
                Request.ForwardHeadersOnlyForIAM(httpRequest);
                httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
                httpRequest->Set("x-amz-target", "Kinesis_20131202.PutRecords");

                httpRequest->Set<&NHttp::THttpRequest::Body>(body);

                ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

                return {message, status};

            } else {
                message = "number of records should be at least 1, at most 500";
                status = "400";
            }
        } else if (Method == "get_records") {
            const TString streamName = UnescapeStreamName(Request.Parameters["name"]);
            auto settings = TGetShardIteratorSettings();

/*            ui32 limit{20};
            if (Request.Parameters.PostData.Has("limit")) {
                limit = FromString<ui32>(Request.Parameters["limit"]);
            }
*/
            TString iterator = "\"ShardIteratorType\" : \"TRIM_HORIZON\"";

            if (Request.Parameters.PostData.Has("shard") && Request.Parameters.PostData.Has("cookie")) {
                    message = "Either cookie or shard should be provided; not both";
                    status = "400";
                    return {message, status};
                }

            if (Request.Parameters.PostData.Has("cookie")) {
                THandlerActorYdb::TEvPrivate::TEvDataStreamsGetRecordsCustomResponse::TCookie cookie("cookie:1;");
                try {
                    cookie = THandlerActorYdb::TEvPrivate::TEvDataStreamsGetRecordsCustomResponse::TCookie(
                        Request.Parameters.PostData["cookie"].GetStringRobust()
                    );
                } catch (yexception& e) {
                    return {e.what(), "400"};
                }

                iterator = TStringBuilder() << "\"ShardIteratorType\" : \"AFTER_SEQUENCE_NUMBER\", \"StartingSequenceNumber\":\"" << cookie.GetSeqNo() <<"\"";

                CallGetShardIterator(streamName, cookie.GetShardId(), iterator, ctx);
                return {message, status};
            }

            if (Request.Parameters.PostData.Has("shard")) {
                if (Request.Parameters.PostData["shard"].Has("timestamp") &&
                    Request.Parameters.PostData["shard"].Has("sequenceNumber")) {
                    message = "Either timestamp or sequenceNumber should be provided; not both";
                    status = "400";
                    return {message, status};
                }

                if (Request.Parameters.PostData["shard"].Has("timestamp")) {
                    NJson::TJsonValue timestampJson;
                    Request.Parameters.PostData.GetValueByPath("shard.timestamp", timestampJson);
                    const ui64 timestamp = FromString<ui64>(timestampJson.GetStringRobust()) / 1000; // in seconds
                    iterator = TStringBuilder() << "\"ShardIteratorType\" : \"AT_TIMESTAMP\", \"Timestamp\":\"" << timestamp <<"\"";
                }

                if (Request.Parameters.PostData["shard"].Has("sequenceNumber")) {
                    NJson::TJsonValue seqnoJson;
                    Request.Parameters.PostData.GetValueByPath("shard.sequenceNumber", seqnoJson);
                    iterator = TStringBuilder() << "\"ShardIteratorType\" : \"AT_SEQUENCE_NUMBER\", \"StartingSequenceNumber\":\"" << seqnoJson.GetStringRobust() <<"\"";

                }

                NJson::TJsonValue idJson;
                Request.Parameters.PostData.GetValueByPath("shard.id", idJson);
                CallGetShardIterator(streamName, idJson.GetStringRobust(), iterator, ctx);

                return {message, status};
            } else {
                message = "Either cookie or non-empty shardId should be provided";
                status = "400";
            }
        } else if (Method == "list_shards") {
            const TString streamName = UnescapeStreamName(Request.Parameters["name"]);

            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
            httpRequest->Set("x-amz-target", "Kinesis_20131202.ListShards");

            httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"StreamName\": \"" << streamName
                                                                 << "\", \"ShardFilter\": { \"Type\" : \"AT_LATEST\" } }");

            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));

            return {message, status};
        } else {
            message = "Invalid method";
            status = "400";
        }

        return {message, status};
    }

    void CallGetShardIterator(const TString& name, const TString& shard, const TString& iterator, const TActorContext& ctx) const {
        Phase = "get_shard_iterator";
        if (GetShardIteratorBody.empty()) {
            GetShardIteratorBody = TStringBuilder() << "{ \"StreamName\": \"" << name << "\", \"ShardId\": \"" << shard << "\", " << iterator << " }";
            Shard = shard;
        }

        NHttp::THttpOutgoingRequestPtr httpRequest =
                NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
        Request.ForwardHeadersOnlyForIAM(httpRequest);
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
        httpRequest->Set("x-amz-target", "Kinesis_20131202.GetShardIterator");

        httpRequest->Set<&NHttp::THttpRequest::Body>(GetShardIteratorBody);

        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    }

    void CallGetRecords(const TString& iterator, const TActorContext& ctx) const {
        Phase = "get_records";
        ui32 limit{20};
        if (Request.Parameters.PostData.Has("limit")) {
            limit = FromString<ui32>(Request.Parameters["limit"]);
        }

        NHttp::THttpOutgoingRequestPtr httpRequest =
                NHttp::THttpOutgoingRequest::CreateRequest("POST", KinesisEndpoint);
        Request.ForwardHeadersOnlyForIAM(httpRequest);
        httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.1");
        httpRequest->Set("x-amz-target", "Kinesis_20131202.GetRecords");

        httpRequest->Set<&NHttp::THttpRequest::Body>(TStringBuilder() << "{ \"ShardIterator\": \"" << iterator
                                                                    << "\", \"Limit\": " << limit <<  " }");

        ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
    }

    NHttp::THttpOutgoingResponsePtr ReplyOnSimulate() const noexcept {
        ui64 retention = FromStringWithDefault<ui32>(Request.Parameters["retentionHours"], 0);
        ui64 shardCount = FromStringWithDefault<ui32>(Request.Parameters["shards"], 0);
        ui64 storageLimitMb{0};
        ui64 writeQuotaBps{0};

        if (Method == "simulate") {
            writeQuotaBps = FromStringWithDefault<ui32>(Request.Parameters["writeQuota"], 0);
        } else if (Method == "simulate_v1") {
            storageLimitMb = FromStringWithDefault<ui32>(Request.Parameters["storageLimitMb"], 0);
            writeQuotaBps = FromStringWithDefault<ui32>(Request.Parameters["writeQuota"], 0) * 1_KB;
        }

        if (retention > 0 && storageLimitMb > 0) {
            return Request.Request->CreateResponseBadRequest(
                "Either storage limit or retention period must be specified", "text/plain");
        }

       if (!(shardCount > 0 && writeQuotaBps > 0)) {
            return Request.Request->CreateResponseBadRequest(
                "Shard number and write quota must be greater than 0", "text/plain");
        }

        TSet<NKikimr::NPQ::EMeteringJson> whatToFlush;
        ui64 reservedSpace{0};
        if (storageLimitMb > 0) {
            whatToFlush.insert(NKikimr::NPQ::EMeteringJson::ThroughputV1);
            whatToFlush.insert(NKikimr::NPQ::EMeteringJson::StorageV1);
            reservedSpace = storageLimitMb * 1_MB;
        } else {
            whatToFlush.insert(NKikimr::NPQ::EMeteringJson::ResourcesReservedV1);
            reservedSpace = TDuration::Hours(retention).Seconds() * writeQuotaBps;
        }

        NHttp::THttpOutgoingResponsePtr response;
        NKikimr::NPQ::TMeteringSink meteringSink;
        TString output{"["};
        const auto now = TInstant::Now();
        meteringSink.Create(now, {
                .FlushLimit = TDuration::Days(30),
                .FlushInterval = TDuration::Hours(1),
                .TabletId = CreateGuidAsString(),
                .YcCloudId = "cloud_id",
                .YcFolderId = "folder_id",
                .YdbDatabaseId = "database_id",
                .StreamName = "simulated_stream",
                .ResourceId = "simulated_stream",
                .PartitionsSize = shardCount,
                .WriteQuota = writeQuotaBps,
                .ReservedSpace = reservedSpace,
            }, whatToFlush, [&](TString json) { output += json + ","; });

        meteringSink.MayFlushForcibly(now + TDuration::Days(30));
        output.back() = ']';
        return Request.Request->CreateResponseOK(output, "application/json; charset=utf-8");
    }
};


class THandlerActorYdbcDatastream : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDatastream> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDatastream>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;
    const TString Method;

    THandlerActorYdbcDatastream(const TYdbcLocation& location, const NActors::TActorId& httpProxyId, const TString& method)
        : TBase(&THandlerActorYdbcDatastream::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
        , Method(method)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;

        if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcDatastreamRequest(Location, HttpProxyId, event->Sender, request, "describe"));
            return;
        } else if (request->Method == "POST") {
            ctx.Register(new THandlerActorYdbcDatastreamRequest(Location, HttpProxyId, event->Sender, request, Method));
            return;
        } else if (request->Method == "DELETE") {
            ctx.Register(new THandlerActorYdbcDatastreamRequest(Location, HttpProxyId, event->Sender, request, Method));
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

