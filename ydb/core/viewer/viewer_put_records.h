#pragma once
#include "json_pipe_req.h"
#include "viewer.h"

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_replication.h>
#include <ydb/services/replication/grpc_service.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr::NViewer {

    struct THeader {
        TString Key;
        TString Value; // bytes
    };

class TPutRecords : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
    using TThis = TPutRecords;
    using TBase::ReplyAndPassAway;
    using TBase::GetHTTPBADREQUEST;

public:
    TPutRecords(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    TPutRecords(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev)
    {
        InitConfig(Params);
    }

private:
    TString TopicPath;
    ui32 Partition;
    TStringBuf Message;
    TStringBuf Key;
    // TVector<std::pair<TString, TStringBuf>> Headers;
    ui32 Cookie = 1;
    TVector<THeader> Headers;
    NKikimrPQ::TPQTabletConfig::EMeteringMode MeteringMode;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
    ui64 TabletId;
    TActorId WriteActorId;
    ui64 SeqNo = 0;

public:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleNavigateKeySetResult);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
        }
    }
    void Bootstrap() override {
        if (!Params.Has("path") || !Params.Has("message") || !Params.Has("seqno")) {
            TBase::Become(&TThis::StateWork);
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "fields 'path','message' and 'seqno' are required and should not be empty"));
        }


        // const auto& params(Event->Get()->Request.GetParams());
        // if (params.Has("headers")) {
        //     Split(params.Get("headers"), ",", Headers);
        // } else {
        //     return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'permissions' is required"));
        // }

        TopicPath = Params.Get("path");

        Message = Params.Get("message");

        if (Params.Has("partition")) {
            Partition = std::stoi(Params.Get("partition"));
        }

        if (Params.Has("key")) {
            Key = Params.Get("key");
        }

        SeqNo = std::stoull(Params.Get("seqno"));
        if (Params.Has("headers")) {
            int num = Params.NumOfValues("headers");
            for (int i = 0; i < num; i++) {
                NJson::TJsonValue headerJson;
                NJson::ReadJsonTree(Params.Get("headers", i), &headerJson, true);
                auto& headerMap = headerJson.GetMap();
                if (!headerMap.contains("key") || !headerMap.contains("value")) {
                    ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'headeers' must be key-value pairs"));
                }
                // auto& key = headerMap.at("key");
                // auto& value = headerMap.at("value");
                Headers.emplace_back(headerMap.at("key").GetStringRobust(), headerMap.at("value").GetStringRobust());
            }
        }

        SendSchemeCacheRequest(ActorContext());
        TBase::Become(&TThis::StateWork);
        // нужно ли проверять topic existance
        // const auto& pqDescription = PQGroupInfo->Description;
        // auto chooser = NPQ::CreatePartitionChooser(pqDescription, true);
        // NPQ::TPartitionWriterOpts opts;
        // opts.WithDeduplication(false)
        //     .WithSourceId(SourceId)
        //     .WithTopicPath(topicPartition.TopicPath)
        //     .WithCheckRequestUnits(topicInfo.MeteringMode, Context->RlContext)
        //     .WithKafkaProducerInstanceId(producerInstanceId);
        // auto* writerActor = CreatePartitionWriter(SelfId(), partition->TabletId, topicPartition.PartitionId, opts);

        // auto writer = PartitionWriter({TopicPath, static_cast<ui32>(Partition)}, producerInstanceId, transactionalId, ctx);

        // ReplyAndPassAway(Viewer->GetHTTPOK(Event->Get(), "text/plain", "field 'path' is required and should not be empty"));
    }

    THolder<NPQ::TEvPartitionWriter::TEvWriteRequest> FormWriteRequest() {
        ui64 totalSize = 0;
        auto ev = MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>();
        auto& request = ev->Record;
        auto* partitionRequest = request.MutablePartitionRequest();
        partitionRequest->SetTopic(TopicPath);
        partitionRequest->SetPartition(Partition);
        partitionRequest->SetCookie(Cookie);

        NKikimrPQClient::TDataChunk proto;
        proto.set_codec(NPersQueueCommon::RAW);
        for(auto& h : Headers) {
            auto res = proto.AddMessageMeta();
            if (h.Key) {
                res->set_key(static_cast<const char*>(h.Key.data()), h.Key.size());
            }
            if (h.Value) {
                res->set_value(static_cast<const char*>(h.Value.data()), h.Value.size());
            }
        }

        if (Key) {
            auto res = proto.AddMessageMeta();
            res->set_key("__key");
            res->set_value(static_cast<const char*>(Key.data()), Key.size());
        }
        proto.SetData(static_cast<const void*>(Message.data()), Message.size());
        TString str;
        bool res = proto.SerializeToString(&str);
        Y_ABORT_UNLESS(res);
        auto w = partitionRequest->AddCmdWrite();
        w->SetData(str);
        // create timestamp?? какой ставить?
        w->SetCreateTimeMS(TInstant::Now().MilliSeconds());
        w->SetDisableDeduplication(true);
        w->SetUncompressedSize(Message ? Message.size() : 0);
        // w->SetClientDC(clientDC);
        w->SetIgnoreQuotaDeadline(true);
        w->SetExternalOperation(true);
        w->SetSeqNo(SeqNo);
        totalSize += Message ? Message.size() : 0;
        partitionRequest->SetPutUnitsSize(NPQ::PutUnitsSize(totalSize));
        return std::move(ev);

    }
    void SendSchemeCacheRequest(const TActorContext& ctx) {
        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        // KAFKA_LOG_D("Produce actor: Describe topic '" << topicPath << "'");
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = NKikimr::SplitPath(TopicPath);
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;
        entry.SyncVersion = true;

        request->ResultSet.emplace_back(entry);

        request->DatabaseName = CanonizePath(Database);

        ctx.Send(MakeSchemeCacheID(), MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(request.release()));
    }

    void HandleNavigateKeySetResult(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        auto* navigate = ev.Get()->Get()->Request.Get();
        auto& info = navigate->ResultSet.front();
        if (info.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "TEvNavigateKeySet finished with unsuccessful status. Check if topic exists"));
        }

        MeteringMode = info.PQGroupInfo->Description.GetPQTabletConfig().GetMeteringMode();

        // if (!Context->RequireAuthentication || info.SecurityObject->CheckAccess(NACLib::EAccessRights::UpdateRow, *Context->UserToken)) {
        //         topic.Status = OK;
        //         topic.ExpirationTime = now + TOPIC_OK_EXPIRATION_INTERVAL;
        //         topic.PartitionChooser = CreatePartitionChooser(info.PQGroupInfo->Description);
        //     } else {
        //         KAFKA_LOG_W("Produce actor: Unauthorized PRODUCE to topic '" << topicPath << "'");
        //         topic.Status = UNAUTHORIZED;
        //         topic.ExpirationTime = now + TOPIC_UNATHORIZED_EXPIRATION_INTERVAL;
        // }
        // TIntrusivePtr<NACLib::TUserToken> token(Event->Get()->UserToken);
        if (Event->Get()->UserToken.empty()) {
            if (AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
                ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Unauthenticated access is forbidden, please provide credentials, PersQueue::ErrorCode::ACCESS_DENIED"));
                return;
            }
        } else {
            if (!info.SecurityObject->CheckAccess(NACLib::EAccessRights::UpdateRow, NACLib::TUserToken(Event->Get()->UserToken))) {
                ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Unauthenticated access is forbidden, please provide credentials, PersQueue::ErrorCode::ACCESS_DENIED"));
                return;
            };
        }

        PQGroupInfo = info.PQGroupInfo;
        // SetMeteringMode(PQGroupInfo->Description.GetPQTabletConfig().GetMeteringMode());

        if (!AppData(this->ActorContext())->PQConfig.GetTopicsAreFirstClassCitizen() && !PQGroupInfo->Description.GetPQTabletConfig().GetLocalDC()) {
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "LocalDC is not set fot federation."));
            return;
        }
        const auto& pqDescription = PQGroupInfo->Description;
        if (!Partition) {
            auto chooser = NPQ::CreatePartitionChooser(pqDescription, true);
            // chooser->GetPartition()
        }
        const auto& partitions = PQGroupInfo->Description.GetPartitions();\
        for (auto& partition : partitions) {
            auto partitionId = partition.GetPartitionId();
            if (partitionId == Partition) {
                TabletId = partition.GetTabletId();
                // SendPQReadRequest();
                // RequestDone();
            }
        }
        NPQ::TPartitionWriterOpts opts;
        opts.WithDeduplication(false)
            .WithTopicPath(TopicPath);
            // .WithSourceId(SourceId)
            // .WithCheckRequestUnits(MeteringMode, Context->RlContext)
            // .WithKafkaProducerInstanceId(producerInstanceId);

        auto* writerActor = CreatePartitionWriter(SelfId(), TabletId, Partition, opts);
        WriteActorId = ctx.RegisterWithSameMailbox(writerActor);
        auto writeEvent = FormWriteRequest();
        Send(WriteActorId, std::move(writeEvent));
        // ReplyAndPassAway(Viewer->GetHTTPOK(Event->Get(), "text/plain", "field 'path' is required and should not be empty"));
    }

    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr request, const TActorContext&) {
        auto r = request->Get();
        auto cookie = r->Record.GetPartitionResponse().GetCookie();
        if (cookie != Cookie) {
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Cookies do not match"));
        }
        // auto error = r->GetError();
        // Cerr << error << Endl;
        ReplyAndPassAway(Viewer->GetHTTPOK(Event->Get(), "text/plain", "Recieved response"));
    }
    void ReplyAndPassAway() override {
        TStringStream jsonBody;
        Send(WriteActorId, new TEvents::TEvPoison());
        TBase::ReplyAndPassAway(GetHTTPOKJSON(jsonBody.Str()));
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        post:
            tags:
              - viewer
            summary: Produce to topic
            description: Puts data to the topic
            parameters:
              - name: database
                description: database name
                type: string
                required: false
              - name: path
                description: path of topic
                required: true
                type: string
              - name: partition
                description: partition to read from
                required: true
                type: integer
              - name: message
                description: message we want to produce
                required: true
                type: bytes
              - name: key
                description: message key
                required: false
                type: string
              - name: message_size_limit
                description: max size of single message (default = 1_MB)
                required: false
                type: integer
              - name: headers
                description: message metadata
                required: false
                in: query
                type: array
                items:
                    type: object
                    title: MetadataItem
                    properties:
                        key:
                            type: string
                        value:
                            type: bytes
            responses:
                200:
                    description: OK
                    content:
                        application/json:
                            schema:
                                {}
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                500:
                    description: Internal Server Error
                504:
                    description: Gateway Timeout
        )___");

        node["post"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<NKikimrClient::TPersQueuePartitionResponse>();
        return node;
    }
};

} // namespace NKikimr::NViewer
