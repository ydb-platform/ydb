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
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>

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
    bool IsAutoScaledTopic = false;
    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> PQGroupInfo;
    ui64 TabletId;
    TActorId WriteActorId;
    ui64 SeqNo = 0;
    TString SourceId = "1";

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
        w->SetSourceId(SourceId);
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

        const auto& pqDescription = PQGroupInfo->Description;
        MeteringMode = info.PQGroupInfo->Description.GetPQTabletConfig().GetMeteringMode();
        if (pqDescription.GetPQTabletConfig().GetPartitionStrategy().HasPartitionStrategyType() &&
            pqDescription.GetPQTabletConfig().GetPartitionStrategy().GetPartitionStrategyType() != 0) {
                IsAutoScaledTopic = true;
        }
        if (!Params.Has("partition")) {
            if (IsAutoScaledTopic) {
                ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "You must provide partition id you want to put records to for autoscaled topic."));
                return;
            }
            // auto chooser = NPQ::CreatePartitionChooser(pqDescription, true); // without hash?
            // if (record.explicit_hash_key().empty()) {
            //     return HexBytesToDecimal(MD5::Calc(Key));
            // } else {
            //     return BytesToDecimal(record.explicit_hash_key());
            // }
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "NOT IMPLEMENTED partition chooser"));
                return;
        }
            // TString hashKey = NPQ::AsKeyBound(GetHashKey(Message));
            // auto* partition = chooser->GetPartition(hashKey);

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


        if (!AppData(this->ActorContext())->PQConfig.GetTopicsAreFirstClassCitizen() && !pqDescription.GetPQTabletConfig().GetLocalDC()) {
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "LocalDC is not set fot federation."));
            return;
        }
        const auto& partitions = pqDescription.GetPartitions();\
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
            .WithTopicPath(TopicPath)
            .WithSourceId(SourceId);
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
            ReplyAndPassAway(Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain", "Cookies mismatch in TEvWriteResponse Handler."));
        }
        if (r->IsSuccess()) {
            ReplyAndPassAway(Viewer->GetHTTPOK(Event->Get(), "text/plain", "Recieved response"));
        } else {
            auto error = r->GetError();
            TString reason = r->GetError().Reason;
            Cerr << reason << Endl;
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", reason));
        }
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
