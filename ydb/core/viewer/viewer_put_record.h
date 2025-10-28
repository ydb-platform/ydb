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
#include <ydb/services/lib/sharding/sharding.h>
#include <library/cpp/digest/md5/md5.h>
#include <ydb/core/persqueue/public/pq_rl_helpers.h>

namespace NKikimr::NViewer {

    struct TMetadataItem {
        TString Key;
        TString Value;
    };

class TPutRecord : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
    using TThis = TPutRecord;
    using TBase::ReplyAndPassAway;
    using TBase::GetHTTPBADREQUEST;

public:
    TPutRecord(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    TPutRecord(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev)
    {
        InitConfig(Params);
    }

private:
    TString TopicPath;
    std::optional<ui32> Partition;
    TString Message;
    TString Key;
    ui32 Cookie = 1;
    TVector<TMetadataItem> Metadata;
    ui64 TabletId;
    TActorId WriteActorId;
    TRequestResponse<TEvTxProxySchemeCache::TEvNavigateKeySetResult> ResultSchemeCache;

public:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteResponse, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvInitResult, Handle);
            HFunc(NPQ::TEvPartitionWriter::TEvWriteAccepted, HandleAccepting);
        }
    }
    void Bootstrap() override {
        if (!Params.Has("path") || !Params.Has("message")) {
            TBase::Become(&TThis::StateWork);
            return ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", "fields 'path' and 'message' are required and should not be empty"));
        }

        TopicPath = Params.Get("path");

        Message = Params.Get("message");

        if (Params.Has("partition")) {
            Partition = std::stoi(Params.Get("partition"));
        }

        if (Params.Has("key")) {
            Key = TStringBuf(Params.Get("key").data(), Params.Get("key").size());
        }

        if (Params.Has("metadata")) {
            int num = Params.NumOfValues("metadata");
            for (int i = 0; i < num; i++) {
                NJson::TJsonValue metadataJson;
                NJson::ReadJsonTree(Params.Get("metadata", i), &metadataJson, true);
                auto& metadataMap = metadataJson.GetMap();
                if (!metadataMap.contains("key") || !metadataMap.contains("value")) {
                    return ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", "field 'metadata' must be key-value pairs"));
                }
                Metadata.emplace_back(metadataMap.at("key").GetStringRobust(), metadataMap.at("value").GetStringRobust());
            }
        }

        ResultSchemeCache = MakeRequestSchemeCacheNavigateWithToken(TopicPath, NACLib::EAccessRights::UpdateRow, 1);
        TBase::Become(&TThis::StateWork);
    }

    THolder<NPQ::TEvPartitionWriter::TEvWriteRequest> FormWriteRequest() {
        auto ev = MakeHolder<NPQ::TEvPartitionWriter::TEvWriteRequest>();
        auto& request = ev->Record;
        auto* partitionRequest = request.MutablePartitionRequest();
        partitionRequest->SetTopic(TopicPath);
        partitionRequest->SetPartition(*Partition);
        partitionRequest->SetCookie(Cookie);
        partitionRequest->SetIsDirectWrite(true);

        NKikimrPQClient::TDataChunk proto;
        proto.set_codec(NPersQueueCommon::RAW);
        for(auto& h : Metadata) {
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
        w->SetCreateTimeMS(TInstant::Now().MilliSeconds());
        w->SetDisableDeduplication(true);
        w->SetUncompressedSize(Message ? Message.size() : 0);
        w->SetIgnoreQuotaDeadline(false);
        w->SetExternalOperation(true);
        ui64 totalSize = Message ? Message.size() : 0;
        partitionRequest->SetPutUnitsSize(NPQ::PutUnitsSize(totalSize));
        return std::move(ev);
    }

     void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto& result(ResultSchemeCache);
        if (!result.Set(std::move(ev))) {
            return ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", "TEvNavigateKeySet finished with unsuccessful status. Check if topic exists or if you have UpdateRow access rights."));
        }
        auto* navigate = result->Request.Get();
        auto& info = navigate->ResultSet.front();
        if (info.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", "TEvNavigateKeySet finished with unsuccessful status. Check if topic exists or if you have UpdateRow access rights."));
        }

        const auto& pqDescription = info.PQGroupInfo->Description;

        if (!Params.Has("partition")) {
            auto chooser = NPQ::CreatePartitionChooser(pqDescription, false);
            if (!Key.empty()) {
                auto* partition = chooser->GetPartition(Key);
                Partition = partition->PartitionId;
            } else {
                auto* partition = chooser->GetPartition(CreateGuidAsString());
                Partition = partition->PartitionId;
            }
        }

        const auto& partitions = pqDescription.GetPartitions();
        bool partitionFound = false;
        for (auto& partition : partitions) {
            auto partitionId = partition.GetPartitionId();
            if (partitionId == Partition) {
                TabletId = partition.GetTabletId();
                partitionFound = true;
            }
        }
        if (!partitionFound) {
            return ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", "Partition not found."));
        }
        NPQ::TPartitionWriterOpts opts;
        opts.WithDeduplication(false)
            .WithTopicPath(TopicPath);

        auto* writerActor = CreatePartitionWriter(SelfId(), TabletId, *Partition, opts);
        WriteActorId = ActorContext().RegisterWithSameMailbox(writerActor);
        auto writeEvent = FormWriteRequest();
        Send(WriteActorId, std::move(writeEvent));
    }

    void Handle(NPQ::TEvPartitionWriter::TEvInitResult::TPtr request, const TActorContext& /*ctx*/) {
        Cerr << "Produce actor: Init " << request->Get()->ToString();
    }

    void Handle(NPQ::TEvPartitionWriter::TEvWriteResponse::TPtr request, const TActorContext&) {
        auto r = request->Get();
        const auto& resp = r->Record.GetPartitionResponse();
        auto cookie = resp.GetCookie();
        if (cookie != Cookie) {
            return ReplyAndPassAway(TBase::GetHTTPINTERNALERROR("text/plain", "Cookies mismatch in TEvWriteResponse Handler."));
        }
        if (r->IsSuccess()) {
            return ReplyAndPassAway(TBase::GetHTTPOK("text/plain", "Recieved response"));
        } else {
            auto error = r->GetError();
            TString reason = r->GetError().Reason;
            Cerr << reason << Endl;
            return ReplyAndPassAway(TBase::GetHTTPBADREQUEST("text/plain", reason));
        }
    }

    void HandleAccepting(NPQ::TEvPartitionWriter::TEvWriteAccepted::TPtr request, const TActorContext&) {
        auto r = request->Get();
        auto cookie = r->Cookie;
        if (cookie != Cookie) {
            return ReplyAndPassAway(TBase::GetHTTPINTERNALERROR("text/plain", "Cookies mismatch in TEvWriteAccepted Handler."));
        }
    }

    void HandleDisconnected(NPQ::TEvPartitionWriter::TEvDisconnected::TPtr, const TActorContext&) {
        return ReplyAndPassAway(TBase::GetHTTPINTERNALERROR("text/plain", "Partition writer is disconnected."));
    }

    void ReplyAndPassAway() override {
        TStringStream jsonBody;
        TBase::ReplyAndPassAway(GetHTTPOKJSON(jsonBody.Str()));
    }

    void PassAway() override {
        Send(WriteActorId, new TEvents::TEvPoison());
        TBase::PassAway();
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
              - name: metadata
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
