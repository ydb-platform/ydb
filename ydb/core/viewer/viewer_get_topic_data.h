#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

namespace NKikimr::NViewer {

class TGetTopicData : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    using TBase::GetHTTPBADREQUEST;

private:
    void HandleDescribe(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        Cerr << "TGetTopicData - Handle describe" << Endl;

        auto ev_ = std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(ev->Release().Release());
        if (!TBase::IsSuccess(ev_)) {
            auto error = TStringBuilder() << "While trying to find topic: '" << TopicPath << "' got error '" << TBase::GetError(ev_) << "'";
            return ReplyAndPassAwayIfAlive(GetHTTPBADREQUEST("text/plain", error));

        }
        const auto& result = ev_->Request;
        const auto response = result->ResultSet.front();
        if (response.Self->Info.GetPathType() != NKikimrSchemeOp::EPathTypePersQueueGroup) {
            auto error = TStringBuilder() << "No such topic '" << TopicPath << "";
            return ReplyAndPassAwayIfAlive(GetHTTPBADREQUEST("text/plain", error));

        }
        const auto& partitions = response.PQGroupInfo->Description.GetPartitions();
        for (auto& partition : partitions) {
            auto partitionId = partition.GetPartitionId();
            if (partitionId == PartitionId) {
                TabletId = partition.GetTabletId();
                return SendPQReadRequest();
            }
        }
    }

    void SendPQReadRequest() {
        Cerr << "TGetTopicData - Send PQ request" << Endl;
        const auto& ctx = ActorContext();
        PipeClient = ConnectTabletPipe(TabletId);

        NKikimrClient::TPersQueueRequest request;
        request.MutablePartitionRequest()->SetTopic(TopicPath);
        request.MutablePartitionRequest()->SetPartition(PartitionId);
        ActorIdToProto(PipeClient, request.MutablePartitionRequest()->MutablePipeClient());

        auto cmdRead = request.MutablePartitionRequest()->MutableCmdRead();
        cmdRead->SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
        cmdRead->SetCount(Limit);
        cmdRead->SetOffset(Offset);
        cmdRead->SetTimeoutMs(READ_TIMEOUT_MS);
        cmdRead->SetExternalOperation(true);

        TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
        req->Record.Swap(&request);
        NTabletPipe::SendData(ctx, PipeClient, req.Release());
    }

    void HandlePQResponse(TEvPersQueue::TEvResponse::TPtr& ev) {
        Cerr << "TGetTopicData - Handle PQ response" << Endl;

        ReadResponse = ev->Release();
        const auto& record = ReadResponse->Record;
        if (record.GetStatus() ==  NMsgBusProxy::MSTATUS_ERROR) {
            switch (record.GetErrorCode()) {
                case ::NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET:
                case ::NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET:
                    ReplyAndPassAwayIfAlive(GetHTTPBADREQUEST("text/plain", "Bad offset"), record.GetErrorReason());
                    break;
                default:
                    ReplyAndPassAwayIfAlive(GetHTTPINTERNALERROR("text/plain", "Error rtrying to read messages"), record.GetErrorReason());
            }
            return;
        }
        RequestDone();
    }

    STATEFN(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleDescribe);
            hFunc(TEvPersQueue::TEvResponse, HandlePQResponse);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    NYdb::NTopic::ICodec* GetCodec(NPersQueueCommon::ECodec codec) {
        ui32 codecId = static_cast<ui32>(codec);
        auto iter = Codecs.find(codecId);
        if (iter != Codecs.end()) {
            return iter->second.Get();
        }
        switch (codec) {
            case NPersQueueCommon::GZIP: {
                auto [iterator, ins] = Codecs.emplace(codecId, MakeHolder<NYdb::NTopic::TGzipCodec>());
                Y_ABORT_UNLESS(ins);
                return iterator->second.Get();
                break;
            }
            case NPersQueueCommon::ZSTD: {
                auto [iterator, ins] = Codecs.emplace(codecId, MakeHolder<NYdb::NTopic::TZstdCodec>());
                Y_ABORT_UNLESS(ins);
                return iterator->second.Get();
            }
            default:
                ReplyAndPassAwayIfAlive(GetHTTPINTERNALERROR("text/plain", "Error trying to decompress messages"));
                return nullptr;
        }
    }

    bool GetIntegerParam(const TString& name, i64& value) {
        const auto& params(Event->Get()->Request.GetParams());
        if (params.Has(name)) {
            value = FromStringWithDefault<i32>(params.Get(name), -1);
            if (value == -1) {
                auto error = TStringBuilder() << "field ' "<< name << "' has invalid value, an interger >= 0 is expected";
                ReplyAndPassAwayIfAlive(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", error));
                return false;
            }
            return true;
        } else {
            auto error = TStringBuilder() << "field ' "<< name << "' is required";
            ReplyAndPassAwayIfAlive(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", error));
            return false;
        }
    }

    void ReplyAndPassAwayIfAlive(TString data, const TString& error = {}) {
        if (IsDead)
            return;
        IsDead = true;
        TBase::ReplyAndPassAway(data, error);
    }

public:
    TGetTopicData(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        Cerr << "TGetTopicData::Bootstrap" << Endl;
        if (NeedToRedirect()) {
            return;
        }
        const auto& params(Event->Get()->Request.GetParams());
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);

        GetIntegerParam("partition", PartitionId);
        GetIntegerParam("offset", Offset);
        Limit = FromStringWithDefault<ui32>(params.Get("limit"), 10);

        if (IsDead)
            return;
        if (params.Has("topic_path")) {
            TopicPath = params.Get("topic_path");
            Cerr << "TGetTopicData - Request navigate" << Endl;

            RequestSchemeCacheNavigate(params.Get("topic_path"));
        } else {
            return ReplyAndPassAwayIfAlive(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'topic_path' is required"));
        }
    }

    void ReplyAndPassAway() override {
        if (ReadResponse == nullptr) {
            return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No Topic read response"));
        }
        const auto& response = ReadResponse->Record.GetPartitionResponse();
        NJson::TJsonValue jsonResponse{NJson::EJsonValueType::JSON_ARRAY};

        if (response.HasCmdReadResult()) {
            const auto& readResult = response.GetCmdReadResult();
            if (readResult.GetReadingFinished()) {
                ReplyAndPassAwayIfAlive(GetHTTPBADREQUEST("text/plain", "Bad Partition id"));
                return;
            }

            const auto& results = readResult.GetResult();
            for (auto& r : results) {
                auto dataChunk = (NKikimr::GetDeserializedData(r.GetData()));


                NJson::TJsonValue jsonRecord{NJson::EJsonValueType::JSON_MAP};
                jsonRecord.InsertValue("Offset", r.GetOffset());
                jsonRecord.InsertValue("CreateTimestamp", r.GetCreateTimestampMS());
                jsonRecord.InsertValue("WriteTimestamp", r.GetWriteTimestampMS());
                i64 diff = r.GetWriteTimestampMS() - r.GetCreateTimestampMS();
                if (diff < 0) {
                    diff = 0;
                }
                jsonRecord.InsertValue("TimestampDiff", diff);
                jsonRecord.InsertValue("Size", dataChunk.GetData().size());

                if (dataChunk.HasCodec() && dataChunk.GetCodec() != NPersQueueCommon::RAW) {
                    const NYdb::NTopic::ICodec* codec = GetCodec(static_cast<NPersQueueCommon::ECodec>(dataChunk.GetCodec()));
                    TString decompressed = codec->Decompress(dataChunk.GetData());
                    jsonRecord.InsertValue("Message", decompressed);
                    jsonRecord.InsertValue("OriginalSize", decompressed.size());


                } else {
                    jsonRecord.InsertValue("Message", dataChunk.GetData());
                    jsonRecord.InsertValue("OriginalSize", dataChunk.GetData().size());
                }
                jsonRecord.InsertValue("Codec", dataChunk.GetCodec());
                jsonRecord.InsertValue("ProducerId", r.GetSourceId());
                jsonRecord.InsertValue("SeqNo", r.GetSeqNo());

                if (dataChunk.MessageMetaSize() > 0) {
                    auto jsonMetadata = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
                    for (const auto& metadata : dataChunk.GetMessageMeta()) {
                        auto jsonMetadataItem = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                        jsonMetadataItem.InsertValue("Key", metadata.key());
                        jsonMetadataItem.InsertValue("Value", metadata.value());
                        jsonMetadata.AppendValue(std::move(jsonMetadataItem));
                    }
                    jsonRecord.InsertValue("Metadata", std::move(jsonMetadata));

                }
                jsonResponse.AppendValue(std::move(jsonRecord));
            }
        }


        ReplyAndPassAway(GetHTTPOKJSON(std::move(jsonResponse)));
    }

private:
    NActors::TActorId PipeClient;
    ui64 TabletId;
    TString TopicPath;
    i64 PartitionId;
    i64 Offset;
    i64 Limit;
    ui32 Timeout = 0;
    bool IsDead = false;

    TAutoPtr<TEvPersQueue::TEvResponse> ReadResponse;
    TMap<ui32, THolder<NYdb::NTopic::ICodec>> Codecs;

    static constexpr ui32 READ_TIMEOUT_MS = 1000;
public:

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        get:
            tags:
              - viewer
            summary: ACL information
            description: Returns information about ACL of an object
            parameters:
              - name: database
                in: query
                description: database name
                type: string
                required: false
              - name: topic_path
                in: query
                description: path of topic
                required: true
                type: string
              - name: partition
                in: query
                description: partition to read from
                required: true
                type: integer
              - name: offset
                in: query
                description: start offset to read from
                required: true
                type: integer
              - name: limit
                in: query
                description: max number of messages to read
                required: false
                type: integer
              - name: timeout
                in: query
                description: timeout in ms
                required: false
                type: integer
            responses:
                200:
                    description: OK
                    content:
                        application/json:
                            schema:
                                type: array
                                items:
                                    type: object
                                    properties:
                                        Offset:
                                            type: integer
                                        CreateTimestamp:
                                            type: integer
                                        WriteTimestamp:
                                            type: integer
                                        Timestamp Diff:
                                            type: integer
                                        Message:
                                            type: string
                                        Size:
                                            type: integer
                                        OriginalSize:
                                            type: integer
                                        Codec:
                                            type: integer
                                        ProducerId:
                                            type: string
                                        SeqNo:
                                            type: integer
                                        Metadata:
                                            type: array
                                            items:
                                                type: object
                                                properties:
                                                    Key:
                                                        type: string
                                                    Value:
                                                        type: string
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                504:
                    description: Gateway Timeout
                )___");

        return node;
    }
};

} // namespace NKikimr::NViewer

