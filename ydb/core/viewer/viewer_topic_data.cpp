#include "viewer_topic_data.h"
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

namespace NKikimr::NViewer {


class TUnpackDataToJsonActor : public TActorBootstrapped<TUnpackDataToJsonActor> {
public:
    TUnpackDataToJsonActor(THolder<TEvPersQueue::TEvResponse>&& readResponse, const TActorId& recipient,
                     ui64 maxSingleMessageSize = 1_MB, ui64 maxTotalSize = 10_MB)
        : ReadResponse(std::move(readResponse))
        , Recipient(recipient)
        , MaxMessageSize(maxSingleMessageSize)
        , MaxTotalSize(maxTotalSize)
    {}

    void Bootstrap() {
        DoWork();
    }

private:
    void DoWork() {
        ui64 totalSize = 0;
        const auto& response = ReadResponse->Record.GetPartitionResponse();
        Y_ABORT_UNLESS (response.HasCmdReadResult());
        const auto& cmdRead = response.GetCmdReadResult();

        NJson::TJsonValue fullJsonResponse{NJson::EJsonValueType::JSON_MAP};
        NJson::TJsonValue jsonMessages{NJson::EJsonValueType::JSON_ARRAY};

        auto setData = [&](NJson::TJsonValue& jsonRecord, TString&& data) {
            jsonRecord.InsertValue("OriginalSize", data.size());
            if (data.size() > MaxMessageSize) {
                data.resize(MaxMessageSize);
            }
            totalSize += data.size();
            jsonRecord.InsertValue("Message", std::move(data));
        };
        fullJsonResponse.InsertValue("StartOffset", cmdRead.GetStartOffset());
        fullJsonResponse.InsertValue("EndOffset", cmdRead.GetEndOffset());


        for (auto& r : cmdRead.GetResult()) {
            if (totalSize >= MaxTotalSize) {
                break;
            }
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
                if (codec == nullptr) {
                    Send(Recipient, new TEvViewerTopicData::TEvTopicDataUnpacked(false, NJson::TJsonValue()));
                    Die(ActorContext());
                    return;
                }
                setData(jsonRecord, std::move(codec->Decompress(dataChunk.GetData())));
            } else {
                setData(jsonRecord, std::move(*dataChunk.MutableData()));
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
            jsonMessages.AppendValue(std::move(jsonRecord));
        }
        fullJsonResponse.InsertValue("Messages", std::move(jsonMessages));

        Send(Recipient, new TEvViewerTopicData::TEvTopicDataUnpacked(true, std::move(fullJsonResponse)));
        Die(ActorContext());
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
                return nullptr;
        }
    }

    TMap<ui32, THolder<NYdb::NTopic::ICodec>> Codecs;
    THolder<TEvPersQueue::TEvResponse> ReadResponse;
    TActorId Recipient;
    ui64 MaxMessageSize;
    ui64 MaxTotalSize;
};


void TTopicData::HandleDescribe(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto ev_ = std::unique_ptr<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(ev->Release().Release());
    const auto& result = ev_->Request;

    if (!TBase::IsSuccess(ev_)) {
        TStringBuilder error;
        if (result->ResultSet.size() != 0) {
            switch (result->ResultSet[0].Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    break; // Unexpected but just in case
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    error << "Got internal schema error  while trying to describe topic: '" << TopicPath << "'";
                    return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", error));

                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
                    error << "Topic not found: '" << TopicPath << "'";
                    return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", error));

                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                    error << "Access denied to topuc: '" << TopicPath << "'";
                    return ReplyAndPassAway(GetHTTPFORBIDDEN("text/plain", error));

                default:
                    return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Got unknown error type trying to describe topic"));

            }
        }
        error << "While trying to find topic: '" << TopicPath << "' got error '" << TBase::GetError(ev_) << "'";
        return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", error));
    }
    const auto response = result->ResultSet.front();
    if (response.Self->Info.GetPathType() != NKikimrSchemeOp::EPathTypePersQueueGroup) {
        auto error = TStringBuilder() << "No such topic '" << TopicPath << "";
        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", error));
    }
    if (Event->Get()->UserToken.empty()) {
        if (AppData(ActorContext())->EnforceUserTokenRequirement || AppData(ActorContext())->PQConfig.GetRequireCredentialsInNewProtocol()) {
            return ReplyAndPassAway(GetHTTPFORBIDDEN("text/plain", "Unauthenticated access is forbidden, please provide credentials"));
        }
    } else {
        NACLib::TUserToken token(Event->Get()->UserToken);
        if (!response.SecurityObject->CheckAccess(NACLib::EAccessRights::SelectRow, token)) {
            TStringBuilder error;
            error << "Access to topic " << TopicPath << " is denied for subject " << token.GetUserSID();
            return ReplyAndPassAway(GetHTTPFORBIDDEN("text/plain", error));
        }
    }
    const auto& partitions = response.PQGroupInfo->Description.GetPartitions();
    for (auto& partition : partitions) {
        auto partitionId = partition.GetPartitionId();
        if (partitionId == PartitionId) {
            TabletId = partition.GetTabletId();
            return SendPQReadRequest();
        }
    }
    ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "No such partition in topic"));
}

void TTopicData::SendPQReadRequest() {
    const auto& ctx = ActorContext();
    auto pipeClient = ConnectTabletPipe(TabletId);

    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(TopicPath);
    request.MutablePartitionRequest()->SetPartition(PartitionId);
    ActorIdToProto(pipeClient, request.MutablePartitionRequest()->MutablePipeClient());

    auto cmdRead = request.MutablePartitionRequest()->MutableCmdRead();
    cmdRead->SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
    cmdRead->SetCount(Limit);
    cmdRead->SetOffset(Offset);
    cmdRead->SetTimeoutMs(READ_TIMEOUT_MS);
    cmdRead->SetExternalOperation(true);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);
    NTabletPipe::SendData(ctx, pipeClient, req.Release());
}

void TTopicData::HandlePQResponse(TEvPersQueue::TEvResponse::TPtr& ev) {
    ReadResponse = ev->Release();
    const auto& record = ReadResponse->Record;
    if (record.GetStatus() ==  NMsgBusProxy::MSTATUS_ERROR) {
        switch (record.GetErrorCode()) {
            case ::NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET:
            case ::NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET:
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Bad offset"), record.GetErrorReason());
                break;
            default:
                return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Error trying to read messages"), record.GetErrorReason());
        }
        return;
    }
    const auto& response = record.GetPartitionResponse();

    if (response.HasCmdReadResult()) {
        const auto& readResult = response.GetCmdReadResult();
        if (readResult.GetReadingFinished()) {
            ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Bad partition-id"));
            return;
        }
    } else {
        return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No data received from topic"));
    }
    Register(new TUnpackDataToJsonActor(std::move(ReadResponse), SelfId()),
                TMailboxType::HTSwap, AppData()->BatchPoolId);
}

void TTopicData::HandleDataUnpacked(TEvViewerTopicData::TEvTopicDataUnpacked::TPtr& ev) {
    if (!ev->Get()->Status) {
        return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Messages decompression failed"));
    }
    Response = std::move(ev->Get()->Data);
    RequestDone();
}

void TTopicData::StateRequestedDescribe(TAutoPtr<::NActors::IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleDescribe);
        hFunc(TEvPersQueue::TEvResponse, HandlePQResponse);
        hFunc(TEvViewerTopicData::TEvTopicDataUnpacked, HandleDataUnpacked);
        cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
    }
}

bool TTopicData::GetIntegerParam(const TString& name, i64& value) {
    const auto& params(Event->Get()->Request.GetParams());
    if (params.Has(name)) {
        value = FromStringWithDefault<i32>(params.Get(name), -1);
        if (value == -1) {
            auto error = TStringBuilder() << "field ' "<< name << "' has invalid value, an interger >= 0 is expected";
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", error));
            return false;
        }
        return true;
    } else {
        auto error = TStringBuilder() << "field ' "<< name << "' is required";
        ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", error));
        return false;
    }
}

void TTopicData::Bootstrap() {
    if (NeedToRedirect()) {
        return;
    }
    const auto& params(Event->Get()->Request.GetParams());
    Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
    Timeout = std::min(Timeout, 30000u);


    if (!GetIntegerParam("partition", PartitionId))
        return;
    if (!GetIntegerParam("offset", Offset))
        return;

    Limit = FromStringWithDefault<ui32>(params.Get("limit"), 10);
    if (Limit > MAX_MESSAGES_LIMIT) {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Too many messages requested"));
    }

    if (params.Has("topic_path")) {
        TopicPath = params.Get("topic_path");
        RequestSchemeCacheNavigateWithParams(params.Get("topic_path"), NACLib::DescribeSchema, true);
    } else {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'topic_path' is required"));
    }
    Become(&TThis::StateRequestedDescribe, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());

}

void TTopicData::ReplyAndPassAway() {
    if (!Response.IsDefined()) {
        return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Could not get topic data"));
    }
    ReplyAndPassAway(GetHTTPOKJSON(std::move(Response)));
}


} // namespace NKikimr::NViewer

