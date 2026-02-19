#include "viewer_topic_data.h"
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb/services/lib/auth/auth_helpers.h>

namespace NKikimr::NViewer {

void TTopicData::HandleDescribe(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    if (ev->Cookie != 1) {
        return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Internal actor state got corrupted while trying to describe topic"));
    }
    NavigateResponse->Set(std::move(ev));
    if (NavigateResponse->IsError()) {
        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", NavigateResponse->GetError()));
    }
    Y_ABORT_UNLESS(NavigateResponse->Get());
    Y_ABORT_UNLESS(NavigateResponse->Get()->Request);
    const auto& request = *NavigateResponse->Get()->Request;
    if (!NavigateResponse->IsOk()) {
        TStringBuilder error;

        if (request.ResultSet.size() != 0) {
            switch (request.ResultSet[0].Status) {
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
                    return ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", error));

                default:
                    return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Got unknown error type trying to describe topic"));

            }
        }
        error << "While trying to find topic: '" << TopicPath << "' got error '" << NavigateResponse->GetError() << "'";
        return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", error));
    }

    const auto& response = request.ResultSet[0];
    {
        TString authError;
        auto pathWithName = TStringBuilder() << "topic " << TopicPath;
        auto authResult = NKikimr::NTopicHelpers::CheckAccess(*AppData(ActorContext()), response, Event->Get()->UserToken, pathWithName, authError);
        switch (authResult) {
            case NKikimr::NTopicHelpers::EAuthResult::AuthOk:
                break;
            case NKikimr::NTopicHelpers::EAuthResult::AccessDenied:
            case NKikimr::NTopicHelpers::EAuthResult::TokenRequired:
                return ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", authError));
        }
    }
    if (response.Kind == NSchemeCache::TSchemeCacheNavigate::KindCdcStream) {
        if (ProcessCdc(response)) {
            RequestDone();
            return;
        }
    }

    if (response.Self->Info.GetPathType() != NKikimrSchemeOp::EPathTypePersQueueGroup) {
        auto error = TStringBuilder() << "No such topic '" << TopicPath << "";
        return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", error));
    }

    const auto& partitions = response.PQGroupInfo->Description.GetPartitions();
    for (auto& partition : partitions) {
        auto partitionId = partition.GetPartitionId();
        if (partitionId == PartitionId) {
            TabletId = partition.GetTabletId();
            SendPQReadRequest();
            RequestDone();
            return;
        }
    }
    ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "No such partition in topic"));
}

bool TTopicData::ProcessCdc(const NSchemeCache::TSchemeCacheNavigate::TEntry& response) {
    if (!response.ListNodeEntry) {
        ReplyAndPassAway(
                GetHTTPINTERNALERROR("text/plain", "Error trying to describe CDC stream"));
        return true;
    }
    if (response.ListNodeEntry->Children.size() != 1)
        return false;

    auto privateTopicName = response.ListNodeEntry->Children.at(0).Name;
    TopicPath = JoinPath(ChildPath(NKikimr::SplitPath(TopicPath), privateTopicName));
    NavigateResponse = MakeRequestSchemeCacheNavigateWithToken(TopicPath, NACLib::DescribeSchema, 1);
    return true;
}

void TTopicData::SendPQReadRequest() {
    auto pipeClient = ConnectTabletPipe(TabletId);

    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(TopicPath);
    request.MutablePartitionRequest()->SetPartition(PartitionId);
    ActorIdToProto(pipeClient, request.MutablePartitionRequest()->MutablePipeClient());

    auto cmdRead = request.MutablePartitionRequest()->MutableCmdRead();
    cmdRead->SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
    cmdRead->SetCount(TruncateLongMessages ? Limit : 1);
    cmdRead->SetOffset(Offset);
    if (LastOffset) {
        cmdRead->SetLastOffset(LastOffset);
    }
    cmdRead->SetReadTimestampMs(Timestamp);

    cmdRead->SetTimeoutMs(READ_TIMEOUT_MS);
    cmdRead->SetExternalOperation(true);

    auto req = MakeHolder<TEvPersQueue::TEvRequest>();
    req->Record.Swap(&request);
    SendRequestToPipe(pipeClient, req.Release());
}

void TTopicData::HandlePQResponse(TEvPersQueue::TEvResponse::TPtr& ev) {

    ReadResponse = ev->Release();
    const auto& record = ReadResponse->Record;
    if (record.GetStatus() == NMsgBusProxy::MSTATUS_ERROR) {
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
    FillProtoResponse();
    RequestDone();
}

void TTopicData::FillProtoResponse(ui64 maxTotalSize) {
    ui64 totalSize = 0;
    const auto& response = ReadResponse->Record.GetPartitionResponse();
    if(!response.HasCmdReadResult()) {
        return;
    }
    const auto& cmdRead = response.GetCmdReadResult();
    bool isTruncated = false;
    auto setData = [&](NKikimrViewer::TTopicDataResponse::TMessage& protoMessage, TString&& data) {
        protoMessage.SetOriginalSize(data.size());
        if (data.size() > MaxSingleMessageSize && TruncateLongMessages) {
            isTruncated = true;
            data.resize(MaxSingleMessageSize);
        }
        totalSize += data.size();
        protoMessage.SetMessage(std::move(Base64Encode(data)));
    };
    ProtoResponse.SetStartOffset(cmdRead.GetStartOffset());
    ProtoResponse.SetEndOffset(cmdRead.GetEndOffset());

    for (auto& r : cmdRead.GetResult()) {
        if (totalSize >= maxTotalSize) {
            isTruncated = true;
            break;
        }
        auto dataChunk = (NKikimr::GetDeserializedData(r.GetData()));
        auto* messageProto = ProtoResponse.AddMessages();
        messageProto->SetOffset(r.GetOffset());

        messageProto->SetCreateTimestamp(r.GetCreateTimestampMS());
        messageProto->SetWriteTimestamp(r.GetWriteTimestampMS());
        i64 diff = r.GetWriteTimestampMS() - r.GetCreateTimestampMS();
        if (diff < 0) {
            diff = 0;
        }
        messageProto->SetTimestampDiff(diff);
        messageProto->SetStorageSize(dataChunk.GetData().size());

        if (dataChunk.HasCodec() && dataChunk.GetCodec() != NPersQueueCommon::RAW) {
            const NYdb::NTopic::ICodec* codec = GetCodec(static_cast<NPersQueueCommon::ECodec>(dataChunk.GetCodec()));
            if (codec == nullptr) {
                return ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Message decompression failed"));
            }
            try {
                setData(*messageProto, std::move(codec->Decompress(dataChunk.GetData())));
            } catch (const std::exception& e) {
                setData(*messageProto, ">>> Message decompression failed <<<");
            }
        } else {
            setData(*messageProto, std::move(*dataChunk.MutableData()));
        }
        messageProto->SetCodec(dataChunk.GetCodec());
        TString decodedSrcId;
        if (!r.GetSourceId().empty()) {
            decodedSrcId = NPQ::NSourceIdEncoding::Decode(r.GetSourceId());
        }
        messageProto->SetProducerId(decodedSrcId);
        messageProto->SetSeqNo(r.GetSeqNo());

        if (dataChunk.MessageMetaSize() > 0) {
            for (const auto& metadata : dataChunk.GetMessageMeta()) {
                auto* metadataProto = messageProto->AddMessageMetadata();
                auto jsonMetadataItem = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
                metadataProto->SetKey(metadata.key());
                metadataProto->SetValue(metadata.value());
            }
        }
    }
    ProtoResponse.SetTruncated(isTruncated);
}

void TTopicData::ReplyAndPassAway() {
    NProtobufJson::TProto2JsonConfig config;
    //config.SetAddMissingFields(true);
    config.SetMissingSingleKeyMode(NProtobufJson::TProto2JsonConfig::MissingKeyDefault);
    TStringStream json;
    NProtobufJson::Proto2Json(ProtoResponse, json, config);
    ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
}

NYdb::NTopic::ICodec* TTopicData::GetCodec(NPersQueueCommon::ECodec codec) {
    ui32 codecId = static_cast<ui32>(codec);
    auto iter = Codecs.find(codecId);
    if (iter != Codecs.end()) {
        return iter->second.Get();
    }
    switch (codec) {
        case NPersQueueCommon::GZIP: {
            auto [iterator, ins] = Codecs.emplace(codecId, MakeHolder<NYdb::NTopic::TGzipCodec>());
            return iterator->second.Get();
            break;
        }
        case NPersQueueCommon::ZSTD: {
            auto [iterator, ins] = Codecs.emplace(codecId, MakeHolder<NYdb::NTopic::TZstdCodec>());
            return iterator->second.Get();
        }
        default:
            return nullptr;
    }
}

void TTopicData::StateRequestedDescribe(TAutoPtr<::NActors::IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, HandleDescribe);
        hFunc(TEvPersQueue::TEvResponse, HandlePQResponse);
        cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
    }
}

void TTopicData::Bootstrap() {
    if (!Database.empty() && TBase::NeedToRedirect()) {
        return;
    }
    const auto& params(Event->Get()->Request.GetParams());
    Timeout = TDuration::Seconds(std::min((ui32)Timeout.Seconds(), 30u));

    TruncateLongMessages = FromStringWithDefault<bool>(params.Get("truncate"), true);
    MaxSingleMessageSize = FromStringWithDefault<ui64>(params.Get("message_size_limit"), MaxSingleMessageSize);

    if (!params.Has("partition")) {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Parameter 'partition' is necessary"));
    }
    PartitionId = FromStringWithDefault(params.Get("partition"), PartitionId);

    Offset = FromStringWithDefault(params.Get("offset"), Offset);
    LastOffset = FromStringWithDefault(params.Get("last_offset"), LastOffset);
    Timestamp = FromStringWithDefault(params.Get("read_timestamp"), Timestamp);

    Limit = FromStringWithDefault(params.Get("limit"), Limit);

    // Only allow timestamp XOR offset to be defined
    if (Offset > 0 && Timestamp > 0) {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only read_timestamp or offset parameter may be specified, not both"));
    }

    // No truncate is available with on offset specified and limit = 1 or undefined
    if (!TruncateLongMessages && Limit > 1) {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "truncate=false can only be specified with limit = 1"));
    }
    if (!TruncateLongMessages && Timestamp > 0) {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "truncate=false can only be specified with an offset, not a timestamp"));
    }
    if (Limit > MAX_MESSAGES_LIMIT) {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Too many messages requested"));
    }

    TopicPath = params.Get("path");
    if (!TopicPath.empty()) {
        NavigateResponse = MakeRequestSchemeCacheNavigateWithToken(TopicPath, NACLib::DescribeSchema, 1);
    } else {
        return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'path' is required and should not be empty"));
    }
    Become(&TThis::StateRequestedDescribe, Timeout, new TEvents::TEvWakeup());
}


} // namespace NKikimr::NViewer

