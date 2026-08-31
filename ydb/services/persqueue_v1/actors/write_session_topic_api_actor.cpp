#include "write_session_topic_api_actor.h"
#include "write_session_actor.h"
#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/core/persqueue/deferred_publish/constants.h>
#include <ydb/core/persqueue/public/codecs/pqv1.h>
#include <ydb/core/persqueue/public/dataplane/dataplane.h>
#include <ydb/core/persqueue/public/dataplane/write/write_session_events.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/persqueue/constants.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb/public/sdk/cpp/src/library/kafka/kafka_records.h>

#include <google/protobuf/util/time_util.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <memory>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_WRITE_PROXY

using namespace NActors;

namespace NKikimr::NGRpcProxy::V1 {

namespace NWrite = NKikimr::NPQ::NDataplane::NWrite;

NWrite::TWriteSessionProtocolOpts TopicWriteSessionProtocol() {
    return {
        .Name = "topic",
        .CounterName = "topic",
        .SessionSpanName = "Topic.WriteSession",
        .AttachRequestContextToPartitionWriter = true,
        .SetDisableDeduplicationWhenUnused = true,
    };
}

namespace {

i32 CodecByName(const TString& codec) {
    const THashMap<TString, i32> codecsByName = {
        { "raw",  (i32)Ydb::Topic::CODEC_RAW  },
        { "gzip", (i32)Ydb::Topic::CODEC_GZIP },
        { "lzop", (i32)Ydb::Topic::CODEC_LZOP },
        { "zstd", (i32)Ydb::Topic::CODEC_ZSTD },
    };
    auto codecIt = codecsByName.find(codec);
    if (codecIt == codecsByName.end()) {
        return (i32)Ydb::Topic::CODEC_UNSPECIFIED;
    }
    return codecIt->second;
}

TString WriteRequestToLog(const Topic::StreamWriteMessage::FromClient& proto) {
    switch (proto.client_message_case()) {
        case Topic::StreamWriteMessage::FromClient::kInitRequest:
            return proto.ShortDebugString();
        case Topic::StreamWriteMessage::FromClient::kWriteRequest:
            return " write_request[data omitted]";
        case Topic::StreamWriteMessage::FromClient::kUpdateTokenRequest:
            return " update_token_request [content omitted]";
        default:
            return TString();
    }
}

void FillBatchFieldsFromTopicWriteMessage(
        const Ydb::Topic::StreamWriteMessage::WriteRequest& writeRequest,
        const Ydb::Topic::StreamWriteMessage::WriteRequest::MessageData& msg,
        NWrite::TWriteSessionMessage& out)
{
    if (writeRequest.codec() != static_cast<i32>(Ydb::Topic::CODEC_KAFKA_BATCH)) {
        return;
    }

    const auto header = NKafka::ReadKafkaBatchHeader(msg.data());
    if (!header || header->RecordsCount == 0) {
        return;
    }

    out.CmdSeqNo = header->BaseSequence;
    out.ExpectedAckSeqNo = header->BaseSequence;
    out.LogicalMessageCount = static_cast<ui32>(header->RecordsCount);
    out.MaxSeqNo = msg.seq_no();
}

} // namespace

class TWriteSessionTopicApiActor
    : public NPQ::TBaseActor<TWriteSessionTopicApiActor>
    , public NPQ::TConstantLogPrefix
{
    using TBase = NPQ::TBaseActor<TWriteSessionTopicApiActor>;
    using TClientMessage = Topic::StreamWriteMessage::FromClient;
    using TServerMessage = Topic::StreamWriteMessage::FromServer;
    using TEvStreamWriteRequest = NGRpcService::TEvStreamTopicWriteRequest;
    using IContext = NGRpcServer::IGRpcStreamingContext<TClientMessage, TServerMessage>;

public:
    TWriteSessionTopicApiActor(
        TEvStreamWriteRequest* request,
        ui64 cookie,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const std::optional<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsController);

    void Bootstrap();
    void PassAway() override;
    TString BuildLogPrefix() const override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_PQ_WRITE;
    }

private:
    STFUNC(StateFunc);

    void Handle(IContext::TEvReadFinished::TPtr& ev);
    void Handle(IContext::TEvWriteFinished::TPtr& ev);
    void Handle(IContext::TEvNotifiedWhenDone::TPtr& ev);
    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev);
    void HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev);

    void Handle(NWrite::TEvInitAck::TPtr& ev);
    void Handle(NWrite::TEvWriteAck::TPtr& ev);
    void Handle(NWrite::TEvUpdateTokenAck::TPtr& ev);
    void Handle(NWrite::TEvRefreshToken::TPtr& ev);
    void Handle(NWrite::TEvUnauthenticated::TPtr& ev);
    void Handle(NWrite::TEvClosed::TPtr& ev);
    void Handle(NWrite::TEvReadNext::TPtr& ev);
    void Handle(NWrite::TEvConsumedRequestUnits::TPtr& ev);

    void HandleInit(TClientMessage&& req);
    void HandleWrite(TClientMessage&& req);
    void HandleUpdateToken(TClientMessage&& req);

    void CloseByProtocolError(const TString& errorReason);
    bool WriteServerMessage(TServerMessage&& message);

    std::unique_ptr<TEvStreamWriteRequest> Request;
    ui64 Cookie;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    std::optional<TString> ClientDC;
    NPersQueue::TTopicsListController TopicsController;
    TActorId Logic;
    bool Dying = false;
};

TWriteSessionTopicApiActor::TWriteSessionTopicApiActor(
        TEvStreamWriteRequest* request, ui64 cookie,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, const std::optional<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsController)
    : TBase(NKikimrServices::PQ_WRITE_PROXY)
    , Request(request)
    , Cookie(cookie)
    , Counters(std::move(counters))
    , ClientDC(clientDC)
    , TopicsController(topicsController)
{
    Y_ASSERT(Request);
}

STFUNC(TWriteSessionTopicApiActor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        hFunc(IContext::TEvReadFinished, Handle);
        hFunc(IContext::TEvWriteFinished, Handle);
        hFunc(IContext::TEvNotifiedWhenDone, Handle);
        hFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);
        hFunc(TEvPQProxy::TEvDieCommand, HandlePoison);

        hFunc(NWrite::TEvInitAck, Handle);
        hFunc(NWrite::TEvWriteAck, Handle);
        hFunc(NWrite::TEvUpdateTokenAck, Handle);
        hFunc(NWrite::TEvRefreshToken, Handle);
        hFunc(NWrite::TEvUnauthenticated, Handle);
        hFunc(NWrite::TEvClosed, Handle);
        hFunc(NWrite::TEvReadNext, Handle);
        hFunc(NWrite::TEvConsumedRequestUnits, Handle);
    default:
        break;
    }
}

void TWriteSessionTopicApiActor::Bootstrap() {
    AFL_ENSURE(Request);
    Request->Attach(SelfId());

    NWrite::TWriteSessionSettings settings{
        .Owner = SelfId(),
        .Cookie = Cookie,
        .Counters = Counters,
        .ClientDC = ClientDC,
        .TopicsController = TopicsController,
        .Protocol = TopicWriteSessionProtocol(),
        .DatabaseName = NWrite::ToOptional(Request->GetDatabaseName()),
        .SerializedToken = Request->GetSerializedToken(),
        .YdbToken = NWrite::ToOptional(Request->GetYdbToken()),
        .TraceId = NWrite::ToOptional(Request->GetTraceId()),
        .RequestType = NWrite::ToOptional(Request->GetRequestType()),
        .WilsonTraceId = Request->GetWilsonTraceId(),
        .RlContext = NPQ::TRlContext(Request.get()),
    };
    if (auto value = Request->GetPeerMetaValues(NYdb::YDB_APPLICATION_NAME)) {
        settings.UserAgent = *value;
    } else {
        settings.UserAgent = "topic server";
    }
    if (auto value = Request->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER)) {
        settings.SdkBuildInfo = *value;
    }

    Logic = RegisterWithSameMailbox(NWrite::CreateWriteSessionLogicActor(std::move(settings)));

    if (!Request->Read()) {
        YDB_LOG_INFO("Grpc read failed at start");
        PassAway();
        return;
    }
    Become(&TWriteSessionTopicApiActor::StateFunc);
}

TString TWriteSessionTopicApiActor::BuildLogPrefix() const {
    return TStringBuilder() << " (Cookie=" << Cookie << ") ";
}

void TWriteSessionTopicApiActor::PassAway() {
    if (Dying) {
        return;
    }
    Dying = true;

    if (Logic) {
        Send(Logic, new TEvents::TEvPoison());
        Logic = {};
    }

    Send(GetPQWriteServiceActorID(), new TEvPQProxy::TEvSessionDead(Cookie));

    if (Request) {
        Request->AuditLogRequestEnd(Ydb::StatusIds::SUCCESS);
    }

    TBase::PassAway();
}

bool TWriteSessionTopicApiActor::WriteServerMessage(TServerMessage&& message) {
    if (!Request->Write(std::move(message))) {
        YDB_LOG_INFO("Session v1 grpc write failed",
            {"cookie", Cookie});
        PassAway();
        return false;
    }
    return true;
}

void TWriteSessionTopicApiActor::CloseByProtocolError(const TString& errorReason) {
    Send(Logic, new NWrite::TEvDieCommand(errorReason, PersQueue::ErrorCode::BAD_REQUEST));
}

void TWriteSessionTopicApiActor::Handle(IContext::TEvNotifiedWhenDone::TPtr&) {
    YDB_LOG_INFO("Session v1 grpc closed",
        {"cookie", Cookie});
    PassAway();
}

void TWriteSessionTopicApiActor::Handle(IContext::TEvReadFinished::TPtr& ev) {
    YDB_LOG_DEBUG("Session v1 grpc read done",
        {"cookie", Cookie},
        {"success", ev->Get()->Success},
        {"data", WriteRequestToLog(ev->Get()->Record)});
    if (!ev->Get()->Success) {
        YDB_LOG_INFO("Session v1 grpc read failed",
            {"cookie", Cookie});
        Send(Logic, new NWrite::TEvClientDone());
        return;
    }

    auto& req = ev->Get()->Record;
    switch (req.client_message_case()) {
        case TClientMessage::kInitRequest:
            HandleInit(std::move(req));
            break;
        case TClientMessage::kWriteRequest:
            HandleWrite(std::move(req));
            break;
        case TClientMessage::kUpdateTokenRequest:
            HandleUpdateToken(std::move(req));
            break;
        case TClientMessage::CLIENT_MESSAGE_NOT_SET:
            CloseByProtocolError("'client_message' is not set");
            return;
    }
}

void TWriteSessionTopicApiActor::Handle(IContext::TEvWriteFinished::TPtr& ev) {
    if (!ev->Get()->Success) {
        YDB_LOG_INFO("Session v1 grpc write failed",
            {"cookie", Cookie});
        PassAway();
    }
}

void TWriteSessionTopicApiActor::HandleInit(TClientMessage&& req) {
    const auto& initRequest = req.init_request();
    auto init = MakeHolder<NWrite::TEvInit>();
    init->PeerName = Request->GetPeerName();
    init->PreferedPartition = Max<ui32>();

    init->TopicPath = initRequest.path();
    if (init->TopicPath.empty()) {
        CloseByProtocolError("no topic in init request");
        return;
    }
    bool isScenarioSupported =
        !initRequest.producer_id().empty() && (
            initRequest.has_message_group_id() && initRequest.message_group_id() == initRequest.producer_id() ||
            initRequest.message_group_id().empty() ||
            initRequest.has_partition_id() ||
            initRequest.has_partition_with_generation())
        ||
        initRequest.producer_id().empty();

    if (!isScenarioSupported) {
        CloseByProtocolError("unsupported producer_id / message_group_id / partition_id settings in init request");
        return;
    }
    if (initRequest.producer_id().empty() && initRequest.message_group_id().empty()) {
        init->UseDeduplication = false;
    }
    init->SourceId = !initRequest.message_group_id().empty() ? initRequest.message_group_id() : initRequest.producer_id();
    if (initRequest.has_partition_id()) {
        init->PreferedPartition = initRequest.partition_id();
        YDB_LOG_INFO("Session",
            {"partition", init->PreferedPartition});
    } else if (initRequest.has_partition_with_generation()) {
        init->PreferedPartition = initRequest.partition_with_generation().partition_id();
        init->ExpectedGeneration = initRequest.partition_with_generation().generation();
        YDB_LOG_INFO("Session",
            {"partition", init->PreferedPartition},
            {"generation", init->ExpectedGeneration});
    }
    for (const auto& item : initRequest.write_session_meta()) {
        if (item.first == NPersQueue::WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX) {
            bool trackProducerId = false;
            if (!TryFromString<bool>(item.second, trackProducerId)) {
                CloseByProtocolError(TStringBuilder()
                    << "invalid value for write_session_meta key '"
                    << NPersQueue::WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX
                    << "': expected boolean, got '" << item.second << "'");
                return;
            }
            init->TrackProducerId = trackProducerId;
        }
        init->SessionMeta[item.first] = item.second;
    }

    Send(Logic, init.Release());
}

void TWriteSessionTopicApiActor::HandleWrite(TClientMessage&& req) {
    const auto& writeRequest = req.write_request();
    auto ev = MakeHolder<NWrite::TEvWrite>();
    ev->UserRequestByteSize = req.ByteSize();

    if (writeRequest.has_deferred_publish()) {
        if (!AppData()->FeatureFlags.GetEnableTopicDeferredPublish()) {
            Send(Logic, new NWrite::TEvDieCommand(
                TString(NPQ::NDeferredPublish::DisabledMessage),
                PersQueue::ErrorCode::BAD_REQUEST,
                Ydb::StatusIds::UNSUPPORTED));
            return;
        }
        if (writeRequest.has_tx()) {
            CloseByProtocolError("WriteRequest must not contain both tx and deferred_publish");
            return;
        }
        const auto& deferredPublish = writeRequest.deferred_publish();
        if (deferredPublish.int_publication_id() == 0) {
            CloseByProtocolError("WriteRequest.deferred_publish.int_publication_id must be greater than 0");
            return;
        }
        if (deferredPublish.has_ext_publication_id()
            && deferredPublish.ext_publication_id().size() > NPQ::NDeferredPublish::MaxDeferredPublishStringLength) {
            CloseByProtocolError("WriteRequest.deferred_publish.ext_publication_id is too long");
            return;
        }
        NPQ::TDeferredPublishWriterOpts opts;
        opts.IntPublicationId = deferredPublish.int_publication_id();
        if (deferredPublish.has_ext_publication_id()) {
            opts.ExtPublicationId = deferredPublish.ext_publication_id();
        }
        ev->DeferredPublish = opts;
    }
    if (writeRequest.has_tx()) {
        ev->Tx = std::make_pair(writeRequest.tx().session(), writeRequest.tx().id());
    }

    if (writeRequest.messages_size() == 0) {
        CloseByProtocolError("messages meta repeated fields are empty, write request contains no messages");
        return;
    }
    if (writeRequest.codec() == static_cast<i32>(Ydb::Topic::CODEC_UNSPECIFIED)) {
        CloseByProtocolError("bad write request - codec is invalid: unspecified (id 0)");
        return;
    }
    for (i32 messageIndex = 0; messageIndex != writeRequest.messages_size(); ++messageIndex) {
        const auto& msg = writeRequest.messages(messageIndex);
        NWrite::TWriteSessionMessage out;
        out.SeqNo = msg.seq_no();
        out.CmdSeqNo = msg.seq_no();
        out.ExpectedAckSeqNo = msg.seq_no();
        out.CreateTimeMs = ::google::protobuf::util::TimeUtil::TimestampToMilliseconds(msg.created_at());
        out.UncompressedSize = msg.uncompressed_size();
        out.CodecId = writeRequest.codec();
        out.SkipCodecValidation = writeRequest.codec() == static_cast<i32>(Ydb::Topic::CODEC_KAFKA_BATCH);
        if (writeRequest.codec() > 0 && !out.SkipCodecValidation) {
            out.ChunkCodec = static_cast<ui32>(NPQ::FromTopicCodec(static_cast<NYdb::NTopic::ECodec>(writeRequest.codec())));
        }
        out.Data = msg.data();
        for (const auto& metaItem : msg.metadata_items()) {
            out.Metadata.emplace_back(metaItem.key(), metaItem.value());
        }
        FillBatchFieldsFromTopicWriteMessage(writeRequest, msg, out);
        ev->Messages.push_back(std::move(out));
    }

    Send(Logic, ev.Release());
}

void TWriteSessionTopicApiActor::HandleUpdateToken(TClientMessage&& req) {
    auto ev = MakeHolder<NWrite::TEvUpdateToken>();
    ev->Token = req.update_token_request().token();
    Send(Logic, ev.Release());
}

void TWriteSessionTopicApiActor::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev) {
    Send(Logic, new NWrite::TEvDieCommand(ev->Get()->Reason, ev->Get()->ErrorCode));
}

void TWriteSessionTopicApiActor::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev) {
    if (ev->Get()->Authenticated && ev->Get()->InternalToken && !ev->Get()->InternalToken->GetSerializedToken().empty()) {
        Request->SetInternalToken(ev->Get()->InternalToken);
        auto refreshed = MakeHolder<NWrite::TEvTokenRefreshed>();
        refreshed->InternalToken = ev->Get()->InternalToken;
        Send(Logic, refreshed.Release());
    } else {
        if (ev->Get()->Retryable) {
            TServerMessage serverMessage;
            serverMessage.set_status(Ydb::StatusIds::UNAVAILABLE);
            Request->WriteAndFinish(std::move(serverMessage), Ydb::StatusIds::UNAVAILABLE);
        } else {
            Request->RaiseIssues(ev->Get()->Issues);
            Request->ReplyUnauthenticated("refreshed token is invalid");
        }
        PassAway();
    }
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvInitAck::TPtr& ev) {
    const auto& ack = *ev->Get();
    TServerMessage response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    auto* init = response.mutable_init_response();

    if (!ack.SessionId.empty()) {
        init->set_session_id(EscapeC(ack.SessionId));
    }
    if (ack.LastSeqNo.has_value()) {
        init->set_last_seq_no(*ack.LastSeqNo);
    }
    init->set_partition_id(ack.PartitionId);
    for (const auto& codecName : ack.SupportedCodecNames) {
        init->mutable_supported_codecs()->add_codecs(CodecByName(codecName));
    }
    init->set_is_batching_supported(ack.BatchingSupported);

    WriteServerMessage(std::move(response));
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvWriteAck::TPtr& ev) {
    using ::google::protobuf::Duration;
    using ::google::protobuf::util::TimeUtil;

    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    auto* writeResponse = result.mutable_write_response();
    writeResponse->set_partition_id(ev->Get()->PartitionId);
    auto* stat = writeResponse->mutable_write_statistics();
    for (const auto& res : ev->Get()->Acks) {
        auto* ack = writeResponse->add_acks();
        ack->set_seq_no(res.SeqNo);
        if (res.AlreadyWritten) {
            ack->mutable_skipped()->set_reason(Topic::StreamWriteMessage::WriteResponse::WriteAck::Skipped::REASON_ALREADY_WRITTEN);
        } else if (res.WrittenInTx) {
            ack->mutable_written_in_tx();
        } else {
            ack->mutable_written()->set_offset(res.Offset);
        }

        auto persisting_time_ms = Max<i64>(res.WriteTimeMs, TimeUtil::DurationToMilliseconds(stat->persisting_time()));
        *stat->mutable_persisting_time() = TimeUtil::MillisecondsToDuration(persisting_time_ms);

        auto min_queue_wait_time_ms = (stat->min_queue_wait_time() == Duration())
                                      ? (i64)res.TotalTimeInPartitionQueueMs
                                      : Min<i64>(res.TotalTimeInPartitionQueueMs, TimeUtil::DurationToMilliseconds(stat->min_queue_wait_time()));
        *stat->mutable_min_queue_wait_time() = TimeUtil::MillisecondsToDuration(min_queue_wait_time_ms);

        auto max_queue_wait_time_ms = Max<i64>(res.TotalTimeInPartitionQueueMs, TimeUtil::DurationToMilliseconds(stat->max_queue_wait_time()));
        *stat->mutable_max_queue_wait_time() = TimeUtil::MillisecondsToDuration(max_queue_wait_time_ms);

        auto partition_quota_wait_time_ms = Max<i64>(res.PartitionQuotedTimeMs, TimeUtil::DurationToMilliseconds(stat->partition_quota_wait_time()));
        *stat->mutable_partition_quota_wait_time() = TimeUtil::MillisecondsToDuration(partition_quota_wait_time_ms);

        auto topic_quota_wait_time_ms = Max<i64>(res.TopicQuotedTimeMs, TimeUtil::DurationToMilliseconds(stat->topic_quota_wait_time()));
        *stat->mutable_topic_quota_wait_time() = TimeUtil::MillisecondsToDuration(topic_quota_wait_time_ms);
    }

    WriteServerMessage(std::move(result));
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvUpdateTokenAck::TPtr&) {
    TServerMessage serverMessage;
    serverMessage.set_status(Ydb::StatusIds::SUCCESS);
    serverMessage.mutable_update_token_response();
    WriteServerMessage(std::move(serverMessage));
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvRefreshToken::TPtr& ev) {
    Request->RefreshToken(ev->Get()->Token, ActorContext(), SelfId(), std::move(ev->Get()->TraceId));
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvUnauthenticated::TPtr& ev) {
    Request->ReplyUnauthenticated(ev->Get()->Reason);
    PassAway();
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvClosed::TPtr& ev) {
    Logic = {};
    const auto errorCode = ev->Get()->ErrorCode;
    const Ydb::StatusIds::StatusCode statusCode = ev->Get()->StatusOverride.value_or(
        ConvertPersQueueInternalCodeToStatus(errorCode));

    if (errorCode != PersQueue::ErrorCode::OK) {
        TServerMessage result;
        result.set_status(statusCode);
        FillIssue(result.add_issues(), errorCode, ev->Get()->ErrorReason);
        if (!Request->WriteAndFinish(std::move(result), statusCode)) {
            YDB_LOG_INFO("Session v1 grpc last write failed",
                {"cookie", Cookie});
        }
    } else {
        if (!Request->Finish(statusCode)) {
            YDB_LOG_INFO("Session v1 double finish call",
                {"cookie", Cookie});
        }
    }
    PassAway();
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvReadNext::TPtr&) {
    if (!Request->Read()) {
        YDB_LOG_INFO("Session v1 grpc read failed",
            {"cookie", Cookie});
        PassAway();
    }
}

void TWriteSessionTopicApiActor::Handle(NWrite::TEvConsumedRequestUnits::TPtr& ev) {
    if (auto counters = Request->GetCounters()) {
        counters->AddConsumedRequestUnits(ev->Get()->Amount);
    }
}

NActors::IActor* CreateWriteSessionTopicApiActor(
    NGRpcService::TEvStreamTopicWriteRequest* request,
    ui64 cookie,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const std::optional<TString> clientDC,
    const NPersQueue::TTopicsListController& topicsController)
{
    return new TWriteSessionTopicApiActor(
        request, cookie, std::move(counters), clientDC, topicsController);
}

} // namespace NKikimr::NGRpcProxy::V1
