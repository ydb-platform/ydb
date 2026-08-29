#include "write_session_pqv1_actor.h"
#include "write_session_actor.h"
#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/core/persqueue/public/dataplane/dataplane.h>
#include <ydb/core/persqueue/public/dataplane/write/write_session_events.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/escape.h>

#include <memory>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::PQ_WRITE_PROXY

using namespace NActors;

namespace NKikimr::NGRpcProxy::V1 {

namespace NWrite = NKikimr::NPQ::NDataplane::NWrite;

NWrite::TWriteSessionProtocolOpts PQv1WriteSessionProtocol() {
    return {
        .Name = "v1",
        .CounterName = "pqv1",
        .SessionSpanName = "Topic.WriteSession[migration]",
        .ChildSpanNameSuffix = "[migration]",
        .CodecCounterIndexOffset = 1,
    };
}

namespace {

Ydb::PersQueue::V1::Codec CodecByName(const TString& codec) {
    const THashMap<TString, Ydb::PersQueue::V1::Codec> codecsByName = {
        { "raw",  Ydb::PersQueue::V1::CODEC_RAW  },
        { "gzip", Ydb::PersQueue::V1::CODEC_GZIP },
        { "lzop", Ydb::PersQueue::V1::CODEC_LZOP },
        { "zstd", Ydb::PersQueue::V1::CODEC_ZSTD },
    };
    auto codecIt = codecsByName.find(codec);
    if (codecIt == codecsByName.end()) {
        return Ydb::PersQueue::V1::CODEC_UNSPECIFIED;
    }
    return codecIt->second;
}

TString WriteRequestToLog(const PersQueue::V1::StreamingWriteClientMessage& proto) {
    switch (proto.client_message_case()) {
        case PersQueue::V1::StreamingWriteClientMessage::kInitRequest:
            return proto.ShortDebugString();
        case PersQueue::V1::StreamingWriteClientMessage::kWriteRequest:
            return " write_request[data omitted]";
        case PersQueue::V1::StreamingWriteClientMessage::kUpdateTokenRequest:
            return " update_token_request [content omitted]";
        default:
            return TString();
    }
}

} // namespace

class TWriteSessionPQv1Actor
    : public NPQ::TBaseActor<TWriteSessionPQv1Actor>
    , public NPQ::TConstantLogPrefix
{
    using TBase = NPQ::TBaseActor<TWriteSessionPQv1Actor>;
    using TClientMessage = PersQueue::V1::StreamingWriteClientMessage;
    using TServerMessage = PersQueue::V1::StreamingWriteServerMessage;
    using TEvStreamWriteRequest = NGRpcService::TEvStreamPQWriteRequest;
    using IContext = NGRpcServer::IGRpcStreamingContext<TClientMessage, TServerMessage>;

    static constexpr ui32 CODEC_ID_SIZE = 1;

public:
    TWriteSessionPQv1Actor(
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

TWriteSessionPQv1Actor::TWriteSessionPQv1Actor(
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

STFUNC(TWriteSessionPQv1Actor::StateFunc) {
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

void TWriteSessionPQv1Actor::Bootstrap() {
    AFL_ENSURE(Request);
    Request->Attach(SelfId());

    NWrite::TWriteSessionSettings settings{
        .Owner = SelfId(),
        .Cookie = Cookie,
        .Counters = Counters,
        .ClientDC = ClientDC,
        .TopicsController = TopicsController,
        .Protocol = PQv1WriteSessionProtocol(),
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
        settings.UserAgent = "pqv1 server";
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
    Become(&TWriteSessionPQv1Actor::StateFunc);
}

TString TWriteSessionPQv1Actor::BuildLogPrefix() const {
    return TStringBuilder() << " (Cookie=" << Cookie << ") ";
}

void TWriteSessionPQv1Actor::PassAway() {
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

bool TWriteSessionPQv1Actor::WriteServerMessage(TServerMessage&& message) {
    if (!Request->Write(std::move(message))) {
        YDB_LOG_INFO("Session v1 grpc write failed",
            {"cookie", Cookie});
        PassAway();
        return false;
    }
    return true;
}

void TWriteSessionPQv1Actor::CloseByProtocolError(const TString& errorReason) {
    Send(Logic, new NWrite::TEvDieCommand(errorReason, PersQueue::ErrorCode::BAD_REQUEST));
}

void TWriteSessionPQv1Actor::Handle(IContext::TEvNotifiedWhenDone::TPtr&) {
    YDB_LOG_INFO("Session v1 grpc closed",
        {"cookie", Cookie});
    PassAway();
}

void TWriteSessionPQv1Actor::Handle(IContext::TEvReadFinished::TPtr& ev) {
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

void TWriteSessionPQv1Actor::Handle(IContext::TEvWriteFinished::TPtr& ev) {
    if (!ev->Get()->Success) {
        YDB_LOG_INFO("Session v1 grpc write failed",
            {"cookie", Cookie});
        PassAway();
    }
}

void TWriteSessionPQv1Actor::HandleInit(TClientMessage&& req) {
    const auto& initRequest = req.init_request();
    auto init = MakeHolder<NWrite::TEvInit>();
    init->PeerName = Request->GetPeerName();
    init->PreferedPartition = Max<ui32>();

    init->TopicPath = initRequest.topic();
    if (init->TopicPath.empty()) {
        CloseByProtocolError("no topic in init request");
        return;
    }
    if (initRequest.message_group_id().empty()) {
        CloseByProtocolError("no message_group_id in init request");
        return;
    }
    init->SourceId = initRequest.message_group_id();
    if (initRequest.partition_group_id() > 0) {
        init->PreferedPartition = initRequest.partition_group_id() - 1;
    }
    const auto& preferredCluster = initRequest.preferred_cluster();
    if (!preferredCluster.empty()) {
        Send(GetPQWriteServiceActorID(), new TEvPQProxy::TEvSessionSetPreferredCluster(Cookie, preferredCluster));
    }
    for (const auto& item : initRequest.session_meta()) {
        init->SessionMeta[item.first] = item.second;
    }

    Send(Logic, init.Release());
}

void TWriteSessionPQv1Actor::HandleWrite(TClientMessage&& req) {
    const auto& writeRequest = req.write_request();
    auto ev = MakeHolder<NWrite::TEvWrite>();
    ev->UserRequestByteSize = req.ByteSize();

    if (!AllEqual(writeRequest.sequence_numbers_size(), writeRequest.created_at_ms_size(), writeRequest.sent_at_ms_size(), writeRequest.message_sizes_size())) {
        CloseByProtocolError(TStringBuilder() << "messages meta repeated fields do not have same size, 'sequence_numbers' size is " << writeRequest.sequence_numbers_size()
            << ", 'message_sizes' size is " << writeRequest.message_sizes_size() << ", 'created_at_ms' size is " << writeRequest.created_at_ms_size()
            << " and 'sent_at_ms' size is " << writeRequest.sent_at_ms_size());
        return;
    }
    if (!AllEqual(writeRequest.blocks_offsets_size(), writeRequest.blocks_part_numbers_size(), writeRequest.blocks_message_counts_size(), writeRequest.blocks_uncompressed_sizes_size(), writeRequest.blocks_headers_size(), writeRequest.blocks_data_size())) {
        CloseByProtocolError(TStringBuilder() << "blocks repeated fields do no have same size, 'blocks_offsets' size is " << writeRequest.blocks_offsets_size()
            << ", 'blocks_part_numbers' size is " << writeRequest.blocks_part_numbers_size() << ", 'blocks_message_counts' size is " << writeRequest.blocks_message_counts_size()
            << ", 'blocks_uncompressed_sizes' size is " << writeRequest.blocks_uncompressed_sizes_size() << ", 'blocks_headers' size is " << writeRequest.blocks_headers_size()
            << " and 'blocks_data' size is " << writeRequest.blocks_data_size());
        return;
    }

    const i32 messageCount = writeRequest.sequence_numbers_size();
    const i32 blockCount = writeRequest.blocks_offsets_size();
    if (messageCount == 0) {
        CloseByProtocolError("messages meta repeated fields are empty, write request contains no messages");
        return;
    }
    if (messageCount != blockCount) {
        CloseByProtocolError(TStringBuilder() << "messages meta repeated fields and blocks repeated fields do not have same size, messages meta fields size is " << messageCount
            << " and blocks fields size is " << blockCount << ", only one message per block is supported in blocks format version 0");
        return;
    }
    for (i32 messageIndex = 0; messageIndex != messageCount; ++messageIndex) {
        if (writeRequest.blocks_headers(messageIndex).size() != CODEC_ID_SIZE) {
            CloseByProtocolError(TStringBuilder() << "bad write request - 'blocks_headers' at position " << messageIndex <<  " has incorrect size " << writeRequest.blocks_headers(messageIndex).size() << " [B]. Only headers of size " << CODEC_ID_SIZE << " [B] (with codec identifier) are supported in block format version 0");
            return;
        }
        if (writeRequest.blocks_message_counts(messageIndex) != 1) {
            CloseByProtocolError(TStringBuilder() << "bad write request - 'blocks_message_counts' at position " << messageIndex << " is " << writeRequest.blocks_message_counts(messageIndex)
                << ", only single message per block is supported by block format version 0");
            return;
        }

        NWrite::TWriteSessionMessage out;
        out.SeqNo = writeRequest.sequence_numbers(messageIndex);
        out.CmdSeqNo = out.SeqNo;
        out.ExpectedAckSeqNo = out.SeqNo;
        out.CreateTimeMs = writeRequest.created_at_ms(messageIndex);
        out.UncompressedSize = writeRequest.blocks_uncompressed_sizes(messageIndex);
        out.CodecId = static_cast<ui32>(static_cast<unsigned char>(writeRequest.blocks_headers(messageIndex).front()));
        out.ChunkCodec = out.CodecId;
        out.Data = writeRequest.blocks_data(messageIndex);
        ev->Messages.push_back(std::move(out));
    }

    Send(Logic, ev.Release());
}

void TWriteSessionPQv1Actor::HandleUpdateToken(TClientMessage&& req) {
    auto ev = MakeHolder<NWrite::TEvUpdateToken>();
    ev->Token = req.update_token_request().token();
    Send(Logic, ev.Release());
}

void TWriteSessionPQv1Actor::HandlePoison(TEvPQProxy::TEvDieCommand::TPtr& ev) {
    Send(Logic, new NWrite::TEvDieCommand(ev->Get()->Reason, ev->Get()->ErrorCode));
}

void TWriteSessionPQv1Actor::Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev) {
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

void TWriteSessionPQv1Actor::Handle(NWrite::TEvInitAck::TPtr& ev) {
    const auto& ack = *ev->Get();
    TServerMessage response;
    response.set_status(Ydb::StatusIds::SUCCESS);
    auto* init = response.mutable_init_response();

    if (!ack.SessionId.empty()) {
        init->set_session_id(EscapeC(ack.SessionId));
    }
    if (ack.LastSeqNo.has_value()) {
        init->set_last_sequence_number(*ack.LastSeqNo);
    }
    init->set_partition_id(ack.PartitionId);
    init->set_topic(ack.FederationPath);
    init->set_cluster(ack.Cluster);
    init->set_block_format_version(0);
    for (const auto& codecName : ack.SupportedCodecNames) {
        init->add_supported_codecs(CodecByName(codecName));
    }

    WriteServerMessage(std::move(response));
}

void TWriteSessionPQv1Actor::Handle(NWrite::TEvWriteAck::TPtr& ev) {
    TServerMessage result;
    result.set_status(Ydb::StatusIds::SUCCESS);

    auto* batchWriteResponse = result.mutable_batch_write_response();
    batchWriteResponse->set_partition_id(ev->Get()->PartitionId);
    auto* stat = batchWriteResponse->mutable_write_statistics();
    for (const auto& ack : ev->Get()->Acks) {
        batchWriteResponse->add_sequence_numbers(ack.SeqNo);
        batchWriteResponse->add_offsets(ack.Offset);
        batchWriteResponse->add_already_written(ack.AlreadyWritten);
        stat->set_queued_in_partition_duration_ms(Max((i64)ack.TotalTimeInPartitionQueueMs, stat->queued_in_partition_duration_ms()));
        stat->set_throttled_on_partition_duration_ms(Max((i64)ack.PartitionQuotedTimeMs, stat->throttled_on_partition_duration_ms()));
        stat->set_throttled_on_topic_duration_ms(Max(static_cast<i64>(ack.TopicQuotedTimeMs), stat->throttled_on_topic_duration_ms()));
        stat->set_persist_duration_ms(Max((i64)ack.WriteTimeMs, stat->persist_duration_ms()));
    }

    WriteServerMessage(std::move(result));
}

void TWriteSessionPQv1Actor::Handle(NWrite::TEvUpdateTokenAck::TPtr&) {
    TServerMessage serverMessage;
    serverMessage.set_status(Ydb::StatusIds::SUCCESS);
    serverMessage.mutable_update_token_response();
    WriteServerMessage(std::move(serverMessage));
}

void TWriteSessionPQv1Actor::Handle(NWrite::TEvRefreshToken::TPtr& ev) {
    Request->RefreshToken(ev->Get()->Token, ActorContext(), SelfId(), std::move(ev->Get()->TraceId));
}

void TWriteSessionPQv1Actor::Handle(NWrite::TEvUnauthenticated::TPtr& ev) {
    Request->ReplyUnauthenticated(ev->Get()->Reason);
    PassAway();
}

void TWriteSessionPQv1Actor::Handle(NWrite::TEvClosed::TPtr& ev) {
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

void TWriteSessionPQv1Actor::Handle(NWrite::TEvReadNext::TPtr&) {
    if (!Request->Read()) {
        YDB_LOG_INFO("Session v1 grpc read failed",
            {"cookie", Cookie});
        PassAway();
    }
}

void TWriteSessionPQv1Actor::Handle(NWrite::TEvConsumedRequestUnits::TPtr& ev) {
    if (auto counters = Request->GetCounters()) {
        counters->AddConsumedRequestUnits(ev->Get()->Amount);
    }
}

NActors::IActor* CreateWriteSessionPQv1Actor(
    NGRpcService::TEvStreamPQWriteRequest* request,
    ui64 cookie,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    const std::optional<TString> clientDC,
    const NPersQueue::TTopicsListController& topicsController)
{
    return new TWriteSessionPQv1Actor(
        request, cookie, std::move(counters), clientDC, topicsController);
}

} // namespace NKikimr::NGRpcProxy::V1
