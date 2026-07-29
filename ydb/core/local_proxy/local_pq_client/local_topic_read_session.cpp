#include "local_topic_read_session.h"
#include "local_topic_client_helpers.h"
#include "local_topic_io_session_common.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/read_session_actor.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/generic/guid.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::YDB_SDK

namespace NKikimr::NKqp {

namespace {

using namespace NGRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

class TLocalTopicReadSessionActor final
    : public TLocalTopicIoSessionActor<TLocalTopicReadSessionActor, Ydb::Topic::StreamReadMessage::FromClient, Ydb::Topic::StreamReadMessage::FromServer, TReadSessionEvent::TEvent, TReaderCounters>
{
    using TBase = TLocalTopicIoSessionActor<TLocalTopicReadSessionActor, Ydb::Topic::StreamReadMessage::FromClient, Ydb::Topic::StreamReadMessage::FromServer, TReadSessionEvent::TEvent, TReaderCounters>;

    struct TEvPartition {
        enum EEv {
            EvStatusRequest = TBase::TEvPrivate::EvEnd,
            EvOffsetsCommitRequest,
            EvConfirmCreate,
            EvConfirmDestroy,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        // Events from partition session callbacks

        struct TEvStatusRequest : public TEventLocal<TEvStatusRequest, EvStatusRequest> {
            explicit TEvStatusRequest(i64 partitionSessionId)
                : PartitionSessionId(partitionSessionId)
            {}

            const i64 PartitionSessionId;
        };

        struct TEvOffsetsCommitRequest : public TEventLocal<TEvOffsetsCommitRequest, EvOffsetsCommitRequest> {
            TEvOffsetsCommitRequest(i64 partitionSessionId, ui64 startOffset, ui64 endOffset)
                : PartitionSessionId(partitionSessionId)
                , StartOffset(startOffset)
                , EndOffset(endOffset)
            {}

            const i64 PartitionSessionId;
            const ui64 StartOffset;
            const ui64 EndOffset;
        };

        struct TEvConfirmCreate : public TEventLocal<TEvConfirmCreate, EvConfirmCreate> {
            TEvConfirmCreate(i64 partitionSessionId, std::optional<ui64> readOffset, std::optional<ui64> commitOffset, std::optional<ui64> maxOffset)
                : PartitionSessionId(partitionSessionId)
                , ReadOffset(readOffset)
                , CommitOffset(commitOffset)
                , MaxOffset(maxOffset)
            {}

            const i64 PartitionSessionId;
            const std::optional<ui64> ReadOffset;
            const std::optional<ui64> CommitOffset;
            const std::optional<ui64> MaxOffset;
        };

        struct TEvConfirmDestroy : public TEventLocal<TEvConfirmDestroy, EvConfirmDestroy> {
            explicit TEvConfirmDestroy(i64 partitionSessionId)
                : PartitionSessionId(partitionSessionId)
            {}

            const i64 PartitionSessionId;
        };
    };

    class TLocalPartitionSession final : public TPartitionSessionControl {
    public:
        struct TSettings {
            i64 PartitionSessionId = 0;
            i64 PartitionId = 0;
            TString TopicPath;
            TString ReadSessionId;
        };

        TLocalPartitionSession(const TActorSystem* actorSystem, const TActorId& selfId, const TSettings& settings)
            : ActorSystem(actorSystem)
            , SelfId(selfId)
        {
            Y_VALIDATE(settings.PartitionSessionId >= 0, "PartitionSessionId must be non negative");
            PartitionSessionId = settings.PartitionSessionId;

            Y_VALIDATE(settings.PartitionId >= 0, "PartitionId must be non negative");
            PartitionId = settings.PartitionId;

            Y_VALIDATE(settings.TopicPath, "Topic path is not set");
            TopicPath = settings.TopicPath;

            Y_VALIDATE(settings.ReadSessionId, "Read session is not started");
            ReadSessionId = settings.ReadSessionId;
        }

        void RequestStatus() final {
            ActorSystem->Send(SelfId, new TEvPartition::TEvStatusRequest(PartitionSessionId));
        }

        void Commit(uint64_t startOffset, uint64_t endOffset) final {
            ActorSystem->Send(SelfId, new TEvPartition::TEvOffsetsCommitRequest(PartitionSessionId, startOffset, endOffset));
        }

        void ConfirmCreate(std::optional<uint64_t> readOffset, std::optional<uint64_t> commitOffset, std::optional<uint64_t> maxOffset) final {
            ActorSystem->Send(SelfId, new TEvPartition::TEvConfirmCreate(PartitionSessionId, readOffset, commitOffset, maxOffset));
        }

        void ConfirmDestroy() final {
            ActorSystem->Send(SelfId, new TEvPartition::TEvConfirmDestroy(PartitionSessionId));
        }

        void ConfirmEnd(std::span<const uint32_t> childIds) final {
            Y_UNUSED(childIds);
        }

    private:
        const TActorSystem* const ActorSystem = nullptr;
        const TActorId SelfId;
    };

    struct TReadSettings {
        TString Consumer;
        std::optional<TInstant> ReadFrom;
        std::optional<TDuration> MaxLag;
        std::vector<i64> PartitionIds;
        bool AutoPartitioningSupport = false;
    };

public:
    TLocalTopicReadSessionActor(const TSettings& actorSettings, const TReadSessionSettings& sessionSettings)
        : TBase(__func__, actorSettings, GetTopicPath(sessionSettings), sessionSettings.MaxMemoryUsageBytes_)
        , Settings(sessionSettings)
        , ReadSettings(GetReadSettings(sessionSettings))
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TSessionEvents::TEvExtractReadyEvents, TBase::Handle);
        hFunc(TSessionEvents::TEvEventsConsumed, Handle);
        hFunc(TSessionEvents::TEvSessionFinished, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvActorAttached, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvReadRequest, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvWriteRequest, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvFinishRequest, TBase::Handle);
        hFunc(TEvPartition::TEvStatusRequest, Handle);
        hFunc(TEvPartition::TEvOffsetsCommitRequest, Handle);
        hFunc(TEvPartition::TEvConfirmCreate, Handle);
        hFunc(TEvPartition::TEvConfirmDestroy, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        sFunc(TEvents::TEvWakeup, HandleWakeup);
    );

protected:
    void StartSession() final {
        auto ev = CreateRpcBiStreamingEvent<TEvStreamTopicReadRequest>(Settings, "StreamRead", NJaegerTracing::ERequestType::TOPIC_STREAMREAD);
        Send(NGRpcProxy::V1::GetPQReadServiceActorID(), ev.release(), IEventHandle::FlagTrackDelivery);
    }

    void SendInitMessage() final {
        YDB_LOG_INFO("Sending init message",
            {"logPrefix", LogPrefix()},
            {"consumer", ReadSettings.Consumer},
            {"readFrom", (ReadSettings.ReadFrom ? ToString(*ReadSettings.ReadFrom) : "null")},
            {"maxLag", (ReadSettings.MaxLag ? ToString(*ReadSettings.MaxLag) : "null")});

        TRpcIn message;

        auto& initRequest = *message.mutable_init_request();
        initRequest.set_consumer(ReadSettings.Consumer);

        auto& topic = *initRequest.add_topics_read_settings();
        topic.set_path(Topic);
        topic.mutable_partition_ids()->Assign(ReadSettings.PartitionIds.begin(), ReadSettings.PartitionIds.end());
        if (ReadSettings.ReadFrom) {
            *topic.mutable_read_from() = NProtoInterop::CastToProto(*ReadSettings.ReadFrom);
        }
        if (ReadSettings.MaxLag) {
            *topic.mutable_max_lag() = NProtoInterop::CastToProto(*ReadSettings.MaxLag);
        }
        initRequest.set_auto_partitioning_support(ReadSettings.AutoPartitioningSupport);

        AddSessionEvent(std::move(message));
    }

    void HandleRpcMessage(TRpcOut& message) final {
        switch (message.server_message_case()) {
            case Ydb::Topic::StreamReadMessage::FromServer::kInitResponse:
                ComputeSessionMessage(message.init_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kReadResponse:
                ComputeSessionMessage(*message.mutable_read_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kCommitOffsetResponse:
                ComputeSessionMessage(message.commit_offset_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kPartitionSessionStatusResponse:
                ComputeSessionMessage(message.partition_session_status_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kUpdateTokenResponse:
                TBase::ComputeSessionMessage(message.update_token_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest:
                ComputeSessionMessage(message.start_partition_session_request());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest:
                ComputeSessionMessage(message.stop_partition_session_request());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kUpdatePartitionSession:
                ComputeSessionMessage(message.update_partition_session());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kEndPartitionSession:
                ComputeSessionMessage(message.end_partition_session());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::SERVER_MESSAGE_NOT_SET:
                CloseSession(EStatus::INTERNAL_ERROR, "Unknown server message");
                break;
        }
    }

private:
    static TString GetTopicPath(const TReadSessionSettings& sessionSettings) {
        Y_VALIDATE(sessionSettings.Topics_.size() == 1, "Only one topic is supported per read session");
        return TString(sessionSettings.Topics_[0].Path_);
    }

    static TReadSettings GetReadSettings(const TReadSessionSettings& sessionSettings) {
        TReadSettings settings;
        settings.Consumer = sessionSettings.ConsumerName_;

        Y_VALIDATE(sessionSettings.Topics_.size() == 1, "Only one topic is supported per read session");
        const auto& topic = sessionSettings.Topics_[0];
        settings.ReadFrom = topic.ReadFromTimestamp_ ? topic.ReadFromTimestamp_ : sessionSettings.ReadFromTimestamp_;
        settings.MaxLag = topic.MaxLag_ ? topic.MaxLag_ : sessionSettings.MaxLag_;

        settings.PartitionIds.reserve(topic.PartitionIds_.size());
        for (auto partitionId : topic.PartitionIds_) {
            Y_VALIDATE(partitionId <= std::numeric_limits<i64>::max(), "PartitionId is too large");
            settings.PartitionIds.emplace_back(partitionId);
        }
        settings.AutoPartitioningSupport = sessionSettings.AutoPartitioningSupport_;

        return settings;
    }

    TPartitionSession::TPtr GetPartitionSession(i64 partitionSessionId) const {
        const auto it = PartitionSessions.find(partitionSessionId);
        Y_VALIDATE(it != PartitionSessions.end(), "Unknown partition session: " << partitionSessionId);
        return it->second;
    }

    // Events from TLocalTopicReadSession

    void Handle(TSessionEvents::TEvEventsConsumed::TPtr& ev) {
        const auto size = std::min(ev->Get()->Size, InflightMemory);
        InflightMemory -= size;
        Counters->BytesInflightTotal->Sub(size);

        TBase::Handle(ev);
        ContinueReading();
    }

    // Events from topic partition session callbacks

    void Handle(TEvPartition::TEvStatusRequest::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        YDB_LOG_DEBUG("Partition status request",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId});

        TRpcIn message;
        message.mutable_partition_session_status_request()->set_partition_session_id(partitionSessionId);

        AddSessionEvent(std::move(message));
    }

    void Handle(TEvPartition::TEvOffsetsCommitRequest::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        const auto start = ev->Get()->StartOffset;
        const auto end = ev->Get()->EndOffset;
        YDB_LOG_DEBUG("Partition offsets commit request",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId},
            {"start", start},
            {"end", end});

        TRpcIn message;

        auto& commitRequest = *message.mutable_commit_offset_request()->add_commit_offsets();
        commitRequest.set_partition_session_id(partitionSessionId);

        auto& offsets = *commitRequest.add_offsets();
        offsets.set_start(start);
        offsets.set_end(end);

        AddSessionEvent(std::move(message));
    }

    void Handle(TEvPartition::TEvConfirmCreate::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        const auto readOffset = ev->Get()->ReadOffset;
        const auto commitOffset = ev->Get()->CommitOffset;
        const auto maxOffset = ev->Get()->MaxOffset;

        YDB_LOG_DEBUG("Partition confirmed read commit",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId},
            {"readOffset", (readOffset ? ToString(*readOffset) : "null")},
            {"commitOffset", (commitOffset ? ToString(*commitOffset) : "null")},
            {"maxOffset", (maxOffset ? ToString(*maxOffset) : "null")},
        );

        TRpcIn message;

        auto& startResponse = *message.mutable_start_partition_session_response();
        startResponse.set_partition_session_id(partitionSessionId);
        if (readOffset) {
            startResponse.set_read_offset(*readOffset);
        }
        if (commitOffset) {
            startResponse.set_commit_offset(*commitOffset);
        }
        if (maxOffset) {
            startResponse.set_max_offset(*maxOffset);
        }

        AddSessionEvent(std::move(message));
    }

    void Handle(TEvPartition::TEvConfirmDestroy::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        YDB_LOG_DEBUG("Partition destroyed",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId});

        TRpcIn message;
        message.mutable_stop_partition_session_response()->set_partition_session_id(partitionSessionId);
        AddSessionEvent(std::move(message));

        AddOutgoingSessionClosedEvent(partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason::StopConfirmedByUser);
    }

    // Events from local RPC session

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto sourceType = ev->Get()->SourceType;
        const auto reason = ev->Get()->Reason;
        Y_VALIDATE(sourceType == TEvStreamTopicReadRequest::EventType, "Unexpected undelivered event: " << sourceType << ", reason: " << reason);

        YDB_LOG_ERROR("PQ read service is unavailable",
            {"logPrefix", LogPrefix()},
            {"reason", reason});
        CloseSession(EStatus::INTERNAL_ERROR, "PQ read service is unavailable, please contact internal support");
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::InitResponse& message) {
        SessionStartedAt = TInstant::Now();
        SessionId = message.session_id();
        YDB_LOG_INFO("Session initialized with",
            {"logPrefix", LogPrefix()},
            {"id", SessionId});
        ContinueReading();
    }

    void ComputeSessionMessage(Ydb::Topic::StreamReadMessage::ReadResponse& message) {
        const auto responseSize = message.bytes_size();
        ServerMemoryDelta -= responseSize;
        Counters->BytesReadCompressed->Add(responseSize);
        YDB_LOG_TRACE("Received read response with new",
            {"logPrefix", LogPrefix()},
            {"size", responseSize},
            {"serverMemoryDelta", ServerMemoryDelta});

        for (auto& partitionData : *message.mutable_partition_data()) {
            const auto partitionSessionId = partitionData.partition_session_id();
            YDB_LOG_TRACE("Partition data received",
                {"logPrefix", LogPrefix()},
                {"session", partitionSessionId},
                {"batches", partitionData.batches_size()});

            i64 messagesSize = 0;
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
            auto partitionSession = GetPartitionSession(partitionSessionId);

            for (auto& batch : *partitionData.mutable_batches()) {
                const auto& producerId = batch.producer_id();
                const auto writtenAt = NProtoInterop::CastFromProto(batch.written_at());

                const auto writeSessionMeta = MakeIntrusive<TWriteSessionMeta>();
                writeSessionMeta->Fields.reserve(batch.write_session_meta_size());
                for (auto& [key, value] : *batch.mutable_write_session_meta()) {
                    messagesSize += key.size() + value.size();
                    writeSessionMeta->Fields.emplace(key, std::move(value));
                }
                messagesSize += sizeof(TWriteSessionMeta);

                for (auto& event : *batch.mutable_message_data()) {
                    TDecompressionResult codecResult;
                    std::exception_ptr decompressionException;
                    if (!IsIn({Ydb::Topic::CODEC_RAW, Ydb::Topic::CODEC_UNSPECIFIED}, static_cast<Ydb::Topic::Codec>(batch.codec()))) {
                        try {
                            const ICodec* codecImpl = TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(batch.codec()));
                            codecResult = codecImpl->DecompressData(event.data());
                        } catch (...) {
                            decompressionException = std::current_exception();
                            codecResult.Messages.push_back(TDecompressedMessage{
                                .Data = std::string(event.data()),
                                .Meta = std::nullopt,
                            });
                        }
                    } else {
                        codecResult.Messages.push_back(TDecompressedMessage{
                            .Data = std::move(*event.mutable_data()),
                            .Meta = std::nullopt,
                        });
                    }

                    auto messageMeta = MakeIntrusive<TMessageMeta>();
                    messageMeta->Fields.reserve(event.metadata_items_size());
                    for (auto& item : *event.mutable_metadata_items()) {
                        messagesSize += item.key().size() + item.value().size();
                        messageMeta->Fields.emplace_back(std::move(*item.mutable_key()), std::move(*item.mutable_value()));
                    }

                    for (const auto& decompressedMsg : codecResult.Messages) {
                        ui64 offset = event.offset();
                        ui64 seqNo = event.seq_no();
                        TInstant createTime = NProtoInterop::CastFromProto(event.created_at());
                        if (decompressedMsg.Meta) {
                            offset = static_cast<ui64>(event.offset()) + static_cast<ui64>(decompressedMsg.Meta->OffsetDelta);
                            seqNo = static_cast<ui64>(*codecResult.BatchBaseSequence) + static_cast<ui64>(decompressedMsg.Meta->SequenceDelta);
                            createTime = TInstant::MilliSeconds(*codecResult.BatchBaseTimestampMs + decompressedMsg.Meta->TimestampDelta);
                        }

                        messagesSize += decompressedMsg.Data.size() + producerId.size() + sizeof(TMessageMeta) + event.message_group_id().size();
                        Counters->BytesRead->Add(decompressedMsg.Data.size());

                        messages.emplace_back(decompressedMsg.Data, decompressionException, TReadSessionEvent::TDataReceivedEvent::TMessageInformation(
                            offset,
                            producerId,
                            seqNo,
                            createTime,
                            writtenAt,
                            writeSessionMeta,
                            messageMeta,
                            event.uncompressed_size(),
                            event.message_group_id()
                        ), partitionSession);
                    }
                }
            }

            messagesSize += sizeof(TReadSessionEvent::TDataReceivedEvent::TMessage) * messages.size();
            Counters->MessagesRead->Add(messages.size());

            AddOutgoingEvent(TReadSessionEvent::TDataReceivedEvent(
                std::move(messages),
                {},
                std::move(partitionSession)
            ), messagesSize);
        }

        // If after decompression data size less than compressed, request new data from server
        ContinueReading();
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::CommitOffsetResponse& message) {
        for (const auto& commitOffset : message.partitions_committed_offsets()) {
            const auto partitionSessionId = commitOffset.partition_session_id();
            const auto offset = commitOffset.committed_offset();
            YDB_LOG_DEBUG("Partition offset commited",
                {"logPrefix", LogPrefix()},
                {"session", partitionSessionId},
                {"offset", offset});

            AddOutgoingEvent(TReadSessionEvent::TCommitOffsetAcknowledgementEvent(
                GetPartitionSession(partitionSessionId),
                offset
            ));
        }
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::PartitionSessionStatusResponse& message) {
        const auto partitionSessionId = message.partition_session_id();
        YDB_LOG_DEBUG("Partition status",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId},
            {"response", message});

        AddOutgoingEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
            GetPartitionSession(partitionSessionId),
            message.committed_offset(),
            message.read_offset(),
            message.partition_offsets().end(),
            NProtoInterop::CastFromProto(message.write_time_high_watermark())
        ));
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::StartPartitionSessionRequest& message) {
        const auto& info = message.partition_session();
        const auto partitionId = info.partition_id();
        const auto partitionSessionId = info.partition_session_id();
        const auto committedOffset = message.committed_offset();
        const auto& offsets = message.partition_offsets();
        YDB_LOG_DEBUG("Start session with id commited offsets",
            {"logPrefix", LogPrefix()},
            {"partition", partitionId},
            {"partitionSessionId", partitionSessionId},
            {"offset", committedOffset},
            {"range", offsets});

        auto partitionSession = MakeIntrusive<TLocalPartitionSession>(ActorContext().ActorSystem(), SelfId(), TLocalPartitionSession::TSettings{
            .PartitionSessionId = partitionSessionId,
            .PartitionId = partitionId,
            .TopicPath = info.path(),
            .ReadSessionId = SessionId,
        });
        if (const auto [it, inserted] = PartitionSessions.emplace(partitionSessionId, partitionSession); !inserted) {
            // After internal server retry session may be reconnected
            YDB_LOG_NOTICE("Partition reconnected",
                {"logPrefix", LogPrefix()},
                {"session", partitionSessionId});
            AddOutgoingSessionClosedEvent(partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason::Lost);
            it->second = partitionSession;
        }

        AddOutgoingEvent(TReadSessionEvent::TStartPartitionSessionEvent(
            std::move(partitionSession),
            committedOffset,
            offsets.end()
        ));
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::StopPartitionSessionRequest& message) {
        const auto partitionSessionId = message.partition_session_id();
        const auto committedOffset = message.committed_offset();
        YDB_LOG_DEBUG("Partition received stop event, commited",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId},
            {"offset", committedOffset});

        if (!message.graceful()) {
            return AddOutgoingSessionClosedEvent(partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason::Lost);
        }

        return AddOutgoingEvent(TReadSessionEvent::TStopPartitionSessionEvent(
            GetPartitionSession(partitionSessionId),
            committedOffset
        ));
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::UpdatePartitionSession& message) {
        YDB_LOG_DEBUG("Partition received update",
            {"logPrefix", LogPrefix()},
            {"session", message.partition_session_id()},
            {"event", message});
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::EndPartitionSession& message) {
        const auto partitionSessionId = message.partition_session_id();
        const auto& adjacentIds = message.adjacent_partition_ids();
        const auto& childIds = message.child_partition_ids();
        YDB_LOG_DEBUG("Partition received end event adjacent child",
            {"logPrefix", LogPrefix()},
            {"session", partitionSessionId},
            {"adjacentIds", adjacentIds.size()},
            {"childIds", childIds.size()});

        AddOutgoingEvent(TReadSessionEvent::TEndPartitionSessionEvent(
            GetPartitionSession(partitionSessionId),
            std::vector<uint32_t>(adjacentIds.begin(), adjacentIds.end()),
            std::vector<uint32_t>(childIds.begin(), childIds.end())
        ), sizeof(uint32_t) * (adjacentIds.size() + childIds.size()));
    }

    // Events to PQ read service

    void ContinueReading() {
        if (!SessionId) {
            YDB_LOG_DEBUG("Session not started yet, skip reading",
                {"logPrefix", LogPrefix()});
            return;
        }

        if (InflightMemory >= MaxMemoryUsage) {
            YDB_LOG_TRACE("Max memory usage reached, skip reading",
                {"logPrefix", LogPrefix()},
                {"inflightMemory", InflightMemory},
                {"maxMemoryUsage", MaxMemoryUsage});
            return;
        }

        const auto readMemoryBudget = MaxMemoryUsage - InflightMemory;
        if (ServerMemoryDelta >= readMemoryBudget) {
            YDB_LOG_TRACE("Server already has enough memory, skip reading, read memory",
                {"logPrefix", LogPrefix()},
                {"serverMemoryDelta", ServerMemoryDelta},
                {"budget", readMemoryBudget});
            return;
        }

        const auto bytesToRead = readMemoryBudget - ServerMemoryDelta;
        ServerMemoryDelta = readMemoryBudget;
        YDB_LOG_TRACE("Reading bytes",
            {"logPrefix", LogPrefix()},
            {"bytesToRead", bytesToRead},
            {"serverMemoryDelta", ServerMemoryDelta},
            {"inflightMemory", InflightMemory},
            {"maxMemoryUsage", MaxMemoryUsage});

        TRpcIn message;
        message.mutable_read_request()->set_bytes_size(bytesToRead);
        AddSessionEvent(std::move(message));
    }

    // Events to TLocalTopicReadSession

    void AddOutgoingSessionClosedEvent(i64 partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason reason) {
        AddOutgoingEvent(TReadSessionEvent::TPartitionSessionClosedEvent(
            GetPartitionSession(partitionSessionId),
            reason
        ));
    }

    template <typename TEvent>
    void AddOutgoingEvent(TEvent&& event, i64 internalSize = 0) {
        const auto size = static_cast<i64>(sizeof(TEvent)) + internalSize;
        InflightMemory += size;
        YDB_LOG_TRACE("Adding outgoing event",
            {"logPrefix", LogPrefix()},
            {"size", size},
            {"inflightMemory", InflightMemory});

        Counters->BytesInflightTotal->Add(size);
        TBase::AddOutgoingEvent(std::move(event), size);
    }

    const TRequestSettings<TReadSessionSettings> Settings;
    const TReadSettings ReadSettings;

    i64 ServerMemoryDelta = 0;
    TString SessionId;
    std::unordered_map<i64, TPartitionSession::TPtr> PartitionSessions;
};

// Supposed to be used from actor system, so all blocking methods are not supported.
// Read session is not thread safe and MUST be used from single actor.
// NOTICE: data is decompressed in one thread inside TLocalTopicReadSessionActor for simplicity.
class TLocalTopicReadSession final : public TLocalTopicSessionBase<TReadSessionEvent::TEvent>, public IReadSession {
    using TBase = TLocalTopicSessionBase<TReadSessionEvent::TEvent>;
    using TReadEvents = TLocalTopicReadSessionActor::TSessionEvents;

public:
    TLocalTopicReadSession(const TLocalTopicSessionSettings& localSettings, const TReadSessionSettings& sessionSettings)
        : TBase(localSettings)
        , Counters(SetupCounters(sessionSettings))
        , SessionId(CreateGuidAsString())
    {
        ValidateSettings(sessionSettings);
        Start(sessionSettings);
    }

    ~TLocalTopicReadSession() {
        try {
            if (!Close(TDuration::Zero()) && ActorSystem && ReadSessionActor) {
                ActorSystem->Send(ReadSessionActor, new TReadEvents::TEvSessionFinished(/* force */ true));
            }
        } catch (...) {
            // ¯\_(ツ)_/¯
        }
    }

    NThreading::TFuture<void> WaitEvent() final {
        return TBase::WaitEvent();
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) final {
        Y_VALIDATE(!block, "Blocking is not supported for local topic read session");
        Y_VALIDATE(maxByteSize > 0, "MaxByteSize must be greater than 0");

        ExtractEvents();

        std::vector<TReadSessionEvent::TEvent> result;
        const auto maxCount = maxEventsCount.value_or(std::numeric_limits<size_t>::max());
        result.reserve(std::min(Events.size(), maxCount));

        i64 totalSize = 0;
        while (!Events.empty() && result.size() < maxCount && static_cast<size_t>(totalSize) < maxByteSize) {
            auto& event = Events.front();
            totalSize += event.Size;

            if (std::holds_alternative<TSessionClosedEvent>(event.Event)) {
                // Skip all events after session closed event
                result.emplace_back(event.Event);
                break;
            }

            result.emplace_back(std::move(event.Event));
            Events.pop();
        }

        if (ReadSessionActor && !result.empty()) {
            ActorSystem->Send(ReadSessionActor, new TReadEvents::TEvEventsConsumed(totalSize, result.size()));
        }

        WaitEvent(); // Request next event batch

        return result;
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) final {
        Y_VALIDATE(!settings.Tx_, "Transaction is not supported for local topic read session");
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) final {
        auto events = GetEvents(block, 1, maxByteSize);
        if (events.empty()) {
            return std::nullopt;
        }
        return std::move(events[0]);
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) final {
        Y_VALIDATE(settings.MaxEventsCount_.value_or(1) == 1, "MaxEventsCount should be one for GetEvent");
        Y_VALIDATE(!settings.Tx_, "Transaction is not supported for local topic read session");
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout) final {
        Y_VALIDATE(!timeout, "Timeout on close is not allowed for local topic read session");

        ExtractEvents(); // Refresh events and maybe accept TSessionClosedEvent
        WaitEvent(); // Request next event batch if there is no TSessionClosedEvent

        if (!ReadSessionActor) {
            // TSessionClosedEvent accepted
            return true;
        }

        // Wait for TSessionClosedEvent
        ActorSystem->Send(ReadSessionActor, new TReadEvents::TEvSessionFinished(/* force */ false));
        return false;
    }

    TReaderCounters::TPtr GetCounters() const final {
        return Counters;
    }

    std::string GetSessionId() const final {
        return SessionId;
    }

protected:
    void RequestEvents(NThreading::TPromise<std::vector<TEvent>> promise) {
        Y_VALIDATE(ReadSessionActor, "Read session actor unexpectedly finished");
        ActorSystem->Send(ReadSessionActor, new TReadEvents::TEvExtractReadyEvents(std::move(promise)));
    }

    void OnCloseReceived() final {
        ReadSessionActor = {};
    }

private:
    static void ValidateSettings(const TReadSessionSettings& settings) {
        TBase::ValidateSettings(settings);

        Y_VALIDATE(settings.Decompress_, "Read session without decompression is not supported");

        const auto& eventHandlers = settings.EventHandlers_;
        Y_VALIDATE(!eventHandlers.DataReceivedHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.CommitOffsetAcknowledgementHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.StartPartitionSessionHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.StopPartitionSessionHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.EndPartitionSessionHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.PartitionSessionStatusHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.PartitionSessionClosedHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.SessionClosedHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.CommonHandler_, "Event handlers are not supported for local topic read session");
    }

    static TReaderCounters::TPtr SetupCounters(const TReadSessionSettings& settings) {
        auto result = settings.Counters_;
        if (!result || HasNullCounters(*result)) {
            result = result ? result : MakeIntrusive<TReaderCounters>();
            MakeCountersNotNull(*result);
        }
        return result;
    }

    void Start(const TReadSessionSettings& sessionSettings) {
        Y_VALIDATE(!ReadSessionActor, "Read session is already started");
        ReadSessionActor = ActorSystem->Register(new TLocalTopicReadSessionActor({
            .Database = Database,
            .CredentialsProvider = CredentialsProvider,
            .Counters = Counters,
        }, sessionSettings), TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    }

    const TReaderCounters::TPtr Counters;
    const TString SessionId;
    TActorId ReadSessionActor;
};

} // anonymous namespace

std::shared_ptr<IReadSession> CreateLocalTopicReadSession(const TLocalTopicSessionSettings& localSettings, const TReadSessionSettings& sessionSettings) {
    return std::make_shared<TLocalTopicReadSession>(localSettings, sessionSettings);
}

} // namespace NKikimr::NKqp
