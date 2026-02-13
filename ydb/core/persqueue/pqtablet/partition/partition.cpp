#include "offload_actor.h"
#include "partition_common.h"
#include "partition_compactification.h"
#include "partition_util.h"

#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/pqtablet/common/event_helpers.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include <ydb/core/persqueue/pqtablet/common/tracing_support.h>
#include <ydb/core/persqueue/pqtablet/partition/autopartitioning_manager.h>
#include <ydb/core/persqueue/pqtablet/partition/mirrorer/mirrorer_factory.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/public/lib/base/msgbus.h>

#include <util/folder/path.h>
#include <util/string/escape.h>
#include <util/system/byteorder.h>
#include <util/generic/overloaded.h>

namespace NKafka {

    // IsDuplicate is only needed for Kafka protocol deduplication.
    // baseSequence field in Kafka protocol has type int32 and the numbers loop back from the maximum possible value back to 0.
    // I.e. the next seqno after int32max is 0.
    // To decide if we got a duplicate seqno or an out of order seqno,
    // we are comparing the difference between maxSeqNo and seqNo with MAX_SEQNO_DIFFERENCE_UNTIL_OUT_OF_ORDER.
    // The value of MAX_SEQNO_DIFFERENCE_UNTIL_OUT_OF_ORDER is half of the int32 range.
    bool IsDuplicate(ui64 maxSeqNo, ui64 seqNo) {
        if (maxSeqNo < seqNo) {
            maxSeqNo += 1ul << 31;
        }
        return maxSeqNo - seqNo < MAX_SEQNO_DIFFERENCE_UNTIL_OUT_OF_ORDER;
    }

    // InSequence is only needed for Kafka protocol deduplication.
    bool InSequence(ui64 maxSeqNo, ui64 seqNo) {
        return (maxSeqNo + 1 == seqNo) || (maxSeqNo == std::numeric_limits<i32>::max() && seqNo == 0);
    }

    ECheckDeduplicationResult CheckDeduplication(i16 lastEpoch, ui64 lastSeqNo, i16 messageEpoch, ui64 messageSeqNo) {
        if (lastEpoch > messageEpoch) {
            return ECheckDeduplicationResult::INVALID_PRODUCER_EPOCH;
        }

        if (lastEpoch < messageEpoch) {
            if (messageSeqNo == 0) {
                // Only the first ever epoch for a given producerId is allowed to have the first seqNo != 0.
                return ECheckDeduplicationResult::OK;
            }
            return ECheckDeduplicationResult::OUT_OF_ORDER_SEQUENCE_NUMBER;
        }

        if (InSequence(lastSeqNo, messageSeqNo)) {
            return ECheckDeduplicationResult::OK;
        }

        if (IsDuplicate(lastSeqNo, messageSeqNo)) {
            // Kafka sends successful answer in response to requests
            // that exactly match some of the last 5 batches (that may contain multiple records each).

            return ECheckDeduplicationResult::DUPLICATE_SEQUENCE_NUMBER;
        }

        return ECheckDeduplicationResult::OUT_OF_ORDER_SEQUENCE_NUMBER;
    }

    std::pair<NPersQueue::NErrorCode::EErrorCode, TString> MakeDeduplicationError(
        ECheckDeduplicationResult res, const TString& topicName, ui32 partitionId, const TString& sourceId, ui64 poffset,
        i16 lastEpoch, ui64 lastSeqNo, i16 messageEpoch, ui64 messageSeqNo
    ) {
        switch (res) {
        case NKafka::ECheckDeduplicationResult::INVALID_PRODUCER_EPOCH: {
            return {
                NPersQueue::NErrorCode::KAFKA_INVALID_PRODUCER_EPOCH,
                TStringBuilder() << "Epoch of producer " << EscapeC(sourceId) << " at offset " << poffset
                                    << " in " << topicName << "-" << partitionId << " is " << messageEpoch
                                    << ", which is smaller than the last seen epoch " << lastEpoch
            };
        }
        case NKafka::ECheckDeduplicationResult::OUT_OF_ORDER_SEQUENCE_NUMBER: {
            auto message = TStringBuilder() << "Out of order sequence number for producer " << EscapeC(sourceId) << " at offset " << poffset
                                    << " in " << topicName << "-" << partitionId << ": ";
            if (lastEpoch < messageEpoch) {
                message << "for new producer epoch expected seqNo 0, got " << messageSeqNo;
            } else {
                message << messageSeqNo << " (incoming seq. number), " << lastSeqNo << " (current end sequence number)";
            }
            return {NPersQueue::NErrorCode::KAFKA_OUT_OF_ORDER_SEQUENCE_NUMBER, message};
        }
        default:
            return {};
        }
    }
}

namespace {

template <class T>
struct TIsSimpleSharedPtr : std::false_type {
};

template <class U>
struct TIsSimpleSharedPtr<TSimpleSharedPtr<U>> : std::true_type {
};

}

namespace NKikimr::NPQ {

static const TDuration WAKE_TIMEOUT = TDuration::Seconds(5);
static const TDuration UPDATE_AVAIL_SIZE_INTERVAL = TDuration::MilliSeconds(100);
static const TDuration MIN_UPDATE_COUNTERS_DELAY = TDuration::MilliSeconds(300);
static const ui32 MAX_USERS = 1000;
static const ui32 MAX_KEYS = 10000;
static const ui32 MAX_TXS = 1000;
static const ui32 MAX_WRITE_CYCLE_SIZE = 16_MB;

TEvPQ::TMessageGroupsPtr CreateExplicitMessageGroups(const NKikimrPQ::TBootstrapConfig& bootstrapCfg,
                                                     const NKikimrPQ::TPartitions& partitionsData,
                                                     const TPartitionGraph& graph,
                                                     ui32 partitionId);

TStringBuilder MakeTxWriteErrorMessage(TMaybe<ui64> txId,
                                       TStringBuf topicName, const TPartitionId& partitionId,
                                       TStringBuf sourceId, ui64 seqNo)
{
    TStringBuilder ss;
    ss << "[TxId: " << txId << ", Topic: '" << topicName << "', Partition " << partitionId << ", SourceId '" << EscapeC(sourceId) << "', SeqNo " << seqNo << "] ";
    return ss;
}

TStringBuilder MakeTxReadErrorMessage(TMaybe<ui64> txId,
                                      TStringBuf topicName, const TPartitionId& partitionId,
                                      TStringBuf consumer)
{
    TStringBuilder ss;
    ss << "[TxId: " << txId << ", Topic: '" << topicName << "', Partition " << partitionId << ", Consumer '" << consumer << "] ";
    return ss;
}

auto GetStepAndTxId(ui64 step, ui64 txId)
{
    return std::make_pair(step, txId);
}

template<class E>
auto GetStepAndTxId(const E& event)
{
    return GetStepAndTxId(event.Step, event.TxId);
}

// SeqnoViolation checks that the message seqno is correct and is used in transaction processing.
// The user may run conflicting transactions and we should block a transaction that tries to write a wrong seqno.
bool SeqnoViolation(TMaybe<i16> lastEpoch, ui64 lastSeqNo, TMaybe<i16> messageEpoch, ui64 messageSeqNo) {
    bool isKafkaRequest = lastEpoch.Defined() && messageEpoch.Defined();
    if (isKafkaRequest) {
        // In Kafka conflicting transactions are not possible if the user follows the protocol.
        return false;
    }

    return messageSeqNo <= lastSeqNo;
}

bool TPartition::LastOffsetHasBeenCommited(const TUserInfoBase& userInfo) const {
    return !IsActive() &&
        (static_cast<ui64>(std::max<i64>(userInfo.Offset, 0)) == GetEndOffset() ||
         GetStartOffset() == GetEndOffset());
}

struct TMirrorerInfo {
    TMirrorerInfo(const TActorId& actor)
    : Actor(actor) {
    }

    TActorId Actor;
};

const TString& TPartition::TopicName() const {
    return TopicConverter->GetClientsideName();
}

TString TPartition::LogPrefix() const {
    TString state;
    if (CurrentStateFunc() == &TThis::StateInit) {
        state = "StateInit";
    } else if (CurrentStateFunc() == &TThis::StateIdle) {
        state = "StateIdle";
    } else {
        state = "Unknown";
    }
    return TStringBuilder() << "[Partition][" << Partition << "][" << state << "] ";
}

const TString& TPartition::GetLogPrefix() const {
    TMaybe<TString>* logPrefix = &UnknownLogPrefix;
    if (CurrentStateFunc() == &TThis::StateInit) {
        logPrefix = &InitLogPrefix;
    } else if (CurrentStateFunc() == &TThis::StateIdle) {
        logPrefix = &IdleLogPrefix;
    }

    if (!logPrefix->Defined()) {
        *logPrefix = LogPrefix();
    }

    return *(*logPrefix);
}

bool TPartition::IsActive() const {
    return PartitionConfig == nullptr || PartitionConfig->GetStatus() == NKikimrPQ::ETopicPartitionStatus::Active;
}

bool TPartition::CanWrite() const {
    if (PartitionConfig == nullptr) {
        // Old format without AllPartitions configuration field.
        // It is not split/merge partition.
        return true;
    }
    if (NewPartition && PartitionConfig->ParentPartitionIdsSize() > 0) {
        // A tx of create partition configuration is not commited.
        return false;
    }
    if (PendingPartitionConfig && PendingPartitionConfig->GetStatus() != NKikimrPQ::ETopicPartitionStatus::Active) {
        // Pending configuration tx inactivate this partition.
        return false;
    }

    if (ClosedInternalPartition) {
        return false;
    }

    return IsActive();
}

bool TPartition::CanEnqueue() const {
    if (ClosedInternalPartition) {
        return false;
    }

    return IsActive();
}

ui64 GetOffsetEstimate(const std::deque<TDataKey>& container, TInstant timestamp, ui64 offset) {
    return GetOffsetEstimate(container, timestamp).GetOrElse(offset);
}

TMaybe<ui64> GetOffsetEstimate(const std::deque<TDataKey>& container, TInstant timestamp) {
    if (container.empty()) {
        return Nothing();
    }
    auto it = std::ranges::lower_bound(container, timestamp, {}, &TDataKey::Timestamp);
    if (it == container.end()) {
        return Nothing();
    } else {
        return it->Key.GetOffset();
    }
}

TActorId TPartition::ReplyTo(const ui64 destination, const TActorId& replyTo) const {
    if (replyTo) {
        return replyTo;
    }
    if (destination == 0) {
        return SelfId();
    }
    return TabletActorId;
}

void TPartition::ReplyError(const TActorContext& ctx, const ui64 dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error, const TActorId& replyTo) {
    auto replyToActor = ReplyTo(dst, replyTo);
    ReplyPersQueueError(
        replyToActor, ctx, TabletId, TopicName(), Partition,
        TabletCounters, NKikimrServices::PERSQUEUE, dst, errorCode, error, true, replyToActor == SelfId()
    );
}

void TPartition::ReplyError(const TActorContext& ctx, const ui64 dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error, NWilson::TSpan& span) {
    ReplyError(ctx, dst, errorCode, error);
    span.EndError(error);
}

void TPartition::ReplyPropose(const TActorContext& ctx,
                              const NKikimrPQ::TEvProposeTransaction& event,
                              NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode,
                              NKikimrPQ::TError::EKind kind,
                              const TString& reason)
{
    ctx.Send(ActorIdFromProto(event.GetSourceActor()),
             MakeReplyPropose(event,
                              statusCode,
                              kind, reason).Release());
}

void TPartition::ReplyOk(const TActorContext& ctx, const ui64 dst) {
    ctx.Send(TabletActorId, MakeReplyOk(dst, false).Release());
}

void TPartition::ReplyOk(const TActorContext& ctx, const ui64 dst, NWilson::TSpan& span) {
    ReplyOk(ctx, dst);
    span.EndOk();
}

void TPartition::ReplyGetClientOffsetOk(const TActorContext& ctx, const ui64 dst, const i64 offset,
    const TInstant writeTimestamp, const TInstant createTimestamp, bool consumerHasAnyCommits,
    const std::optional<TString>& committedMetadata) {
    ctx.Send(TabletActorId, MakeReplyGetClientOffsetOk(dst, offset, writeTimestamp, createTimestamp, consumerHasAnyCommits, committedMetadata).Release());
}

NKikimrClient::TKeyValueRequest::EStorageChannel GetChannel(ui32 i) {
    return NKikimrClient::TKeyValueRequest::EStorageChannel(NKikimrClient::TKeyValueRequest::MAIN + i);
}

void AddCheckDiskRequest(TEvKeyValue::TEvRequest *request, ui32 numChannels) {
    for (ui32 i = 0; i < numChannels; ++i) {
        request->Record.AddCmdGetStatus()->SetStorageChannel(GetChannel(i));
    }
}

TPartition::TPartition(ui64 tabletId, const TPartitionId& partition, const TActorId& tablet, ui32 tabletGeneration, const TActorId& blobCache,
                       const NPersQueue::TTopicConverterPtr& topicConverter, TString dcId, bool isServerless,
                       const NKikimrPQ::TPQTabletConfig& tabletConfig, const std::shared_ptr<TTabletCountersBase>& counters, bool subDomainOutOfSpace, ui32 numChannels,
                       const TActorId& writeQuoterActorId,
                       TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> samplingControl,
                       bool newPartition)
    : TBaseTabletActor(tabletId, tablet, NKikimrServices::PERSQUEUE)
    , Initializer(this)
    , TabletGeneration(tabletGeneration)
    , Partition(partition)
    , TabletConfig(tabletConfig)
    , Counters(counters)
    , TopicConverter(topicConverter)
    , IsLocalDC(TabletConfig.GetLocalDC())
    , DCId(std::move(dcId))
    , PartitionGraph()
    , SourceManager(*this)
    , WriteInflightSize(0)
    , BlobCache(blobCache)
    , CompactionBlobEncoder(partition, false)
    , BlobEncoder(partition, true)
    , GapSize(0)
    , IsServerless(isServerless)
    , ReadingTimestamp(false)
    , Cookie(ERequestCookie::End)
    , InitDuration(TDuration::Zero())
    , InitDone(false)
    , NewPartition(newPartition)
    , Subscriber(partition, TabletCounters, TabletActorId)
    , DiskIsFull(false)
    , SubDomainOutOfSpace(subDomainOutOfSpace)
    , HasDataReqNum(0)
    , WriteQuotaTrackerActor(writeQuoterActorId)
    , AvgWriteBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000}, {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
    , AvgReadBytes(TDuration::Minutes(1), 1000)
    , AvgQuotaBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000}, {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
    , ReservedSize(0)
    , Channel(0)
    , NumChannels(numChannels)
    , WriteBufferIsFullCounter(nullptr)
    , WriteLagMs(TDuration::Minutes(1), 100)
    , LastEmittedHeartbeat(TRowVersion::Min())
    , SamplingControl(samplingControl)
    , MessageIdDeduplicator(Partition, CreateDefaultTimeProvider(), TDuration::Minutes(5))
{
    TabletCounters.Populate(*Counters);
    TabletCounters.ResetCounters();
}

void TPartition::EmplaceResponse(TMessage&& message, const TActorContext& ctx) {
    const auto now = ctx.Now();
    Responses.emplace_back(
        message.Body,
        std::move(message.Span),
        (now - TInstant::Zero()) - message.QueueTime,
        now
    );
}

ui64 TPartition::UserDataSize() const {
    if (CompactionBlobEncoder.DataKeysBody.size() <= 1) {
        // tiny optimization - we do not meter very small queues up to 16MB
        return 0;
    }

    // We assume that DataKeysBody contains an up-to-date set of blobs, their relevance is
    // maintained by the background process. However, the last block may contain several irrelevant
    // messages. Because of them, we throw out the size of the entire blob.
    auto size = Size();
    auto lastBlobSize = CompactionBlobEncoder.DataKeysBody[0].Size;
    Y_DEBUG_ABORT_UNLESS(size >= lastBlobSize, "Metering data size must be positive");
    return size >= lastBlobSize ? size - lastBlobSize : 0;
}

ui64 TPartition::MeteringDataSize(TInstant now) const {
    if (IsActive() || NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == Config.GetMeteringMode()) {
        return UserDataSize();
    } else {
        // We only add the amount of data that is blocked by an important consumer.
        auto expirationTimestamp = now - TDuration::Seconds(Config.GetPartitionConfig().GetLifetimeSeconds()) - WAKE_TIMEOUT;
        ui64 size = BlobEncoder.GetBodySizeBefore(expirationTimestamp);
        return size;
    }
}

ui64 TPartition::ReserveSize() const {
    return IsActive() ? TopicPartitionReserveSize(Config) : 0;
}

ui64 TPartition::StorageSize(const TActorContext&) const {
    return std::max<ui64>(UserDataSize(), ReserveSize());
}

ui64 TPartition::UsedReserveSize(const TActorContext&) const {
    return std::min<ui64>(UserDataSize(), ReserveSize());
}

ui64 TPartition::GetUsedStorage(const TInstant& now) {
    const auto duration = now - LastUsedStorageMeterTimestamp;
    LastUsedStorageMeterTimestamp = now;

    auto dataSize = MeteringDataSize(now);
    auto reservedSize = ReserveSize();
    auto size = dataSize > reservedSize ? dataSize - reservedSize : 0;
    return size * duration.MilliSeconds() / 1000 / 1_MB; // mb*seconds
}

static TDuration GetAvailabilityPeriod(const TUserInfo& consumer)  {
    if (consumer.Important) {
        return TDuration::Max();
    }
    return consumer.AvailabilityPeriod;
}

bool TPartition::ImportantConsumersNeedToKeepCurrentKey(const TDataKey& currentKey, const TDataKey& nextKey, const TInstant now) const {
    for (const auto& [name, userInfo] : UsersInfoStorage->ViewImportant()) {
        const TDuration availabilityPeriod = GetAvailabilityPeriod(userInfo);
        if (availabilityPeriod == TDuration::Zero()) {
            continue;
        }
        ui64 curOffset = GetStartOffset();
        if (userInfo.Offset >= 0) // -1 means no offset
            curOffset = userInfo.Offset;

        if (ImportantConsumerNeedToKeepCurrentKey(availabilityPeriod, curOffset, currentKey, nextKey, now)) {
            return true;
        }
    }

    return false;
}


TInstant TPartition::GetEndWriteTimestamp() const {
    return EndWriteTimestamp;
}

void TPartition::HandleWakeup(const TActorContext& ctx) {
    const auto now = ctx.Now();

    FilterDeadlinedWrites(ctx);

    ctx.Schedule(WAKE_TIMEOUT, new TEvents::TEvWakeup());
    ctx.Send(TabletActorId, new TEvPQ::TEvPartitionCounters(Partition, TabletCounters));
    TabletCounters.Cumulative().ResetCounters();
    TabletCounters.Percentile().ResetCounters();

    ui64 usedStorage = GetUsedStorage(now);
    if (usedStorage > 0) {
        ctx.Send(TabletActorId, new TEvPQ::TEvMetering(EMeteringJson::UsedStorageV1, usedStorage));
    }

    if (ManageWriteTimestampEstimate || !IsActive()) {
        WriteTimestampEstimate = now;
    }

    ReportCounters(ctx);

    ProcessHasDataRequests(ctx);

    for (auto&& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.UpdateReadingTimeAndState(GetEndOffset(), now);
        for (auto& avg : userInfo.second.AvgReadBytes) {
            avg.Update(now);
        }
    }
    WriteBufferIsFullCounter.UpdateWorkingTime(now);

    WriteLagMs.Update(0, now);

    for (auto& avg : AvgWriteBytes) {
        avg.Update(now);
    }
    for (auto& avg : AvgQuotaBytes) {
        avg.Update(now);
    }

    TryRunCompaction();

    AutopartitioningManager->CleanUp();
}

void TPartition::AddMetaKey(TEvKeyValue::TEvRequest* request) {
    //Set Start Offset
    auto write = request->Record.AddCmdWrite();
    TKeyPrefix ikey(TKeyPrefix::TypeMeta, Partition);

    NKikimrPQ::TPartitionMeta meta;
    //meta.SetStartOffset(GetStartOffset());
    //meta.SetEndOffset(Max(BlobEncoder.NewHead.GetNextOffset(), GetEndOffset()));
    meta.SetSubDomainOutOfSpace(SubDomainOutOfSpace);
    meta.SetEndWriteTimestamp(PendingWriteTimestamp.MilliSeconds());
    meta.SetNextMessageIdDeduplicatorWAL(MessageIdDeduplicator.NextMessageIdDeduplicatorWAL);

    if (IsSupportive()) {
        auto* counterData = meta.MutableCounterData();
        counterData->SetMessagesWrittenGrpc(MsgsWrittenGrpc.Value());
        counterData->SetMessagesWrittenTotal(MsgsWrittenTotal.Value());
        counterData->SetBytesWrittenGrpc(BytesWrittenGrpc.Value());
        counterData->SetBytesWrittenTotal(BytesWrittenTotal.Value());
        counterData->SetBytesWrittenUncompressed(BytesWrittenUncompressed.Value());
        for(const auto& v : MessageSize.GetValues()) {
            counterData->AddMessagesSizes(v);
        }
    }

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);

    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(std::move(out));
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

bool TPartition::CleanUp(TEvKeyValue::TEvRequest* request, const TActorContext& ctx) {
    if (IsSupportive()) {
        return false;
    }

    bool haveChanges = CleanUpBlobs(request, ctx);

    LOG_T("Have " << request->Record.CmdDeleteRangeSize() << " items to delete old stuff");

    haveChanges |= SourceIdStorage.DropOldSourceIds(request, ctx.Now(), GetStartOffset(), Partition,
                                                    Config.GetPartitionConfig());
    if (haveChanges) {
        SourceIdStorage.MarkOwnersForDeletedSourceId(Owners);
    }

    LOG_T("Have " << request->Record.CmdDeleteRangeSize() << " items to delete all stuff. "
            << "Delete command " << request->ToString());

    return haveChanges;
}

bool TPartition::CleanUpBlobs(TEvKeyValue::TEvRequest *request, const TActorContext& ctx) {
    if (GetStartOffset() == GetEndOffset() || CompactionBlobEncoder.DataKeysBody.size() <= 1) {
        return false;
    }
    if (Config.GetEnableCompactification()) {
        return false;
    }
    const auto& partConfig = Config.GetPartitionConfig();

    const TDuration lifetimeLimit{TDuration::Seconds(partConfig.GetLifetimeSeconds())};

    const bool hasStorageLimit = partConfig.HasStorageLimitBytes();
    const auto now = ctx.Now();

    bool hasDrop = false;
    while (CompactionBlobEncoder.DataKeysBody.size() > 1) {
        const auto& nextKey = CompactionBlobEncoder.DataKeysBody[1];
        const auto& firstKey = CompactionBlobEncoder.DataKeysBody.front();
        if (ImportantConsumersNeedToKeepCurrentKey(firstKey, nextKey, now)) {
            break;
        }

        if (hasStorageLimit) {
            const auto bodySize = CompactionBlobEncoder.BodySize - firstKey.Size;
            if (bodySize < partConfig.GetStorageLimitBytes()) {
                break;
            }
        } else {
            if (now < firstKey.Timestamp + lifetimeLimit) {
                break;
            }
        }

        CompactionBlobEncoder.pop_front();

        if (!GapOffsets.empty() && nextKey.Key.GetOffset() == GapOffsets.front().second) {
            GapSize -= GapOffsets.front().second - GapOffsets.front().first;
            GapOffsets.pop_front();
        }

        hasDrop = true;
    }

    PQ_ENSURE(!CompactionBlobEncoder.DataKeysBody.empty());

    if (!hasDrop) {
        return false;
    }

    const auto& lastKey = CompactionBlobEncoder.DataKeysBody.front().Key;

    CompactionBlobEncoder.StartOffset = lastKey.GetOffset();
    if (lastKey.GetPartNo() > 0) {
        ++CompactionBlobEncoder.StartOffset;
    }

    Y_UNUSED(request);

    return true;
}

void TPartition::Handle(TEvPQ::TEvMirrorerCounters::TPtr& ev, const TActorContext& /*ctx*/) {
    if (Mirrorer) {
        TabletCounters.Populate(ev->Get()->Counters);
    }
}

void TPartition::Handle(TEvPQ::TBroadcastPartitionError::TPtr& ev, const TActorContext& ctx) {
    const NKikimrPQ::TBroadcastPartitionError& record = ev->Get()->Record;
    for (const auto& group : record.GetMessageGroups()) {
        for (const auto& error : group.GetErrors()) {
            LogAndCollectError(error, ctx);
        }
    }
}

void TPartition::DestroyActor(const TActorContext& ctx)
{
    // Reply to all outstanding requests in order to destroy corresponding actors

    NPersQueue::NErrorCode::EErrorCode errorCode;
    TStringBuilder ss;

    if (IsSupportive()) {
        errorCode = NPersQueue::NErrorCode::ERROR;
        ss << "The transaction is completed";
    } else {
        errorCode = NPersQueue::NErrorCode::INITIALIZING;
        ss << "Tablet is restarting, topic '" << TopicName() << "'";
    }

    for (const auto& ev : WaitToChangeOwner) {
        ReplyError(ctx, ev->Cookie, errorCode, ss);
    }

    for (auto& w : PendingRequests) {
        ReplyError(ctx, w.GetCookie(), errorCode, ss);
        w.Span.EndError(static_cast<const TString&>(ss));
    }

    for (const auto& w : Responses) {
        ReplyError(ctx, w.GetCookie(), errorCode, TStringBuilder() << ss << " (WriteResponses)");
    }

    for (const auto& ri : ReadInfo) {
        ReplyError(ctx, ri.second.Destination, errorCode,
            TStringBuilder() << ss << " (ReadInfo) cookie " << ri.first);
    }

    if (Mirrorer) {
        Send(Mirrorer->Actor, new TEvents::TEvPoisonPill());
    }

    if (UsersInfoStorage.Defined()) {
        UsersInfoStorage->Clear(ctx);
    }

    Send(ReadQuotaTrackerActor, new TEvents::TEvPoisonPill());
    if (!IsSupportive()) {
        Send(WriteQuotaTrackerActor, new TEvents::TEvPoisonPill());
    }

    if (OffloadActor) {
        Send(OffloadActor, new TEvents::TEvPoisonPill());
    }

    Die(ctx);
}

void TPartition::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx)
{
    DestroyActor(ctx);
}

bool CheckDiskStatus(const TStorageStatusFlags status) {
    return !status.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop);
}

void TPartition::InitComplete(const TActorContext& ctx) {
    if (BlobEncoder.StartOffset == BlobEncoder.EndOffset && BlobEncoder.EndOffset == 0) {
        for (auto&& [user, info] : UsersInfoStorage->ViewAll()) {
            if (info.Offset > 0 && BlobEncoder.StartOffset < (ui64)info.Offset) {
                 BlobEncoder.Head.Offset = BlobEncoder.EndOffset = BlobEncoder.StartOffset = info.Offset;
            }
        }
    }

    LOG_I("init complete for topic '" << TopicName() << "' partition " << Partition << " generation " << TabletGeneration << " " << ctx.SelfID);

    TStringBuilder ss;
    ss << "SYNC INIT topic " << TopicName() << " partitition " << Partition
       << " so " << GetStartOffset() << " endOffset " << GetEndOffset() << " Head " << BlobEncoder.Head << "\n";
    for (const auto& s : SourceIdStorage.GetInMemorySourceIds()) {
        ss << "SYNC INIT sourceId " << s.first << " seqNo " << s.second.SeqNo << " offset " << s.second.Offset << "\n";
    }
    for (const auto& h : CompactionBlobEncoder.DataKeysBody) {
        ss << "SYNC INIT DATA KEY: " << h.Key.ToString() << " size " << h.Size << "\n";
    }
    for (const auto& h : CompactionBlobEncoder.HeadKeys) {
        ss << "SYNC INIT HEAD KEY: " << h.Key.ToString() << " size " << h.Size << "\n";
    }
    for (const auto& h : BlobEncoder.DataKeysBody) {
        ss << "SYNC INIT DATA KEY: " << h.Key.ToString() << " size " << h.Size << "\n";
    }
    for (const auto& h : BlobEncoder.HeadKeys) {
        ss << "SYNC INIT HEAD KEY: " << h.Key.ToString() << " size " << h.Size << "\n";
    }
    LOG_D(ss);

    CompactionBlobEncoder.CheckHeadConsistency(CompactLevelBorder, TotalLevels, TotalMaxCount);

    Become(&TThis::StateIdle);
    InitDuration = ctx.Now() - CreationTime;
    InitDone = true;
    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_INIT].IncrementFor(InitDuration.MilliSeconds());

    CreateCompacter();
    InitializeMLPConsumers();

    InitUserInfoForImportantClients(ctx);

    FillReadFromTimestamps(ctx);
    ProcessPendingEvents(ctx);
    ProcessTxsAndUserActs(ctx);

    ctx.Send(ctx.SelfID, new TEvents::TEvWakeup());
    ctx.Send(TabletActorId, new TEvPQ::TEvInitComplete(Partition));

    for (const auto& s : SourceIdStorage.GetInMemorySourceIds()) {
        LOG_D("Init complete for topic '" << TopicName() << "' Partition: " << Partition
                    << " SourceId: " << s.first << " SeqNo: " << s.second.SeqNo << " offset: " << s.second.Offset
                    << " MaxOffset: " << GetEndOffset()
        );
    }
    ProcessHasDataRequests(ctx);


    for (auto&& userInfoPair : UsersInfoStorage->GetAll()) {
        PQ_ENSURE(userInfoPair.second.Offset >= 0);
        ReadTimestampForOffset(userInfoPair.first, userInfoPair.second, ctx);
    }
    if (PartitionCountersLabeled) {
        PartitionCountersLabeled->GetCounters()[METRIC_INIT_TIME] = InitDuration.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_LIFE_TIME] = CreationTime.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS] = 1;
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS_TOTAL] = Config.PartitionsSize();
        ctx.Send(TabletActorId, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
    UpdateUserInfoEndOffset(ctx.Now());

    ScheduleUpdateAvailableSize(ctx);

    if (MirroringEnabled(Config)) {
        CreateMirrorerActor();
    }

    ProcessMLPPendingEvents();

    ReportCounters(ctx, true);
}


void TPartition::UpdateUserInfoEndOffset(const TInstant& now) {
    for (auto&& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.UpdateReadingTimeAndState(GetEndOffset(), now);
    }
}

void TPartition::Handle(TEvPQ::TEvChangePartitionConfig::TPtr& ev, const TActorContext& ctx) {
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvPipeDisconnected::TPtr& ev, const TActorContext& ctx) {
    const TString& owner = ev->Get()->Owner;
    const TActorId& pipeClient = ev->Get()->PipeClient;

    OwnerPipes.erase(pipeClient);

    auto it = Owners.find(owner);
    if (it == Owners.end() || it->second.PipeClient != pipeClient) // owner session is already dead
        return;
    //TODO: Uncommet when writes will be done via new gRPC protocol
    // msgbus do not reserve bytes right now!!
    // DropOwner will drop reserved bytes and ownership
    if (owner != "default") { //default owner is for old LB protocol, pipe is dead right now after GetOwnership request, and no ReserveBytes done. So, ignore pipe disconnection
        DropOwner(it, ctx);
        ProcessChangeOwnerRequests(ctx);
    }

}

TConsumerSnapshot TPartition::CreateSnapshot(TUserInfo& userInfo) const {
    auto now = TAppData::TimeProvider->Now();

    userInfo.UpdateReadingTimeAndState(GetEndOffset(), now);

    TConsumerSnapshot result;
    result.Now = now;

    if (userInfo.Offset >= static_cast<i64>(GetEndOffset())) {
        result.LastCommittedMessage.CreateTimestamp = now;
        result.LastCommittedMessage.WriteTimestamp = now;
    } else if (userInfo.ActualTimestamps) {
        result.LastCommittedMessage.CreateTimestamp = userInfo.CreateTimestamp;
        result.LastCommittedMessage.WriteTimestamp = userInfo.WriteTimestamp;
    } else {
        auto timestamp = GetWriteTimeEstimate(userInfo.Offset);
        result.LastCommittedMessage.CreateTimestamp = timestamp;
        result.LastCommittedMessage.WriteTimestamp = timestamp;
    }

    auto readOffset = userInfo.GetReadOffset();

    result.ReadOffset = readOffset;
    result.LastReadTimestamp = userInfo.ReadTimestamp;

    if (readOffset >= static_cast<i64>(GetEndOffset())) {
        result.LastReadMessage.CreateTimestamp = now;
        result.LastReadMessage.WriteTimestamp = now;
    } else if (userInfo.ReadOffset == -1) {
        result.LastReadMessage = result.LastCommittedMessage;
    } else if (userInfo.ReadWriteTimestamp) {
        result.LastReadMessage.CreateTimestamp = userInfo.ReadCreateTimestamp;
        result.LastReadMessage.WriteTimestamp = userInfo.ReadWriteTimestamp;
    } else {
        auto timestamp = GetWriteTimeEstimate(readOffset);
        result.LastReadMessage.CreateTimestamp = timestamp;
        result.LastReadMessage.WriteTimestamp = timestamp;
    }

    if (readOffset < (i64)GetEndOffset()) {
        result.ReadLag = result.LastReadTimestamp - result.LastReadMessage.WriteTimestamp;
    }
    result.CommitedLag = now - result.LastCommittedMessage.WriteTimestamp;
    result.TotalLag = TDuration::MilliSeconds(userInfo.GetWriteLagMs()) + result.ReadLag + (now - result.LastReadTimestamp);

    return result;
}

void TPartition::Handle(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    const auto now = ctx.Now();

    NKikimrPQ::TStatusResponse::TPartResult result;
    result.SetPartition(Partition.InternalPartitionId);
    result.SetGeneration(TabletGeneration);
    result.SetCookie(++PQRBCookie);

    if (DiskIsFull || WaitingForSubDomainQuota()) {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_DISK_IS_FULL);
    } else if (GetEndOffset() - GetStartOffset() >= static_cast<ui64>(Config.GetPartitionConfig().GetMaxCountInPartition()) ||
               Size() >= static_cast<ui64>(Config.GetPartitionConfig().GetMaxSizeInPartition())) {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_PARTITION_IS_FULL);
    } else {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
    }
    result.SetLastInitDurationSeconds(InitDuration.Seconds());
    result.SetCreationTimestamp(CreationTime.Seconds());
    ui64 headGapSize = BlobEncoder.GetHeadGapSize();
    ui32 gapsCount = GapOffsets.size() + (headGapSize ? 1 : 0);
    result.SetGapCount(gapsCount);
    result.SetGapSize(headGapSize + GapSize);

    PQ_ENSURE(AvgWriteBytes.size() == 4);
    result.SetAvgWriteSpeedPerSec(AvgWriteBytes[0].GetValue());
    result.SetAvgWriteSpeedPerMin(AvgWriteBytes[1].GetValue());
    result.SetAvgWriteSpeedPerHour(AvgWriteBytes[2].GetValue());
    result.SetAvgWriteSpeedPerDay(AvgWriteBytes[3].GetValue());

    PQ_ENSURE(AvgQuotaBytes.size() == 4);
    result.SetAvgQuotaSpeedPerSec(AvgQuotaBytes[0].GetValue());
    result.SetAvgQuotaSpeedPerMin(AvgQuotaBytes[1].GetValue());
    result.SetAvgQuotaSpeedPerHour(AvgQuotaBytes[2].GetValue());
    result.SetAvgQuotaSpeedPerDay(AvgQuotaBytes[3].GetValue());

    result.SetSourceIdCount(SourceIdStorage.GetInMemorySourceIds().size());
    result.SetSourceIdRetentionPeriodSec((now - SourceIdStorage.MinAvailableTimestamp(now)).Seconds());

    result.SetWriteBytesQuota(TotalPartitionWriteSpeed);

    TVector<ui64> resSpeed;
    resSpeed.resize(4);
    ui64 maxQuota = 0;
    bool filterConsumers = !ev->Get()->Consumers.empty();
    const auto& clientId = ev->Get()->ClientId;
    TSet<TString> requiredConsumers(ev->Get()->Consumers.begin(), ev->Get()->Consumers.end());
    for (auto&& [_, userInfo] : UsersInfoStorage->GetAll()) {
        bool consumerShouldBeProcessed = filterConsumers
            ? requiredConsumers.contains(userInfo.User)
            : clientId.empty() || clientId == userInfo.User;
        if (consumerShouldBeProcessed) {
            PQ_ENSURE(userInfo.AvgReadBytes.size() == 4);
            for (ui32 i = 0; i < 4; ++i) {
                resSpeed[i] += userInfo.AvgReadBytes[i].GetValue();
            }
            maxQuota += userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get();
        }
        if (filterConsumers) {
            if (requiredConsumers.contains(userInfo.User)) {
                auto* clientInfo = result.AddConsumerResult();
                clientInfo->SetConsumer(userInfo.User);
                clientInfo->set_errorcode(NPersQueue::NErrorCode::EErrorCode::OK);
                clientInfo->SetCommitedOffset(userInfo.Offset);
                if (userInfo.CommittedMetadata.has_value()) {
                    clientInfo->SetCommittedMetadata(*userInfo.CommittedMetadata);
                }
                requiredConsumers.extract(userInfo.User);
            }
            continue;
        }

        if (clientId == userInfo.User) { //fill lags
            NKikimrPQ::TClientInfo* clientInfo = result.MutableLagsInfo();
            clientInfo->SetClientId(userInfo.User);

            const auto snapshot = CreateSnapshot(userInfo);

            auto write = clientInfo->MutableWritePosition();
            write->SetOffset(userInfo.Offset);
            write->SetWriteTimestamp(snapshot.LastCommittedMessage.WriteTimestamp.MilliSeconds());
            write->SetCreateTimestamp(snapshot.LastCommittedMessage.CreateTimestamp.MilliSeconds());
            write->SetSize(GetSizeLag(userInfo.Offset));

            auto readOffset = userInfo.GetReadOffset();

            auto read = clientInfo->MutableReadPosition();
            read->SetOffset(readOffset);
            read->SetWriteTimestamp(snapshot.LastReadMessage.WriteTimestamp.MilliSeconds());
            read->SetCreateTimestamp(snapshot.LastReadMessage.CreateTimestamp.MilliSeconds());
            read->SetSize(GetSizeLag(readOffset));

            clientInfo->SetLastReadTimestampMs(snapshot.LastReadTimestamp.MilliSeconds());
            clientInfo->SetCommitedLagMs(snapshot.CommitedLag.MilliSeconds());
            if (IsActive() || readOffset < (i64)GetEndOffset()) {
                clientInfo->SetReadLagMs(snapshot.ReadLag.MilliSeconds());
                clientInfo->SetWriteLagMs(userInfo.GetWriteLagMs());
                clientInfo->SetTotalLagMs(snapshot.TotalLag.MilliSeconds());
            } else {
                clientInfo->SetReadLagMs(0);
                clientInfo->SetWriteLagMs(0);
                clientInfo->SetTotalLagMs(0);
            }
        }

        if (ev->Get()->GetStatForAllConsumers) { //fill lags
            const auto snapshot = CreateSnapshot(userInfo);

            auto* clientInfo = result.AddConsumerResult();
            clientInfo->SetConsumer(userInfo.User);
            clientInfo->SetLastReadTimestampMs(userInfo.GetReadTimestamp().MilliSeconds());
            clientInfo->SetCommitedLagMs(snapshot.CommitedLag.MilliSeconds());

            auto readOffset = userInfo.GetReadOffset();
            if (IsActive() || readOffset < (i64)GetEndOffset()) {
                clientInfo->SetReadLagMs(snapshot.ReadLag.MilliSeconds());
                clientInfo->SetWriteLagMs(userInfo.GetWriteLagMs());
            } else {
                clientInfo->SetReadLagMs(0);
                clientInfo->SetWriteLagMs(0);
            }

            clientInfo->SetAvgReadSpeedPerMin(userInfo.AvgReadBytes[1].GetValue());
            clientInfo->SetAvgReadSpeedPerHour(userInfo.AvgReadBytes[2].GetValue());
            clientInfo->SetAvgReadSpeedPerDay(userInfo.AvgReadBytes[3].GetValue());

            auto lastOffsetHasBeenCommited = LastOffsetHasBeenCommited(userInfo);
            clientInfo->SetReadingFinished(lastOffsetHasBeenCommited);

            auto mit = MLPConsumers.find(userInfo.User);
            if (mit != MLPConsumers.end()) {
                clientInfo->SetUseForReading(mit->second.UseForReading);
            }
        }
    }

    result.SetStartOffset(GetStartOffset());
    result.SetEndOffset(GetEndOffset());

    if (filterConsumers) {
        for (TString consumer : requiredConsumers) {
            auto* clientInfo = result.AddConsumerResult();
            clientInfo->SetConsumer(consumer);
            clientInfo->set_errorcode(NPersQueue::NErrorCode::EErrorCode::SCHEMA_ERROR);
        }
    } else {
        result.SetAvgReadSpeedPerSec(resSpeed[0]);
        result.SetAvgReadSpeedPerMin(resSpeed[1]);
        result.SetAvgReadSpeedPerHour(resSpeed[2]);
        result.SetAvgReadSpeedPerDay(resSpeed[3]);

        result.SetReadBytesQuota(maxQuota);

        result.SetPartitionSize(UserDataSize());
        result.SetUsedReserveSize(UsedReserveSize(ctx));

        result.SetLastWriteTimestampMs(WriteTimestamp.MilliSeconds());
        result.SetWriteLagMs(WriteLagMs.GetValue());

        *result.MutableErrors() = {Errors.begin(), Errors.end()};

        LOG_D("Topic PartitionStatus PartitionSize: " << result.GetPartitionSize()
                    << " UsedReserveSize: " << result.GetUsedReserveSize()
                    << " ReserveSize: " << ReserveSize()
                    << " PartitionConfig" << Config.GetPartitionConfig();
        );
    }

    UpdateCounters(ctx);
    if (PartitionCountersLabeled) {
        auto* ac = result.MutableAggregatedCounters();
        for (ui32 i = 0; i < PartitionCountersLabeled->GetCounters().Size(); ++i) {
            ac->AddValues(PartitionCountersLabeled->GetCounters()[i].Get());
        }
        if (PartitionKeyCompactionCounters) {
            for (ui32 i = 0; i < PartitionKeyCompactionCounters->GetCounters().Size(); ++i) {
                ac->MutableCompactionCounters()->AddValues(PartitionKeyCompactionCounters->GetCounters()[i].Get());
            }
        }
        if (PartitionCountersExtended) {
            for (ui32 i = 0; i < PartitionCountersExtended->GetCounters().Size(); ++i) {
                ac->MutableExtendedCounters()->AddValues((PartitionCountersExtended->GetCounters()[i].Get()));
            }
        }


        for (auto&& userInfoPair : UsersInfoStorage->ViewAll()) {

            auto& userInfo = userInfoPair.second;
            if (!userInfo.LabeledCounters)
                continue;
            if (userInfoPair.first != CLIENTID_WITHOUT_CONSUMER && !userInfo.HasReadRule && !ImporantOrExtendedAvailabilityPeriod(userInfo))
                continue;
            auto* cac = ac->AddConsumerAggregatedCounters();
            cac->SetConsumer(userInfo.User);
            for (ui32 i = 0; i < userInfo.LabeledCounters->GetCounters().Size(); ++i) {
                cac->AddValues(userInfo.LabeledCounters->GetCounters()[i].Get());
            }
        }
    }
    if (SplitMergeEnabled(TabletConfig)) {
        if (PartitionScaleParticipants.Defined()) {
            result.MutableScaleParticipatingPartitions()->CopyFrom(*PartitionScaleParticipants);
        }
        if (SplitBoundary.Defined()) {
            result.SetSplitBoundary(*SplitBoundary);
        }
        result.SetScaleStatus(ScaleStatus);
    } else {
        result.SetScaleStatus(NKikimrPQ::EScaleStatus::NORMAL);
    }

    for (const auto& [_, consumer] : MLPConsumers) {
        result.MutableAggregatedCounters()->AddMLPConsumerCounters()->CopyFrom(consumer.Metrics);
    }

    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result, Partition));
}

void TPartition::Handle(TEvPQ::TEvPartitionScaleStatusChanged::TPtr& ev, const TActorContext& ctx)
{
    const bool mirroredPartition = MirroringEnabled(Config);
    const NKikimrPQ::TEvPartitionScaleStatusChanged& record = ev->Get()->Record;
    if (mirroredPartition) {
        if (record.HasParticipatingPartitions()) [[likely]] {
            LOG_I("Got split-merge event from mirrorer: " << ev->ToString());

            ScaleStatus = record.GetScaleStatus();
            PartitionScaleParticipants.ConstructInPlace();
            PartitionScaleParticipants->CopyFrom(record.GetParticipatingPartitions());
            ctx.Send(TabletActorId, ev->Release());
        } else {
            LOG_W("Ignoring split-merge event from the mirrorer because it does not have participating partitions info: " << ev->ToString());
        }
    } else {
        LOG_W("Ignoring split-merge event because mirroring is disabled: " << ev->ToString());
    }
}

void TPartition::HandleOnInit(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TPartResult result;
    result.SetPartition(Partition.InternalPartitionId);
    result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_INITIALIZING);
    result.SetLastInitDurationSeconds((ctx.Now() - CreationTime).Seconds());
    result.SetCreationTimestamp(CreationTime.Seconds());
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result, Partition));
}


void TPartition::Handle(TEvPQ::TEvGetPartitionClientInfo::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPersQueue::TEvPartitionClientInfoResponse> response = MakeHolder<TEvPersQueue::TEvPartitionClientInfoResponse>();
    NKikimrPQ::TClientInfoResponse& result(response->Record);
    result.SetPartition(Partition.InternalPartitionId);
    result.SetStartOffset(GetStartOffset());
    result.SetEndOffset(GetEndOffset());
    result.SetResponseTimestamp(ctx.Now().MilliSeconds());
    for (auto&& pr : UsersInfoStorage->GetAll()) {
        const auto snapshot = CreateSnapshot(pr.second);

        const TUserInfo& userInfo(pr.second);
        NKikimrPQ::TClientInfo& clientInfo = *result.AddClientInfo();
        clientInfo.SetClientId(pr.first);

        auto& write = *clientInfo.MutableWritePosition();
        write.SetOffset(userInfo.Offset);
        write.SetWriteTimestamp(snapshot.LastCommittedMessage.WriteTimestamp.MilliSeconds());
        write.SetCreateTimestamp(snapshot.LastCommittedMessage.CreateTimestamp.MilliSeconds());
        write.SetSize(GetSizeLag(userInfo.Offset));

        auto& read = *clientInfo.MutableReadPosition();
        read.SetOffset(userInfo.GetReadOffset());
        read.SetWriteTimestamp(snapshot.LastReadMessage.WriteTimestamp.MilliSeconds());
        read.SetCreateTimestamp(snapshot.LastReadMessage.CreateTimestamp.MilliSeconds());
        read.SetSize(GetSizeLag(userInfo.GetReadOffset()));
    }
    ctx.Send(ev->Get()->Sender, response.Release(), 0, ev->Cookie);
}

void TPartition::Handle(TEvPersQueue::TEvReportPartitionError::TPtr& ev, const TActorContext& ctx) {
    LogAndCollectError(ev->Get()->Record, ctx);
}

void TPartition::LogAndCollectError(const NKikimrPQ::TStatusResponse::TErrorMessage& error, const TActorContext& ctx) {
    if (Errors.size() == MAX_ERRORS_COUNT_TO_STORE) {
        Errors.pop_front();
    }
    Errors.push_back(error);
    LOG_ERROR_S(ctx, error.GetService(), error.GetMessage());
}

void TPartition::LogAndCollectError(NKikimrServices::EServiceKikimr service, const TString& msg, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TErrorMessage error;
    error.SetTimestamp(ctx.Now().Seconds());
    error.SetService(service);
    error.SetMessage(TStringBuilder() << "topic '" << TopicName() << "' partition " << Partition << " got error: " << msg);
    LogAndCollectError(error, ctx);
}

const TPartitionBlobEncoder& TPartition::GetBlobEncoder(ui64 offset) const
{
    if ((offset >= CompactionBlobEncoder.EndOffset) && (offset < BlobEncoder.StartOffset)) {
        offset = BlobEncoder.StartOffset;
    }

    if (BlobEncoder.DataKeysBody.empty()) {
        return CompactionBlobEncoder;
    }

    const auto required = std::make_tuple(offset, 0);
    const auto& key = BlobEncoder.DataKeysBody.front().Key;
    const auto fastWriteStart = std::make_tuple(key.GetOffset(), key.GetPartNo());

    if (required < fastWriteStart) {
        return CompactionBlobEncoder;
    }

    return BlobEncoder;
}

const std::deque<TDataKey>& GetContainer(const TPartitionBlobEncoder& zone, ui64 offset)
{
    return zone.PositionInBody(offset, 0) ? zone.DataKeysBody : zone.HeadKeys;
}

//zero means no such record
TInstant TPartition::GetWriteTimeEstimate(ui64 offset) const {
    if (offset < GetStartOffset()) {
        offset = GetStartOffset();
    }
    if (offset >= GetEndOffset()) {
        return TInstant::Zero();
    }

    const TPartitionBlobEncoder& blobEncoder = GetBlobEncoder(offset);
    offset = Max(offset, blobEncoder.StartOffset);
    const std::deque<TDataKey>& container = GetContainer(blobEncoder, offset);
    PQ_ENSURE(!container.empty())
        ("offset", offset)
        ("cz.StartOffset", CompactionBlobEncoder.StartOffset)("cz.EndOffset", CompactionBlobEncoder.EndOffset)
        ("fwz.StartOffset", BlobEncoder.StartOffset)("fwz.EndOffset", BlobEncoder.EndOffset)
        ;

    auto it = std::upper_bound(container.begin(), container.end(), offset,
                    [](const ui64 offset, const TDataKey& p) {
                        return offset < p.Key.GetOffset() ||
                                        offset == p.Key.GetOffset() && p.Key.GetPartNo() > 0;
                    });
    // Always greater
    PQ_ENSURE(it != container.begin())
        ("StartOffset", blobEncoder.StartOffset)("HeadOffset", blobEncoder.Head.Offset)
        ("offset", offset)
        ("containter size", container.size())("first-elem", container.front().Key.ToString())
        ("is-fast-write", blobEncoder.ForFastWrite);
    PQ_ENSURE(it == container.end() ||
                   offset < it->Key.GetOffset() ||
                   it->Key.GetOffset() == offset && it->Key.GetPartNo() > 0);

    --it;
    if (it != container.begin())
        --it;

    return it->Timestamp;
}


void TPartition::Handle(TEvPQ::TEvUpdateWriteTimestamp::TPtr& ev, const TActorContext& ctx) {
    TInstant timestamp = TInstant::MilliSeconds(ev->Get()->WriteTimestamp);
    if (WriteTimestampEstimate > timestamp) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "too big timestamp: " << timestamp << " known " << WriteTimestampEstimate);
        return;
    }
    WriteTimestampEstimate = timestamp;
    ReplyOk(ctx, ev->Get()->Cookie);
}

void TPartition::Handle(TEvPersQueue::TEvProposeTransaction::TPtr& ev, const TActorContext& ctx)
{
    const NKikimrPQ::TEvProposeTransaction& event = ev->Get()->GetRecord();
    PQ_ENSURE(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    PQ_ENSURE(event.HasData());
    const NKikimrPQ::TDataTransaction& txBody = event.GetData();

    if (!txBody.GetImmediate()) {
        ReplyPropose(ctx,
                     event,
                     NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                     NKikimrPQ::TError::INTERNAL,
                     "immediate transaction is expected");
        return;
    }

    if (ImmediateTxCount >= MAX_TXS) {
        ReplyPropose(ctx,
                     event,
                     NKikimrPQ::TEvProposeTransactionResult::OVERLOADED,
                     NKikimrPQ::TError::INTERNAL,
                     "the allowed number of transactions has been exceeded");
        return;
    }
    AddImmediateTx(ev->Release());
    ProcessTxsAndUserActs(ctx);
}

template <class T>
void TPartition::ProcessPendingEvent(TAutoPtr<TEventHandle<T>>& ev, const TActorContext& ctx)
{
    if (PendingEvents.empty()) {
        // Optimization: if the queue is empty, you can process the message immediately
        ProcessPendingEvent(std::unique_ptr<T>(ev->Release().Release()), ctx);
    } else {
        // We need to keep the order in which the messages arrived
        AddPendingEvent(ev);
        ProcessPendingEvents(ctx);
    }
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvProposePartitionConfig> ev, const TActorContext& ctx)
{
    PushBackDistrTx(ev.release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx)
{
    LOG_D("Handle TEvPQ::TEvProposePartitionConfig" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    ProcessPendingEvent(ev, ctx);
}

template <class T>
void TPartition::AddPendingEvent(TAutoPtr<TEventHandle<T>>& ev)
{
    std::unique_ptr<T> p(ev->Release().Release());
    PendingEvents.emplace_back(std::move(p));
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext&)
{
    LOG_D("HandleOnInit TEvPQ::TEvTxCalcPredicate");

    AddPendingEvent(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext&)
{
    LOG_D("HandleOnInit TEvPQ::TEvTxCommit");

    AddPendingEvent(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext&)
{
    LOG_D("HandleOnInit TEvPQ::TEvTxRollback");

    AddPendingEvent(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext&)
{
    LOG_D("HandleOnInit TEvPQ::TEvProposePartitionConfig");

    AddPendingEvent(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvGetWriteInfoRequest::TPtr& ev, const TActorContext& /* ctx */)
{
    LOG_D("HandleOnInit TEvPQ::TEvGetWriteInfoRequest");

    PQ_ENSURE(IsSupportive());

    ev->Get()->OriginalPartition = ev->Sender;
    AddPendingEvent(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvGetWriteInfoResponse::TPtr& ev, const TActorContext& /* ctx */)
{
    LOG_D("HandleOnInit TEvPQ::TEvGetWriteInfoResponse");

    PQ_ENSURE(!IsSupportive());

    AddPendingEvent(ev);
}

void TPartition::HandleOnInit(TEvPQ::TEvGetWriteInfoError::TPtr& ev, const TActorContext& /* ctx */)
{
    LOG_D("HandleOnInit TEvPQ::TEvGetWriteInfoError");

    PQ_ENSURE(!IsSupportive());

    AddPendingEvent(ev);
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvTxCalcPredicate> ev, const TActorContext& ctx)
{
    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(*ev) < GetStepAndTxId(*PlanStep, *TxId)) {
            Send(TabletActorId,
                 MakeHolder<TEvPQ::TEvTxCalcPredicateResult>(ev->Step,
                                                             ev->TxId,
                                                             Partition,
                                                             Nothing(),
                                                             TString()).Release());
            return;
        }
    }

    PushBackDistrTx(ev.release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx)
{
    LOG_D("Handle TEvPQ::TEvTxCalcPredicate" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    ev->Get()->Span = NWilson::TSpan(TWilsonTopic::TopicTopLevel,
                                     std::move(ev->TraceId),
                                     "Topic.Partition.CalcPredicate",
                                     NWilson::EFlags::AUTO_END);
    auto span = ev->Get()->Span.CreateChild(TWilsonTopic::TopicTopLevel,
                                            "Topic.Partition.ProcessEvent",
                                            NWilson::EFlags::AUTO_END);

    ProcessPendingEvent(ev, ctx);
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvTxCommit> ev, const TActorContext& ctx)
{
    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(*ev) < GetStepAndTxId(*PlanStep, *TxId)) {
            LOG_D("Send TEvTxDone (commit)" <<
                     " Step " << ev->Step <<
                     ", TxId " << ev->TxId);
            ctx.Send(TabletActorId, MakeTxDone(ev->Step, ev->TxId).Release());
            return;
        }
    }

    PQ_ENSURE(!TransactionsInflight.empty())("Step", ev->Step)("TxId",  ev->TxId);

    auto txIter = TransactionsInflight.find(ev->TxId);
    PQ_ENSURE(!txIter.IsEnd())("Step", ev->Step)("TxId",  ev->TxId);
    auto& tx = txIter->second;

    PQ_ENSURE(tx->State == ECommitState::Pending);

    tx->State = ECommitState::Committed;
    tx->ExplicitMessageGroups = std::move(ev->ExplicitMessageGroups);
    tx->SerializedTx = std::move(ev->SerializedTx);
    tx->TabletConfig = std::move(ev->TabletConfig);
    tx->BootstrapConfig = std::move(ev->BootstrapConfig);
    tx->PartitionsData = std::move(ev->PartitionsData);

    if (!tx->ChangeConfig && !tx->ProposeConfig) {
        tx->CommitSpan = std::move(ev->Span);
    }

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx)
{
    LOG_D("Handle TEvPQ::TEvTxCommit" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    ev->Get()->Span = NWilson::TSpan(TWilsonTopic::TopicTopLevel,
                                     std::move(ev->TraceId),
                                     "Topic.Partition.Commit",
                                     NWilson::EFlags::AUTO_END);
    auto span = ev->Get()->Span.CreateChild(TWilsonTopic::TopicTopLevel,
                                            "Topic.Partition.ProcessEvent",
                                            NWilson::EFlags::AUTO_END);

    ProcessPendingEvent(ev, ctx);
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvTxRollback> ev, const TActorContext& ctx)
{
    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(*ev) < GetStepAndTxId(*PlanStep, *TxId)) {
            LOG_D("Send TEvTxDone (rollback)" <<
                     " Step " << ev->Step <<
                     ", TxId " << ev->TxId);
            ctx.Send(TabletActorId, MakeTxDone(ev->Step, ev->TxId).Release());
            return;
        }
    }

    auto txIter = TransactionsInflight.begin();
    if (ChangeConfig) {
        PQ_ENSURE(TransactionsInflight.size() == 1);
    } else {
        PQ_ENSURE(!TransactionsInflight.empty());
        txIter = TransactionsInflight.find(ev->TxId);
        PQ_ENSURE(!txIter.IsEnd());
    }
    auto& tx = txIter->second;

    PQ_ENSURE(tx->State == ECommitState::Pending);

    tx->State = ECommitState::Aborted;
    tx->SerializedTx = std::move(ev->SerializedTx);

    if (!tx->ChangeConfig && !tx->ProposeConfig) {
        tx->CommitSpan = std::move(ev->Span);
    }

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx)
{
    LOG_D("Handle TEvPQ::TEvTxRollback" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    ProcessPendingEvent(ev, ctx);
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvGetWriteInfoRequest> ev, const TActorContext& ctx)
{
    TActorId originalPartition = ev->OriginalPartition;
    PQ_ENSURE(originalPartition != TActorId());

    if (ClosedInternalPartition || WaitingForPreviousBlobQuota() || (CurrentStateFunc() != &TThis::StateIdle)) {
        LOG_D("Send TEvPQ::TEvGetWriteInfoError");
        auto* response = new TEvPQ::TEvGetWriteInfoError(Partition.InternalPartitionId,
                                                         "Write info requested while writes are not complete");
        ctx.Send(originalPartition, response);
        ClosedInternalPartition = true;
        return;
    }
    ClosedInternalPartition = true;
    auto response = new TEvPQ::TEvGetWriteInfoResponse();
    response->Cookie = Partition.InternalPartitionId;
    response->BodyKeys = std::move(CompactionBlobEncoder.DataKeysBody);
    std::move(CompactionBlobEncoder.HeadKeys.begin(), CompactionBlobEncoder.HeadKeys.end(),
              std::back_inserter(response->BodyKeys));
    std::move(BlobEncoder.DataKeysBody.begin(), BlobEncoder.DataKeysBody.end(),
              std::back_inserter(response->BodyKeys));
    response->SrcIdInfo = std::move(SourceIdStorage.ExtractInMemorySourceIds());

    response->BytesWrittenGrpc = BytesWrittenGrpc.Value();
    response->BytesWrittenUncompressed = BytesWrittenUncompressed.Value();
    response->BytesWrittenTotal = BytesWrittenTotal.Value();
    response->MessagesWrittenTotal = MsgsWrittenTotal.Value();
    response->MessagesWrittenGrpc = MsgsWrittenGrpc.Value();
    response->MessagesSizes = std::move(MessageSize.GetValues());
    response->InputLags = std::move(SupportivePartitionTimeLag);
    response->WrittenBytes = AutopartitioningManager->GetWrittenBytes();

    LOG_D("Send TEvPQ::TEvGetWriteInfoResponse");
    ctx.Send(originalPartition, response);
}

void TPartition::Handle(TEvPQ::TEvGetWriteInfoRequest::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Handle TEvPQ::TEvGetWriteInfoRequest");

    ev->Get()->OriginalPartition = ev->Sender;
    ev->Get()->Span = NWilson::TSpan(TWilsonTopic::TopicTopLevel,
                                     std::move(ev->TraceId),
                                     "Topic.Partition.GetWriteInfo",
                                     NWilson::EFlags::AUTO_END);

    StopCompaction = true;
    if (CompactionInProgress) {
        LOG_D("Event TEvPQ::TEvGetWriteInfoRequest will be processed later");
        PendingGetWriteInfoRequest.reset(ev->Release().Release());
        return;
    }

    ProcessPendingEvent(ev, ctx);
}

void TPartition::WriteInfoResponseHandler(
        const TActorId& sender,
        TGetWriteInfoResp&& ev,
        const TActorContext& ctx
) {
    auto txIter = WriteInfosToTx.find(sender);
    PQ_ENSURE(!txIter.IsEnd());

    auto& tx = (*txIter->second);

    PQ_LOG_TX_D("Received TEvGetWriteInfoResponse for TxId " << tx.GetTxId());

    tx.GetWriteInfoSpan.End();
    tx.GetWriteInfoSpan = {};

    tx.WriteInfoResponseTimestamp = Now();

    std::visit(TOverloaded{
        [&tx](TAutoPtr<TEvPQ::TEvGetWriteInfoResponse>& msg) {
            tx.WriteInfo.Reset(msg.Release());
        },
        [&tx](TAutoPtr<TEvPQ::TEvGetWriteInfoError>& err) {
            tx.Predicate = false;
            tx.WriteInfoApplied = true;
            tx.Message = err->Message;
        }
    }, ev);

    WriteInfosToTx.erase(txIter);
    ProcessTxsAndUserActs(ctx);
}

TPartition::EProcessResult TPartition::ApplyWriteInfoResponse(TTransaction& tx,
                                                              TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    bool isImmediate = (tx.ProposeTransaction != nullptr);
    PQ_ENSURE(tx.WriteInfo);
    PQ_ENSURE(!tx.WriteInfoApplied);
    if (!tx.Predicate.GetOrElse(true)) {
        return EProcessResult::Continue;
    }

    if (!CanWrite()) {
        tx.Predicate = false;
        tx.Message = TStringBuilder() << "Partition " << Partition << " is inactive. Writing is not possible";
        tx.WriteInfoApplied = true;
        return EProcessResult::Continue;
    }

    auto& srcIdInfo = tx.WriteInfo->SrcIdInfo;

    EProcessResult ret = EProcessResult::Continue;
    const auto& knownSourceIds = SourceIdStorage.GetInMemorySourceIds();
    TVector<TString> txSourceIds;
    for (const auto& s : srcIdInfo) {
        if (TxAffectedSourcesIds.contains(s.first)) {
            PQ_LOG_TX_D("TxAffectedSourcesIds contains SourceId " << s.first << ". TxId " << tx.GetTxId());
            ret = EProcessResult::Blocked;
            break;
        }
        if (isImmediate) {
            txSourceIds.push_back(s.first);
            LOG_D("TxId " << tx.GetTxId() << " affect SourceId " << s.first);
        } else {
            if (WriteAffectedSourcesIds.contains(s.first)) {
                PQ_LOG_TX_D("WriteAffectedSourcesIds contains SourceId " << s.first << ". TxId " << tx.GetTxId());
                ret = EProcessResult::Blocked;
                break;
            }
            txSourceIds.push_back(s.first);
            LOG_D("TxId " << tx.GetTxId() << " affect SourceId " << s.first);
        }

        if (auto inFlightIter = TxInflightMaxSeqNoPerSourceId.find(s.first); !inFlightIter.IsEnd()) {
            if (SeqnoViolation(inFlightIter->second.KafkaProducerEpoch, inFlightIter->second.SeqNo, s.second.ProducerEpoch, s.second.MinSeqNo)) {
                LOG_W("MinSeqNo violation failure. " <<
                         "TxId=" << tx.GetTxId() <<
                         ", SourceId=" << s.first <<
                         ", SeqNo=" << inFlightIter->second.SeqNo <<
                         ", MinSeqNo=" << s.second.MinSeqNo);
                tx.Predicate = false;
                tx.Message = (MakeTxWriteErrorMessage(tx.GetTxId(), TopicName(), Partition, s.first, inFlightIter->second.SeqNo) << "MinSeqNo violation failure. " <<
                              "SeqNo " << s.second.MinSeqNo);
                tx.WriteInfoApplied = true;
                break;
            }
        }

        if (auto existing = knownSourceIds.find(s.first); !existing.IsEnd()) {
            if (SeqnoViolation(existing->second.ProducerEpoch, existing->second.SeqNo, s.second.ProducerEpoch, s.second.MinSeqNo)) {
                LOG_W("MinSeqNo violation failure. " <<
                         "TxId=" << tx.GetTxId() <<
                         ", SourceId=" << s.first <<
                         ", SeqNo=" << existing->second.SeqNo <<
                         ", MinSeqNo=" << s.second.MinSeqNo);
                tx.Predicate = false;
                tx.Message = (MakeTxWriteErrorMessage(tx.GetTxId(), TopicName(), Partition, s.first, existing->second.SeqNo) << "MinSeqNo violation failure. " <<
                              "SeqNo " << s.second.MinSeqNo);
                tx.WriteInfoApplied = true;
                break;
            }
        }
    }

    if (ret == EProcessResult::Continue && tx.Predicate.GetOrElse(true)) {
        auto& sourceIds =
            (isImmediate ? affectedSourceIdsAndConsumers.WriteSourcesIds : affectedSourceIdsAndConsumers.TxWriteSourcesIds);
        sourceIds = std::move(txSourceIds);

        tx.WriteInfoApplied = true;

        affectedSourceIdsAndConsumers.WriteKeysSize += tx.WriteInfo->BodyKeys.size();
        affectedSourceIdsAndConsumers.WriteKeysSize += tx.WriteInfo->SrcIdInfo.size();
    }

    return ret;
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvGetWriteInfoResponse> ev, const TActorContext& ctx)
{
    const auto sender = ev->SupportivePartition;
    WriteInfoResponseHandler(sender, ev.release(), ctx);
}

void TPartition::Handle(TEvPQ::TEvGetWriteInfoResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Handle TEvPQ::TEvGetWriteInfoResponse");

    ev->Get()->SupportivePartition = ev->Sender;

    ProcessPendingEvent(ev, ctx);
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvGetWriteInfoError> ev, const TActorContext& ctx)
{
    const auto sender = ev->SupportivePartition;
    WriteInfoResponseHandler(sender, ev.release(), ctx);
}

void TPartition::Handle(TEvPQ::TEvGetWriteInfoError::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Handle TEvPQ::TEvGetWriteInfoError " <<
             "Cookie " << ev->Get()->Cookie <<
             ", Message " << ev->Get()->Message);

    ev->Get()->SupportivePartition = ev->Sender;

    ProcessPendingEvent(ev, ctx);
}

void TPartition::ReplyToProposeOrPredicate(TSimpleSharedPtr<TTransaction>& tx, bool isPredicate) {

    if (isPredicate) {
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_TXCALCPREDICATE].IncrementFor((Now() - tx->CalcPredicateTimestamp).MilliSeconds());

        tx->CalcPredicateSpan.End();
        tx->CalcPredicateSpan = {};

        auto insRes = TransactionsInflight.emplace(tx->Tx->TxId, tx);
        PQ_ENSURE(insRes.second);

        if ((Now() - tx->WriteInfoResponseTimestamp) >= TDuration::Seconds(1)) {
            PQ_LOG_TX_D("The long answer to TEvTxCalcPredicate. TxId: " << tx->GetTxId());
        }

        PQ_LOG_TX_D("Send TEvTxCalcPredicateResult. TxId: " << tx->GetTxId());

        Send(TabletActorId,
             MakeHolder<TEvPQ::TEvTxCalcPredicateResult>(tx->Tx->Step,
                                                         tx->Tx->TxId,
                                                         Partition,
                                                         *tx->Predicate,
                                                         tx->Message).Release());
    } else {
        auto insRes = TransactionsInflight.emplace(tx->ProposeConfig->TxId, tx);
        PQ_ENSURE(insRes.second);

        auto result = MakeHolder<TEvPQ::TEvProposePartitionConfigResult>(tx->ProposeConfig->Step,
                                                                tx->ProposeConfig->TxId,
                                                                Partition);

        result->Data.SetPartitionId(Partition.OriginalPartitionId);
        for (auto& [id, v] : SourceIdStorage.GetInMemorySourceIds()) {
            if (v.Explicit) {
                auto* m = result->Data.AddMessageGroup();
                m->SetId(id);
                m->SetSeqNo(v.SeqNo);
            }
        }
        Send(TabletActorId, result.Release());
    }
}

void TPartition::Handle(TEvPQ::TEvGetMaxSeqNoRequest::TPtr& ev, const TActorContext& ctx) {
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(ev->Get()->Cookie, false);
    NKikimrClient::TResponse& resp = *response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto& result = *resp.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult();
    result.SetIsPartitionActive(IsActive());
    for (const auto& sourceId : ev->Get()->SourceIds) {
        auto& protoInfo = *result.AddSourceIdInfo();
        protoInfo.SetSourceId(sourceId);

        auto info = SourceManager.Get(sourceId);

        PQ_ENSURE(info.Offset <= (ui64)Max<i64>())("description", "Offset is too big")("offset", info.Offset);
        PQ_ENSURE(info.SeqNo <= (ui64)Max<i64>())("description", "SeqNo is too big")("seqNo", info.SeqNo);

        protoInfo.SetSeqNo(info.SeqNo);
        protoInfo.SetOffset(info.Offset);
        protoInfo.SetWriteTimestampMS(info.WriteTimestamp.MilliSeconds());
        protoInfo.SetExplicit(info.Explicit);
        protoInfo.SetState(TSourceIdInfo::ConvertState(info.State));
    }

    ctx.Send(TabletActorId, response.Release());
}

void TPartition::OnReadComplete(TReadInfo& info,
                                TUserInfo* userInfo,
                                const TEvPQ::TEvBlobResponse* blobResponse,
                                const TActorContext& ctx)
{
    TReadAnswer answer = info.FormAnswer(
        ctx, blobResponse, GetStartOffset(), GetEndOffset(), Partition, userInfo,
        info.Destination, GetSizeLag(info.Offset), TabletActorId, Config.GetMeteringMode(), IsActive(),
        GetResultPostProcessor<NKikimrClient::TCmdReadResult>(info.User)
    );
    const auto& resp = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get())->Response;

    if (blobResponse && HasError(*blobResponse)) {
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_ERROR].Increment(1);
        }
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
    } else {
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_OK].Increment(1);
        }

        if (blobResponse) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_OK].Increment(1);
            TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_OK].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        } else {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_HEAD_ONLY_OK].Increment(1);
            TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_HEAD_ONLY].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        }

        TabletCounters.Cumulative()[COUNTER_PQ_READ_BYTES].Increment(resp->ByteSize());
    }

    ctx.Send(ReplyTo(info.Destination, info.ReplyTo), answer.Event.Release());

    OnReadRequestFinished(info.Destination, answer.Size, info.User, ctx);
}

void TPartition::Handle(TEvPQ::TEvBlobResponse::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Get()->GetCookie();
    if (cookie == ERequestCookie::ReadBlobsForCompaction) {
        BlobsForCompactionWereRead(ev->Get()->GetBlobs());
        return;
    }
    auto it = ReadInfo.find(cookie);

    // If there is no such cookie, then read was canceled.
    // For example, it can be after consumer deletion
    if (it == ReadInfo.end()) {
        return;
    }

    TReadInfo info = std::move(it->second);
    ReadInfo.erase(it);

    auto* userInfo = UsersInfoStorage->GetIfExists(info.User);
    if (!userInfo) {
        ReplyError(ctx, info.Destination,  NPersQueue::NErrorCode::BAD_REQUEST, GetConsumerDeletedMessage(info.User));
        OnReadRequestFinished(info.Destination, 0, info.User, ctx);
    }

    OnReadComplete(info, userInfo, ev->Get(), ctx);
}

void TPartition::Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->IsInternal) {
        CompacterPartitionRequestInflight = false;
        if (Compacter) {
            Compacter->ProcessResponse(ev);
            // auto compacterCounters = Compacter->GetCounters();
            // KeyCompactionReadCyclesTotal.Set(compacterCounters.ReadCyclesCount);
            // KeyCompactionWriteCyclesTotal.Set(compacterCounters.WriteCyclesCount);
        }
    }
    ReadingTimestamp = false;
    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);
    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        ProcessTimestampRead(ctx);
        return;
    }
    PQ_ENSURE(userInfo->ReadScheduled);
    PQ_ENSURE(ReadingForUser != "");

    LOG_E("Topic '" << TopicName() << "' partition " << Partition
            << " user " << ReadingForUser << " readTimeStamp error: " << ev->Get()->Error
    );

    UpdateUserInfoTimestamp.push_back(std::make_pair(ReadingForUser, ReadingForUserReadRuleGeneration));

    ProcessTimestampRead(ctx);
}

void TPartition::CheckHeadConsistency() const
{
    BlobEncoder.CheckHeadConsistency(CompactLevelBorder, TotalLevels, TotalMaxCount);
}

ui64 TPartition::GetSizeLag(i64 offset) {
    return BlobEncoder.GetSizeLag(offset) + CompactionBlobEncoder.GetSizeLag(offset);
}


#define SET_METRIC_VALUE(scope, name, newValue)             \
    scope->GetCounters()[name].Set(newValue);

#define SET_METRIC(scope, name, newValue)                   \
    if (scope->GetCounters()[name].Get() != newValue) {     \
        SET_METRIC_VALUE(scope, name, newValue);            \
        haveChanges = true;                                 \
    }

#define SET_METRICS_COUPLE_IMPL(scope, name, newValue, secondName, secondValue)     \
    if (scope->GetCounters()[name].Get() != newValue) {                             \
        SET_METRIC_VALUE(scope, name, newValue);                                    \
        SET_METRIC_VALUE(scope, secondName, secondValue);                           \
        haveChanges = true;                                                         \
    }

#define SET_METRICS_COUPLE(scope, name, newValue, secondName)                       \
    SET_METRICS_COUPLE_IMPL(scope, name, newValue, secondName, newValue);


bool TPartition::UpdateCounters(const TActorContext& ctx, bool force) {
    if (!PartitionCountersLabeled) {
        return false;
    }

    const auto now = ctx.Now();
    const auto nowMs = now.MilliSeconds();
    if ((now - LastCountersUpdate < MIN_UPDATE_COUNTERS_DELAY) && !force)
        return false;

    LastCountersUpdate = now;

    // per client counters
    for (auto&& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        if (!userInfo.LabeledCounters) {
            continue;
        }
        if (userInfoPair.first != CLIENTID_WITHOUT_CONSUMER && !userInfo.HasReadRule && !userInfo.Important) {
            continue;
        }
        bool haveChanges = false;
        const auto snapshot = CreateSnapshot(userInfo);

        auto ts = snapshot.LastCommittedMessage.WriteTimestamp.MilliSeconds();
        if (ts < MIN_TIMESTAMP_MS) {
            ts = Max<i64>();
        }
        for (auto& perPartitionCounters : userInfo.PerPartitionCounters) {
            perPartitionCounters.WriteTimeLagMsByCommittedPerPartition->Set(nowMs < ts ? 0 : nowMs - ts);
        }
        SET_METRIC(userInfo.LabeledCounters, METRIC_COMMIT_WRITE_TIME, ts);

        ts = snapshot.LastCommittedMessage.CreateTimestamp.MilliSeconds();
        if (ts < MIN_TIMESTAMP_MS) {
            ts = Max<i64>();
        }
        SET_METRIC(userInfo.LabeledCounters, METRIC_COMMIT_CREATE_TIME, ts);

        auto readWriteTimestamp = snapshot.LastReadMessage.WriteTimestamp;
        SET_METRIC(userInfo.LabeledCounters, METRIC_READ_WRITE_TIME, readWriteTimestamp.MilliSeconds());
        SET_METRIC(userInfo.LabeledCounters, METRIC_READ_TOTAL_TIME, snapshot.TotalLag.MilliSeconds());

        ts = snapshot.LastReadTimestamp.MilliSeconds();
        for (auto& perPartitionCounters : userInfo.PerPartitionCounters) {
            perPartitionCounters.TimeSinceLastReadMsPerPartition->Set(nowMs < ts ? 0 : nowMs - ts);
        }
        SET_METRIC(userInfo.LabeledCounters, METRIC_LAST_READ_TIME, ts);

        {
            ui64 timeLag = userInfo.GetWriteLagMs();
            for (auto& perPartitionCounters : userInfo.PerPartitionCounters) {
                perPartitionCounters.WriteTimeLagMsByLastReadPerPartition->Set(timeLag);
            }
            SET_METRIC(userInfo.LabeledCounters, METRIC_WRITE_TIME_LAG, timeLag);
        }

        {
            auto lag = snapshot.ReadLag.MilliSeconds();
            for (auto& perPartitionCounters : userInfo.PerPartitionCounters) {
                perPartitionCounters.ReadTimeLagMsPerPartition->Set(lag);
            }
            SET_METRIC(userInfo.LabeledCounters, METRIC_READ_TIME_LAG, lag);
        }

        {
            ui64 lag = GetEndOffset() - userInfo.Offset;
            for (auto& perPartitionCounters : userInfo.PerPartitionCounters) {
                perPartitionCounters.MessageLagByCommittedPerPartition->Set(lag);
            }

            SET_METRIC(userInfo.LabeledCounters, METRIC_COMMIT_MESSAGE_LAG, lag);
            SET_METRIC(userInfo.LabeledCounters, METRIC_TOTAL_COMMIT_MESSAGE_LAG, lag);
        }

        ui64 readMessageLag = GetEndOffset() - snapshot.ReadOffset;
        for (auto& perPartitionCounters : userInfo.PerPartitionCounters) {
            perPartitionCounters.MessageLagByLastReadPerPartition->Set(readMessageLag);
        }

        SET_METRICS_COUPLE(userInfo.LabeledCounters, METRIC_READ_MESSAGE_LAG, readMessageLag, METRIC_READ_TOTAL_MESSAGE_LAG);

        ui64 sizeLag = GetSizeLag(userInfo.Offset);
        SET_METRIC(userInfo.LabeledCounters, METRIC_COMMIT_SIZE_LAG, sizeLag);

        ui64 sizeLagRead = GetSizeLag(userInfo.ReadOffset);
        SET_METRICS_COUPLE_IMPL(userInfo.LabeledCounters, METRIC_READ_SIZE_LAG, sizeLagRead,
            METRIC_READ_TOTAL_SIZE_LAG, sizeLag);

        SET_METRIC(userInfo.LabeledCounters, METRIC_USER_PARTITIONS, 1);

        ui64 readOffsetRewindSum = userInfo.ReadOffsetRewindSum;
        SET_METRIC(userInfo.LabeledCounters, METRIC_READ_OFFSET_REWIND_SUM, readOffsetRewindSum);

        ui32 id = METRIC_TOTAL_READ_SPEED_1;
        for (ui32 i = 0; i < userInfo.AvgReadBytes.size(); ++i) {
            ui64 avg = userInfo.AvgReadBytes[i].GetValue();
            if (avg != userInfo.LabeledCounters->GetCounters()[id].Get()) {
                haveChanges = true;
                userInfo.LabeledCounters->GetCounters()[id].Set(avg); //total
                userInfo.LabeledCounters->GetCounters()[id + 1].Set(avg); //max
            }
            id += 2;
        }
        PQ_ENSURE(id == METRIC_MAX_READ_SPEED_4 + 1);
        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get()) {
            ui64 quotaUsage = ui64(userInfo.AvgReadBytes[1].GetValue()) * 1000000 / userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get() / 60;

            SET_METRIC(userInfo.LabeledCounters, METRIC_READ_QUOTA_PER_CONSUMER_USAGE, quotaUsage);
        }

        if (userInfoPair.first == CLIENTID_WITHOUT_CONSUMER ) {
            SET_METRIC_VALUE(PartitionCountersLabeled, METRIC_READ_QUOTA_NO_CONSUMER_BYTES,
                userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get());

            SET_METRIC_VALUE(PartitionCountersLabeled, METRIC_READ_QUOTA_NO_CONSUMER_USAGE,
                userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_USAGE].Get());
        }

        if (haveChanges) {
            ctx.Send(TabletActorId, new TEvPQ::TEvPartitionLabeledCounters(Partition, *userInfo.LabeledCounters));
        }
    }

    bool haveChanges = false;

    {
        auto count = SourceIdStorage.GetInMemorySourceIds().size();

        if (SourceIdCountPerPartition) {
            SourceIdCountPerPartition->Set(count);
        }
        SET_METRICS_COUPLE(PartitionCountersLabeled, METRIC_MAX_NUM_SIDS, count, METRIC_NUM_SIDS);
    }

    TDuration lifetimeNow = ctx.Now() - SourceIdStorage.MinAvailableTimestamp(ctx.Now());
    SET_METRIC(PartitionCountersLabeled, METRIC_MIN_SID_LIFETIME, lifetimeNow.MilliSeconds());

    const ui64 headGapSize = BlobEncoder.GetHeadGapSize();
    const ui64 gapSize = GapSize + headGapSize;
    SET_METRICS_COUPLE(PartitionCountersLabeled, METRIC_GAPS_SIZE, gapSize, METRIC_MAX_GAPS_SIZE);

    const ui32 gapsCount = GapOffsets.size() + (headGapSize ? 1 : 0);
    SET_METRICS_COUPLE(PartitionCountersLabeled, METRIC_GAPS_COUNT, gapsCount, METRIC_MAX_GAPS_COUNT);

    SET_METRIC(PartitionCountersLabeled, METRIC_WRITE_QUOTA_BYTES, TotalPartitionWriteSpeed);

    ui32 id = METRIC_TOTAL_WRITE_SPEED_1;
    for (ui32 i = 0; i < AvgWriteBytes.size(); ++i) {
        ui64 avg = AvgWriteBytes[i].GetValue();
        if (avg != PartitionCountersLabeled->GetCounters()[id].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[id].Set(avg); //total
            PartitionCountersLabeled->GetCounters()[id + 1].Set(avg); //max
        }
        id += 2;
    }
    PQ_ENSURE(id == METRIC_MAX_WRITE_SPEED_4 + 1);


    id = METRIC_TOTAL_QUOTA_SPEED_1;
    for (ui32 i = 0; i < AvgQuotaBytes.size(); ++i) {
        ui64 avg = AvgQuotaBytes[i].GetValue();
        if (avg != PartitionCountersLabeled->GetCounters()[id].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[id].Set(avg); //total
            PartitionCountersLabeled->GetCounters()[id + 1].Set(avg); //max
        }
        id += 2;
    }
    PQ_ENSURE(id == METRIC_MAX_QUOTA_SPEED_4 + 1);

    if (TotalPartitionWriteSpeed) {
        ui64 quotaUsage = ui64(AvgQuotaBytes[1].GetValue()) * 1000000 / TotalPartitionWriteSpeed / 60;
        SET_METRIC(PartitionCountersLabeled, METRIC_WRITE_QUOTA_USAGE, quotaUsage);
    }

    ui64 storageSize = StorageSize(ctx);
    SET_METRICS_COUPLE(PartitionCountersLabeled, METRIC_TOTAL_PART_SIZE, storageSize, METRIC_MAX_PART_SIZE);

    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY == Config.GetMeteringMode()) {
        ui64 reserveSize = ReserveSize();
        SET_METRIC(PartitionCountersLabeled, METRIC_RESERVE_LIMIT_BYTES, reserveSize);

        ui64 reserveUsed = UsedReserveSize(ctx);
        SET_METRIC(PartitionCountersLabeled, METRIC_RESERVE_USED_BYTES, reserveUsed);
    }

    {
        ui64 ts = (WriteTimestamp.MilliSeconds() < MIN_TIMESTAMP_MS) ? Max<i64>() : WriteTimestamp.MilliSeconds();

        if (TimeSinceLastWriteMsPerPartition) {
            TimeSinceLastWriteMsPerPartition->Set(nowMs < ts ? 0 : nowMs - ts);
        }
        SET_METRIC(PartitionCountersLabeled, METRIC_LAST_WRITE_TIME, ts);
    }

    ui64 timeLag = WriteLagMs.GetValue();
    if (WriteTimeLagMsByLastWritePerPartition) {
        WriteTimeLagMsByLastWritePerPartition->Set(timeLag);
    }
    SET_METRIC(PartitionCountersLabeled, METRIC_WRITE_TIME_LAG_MS, timeLag);

    if (PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Get()) {
        ui64 quotaUsage = ui64(AvgReadBytes.GetValue()) * 1000000 / PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Get() / 60;
        SET_METRIC(PartitionCountersLabeled, METRIC_READ_QUOTA_PARTITION_TOTAL_USAGE, quotaUsage);
    }

    if (PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_NO_CONSUMER_BYTES].Get()) {
        ui64 quotaUsage = ui64(AvgReadBytes.GetValue()) * 1000000 / PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Get() / 60;
        SET_METRIC(PartitionCountersLabeled, METRIC_READ_QUOTA_PARTITION_TOTAL_USAGE, quotaUsage);
    }
    if (PartitionKeyCompactionCounters) {
        Y_ENSURE(Compacter);
        auto counters = Compacter->GetCounters();
        SET_METRIC(PartitionKeyCompactionCounters, METRIC_UNCOMPACTED_SIZE_MAX, counters.UncompactedSize);
        SET_METRIC(PartitionKeyCompactionCounters, METRIC_UNCOMPACTED_SIZE_SUM, counters.UncompactedSize);

        SET_METRIC(PartitionKeyCompactionCounters, METRIC_COMPACTED_SIZE_MAX, counters.CompactedSize);
        SET_METRIC(PartitionKeyCompactionCounters, METRIC_COMPACTED_SIZE_SUM, counters.CompactedSize);

        SET_METRIC(PartitionKeyCompactionCounters, METRIC_UNCOMPACTED_COUNT, counters.UncompactedCount);
        SET_METRIC(PartitionKeyCompactionCounters, METRIC_COMPACTED_COUNT, counters.CompactedCount);

        // if (counters.GetUncompactedRatio() != PartitionKeyCompactionCounters->GetCounters()[METRIC_UNCOMPACTED_RATIO].Get()) {
        //     PartitionKeyCompactionCounters->GetCounters()[METRIC_UNCOMPACTED_RATIO].Set(counters.GetUncompactedRatio());
        //     haveChanges = true;
        // }
        SET_METRIC(PartitionKeyCompactionCounters, METRIC_CURR_CYCLE_DURATION, counters.CurrReadCycleDuration.MilliSeconds());
        SET_METRIC(PartitionKeyCompactionCounters, METRIC_CURR_READ_CYCLE_KEYS, counters.CurrentReadCycleKeys);
    }

    if (PartitionCountersExtended && InitDone) {
        ui64 lag = 0;
        if (ThereIsUncompactedData()) {
            auto now = ctx.Now();
            auto begin = GetFirstUncompactedBlobTimestamp();
            lag = (now - begin).MilliSeconds();
        }
        SET_METRIC(PartitionCountersExtended, METRIC_BLOB_UNCOMPACTED_LAG_MAX, lag);
        SET_METRIC(PartitionCountersExtended, METRIC_BLOB_UNCOMPACTED_SIZE_MAX, BlobEncoder.BodySize);
        SET_METRIC(PartitionCountersExtended, METRIC_BLOB_UNCOMPACTED_COUNT_MAX, BlobEncoder.DataKeysBody.size());

    }
    return haveChanges;
}

void TPartition::ReportCounters(const TActorContext& ctx, bool force) {
    if (UpdateCounters(ctx, force)) {
        ctx.Send(TabletActorId, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
}

void TPartition::Handle(NQuoterEvents::TEvQuotaUpdated::TPtr& ev, const TActorContext&) {
    for (auto& [consumerStr, quota] : ev->Get()->UpdatedConsumerQuotas) {
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(consumerStr);
        if (userInfo && userInfo->LabeledCounters) {
            userInfo->LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Set(quota);
        }
    }
    if (PartitionCountersLabeled)
        PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Set(ev->Get()->UpdatedTotalPartitionReadQuota);
}

void TPartition::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Received TEvKeyValue::TEvResponse");

    auto& response = ev->Get()->Record;

    if (response.HasCookie() && (response.GetCookie() == static_cast<ui64>(ERequestCookie::CompactificationWrite))) {
        PQ_ENSURE(CompacterKvRequestInflight);
        CompacterKvRequestInflight = false;
        LOG_D("Topic '" << TopicConverter->GetClientsideName() << "'" << " partition " << Partition
                 << ": Got compacter KV response, release RW lock");
        Send(ReadQuotaTrackerActor, new TEvPQ::TEvReleaseExclusiveLock());
        if (Compacter) {
            Compacter->ProcessResponse(ev);
            // auto compacterCounters = Compacter->GetCounters();
            // KeyCompactionReadCyclesTotal.Set(compacterCounters.ReadCyclesCount);
            // KeyCompactionWriteCyclesTotal.Set(compacterCounters.WriteCyclesCount);
        }
        return;
    }

    //check correctness of response
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        LOG_E("OnWrite topic '" << TopicName() << "' partition " << Partition
                << " commands are not processed at all, reason: " << response.DebugString());
        ctx.Send(TabletActorId, new TEvents::TEvPoisonPill());
        //TODO: if status is DISK IS FULL, is global status MSTATUS_OK? it will be good if it is true
        return;
    }
    if (response.DeleteRangeResultSize()) {
        for (ui32 i = 0; i < response.DeleteRangeResultSize(); ++i) {
            if (response.GetDeleteRangeResult(i).GetStatus() != NKikimrProto::OK) {
                LOG_E("OnWrite topic '" << TopicName() << "' partition " << Partition << " delete range error");
                //TODO: if disk is full, could this be ok? delete must be ok, of course
                ctx.Send(TabletActorId, new TEvents::TEvPoisonPill());
                return;
            }
        }
    }

    if (response.WriteResultSize()) {
        bool diskIsOk = true;
        for (ui32 i = 0; i < response.WriteResultSize(); ++i) {
            if (response.GetWriteResult(i).GetStatus() != NKikimrProto::OK) {
                LOG_E("OnWrite  topic '" << TopicName() << "' partition " << Partition << " write error");
                ctx.Send(TabletActorId, new TEvents::TEvPoisonPill());
                return;
            }
            diskIsOk = diskIsOk && CheckDiskStatus(response.GetWriteResult(i).GetStatusFlags());
        }
        DiskIsFull = !diskIsOk;
    }
    bool diskIsOk = true;
    for (ui32 i = 0; i < response.GetStatusResultSize(); ++i) {
        auto& res = response.GetGetStatusResult(i);
        if (res.GetStatus() != NKikimrProto::OK) {
            LOG_E("OnWrite  topic '" << TopicName() << "' partition " << Partition
                    << " are not processed at all, got KV error in CmdGetStatus " << res.GetStatus());
            ctx.Send(TabletActorId, new TEvents::TEvPoisonPill());
            return;
        }
        diskIsOk = diskIsOk && CheckDiskStatus(res.GetStatusFlags());
    }
    if (response.GetStatusResultSize()) {
        DiskIsFull = !diskIsOk;
    }
    const auto writeDuration = ctx.Now() - WriteStartTime;
    const auto minWriteLatency = TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetMinWriteLatencyMs());

    if (response.HasCookie() && (response.GetCookie() == ERequestCookie::WriteBlobsForCompaction)) {
        BlobsForCompactionWereWrite();
        return;
    }

    if (writeDuration > minWriteLatency) {
        OnHandleWriteResponse(ctx);
    } else {
        ctx.Schedule(minWriteLatency - writeDuration, new TEvPQ::TEvHandleWriteResponse(response.GetCookie()));
    }
}


void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> event)
{
    UserActionAndTransactionEvents.emplace_back(MakeSimpleShared<TTransaction>(std::move(event), Now()));
    RequestWriteInfoIfRequired();
}

void TPartition::RequestWriteInfoIfRequired()
{
    auto tx = std::get<1>(UserActionAndTransactionEvents.back().Event);
    auto supportId = tx->SupportivePartitionActor;
    if (supportId) {
        PQ_LOG_TX_D("Send TEvGetWriteInfoRequest for TxId " << tx->GetTxId());
        Send(supportId, new TEvPQ::TEvGetWriteInfoRequest(),
             0, 0,
             tx->CalcPredicateSpan.GetTraceId());
        WriteInfosToTx.insert(std::make_pair(supportId, tx));
    }
}


void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event)
{
    UserActionAndTransactionEvents.emplace_back(MakeSimpleShared<TTransaction>(std::move(event), true));
}

void TPartition::PushFrontDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event)
{
    UserActionAndTransactionEvents.emplace_front(MakeSimpleShared<TTransaction>(std::move(event), false));
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> event)
{
    UserActionAndTransactionEvents.emplace_back(MakeSimpleShared<TTransaction>(std::move(event)));
}

void TPartition::AddImmediateTx(TSimpleSharedPtr<TEvPersQueue::TEvProposeTransaction> tx)
{
    UserActionAndTransactionEvents.emplace_back(MakeSimpleShared<TTransaction>(std::move(tx)));
    ++ImmediateTxCount;
    RequestWriteInfoIfRequired();
}

void TPartition::AddUserAct(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo> act)
{
    TString clientId = act->ClientId;
    UserActionAndTransactionEvents.emplace_back(std::move(act));
    ++UserActCount[clientId];
}

void TPartition::RemoveUserAct(const TString& consumerId)
{
    auto p = UserActCount.find(consumerId);
    PQ_ENSURE(p != UserActCount.end());

    PQ_ENSURE(p->second > 0);
    if (!--p->second) {
        UserActCount.erase(p);
    }
}

size_t TPartition::GetUserActCount(const TString& consumer) const
{
    if (auto i = UserActCount.find(consumer); i != UserActCount.end()) {
        return i->second;
    } else {
        return 0;
    }
}

void TPartition::ProcessTxsAndUserActs(const TActorContext&)
{
    if (KVWriteInProgress) {
        LOG_D("Writing. Can't process user action and tx events");
        return;
    }

    if (DeletePartitionState == DELETION_INITED) {
        if (!PersistRequest) {
            PersistRequest = MakeHolder<TEvKeyValue::TEvRequest>();
        }

        ScheduleNegativeReplies();
        ScheduleDeletePartitionDone();

        AddCmdDeleteRangeForAllKeys(*PersistRequest);

        Send(BlobCache, PersistRequest.Release(), 0, 0, PersistRequestSpan.GetTraceId());
        PersistRequest = nullptr;
        CurrentPersistRequestSpan = std::move(PersistRequestSpan);
        PersistRequestSpan = NWilson::TSpan();
        DeletePartitionState = DELETION_IN_PROCESS;
        KVWriteInProgress = true;

        return;
    }

    LOG_D("Process user action and tx events");
    ProcessUserActionAndTxEvents();
    DumpTheSizeOfInternalQueues();
    if (!UserActionAndTxPendingWrite.empty()) {
        LOG_D("Waiting for the batch to finish");
        return;
    }

    LOG_D("Process user action and tx pending commits");
    ProcessUserActionAndTxPendingCommits();
    DumpTheSizeOfInternalQueues();

    if (CurrentBatchSize > 0) {
        LOG_D("Batch completed (" << CurrentBatchSize << ")");
        Send(SelfId(), new TEvPQ::TEvTxBatchComplete(CurrentBatchSize));
    }
    CurrentBatchSize = 0;

    LOG_D("Try persist");
    RunPersist();
}

void TPartition::ProcessUserActionAndTxEvents()
{
    while (!UserActionAndTransactionEvents.empty()) {
        if (ChangingConfig) {
            break;
        }

        auto& front = UserActionAndTransactionEvents.front();
        if (TMessage* msg = std::get_if<TMessage>(&front.Event); msg && msg->WaitPreviousWriteSpan) {
            msg->WaitPreviousWriteSpan.End();
        }

        auto visitor = [this, &front](auto& event) {
            return this->ProcessUserActionAndTxEvent(event, front.AffectedSourceIdsAndConsumers);
        };
        switch (std::visit(visitor, front.Event)) {
            case EProcessResult::Continue:
                MoveUserActionAndTxToPendingCommitQueue();
                break;
            case EProcessResult::ContinueDrop:
                UserActionAndTransactionEvents.pop_front();
                break;
            case EProcessResult::Break:
                MoveUserActionAndTxToPendingCommitQueue();
                break;
            case EProcessResult::Blocked:
                return;
            case EProcessResult::NotReady:
                return;
        }
    }
}

void TPartition::DumpTheSizeOfInternalQueues() const
{
    LOG_D("Events: " << UserActionAndTransactionEvents.size() <<
          ", PendingCommits: " << UserActionAndTxPendingCommit.size() <<
          ", PendingWrites: " << UserActionAndTxPendingWrite.size());
}

TString GetTransactionType(const TTransaction& tx)
{
    if (tx.Tx) {
        return "Tx";
    } else if (tx.ProposeTransaction) {
        return "ImmediateTx";
    } else if (tx.ProposeConfig) {
        return "ProposeConfig";
    } else if (tx.ChangeConfig) {
        return "ChangeConfig";
    } else {
        return "???";
    }
}

auto TPartition::ProcessUserActionAndTxEvent(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& event,
                                             TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers) -> EProcessResult
{
    LOG_D("TPartition::ProcessUserActionAndTxEvent(TEvPQ::TEvSetClientInfo)");
    return PreProcessUserActionOrTransaction(event, affectedSourceIdsAndConsumers);
}

auto TPartition::ProcessUserActionAndTxEvent(TSimpleSharedPtr<TTransaction>& tx,
                                             TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers) -> EProcessResult
{
    LOG_D("TPartition::ProcessUserActionAndTxEvent(TTransaction[" << GetTransactionType(*tx) << "])");
    return PreProcessUserActionOrTransaction(tx, affectedSourceIdsAndConsumers);
}

auto TPartition::ProcessUserActionAndTxEvent(TMessage& msg,
                                             TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers) -> EProcessResult
{
    LOG_D("TPartition::ProcessUserActionAndTxEvent(TMessage)");
    return PreProcessUserActionOrTransaction(msg, affectedSourceIdsAndConsumers);
}

bool TPartition::WritingCycleDoesNotExceedTheLimits() const
{
    return WriteCycleSizeEstimate < MAX_WRITE_CYCLE_SIZE && WriteKeysSizeEstimate < MAX_KEYS;
}

void TPartition::ProcessUserActionAndTxPendingCommits() {
    CurrentBatchSize = 0;

    PQ_ENSURE(!KVWriteInProgress);
    if (!PersistRequest) {
        PersistRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    }

    while (!UserActionAndTxPendingCommit.empty() && WritingCycleDoesNotExceedTheLimits()) {
        auto& front = UserActionAndTxPendingCommit.front();
        auto state = ECommitState::Committed;

        if (auto* tx = get_if<TSimpleSharedPtr<TTransaction>>(&front.Event)) {
            state = tx->Get()->State;
        }

        switch (state) {
            case ECommitState::Pending:
                return;
            case ECommitState::Aborted:
                break;
            case ECommitState::Committed:
                break;
        }

        UserActionAndTxPendingWrite.push_back(std::move(front));
        UserActionAndTxPendingCommit.pop_front();

        auto& event = UserActionAndTxPendingWrite.back().Event;
        auto visitor = [this, request = PersistRequest.Get()](auto& event) {
            return this->ProcessUserActionAndTxPendingCommit(event, request);
        };
        std::visit(visitor, event);

        ++CurrentBatchSize;
    }
}

void TPartition::ProcessUserActionAndTxPendingCommit(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& event,
                                                     TEvKeyValue::TEvRequest* request)
{
    LOG_D("TPartition::ProcessUserActionAndTxPendingCommit(TEvPQ::TEvSetClientInfo)");
    ExecUserActionOrTransaction(event, request);
}

void TPartition::ProcessUserActionAndTxPendingCommit(TSimpleSharedPtr<TTransaction>& tx,
                                                     TEvKeyValue::TEvRequest* request)
{
    LOG_D("TPartition::ProcessUserActionAndTxPendingCommit(TTransaction[" << GetTransactionType(*tx) << "])");
    ExecUserActionOrTransaction(tx, request);
}

void TPartition::ProcessUserActionAndTxPendingCommit(TMessage& msg,
                                                     TEvKeyValue::TEvRequest* request)
{
    LOG_D("TPartition::ProcessUserActionAndTxPendingCommit(TMessage)");
    ExecUserActionOrTransaction(msg, request);
}

static void AppendToSet(const TVector<TString>& p, THashMap<TString, size_t>& q)
{
    for (const auto& s : p) {
        ++q[s];
    }
}

void TPartition::AppendAffectedSourceIdsAndConsumers(const TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    AppendToSet(affectedSourceIdsAndConsumers.TxWriteSourcesIds, TxAffectedSourcesIds);
    AppendToSet(affectedSourceIdsAndConsumers.WriteSourcesIds, WriteAffectedSourcesIds);
    AppendToSet(affectedSourceIdsAndConsumers.TxReadConsumers, TxAffectedConsumers);
    AppendToSet(affectedSourceIdsAndConsumers.ReadConsumers, SetOffsetAffectedConsumers);

    WriteKeysSizeEstimate += affectedSourceIdsAndConsumers.WriteKeysSize;
}

void TPartition::DeleteAffectedSourceIdsAndConsumers()
{
    if (UserActionAndTxPendingWrite.empty()) {
        return;
    }

    for (const auto& e : UserActionAndTxPendingWrite) {
        DeleteAffectedSourceIdsAndConsumers(e.AffectedSourceIdsAndConsumers);

        if (auto* tx = std::get_if<TSimpleSharedPtr<TTransaction>>(&e.Event); tx) {
            if (auto txId = (*tx)->GetTxId(); txId.Defined()) {
                TransactionsInflight.erase(*txId);
            }

            if ((*tx)->ChangeConfig || (*tx)->ProposeConfig) {
                ChangingConfig = false;
            }
        }
    }

    UserActionAndTxPendingWrite.clear();
}

void TPartition::DeleteFromSet(const TVector<TString>& p, THashMap<TString, size_t>& q) const
{
    for (const auto& s : p) {
        auto i = q.find(s);
        PQ_ENSURE(i != q.end());
        PQ_ENSURE(i->second > 0);
        if (--i->second) {
            continue;
        }
        q.erase(s);
    }
}

void TPartition::DeleteAffectedSourceIdsAndConsumers(const TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    DeleteFromSet(affectedSourceIdsAndConsumers.TxWriteSourcesIds, TxAffectedSourcesIds);
    DeleteFromSet(affectedSourceIdsAndConsumers.WriteSourcesIds, WriteAffectedSourcesIds);
    DeleteFromSet(affectedSourceIdsAndConsumers.TxReadConsumers, TxAffectedConsumers);
    DeleteFromSet(affectedSourceIdsAndConsumers.ReadConsumers, SetOffsetAffectedConsumers);

    PQ_ENSURE(WriteKeysSizeEstimate >= affectedSourceIdsAndConsumers.WriteKeysSize);
    WriteKeysSizeEstimate -= affectedSourceIdsAndConsumers.WriteKeysSize;
}

void TPartition::MoveUserActionAndTxToPendingCommitQueue() {
    auto& front = UserActionAndTransactionEvents.front();
    AppendAffectedSourceIdsAndConsumers(front.AffectedSourceIdsAndConsumers);
    UserActionAndTxPendingCommit.push_back(std::move(front));
    UserActionAndTransactionEvents.pop_front();
}

ui64 TPartition::NextReadCookie()
{
    if (Cookie == Max<ui64>()) {
        Cookie = ERequestCookie::End;
    }
    return Cookie++;
}

void TPartition::RunPersist() {
    const auto& ctx = ActorContext();
    const auto now = ctx.Now();
    if (!PersistRequest) {
        PersistRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    }

    if (ManageWriteTimestampEstimate) {
        WriteTimestampEstimate = now;
    }

    HaveDrop = CleanUp(PersistRequest.Get(), ctx);
    bool haveChanges = HaveDrop;
    if (DiskIsFull) {
        AddCheckDiskRequest(PersistRequest.Get(), NumChannels);
        haveChanges = true;
    }

    ProcessReserveRequests(ctx);

    WriteStartTime = TActivationContext::Now();

    if (HaveWriteMsg) {
        if (!DiskIsFull) {
            EndAppendHeadWithNewWrites(ctx);
            EndProcessWrites(PersistRequest.Get(), ctx);
        }
        EndHandleRequests(PersistRequest.Get(), ctx);
    }

    if (Compacter) {
        Compacter->TryCompactionIfPossible();
    }

    haveChanges |= TryAddDeleteHeadKeysToPersistRequest();
    haveChanges |= TxIdHasChanged || !AffectedUsers.empty() || ChangeConfig;

    if (haveChanges) {
        WriteCycleStartTime = now;
        WriteStartTime = now;
        TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
        PartitionQuotaWaitTimeForCurrentBlob = TDuration::Zero();
        WritesTotal.Inc();
        HaveWriteMsg = true;

        AddCmdWriteTxMeta(PersistRequest->Record);
        AddCmdWriteUserInfos(PersistRequest->Record);
        AddCmdWriteConfig(PersistRequest->Record);
    }

    auto requestEmpty = PersistRequest->Record.CmdDeleteRangeSize() == 0
        && PersistRequest->Record.CmdWriteSize() == 0
        && PersistRequest->Record.CmdRenameSize() == 0;

    if (haveChanges || !requestEmpty) {
        AddMessageDeduplicatorKeys(PersistRequest.Get());
        AddMetaKey(PersistRequest.Get());

        // Apply counters
        for (const auto& writeInfo : WriteInfosApplied) {
            // writeTimeLag
            if (InputTimeLag && writeInfo->InputLags) {
                writeInfo->InputLags->UpdateTimestamp(ctx.Now().MilliSeconds());
                for (const auto& values : writeInfo->InputLags->GetValues()) {
                    if (values.second)
                        InputTimeLag->IncFor(std::ceil(values.first), values.second);
                }
            }
            //MessageSize
            auto i = 0u;
            for (auto range : MessageSize.GetRanges()) {
                if (i >= writeInfo->MessagesSizes.size()) {
                    break;
                }
                MessageSize.IncFor(range, writeInfo->MessagesSizes[i++]);
            }

            // Bytes Written
            if (BytesWrittenPerPartition) {
                BytesWrittenPerPartition->Add(writeInfo->BytesWrittenTotal);
            }
            BytesWrittenTotal.Inc(writeInfo->BytesWrittenTotal);
            BytesWrittenGrpc.Inc(writeInfo->BytesWrittenGrpc);
            BytesWrittenUncompressed.Inc(writeInfo->BytesWrittenUncompressed);

            // Messages written
            if (MessagesWrittenPerPartition) {
                MessagesWrittenPerPartition->Add(writeInfo->MessagesWrittenTotal);
            }
            MsgsWrittenTotal.Inc(writeInfo->MessagesWrittenTotal);
            MsgsWrittenGrpc.Inc(writeInfo->MessagesWrittenTotal);

            WriteNewSizeFromSupportivePartitions += writeInfo->BytesWrittenTotal;

            for (auto& [sourceId, writtenBytes] : writeInfo->WrittenBytes) {
                AutopartitioningManager->OnWrite(sourceId, writtenBytes);
            }
        }
        WriteInfosApplied.clear();
        //Done with counters.

        // for debugging purposes
        //DumpKeyValueRequest(PersistRequest->Record);

        for (auto& traceId : TxForPersistTraceIds) {
            TxForPersistSpans.emplace_back(TWilsonTopic::TopicTopLevel,
                                           std::move(traceId),
                                           "Topic.Partition.TxPersistRequest",
                                           NWilson::EFlags::AUTO_END);
            AttachPersistRequestSpan(TxForPersistSpans.back());
        }
        TxForPersistTraceIds.clear();

        PersistRequestSpan.Attribute("bytes", static_cast<i64>(PersistRequest->Record.ByteSizeLong()));
        ctx.Send(HaveWriteMsg ? BlobCache : TabletActorId, PersistRequest.Release(), 0, 0, PersistRequestSpan.GetTraceId());
        CurrentPersistRequestSpan = std::move(PersistRequestSpan);
        PersistRequestSpan = NWilson::TSpan();

        KVWriteInProgress = true;
    } else {
        OnProcessTxsAndUserActsWriteComplete(ActorContext());
        AnswerCurrentWrites(ctx);
        AnswerCurrentReplies(ctx);
        HaveWriteMsg = false;
    }
    PersistRequest = nullptr;
}

bool TPartition::TryAddDeleteHeadKeysToPersistRequest()
{
    bool haveChanges = false;

    auto doDelete = [&](auto& deletedKeys) {
        while (!deletedKeys.empty()) {
            auto& k = deletedKeys.front();

            if (auto lock = k.Lock.lock(); lock) {
                LOG_D("Key locked: " << k.Key);
                // key is locked, wait for it to be unlocked
                break;
            }

            LOG_D("Key deleted: " << k.Key);

            haveChanges = true;

            auto* cmd = PersistRequest->Record.AddCmdDeleteRange();
            auto* range = cmd->MutableRange();

            range->SetFrom(k.Key);
            range->SetIncludeFrom(true);
            range->SetTo(std::move(k.Key));
            range->SetIncludeTo(true);

            deletedKeys.pop_front();
        }
    };

    doDelete(CompactionBlobEncoder.DeletedKeys);
    doDelete(BlobEncoder.DeletedKeys);

    return haveChanges;
}

//void TPartition::DumpKeyValueRequest(const NKikimrClient::TKeyValueRequest& request) const
//{
//    DBGTRACE_LOG("=== DumpKeyValueRequest ===");
//    DBGTRACE_LOG("--- delete ----------------");
//    for (size_t i = 0; i < request.CmdDeleteRangeSize(); ++i) {
//        const auto& cmd = request.GetCmdDeleteRange(i);
//        const auto& range = cmd.GetRange();
//        Y_UNUSED(range);
//        DBGTRACE_LOG((range.GetIncludeFrom() ? '[' : '(') << range.GetFrom() <<
//                     ", " <<
//                     range.GetTo() << (range.GetIncludeTo() ? ']' : ')'));
//    }
//    DBGTRACE_LOG("--- write -----------------");
//    for (size_t i = 0; i < request.CmdWriteSize(); ++i) {
//        const auto& cmd = request.GetCmdWrite(i);
//        Y_UNUSED(cmd);
//        DBGTRACE_LOG(cmd.GetKey());
//    }
//    DBGTRACE_LOG("--- rename ----------------");
//    for (size_t i = 0; i < request.CmdRenameSize(); ++i) {
//        const auto& cmd = request.GetCmdRename(i);
//        Y_UNUSED(cmd);
//        DBGTRACE_LOG(cmd.GetOldKey() << ", " << cmd.GetNewKey());
//    }
//    DBGTRACE_LOG("===========================");
//}

//void TPartition::DumpZones(const char* file, unsigned line) const
//{
//    DBGTRACE("TPartition::DumpZones");
//
//    if (file) {
//        Y_UNUSED(line);
//        DBGTRACE_LOG(file << "(" << line << ")");
//    }
//
//    DBGTRACE_LOG("=== DumpPartitionZones ===");
//    DBGTRACE_LOG("--- Compaction -----------");
//    CompactionBlobEncoder.Dump();
//    DBGTRACE_LOG("--- FastWrite ------------");
//    BlobEncoder.Dump();
//    DBGTRACE_LOG("==========================");
//}

TBlobKeyTokenPtr TPartition::MakeBlobKeyToken(const TString& key)
{
    return std::make_shared<TBlobKeyToken>(key);
}

void TPartition::AnswerCurrentReplies(const TActorContext& ctx)
{
    for (auto& [actor, reply] : Replies) {
        ctx.Send(actor, reply.release());
    }
    Replies.clear();
}

TPartition::EProcessResult TPartition::PreProcessUserActionOrTransaction(TSimpleSharedPtr<TTransaction>& t,
                                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    auto span = t->CalcPredicateSpan.CreateChild(TWilsonTopic::TopicTopLevel,
                                                 "Topic.Partition.PreProcess",
                                                 NWilson::EFlags::AUTO_END);

    auto result = EProcessResult::Continue;
    if (t->SupportivePartitionActor && !t->WriteInfo && !t->WriteInfoApplied) { // Pending for write info
        PQ_LOG_TX_D("The TxId " << t->GetTxId() << " is waiting for TEvGetWriteInfoResponse");
        return EProcessResult::NotReady;
    }
    if (t->WriteInfo && !t->WriteInfoApplied) { //Received write info but not applied
        result = ApplyWriteInfoResponse(*t, affectedSourceIdsAndConsumers);
        if (!t->WriteInfoApplied) { // Tried to apply write info but couldn't - TX must be blocked.
            PQ_LOG_TX_D("The TxId " << t->GetTxId() << " must be blocked");
            PQ_ENSURE(result != EProcessResult::Continue);
            return result;
        }
    }
    if (t->ProposeTransaction) { // Immediate TX
        if (!t->Predicate.GetOrElse(true)) {
            t->State = ECommitState::Aborted;
            return EProcessResult::Continue;
        }
        t->Predicate.ConstructInPlace(true);
        return PreProcessImmediateTx(*t, affectedSourceIdsAndConsumers);

    } else if (t->Tx) { // Distributed TX
        if (t->Predicate.Defined()) { // Predicate defined - either failed previously or Tx created with predicate defined.
            ReplyToProposeOrPredicate(t, true);
            return EProcessResult::Continue;
        }
        result = BeginTransactionData(*t, affectedSourceIdsAndConsumers);
        if (t->Predicate.Defined()) {
            ReplyToProposeOrPredicate(t, true);
        }
        return result;
    } else if (t->ProposeConfig) {
        if (HasPendingCommitsOrPendingWrites()) {
            LOG_D("Wait until the operation with the config becomes the first in the queue");
            return EProcessResult::Blocked;
        }
        t->Predicate = BeginTransactionConfig();
        ChangingConfig = true;
        PendingPartitionConfig = GetPartitionConfig(t->ProposeConfig->Config);
        //Y_VERIFY_DEBUG_S(PendingPartitionConfig, "Partition " << Partition << " config not found");
        ReplyToProposeOrPredicate(t, false);
        return EProcessResult::Break;
    } else {
        PQ_ENSURE(t->ChangeConfig);

        PQ_ENSURE(!ChangeConfig && !ChangingConfig);
        if (HasPendingCommitsOrPendingWrites()) {
            LOG_D("Wait until the operation with the config becomes the first in the queue");
            return EProcessResult::Blocked;
        }
        ChangingConfig = true;
        // Should remove this and add some id to TEvChangeConfig if we want to batch change of configs
        t->State = ECommitState::Committed;
        return EProcessResult::Break;
    }
    Y_ABORT();
    return result;
}

bool TPartition::HasPendingCommitsOrPendingWrites() const
{
    return !UserActionAndTxPendingCommit.empty() || !UserActionAndTxPendingWrite.empty();
}

void TPartition::TryAddCmdWriteForTransaction(const TTransaction& tx)
{
    Y_ENSURE(!IsSupportive());

    if (!tx.SerializedTx.Defined()) {
        return;
    }

    PQ_ENSURE(PersistRequest);
    TMaybe<ui64> txId = tx.GetTxId();
    PQ_ENSURE(txId.Defined());

    TString value;
    PQ_ENSURE(tx.SerializedTx->SerializeToString(&value));

    auto command = PersistRequest->Record.AddCmdWrite();
    command->SetKey(GetTxKey(*txId, Partition.OriginalPartitionId));
    command->SetValue(value);
    command->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

    if (!tx.TabletConfig.Defined()) {
        return;
    }

    value.clear();
    PQ_ENSURE(tx.TabletConfig->SerializeToString(&value));

    command = PersistRequest->Record.AddCmdWrite();
    command->SetKey("_config");
    command->SetValue(value);
    command->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

    const TActorContext& ctx = ActorContext();

    const auto graph = MakePartitionGraph(*tx.TabletConfig);
    for (const auto& partition : tx.TabletConfig->GetPartitions()) {
        const auto explicitMessageGroups = CreateExplicitMessageGroups(*tx.BootstrapConfig, *tx.PartitionsData, graph, partition.GetPartitionId());

        TSourceIdWriter sourceIdWriter(ESourceIdFormat::Proto);
        for (const auto& [id, mg] : *explicitMessageGroups) {
            sourceIdWriter.RegisterSourceId(id, mg.SeqNo, 0, ctx.Now(), std::move(mg.KeyRange), false);
        }

        sourceIdWriter.FillRequest(PersistRequest.Get(), TPartitionId(partition.GetPartitionId()));
    }
}

bool TPartition::ExecUserActionOrTransaction(TSimpleSharedPtr<TTransaction>& t,
                                             TEvKeyValue::TEvRequest*)
{
    auto span = t->CommitSpan.CreateChild(TWilsonTopic::TopicTopLevel,
                                          "Topic.Partition.Process",
                                          NWilson::EFlags::AUTO_END);
    TryAddCmdWriteForTransaction(*t);
    if (t->ProposeTransaction) {
        ExecImmediateTx(*t);
        return true;
    }
    switch(t->State) {
        case ECommitState::Pending:
            return false;
        case ECommitState::Aborted:
            RollbackTransaction(t);
            return true;
        case ECommitState::Committed:
            break;
    }
    if (t->ChangeConfig) {
        PQ_ENSURE(!ChangeConfig);
        PQ_ENSURE(ChangingConfig);
        ChangeConfig = t->ChangeConfig;
        SendChangeConfigReply = t->SendReply;
        BeginChangePartitionConfig(ChangeConfig->Config);
    } else if (t->ProposeConfig) {
        PQ_ENSURE(!ChangeConfig);
        PQ_ENSURE(ChangingConfig);
        ChangeConfig =
            MakeSimpleShared<TEvPQ::TEvChangePartitionConfig>(TopicConverter,
                                                              t->ProposeConfig->Config);
        PendingPartitionConfig = GetPartitionConfig(ChangeConfig->Config);
        PendingExplicitMessageGroups = t->ExplicitMessageGroups;
        SendChangeConfigReply = false;
    }

    CommitTransaction(t);
    return true;
}

TPartition::EProcessResult TPartition::BeginTransactionData(TTransaction& t,
                                                            TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    const TEvPQ::TEvTxCalcPredicate& tx = *t.Tx;
    TMaybe<bool>& predicateOut = t.Predicate;
    TString& issueMsg = t.Message;

    if (tx.ForcePredicateFalse) {
        predicateOut = false;
        return EProcessResult::Continue;
    }

    TVector<TString> consumers;
    bool result = true;
    for (const auto& operation : tx.Operations) {
        const TString& consumer = operation.GetConsumer();
        if (TxAffectedConsumers.contains(consumer)) {
            PQ_LOG_TX_D("TxAffectedConsumers contains consumer " << consumer << ". TxId " << tx.TxId);
            return EProcessResult::Blocked;
        }
        if (SetOffsetAffectedConsumers.contains(consumer)) {
            PQ_LOG_TX_D("SetOffsetAffectedConsumers contains consumer " << consumer << ". TxId " << tx.TxId);
            return EProcessResult::Blocked;
        }

        if (AffectedUsers.contains(consumer) && !GetPendingUserIfExists(consumer)) {
            LOG_W("Partition " << Partition <<
                     " Consumer '" << consumer << "' has been removed");
            issueMsg = (MakeTxReadErrorMessage(tx.TxId, TopicName(), Partition, consumer) << "Consumer has been removed");
            result = false;
            break;
        }

        if (!UsersInfoStorage->GetIfExists(consumer)) {
            LOG_W("Partition " << Partition <<
                     " Unknown consumer '" << consumer << "'");
            issueMsg = (MakeTxReadErrorMessage(tx.TxId, TopicName(), Partition, consumer) << "Unknown consumer");
            result = false;
            break;
        }

        bool isAffectedConsumer = AffectedUsers.contains(consumer);
        TUserInfoBase& userInfo = GetOrCreatePendingUser(consumer);

        if (operation.GetOnlyCheckCommitedToFinish()) {
            if (IsActive() || static_cast<ui64>(userInfo.Offset) != GetEndOffset()) {
               result = false;
            }
        } else if (!operation.GetReadSessionId().empty() && operation.GetReadSessionId() != userInfo.Session) {
            if (IsActive() || operation.GetCommitOffsetsEnd() < GetEndOffset() || userInfo.Offset != i64(GetEndOffset())) {
                LOG_W("Partition " << Partition <<
                         " Consumer '" << consumer << "'" <<
                         " Bad request (session already dead) " <<
                         " RequestSessionId '" << operation.GetReadSessionId() <<
                         " CurrentSessionId '" << userInfo.Session << "'");
                issueMsg = (MakeTxReadErrorMessage(tx.TxId, TopicName(), Partition, consumer) << "Session already dead. " <<
                            "Request session id '" << operation.GetReadSessionId() << "'" <<
                            ", current session id '" << userInfo.Session << "'");
                result = false;
            }
        } else {
            if (!operation.GetForceCommit() && operation.GetCommitOffsetsBegin() > operation.GetCommitOffsetsEnd()) {
                LOG_W("Partition " << Partition <<
                         " Consumer '" << consumer << "'" <<
                         " Bad request (invalid range) " <<
                         " Begin " << operation.GetCommitOffsetsBegin() <<
                         " End " << operation.GetCommitOffsetsEnd());
                issueMsg = (MakeTxReadErrorMessage(tx.TxId, TopicName(), Partition, consumer) << "Invalid range. " <<
                            "Range begin " << operation.GetCommitOffsetsBegin() <<
                            ", range end " << operation.GetCommitOffsetsEnd());
                result = false;
            } else if (!operation.GetForceCommit() && userInfo.Offset != (i64)operation.GetCommitOffsetsBegin()) {
                LOG_W("Partition " << Partition <<
                         " Consumer '" << consumer << "'" <<
                         " Bad request (gap) " <<
                         " Offset " << userInfo.Offset <<
                         " Begin " << operation.GetCommitOffsetsBegin());
                issueMsg = (MakeTxReadErrorMessage(tx.TxId, TopicName(), Partition, consumer) << "Gap. " <<
                            "Offset " << userInfo.Offset <<
                            ", range begin " << operation.GetCommitOffsetsBegin());
                result = false;
            } else if (!operation.GetForceCommit() && operation.GetCommitOffsetsEnd() > GetEndOffset()) {
                LOG_W("Partition " << Partition <<
                         " Consumer '" << consumer << "'" <<
                         " Bad request (behind the last offset) " <<
                         " EndOffset " << GetEndOffset() <<
                         " End " << operation.GetCommitOffsetsEnd());
                issueMsg = (MakeTxReadErrorMessage(tx.TxId, TopicName(), Partition, consumer) << "Behind the last offset. " <<
                            "Partition end offset " << GetEndOffset() <<
                            ", range end " << operation.GetCommitOffsetsBegin());
                result = false;
            }

            if (!result) {
                if (!isAffectedConsumer) {
                    AffectedUsers.erase(consumer);
                }
                break;
            }
            consumers.push_back(consumer);
            PQ_LOG_TX_D("TxId " << tx.TxId << " affect consumer " << consumer);
        }
    }

    if (result) {
        affectedSourceIdsAndConsumers.TxReadConsumers = std::move(consumers);
    }
    predicateOut = result;
    return EProcessResult::Continue;
}

bool TPartition::BeginTransactionConfig()
{
    SendChangeConfigReply = false;
    return true;
}

void TPartition::CommitWriteOperations(TTransaction& t)
{
    LOG_D("TPartition::CommitWriteOperations TxId: " << t.GetTxId());

    PQ_ENSURE(PersistRequest);
    PQ_ENSURE(!BlobEncoder.PartitionedBlob.IsInited());

    if (!t.WriteInfo) {
        return;
    }
    for (const auto& s : t.WriteInfo->SrcIdInfo) {
        auto pair = TSeqNoProducerEpoch{s.second.SeqNo, s.second.ProducerEpoch};
        auto [iter, ins] = TxInflightMaxSeqNoPerSourceId.emplace(s.first, pair);
        if (!ins) {
            bool ok = !SeqnoViolation(iter->second.KafkaProducerEpoch, iter->second.SeqNo, s.second.ProducerEpoch, s.second.SeqNo);
            PQ_ENSURE(ok);
            iter->second = pair;
        }
    }
    const auto& ctx = ActorContext();

    if (!HaveWriteMsg) {
        BeginHandleRequests(PersistRequest.Get(), ctx);
        if (!DiskIsFull) {
            BeginProcessWrites(ctx);
            BeginAppendHeadWithNewWrites(ctx);
        }
        HaveWriteMsg = true;
    }

    LOG_D("Head=" << BlobEncoder.Head << ", NewHead=" << BlobEncoder.NewHead);

    auto oldHeadOffset = BlobEncoder.NewHead.Offset;

    if (!t.WriteInfo->BodyKeys.empty()) {
        bool needCompactHead =
            (Parameters->FirstCommitWriteOperations ? BlobEncoder.Head : BlobEncoder.NewHead).PackedSize != 0;

        BlobEncoder.NewPartitionedBlob(Partition,
                                       BlobEncoder.NewHead.Offset,
                                       "", // SourceId
                                       0,  // SeqNo
                                       0,  // TotalParts
                                       0,  // TotalSize
                                       Parameters->HeadCleared,  // headCleared
                                       needCompactHead,          // needCompactHead
                                       MaxBlobSize);

        for (auto& k : t.WriteInfo->BodyKeys) {
            LOG_D("add key " << k.Key.ToString());
            auto write = BlobEncoder.PartitionedBlob.Add(k.Key, k.Size, k.Timestamp, true);
            if (write && !write->Value.empty()) {
                AddCmdWriteWithDeferredTimestamp(write, PersistRequest.Get(), ctx);
                BlobEncoder.CompactedKeys.emplace_back(write->Key, write->Value.size());
            }
            Parameters->CurOffset += k.Key.GetCount();
            // The key does not need to be deleted, as it will be renamed
            k.BlobKeyToken->NeedDelete = false;
        }

        LOG_D("PartitionedBlob.GetFormedBlobs().size=" << BlobEncoder.PartitionedBlob.GetFormedBlobs().size());
        if (const auto& formedBlobs = BlobEncoder.PartitionedBlob.GetFormedBlobs(); !formedBlobs.empty()) {
            ui32 curWrites = RenameTmpCmdWrites(PersistRequest.Get());
            RenameFormedBlobs(formedBlobs,
                              *Parameters,
                              curWrites,
                              PersistRequest.Get(),
                              BlobEncoder,
                              ctx);
        }

        BlobEncoder.ClearPartitionedBlob(Partition, MaxBlobSize);

        BlobEncoder.NewHead.Clear();
        BlobEncoder.NewHead.Offset = Parameters->CurOffset;
    }

    for (const auto& [srcId, info] : t.WriteInfo->SrcIdInfo) {
        auto& sourceIdBatch = Parameters->SourceIdBatch;
        auto sourceId = sourceIdBatch.GetSource(srcId);
        sourceId.Update(info.SeqNo, info.Offset + oldHeadOffset, CurrentTimestamp, info.ProducerEpoch);
        auto& persistInfo = TxSourceIdForPostPersist[srcId];
        persistInfo.SeqNo = info.SeqNo;
        persistInfo.Offset = info.Offset + oldHeadOffset;
        persistInfo.KafkaProducerEpoch = info.ProducerEpoch;
    }

    Parameters->FirstCommitWriteOperations = false;

    WriteInfosApplied.emplace_back(std::move(t.WriteInfo));
}

void TPartition::CommitTransaction(TSimpleSharedPtr<TTransaction>& t)
{
    if (t->Tx) {
        PQ_ENSURE(t->Predicate.Defined() && *t->Predicate);

        for (auto& operation : t->Tx->Operations) {
            if (operation.GetOnlyCheckCommitedToFinish()) {
                continue;
            }

            TUserInfoBase& userInfo = GetOrCreatePendingUser(operation.GetConsumer());

            if (!operation.GetForceCommit()) {
                PQ_ENSURE(userInfo.Offset == (i64)operation.GetCommitOffsetsBegin());
            }

            if ((i64)operation.GetCommitOffsetsEnd() < userInfo.Offset && !operation.GetReadSessionId().empty()) {
                continue; // this is stale request, answer ok for it
            }

            if (operation.GetCommitOffsetsEnd() <= GetStartOffset()) {
                userInfo.AnyCommits = false;
                userInfo.Offset = GetStartOffset();
            } else if (operation.GetCommitOffsetsEnd() > GetEndOffset()) {
                userInfo.AnyCommits = true;
                userInfo.Offset = GetEndOffset();
            } else {
                userInfo.AnyCommits = true;
                userInfo.Offset = operation.GetCommitOffsetsEnd();
            }

            if (operation.GetKillReadSession()) {
                userInfo.Session = "";
                userInfo.PartitionSessionId = 0;
                userInfo.Generation = 0;
                userInfo.Step = 0;
                userInfo.PipeClient = {};
            }
        }

        CommitWriteOperations(*t);
        ChangePlanStepAndTxId(t->Tx->Step, t->Tx->TxId);
        ScheduleReplyTxDone(t->Tx->Step, t->Tx->TxId, std::move(t->CommitSpan));
    } else if (t->ProposeConfig) {
        PQ_ENSURE(t->Predicate.Defined() && *t->Predicate);

        BeginChangePartitionConfig(t->ProposeConfig->Config);
        ExecChangePartitionConfig();
        ChangePlanStepAndTxId(t->ProposeConfig->Step, t->ProposeConfig->TxId);

        ScheduleReplyTxDone(t->ProposeConfig->Step, t->ProposeConfig->TxId, std::move(t->CommitSpan));
    } else {
        PQ_ENSURE(t->ChangeConfig);
        ExecChangePartitionConfig();
    }
}

void TPartition::RollbackTransaction(TSimpleSharedPtr<TTransaction>& t)
{
    auto stepAndId = GetStepAndTxId(*t->Tx);

    auto txIter = TransactionsInflight.find(stepAndId.second);
    PQ_ENSURE(!txIter.IsEnd());

    if (t->Tx) {
        PQ_ENSURE(t->Predicate.Defined());
        ChangePlanStepAndTxId(t->Tx->Step, t->Tx->TxId);
    } else if (t->ProposeConfig) {
        PQ_ENSURE(t->Predicate.Defined());
        ChangingConfig = false;
        ChangePlanStepAndTxId(t->ProposeConfig->Step, t->ProposeConfig->TxId);
    } else {
        PQ_ENSURE(t->ChangeConfig);
        ChangeConfig = nullptr;
        ChangingConfig = false;
    }

    ScheduleReplyTxDone(t->Tx->Step, t->Tx->TxId, std::move(t->CommitSpan));
}

void TPartition::BeginChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config)
{
    TSet<TString> hasReadRule;
    for (auto&& [consumer, info] : UsersInfoStorage->ViewAll()) {
        hasReadRule.insert(consumer);
    }

    for (const auto& consumer : config.GetConsumers()) {
        auto& userInfo = GetOrCreatePendingUser(consumer.GetName(), 0);

        TInstant ts = TInstant::MilliSeconds(consumer.GetReadFromTimestampsMs());
        if (!ts) {
            ts += TDuration::MilliSeconds(1);
        }
        userInfo.ReadFromTimestamp = ts;
        userInfo.Important = IsImportant(consumer);
        userInfo.AvailabilityPeriod = TDuration::MilliSeconds(consumer.GetAvailabilityPeriodMs());

        ui64 rrGen = consumer.GetGeneration();
        if (userInfo.ReadRuleGeneration != rrGen) {
            auto act = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer.GetName(), 0, "", 0, 0, 0, TActorId{},
                                        TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, rrGen);

            auto res = PreProcessUserAct(*act, nullptr);
            PQ_ENSURE(res == EProcessResult::Continue);

            ChangeConfigActs.emplace_back(std::move(act));
        }
        hasReadRule.erase(consumer.GetName());
    }

    for (const auto& consumer : hasReadRule) {
        GetOrCreatePendingUser(consumer);
        auto act = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer, 0, "", 0, 0, 0, TActorId{},
                                    TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0);

        auto res = PreProcessUserAct(*act, nullptr);
        PQ_ENSURE(res == EProcessResult::Continue);

        ChangeConfigActs.emplace_back(std::move(act));
    }
}

void TPartition::ExecChangePartitionConfig() {
    for (auto& act : ChangeConfigActs) {
        auto& userInfo = GetOrCreatePendingUser(act->ClientId);
        EmulatePostProcessUserAct(*act, userInfo, ActorContext());
    }
}

void TPartition::OnProcessTxsAndUserActsWriteComplete(const TActorContext& ctx) {
    DeleteAffectedSourceIdsAndConsumers();
    WriteCycleSizeEstimate = 0;

    if (ChangeConfig) {
        EndChangePartitionConfig(std::move(ChangeConfig->Config),
                                 PendingExplicitMessageGroups,
                                 ChangeConfig->TopicConverter,
                                 ctx);
        PendingExplicitMessageGroups.reset();
    }

    for (auto& user : AffectedUsers) {
        if (auto* actual = GetPendingUserIfExists(user)) {
            TUserInfo& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);
            bool offsetHasChanged = (userInfo.Offset != actual->Offset);

            userInfo.Session = actual->Session;
            userInfo.Generation = actual->Generation;
            userInfo.Step = actual->Step;
            userInfo.Offset = actual->Offset;
            userInfo.CommittedMetadata = actual->CommittedMetadata;
            if (userInfo.Offset <= (i64)GetStartOffset()) {
                userInfo.AnyCommits = false;
            }
            userInfo.ReadRuleGeneration = actual->ReadRuleGeneration;
            userInfo.ReadFromTimestamp = actual->ReadFromTimestamp;
            userInfo.HasReadRule = true;

            if (userInfo.Important != actual->Important || userInfo.AvailabilityPeriod != actual->AvailabilityPeriod) {
                if (userInfo.LabeledCounters) {
                    ScheduleDropPartitionLabeledCounters(userInfo.LabeledCounters->GetGroup());
                }
                UsersInfoStorage->SetImportant(userInfo, actual->Important, actual->AvailabilityPeriod);
            }
            if (ImporantOrExtendedAvailabilityPeriod(userInfo) && userInfo.Offset < (i64)GetStartOffset()) {
                userInfo.Offset = GetStartOffset();
            }

            if (offsetHasChanged && !userInfo.UpdateTimestampFromCache()) {
                userInfo.ActualTimestamps = false;
                ReadTimestampForOffset(user, userInfo, ctx);
            } else {
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_CACHE_HIT].Increment(1);
            }

            if (LastOffsetHasBeenCommited(userInfo)) {
                SendReadingFinished(user);
            }
        } else if (user != CLIENTID_WITHOUT_CONSUMER) {
            auto ui = UsersInfoStorage->GetIfExists(user);
            if (ui && ui->LabeledCounters) {
                ScheduleDropPartitionLabeledCounters(ui->LabeledCounters->GetGroup());
            }

            UsersInfoStorage->Remove(user, ctx);

            // Finish all ongoing reads
            std::unordered_set<ui64> readCookies;
            for (auto& [cookie, info] : ReadInfo) {
                if (info.User == user) {
                    readCookies.insert(cookie);
                    ReplyError(ctx, info.Destination,  NPersQueue::NErrorCode::BAD_REQUEST, GetConsumerDeletedMessage(user));
                    OnReadRequestFinished(info.Destination, 0, user, ctx);
                }
            }
            for (ui64 cookie : readCookies) {
                ReadInfo.erase(cookie);
            }

            Send(ReadQuotaTrackerActor, new TEvPQ::TEvConsumerRemoved(user));
        }
    }

    ChangeConfigActs.clear();
    AnswerCurrentReplies(ctx);

    PendingUsersInfo.clear();
    AffectedUsers.clear();

    TxIdHasChanged = false;

    if (ChangeConfig) {
        ReportCounters(ctx, true);
        ChangeConfig = nullptr;
        PendingPartitionConfig = nullptr;
    }
}

void TPartition::EndChangePartitionConfig(NKikimrPQ::TPQTabletConfig&& config,
                                          const TEvPQ::TMessageGroupsPtr& explicitMessageGroups,
                                          NPersQueue::TTopicConverterPtr topicConverter,
                                          const TActorContext& ctx)
{
    bool autopartitioningChanged = SplitMergeEnabled(Config) != SplitMergeEnabled(config);

    Config = std::move(config);
    PartitionConfig = GetPartitionConfig(Config);
    PartitionGraph = MakePartitionGraph(Config);

    if (explicitMessageGroups) {
        for (const auto& [id, group] : *explicitMessageGroups) {
            NPQ::TPartitionKeyRange keyRange = group.KeyRange;
            TSourceIdInfo sourceId(group.SeqNo, 0, ctx.Now(), std::move(keyRange), false);
            SourceIdStorage.RegisterSourceIdInfo(id, std::move(sourceId), true);
        }
    }

    TopicConverter = topicConverter;
    NewPartition = false;

    PQ_ENSURE(Config.GetPartitionConfig().GetTotalPartitions() > 0);

    if (autopartitioningChanged) {
        AutopartitioningManager.reset(CreateAutopartitioningManager(Config, Partition));
    } else {
        AutopartitioningManager->UpdateConfig(Config);
    }

    Send(ReadQuotaTrackerActor, new TEvPQ::TEvChangePartitionConfig(TopicConverter, Config));
    Send(WriteQuotaTrackerActor, new TEvPQ::TEvChangePartitionConfig(TopicConverter, Config));
    TotalPartitionWriteSpeed = config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();

    CreateCompacter();
    InitializeMLPConsumers();

    if (MirroringEnabled(Config)) {
        if (Mirrorer) {
            ctx.Send(Mirrorer->Actor, new TEvPQ::TEvChangePartitionConfig(TopicConverter,
                                                                          Config));
        } else {
            ScaleStatus = NKikimrPQ::EScaleStatus::NORMAL;
            CreateMirrorerActor();
        }
    } else {
        if (Mirrorer) {
            ctx.Send(Mirrorer->Actor, new TEvents::TEvPoisonPill());
            Mirrorer.Reset();
        }
    }

    if (SendChangeConfigReply) {
        SchedulePartitionConfigChanged();
    }

    if (Config.HasOffloadConfig() && !OffloadActor && !IsSupportive()) {
        OffloadActor = Register(CreateOffloadActor(TabletActorId, TabletId, Partition,
            Config.GetYdbDatabasePath(), Config.GetOffloadConfig()));
    } else if (!Config.HasOffloadConfig() && OffloadActor) {
        Send(OffloadActor, new TEvents::TEvPoisonPill());
        OffloadActor = {};
    }

    if (MonitoringProjectId != Config.GetMonitoringProjectId()) {
        UsersInfoStorage->ResetDetailedMetrics();
        ResetDetailedMetrics();
    } else if (!DetailedMetricsAreEnabled(Config)) {
        ResetDetailedMetrics();
    }
    MonitoringProjectId = Config.GetMonitoringProjectId();
    SetupDetailedMetrics();
    UsersInfoStorage->SetupDetailedMetrics(ActorContext());
}


TString TPartition::GetKeyConfig() const
{
    return Sprintf("_config_%u", Partition.InternalPartitionId);
}

void TPartition::ChangePlanStepAndTxId(ui64 step, ui64 txId)
{
    PlanStep = step;
    TxId = txId;
    TxIdHasChanged = true;
}

TPartition::EProcessResult TPartition::PreProcessImmediateTx(TTransaction& t,
                                                             TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    const NKikimrPQ::TEvProposeTransaction& tx = t.ProposeTransaction->GetRecord();

    if (AffectedUsers.size() >= MAX_USERS) {
        return EProcessResult::Blocked;
    }
    PQ_ENSURE(tx.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    PQ_ENSURE(tx.HasData());
    TVector<TString> consumers;
    for (const auto& operation : tx.GetData().GetOperations()) {
        if (!operation.HasCommitOffsetsBegin() || !operation.HasCommitOffsetsEnd() || !operation.HasConsumer()) {
            continue; //Write operation - handled separately via WriteInfo
        }

        PQ_ENSURE(operation.GetCommitOffsetsBegin() <= (ui64)Max<i64>())("Unexpected begin offset", operation.GetCommitOffsetsBegin());
        PQ_ENSURE(operation.GetCommitOffsetsEnd() <= (ui64)Max<i64>())("Unexpected end offset", operation.GetCommitOffsetsEnd());

        const TString& user = operation.GetConsumer();
        if (TxAffectedConsumers.contains(user)) {
            return EProcessResult::Blocked;
        }
        if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "the consumer has been deleted");
            return EProcessResult::ContinueDrop;
        }
        if (operation.GetCommitOffsetsBegin() > operation.GetCommitOffsetsEnd()) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (begin > end)");
            return EProcessResult::ContinueDrop;
        }

        consumers.push_back(user);
    }
    affectedSourceIdsAndConsumers.ReadConsumers = std::move(consumers);
    affectedSourceIdsAndConsumers.WriteKeysSize += consumers.size();
    return EProcessResult::Continue;
}

void TPartition::ExecImmediateTx(TTransaction& t)
{
    --ImmediateTxCount;
    const auto& record = t.ProposeTransaction->GetRecord();
    PQ_ENSURE(record.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    PQ_ENSURE(record.HasData());


    //ToDo - check, this probably wouldn't work any longer.
    if (!t.Predicate.GetRef()) {
        ScheduleReplyPropose(record,
                             NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                             NKikimrPQ::TError::BAD_REQUEST,
                             t.Message);
        return;
    }
    for (const auto& operation : record.GetData().GetOperations()) {
        if (operation.GetOnlyCheckCommitedToFinish()) {
            continue;
        }

        if (!operation.HasCommitOffsetsBegin() || !operation.HasCommitOffsetsEnd() || !operation.HasConsumer()) {
            continue; //Write operation - handled separately via WriteInfo
        }

        PQ_ENSURE(operation.GetCommitOffsetsBegin() <= (ui64)Max<i64>())("Unexpected begin offset", operation.GetCommitOffsetsBegin());
        PQ_ENSURE(operation.GetCommitOffsetsEnd() <= (ui64)Max<i64>())("Unexpected end offset", operation.GetCommitOffsetsEnd());

        const TString& user = operation.GetConsumer();
        if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "the consumer has been deleted");
            return;
        }
        TUserInfoBase& pendingUserInfo = GetOrCreatePendingUser(user);

        if (!operation.GetForceCommit() && operation.GetCommitOffsetsBegin() > operation.GetCommitOffsetsEnd()) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (begin > end)");
            return;
        }

        if (!operation.GetForceCommit() && pendingUserInfo.Offset != (i64)operation.GetCommitOffsetsBegin()) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (gap)");
            return;
        }

        if (!operation.GetForceCommit() && operation.GetCommitOffsetsEnd() > GetEndOffset()) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (commit to the future)");
            return;
        }

        if (!operation.GetReadSessionId().empty() && operation.GetReadSessionId() != pendingUserInfo.Session) {
            if (IsActive() || operation.GetCommitOffsetsEnd() < GetEndOffset() || pendingUserInfo.Offset != i64(GetEndOffset())) {
                ScheduleReplyPropose(record,
                            NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                            NKikimrPQ::TError::BAD_REQUEST,
                            "session already dead");
                return;
            }
        }

        if ((i64)operation.GetCommitOffsetsEnd() < pendingUserInfo.Offset && !operation.GetReadSessionId().empty()) {
            continue; // this is stale request, answer ok for it
        }

        pendingUserInfo.Offset = operation.GetCommitOffsetsEnd();
    }
    CommitWriteOperations(t);

    ScheduleReplyPropose(record,
                         NKikimrPQ::TEvProposeTransactionResult::COMPLETE,
                         NKikimrPQ::TError::OK,
                         "");

    ScheduleTransactionCompleted(record);
    return;
}

TPartition::EProcessResult TPartition::PreProcessUserActionOrTransaction(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& act,
                                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    if (AffectedUsers.size() >= MAX_USERS) {
        return EProcessResult::Blocked;
    }

    return PreProcessUserAct(*act, &affectedSourceIdsAndConsumers);
}

bool TPartition::ExecUserActionOrTransaction(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& event,
                                             TEvKeyValue::TEvRequest*)
{
    CommitUserAct(*event);
    return true;
}

TPartition::EProcessResult TPartition::PreProcessUserActionOrTransaction(TMessage& msg,
                                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    if (WriteCycleSize >= MAX_WRITE_CYCLE_SIZE) {
        return EProcessResult::Blocked;
    }

    auto result = EProcessResult::Continue;
    if (msg.IsWrite()) {
        result = PreProcessRequest(msg.GetWrite(), affectedSourceIdsAndConsumers);
    } else if (msg.IsRegisterMessageGroup()) {
        result = PreProcessRequest(msg.GetRegisterMessageGroup(), affectedSourceIdsAndConsumers);
    } else if (msg.IsDeregisterMessageGroup()) {
        result = PreProcessRequest(msg.GetDeregisterMessageGroup(), affectedSourceIdsAndConsumers);
    } else if (msg.IsSplitMessageGroup()) {
        result = PreProcessRequest(msg.GetSplitMessageGroup(), affectedSourceIdsAndConsumers);
    } else {
        PQ_ENSURE(msg.IsOwnership());
    }

    return result;
}

bool TPartition::ExecUserActionOrTransaction(TMessage& msg,
                                             TEvKeyValue::TEvRequest* request)
{
    const auto& ctx = ActorContext();
    if (!HaveWriteMsg) {
        BeginHandleRequests(request, ctx);
        if (!DiskIsFull) {
            BeginProcessWrites(ctx);
            BeginAppendHeadWithNewWrites(ctx);
        }
        HaveWriteMsg = true;
    }

    bool doReply = true;
    if (msg.IsWrite()) {
        doReply = ExecRequest(msg.GetWrite(), *Parameters, request);
    } else if (msg.IsRegisterMessageGroup()) {
        ExecRequest(msg.GetRegisterMessageGroup(), *Parameters);
    } else if (msg.IsDeregisterMessageGroup()) {
        ExecRequest(msg.GetDeregisterMessageGroup(), *Parameters);
    } else if (msg.IsSplitMessageGroup()) {
        ExecRequest(msg.GetSplitMessageGroup(), *Parameters);
    } else {
        PQ_ENSURE(msg.IsOwnership());
    }
    if (doReply) {
        EmplaceResponse(std::move(msg), ctx);
    }
    return true;
}

TPartition::EProcessResult TPartition::PreProcessUserAct(TEvPQ::TEvSetClientInfo& act,
                                                         TAffectedSourceIdsAndConsumers* affectedSourceIdsAndConsumers)
{
    PQ_ENSURE(!KVWriteInProgress);

    const TString& user = act.ClientId;
    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET) {
        if (TxAffectedConsumers.contains(user)) {
            return EProcessResult::Blocked;
        }
    }

    if (affectedSourceIdsAndConsumers) {
        ++affectedSourceIdsAndConsumers->WriteKeysSize;
        affectedSourceIdsAndConsumers->ReadConsumers.push_back(user);
    }

    return EProcessResult::Continue;
}

void TPartition::CommitUserAct(TEvPQ::TEvSetClientInfo& act) {
    const bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.Strict);
    const TString& user = act.ClientId;
    RemoveUserAct(user);
    const auto& ctx = ActorContext();
    if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
        switch (act.Type) {
        case TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE:
            break;
        case TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE:
            return;
        default:
            ScheduleReplyError(act.Cookie, act.IsInternal,
                               NPersQueue::NErrorCode::WRONG_COOKIE,
                               "request to deleted read rule");
            return;
        }
    }
    auto& userInfo = GetOrCreatePendingUser(user);

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        LOG_D("Topic '" << TopicName() << "' partition " << Partition
                    << " user " << user << " drop request");

        EmulatePostProcessUserAct(act, userInfo, ctx);

        return;
    }

    if ( //this is retry of current request, answer ok
            act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION
            && act.SessionId == userInfo.Session
            && act.Generation == userInfo.Generation
            && act.Step == userInfo.Step
    ) {
        auto* ui = UsersInfoStorage->GetIfExists(userInfo.User);
        auto ts = ui ? GetTime(*ui, userInfo.Offset) : std::make_pair<TInstant, TInstant>(TInstant::Zero(), TInstant::Zero());

        userInfo.PipeClient = act.PipeClient;
        ScheduleReplyGetClientOffsetOk(act.Cookie,
                                       userInfo.Offset,
                                       ts.first, ts.second, ui->AnyCommits);

        return;
    }

    if (act.Type != TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && act.Type != TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE
            && !act.SessionId.empty() && userInfo.Session != act.SessionId //request to wrong session
            && (act.Type != TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION || !userInfo.Session.empty()) //but allow DropSession request when session is already dropped - for idempotence
            || (act.ClientId != CLIENTID_WITHOUT_CONSUMER && act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && !userInfo.Session.empty()
                 && (act.Generation < userInfo.Generation || act.Generation == userInfo.Generation && act.Step <= userInfo.Step))) { //old generation request
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);

        ScheduleReplyError(act.Cookie, act.IsInternal,
                           NPersQueue::NErrorCode::WRONG_COOKIE,
                           TStringBuilder() << "set offset in already dead session " << act.SessionId << " actual is " << userInfo.Session);

        return;
    }

    if (!act.SessionId.empty() && act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && (i64)act.Offset <= userInfo.Offset) { //this is stale request, answer ok for it
        ScheduleReplyOk(act.Cookie, act.IsInternal);
        return;
    }

    //request in correct session - make it

    ui64 offset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET ? act.Offset : userInfo.Offset);

    if (strictCommitOffset && act.Offset < GetStartOffset()) {
        offset = GetStartOffset();
    }
    ui64 readRuleGeneration = userInfo.ReadRuleGeneration;

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE) {
        readRuleGeneration = act.ReadRuleGeneration;
        offset = 0;
        LOG_D("Topic '" << TopicName() << "' partition " << Partition
                    << " user " << act.ClientId << " reinit request with generation " << readRuleGeneration
        );
    }

    PQ_ENSURE(offset <= (ui64)Max<i64>())("Offset is too big", offset);

    if (offset > GetEndOffset()) {
        if (strictCommitOffset) {
            TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
            ScheduleReplyError(act.Cookie, act.IsInternal,
                            NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE,
                            TStringBuilder() << "strict commit can't set offset " <<  act.Offset << " to future, consumer " << act.ClientId << ", actual end offset is " << GetEndOffset());

            return;
        }
        LOG_W("commit to future - topic " << TopicName() << " partition " << Partition
                << " client " << act.ClientId << " EndOffset " << GetEndOffset() << " offset " << offset
        );
        act.Offset = GetEndOffset();
/*
        TODO:
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE,
            TStringBuilder() << "can't commit to future. Offset " << offset << " EndOffset " << EndOffset);
        userInfo.UserActrs.pop_front();
        continue;
*/
    }

    if (!IsActive() && act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && static_cast<i64>(GetEndOffset()) == userInfo.Offset && offset < GetEndOffset()) {
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ScheduleReplyError(act.Cookie, act.IsInternal,
                           NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST,
                           TStringBuilder() << "set offset " <<  act.Offset << " to past for consumer " << act.ClientId << " for inactive partition");

        return;
    }

    return EmulatePostProcessUserAct(act, userInfo, ActorContext());
}

void TPartition::EmulatePostProcessUserAct(const TEvPQ::TEvSetClientInfo& act,
                                           TUserInfoBase& userInfo,
                                           const TActorContext&)
{
    const TString& user = act.ClientId;
    ui64 offset = act.Offset;
    const std::optional<TString>& committedMetadata = act.CommittedMetadata ? act.CommittedMetadata : userInfo.CommittedMetadata;
    const TString& session = act.SessionId;
    ui32 generation = act.Generation;
    ui32 step = act.Step;
    const ui64 readRuleGeneration = act.ReadRuleGeneration;
    const bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.Strict);

    bool createSession = act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION;
    bool dropSession = act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION;
    bool commitNotFromReadSession = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.SessionId.empty());

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        userInfo.ReadRuleGeneration = 0;
        userInfo.Session = "";
        userInfo.Generation = userInfo.Step = 0;
        userInfo.Offset = 0;
        userInfo.AnyCommits = false;

        LOG_D("Topic '" << TopicName() << "' partition " << Partition << " user " << user
                    << " drop done"
        );
        PendingUsersInfo.erase(user);
    } else if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE) {
        LOG_D("Topic '" << TopicName() << "' partition " << Partition << " user " << user
                    << " reinit with generation " << readRuleGeneration << " done"
        );

        userInfo.ReadRuleGeneration = readRuleGeneration;
        userInfo.Session = "";
        userInfo.PartitionSessionId = 0;
        userInfo.Generation = userInfo.Step = 0;
        userInfo.Offset = 0;
        userInfo.AnyCommits = false;

        if (ImporantOrExtendedAvailabilityPeriod(userInfo)) {
            userInfo.Offset = GetStartOffset();
        }
    } else {
        if (createSession || dropSession) {
            offset = userInfo.Offset;

            auto *ui = UsersInfoStorage->GetIfExists(userInfo.User);
            auto ts = ui ? GetTime(*ui, userInfo.Offset) : std::make_pair<TInstant, TInstant>(TInstant::Zero(), TInstant::Zero());

            ScheduleReplyGetClientOffsetOk(act.Cookie,
                                           offset,
                                           ts.first, ts.second, ui ? ui->AnyCommits : false);
        } else {
            ScheduleReplyOk(act.Cookie, act.IsInternal);
        }

        if (createSession) {
            userInfo.Session = session;
            userInfo.Generation = generation;
            userInfo.Step = step;
            userInfo.PipeClient = act.PipeClient;
            userInfo.PartitionSessionId = act.PartitionSessionId;
        } else if ((dropSession && act.PipeClient == userInfo.PipeClient) || commitNotFromReadSession) {
            userInfo.Session = "";
            userInfo.PartitionSessionId = 0;
            userInfo.Generation = 0;
            userInfo.Step = 0;
            userInfo.PipeClient = {};
        }

        PQ_ENSURE(offset <= (ui64)Max<i64>())("Unexpected Offset", offset);
        LOG_D("Topic '" << TopicName() << "' partition " << Partition << " user " << user
                    << (createSession || dropSession ? " session" : " offset")
                    << " is set to " << offset << " (startOffset " << GetStartOffset() << ") session " << session
        );
        if (strictCommitOffset) {
            userInfo.Offset = std::max(offset, GetStartOffset());
        } else {
            userInfo.Offset = offset;
        }

        userInfo.CommittedMetadata = committedMetadata;
        userInfo.AnyCommits = userInfo.Offset > (i64)GetStartOffset();

        if (LastOffsetHasBeenCommited(userInfo)) {
            SendReadingFinished(user);
        }

        auto counter = createSession ? COUNTER_PQ_CREATE_SESSION_OK : (dropSession ? COUNTER_PQ_DELETE_SESSION_OK : COUNTER_PQ_SET_CLIENT_OFFSET_OK);
        TabletCounters.Cumulative()[counter].Increment(1);
    }
}

void TPartition::ScheduleReplyOk(const ui64 dst, bool internal)
{
    Replies.emplace_back(internal ? SelfId() : TabletActorId,
                         MakeReplyOk(dst, internal).Release());
}

void TPartition::ScheduleReplyGetClientOffsetOk(const ui64 dst,
                                                const i64 offset,
                                                const TInstant writeTimestamp,
                                                const TInstant createTimestamp,
                                                bool consumerHasAnyCommits,
                                                const std::optional<TString>& committedMetadata)
{
    Replies.emplace_back(TabletActorId,
                         MakeReplyGetClientOffsetOk(dst,
                                                    offset,
                                                    writeTimestamp,
                                                    createTimestamp,
                                                    consumerHasAnyCommits, committedMetadata).Release());

}

void TPartition::ScheduleReplyError(const ui64 dst, bool internal,
                                    NPersQueue::NErrorCode::EErrorCode errorCode,
                                    const TString& error)
{
    auto logLevel = NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_INACTIVE == errorCode ? NActors::NLog::PRI_INFO : NActors::NLog::PRI_ERROR;
    LOG(logLevel, "Got error: " << error);
    Replies.emplace_back(internal ? SelfId() : TabletActorId,
                         MakeReplyError(dst,
                                        errorCode,
                                        error).Release());
}

void TPartition::ScheduleReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                      NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode,
                                      NKikimrPQ::TError::EKind kind,
                                      const TString& reason)
{
    LOG_D("schedule TEvPersQueue::TEvProposeTransactionResult(" <<
             NKikimrPQ::TEvProposeTransactionResult_EStatus_Name(statusCode) <<
             ")" <<
             ", reason=" << reason);
    Replies.emplace_back(ActorIdFromProto(event.GetSourceActor()),
                         MakeReplyPropose(event,
                                          statusCode,
                                          kind, reason).Release());
}

void TPartition::ScheduleReplyTxDone(ui64 step, ui64 txId, NWilson::TSpan&& commitSpan)
{
    LOG_D("Schedule reply tx done " << txId);

    if (auto traceId = commitSpan.GetTraceId(); traceId) {
        TxForPersistTraceIds.push_back(traceId);
    }

    auto event = MakeTxDone(step, txId);
    event->Span = std::move(commitSpan);

    Replies.emplace_back(TabletActorId, event.Release());
}

void TPartition::ScheduleDropPartitionLabeledCounters(const TString& group)
{
    Replies.emplace_back(TabletActorId,
                         MakeHolder<TEvPQ::TEvPartitionLabeledCountersDrop>(Partition, group).Release());
}

void TPartition::SchedulePartitionConfigChanged()
{
    Replies.emplace_back(TabletActorId,
                         MakeHolder<TEvPQ::TEvPartitionConfigChanged>(Partition).Release());
}

void TPartition::ScheduleDeletePartitionDone()
{
    Replies.emplace_back(TabletActorId,
                         MakeHolder<TEvPQ::TEvDeletePartitionDone>(Partition).Release());
}

void TPartition::AddCmdDeleteRange(NKikimrClient::TKeyValueRequest& request,
                                   const TKeyPrefix& ikey, const TKeyPrefix& ikeyDeprecated)
{
    auto del = request.AddCmdDeleteRange();
    auto range = del->MutableRange();
    range->SetFrom(ikey.Data(), ikey.Size());
    range->SetTo(ikey.Data(), ikey.Size());
    range->SetIncludeFrom(true);
    range->SetIncludeTo(true);

    del = request.AddCmdDeleteRange();
    range = del->MutableRange();
    range->SetFrom(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    range->SetTo(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    range->SetIncludeFrom(true);
    range->SetIncludeTo(true);
}

void TPartition::AddCmdWrite(NKikimrClient::TKeyValueRequest& request,
                             const TKeyPrefix& ikey, const TKeyPrefix& ikeyDeprecated,
                             ui64 offset, ui32 gen, ui32 step, const TString& session,
                             ui64 readOffsetRewindSum,
                             ui64 readRuleGeneration,
                             bool anyCommits, const std::optional<TString>& committedMetadata)
{
    TBuffer idata;
    {
        NKikimrPQ::TUserInfo userData;
        userData.SetOffset(offset);
        userData.SetGeneration(gen);
        userData.SetStep(step);
        userData.SetSession(session);
        userData.SetOffsetRewindSum(readOffsetRewindSum);
        userData.SetReadRuleGeneration(readRuleGeneration);
        userData.SetAnyCommits(anyCommits);
        if (committedMetadata.has_value()) {
            userData.SetCommittedMetadata(*committedMetadata);
        }

        TString out;
        Y_PROTOBUF_SUPPRESS_NODISCARD userData.SerializeToString(&out);

        idata.Append(out.c_str(), out.size());
    }

    auto write = request.AddCmdWrite();
    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(idata.Data(), idata.Size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

    TBuffer idataDeprecated = NDeprecatedUserData::Serialize(offset, gen, step, session);

    write = request.AddCmdWrite();
    write->SetKey(ikeyDeprecated.Data(), ikeyDeprecated.Size());
    write->SetValue(idataDeprecated.Data(), idataDeprecated.Size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

void TPartition::AddCmdWriteTxMeta(NKikimrClient::TKeyValueRequest& request)
{
    if (!TxIdHasChanged) {
        return;
    }

    PQ_ENSURE(PlanStep.Defined());
    PQ_ENSURE(TxId.Defined());

    TKeyPrefix ikey(TKeyPrefix::TypeTxMeta, Partition);

    NKikimrPQ::TPartitionTxMeta meta;
    meta.SetPlanStep(*PlanStep);
    meta.SetTxId(*TxId);

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);

    auto write = request.AddCmdWrite();
    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(out.c_str(), out.size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

void TPartition::AddCmdWriteUserInfos(NKikimrClient::TKeyValueRequest& request)
{
    for (auto& user : AffectedUsers) {
        TKeyPrefix ikey(TKeyPrefix::TypeInfo, Partition, TKeyPrefix::MarkUser);
        ikey.Append(user.c_str(), user.size());
        TKeyPrefix ikeyDeprecated(TKeyPrefix::TypeInfo, Partition, TKeyPrefix::MarkUserDeprecated);
        ikeyDeprecated.Append(user.c_str(), user.size());

        if (TUserInfoBase* userInfo = GetPendingUserIfExists(user)) {
            auto *ui = UsersInfoStorage->GetIfExists(user);

            AddCmdWrite(request,
                        ikey, ikeyDeprecated,
                        userInfo->Offset, userInfo->Generation, userInfo->Step,
                        userInfo->Session,
                        ui ? ui->ReadOffsetRewindSum : 0,
                        userInfo->ReadRuleGeneration,
                        userInfo->AnyCommits, userInfo->CommittedMetadata);
        } else {
            AddCmdDeleteRange(request,
                              ikey, ikeyDeprecated);

            DropDataOfMLPConsumer(request, user);
        }
    }
}

void TPartition::AddCmdWriteConfig(NKikimrClient::TKeyValueRequest& request)
{
    if (!ChangeConfig) {
        return;
    }

    TString key = GetKeyConfig();

    TString data;
    PQ_ENSURE(ChangeConfig->Config.SerializeToString(&data));

    auto write = request.AddCmdWrite();
    write->SetKey(key.data(), key.size());
    write->SetValue(data.data(), data.size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

TUserInfoBase& TPartition::GetOrCreatePendingUser(const TString& user,
                                                  TMaybe<ui64> readRuleGeneration)
{
    TUserInfoBase* userInfo = nullptr;
    auto pendingUserIt = PendingUsersInfo.find(user);
    if (pendingUserIt == PendingUsersInfo.end()) {
        auto userIt = UsersInfoStorage->GetIfExists(user);
        auto [newPendingUserIt, _] = PendingUsersInfo.emplace(user, UsersInfoStorage->CreateUserInfo(user, readRuleGeneration));

        if (userIt) {
            newPendingUserIt->second.Session = userIt->Session;
            newPendingUserIt->second.PartitionSessionId = userIt->PartitionSessionId;
            newPendingUserIt->second.PipeClient = userIt->PipeClient;

            newPendingUserIt->second.Generation = userIt->Generation;
            newPendingUserIt->second.Step = userIt->Step;
            newPendingUserIt->second.Offset = userIt->Offset;
            newPendingUserIt->second.CommittedMetadata = userIt->CommittedMetadata;
            newPendingUserIt->second.ReadRuleGeneration = userIt->ReadRuleGeneration;
            newPendingUserIt->second.Important = userIt->Important;
            newPendingUserIt->second.AvailabilityPeriod = userIt->AvailabilityPeriod;
            newPendingUserIt->second.ReadFromTimestamp = userIt->ReadFromTimestamp;
        }

        userInfo = &newPendingUserIt->second;
    } else {
        userInfo = &pendingUserIt->second;
    }
    AffectedUsers.insert(user);

    return *userInfo;
}

TUserInfoBase* TPartition::GetPendingUserIfExists(const TString& user)
{
    if (auto i = PendingUsersInfo.find(user); i != PendingUsersInfo.end()) {
        return &i->second;
    }

    return nullptr;
}

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyOk(const ui64 dst, bool internal)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst, internal);
    NKikimrClient::TResponse& resp = *response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    return response;
}

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyGetClientOffsetOk(const ui64 dst,
                                                                        const i64 offset,
                                                                        const TInstant writeTimestamp,
                                                                        const TInstant createTimestamp,
                                                                        bool consumerHasAnyCommits,
                                                                        const std::optional<TString>& committedMetadata)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst, false);
    NKikimrClient::TResponse& resp = *response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto user = resp.MutablePartitionResponse()->MutableCmdGetClientOffsetResult();
    if (offset > -1)
        user->SetOffset(offset);

    if (committedMetadata.has_value()) {
        user->SetCommittedMetadata(*committedMetadata);
    }
    if (writeTimestamp)
        user->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
    if (createTimestamp) {
        PQ_ENSURE(writeTimestamp);
        user->SetCreateTimestampMS(createTimestamp.MilliSeconds());
    }
    user->SetEndOffset(GetEndOffset());
    user->SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
    if (IsActive() || (offset > -1 && offset < (i64)GetEndOffset())) {
        user->SetSizeLag(GetSizeLag(offset));
    } else {
        user->SetSizeLag(0);
    }
    user->SetClientHasAnyCommits(consumerHasAnyCommits);
    return response;
}
THolder<TEvPQ::TEvError> TPartition::MakeReplyError(const ui64 dst,
                                                    NPersQueue::NErrorCode::EErrorCode errorCode,
                                                    const TString& error,
                                                    bool internal)
{
    //
    // FIXME(abcdef): в ReplyPersQueueError есть дополнительные действия
    //
    return MakeHolder<TEvPQ::TEvError>(errorCode, error, dst, internal);
}

THolder<TEvPersQueue::TEvProposeTransactionResult> TPartition::MakeReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                                                                NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode,
                                                                                NKikimrPQ::TError::EKind kind,
                                                                                const TString& reason)
{
    auto response = MakeHolder<TEvPersQueue::TEvProposeTransactionResult>();

    response->Record.SetOrigin(TabletId);
    response->Record.SetStatus(statusCode);
    response->Record.SetTxId(event.GetTxId());

    if (kind != NKikimrPQ::TError::OK) {
        auto* error = response->Record.MutableErrors()->Add();
        error->SetKind(kind);
        error->SetReason(reason);
    }

    return response;
}

THolder<TEvPQ::TEvTxDone> TPartition::MakeTxDone(ui64 step, ui64 txId) const
{
    return MakeHolder<TEvPQ::TEvTxDone>(step, txId, Partition);
}

void TPartition::ScheduleUpdateAvailableSize(const TActorContext& ctx) {
    ctx.Schedule(UPDATE_AVAIL_SIZE_INTERVAL, new TEvPQ::TEvUpdateAvailableSize());
}

ui32 TPartition::NextChannel(bool isHead, ui32 blobSize) {

    if (isHead) {
        ui32 i = 0;
        for (ui32 j = 1; j < TotalChannelWritesByHead.size(); ++j) {
            if (TotalChannelWritesByHead[j] < TotalChannelWritesByHead[i])
                i = j;
        }
        TotalChannelWritesByHead[i] += blobSize;

        return i;
    };

    ui32 res = Channel;
    Channel = (Channel + 1) % NumChannels;

    return res;
}

void TPartition::Handle(TEvPQ::TEvApproveWriteQuota::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Get()->Cookie;
    LOG_D("Got quota." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << ": Cookie: " << cookie
    );

    // Search for proper request
    PQ_ENSURE(TopicQuotaRequestCookie == cookie);
    ConsumeBlobQuota();
    TopicQuotaRequestCookie = 0;
    for (auto& r : QuotaWaitingRequests) {
        r.WaitQuotaSpan.End();
    }

    RemoveMessagesToQueue(QuotaWaitingRequests);

    // Metrics
    TopicQuotaWaitTimeForCurrentBlob = ev->Get()->AccountQuotaWaitTime;
    PartitionQuotaWaitTimeForCurrentBlob = ev->Get()->PartitionQuotaWaitTime;
    if (TopicWriteQuotaWaitCounter) {
        TopicWriteQuotaWaitCounter->IncFor(TopicQuotaWaitTimeForCurrentBlob.MilliSeconds());
    }

    if (NeedDeletePartition) {
        // deferred TEvPQ::TEvDeletePartition
        DeletePartitionState = DELETION_INITED;
    } else {
        RequestBlobQuota();
    }

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(NQuoterEvents::TEvQuotaCountersUpdated::TPtr& ev, const TActorContext&) {
    if (ev->Get()->ForWriteQuota) {
        LOG_A("Got TEvQuotaCountersUpdated for write counters, this is unexpected. Event ignored");
        return;
    } else if (PartitionCountersLabeled) {
        PartitionCountersLabeled->GetCounters()[METRIC_READ_INFLIGHT_LIMIT_THROTTLED].Set(ev->Get()->AvgInflightLimitThrottledMicroseconds);
    }
}

size_t TPartition::GetQuotaRequestSize(const TEvKeyValue::TEvRequest& request) {
    if (AppData()->PQConfig.GetQuotingConfig().GetTopicWriteQuotaEntityToLimit() ==
        NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE) {
        return WriteNewSize;
    } else {
        return std::accumulate(request.Record.GetCmdWrite().begin(), request.Record.GetCmdWrite().end(), 0ul,
                               [](size_t sum, const auto& el) { return sum + el.GetValue().size(); });
    }
}

void TPartition::CreateMirrorerActor() {
    Mirrorer = MakeHolder<TMirrorerInfo>(
        RegisterWithSameMailbox(CreateMirrorer(TabletId, TabletActorId, SelfId(), TopicConverter, Partition.InternalPartitionId, IsLocalDC, GetEndOffset(), Config.GetPartitionConfig().GetMirrorFrom(), TabletCounters))
    );
}

bool TPartition::IsQuotingEnabled() const
{
    return NPQ::IsQuotingEnabled(AppData()->PQConfig,
                                 IsLocalDC);
}

void TPartition::Handle(TEvPQ::TEvSubDomainStatus::TPtr& ev, const TActorContext& ctx)
{
    const TEvPQ::TEvSubDomainStatus& event = *ev->Get();

    bool statusChanged = SubDomainOutOfSpace != event.SubDomainOutOfSpace();
    SubDomainOutOfSpace = event.SubDomainOutOfSpace();

    if (statusChanged) {
        LOG_I("SubDomainOutOfSpace was changed." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << "." <<
            " SubDomainOutOfSpace: " << SubDomainOutOfSpace
        );

        if (!SubDomainOutOfSpace) {
            ProcessTxsAndUserActs(ctx);
            ProcessReserveRequests(ctx);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext&) {
    auto& record = ev->Get()->Record;

    if (Partition.InternalPartitionId != record.GetPartition()) {
        LOG_I("TEvCheckPartitionStatusRequest for wrong partition " << record.GetPartition() << "." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << "."
        );
        return;
    }

    auto response = MakeHolder<TEvPQ::TEvCheckPartitionStatusResponse>();
    response->Record.SetStatus(PartitionConfig ? PartitionConfig->GetStatus() : NKikimrPQ::ETopicPartitionStatus::Active);

    if (record.HasSourceId()) {
        auto sit = SourceIdStorage.GetInMemorySourceIds().find(NSourceIdEncoding::EncodeSimple(record.GetSourceId()));
        if (sit != SourceIdStorage.GetInMemorySourceIds().end()) {
            response->Record.SetSeqNo(sit->second.SeqNo);
        }
    }

    Send(ev->Sender, response.Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvDeletePartition::TPtr& ev, const TActorContext&)
{
    LOG_D("HandleOnInit TEvPQ::TEvDeletePartition");

    PQ_ENSURE(IsSupportive());

    AddPendingEvent(ev);
}

template <>
void TPartition::ProcessPendingEvent(std::unique_ptr<TEvPQ::TEvDeletePartition> ev, const TActorContext& ctx)
{
    Y_UNUSED(ev);

    PQ_ENSURE(IsSupportive());
    PQ_ENSURE(DeletePartitionState == DELETION_NOT_INITED);

    NeedDeletePartition = true;

    if (TopicQuotaRequestCookie != 0) {
        // wait for TEvPQ::TEvApproveWriteQuota
        return;
    }

    DeletePartitionState = DELETION_INITED;

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvDeletePartition::TPtr& ev, const TActorContext& ctx)
{
    LOG_D("Handle TEvPQ::TEvDeletePartition");

    ProcessPendingEvent(ev, ctx);
}

void TPartition::ScheduleNegativeReplies()
{
    auto processQueue = [&](std::deque<TUserActionAndTransactionEvent>& queue) {
        for (auto& event : queue) {
            std::visit(TOverloaded{
                [this](TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& v) {
                    ScheduleNegativeReply(*v);
                },
                [this](TSimpleSharedPtr<TTransaction>& v) {
                    if (v->ProposeTransaction) {
                        ScheduleNegativeReply(*v->ProposeTransaction);
                    } else {
                        ScheduleNegativeReply(*v);
                    }
                },
                [this](TMessage& v) {
                    ScheduleNegativeReply(v);
                }
            }, event.Event);
        }
        queue.clear();
    };

    processQueue(UserActionAndTransactionEvents);
    processQueue(UserActionAndTxPendingCommit);
}

void TPartition::AddCmdDeleteRangeForAllKeys(TEvKeyValue::TEvRequest& request)
{
    NPQ::AddCmdDeleteRange(request, TKeyPrefix::TypeInfo, Partition);
    NPQ::AddCmdDeleteRange(request, TKeyPrefix::TypeData, Partition);
    NPQ::AddCmdDeleteRange(request, TKeyPrefix::TypeTmpData, Partition);
    NPQ::AddCmdDeleteRange(request, TKeyPrefix::TypeMeta, Partition);
    NPQ::AddCmdDeleteRange(request, TKeyPrefix::TypeTxMeta, Partition);
}

void TPartition::ScheduleNegativeReply(const TEvPQ::TEvSetClientInfo&)
{
    Y_ABORT("The supportive partition does not accept read operations");
}

void TPartition::ScheduleNegativeReply(const TEvPersQueue::TEvProposeTransaction&)
{
    Y_ABORT("The supportive partition does not accept immediate transactions");
}

void TPartition::ScheduleNegativeReply(const TTransaction&)
{
    Y_ABORT("The supportive partition does not accept distribute transactions");
}

void TPartition::ScheduleNegativeReply(const TMessage& msg)
{
    ScheduleReplyError(msg.GetCookie(), false, NPersQueue::NErrorCode::ERROR, "The transaction is completed");
}

void TPartition::ScheduleTransactionCompleted(const NKikimrPQ::TEvProposeTransaction& tx)
{
    PQ_ENSURE(tx.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    PQ_ENSURE(tx.HasData());

    TMaybe<TWriteId> writeId;
    if (tx.GetData().HasWriteId()) {
        writeId = GetWriteId(tx.GetData());
    }

    Replies.emplace_back(TabletActorId,
                         MakeHolder<TEvPQ::TEvTransactionCompleted>(writeId).Release());
}

void TPartition::ProcessPendingEvents(const TActorContext& ctx)
{
    LOG_D("Process pending events. Count " << PendingEvents.size());

    while (!PendingEvents.empty()) {
        auto ev = std::move(PendingEvents.front());
        PendingEvents.pop_front();

        auto visitor = [this, &ctx](auto&& v) {
            using T = std::decay_t<decltype(v)>;
            ProcessPendingEvent(std::forward<T>(v), ctx);
        };

        std::visit(visitor, std::move(ev));
    }
}

const NKikimrPQ::TPQTabletConfig::TPartition* TPartition::GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config)
{
    return NPQ::GetPartitionConfig(config, Partition.OriginalPartitionId);
}

bool TPartition::IsSupportive() const
{
    return Partition.IsSupportivePartition();
}

bool TPartition::IsKeyCompactionEnabled() const {
    return Config.GetEnableCompactification() && AppData()->FeatureFlags.GetEnableTopicCompactificationByKey() && !IsSupportive();
}

void TPartition::AttachPersistRequestSpan(NWilson::TSpan& span)
{
    if (span) {
        if (!PersistRequestSpan) {
            PersistRequestSpan = NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId::NewTraceId(span.GetTraceId().GetVerbosity(), Max<ui32>()), "Topic.Partition.PersistRequest");
        }
        span.Link(PersistRequestSpan.GetTraceId());
    }
}

void TPartition::SendCompacterWriteRequest(THolder<TEvKeyValue::TEvRequest>&& request) {
    Y_ENSURE(!CompacterKvRequestInflight);
    Y_ENSURE(!CompacterKvRequest);
    LOG_D("Topic '" << TopicConverter->GetClientsideName() << "'" << " partition " << Partition
                       << ": Acquire RW Lock");
    Send(ReadQuotaTrackerActor, new TEvPQ::TEvAcquireExclusiveLock());
    CompacterKvRequestInflight = true;
    CompacterKvRequest = std::move(request);
}

void TPartition::Handle(TEvPQ::TEvExclusiveLockAcquired::TPtr&) {
    LOG_D("Topic '" << TopicConverter->GetClientsideName() << "'" << " partition " << Partition
                       << ": Acquired RW Lock, send compacter KV request    ");
    Send(BlobCache, CompacterKvRequest.Release(), 0, 0, PersistRequestSpan.GetTraceId());
}

IActor* CreatePartitionActor(ui64 tabletId, const TPartitionId& partition, const TActorId& tablet, ui32 tabletGeneration,
    const TActorId& blobCache, const NPersQueue::TTopicConverterPtr& topicConverter, TString dcId, bool isServerless,
    const NKikimrPQ::TPQTabletConfig& config, const std::shared_ptr<TTabletCountersBase>& counters, bool SubDomainOutOfSpace,
    ui32 numChannels, const TActorId& writeQuoterActorId,
    TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> samplingControl, bool newPartition
) {

    return new TPartition(tabletId, partition, tablet, tabletGeneration, blobCache, topicConverter, dcId, isServerless,
        config, counters, SubDomainOutOfSpace, numChannels, writeQuoterActorId, samplingControl,
        newPartition);
}

::NMonitoring::TDynamicCounterPtr TPartition::GetPerPartitionCounterSubgroup() const {
    auto counters = AppData(ActorContext())->Counters;
    if (!counters) {
        return nullptr;
    }
    if (AppData()->PQConfig.GetTopicsAreFirstClassCitizen()) {
        auto s = counters
            ->GetSubgroup("counters", IsServerless ? "topics_per_partition_serverless" : "topics_per_partition")
            ->GetSubgroup("host", "");
        if (!MonitoringProjectId.empty()) {
            s = s->GetSubgroup("monitoring_project_id", MonitoringProjectId);
        }
        return s
            ->GetSubgroup("database", Config.GetYdbDatabasePath())
            ->GetSubgroup("cloud_id", CloudId)
            ->GetSubgroup("folder_id", FolderId)
            ->GetSubgroup("database_id", DbId)
            ->GetSubgroup("topic", EscapeBadChars(TopicName()))
            ->GetSubgroup("partition_id", ToString(Partition.InternalPartitionId));
    } else {
        auto s = counters
            ->GetSubgroup("counters", "topics_per_partition")
            ->GetSubgroup("host", "cluster");
        if (!MonitoringProjectId.empty()) {
            s = s->GetSubgroup("monitoring_project_id", MonitoringProjectId);
        }
        return s
            ->GetSubgroup("Account", TopicConverter->GetAccount())
            ->GetSubgroup("TopicPath", TopicConverter->GetFederationPath())
            ->GetSubgroup("OriginDC", to_title(TopicConverter->GetCluster()))
            ->GetSubgroup("Partition", ToString(Partition.InternalPartitionId));
    }
}

void TPartition::SetupDetailedMetrics() {
    if (!DetailedMetricsAreEnabled(Config) || IsSupportive()) {
        return;
    }
    if (WriteTimeLagMsByLastWritePerPartition) {
        // Don't recreate the counters if they already exist.
        return;
    }

    auto subgroup = GetPerPartitionCounterSubgroup();
    if (!subgroup) {
        return;
    }

    auto getCounter = [&](const TString& forFCC, const TString& forFederation, bool deriv) {
        bool fcc = AppData()->PQConfig.GetTopicsAreFirstClassCitizen();
        return subgroup->GetExpiringNamedCounter(
            fcc ? "name" : "sensor",
            fcc ? forFCC : forFederation,
            deriv);
    };

    WriteTimeLagMsByLastWritePerPartition = getCounter("topic.partition.write.lag_milliseconds", "WriteTimeLagMsByLastWrite", false);

    SourceIdCountPerPartition = getCounter("topic.partition.producers_count", "SourceIdCount", false);
    TimeSinceLastWriteMsPerPartition = getCounter("topic.partition.write.idle_milliseconds", "TimeSinceLastWriteMs", false);

    BytesWrittenPerPartition = getCounter("topic.partition.write.bytes", "BytesWrittenPerPartition", true);
    MessagesWrittenPerPartition = getCounter("topic.partition.write.messages", "MessagesWrittenPerPartition", true);
}

void TPartition::ResetDetailedMetrics() {
    UsersInfoStorage->ResetDetailedMetrics();

    WriteTimeLagMsByLastWritePerPartition.Reset();
    SourceIdCountPerPartition.Reset();
    TimeSinceLastWriteMsPerPartition.Reset();
    BytesWrittenPerPartition.Reset();
    MessagesWrittenPerPartition.Reset();
}

bool IsImportant(const NKikimrPQ::TPQTabletConfig::TConsumer& consumer) {
    return consumer.GetImportant() || consumer.GetType() == NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP;
}

} // namespace NKikimr::NPQ
