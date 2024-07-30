#include "event_helpers.h"
#include "mirrorer.h"
#include "offload_actor.h"
#include "partition_util.h"
#include "partition.h"
#include "partition_log.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/lib/base/msgbus.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/folder/path.h>
#include <util/string/escape.h>
#include <util/system/byteorder.h>

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

auto GetStepAndTxId(ui64 step, ui64 txId)
{
    return std::make_pair(step, txId);
}

template<class E>
auto GetStepAndTxId(const E& event)
{
    return GetStepAndTxId(event.Step, event.TxId);
}

bool TPartition::LastOffsetHasBeenCommited(const TUserInfo& userInfo) const {
    return !IsActive() && static_cast<ui64>(std::max<i64>(userInfo.Offset, 0)) == EndOffset;
}

struct TMirrorerInfo {
    TMirrorerInfo(const TActorId& actor, const TTabletCountersBase& baseline)
    : Actor(actor) {
        Baseline.Populate(baseline);
    }

    TActorId Actor;
    TTabletCountersBase Baseline;
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
    return TStringBuilder() << "[PQ: " << TabletID << ", Partition:" << Partition << ", State:" << state << "] ";
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
    if (container.empty()) {
        return offset;
    }
    auto it = std::lower_bound(container.begin(), container.end(), timestamp,
                    [](const TDataKey& p, const TInstant timestamp) { return timestamp > p.Timestamp; });
    if (it == container.end()) {
        return offset;
    } else {
        return it->Key.GetOffset();
    }
}

void TPartition::ReplyError(const TActorContext& ctx, const ui64 dst, NPersQueue::NErrorCode::EErrorCode errorCode, const TString& error) {
    ReplyPersQueueError(
        dst == 0 ? ctx.SelfID : Tablet, ctx, TabletID, TopicName(), Partition,
        TabletCounters, NKikimrServices::PERSQUEUE, dst, errorCode, error, true
    );
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
    ctx.Send(Tablet, MakeReplyOk(dst).Release());
}

void TPartition::ReplyGetClientOffsetOk(const TActorContext& ctx, const ui64 dst, const i64 offset,
    const TInstant writeTimestamp, const TInstant createTimestamp) {
    ctx.Send(Tablet, MakeReplyGetClientOffsetOk(dst, offset, writeTimestamp, createTimestamp).Release());
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
                       const NKikimrPQ::TPQTabletConfig& tabletConfig, const TTabletCountersBase& counters, bool subDomainOutOfSpace, ui32 numChannels,
                       const TActorId& writeQuoterActorId, bool newPartition, TVector<TTransaction> distrTxs)
    : Initializer(this)
    , TabletID(tabletId)
    , TabletGeneration(tabletGeneration)
    , Partition(partition)
    , TabletConfig(tabletConfig)
    , Counters(counters)
    , TopicConverter(topicConverter)
    , IsLocalDC(TabletConfig.GetLocalDC())
    , DCId(std::move(dcId))
    , PartitionGraph()
    , SourceManager(*this)
    , StartOffset(0)
    , EndOffset(0)
    , WriteInflightSize(0)
    , Tablet(tablet)
    , BlobCache(blobCache)
    , PartitionedBlob(partition, 0, 0, 0, 0, 0, Head, NewHead, true, false, 8_MB)
    , NewHeadKey{TKey{}, 0, TInstant::Zero(), 0}
    , BodySize(0)
    , MaxWriteResponsesSize(0)
    , GapSize(0)
    , IsServerless(isServerless)
    , ReadingTimestamp(false)
    , Cookie(0)
    , InitDuration(TDuration::Zero())
    , InitDone(false)
    , NewPartition(newPartition)
    , Subscriber(partition, TabletCounters, Tablet)
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
{
    TabletCounters.Populate(Counters);

    if (!distrTxs.empty()) {
        for (auto& tx : distrTxs) {
            if (!tx.Predicate.Defined()) {
                UserActionAndTransactionEvents.emplace_back(MakeSimpleShared<TTransaction>(std::move(tx)));
            } else if (*tx.Predicate) {
                auto txId = tx.GetTxId();
                auto txPtr = MakeSimpleShared<TTransaction>(std::move(tx));
                BatchingState = ETxBatchingState::Executing;
                if (txId.Defined()) {
                    TransactionsInflight.insert(std::make_pair(*txId, txPtr));
                }
                UserActionAndTxPendingCommit.emplace_back(std::move(txPtr));
            }
        }
    }
}

void TPartition::EmplaceResponse(TMessage&& message, const TActorContext& ctx) {
    const auto now = ctx.Now();
    Responses.emplace_back(
        message.Body,
        (now - TInstant::Zero()) - message.QueueTime,
        now
    );
}

ui64 TPartition::UserDataSize() const {
    if (DataKeysBody.size() <= 1) {
        // tiny optimization - we do not meter very small queues up to 16MB
        return 0;
    }

    // We assume that DataKyesBody contains an up-to-date set of blobs, their relevance is
    // maintained by the background process. However, the last block may contain several irrelevant
    // messages. Because of them, we throw out the size of the entire blob.
    auto size = Size();
    auto lastBlobSize = DataKeysBody[0].Size;
    Y_DEBUG_ABORT_UNLESS(size >= lastBlobSize, "Metering data size must be positive");
    return size >= lastBlobSize ? size - lastBlobSize : 0;
}

ui64 TPartition::MeteringDataSize(TInstant now) const {
    if (IsActive() || NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == Config.GetMeteringMode()) {
        return UserDataSize();
    } else {
        ui64 size = 0;
        auto expirationTimestamp = now - TDuration::Seconds(Config.GetPartitionConfig().GetLifetimeSeconds()) - WAKE_TIMEOUT;
        for (size_t i = 1; i < DataKeysBody.size() && DataKeysBody[i].Timestamp < expirationTimestamp; ++i) {
            size += DataKeysBody[i].Size;
        }
        return size;
    }
}

ui64 TPartition::ReserveSize() const {
    return IsActive() ? TopicPartitionReserveSize(Config) : 0;
}

ui64 TPartition::StorageSize(const TActorContext&) const {
    return std::max<ui64>(UserDataSize(), ReserveSize());
}

ui64 TPartition::UsedReserveSize(const TActorContext& ctx) const {
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

ui64 TPartition::ImportantClientsMinOffset() const {
    ui64 minOffset = EndOffset;
    for (const auto& consumer : Config.GetConsumers()) {
        if (!consumer.GetImportant()) {
            continue;
        }

        const TUserInfo* userInfo = UsersInfoStorage->GetIfExists(consumer.GetName());
        ui64 curOffset = StartOffset;
        if (userInfo && userInfo->Offset >= 0) //-1 means no offset
            curOffset = userInfo->Offset;
        minOffset = Min<ui64>(minOffset, curOffset);
    }

    return minOffset;
}

void TPartition::HandleWakeup(const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);

    ctx.Schedule(WAKE_TIMEOUT, new TEvents::TEvWakeup());
    ctx.Send(Tablet, new TEvPQ::TEvPartitionCounters(Partition, TabletCounters));

    ui64 usedStorage = GetUsedStorage(ctx.Now());
    if (usedStorage > 0) {
        ctx.Send(Tablet, new TEvPQ::TEvMetering(EMeteringJson::UsedStorageV1, usedStorage));
    }

    ReportCounters(ctx);

    ProcessHasDataRequests(ctx);

    const auto now = ctx.Now();
    for (auto& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.UpdateReadingTimeAndState(now);
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
}

void TPartition::AddMetaKey(TEvKeyValue::TEvRequest* request) {
    //Set Start Offset
    auto write = request->Record.AddCmdWrite();
    TKeyPrefix ikey(TKeyPrefix::TypeMeta, Partition);

    NKikimrPQ::TPartitionMeta meta;
    meta.SetStartOffset(StartOffset);
    meta.SetEndOffset(Max(NewHead.GetNextOffset(), EndOffset));
    meta.SetSubDomainOutOfSpace(SubDomainOutOfSpace);

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
    write->SetValue(out.c_str(), out.size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

bool TPartition::CleanUp(TEvKeyValue::TEvRequest* request, const TActorContext& ctx) {
    bool haveChanges = CleanUpBlobs(request, ctx);

    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "Have " << request->Record.CmdDeleteRangeSize() << " items to delete old stuff");

    haveChanges |= SourceIdStorage.DropOldSourceIds(request, ctx.Now(), StartOffset, Partition,
                                                    Config.GetPartitionConfig());
    if (haveChanges) {
        SourceIdStorage.MarkOwnersForDeletedSourceId(Owners);
    }

    LOG_TRACE_S(ctx, NKikimrServices::PERSQUEUE, "Have " << request->Record.CmdDeleteRangeSize() << " items to delete all stuff. "
            << "Delete command " << request->ToString());

    return haveChanges;
}

bool TPartition::CleanUpBlobs(TEvKeyValue::TEvRequest *request, const TActorContext& ctx) {
    if (StartOffset == EndOffset || DataKeysBody.size() <= 1) {
        return false;
    }

    const auto& partConfig = Config.GetPartitionConfig();
    const TDuration lifetimeLimit{TDuration::Seconds(partConfig.GetLifetimeSeconds())};

    const bool hasStorageLimit = partConfig.HasStorageLimitBytes();
    const auto now = ctx.Now();
    const ui64 importantConsumerMinOffset = ImportantClientsMinOffset();

    bool hasDrop = false;
    while(DataKeysBody.size() > 1) {
        auto& nextKey = DataKeysBody[1].Key;
        if (importantConsumerMinOffset < nextKey.GetOffset()) {
            // The first message in the next blob was not read by an important consumer.
            // We also save the current blob, since not all messages from it could be read.
            break;
        }
        if (importantConsumerMinOffset == nextKey.GetOffset() && nextKey.GetPartNo() != 0) {
            // We save all the blobs that contain parts of the last message read by an important consumer.
            break;
        }

        auto& firstKey = DataKeysBody.front();
        if (hasStorageLimit) {
            const auto bodySize = BodySize - firstKey.Size;
            if (bodySize < partConfig.GetStorageLimitBytes()) {
                break;
            }
        } else {
            if (now < firstKey.Timestamp + lifetimeLimit) {
                break;
            }
        }

        BodySize -= firstKey.Size;
        DataKeysBody.pop_front();

        if (!GapOffsets.empty() && nextKey.GetOffset() == GapOffsets.front().second) {
            GapSize -= GapOffsets.front().second - GapOffsets.front().first;
            GapOffsets.pop_front();
        }

        hasDrop = true;
    }

    Y_ABORT_UNLESS(!DataKeysBody.empty());

    if (!hasDrop) {
        return false;
    }

    const auto& lastKey = DataKeysBody.front().Key;

    StartOffset = lastKey.GetOffset();
    if (lastKey.GetPartNo() > 0) {
        ++StartOffset;
    }

    TKey firstKey(TKeyPrefix::TypeData, Partition, 0, 0, 0, 0); //will drop all that could not be dropped before of case of full disks

    auto del = request->Record.AddCmdDeleteRange();
    auto range = del->MutableRange();
    range->SetFrom(firstKey.Data(), firstKey.Size());
    range->SetIncludeFrom(true);
    range->SetTo(lastKey.Data(), lastKey.Size());
    range->SetIncludeTo(StartOffset == EndOffset);

    return true;
}

void TPartition::Handle(TEvPQ::TEvMirrorerCounters::TPtr& ev, const TActorContext& /*ctx*/) {
    if (Mirrorer) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(Mirrorer->Baseline);
        TabletCounters.Populate(*diff.Get());
        ev->Get()->Counters.RememberCurrentStateAsBaseline(Mirrorer->Baseline);
    }
}

void TPartition::DestroyActor(const TActorContext& ctx)
{
    // Reply to all outstanding requests in order to destroy corresponding actors

    TStringBuilder ss;
    ss << "Tablet is restarting, topic '" << TopicName() << "'";

    for (const auto& ev : WaitToChangeOwner) {
        ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::INITIALIZING, ss);
    }

    for (const auto& w : PendingRequests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::INITIALIZING, ss);
    }

    for (const auto& w : Responses) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::INITIALIZING, TStringBuilder() << ss << " (WriteResponses)");
    }

    for (const auto& ri : ReadInfo) {
        ReplyError(ctx, ri.second.Destination, NPersQueue::NErrorCode::INITIALIZING,
            TStringBuilder() << ss << " (ReadInfo) cookie " << ri.first);
    }

    if (Mirrorer) {
        Send(Mirrorer->Actor, new TEvents::TEvPoisonPill());
    }

    if (UsersInfoStorage.Defined()) {
        UsersInfoStorage->Clear(ctx);
    }

    if (!IsSupportive()) {
        Send(ReadQuotaTrackerActor, new TEvents::TEvPoisonPill());
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
    if (StartOffset == EndOffset && EndOffset == 0) {
        for (auto& [user, info] : UsersInfoStorage->GetAll()) {
            if (info.Offset > 0 && StartOffset < (ui64)info.Offset) {
                 Head.Offset = EndOffset = StartOffset = info.Offset;
            }
        }
    }

    LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "init complete for topic '" << TopicName() << "' partition " << Partition << " generation " << TabletGeneration << " " << ctx.SelfID
    );

    TStringBuilder ss;
    ss << "SYNC INIT topic " << TopicName() << " partitition " << Partition
       << " so " << StartOffset << " endOffset " << EndOffset << " Head " << Head << "\n";
    for (const auto& s : SourceIdStorage.GetInMemorySourceIds()) {
        ss << "SYNC INIT sourceId " << s.first << " seqNo " << s.second.SeqNo << " offset " << s.second.Offset << "\n";
    }
    for (const auto& h : DataKeysBody) {
        ss << "SYNC INIT DATA KEY: " << TString(h.Key.Data(), h.Key.Size()) << " size " << h.Size << "\n";
    }
    for (const auto& h : HeadKeys) {
        ss << "SYNC INIT HEAD KEY: " << TString(h.Key.Data(), h.Key.Size()) << " size " << h.Size << "\n";
    }
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, ss);

    CheckHeadConsistency();

    Become(&TThis::StateIdle);
    InitDuration = ctx.Now() - CreationTime;
    InitDone = true;
    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_INIT].IncrementFor(InitDuration.MilliSeconds());

    FillReadFromTimestamps(ctx);
    ResendPendingEvents(ctx);
    ProcessTxsAndUserActs(ctx);

    ctx.Send(ctx.SelfID, new TEvents::TEvWakeup());
    ctx.Send(Tablet, new TEvPQ::TEvInitComplete(Partition));

    for (const auto& s : SourceIdStorage.GetInMemorySourceIds()) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Init complete for topic '" << TopicName() << "' Partition: " << Partition
                    << " SourceId: " << s.first << " SeqNo: " << s.second.SeqNo << " offset: " << s.second.Offset
                    << " MaxOffset: " << EndOffset
        );
    }
    ProcessHasDataRequests(ctx);

    InitUserInfoForImportantClients(ctx);

    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        Y_ABORT_UNLESS(userInfoPair.second.Offset >= 0);
        ReadTimestampForOffset(userInfoPair.first, userInfoPair.second, ctx);
    }
    if (PartitionCountersLabeled) {
        PartitionCountersLabeled->GetCounters()[METRIC_INIT_TIME] = InitDuration.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_LIFE_TIME] = CreationTime.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS] = 1;
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS_TOTAL] = Config.PartitionsSize();
        ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
    UpdateUserInfoEndOffset(ctx.Now());

    ScheduleUpdateAvailableSize(ctx);

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        CreateMirrorerActor();
    }

    ReportCounters(ctx, true);
}


void TPartition::UpdateUserInfoEndOffset(const TInstant& now) {
    for (auto& userInfo : UsersInfoStorage->GetAll()) {
        userInfo.second.EndOffset = (i64)EndOffset;
        userInfo.second.UpdateReadingTimeAndState(now);
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

void TPartition::Handle(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TPartResult result;
    result.SetPartition(Partition.InternalPartitionId);
    result.SetGeneration(TabletGeneration);
    result.SetCookie(++PQRBCookie);

    if (DiskIsFull || WaitingForSubDomainQuota(ctx)) {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_DISK_IS_FULL);
    } else if (EndOffset - StartOffset >= static_cast<ui64>(Config.GetPartitionConfig().GetMaxCountInPartition()) ||
               Size() >= static_cast<ui64>(Config.GetPartitionConfig().GetMaxSizeInPartition())) {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_PARTITION_IS_FULL);
    } else {
        result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_OK);
    }
    result.SetLastInitDurationSeconds(InitDuration.Seconds());
    result.SetCreationTimestamp(CreationTime.Seconds());
    ui64 headGapSize = DataKeysBody.empty() ? 0 : (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));
    ui32 gapsCount = GapOffsets.size() + (headGapSize ? 1 : 0);
    result.SetGapCount(gapsCount);
    result.SetGapSize(headGapSize + GapSize);

    Y_ABORT_UNLESS(AvgWriteBytes.size() == 4);
    result.SetAvgWriteSpeedPerSec(AvgWriteBytes[0].GetValue());
    result.SetAvgWriteSpeedPerMin(AvgWriteBytes[1].GetValue());
    result.SetAvgWriteSpeedPerHour(AvgWriteBytes[2].GetValue());
    result.SetAvgWriteSpeedPerDay(AvgWriteBytes[3].GetValue());

    Y_ABORT_UNLESS(AvgQuotaBytes.size() == 4);
    result.SetAvgQuotaSpeedPerSec(AvgQuotaBytes[0].GetValue());
    result.SetAvgQuotaSpeedPerMin(AvgQuotaBytes[1].GetValue());
    result.SetAvgQuotaSpeedPerHour(AvgQuotaBytes[2].GetValue());
    result.SetAvgQuotaSpeedPerDay(AvgQuotaBytes[3].GetValue());

    result.SetSourceIdCount(SourceIdStorage.GetInMemorySourceIds().size());
    result.SetSourceIdRetentionPeriodSec((ctx.Now() - SourceIdStorage.MinAvailableTimestamp(ctx.Now())).Seconds());

    result.SetWriteBytesQuota(TotalPartitionWriteSpeed);

    TVector<ui64> resSpeed;
    resSpeed.resize(4);
    ui64 maxQuota = 0;
    bool filterConsumers = !ev->Get()->Consumers.empty();
    TSet<TString> requiredConsumers(ev->Get()->Consumers.begin(), ev->Get()->Consumers.end());
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        auto& clientId = ev->Get()->ClientId;
        bool consumerShouldBeProcessed = filterConsumers
            ? requiredConsumers.contains(userInfo.User)
            : clientId.empty() || clientId == userInfo.User;
        if (consumerShouldBeProcessed) {
            Y_ABORT_UNLESS(userInfo.AvgReadBytes.size() == 4);
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
                requiredConsumers.extract(userInfo.User);
            }
            continue;
        }
        if (clientId == userInfo.User) { //fill lags
            NKikimrPQ::TClientInfo* clientInfo = result.MutableLagsInfo();
            clientInfo->SetClientId(userInfo.User);
            auto write = clientInfo->MutableWritePosition();
            write->SetOffset(userInfo.Offset);
            userInfo.EndOffset = EndOffset;
            write->SetWriteTimestamp((userInfo.GetWriteTimestamp() ? userInfo.GetWriteTimestamp() : GetWriteTimeEstimate(userInfo.Offset)).MilliSeconds());
            write->SetCreateTimestamp(userInfo.GetCreateTimestamp().MilliSeconds());
            auto read = clientInfo->MutableReadPosition();
            read->SetOffset(userInfo.GetReadOffset());
            read->SetWriteTimestamp((userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset())).MilliSeconds());
            read->SetCreateTimestamp(userInfo.GetReadCreateTimestamp().MilliSeconds());
            write->SetSize(GetSizeLag(userInfo.Offset));
            read->SetSize(GetSizeLag(userInfo.GetReadOffset()));

            clientInfo->SetReadLagMs(userInfo.GetReadOffset() < (i64)EndOffset
                                        ? (userInfo.GetReadTimestamp() - TInstant::MilliSeconds(read->GetWriteTimestamp())).MilliSeconds()
                                        : 0);
            clientInfo->SetLastReadTimestampMs(userInfo.GetReadTimestamp().MilliSeconds());
            clientInfo->SetWriteLagMs(userInfo.GetWriteLagMs());
            ui64 totalLag = clientInfo->GetReadLagMs() + userInfo.GetWriteLagMs() + (ctx.Now() - userInfo.GetReadTimestamp()).MilliSeconds();
            clientInfo->SetTotalLagMs(totalLag);
        }

        if (ev->Get()->GetStatForAllConsumers) { //fill lags
            auto* clientInfo = result.AddConsumerResult();
            clientInfo->SetConsumer(userInfo.User);
            auto readTimestamp = (userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset())).MilliSeconds();
            clientInfo->SetReadLagMs(userInfo.GetReadOffset() < (i64)EndOffset
                                        ? (userInfo.GetReadTimestamp() - TInstant::MilliSeconds(readTimestamp)).MilliSeconds()
                                        : 0);
            clientInfo->SetLastReadTimestampMs(userInfo.GetReadTimestamp().MilliSeconds());
            clientInfo->SetWriteLagMs(userInfo.GetWriteLagMs());

            clientInfo->SetAvgReadSpeedPerMin(userInfo.AvgReadBytes[1].GetValue());
            clientInfo->SetAvgReadSpeedPerHour(userInfo.AvgReadBytes[2].GetValue());
            clientInfo->SetAvgReadSpeedPerDay(userInfo.AvgReadBytes[3].GetValue());

            clientInfo->SetReadingFinished(LastOffsetHasBeenCommited(userInfo));
        }

    }

    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);

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

        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                    "Topic PartitionStatus PartitionSize: " << result.GetPartitionSize()
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
        for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
            auto& userInfo = userInfoPair.second;
            if (!userInfo.LabeledCounters)
                continue;
            if (userInfoPair.first != CLIENTID_WITHOUT_CONSUMER && !userInfo.HasReadRule && !userInfo.Important)
                continue;
            auto* cac = ac->AddConsumerAggregatedCounters();
            cac->SetConsumer(userInfo.User);
            for (ui32 i = 0; i < userInfo.LabeledCounters->GetCounters().Size(); ++i) {
                cac->AddValues(userInfo.LabeledCounters->GetCounters()[i].Get());
            }
        }
    }
    result.SetScaleStatus(SplitMergeEnabled(TabletConfig) ? ScaleStatus :NKikimrPQ::EScaleStatus::NORMAL);
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result, Partition));
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
    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);
    result.SetResponseTimestamp(ctx.Now().MilliSeconds());
    for (auto& pr : UsersInfoStorage->GetAll()) {
        TUserInfo& userInfo(pr.second);
        NKikimrPQ::TClientInfo& clientInfo = *result.AddClientInfo();
        clientInfo.SetClientId(pr.first);
        auto& write = *clientInfo.MutableWritePosition();
        write.SetOffset(userInfo.Offset);
        userInfo.EndOffset = EndOffset;
        write.SetWriteTimestamp((userInfo.GetWriteTimestamp() ? userInfo.GetWriteTimestamp() : GetWriteTimeEstimate(userInfo.Offset)).MilliSeconds());
        write.SetCreateTimestamp(userInfo.GetCreateTimestamp().MilliSeconds());
        auto& read = *clientInfo.MutableReadPosition();
        read.SetOffset(userInfo.GetReadOffset());
        read.SetWriteTimestamp((userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset())).MilliSeconds());
        read.SetCreateTimestamp(userInfo.GetReadCreateTimestamp().MilliSeconds());
        write.SetSize(GetSizeLag(userInfo.Offset));
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

//zero means no such record
TInstant TPartition::GetWriteTimeEstimate(ui64 offset) const {
    if (offset < StartOffset) offset = StartOffset;
    if (offset >= EndOffset)
        return TInstant::Zero();
    const std::deque<TDataKey>& container =
        (offset < Head.Offset || offset == Head.Offset && Head.PartNo > 0) ? DataKeysBody : HeadKeys;
    Y_ABORT_UNLESS(!container.empty());
    auto it = std::upper_bound(container.begin(), container.end(), offset,
                    [](const ui64 offset, const TDataKey& p) {
                        return offset < p.Key.GetOffset() ||
                                        offset == p.Key.GetOffset() && p.Key.GetPartNo() > 0;
                    });
    // Always greater
    Y_ABORT_UNLESS(it != container.begin(),
             "Tablet %lu StartOffset %lu, HeadOffset %lu, offset %lu, containter size %lu, first-elem: %s",
             TabletID, StartOffset, Head.Offset, offset, container.size(),
             container.front().Key.ToString().c_str());
    Y_ABORT_UNLESS(it == container.end() ||
             it->Key.GetOffset() > offset ||
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
    const NKikimrPQ::TEvProposeTransaction& event = ev->Get()->Record;
    Y_ABORT_UNLESS(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_ABORT_UNLESS(event.HasData());
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

void TPartition::Handle(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPQ::TEvProposePartitionConfig" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext&)
{
    PQ_LOG_D("HandleOnInit TEvPQ::TEvTxCommit" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext&)
{
    PQ_LOG_D("HandleOnInit TEvPQ::TEvProposePartitionConfig" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::Handle(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx)
{
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx)
{
    PQ_LOG_D("Handle TEvPQ::TEvTxCommit" <<
             " Step " << ev->Get()->Step <<
             ", TxId " << ev->Get()->TxId);

    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(*ev->Get()) < GetStepAndTxId(*PlanStep, *TxId)) {
            PQ_LOG_D("Send TEvTxCommitDone" <<
                     " Step " << ev->Get()->Step <<
                     ", TxId " << ev->Get()->TxId);
            ctx.Send(Tablet, MakeCommitDone(ev->Get()->Step, ev->Get()->TxId).Release());
            return;
        }
    }

    auto txIter = TransactionsInflight.begin();
    if (ChangeConfig) {
        Y_ABORT_UNLESS(TransactionsInflight.size() == 1,
                       "PQ: %" PRIu64 ", Partition: %" PRIu32 ", Step: %" PRIu64 ", TxId: %" PRIu64,
                       TabletID, Partition.OriginalPartitionId,
                       ev->Get()->Step, ev->Get()->TxId);
    } else {
        Y_ABORT_UNLESS(!TransactionsInflight.empty(),
                       "PQ: %" PRIu64 ", Partition: %" PRIu32 ", Step: %" PRIu64 ", TxId: %" PRIu64,
                       TabletID, Partition.OriginalPartitionId,
                       ev->Get()->Step, ev->Get()->TxId);
        txIter = TransactionsInflight.find(ev->Get()->TxId);
        Y_ABORT_UNLESS(!txIter.IsEnd(),
                       "PQ: %" PRIu64 ", Partition: %" PRIu32 ", Step: %" PRIu64 ", TxId: %" PRIu64,
                       TabletID, Partition.OriginalPartitionId,
                       ev->Get()->Step, ev->Get()->TxId);
    }
    Y_ABORT_UNLESS(txIter->second->State == ECommitState::Pending);

    txIter->second->State = ECommitState::Committed;
    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx)
{
    auto* event = ev->Get();
    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(*event) < GetStepAndTxId(*PlanStep, *TxId)) {
            PQ_LOG_D("Rollback for" <<
                     " Step " << ev->Get()->Step <<
                     ", TxId " << ev->Get()->TxId);
            return;
        }
    }

    auto txIter = TransactionsInflight.begin();
    if (ChangeConfig) {
        Y_ABORT_UNLESS(TransactionsInflight.size() == 1,
                       "PQ: %" PRIu64 ", Partition: %" PRIu32,
                       TabletID, Partition.OriginalPartitionId);
    } else {
        Y_ABORT_UNLESS(!TransactionsInflight.empty(),
                       "PQ: %" PRIu64 ", Partition: %" PRIu32,
                       TabletID, Partition.OriginalPartitionId);
        txIter = TransactionsInflight.find(ev->Get()->TxId);
        Y_ABORT_UNLESS(!txIter.IsEnd(),
                       "PQ: %" PRIu64 ", Partition: %" PRIu32,
                       TabletID, Partition.OriginalPartitionId);
    }
    Y_ABORT_UNLESS(txIter->second->State == ECommitState::Pending);

    txIter->second->State = ECommitState::Aborted;
    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvGetWriteInfoRequest::TPtr& ev, const TActorContext& ctx) {
    if (ClosedInternalPartition || WaitingForPreviousBlobQuota() || (CurrentStateFunc() != &TThis::StateIdle)) {
        auto* response = new TEvPQ::TEvGetWriteInfoError(Partition.InternalPartitionId,
                                                         "Write info requested while writes are not complete");
        ctx.Send(ev->Sender, response);
        ClosedInternalPartition = true;
        return;
    }
    ClosedInternalPartition = true;
    auto response = new TEvPQ::TEvGetWriteInfoResponse();
    response->Cookie = Partition.InternalPartitionId;
    response->BodyKeys = std::move(DataKeysBody);
    response->SrcIdInfo = std::move(SourceIdStorage.ExtractInMemorySourceIds());
    ui32 rcount = 0, rsize = 0;
    ui64 insideHeadOffset = 0;

    response->BlobsFromHead = std::move(GetReadRequestFromHead(0, 0, std::numeric_limits<ui32>::max(),
                                                               std::numeric_limits<ui32>::max(), 0, &rcount, &rsize,
                                                               &insideHeadOffset, 0));
    response->BytesWrittenGrpc = BytesWrittenGrpc.Value();
    response->BytesWrittenUncompressed = BytesWrittenUncompressed.Value();
    response->BytesWrittenTotal = BytesWrittenTotal.Value();
    response->MessagesWrittenTotal = MsgsWrittenTotal.Value();
    response->MessagesWrittenGrpc = MsgsWrittenGrpc.Value();
    response->MessagesSizes = std::move(MessageSize.GetValues());
    response->InputLags = std::move(SupportivePartitionTimeLag);

    ctx.Send(ev->Sender, response);
}

void TPartition::WriteInfoResponseHandler(
        const TActorId& sender,
        TGetWriteInfoResp&& ev,
        const TActorContext& ctx
) {
    auto txIter = WriteInfosToTx.find(sender);
    Y_ABORT_UNLESS(!txIter.IsEnd());

    auto& tx = (*txIter->second);

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

TPartition::EProcessResult TPartition::ApplyWriteInfoResponse(TTransaction& tx) {
    bool isImmediate = (tx.ProposeTransaction != nullptr);
    Y_ABORT_UNLESS(tx.WriteInfo);
    Y_ABORT_UNLESS(!tx.WriteInfoApplied);
    if (!tx.Predicate.GetOrElse(true)) {
        return EProcessResult::Continue;
    }
    auto& srcIdInfo = tx.WriteInfo->SrcIdInfo;

    EProcessResult ret = EProcessResult::Continue;
    const auto& knownSourceIds = SourceIdStorage.GetInMemorySourceIds();
    THashSet<TString> txSourceIds;
    for (auto& s : srcIdInfo) {
        if (TxAffectedSourcesIds.contains(s.first)) {
            ret = EProcessResult::Blocked;
            break;
        }
        if (isImmediate) {
            WriteAffectedSourcesIds.insert(s.first);
        } else {
            if (WriteAffectedSourcesIds.contains(s.first)) {
                ret = EProcessResult::Blocked;
                break;
            }
            txSourceIds.insert(s.first);
        }

        auto existing = knownSourceIds.find(s.first);
        if (existing.IsEnd())
            continue;
        if (s.second.MinSeqNo <= existing->second.SeqNo) {
            tx.Predicate = false;
            tx.Message = TStringBuilder() << "MinSeqNo violation failure on " << s.first;
            tx.WriteInfoApplied = true;
            break;
        }
    }
    if (ret == EProcessResult::Continue && tx.Predicate.GetOrElse(true)) {
        TxAffectedSourcesIds.insert(txSourceIds.begin(), txSourceIds.end());
        tx.WriteInfoApplied = true;
        WriteKeysSizeEstimate += tx.WriteInfo->BodyKeys.size();
        WriteKeysSizeEstimate += tx.WriteInfo->SrcIdInfo.size();
        WriteKeysSizeEstimate += tx.WriteInfo->BlobsFromHead.size();
        for (const auto& blob : tx.WriteInfo->BlobsFromHead) {
            WriteCycleSizeEstimate += blob.GetBlobSize();
        }
    }

    return ret;
}

void TPartition::Handle(TEvPQ::TEvGetWriteInfoResponse::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_D("Handle TEvPQ::TEvGetWriteInfoResponse");
    WriteInfoResponseHandler(ev->Sender, ev->Release(), ctx);
}


void TPartition::Handle(TEvPQ::TEvGetWriteInfoError::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_D("Handle TEvPQ::TEvGetWriteInfoError");
    WriteInfoResponseHandler(ev->Sender, ev->Release(), ctx);
}

void TPartition::ReplyToProposeOrPredicate(TSimpleSharedPtr<TTransaction>& tx, bool isPredicate) {

    if (isPredicate) {
        auto insRes = TransactionsInflight.emplace(tx->Tx->TxId, tx);
        Y_ABORT_UNLESS(insRes.second);
        Send(Tablet,
             MakeHolder<TEvPQ::TEvTxCalcPredicateResult>(tx->Tx->Step,
                                                         tx->Tx->TxId,
                                                         Partition,
                                                         *tx->Predicate).Release());
    } else {
        auto insRes = TransactionsInflight.emplace(tx->ProposeConfig->TxId, tx);
        Y_ABORT_UNLESS(insRes.second);
        Send(Tablet,
             MakeHolder<TEvPQ::TEvProposePartitionConfigResult>(tx->ProposeConfig->Step,
                                                                tx->ProposeConfig->TxId,
                                                                Partition).Release());
    }
}

void TPartition::Handle(TEvPQ::TEvGetMaxSeqNoRequest::TPtr& ev, const TActorContext& ctx) {
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(ev->Get()->Cookie);
    NKikimrClient::TResponse& resp = *response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto& result = *resp.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult();
    for (const auto& sourceId : ev->Get()->SourceIds) {
        auto& protoInfo = *result.AddSourceIdInfo();
        protoInfo.SetSourceId(sourceId);

        auto info = SourceManager.Get(sourceId);
        if (info.State == TSourceIdInfo::EState::Unknown) {
            continue;
        }

        Y_ABORT_UNLESS(info.Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, info.Offset);
        Y_ABORT_UNLESS(info.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, info.SeqNo);

        protoInfo.SetSeqNo(info.SeqNo);
        protoInfo.SetOffset(info.Offset);
        protoInfo.SetWriteTimestampMS(info.WriteTimestamp.MilliSeconds());
        protoInfo.SetExplicit(info.Explicit);
        protoInfo.SetState(TSourceIdInfo::ConvertState(info.State));
    }

    ctx.Send(Tablet, response.Release());
}

void TPartition::Handle(TEvPQ::TEvBlobResponse::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Get()->GetCookie();
    Y_ABORT_UNLESS(ReadInfo.contains(cookie));

    auto it = ReadInfo.find(cookie);
    Y_ABORT_UNLESS(it != ReadInfo.end());

    TReadInfo info = std::move(it->second);
    ReadInfo.erase(it);

    //make readinfo class
    auto& userInfo = UsersInfoStorage->GetOrCreate(info.User, ctx);
    TReadAnswer answer(info.FormAnswer(
        ctx, *ev->Get(), EndOffset, Partition, &userInfo,
        info.Destination, GetSizeLag(info.Offset), Tablet, Config.GetMeteringMode()
    ));
    const auto& resp = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get())->Response;

    if (HasError(*ev->Get())) {
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_ERROR].Increment(1);
        }
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
    } else {
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_OK].Increment(1);
        }
        TabletCounters.Cumulative()[COUNTER_PQ_READ_OK].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_OK].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        TabletCounters.Cumulative()[COUNTER_PQ_READ_BYTES].Increment(resp->ByteSize());
    }
    ctx.Send(info.Destination != 0 ? Tablet : ctx.SelfID, answer.Event.Release());
    OnReadRequestFinished(info.Destination, answer.Size, info.User, ctx);
}

void TPartition::Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx) {
    ReadingTimestamp = false;
    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);
    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        ProcessTimestampRead(ctx);
        return;
    }
    Y_ABORT_UNLESS(userInfo->ReadScheduled);
    Y_ABORT_UNLESS(ReadingForUser != "");

    LOG_ERROR_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicName() << "' partition " << Partition
                << " user " << ReadingForUser << " readTimeStamp error: " << ev->Get()->Error
    );

    UpdateUserInfoTimestamp.push_back(std::make_pair(ReadingForUser, ReadingForUserReadRuleGeneration));

    ProcessTimestampRead(ctx);
}

void TPartition::CheckHeadConsistency() const {
    ui32 p = 0;
    for (ui32 j = 0; j < DataKeysHead.size(); ++j) {
        ui32 s = 0;
        for (ui32 k = 0; k < DataKeysHead[j].KeysCount(); ++k) {
            Y_ABORT_UNLESS(p < HeadKeys.size());
            Y_ABORT_UNLESS(DataKeysHead[j].GetKey(k) == HeadKeys[p].Key);
            Y_ABORT_UNLESS(DataKeysHead[j].GetSize(k) == HeadKeys[p].Size);
            s += DataKeysHead[j].GetSize(k);
            Y_ABORT_UNLESS(j + 1 == TotalLevels || DataKeysHead[j].GetSize(k) >= CompactLevelBorder[j + 1]);
            ++p;
        }
        Y_ABORT_UNLESS(s < DataKeysHead[j].Border());
    }
    Y_ABORT_UNLESS(DataKeysBody.empty() ||
             Head.Offset >= DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
    Y_ABORT_UNLESS(p == HeadKeys.size());
    if (!HeadKeys.empty()) {
        Y_ABORT_UNLESS(HeadKeys.size() <= TotalMaxCount);
        Y_ABORT_UNLESS(HeadKeys.front().Key.GetOffset() == Head.Offset);
        Y_ABORT_UNLESS(HeadKeys.front().Key.GetPartNo() == Head.PartNo);
        for (p = 1; p < HeadKeys.size(); ++p) {
            Y_ABORT_UNLESS(HeadKeys[p].Key.GetOffset() == HeadKeys[p-1].Key.GetOffset() + HeadKeys[p-1].Key.GetCount());
            Y_ABORT_UNLESS(HeadKeys[p].Key.ToString() > HeadKeys[p-1].Key.ToString());
        }
    }
}

ui64 TPartition::GetSizeLag(i64 offset) {
    ui64 sizeLag = 0;
    if (!DataKeysBody.empty() && (offset < (i64)Head.Offset || offset == (i64)Head.Offset && Head.PartNo > 0)) { //there will be something in body
        auto it = std::upper_bound(DataKeysBody.begin(), DataKeysBody.end(), std::make_pair(offset, 0),
                [](const std::pair<ui64, ui16>& offsetAndPartNo, const TDataKey& p) { return offsetAndPartNo.first < p.Key.GetOffset() || offsetAndPartNo.first == p.Key.GetOffset() && offsetAndPartNo.second < p.Key.GetPartNo();});
        if (it != DataKeysBody.begin())
            --it; //point to blob with this offset
        Y_ABORT_UNLESS(it != DataKeysBody.end());
        sizeLag = it->Size + DataKeysBody.back().CumulativeSize - it->CumulativeSize;
        Y_ABORT_UNLESS(BodySize == DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size - DataKeysBody.front().CumulativeSize);
    }
    for (auto& b : HeadKeys) {
        if ((i64)b.Key.GetOffset() >= offset)
            sizeLag += b.Size;
    }
    return sizeLag;
}


bool TPartition::UpdateCounters(const TActorContext& ctx, bool force) {
    if (!PartitionCountersLabeled) {
        return false;
    }

    const auto now = ctx.Now();
    if ((now - LastCountersUpdate < MIN_UPDATE_COUNTERS_DELAY) && !force)
        return false;

    LastCountersUpdate = now;

    // per client counters
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        if (!userInfo.LabeledCounters)
            continue;
        if (userInfoPair.first != CLIENTID_WITHOUT_CONSUMER && !userInfo.HasReadRule && !userInfo.Important)
            continue;
        bool haveChanges = false;
        userInfo.EndOffset = EndOffset;
        userInfo.UpdateReadingTimeAndState(now);
        ui64 ts = userInfo.GetWriteTimestamp().MilliSeconds();
        if (ts < MIN_TIMESTAMP_MS) ts = Max<i64>();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_WRITE_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_WRITE_TIME].Set(ts);
        }
        ts = userInfo.GetCreateTimestamp().MilliSeconds();
        if (ts < MIN_TIMESTAMP_MS) ts = Max<i64>();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_CREATE_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_CREATE_TIME].Set(ts);
        }
        ts = userInfo.GetReadWriteTimestamp().MilliSeconds();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_WRITE_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_WRITE_TIME].Set(ts);
        }

        i64 off = userInfo.GetReadOffset(); //we want to track first not-readed offset
        TInstant wts = userInfo.GetReadWriteTimestamp() ? userInfo.GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo.GetReadOffset());
        TInstant readTimestamp = userInfo.GetReadTimestamp();
        ui64 readTimeLag = off >= (i64)EndOffset ? 0 : (readTimestamp - wts).MilliSeconds();
        ui64 totalLag = userInfo.GetWriteLagMs() + readTimeLag + (now - readTimestamp).MilliSeconds();

        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_TIME].Get() != totalLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_TIME].Set(totalLag);
        }

        ts = readTimestamp.MilliSeconds();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_LAST_READ_TIME].Get() != ts) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_LAST_READ_TIME].Set(ts);
        }

        ui64 timeLag = userInfo.GetWriteLagMs();
        if (userInfo.LabeledCounters->GetCounters()[METRIC_WRITE_TIME_LAG].Get() != timeLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_WRITE_TIME_LAG].Set(timeLag);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_TIME_LAG].Get() != readTimeLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TIME_LAG].Set(readTimeLag);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_MESSAGE_LAG].Get() != EndOffset - userInfo.Offset) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_MESSAGE_LAG].Set(EndOffset - userInfo.Offset);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_MESSAGE_LAG].Get() != EndOffset - off) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_MESSAGE_LAG].Set(EndOffset - off);
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_MESSAGE_LAG].Set(EndOffset - off);
        }

        ui64 sizeLag = GetSizeLag(userInfo.Offset);
        if (userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_SIZE_LAG].Get() != sizeLag) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_COMMIT_SIZE_LAG].Set(sizeLag);
        }

        ui64 sizeLagRead = GetSizeLag(userInfo.ReadOffset);
        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_SIZE_LAG].Get() != sizeLagRead) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_SIZE_LAG].Set(sizeLagRead);
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_TOTAL_SIZE_LAG].Set(sizeLag);
        }

        if (userInfo.LabeledCounters->GetCounters()[METRIC_USER_PARTITIONS].Get() == 0) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_USER_PARTITIONS].Set(1);
        }

        ui64 readOffsetRewindSum = userInfo.ReadOffsetRewindSum;
        if (readOffsetRewindSum != userInfo.LabeledCounters->GetCounters()[METRIC_READ_OFFSET_REWIND_SUM].Get()) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_OFFSET_REWIND_SUM].Set(readOffsetRewindSum);
        }

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
        Y_ABORT_UNLESS(id == METRIC_MAX_READ_SPEED_4 + 1);
        if (userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get()) {
            ui64 quotaUsage = ui64(userInfo.AvgReadBytes[1].GetValue()) * 1000000 / userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get() / 60;
            if (quotaUsage != userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_USAGE].Get()) {
                haveChanges = true;
                userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_USAGE].Set(quotaUsage);
            }
        }

        if (userInfoPair.first == CLIENTID_WITHOUT_CONSUMER ) {
            PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_NO_CONSUMER_BYTES].Set(userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Get());
            PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_NO_CONSUMER_USAGE].Set(userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_USAGE].Get());
        }

        if (haveChanges) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *userInfo.LabeledCounters));
        }
    }

    bool haveChanges = false;
    if (SourceIdStorage.GetInMemorySourceIds().size() != PartitionCountersLabeled->GetCounters()[METRIC_MAX_NUM_SIDS].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_NUM_SIDS].Set(SourceIdStorage.GetInMemorySourceIds().size());
        PartitionCountersLabeled->GetCounters()[METRIC_NUM_SIDS].Set(SourceIdStorage.GetInMemorySourceIds().size());
    }

    TDuration lifetimeNow = ctx.Now() - SourceIdStorage.MinAvailableTimestamp(ctx.Now());
    if (lifetimeNow.MilliSeconds() != PartitionCountersLabeled->GetCounters()[METRIC_MIN_SID_LIFETIME].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MIN_SID_LIFETIME].Set(lifetimeNow.MilliSeconds());
    }

    const ui64 headGapSize = DataKeysBody.empty() ? 0 : (Head.Offset - (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount()));
    const ui64 gapSize = GapSize + headGapSize;
    if (gapSize != PartitionCountersLabeled->GetCounters()[METRIC_GAPS_SIZE].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_GAPS_SIZE].Set(gapSize);
        PartitionCountersLabeled->GetCounters()[METRIC_GAPS_SIZE].Set(gapSize);
    }

    const ui32 gapsCount = GapOffsets.size() + (headGapSize ? 1 : 0);
    if (gapsCount != PartitionCountersLabeled->GetCounters()[METRIC_GAPS_COUNT].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_GAPS_COUNT].Set(gapsCount);
        PartitionCountersLabeled->GetCounters()[METRIC_GAPS_COUNT].Set(gapsCount);
    }

    if (TotalPartitionWriteSpeed != PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_BYTES].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_BYTES].Set(TotalPartitionWriteSpeed);
    }

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
    Y_ABORT_UNLESS(id == METRIC_MAX_WRITE_SPEED_4 + 1);


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
    Y_ABORT_UNLESS(id == METRIC_MAX_QUOTA_SPEED_4 + 1);

    if (TotalPartitionWriteSpeed) {
        ui64 quotaUsage = ui64(AvgQuotaBytes[1].GetValue()) * 1000000 / TotalPartitionWriteSpeed / 60;
        if (quotaUsage != PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_USAGE].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_USAGE].Set(quotaUsage);
        }
    }

    ui64 storageSize = StorageSize(ctx);
    if (storageSize != PartitionCountersLabeled->GetCounters()[METRIC_TOTAL_PART_SIZE].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_PART_SIZE].Set(storageSize);
        PartitionCountersLabeled->GetCounters()[METRIC_TOTAL_PART_SIZE].Set(storageSize);
    }

    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY == Config.GetMeteringMode()) {
        ui64 reserveSize = ReserveSize();
        if (reserveSize != PartitionCountersLabeled->GetCounters()[METRIC_RESERVE_LIMIT_BYTES].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_RESERVE_LIMIT_BYTES].Set(reserveSize);
        }

        ui64 reserveUsed = UsedReserveSize(ctx);
        if (reserveUsed != PartitionCountersLabeled->GetCounters()[METRIC_RESERVE_USED_BYTES].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_RESERVE_USED_BYTES].Set(reserveUsed);
        }
    }

    ui64 ts = (WriteTimestamp.MilliSeconds() < MIN_TIMESTAMP_MS) ? Max<i64>() : WriteTimestamp.MilliSeconds();
    if (PartitionCountersLabeled->GetCounters()[METRIC_LAST_WRITE_TIME].Get() != ts) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_LAST_WRITE_TIME].Set(ts);
    }

    ui64 timeLag = WriteLagMs.GetValue();
    if (PartitionCountersLabeled->GetCounters()[METRIC_WRITE_TIME_LAG_MS].Get() != timeLag) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_WRITE_TIME_LAG_MS].Set(timeLag);
    }

    if (PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Get()) {
        ui64 quotaUsage = ui64(AvgReadBytes.GetValue()) * 1000000 / PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Get() / 60;
        if (quotaUsage != PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_USAGE].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_USAGE].Set(quotaUsage);
        }
    }

    if (PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_NO_CONSUMER_BYTES].Get()) {
        ui64 quotaUsage = ui64(AvgReadBytes.GetValue()) * 1000000 / PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Get() / 60;
        if (quotaUsage != PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_USAGE].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_USAGE].Set(quotaUsage);
        }
    }
    return haveChanges;
}

void TPartition::ReportCounters(const TActorContext& ctx, bool force) {
    if (UpdateCounters(ctx, force)) {
        ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
}

void TPartition::Handle(NReadQuoterEvents::TEvQuotaUpdated::TPtr& ev, const TActorContext&) {
    for (auto& [consumerStr, quota] : ev->Get()->UpdatedConsumerQuotas) {
        const TUserInfo* userInfo = UsersInfoStorage->GetIfExists(consumerStr);
        if (userInfo) {
            userInfo->LabeledCounters->GetCounters()[METRIC_READ_QUOTA_PER_CONSUMER_BYTES].Set(quota);
        }
    }
    if (PartitionCountersLabeled)
        PartitionCountersLabeled->GetCounters()[METRIC_READ_QUOTA_PARTITION_TOTAL_BYTES].Set(ev->Get()->UpdatedTotalPartitionReadQuota);
}

void TPartition::Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    auto& response = ev->Get()->Record;

    //check correctness of response
    if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        LOG_ERROR_S(
                ctx, NKikimrServices::PERSQUEUE,
                "OnWrite topic '" << TopicName() << "' partition " << Partition
                    << " commands are not processed at all, reason: " << response.DebugString()
        );
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        //TODO: if status is DISK IS FULL, is global status MSTATUS_OK? it will be good if it is true
        return;
    }
    if (response.DeleteRangeResultSize()) {
        for (ui32 i = 0; i < response.DeleteRangeResultSize(); ++i) {
            if (response.GetDeleteRangeResult(i).GetStatus() != NKikimrProto::OK) {
                LOG_ERROR_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "OnWrite topic '" << TopicName() << "' partition " << Partition
                            << " delete range error"
                );
                //TODO: if disk is full, could this be ok? delete must be ok, of course
                ctx.Send(Tablet, new TEvents::TEvPoisonPill());
                return;
            }
        }
    }

    if (response.WriteResultSize()) {
        bool diskIsOk = true;
        for (ui32 i = 0; i < response.WriteResultSize(); ++i) {
            if (response.GetWriteResult(i).GetStatus() != NKikimrProto::OK) {
                LOG_ERROR_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "OnWrite  topic '" << TopicName() << "' partition " << Partition
                            << " write error"
                );
                ctx.Send(Tablet, new TEvents::TEvPoisonPill());
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
            LOG_ERROR_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "OnWrite  topic '" << TopicName() << "' partition " << Partition
                        << " are not processed at all, got KV error in CmdGetStatus " << res.GetStatus()
            );
            ctx.Send(Tablet, new TEvents::TEvPoisonPill());
            return;
        }
        diskIsOk = diskIsOk && CheckDiskStatus(res.GetStatusFlags());
    }
    if (response.GetStatusResultSize()) {
        DiskIsFull = !diskIsOk;
    }
    const auto writeDuration = ctx.Now() - WriteStartTime;
    const auto minWriteLatency = TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetMinWriteLatencyMs());

    if (writeDuration > minWriteLatency) {
        OnHandleWriteResponse(ctx);
    } else {
        ctx.Schedule(minWriteLatency - writeDuration, new TEvPQ::TEvHandleWriteResponse(response.GetCookie()));
    }
}


void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> event)
{
    UserActionAndTransactionEvents.emplace_back(MakeSimpleShared<TTransaction>(std::move(event)));
    RequestWriteInfoIfRequired();
}

void TPartition::RequestWriteInfoIfRequired()
{
    auto tx = std::get<1>(UserActionAndTransactionEvents.back().Event);
    auto supportId = tx->SupportivePartitionActor;
    if (supportId) {
        Send(supportId, new TEvPQ::TEvGetWriteInfoRequest());
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
    Y_ABORT_UNLESS(p != UserActCount.end());

    Y_ABORT_UNLESS(p->second > 0);
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

void TPartition::ProcessTxsAndUserActs(const TActorContext& ctx)
{
    if (KVWriteInProgress) {
        return;
    }
    if (DeletePartitionState == DELETION_INITED) {
        if (!PersistRequest) {
            PersistRequest = MakeHolder<TEvKeyValue::TEvRequest>();
        }
        ScheduleNegativeReplies();
        ScheduleDeletePartitionDone();

        AddCmdDeleteRangeForAllKeys(*PersistRequest);

        ctx.Send(Tablet, PersistRequest.Release());
        PersistRequest = nullptr;
        DeletePartitionState = DELETION_IN_PROCESS;
        KVWriteInProgress = true;

        return;
    }
    while (true) {
        if (BatchingState == ETxBatchingState::PreProcessing) {
            ContinueProcessTxsAndUserActs(ctx);
        }
        if (BatchingState == ETxBatchingState::PreProcessing) {
            return; // Still preprocessing - waiting for something;
        }

        // Preprocessing complete;
        if (CurrentBatchSize > 0) {
            Send(SelfId(), new TEvPQ::TEvTxBatchComplete(CurrentBatchSize));
        }
        CurrentBatchSize = 0;

        if (UserActionAndTxPendingCommit.empty()) {
            // Processing stopped and nothing to commit - finalize
            BatchingState = ETxBatchingState::Finishing;
        } else {
            // Process commit queue
            ProcessCommitQueue();
        }
        if (!UserActionAndTxPendingCommit.empty()) {
            // Still pending for come commits
            return;
        }
        // Commit queue processing complete. Now can either swith to persist or continue preprocessing;
        if (BatchingState == ETxBatchingState::Finishing) { // Persist required;
            RunPersist();
            return;
        }
        BatchingState = ETxBatchingState::PreProcessing;
    }
}

void TPartition::ContinueProcessTxsAndUserActs(const TActorContext&)
{
    Y_ABORT_UNLESS(!KVWriteInProgress);

    if (WriteCycleSizeEstimate >= MAX_WRITE_CYCLE_SIZE || WriteKeysSizeEstimate >= MAX_KEYS) {
        BatchingState = ETxBatchingState::Finishing;
        return;
    }
    auto visitor = [this](auto& event) {
        return this->PreProcessUserActionOrTransaction(event);
    };
    while (BatchingState == ETxBatchingState::PreProcessing && !UserActionAndTransactionEvents.empty()) {
        if (ChangingConfig) {
            BatchingState = ETxBatchingState::Finishing;
            break;
        }
        auto& front = UserActionAndTransactionEvents.front();
        switch (std::visit(visitor, front.Event)) {
        case EProcessResult::Continue:
            MoveUserActOrTxToCommitState();
            FirstEvent = false;
            break;
        case EProcessResult::ContinueDrop:
            UserActionAndTransactionEvents.pop_front();
            break;
        case EProcessResult::Break:
            MoveUserActOrTxToCommitState();
            BatchingState = ETxBatchingState::Finishing;
            FirstEvent = false;
            break;
        case EProcessResult::Blocked:
            BatchingState = ETxBatchingState::Executing;
            return;
        case EProcessResult::NotReady:
            return;
        }
        CurrentBatchSize += 1;
    }
    if (UserActionAndTransactionEvents.empty()) {
        BatchingState = ETxBatchingState::Executing;
        return;
    }

}

void TPartition::MoveUserActOrTxToCommitState() {
    auto& front = UserActionAndTransactionEvents.front();
    UserActionAndTxPendingCommit.push_back(std::move(front));
    UserActionAndTransactionEvents.pop_front();
}

void TPartition::ProcessCommitQueue() {
    CurrentBatchSize = 0;

    Y_ABORT_UNLESS(!KVWriteInProgress);
    if (!PersistRequest) {
        PersistRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    }
    auto visitor = [this, request=PersistRequest.Get()](auto& event) {
        return this->ExecUserActionOrTransaction(event, request);
    };
    while (!UserActionAndTxPendingCommit.empty()) {
        // UserActionAndTxPendingCommit.pop_front();
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
        auto event = std::move(front.Event);
        UserActionAndTxPendingCommit.pop_front();
        std::visit(visitor, event);
    }
    if (UserActionAndTxPendingCommit.empty()) {
        TxAffectedConsumers.clear();
        TxAffectedSourcesIds.clear();
        Y_ABORT_UNLESS(UserActionAndTxPendingCommit.empty());
        TransactionsInflight.clear();
    }
}

void TPartition::RunPersist() {
    TransactionsInflight.clear();

    Y_ABORT_UNLESS(UserActionAndTxPendingCommit.empty());
    const auto& ctx = ActorContext();
    const auto now = ctx.Now();
    if (!PersistRequest) {
        PersistRequest = MakeHolder<TEvKeyValue::TEvRequest>();
    }

    if (ManageWriteTimestampEstimate) {
        WriteTimestampEstimate = now;
    }

    bool haveChanges = CleanUp(PersistRequest.Get(), ctx);
    if (DiskIsFull) {
        AddCheckDiskRequest(PersistRequest.Get(), NumChannels);
        haveChanges = true;
    }

    ProcessReserveRequests(ctx);

    WriteStartTime = TActivationContext::Now();

    if (HaveWriteMsg) {
        if (!DiskIsFull) {
            EndAppendHeadWithNewWrites(PersistRequest.Get(), ctx);
            EndProcessWrites(PersistRequest.Get(), ctx);
        }
        EndHandleRequests(PersistRequest.Get(), ctx);
        //haveChanges = true;
    }
    if (haveChanges || TxIdHasChanged || !AffectedUsers.empty() || ChangeConfig) {
        WriteCycleStartTime = now;
        WriteStartTime = now;
        TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
        PartitionQuotaWaitTimeForCurrentBlob = TDuration::Zero();
        WritesTotal.Inc();
        AddMetaKey(PersistRequest.Get());
        HaveWriteMsg = true;

        AddCmdWriteTxMeta(PersistRequest->Record);
        AddCmdWriteUserInfos(PersistRequest->Record);
        AddCmdWriteConfig(PersistRequest->Record);
    }
    if (PersistRequest->Record.CmdDeleteRangeSize() || PersistRequest->Record.CmdWriteSize() || PersistRequest->Record.CmdRenameSize()) {
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
            BytesWrittenTotal.Inc(writeInfo->BytesWrittenTotal);
            BytesWrittenGrpc.Inc(writeInfo->BytesWrittenGrpc);
            BytesWrittenUncompressed.Inc(writeInfo->BytesWrittenUncompressed);
            // Messages written
            MsgsWrittenTotal.Inc(writeInfo->MessagesWrittenTotal);
            MsgsWrittenGrpc.Inc(writeInfo->MessagesWrittenTotal);
        }
        WriteInfosApplied.clear();
        //Done with counters.

        ctx.Send(HaveWriteMsg ? BlobCache : Tablet, PersistRequest.Release());
        KVWriteInProgress = true;
    } else {
        OnProcessTxsAndUserActsWriteComplete(ActorContext());
        AnswerCurrentWrites(ctx);
        AnswerCurrentReplies(ctx);
        HaveWriteMsg = false;
    }
    PersistRequest = nullptr;
}


void TPartition::AnswerCurrentReplies(const TActorContext& ctx)
{
    for (auto& [actor, reply] : Replies) {
        ctx.Send(actor, reply.release());
    }
    Replies.clear();
}

TPartition::EProcessResult TPartition::PreProcessUserActionOrTransaction(TSimpleSharedPtr<TTransaction>& t)
{
    auto result = EProcessResult::Continue;
    if (t->SupportivePartitionActor && !t->WriteInfo) { // Pending for write info
        return EProcessResult::NotReady;
    }
    if (t->WriteInfo && !t->WriteInfoApplied) { //Recieved write info but not applied
        result = ApplyWriteInfoResponse(*t);
        if (!t->WriteInfoApplied) { // Tried to apply write info but couldn't - TX must be blocked.
            Y_ABORT_UNLESS(result != EProcessResult::Continue);
            return result;
        }
    }
    if (t->ProposeTransaction) { // Immediate TX
        if (!t->Predicate.GetOrElse(true)) {
            t->State = ECommitState::Aborted;
            return EProcessResult::Continue;
        }
        t->Predicate.ConstructInPlace(true);
        return PreProcessImmediateTx(t->ProposeTransaction->Record);

    } else if (t->Tx) { // Distributed TX
        if (t->Predicate.Defined()) { // Predicate defined - either failed previously or Tx created with predicate defined.
            ReplyToProposeOrPredicate(t, true);
            return EProcessResult::Continue;
        }
        result = BeginTransaction(*t->Tx, t->Predicate);
        if (t->Predicate.Defined()) {
            ReplyToProposeOrPredicate(t, true);
        }
        return result;
    } else if (t->ProposeConfig) {
        if (!FirstEvent) {
            return EProcessResult::Blocked;
        }
        t->Predicate = BeginTransaction(*t->ProposeConfig);
        ChangingConfig = true;
        PendingPartitionConfig = GetPartitionConfig(t->ProposeConfig->Config);
        //Y_VERIFY_DEBUG_S(PendingPartitionConfig, "Partition " << Partition << " config not found");
        ReplyToProposeOrPredicate(t, false);
        return EProcessResult::Break;
    } else {
        Y_ABORT_UNLESS(t->ChangeConfig);

        Y_ABORT_UNLESS(!ChangeConfig && !ChangingConfig);
        if (!FirstEvent)
            return EProcessResult::Blocked;
        ChangingConfig = true;
        // Should remove this and add some id to TEvChangeConfig if we want to batch change of configs
        t->State = ECommitState::Committed;
        return EProcessResult::Break;
    }
    Y_ABORT();
    return result;
}

bool TPartition::ExecUserActionOrTransaction(TSimpleSharedPtr<TTransaction>& t, TEvKeyValue::TEvRequest*)
{
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
    const auto& ctx = ActorContext();
    if (t->ChangeConfig) {
        Y_ABORT_UNLESS(!ChangeConfig);
        Y_ABORT_UNLESS(ChangingConfig);
        ChangeConfig = t->ChangeConfig;
        SendChangeConfigReply = t->SendReply;
        BeginChangePartitionConfig(ChangeConfig->Config, ctx);
    } else if (t->ProposeConfig) {
        Y_ABORT_UNLESS(ChangingConfig);
        ChangeConfig = MakeSimpleShared<TEvPQ::TEvChangePartitionConfig>(TopicConverter,
                                                                         t->ProposeConfig->Config);
        PendingPartitionConfig = GetPartitionConfig(ChangeConfig->Config);
        SendChangeConfigReply = false;
    }
    CommitTransaction(t);
    return true;
}

TPartition::EProcessResult TPartition::BeginTransaction(const TEvPQ::TEvTxCalcPredicate& tx, TMaybe<bool>& predicate)
{
    const auto& ctx = ActorContext();
    THashSet<TString> consumers;
    bool ok = true;
    for (auto& operation : tx.Operations) {
        const TString& consumer = operation.GetConsumer();
        if (TxAffectedConsumers.contains(consumer)) {
            return EProcessResult::Blocked;
        }
        if (SetOffsetAffectedConsumers.contains(consumer)) {
            return EProcessResult::Blocked;
        }

        if (AffectedUsers.contains(consumer) && !GetPendingUserIfExists(consumer)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "' has been removed");
            ok = false;
            break;
        }

        if (!UsersInfoStorage->GetIfExists(consumer)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Unknown consumer '" << consumer << "'");
            ok = false;
            break;
        }

        bool isAffectedConsumer = AffectedUsers.contains(consumer);
        TUserInfoBase& userInfo = GetOrCreatePendingUser(consumer);

        if (operation.GetBegin() > operation.GetEnd()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "'" <<
                        " Bad request (invalid range) " <<
                        " Begin " << operation.GetBegin() <<
                        " End " << operation.GetEnd());
            ok = false;
        } else if (userInfo.Offset != (i64)operation.GetBegin()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "'" <<
                        " Bad request (gap) " <<
                        " Offset " << userInfo.Offset <<
                        " Begin " << operation.GetBegin());
            ok = false;
        } else if (operation.GetEnd() > EndOffset) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "'" <<
                        " Bad request (behind the last offset) " <<
                        " EndOffset " << EndOffset <<
                        " End " << operation.GetEnd());
            ok = false;
        }

        if (!ok) {
            if (!isAffectedConsumer) {
                AffectedUsers.erase(consumer);
            }
            break;
        }
        consumers.insert(consumer);
    }
    if (ok) {
        TxAffectedConsumers.insert(consumers.begin(), consumers.end());
    }
    predicate = ok;
    return EProcessResult::Continue;
}

bool TPartition::BeginTransaction(const TEvPQ::TEvProposePartitionConfig& event)
{
    ChangeConfig =
        MakeSimpleShared<TEvPQ::TEvChangePartitionConfig>(TopicConverter,
                                                          event.Config);
    PendingPartitionConfig = GetPartitionConfig(ChangeConfig->Config);

    SendChangeConfigReply = false;
    return true;
}

void TPartition::CommitWriteOperations(TTransaction& t)
{
    Y_ABORT_UNLESS(PersistRequest);
    Y_ABORT_UNLESS(!PartitionedBlob.IsInited());

    if (!t.WriteInfo) {
        return;
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

    if (!t.WriteInfo->BodyKeys.empty()) {
        PartitionedBlob = TPartitionedBlob(Partition,
                                           NewHead.Offset,
                                           "", // SourceId
                                           0,  // SeqNo
                                           1,  // TotalParts
                                           0,  // TotalSize
                                           Head,
                                           NewHead,
                                           Parameters->HeadCleared,  // headCleared
                                           Head.PackedSize != 0,     // needCompactHead
                                           MaxBlobSize);

        for (auto& k : t.WriteInfo->BodyKeys) {
            auto write = PartitionedBlob.Add(k.Key, k.Size);
            if (write && !write->Value.empty()) {
                AddCmdWrite(write, PersistRequest.Get(), ctx);
                CompactedKeys.emplace_back(write->Key, write->Value.size());
                ClearOldHead(write->Key.GetOffset(), write->Key.GetPartNo(), PersistRequest.Get());
            }
        }

    }

    if (const auto& formedBlobs = PartitionedBlob.GetFormedBlobs(); !formedBlobs.empty()) {
        ui32 curWrites = RenameTmpCmdWrites(PersistRequest.Get());
        RenameFormedBlobs(formedBlobs,
                          *Parameters,
                          curWrites,
                          PersistRequest.Get(),
                          ctx);
    }

    if (!t.WriteInfo->BodyKeys.empty()) {
        const auto& last = t.WriteInfo->BodyKeys.back();

        NewHead.Offset += (last.Key.GetOffset() + last.Key.GetCount());
    }

    if (!t.WriteInfo->BlobsFromHead.empty()) {
        auto& first = t.WriteInfo->BlobsFromHead.front();
        NewHead.PartNo = first.GetPartNo();

        Parameters->CurOffset = NewHead.Offset;
        Parameters->HeadCleared = !t.WriteInfo->BodyKeys.empty();

        PartitionedBlob = TPartitionedBlob(Partition,
                                           NewHead.Offset,
                                           first.SourceId,
                                           first.SeqNo,
                                           first.GetTotalParts(),
                                           first.GetTotalSize(),
                                           Head,
                                           NewHead,
                                           Parameters->HeadCleared, // headCleared
                                           false,                   // needCompactHead
                                           MaxBlobSize,
                                           first.GetPartNo());

        for (auto& blob : t.WriteInfo->BlobsFromHead) {
            TWriteMsg msg{Max<ui64>(), Nothing(), TEvPQ::TEvWrite::TMsg{
                .SourceId = blob.SourceId,
                .SeqNo = blob.SeqNo,
                .PartNo = (ui16)(blob.PartData ? blob.PartData->PartNo : 0),
                .TotalParts = (ui16)(blob.PartData ? blob.PartData->TotalParts : 1),
                .TotalSize = (ui32)(blob.PartData ? blob.PartData->TotalSize : blob.UncompressedSize),
                .CreateTimestamp = blob.CreateTimestamp.MilliSeconds(),
                .ReceiveTimestamp = blob.CreateTimestamp.MilliSeconds(),
                .DisableDeduplication = false,
                .WriteTimestamp = blob.WriteTimestamp.MilliSeconds(),
                .Data = blob.Data,
                .UncompressedSize = blob.UncompressedSize,
                .PartitionKey = blob.PartitionKey,
                .ExplicitHashKey = blob.ExplicitHashKey,
                .External = false,
                .IgnoreQuotaDeadline = true,
                .HeartbeatVersion = std::nullopt,
            }, std::nullopt};
            msg.Internal = true;

            ExecRequest(msg, *Parameters, PersistRequest.Get());

            auto& info = TxSourceIdForPostPersist[blob.SourceId];
            info.SeqNo = blob.SeqNo;
            info.Offset = NewHead.Offset;
        }
    }

    WriteInfosApplied.emplace_back(std::move(t.WriteInfo));
}

void TPartition::CommitTransaction(TSimpleSharedPtr<TTransaction>& t)
{
    const auto& ctx = ActorContext();
    if (t->Tx) {
        Y_ABORT_UNLESS(t->Predicate.Defined() && *t->Predicate);

        for (auto& operation : t->Tx->Operations) {
            TUserInfoBase& userInfo = GetOrCreatePendingUser(operation.GetConsumer());

            Y_ABORT_UNLESS(userInfo.Offset == (i64)operation.GetBegin());

            userInfo.Offset = operation.GetEnd();
        }
        CommitWriteOperations(*t);
        ChangePlanStepAndTxId(t->Tx->Step, t->Tx->TxId);
        ScheduleReplyCommitDone(t->Tx->Step, t->Tx->TxId);
    } else if (t->ProposeConfig) {
        Y_ABORT_UNLESS(t->Predicate.Defined() && *t->Predicate);

        BeginChangePartitionConfig(t->ProposeConfig->Config, ctx);
        ExecChangePartitionConfig();
        ChangePlanStepAndTxId(t->ProposeConfig->Step, t->ProposeConfig->TxId);

        ScheduleReplyCommitDone(t->ProposeConfig->Step, t->ProposeConfig->TxId);
    } else {
        Y_ABORT_UNLESS(t->ChangeConfig);
        ExecChangePartitionConfig();
    }
}

void TPartition::RollbackTransaction(TSimpleSharedPtr<TTransaction>& t)
{
    auto stepAndId = GetStepAndTxId(*t->Tx);

    auto txIter = TransactionsInflight.find(stepAndId.second);
    Y_ABORT_UNLESS(!txIter.IsEnd());

    if (t->Tx) {
        Y_ABORT_UNLESS(t->Predicate.Defined());
        ChangePlanStepAndTxId(t->Tx->Step, t->Tx->TxId);
    } else if (t->ProposeConfig) {
        Y_ABORT_UNLESS(t->Predicate.Defined());
        ChangingConfig = false;
        ChangePlanStepAndTxId(t->ProposeConfig->Step, t->ProposeConfig->TxId);
    } else {
        Y_ABORT_UNLESS(t->ChangeConfig);
        ChangeConfig = nullptr;
        ChangingConfig = false;
    }
}

void TPartition::BeginChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                            const TActorContext& ctx)
{
    TSet<TString> hasReadRule;
    for (auto& [consumer, info] : UsersInfoStorage->GetAll()) {
        hasReadRule.insert(consumer);
    }

    TSet<TString> important;
    for (const auto& consumer : config.GetConsumers()) {
        if (consumer.GetImportant()) {
            important.insert(consumer.GetName());
        }
    }

    for (auto& consumer : config.GetConsumers()) {
        auto& userInfo = GetOrCreatePendingUser(consumer.GetName(), 0);

        TInstant ts = TInstant::MilliSeconds(consumer.GetReadFromTimestampsMs());
        if (!ts) {
            ts += TDuration::MilliSeconds(1);
        }
        userInfo.ReadFromTimestamp = ts;
        userInfo.Important = important.contains(consumer.GetName());

        ui64 rrGen = consumer.GetGeneration();
        if (userInfo.ReadRuleGeneration != rrGen) {
            auto act = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer.GetName(), 0, "", 0, 0, 0, TActorId{},
                                        TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, rrGen);

            auto res = PreProcessUserAct(*act, ctx);
            ChangeConfigActs.emplace_back(std::move(act));

            Y_ABORT_UNLESS(res == EProcessResult::Continue);
        }
        hasReadRule.erase(consumer.GetName());
    }

    for (auto& consumer : hasReadRule) {
        GetOrCreatePendingUser(consumer);
        auto act = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer, 0, "", 0, 0, 0, TActorId{},
                                    TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0);

        auto res = PreProcessUserAct(*act, ctx);
        Y_ABORT_UNLESS(res == EProcessResult::Continue);
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
    FirstEvent = true;
    TxAffectedConsumers.clear();
    TxAffectedSourcesIds.clear();
    WriteAffectedSourcesIds.clear();
    SetOffsetAffectedConsumers.clear();
    BatchingState = ETxBatchingState::PreProcessing;
    WriteCycleSizeEstimate = 0;
    WriteKeysSizeEstimate = 0;

    if (ChangeConfig) {
        EndChangePartitionConfig(std::move(ChangeConfig->Config),
                                 ChangeConfig->TopicConverter,
                                 ctx);
    }

    for (auto& user : AffectedUsers) {
        if (auto* actual = GetPendingUserIfExists(user)) {
            TUserInfo& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);
            bool offsetHasChanged = (userInfo.Offset != actual->Offset);

            userInfo.Session = actual->Session;
            userInfo.Generation = actual->Generation;
            userInfo.Step = actual->Step;
            userInfo.Offset = actual->Offset;
            userInfo.ReadRuleGeneration = actual->ReadRuleGeneration;
            userInfo.ReadFromTimestamp = actual->ReadFromTimestamp;
            userInfo.HasReadRule = true;

            if (userInfo.Important != actual->Important) {
                if (userInfo.LabeledCounters) {
                    ScheduleDropPartitionLabeledCounters(userInfo.LabeledCounters->GetGroup());
                }
                userInfo.SetImportant(actual->Important);
            }
            if (userInfo.Important && userInfo.Offset < (i64)StartOffset) {
                userInfo.Offset = StartOffset;
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
    ChangingConfig = false;
    BatchingState = ETxBatchingState::PreProcessing;
}

void TPartition::EndChangePartitionConfig(NKikimrPQ::TPQTabletConfig&& config,
                                          NPersQueue::TTopicConverterPtr topicConverter,
                                          const TActorContext& ctx)
{
    Config = std::move(config);
    PartitionConfig = GetPartitionConfig(Config);
    PartitionGraph = MakePartitionGraph(Config);
    TopicConverter = topicConverter;
    NewPartition = false;

    Y_ABORT_UNLESS(Config.GetPartitionConfig().GetTotalPartitions() > 0);

    if (Config.GetPartitionStrategy().GetScaleThresholdSeconds() != SplitMergeAvgWriteBytes->GetDuration().Seconds()) {
        InitSplitMergeSlidingWindow();
    }

    Send(ReadQuotaTrackerActor, new TEvPQ::TEvChangePartitionConfig(TopicConverter, Config));
    Send(WriteQuotaTrackerActor, new TEvPQ::TEvChangePartitionConfig(TopicConverter, Config));
    TotalPartitionWriteSpeed = config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond();

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        if (Mirrorer) {
            ctx.Send(Mirrorer->Actor, new TEvPQ::TEvChangePartitionConfig(TopicConverter,
                                                                          Config));
        } else {
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
        OffloadActor = Register(CreateOffloadActor(Tablet, Partition, Config.GetOffloadConfig()));
    } else if (!Config.HasOffloadConfig() && OffloadActor) {
        Send(OffloadActor, new TEvents::TEvPoisonPill());
        OffloadActor = {};
    }
}


TString TPartition::GetKeyConfig() const
{
    return Sprintf("_config_%u", Partition.OriginalPartitionId);
}

void TPartition::ChangePlanStepAndTxId(ui64 step, ui64 txId)
{
    PlanStep = step;
    TxId = txId;
    TxIdHasChanged = true;
}

void TPartition::ResendPendingEvents(const TActorContext& ctx)
{
    while (!PendingEvents.empty()) {
        ctx.Schedule(TDuration::Zero(), PendingEvents.front().release());
        PendingEvents.pop_front();
    }
}

TPartition::EProcessResult TPartition::PreProcessImmediateTx(const NKikimrPQ::TEvProposeTransaction& tx)
{
    if (AffectedUsers.size() >= MAX_USERS) {
        return EProcessResult::Blocked;
    }
    Y_ABORT_UNLESS(tx.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_ABORT_UNLESS(tx.HasData());
    THashSet<TString> consumers;
    for (auto& operation : tx.GetData().GetOperations()) {
        if (!operation.HasBegin() || !operation.HasEnd() || !operation.HasConsumer()) {
            continue; //Write operation - handled separately via WriteInfo
        }

        Y_ABORT_UNLESS(operation.GetBegin() <= (ui64)Max<i64>(), "Unexpected begin offset: %" PRIu64, operation.GetBegin());
        Y_ABORT_UNLESS(operation.GetEnd() <= (ui64)Max<i64>(), "Unexpected end offset: %" PRIu64, operation.GetEnd());

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
        if (operation.GetBegin() > operation.GetEnd()) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (begin > end)");
            return EProcessResult::ContinueDrop;
        }
        consumers.insert(user);
    }
    SetOffsetAffectedConsumers.insert(consumers.begin(), consumers.end());
    WriteKeysSizeEstimate += consumers.size();
    return EProcessResult::Continue;
}

void TPartition::ExecImmediateTx(TTransaction& t)
{
    --ImmediateTxCount;
    auto& record = t.ProposeTransaction->Record;
    Y_ABORT_UNLESS(record.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_ABORT_UNLESS(record.HasData());


    //ToDo - check, this probably wouldn't work any longer.
    if (!t.Predicate.GetRef()) {
        ScheduleReplyPropose(record,
                             NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                             NKikimrPQ::TError::BAD_REQUEST,
                             t.Message);
        return;
    }
    for (auto& operation : record.GetData().GetOperations()) {
        if (!operation.HasBegin() || !operation.HasEnd() || !operation.HasConsumer()) {
            continue; //Write operation - handled separately via WriteInfo
        }

        Y_ABORT_UNLESS(operation.GetBegin() <= (ui64)Max<i64>(), "Unexpected begin offset: %" PRIu64, operation.GetBegin());
        Y_ABORT_UNLESS(operation.GetEnd() <= (ui64)Max<i64>(), "Unexpected end offset: %" PRIu64, operation.GetEnd());

        const TString& user = operation.GetConsumer();
        if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "the consumer has been deleted");
            return;
        }
        TUserInfoBase& userInfo = GetOrCreatePendingUser(user);

        if (operation.GetBegin() > operation.GetEnd()) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (begin > end)");
            return;
        }

        if (userInfo.Offset != (i64)operation.GetBegin()) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (gap)");
            return;
        }

        if (operation.GetEnd() > EndOffset) {
            ScheduleReplyPropose(record,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST,
                                 NKikimrPQ::TError::BAD_REQUEST,
                                 "incorrect offset range (commit to the future)");
            return;
        }
        userInfo.Offset = operation.GetEnd();
    }
    CommitWriteOperations(t);

    ScheduleReplyPropose(record,
                         NKikimrPQ::TEvProposeTransactionResult::COMPLETE,
                         NKikimrPQ::TError::OK,
                         "");

    ScheduleTransactionCompleted(record);
    return;
}

TPartition::EProcessResult TPartition::PreProcessUserActionOrTransaction(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& act)
{
    if (AffectedUsers.size() >= MAX_USERS) {
        return EProcessResult::Blocked;
    }
    return PreProcessUserAct(*act, ActorContext());
}

bool TPartition::ExecUserActionOrTransaction(
        TSimpleSharedPtr<TEvPQ::TEvSetClientInfo>& event, TEvKeyValue::TEvRequest*
) {
    CommitUserAct(*event);
    return true;
}

TPartition::EProcessResult TPartition::PreProcessUserActionOrTransaction(TMessage& msg)
{
    if (WriteCycleSize >= MAX_WRITE_CYCLE_SIZE) {
        return EProcessResult::Blocked;
    }

    auto result = EProcessResult::Continue;
    if (msg.IsWrite()) {
        result = PreProcessRequest(msg.GetWrite());
    } else if (msg.IsRegisterMessageGroup()) {
        result = PreProcessRequest(msg.GetRegisterMessageGroup());
    } else if (msg.IsDeregisterMessageGroup()) {
        result = PreProcessRequest(msg.GetDeregisterMessageGroup());
    } else if (msg.IsSplitMessageGroup()) {
        result = PreProcessRequest(msg.GetSplitMessageGroup());
    } else {
        Y_ABORT_UNLESS(msg.IsOwnership());
    }
    return result;
}

bool TPartition::ExecUserActionOrTransaction(TMessage& msg, TEvKeyValue::TEvRequest* request)
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
        Y_ABORT_UNLESS(msg.IsOwnership());
    }
    if (doReply) {
        EmplaceResponse(std::move(msg), ctx);
    }
    return true;
}

TPartition::EProcessResult TPartition::PreProcessUserAct(
        TEvPQ::TEvSetClientInfo& act, const TActorContext&
) {
    Y_ABORT_UNLESS(!KVWriteInProgress);

    const TString& user = act.ClientId;
    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET) {
        if (TxAffectedConsumers.contains(user)) {
            return EProcessResult::Blocked;
        }
    }
    WriteKeysSizeEstimate += 1;
    SetOffsetAffectedConsumers.insert(user);
    return EProcessResult::Continue;
}

void TPartition::CommitUserAct(TEvPQ::TEvSetClientInfo& act) {
    const bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.Strict);
    const TString& user = act.ClientId;
    const auto& ctx = ActorContext();
    if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
        switch (act.Type) {
        case TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE:
            break;
        case TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE:
            return;
        default:
            ScheduleReplyError(act.Cookie,
                               NPersQueue::NErrorCode::WRONG_COOKIE,
                               "request to deleted read rule");
            return;
        }
    }
    auto& userInfo = GetOrCreatePendingUser(user);

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition
                    << " user " << user << " drop request"
        );

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
                                       ts.first, ts.second);

        return;
    }

    if (act.Type != TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && act.Type != TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE
            && !act.SessionId.empty() && userInfo.Session != act.SessionId //request to wrong session
            && (act.Type != TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION || !userInfo.Session.empty()) //but allow DropSession request when session is already dropped - for idempotence
            || (act.ClientId != CLIENTID_WITHOUT_CONSUMER && act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && !userInfo.Session.empty()
                 && (act.Generation < userInfo.Generation || act.Generation == userInfo.Generation && act.Step <= userInfo.Step))) { //old generation request
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);

        ScheduleReplyError(act.Cookie,
                           NPersQueue::NErrorCode::WRONG_COOKIE,
                           TStringBuilder() << "set offset in already dead session " << act.SessionId << " actual is " << userInfo.Session);

        return;
    }

    if (!act.SessionId.empty() && act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && (i64)act.Offset <= userInfo.Offset) { //this is stale request, answer ok for it
        ScheduleReplyOk(act.Cookie);

        return;
    }

    if (strictCommitOffset && act.Offset < StartOffset) {
        // strict commit to past, reply error
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ScheduleReplyError(act.Cookie,
                           NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST,
                           TStringBuilder() << "set offset " <<  act.Offset << " to past for consumer " << act.ClientId << " actual start offset is " << StartOffset);

        return;
    }

    //request in correct session - make it

    ui64 offset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET ? act.Offset : userInfo.Offset);
    ui64 readRuleGeneration = userInfo.ReadRuleGeneration;

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE) {
        readRuleGeneration = act.ReadRuleGeneration;
        offset = 0;
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition
                    << " user " << act.ClientId << " reinit request with generation " << readRuleGeneration
        );
    }

    Y_ABORT_UNLESS(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);

    if (offset > EndOffset) {
        if (strictCommitOffset) {
            TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
            ScheduleReplyError(act.Cookie,
                            NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE,
                            TStringBuilder() << "strict commit can't set offset " <<  act.Offset << " to future, consumer " << act.ClientId << ", actual end offset is " << EndOffset);

            return;
        }
        LOG_WARN_S(
                ctx, NKikimrServices::PERSQUEUE,
                "commit to future - topic " << TopicName() << " partition " << Partition
                    << " client " << act.ClientId << " EndOffset " << EndOffset << " offset " << offset
        );
        act.Offset = EndOffset;
/*
        TODO:
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_FUTURE,
            TStringBuilder() << "can't commit to future. Offset " << offset << " EndOffset " << EndOffset);
        userInfo.UserActrs.pop_front();
        continue;
*/
    }

    if (!IsActive() && act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && static_cast<i64>(EndOffset) == userInfo.Offset && offset < EndOffset) {
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ScheduleReplyError(act.Cookie,
                           NPersQueue::NErrorCode::SET_OFFSET_ERROR_COMMIT_TO_PAST,
                           TStringBuilder() << "set offset " <<  act.Offset << " to past for consumer " << act.ClientId << " for inactive partition");

        return;
    }

    RemoveUserAct(act.ClientId);
    return EmulatePostProcessUserAct(act, userInfo, ActorContext());
}

void TPartition::EmulatePostProcessUserAct(const TEvPQ::TEvSetClientInfo& act,
                                           TUserInfoBase& userInfo,
                                           const TActorContext& ctx)
{
    const TString& user = act.ClientId;
    ui64 offset = act.Offset;
    const TString& session = act.SessionId;
    ui32 generation = act.Generation;
    ui32 step = act.Step;
    const ui64 readRuleGeneration = act.ReadRuleGeneration;

    bool setSession = act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION;
    bool dropSession = act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION;
    bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.SessionId.empty());

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        userInfo.ReadRuleGeneration = 0;
        userInfo.Session = "";
        userInfo.Generation = userInfo.Step = 0;
        userInfo.Offset = 0;

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition << " user " << user
                    << " drop done"
        );
        PendingUsersInfo.erase(user);
    } else if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition << " user " << user
                    << " reinit with generation " << readRuleGeneration << " done"
        );

        userInfo.ReadRuleGeneration = readRuleGeneration;
        userInfo.Session = "";
        userInfo.PartitionSessionId = 0;
        userInfo.Generation = userInfo.Step = 0;
        userInfo.Offset = 0;

        if (userInfo.Important) {
            userInfo.Offset = StartOffset;
        }
    } else {
        if (setSession || dropSession) {
            offset = userInfo.Offset;
            auto *ui = UsersInfoStorage->GetIfExists(userInfo.User);
            auto ts = ui ? GetTime(*ui, userInfo.Offset) : std::make_pair<TInstant, TInstant>(TInstant::Zero(), TInstant::Zero());

            ScheduleReplyGetClientOffsetOk(act.Cookie,
                                           offset,
                                           ts.first, ts.second);
        } else {
            ScheduleReplyOk(act.Cookie);
        }

        if (setSession) {
            userInfo.Session = session;
            userInfo.Generation = generation;
            userInfo.Step = step;
            userInfo.PipeClient = act.PipeClient;
            userInfo.PartitionSessionId = act.PartitionSessionId;
        } else if ((dropSession && act.PipeClient == userInfo.PipeClient) || strictCommitOffset) {
            userInfo.Session = "";
            userInfo.PartitionSessionId = 0;
            userInfo.Generation = 0;
            userInfo.Step = 0;
            userInfo.PipeClient = {};
        }

        Y_ABORT_UNLESS(offset <= (ui64)Max<i64>(), "Unexpected Offset: %" PRIu64, offset);
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition << " user " << user
                    << (setSession || dropSession ? " session" : " offset")
                    << " is set to " << offset << " (startOffset " << StartOffset << ") session " << session
        );

        userInfo.Offset = offset;

        auto counter = setSession ? COUNTER_PQ_CREATE_SESSION_OK : (dropSession ? COUNTER_PQ_DELETE_SESSION_OK : COUNTER_PQ_SET_CLIENT_OFFSET_OK);
        TabletCounters.Cumulative()[counter].Increment(1);
    }
}

void TPartition::ScheduleReplyOk(const ui64 dst)
{
    Replies.emplace_back(Tablet,
                         MakeReplyOk(dst).Release());
}

void TPartition::ScheduleReplyGetClientOffsetOk(const ui64 dst,
                                                const i64 offset,
                                                const TInstant writeTimestamp, const TInstant createTimestamp)
{
    Replies.emplace_back(Tablet,
                         MakeReplyGetClientOffsetOk(dst,
                                                    offset,
                                                    writeTimestamp, createTimestamp).Release());

}

void TPartition::ScheduleReplyError(const ui64 dst,
                                    NPersQueue::NErrorCode::EErrorCode errorCode,
                                    const TString& error)
{
    Replies.emplace_back(Tablet,
                         MakeReplyError(dst,
                                        errorCode,
                                        error).Release());
}

void TPartition::ScheduleReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                      NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode,
                                      NKikimrPQ::TError::EKind kind,
                                      const TString& reason)
{
    Replies.emplace_back(ActorIdFromProto(event.GetSourceActor()),
                         MakeReplyPropose(event,
                                          statusCode,
                                          kind, reason).Release());
}

void TPartition::ScheduleReplyCommitDone(ui64 step, ui64 txId)
{
    Replies.emplace_back(Tablet,
                         MakeCommitDone(step, txId).Release());
}

void TPartition::ScheduleDropPartitionLabeledCounters(const TString& group)
{
    Replies.emplace_back(Tablet,
                         MakeHolder<TEvPQ::TEvPartitionLabeledCountersDrop>(Partition, group).Release());
}

void TPartition::SchedulePartitionConfigChanged()
{
    Replies.emplace_back(Tablet,
                         MakeHolder<TEvPQ::TEvPartitionConfigChanged>(Partition).Release());
}

void TPartition::ScheduleDeletePartitionDone()
{
    Replies.emplace_back(Tablet,
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
                             ui64 readRuleGeneration)
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

    Y_ABORT_UNLESS(PlanStep.Defined());
    Y_ABORT_UNLESS(TxId.Defined());

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
                        userInfo->ReadRuleGeneration);
        } else {
            AddCmdDeleteRange(request,
                              ikey, ikeyDeprecated);
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
    Y_ABORT_UNLESS(ChangeConfig->Config.SerializeToString(&data));

    auto write = request.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(data.Data(), data.Size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
}

TUserInfoBase& TPartition::GetOrCreatePendingUser(const TString& user,
                                                  TMaybe<ui64> readRuleGeneration)
{
    TUserInfoBase* userInfo = nullptr;
    auto i = PendingUsersInfo.find(user);
    if (i == PendingUsersInfo.end()) {
        auto ui = UsersInfoStorage->GetIfExists(user);
        auto [p, _] = PendingUsersInfo.emplace(user, UsersInfoStorage->CreateUserInfo(user, readRuleGeneration));

        if (ui) {
            p->second.Session = ui->Session;
            p->second.PartitionSessionId = ui->PartitionSessionId;
            p->second.PipeClient = ui->PipeClient;

            p->second.Generation = ui->Generation;
            p->second.Step = ui->Step;
            p->second.Offset = ui->Offset;
            p->second.ReadRuleGeneration = ui->ReadRuleGeneration;
            p->second.Important = ui->Important;
            p->second.ReadFromTimestamp = ui->ReadFromTimestamp;
        }

        userInfo = &p->second;
    } else {
        userInfo = &i->second;
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

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyOk(const ui64 dst)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = *response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    return response;
}

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyGetClientOffsetOk(const ui64 dst,
                                                                        const i64 offset,
                                                                        const TInstant writeTimestamp, const TInstant createTimestamp)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = *response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto user = resp.MutablePartitionResponse()->MutableCmdGetClientOffsetResult();
    if (offset > -1)
        user->SetOffset(offset);
    if (writeTimestamp)
        user->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
    if (createTimestamp) {
        Y_ABORT_UNLESS(writeTimestamp);
        user->SetCreateTimestampMS(createTimestamp.MilliSeconds());
    }
    user->SetEndOffset(EndOffset);
    user->SetSizeLag(GetSizeLag(offset));
    user->SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());

    return response;
}

THolder<TEvPQ::TEvError> TPartition::MakeReplyError(const ui64 dst,
                                                    NPersQueue::NErrorCode::EErrorCode errorCode,
                                                    const TString& error)
{
    //
    // FIXME(abcdef): в ReplyPersQueueError есть дополнительные действия
    //
    return MakeHolder<TEvPQ::TEvError>(errorCode, error, dst);
}

THolder<TEvPersQueue::TEvProposeTransactionResult> TPartition::MakeReplyPropose(const NKikimrPQ::TEvProposeTransaction& event,
                                                                                NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode,
                                                                                NKikimrPQ::TError::EKind kind,
                                                                                const TString& reason)
{
    auto response = MakeHolder<TEvPersQueue::TEvProposeTransactionResult>();

    response->Record.SetOrigin(TabletID);
    response->Record.SetStatus(statusCode);
    response->Record.SetTxId(event.GetTxId());

    if (kind != NKikimrPQ::TError::OK) {
        auto* error = response->Record.MutableErrors()->Add();
        error->SetKind(kind);
        error->SetReason(reason);
    }

    return response;
}

THolder<TEvPQ::TEvTxCommitDone> TPartition::MakeCommitDone(ui64 step, ui64 txId)
{
    return MakeHolder<TEvPQ::TEvTxCommitDone>(step, txId, Partition);
}

void TPartition::ScheduleUpdateAvailableSize(const TActorContext& ctx) {
    ctx.Schedule(UPDATE_AVAIL_SIZE_INTERVAL, new TEvPQ::TEvUpdateAvailableSize());
}

void TPartition::ClearOldHead(const ui64 offset, const ui16 partNo, TEvKeyValue::TEvRequest* request) {
    for (auto it = HeadKeys.rbegin(); it != HeadKeys.rend(); ++it) {
        if (it->Key.GetOffset() > offset || it->Key.GetOffset() == offset && it->Key.GetPartNo() >= partNo) {
            auto del = request->Record.AddCmdDeleteRange();
            auto range = del->MutableRange();
            range->SetFrom(it->Key.Data(), it->Key.Size());
            range->SetIncludeFrom(true);
            range->SetTo(it->Key.Data(), it->Key.Size());
            range->SetIncludeTo(true);
        } else {
            break;
        }
    }
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
    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Got quota." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << ": Cookie: " << cookie
    );

    // Search for proper request
    Y_ABORT_UNLESS(TopicQuotaRequestCookie == cookie);
    ConsumeBlobQuota();
    TopicQuotaRequestCookie = 0;
    RemoveMessagesToQueue(QuotaWaitingRequests);

    // Metrics
    TopicQuotaWaitTimeForCurrentBlob = ev->Get()->AccountQuotaWaitTime;
    PartitionQuotaWaitTimeForCurrentBlob = ev->Get()->PartitionQuotaWaitTime;
    if (TopicWriteQuotaWaitCounter) {
        TopicWriteQuotaWaitCounter->IncFor(TopicQuotaWaitTimeForCurrentBlob.MilliSeconds());
    }

    RequestBlobQuota();
    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(NReadQuoterEvents::TEvQuotaCountersUpdated::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->ForWriteQuota) {
        LOG_ALERT_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Got TEvQuotaCountersUpdated for write counters, this is unexpected. Event ignored");
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
        Register(new TMirrorer(Tablet, SelfId(), TopicConverter, Partition.InternalPartitionId, IsLocalDC,  EndOffset, Config.GetPartitionConfig().GetMirrorFrom(), TabletCounters)),
        TabletCounters
    );
}

bool IsQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig,
                      bool isLocalDC)
{
    const auto& quotingConfig = pqConfig.GetQuotingConfig();
    return isLocalDC && quotingConfig.GetEnableQuoting() && !pqConfig.GetTopicsAreFirstClassCitizen();
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
        LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "SubDomainOutOfSpace was changed." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << "." <<
            " SubDomainOutOfSpace: " << SubDomainOutOfSpace
        );

        if (!SubDomainOutOfSpace) {
            ProcessTxsAndUserActs(ctx);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;

    if (Partition.InternalPartitionId != record.GetPartition()) {
        LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "TEvCheckPartitionStatusRequest for wrong partition " << record.GetPartition() << "." <<
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
    Y_ABORT_UNLESS(IsSupportive());

    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::Handle(TEvPQ::TEvDeletePartition::TPtr&, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(IsSupportive());
    Y_ABORT_UNLESS(DeletePartitionState == DELETION_NOT_INITED);

    DeletePartitionState = DELETION_INITED;

    ProcessTxsAndUserActs(ctx);
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
    ScheduleReplyError(msg.GetCookie(), NPersQueue::NErrorCode::ERROR, "The transaction is completed");
}

void TPartition::ScheduleTransactionCompleted(const NKikimrPQ::TEvProposeTransaction& tx)
{
    Y_ABORT_UNLESS(tx.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_ABORT_UNLESS(tx.HasData());

    TMaybe<TWriteId> writeId;
    if (tx.GetData().HasWriteId()) {
        writeId = GetWriteId(tx.GetData());
    }

    Replies.emplace_back(Tablet,
                         MakeHolder<TEvPQ::TEvTransactionCompleted>(writeId).Release());
}

const NKikimrPQ::TPQTabletConfig::TPartition* TPartition::GetPartitionConfig(const NKikimrPQ::TPQTabletConfig& config)
{
    return NPQ::GetPartitionConfig(config, Partition.OriginalPartitionId);
}

bool TPartition::IsSupportive() const
{
    return Partition.IsSupportivePartition();
}

} // namespace NKikimr::NPQ
