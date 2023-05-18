#include "event_helpers.h"
#include "mirrorer.h"
#include "partition_util.h"
#include "partition.h"
#include "read.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/quoter.h>
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

namespace NKikimr::NPQ {

static const TDuration WAKE_TIMEOUT = TDuration::Seconds(5);
static const TDuration UPDATE_AVAIL_SIZE_INTERVAL = TDuration::MilliSeconds(100);
static const ui32 MAX_USERS = 1000;
static const ui32 MAX_TXS = 1000;

auto GetStepAndTxId(ui64 step, ui64 txId)
{
    return std::make_pair(step, txId);
}

template<class E>
auto GetStepAndTxId(const E& event)
{
    return GetStepAndTxId(event.Step, event.TxId);
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
                              NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode)
{
    ctx.Send(ActorIdFromProto(event.GetSourceActor()),
             MakeReplyPropose(event, statusCode).Release());
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

TPartition::TPartition(ui64 tabletId, ui32 partition, const TActorId& tablet, const TActorId& blobCache,
                       const NPersQueue::TTopicConverterPtr& topicConverter, bool isLocalDC, TString dcId, bool isServerless,
                       const NKikimrPQ::TPQTabletConfig& tabletConfig, const TTabletCountersBase& counters, bool subDomainOutOfSpace,
                       bool newPartition,
                       TVector<TTransaction> distrTxs)
    : Initializer(this)
    , TabletID(tabletId)
    , Partition(partition)
    , TabletConfig(tabletConfig)
    , Counters(counters)
    , TopicConverter(topicConverter)
    , IsLocalDC(isLocalDC)
    , DCId(std::move(dcId))
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
    , WriteCycleSize(0)
    , WriteNewSize(0)
    , WriteNewSizeInternal(0)
    , WriteNewSizeUncompressed(0)
    , WriteNewMessages(0)
    , WriteNewMessagesInternal(0)
    , DiskIsFull(false)
    , SubDomainOutOfSpace(subDomainOutOfSpace)
    , HasDataReqNum(0)
    , AvgWriteBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000}, {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
    , AvgQuotaBytes{{TDuration::Seconds(1), 1000}, {TDuration::Minutes(1), 1000}, {TDuration::Hours(1), 2000}, {TDuration::Days(1), 2000}}
    , ReservedSize(0)
    , Channel(0)
    , WriteBufferIsFullCounter(nullptr)
    , WriteLagMs(TDuration::Minutes(1), 100)
{
    TabletCounters.Populate(Counters);

    if (!distrTxs.empty()) {
        std::move(distrTxs.begin(), distrTxs.end(),
                  std::back_inserter(DistrTxs));
        TxInProgress = DistrTxs.front().Predicate.Defined();
    }
}

void TPartition::EmplaceResponse(TMessage&& message, const TActorContext& ctx) {
    Responses.emplace_back(
        message.Body,
        WriteQuota->GetQuotedTime(ctx.Now()) - message.QuotedTime,
        (ctx.Now() - TInstant::Zero()) - message.QueueTime,
        ctx.Now()
    );
}

ui64 TPartition::MeteringDataSize(const TActorContext& ctx) const {
    ui64 size = Size();
    if (!DataKeysBody.empty()) {
        size -= DataKeysBody.front().Size;
    }
    auto expired = ctx.Now() - TDuration::Seconds(Config.GetPartitionConfig().GetLifetimeSeconds());
    for(size_t i = 0; i < HeadKeys.size(); ++i) {
        auto& key = HeadKeys[i];
        if (expired < key.Timestamp) {
            break;
        }
        size -= key.Size;
    }
    Y_VERIFY(size >= 0, "Metering data size must be positive");
    return size;
}

ui64 TPartition::GetUsedStorage(const TActorContext& ctx) {
    auto duration = ctx.Now() - LastUsedStorageMeterTimestamp;
    LastUsedStorageMeterTimestamp = ctx.Now();
    ui64 size = MeteringDataSize(ctx);
    return size * duration.MilliSeconds() / 1000 / 1_MB; // mb*seconds
}

void TPartition::HandleWakeup(const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);

    ctx.Schedule(WAKE_TIMEOUT, new TEvents::TEvWakeup());
    ctx.Send(Tablet, new TEvPQ::TEvPartitionCounters(Partition, TabletCounters));

    ui64 usedStorage = GetUsedStorage(ctx);
    if (usedStorage > 0) {
        ctx.Send(Tablet, new TEvPQ::TEvMetering(EMeteringJson::UsedStorageV1, usedStorage));
    }

    ReportCounters(ctx);

    ProcessHasDataRequests(ctx);

    auto now = ctx.Now();
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

    if (CurrentStateFunc() == &TThis::StateWrite) {//Write will handle all itself
        return;
    }
    Y_VERIFY(CurrentStateFunc() == &TThis::StateIdle);

    if (ManageWriteTimestampEstimate)
        WriteTimestampEstimate = now;

    THolder <TEvKeyValue::TEvRequest> request = MakeHolder<TEvKeyValue::TEvRequest>();
    bool haveChanges = CleanUp(request.Get(), false, ctx);
    if (DiskIsFull) {
        AddCheckDiskRequest(request.Get(), Config.GetPartitionConfig().GetNumChannels());
        haveChanges = true;
    }

    if (haveChanges) {
        WriteCycleStartTime = ctx.Now();
        WriteStartTime = ctx.Now();
        TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
        WritesTotal.Inc();
        Become(&TThis::StateWrite);
        AddMetaKey(request.Get());
        ctx.Send(Tablet, request.Release());
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

    TString out;
    Y_PROTOBUF_SUPPRESS_NODISCARD meta.SerializeToString(&out);

    write->SetKey(ikey.Data(), ikey.Size());
    write->SetValue(out.c_str(), out.size());
    write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);

}

bool TPartition::CleanUp(TEvKeyValue::TEvRequest* request, bool hasWrites, const TActorContext& ctx) {
    bool haveChanges = CleanUpBlobs(request, hasWrites, ctx);
    LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE, TStringBuilder() << "Have " <<
              request->Record.CmdDeleteRangeSize() << " items to delete old stuff");

    haveChanges |= SourceIdStorage.DropOldSourceIds(request, ctx.Now(), StartOffset, Partition,
                                                    Config.GetPartitionConfig());
    if (haveChanges) {
        SourceIdStorage.MarkOwnersForDeletedSourceId(Owners);
    }
    LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE, TStringBuilder() << "Have " <<
              request->Record.CmdDeleteRangeSize() << " items to delete all stuff");
    LOG_TRACE(ctx, NKikimrServices::PERSQUEUE, TStringBuilder() << "Delete command " << request->ToString());

    return haveChanges;
}

bool TPartition::CleanUpBlobs(TEvKeyValue::TEvRequest *request, bool hasWrites, const TActorContext& ctx) {
    if (StartOffset == EndOffset || DataKeysBody.size() <= 1)
        return false;

    const auto& partConfig = Config.GetPartitionConfig();
    ui64 minOffset = EndOffset;
    for (const auto& importantClientId : partConfig.GetImportantClientId()) {
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(importantClientId);
        ui64 curOffset = StartOffset;
        if (userInfo && userInfo->Offset >= 0) //-1 means no offset
            curOffset = userInfo->Offset;
        minOffset = Min<ui64>(minOffset, curOffset);
    }

    bool hasDrop = false;
    ui64 endOffset = StartOffset;

    const std::optional<ui64> storageLimit = partConfig.HasStorageLimitBytes()
        ? std::optional<ui64>{partConfig.GetStorageLimitBytes()} : std::nullopt;
    const TDuration lifetimeLimit{TDuration::Seconds(partConfig.GetLifetimeSeconds())};

    if (DataKeysBody.size() > 1) {
        auto retentionCondition = [&]() -> bool {
            const auto bodySize = BodySize - DataKeysBody.front().Size;
            const bool timeRetention = (ctx.Now() >= (DataKeysBody.front().Timestamp + lifetimeLimit));
            return storageLimit.has_value()
                ? ((bodySize >= *storageLimit) || timeRetention)
                : timeRetention;
        };

        while (DataKeysBody.size() > 1 &&
               retentionCondition() &&
               (minOffset > DataKeysBody[1].Key.GetOffset() ||
                (minOffset == DataKeysBody[1].Key.GetOffset() &&
                 DataKeysBody[1].Key.GetPartNo() == 0))) { // all offsets from blob[0] are readed, and don't delete last blob
            BodySize -= DataKeysBody.front().Size;

            DataKeysBody.pop_front();
            if (!GapOffsets.empty() && DataKeysBody.front().Key.GetOffset() == GapOffsets.front().second) {
                GapSize -= GapOffsets.front().second - GapOffsets.front().first;
                GapOffsets.pop_front();
            }
            hasDrop = true;
        }

        Y_VERIFY(!DataKeysBody.empty());

        endOffset = DataKeysBody.front().Key.GetOffset();
        if (DataKeysBody.front().Key.GetPartNo() > 0) {
            ++endOffset;
        }
    }

    TDataKey lastKey = HeadKeys.empty() ? DataKeysBody.back() : HeadKeys.back();

    if (!hasWrites &&
        ctx.Now() >= lastKey.Timestamp + lifetimeLimit &&
        minOffset == EndOffset &&
        false) { // disable drop of all data
        Y_VERIFY(!HeadKeys.empty() || !DataKeysBody.empty());

        Y_VERIFY(CompactedKeys.empty());
        Y_VERIFY(NewHead.PackedSize == 0);
        Y_VERIFY(NewHeadKey.Size == 0);

        Y_VERIFY(EndOffset == Head.GetNextOffset());
        Y_VERIFY(EndOffset == NewHead.GetNextOffset() || NewHead.GetNextOffset() == 0);

        hasDrop = true;

        BodySize = 0;
        DataKeysBody.clear();
        GapSize = 0;
        GapOffsets.clear();

        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
        HeadKeys.clear();
        Head.Clear();
        Head.Offset = EndOffset;
        NewHead.Clear();
        NewHead.Offset = EndOffset;
        endOffset = EndOffset;
    } else {
        if (hasDrop) {
            lastKey = DataKeysBody.front();
        }
    }

    if (!hasDrop)
        return false;

    StartOffset = endOffset;

    TKey key(TKeyPrefix::TypeData, Partition, 0, 0, 0, 0); //will drop all that could not be dropped before of case of full disks

    auto del = request->Record.AddCmdDeleteRange();
    auto range = del->MutableRange();
    range->SetFrom(key.Data(), key.Size());
    range->SetIncludeFrom(true);
    range->SetTo(lastKey.Key.Data(), lastKey.Key.Size());
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

void TPartition::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    // Reply to all outstanding requests in order to destroy corresponding actors

    TStringBuilder ss;
    ss << "Tablet is restarting, topic '" << TopicName() << "'";

    for (const auto& ev : WaitToChangeOwner) {
        ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::INITIALIZING, ss);
    }

    for (const auto& w : Requests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::INITIALIZING, ss);
    }

    for (const auto& wr : Responses) {
        ReplyError(ctx, wr.GetCookie(), NPersQueue::NErrorCode::INITIALIZING, TStringBuilder() << ss << " (WriteResponses)");
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

    Die(ctx);
}


bool CheckDiskStatus(const TStorageStatusFlags status) {
    return !status.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove);
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
            "init complete for topic '" << TopicName() << "' partition " << Partition << " " << ctx.SelfID
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

    FillReadFromTimestamps(Config, ctx);
    ResendPendingEvents(ctx);
    ProcessTxsAndUserActs(ctx);

    ctx.Send(ctx.SelfID, new TEvents::TEvWakeup());
    if (!NewPartition) {
        ctx.Send(Tablet, new TEvPQ::TEvInitComplete(Partition));
    }
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
        Y_VERIFY(userInfoPair.second.Offset >= 0);
        ReadTimestampForOffset(userInfoPair.first, userInfoPair.second, ctx);
    }
    if (PartitionCountersLabeled) {
        PartitionCountersLabeled->GetCounters()[METRIC_INIT_TIME] = InitDuration.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_LIFE_TIME] = CreationTime.MilliSeconds();
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS] = 1;
        PartitionCountersLabeled->GetCounters()[METRIC_PARTITIONS_TOTAL] = Config.PartitionIdsSize();
        ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
    UpdateUserInfoEndOffset(ctx.Now());

    ScheduleUpdateAvailableSize(ctx);

    if (Config.GetPartitionConfig().HasMirrorFrom()) {
        CreateMirrorerActor();
    }

    ReportCounters(ctx);
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
    result.SetPartition(Partition);
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

    Y_VERIFY(AvgWriteBytes.size() == 4);
    result.SetAvgWriteSpeedPerSec(AvgWriteBytes[0].GetValue());
    result.SetAvgWriteSpeedPerMin(AvgWriteBytes[1].GetValue());
    result.SetAvgWriteSpeedPerHour(AvgWriteBytes[2].GetValue());
    result.SetAvgWriteSpeedPerDay(AvgWriteBytes[3].GetValue());

    Y_VERIFY(AvgQuotaBytes.size() == 4);
    result.SetAvgQuotaSpeedPerSec(AvgQuotaBytes[0].GetValue());
    result.SetAvgQuotaSpeedPerMin(AvgQuotaBytes[1].GetValue());
    result.SetAvgQuotaSpeedPerHour(AvgQuotaBytes[2].GetValue());
    result.SetAvgQuotaSpeedPerDay(AvgQuotaBytes[3].GetValue());

    result.SetSourceIdCount(SourceIdStorage.GetInMemorySourceIds().size());
    result.SetSourceIdRetentionPeriodSec((ctx.Now() - SourceIdStorage.MinAvailableTimestamp(ctx.Now())).Seconds());

    result.SetWriteBytesQuota(WriteQuota->GetTotalSpeed());

    TVector<ui64> resSpeed;
    resSpeed.resize(4);
    ui64 maxQuota = 0;
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        if (ev->Get()->ClientId.empty() || ev->Get()->ClientId == userInfo.User) {
            Y_VERIFY(userInfo.AvgReadBytes.size() == 4);
            for (ui32 i = 0; i < 4; ++i) {
                resSpeed[i] += userInfo.AvgReadBytes[i].GetValue();
            }
            maxQuota += userInfo.ReadQuota.GetTotalSpeed();
        }
        if (ev->Get()->ClientId == userInfo.User) { //fill lags
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
        }

    }
    result.SetAvgReadSpeedPerSec(resSpeed[0]);
    result.SetAvgReadSpeedPerMin(resSpeed[1]);
    result.SetAvgReadSpeedPerHour(resSpeed[2]);
    result.SetAvgReadSpeedPerDay(resSpeed[3]);

    result.SetReadBytesQuota(maxQuota);

    result.SetPartitionSize(MeteringDataSize(ctx));
    result.SetUsedReserveSize(UsedReserveSize(ctx));
    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);

    result.SetLastWriteTimestampMs(WriteTimestamp.MilliSeconds());
    result.SetWriteLagMs(WriteLagMs.GetValue());

    *result.MutableErrors() = {Errors.begin(), Errors.end()};

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                "Topic PartitionStatus PartitionSize: " << result.GetPartitionSize()
                << " UsedReserveSize: " << result.GetUsedReserveSize()
                << " ReserveSize: " << ReserveSize()
                << " PartitionConfig" << Config.GetPartitionConfig();
    );

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
            if (!userInfo.HasReadRule && !userInfo.Important)
                continue;
            auto* cac = ac->AddConsumerAggregatedCounters();
            cac->SetConsumer(userInfo.User);
            for (ui32 i = 0; i < userInfo.LabeledCounters->GetCounters().Size(); ++i) {
                cac->AddValues(userInfo.LabeledCounters->GetCounters()[i].Get());
            }
        }
    }
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result));
}

void TPartition::HandleOnInit(TEvPQ::TEvPartitionStatus::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TStatusResponse::TPartResult result;
    result.SetPartition(Partition);
    result.SetStatus(NKikimrPQ::TStatusResponse::STATUS_INITIALIZING);
    result.SetLastInitDurationSeconds((ctx.Now() - CreationTime).Seconds());
    result.SetCreationTimestamp(CreationTime.Seconds());
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionStatusResponse(result));
}


void TPartition::Handle(TEvPQ::TEvGetPartitionClientInfo::TPtr& ev, const TActorContext& ctx) {
    THolder<TEvPersQueue::TEvPartitionClientInfoResponse> response = MakeHolder<TEvPersQueue::TEvPartitionClientInfoResponse>();
    NKikimrPQ::TClientInfoResponse& result(response->Record);
    result.SetPartition(Partition);
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
    Y_VERIFY(!container.empty());
    auto it = std::upper_bound(container.begin(), container.end(), offset,
                    [](const ui64 offset, const TDataKey& p) {
                        return offset < p.Key.GetOffset() ||
                                        offset == p.Key.GetOffset() && p.Key.GetPartNo() > 0;
                    });
    // Always greater
    Y_VERIFY(it != container.begin(),
             "Tablet %lu StartOffset %lu, HeadOffset %lu, offset %lu, containter size %lu, first-elem: %s",
             TabletID, StartOffset, Head.Offset, offset, container.size(),
             container.front().Key.ToString().c_str());
    Y_VERIFY(it == container.end() ||
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
    Y_VERIFY(event.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_VERIFY(event.HasData());
    const NKikimrPQ::TDataTransaction& txBody = event.GetData();

    if (!txBody.GetImmediate()) {
        ReplyPropose(ctx,
                     event,
                     NKikimrPQ::TEvProposeTransactionResult::ABORTED);
        return;
    }

    if (ImmediateTxs.size() > MAX_TXS) {
        ReplyPropose(ctx,
                     event,
                     NKikimrPQ::TEvProposeTransactionResult::OVERLOADED);
        return;
    }

    AddImmediateTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext& ctx)
{
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::HandleOnInit(TEvPQ::TEvProposePartitionConfig::TPtr& ev, const TActorContext&)
{
    PendingEvents.emplace_back(ev->ReleaseBase().Release());
}

void TPartition::Handle(TEvPQ::TEvTxCalcPredicate::TPtr& ev, const TActorContext& ctx)
{
    PushBackDistrTx(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxCommit::TPtr& ev, const TActorContext& ctx)
{
    EndTransaction(*ev->Get(), ctx);

    TxInProgress = false;

    ContinueProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvTxRollback::TPtr& ev, const TActorContext& ctx)
{
    EndTransaction(*ev->Get(), ctx);

    TxInProgress = false;

    ContinueProcessTxsAndUserActs(ctx);
}

void TPartition::Handle(TEvPQ::TEvGetMaxSeqNoRequest::TPtr& ev, const TActorContext& ctx) {
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(ev->Get()->Cookie);
    NKikimrClient::TResponse& resp = response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto& result = *resp.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult();
    for (const auto& sourceId : ev->Get()->SourceIds) {
        auto& protoInfo = *result.AddSourceIdInfo();
        protoInfo.SetSourceId(sourceId);

        auto it = SourceIdStorage.GetInMemorySourceIds().find(sourceId);
        if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
            continue;
        }

        const auto& memInfo = it->second;
        Y_VERIFY(memInfo.Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, memInfo.Offset);
        Y_VERIFY(memInfo.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, memInfo.SeqNo);

        protoInfo.SetSeqNo(memInfo.SeqNo);
        protoInfo.SetOffset(memInfo.Offset);
        protoInfo.SetWriteTimestampMS(memInfo.WriteTimestamp.MilliSeconds());
        protoInfo.SetExplicit(memInfo.Explicit);
        protoInfo.SetState(TSourceIdInfo::ConvertState(memInfo.State));
    }

    ctx.Send(Tablet, response.Release());
}

void TPartition::Handle(TEvPQ::TEvBlobResponse::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Get()->GetCookie();
    Y_VERIFY(ReadInfo.contains(cookie));

    auto it = ReadInfo.find(cookie);
    Y_VERIFY(it != ReadInfo.end());
    TReadInfo info = std::move(it->second);
    ReadInfo.erase(it);

    //make readinfo class
    TReadAnswer answer(info.FormAnswer(
        ctx, *ev->Get(), EndOffset, Partition, &UsersInfoStorage->GetOrCreate(info.User, ctx),
        info.Destination, GetSizeLag(info.Offset), Tablet, Config.GetMeteringMode()
    ));

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
        const auto& resp = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get())->Response;
        TabletCounters.Cumulative()[COUNTER_PQ_READ_OK].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_OK].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        TabletCounters.Cumulative()[COUNTER_PQ_READ_BYTES].Increment(resp.ByteSize());
    }
    ctx.Send(info.Destination != 0 ? Tablet : ctx.SelfID, answer.Event.Release());
    OnReadRequestFinished(std::move(info), answer.Size);
}

void TPartition::Handle(TEvPQ::TEvError::TPtr& ev, const TActorContext& ctx) {
    ReadingTimestamp = false;
    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);
    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        ProcessTimestampRead(ctx);
        return;
    }
    Y_VERIFY(userInfo->ReadScheduled);
    Y_VERIFY(ReadingForUser != "");

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
            Y_VERIFY(p < HeadKeys.size());
            Y_VERIFY(DataKeysHead[j].GetKey(k) == HeadKeys[p].Key);
            Y_VERIFY(DataKeysHead[j].GetSize(k) == HeadKeys[p].Size);
            s += DataKeysHead[j].GetSize(k);
            Y_VERIFY(j + 1 == TotalLevels || DataKeysHead[j].GetSize(k) >= CompactLevelBorder[j + 1]);
            ++p;
        }
        Y_VERIFY(s < DataKeysHead[j].Border());
    }
    Y_VERIFY(DataKeysBody.empty() ||
             Head.Offset >= DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
    Y_VERIFY(p == HeadKeys.size());
    if (!HeadKeys.empty()) {
        Y_VERIFY(HeadKeys.size() <= TotalMaxCount);
        Y_VERIFY(HeadKeys.front().Key.GetOffset() == Head.Offset);
        Y_VERIFY(HeadKeys.front().Key.GetPartNo() == Head.PartNo);
        for (p = 1; p < HeadKeys.size(); ++p) {
            Y_VERIFY(HeadKeys[p].Key.GetOffset() == HeadKeys[p-1].Key.GetOffset() + HeadKeys[p-1].Key.GetCount());
            Y_VERIFY(HeadKeys[p].Key.ToString() > HeadKeys[p-1].Key.ToString());
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
        Y_VERIFY(it != DataKeysBody.end());
        sizeLag = it->Size + DataKeysBody.back().CumulativeSize - it->CumulativeSize;
        Y_VERIFY(BodySize == DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size - DataKeysBody.front().CumulativeSize);
    }
    for (auto& b : HeadKeys) {
        if ((i64)b.Key.GetOffset() >= offset)
            sizeLag += b.Size;
    }
    return sizeLag;
}


bool TPartition::UpdateCounters(const TActorContext& ctx) {
    if (!PartitionCountersLabeled) {
        return false;
    }
    // per client counters
    const auto now = ctx.Now();
    for (auto& userInfoPair : UsersInfoStorage->GetAll()) {
        auto& userInfo = userInfoPair.second;
        if (!userInfo.LabeledCounters)
            continue;
        if (!userInfo.HasReadRule && !userInfo.Important)
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

        ui64 speed = userInfo.ReadQuota.GetTotalSpeed();
        if (speed != userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_BYTES].Get()) {
            haveChanges = true;
            userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_BYTES].Set(speed);
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
        Y_VERIFY(id == METRIC_MAX_READ_SPEED_4 + 1);
        if (userInfo.ReadQuota.GetTotalSpeed()) {
            ui64 quotaUsage = ui64(userInfo.AvgReadBytes[1].GetValue()) * 1000000 / userInfo.ReadQuota.GetTotalSpeed() / 60;
            if (quotaUsage != userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_USAGE].Get()) {
                haveChanges = true;
                userInfo.LabeledCounters->GetCounters()[METRIC_READ_QUOTA_USAGE].Set(quotaUsage);
            }
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

    ui64 speed = WriteQuota->GetTotalSpeed();
    if (speed != PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_BYTES].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_BYTES].Set(speed);
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
    Y_VERIFY(id == METRIC_MAX_WRITE_SPEED_4 + 1);


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
    Y_VERIFY(id == METRIC_MAX_QUOTA_SPEED_4 + 1);

    if (WriteQuota->GetTotalSpeed()) {
        ui64 quotaUsage = ui64(AvgQuotaBytes[1].GetValue()) * 1000000 / WriteQuota->GetTotalSpeed() / 60;
        if (quotaUsage != PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_USAGE].Get()) {
            haveChanges = true;
            PartitionCountersLabeled->GetCounters()[METRIC_WRITE_QUOTA_USAGE].Set(quotaUsage);
        }
    }

    ui64 partSize = Size();
    if (partSize != PartitionCountersLabeled->GetCounters()[METRIC_TOTAL_PART_SIZE].Get()) {
        haveChanges = true;
        PartitionCountersLabeled->GetCounters()[METRIC_MAX_PART_SIZE].Set(partSize);
        PartitionCountersLabeled->GetCounters()[METRIC_TOTAL_PART_SIZE].Set(partSize);
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
    return haveChanges;
}

void TPartition::ReportCounters(const TActorContext& ctx) {
    if (UpdateCounters(ctx)) {
        ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCounters(Partition, *PartitionCountersLabeled));
    }
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
    if (response.GetStatusResultSize())
        DiskIsFull = !diskIsOk;

    if (response.HasCookie()) {
        HandleSetOffsetResponse(response.GetCookie(), ctx);
    } else {
        if (ctx.Now() - WriteStartTime > TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetMinWriteLatencyMs())) {
            HandleWriteResponse(ctx);
        } else {
            ctx.Schedule(TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetMinWriteLatencyMs()) - (ctx.Now() - WriteStartTime), new TEvPQ::TEvHandleWriteResponse());
        }
    }
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvTxCalcPredicate> event)
{
    DistrTxs.emplace_back(std::move(event));
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event)
{
    DistrTxs.emplace_back(std::move(event), true);
}

void TPartition::PushFrontDistrTx(TSimpleSharedPtr<TEvPQ::TEvChangePartitionConfig> event)
{
    DistrTxs.emplace_front(std::move(event), false);
}

void TPartition::PushBackDistrTx(TSimpleSharedPtr<TEvPQ::TEvProposePartitionConfig> event)
{
    DistrTxs.emplace_back(std::move(event));
}

void TPartition::AddImmediateTx(TSimpleSharedPtr<TEvPersQueue::TEvProposeTransaction> tx)
{
    ImmediateTxs.push_back(std::move(tx));
}

void TPartition::AddUserAct(TSimpleSharedPtr<TEvPQ::TEvSetClientInfo> act)
{
    UserActs.push_back(std::move(act));
    ++UserActCount[UserActs.back()->ClientId];
}

void TPartition::RemoveImmediateTx()
{
    Y_VERIFY(!ImmediateTxs.empty());

    ImmediateTxs.pop_front();
}

void TPartition::RemoveUserAct()
{
    Y_VERIFY(!UserActs.empty());

    auto p = UserActCount.find(UserActs.front()->ClientId);
    Y_VERIFY(p != UserActCount.end());

    Y_VERIFY(p->second > 0);
    if (!--p->second) {
        UserActCount.erase(p);
    }

    UserActs.pop_front();
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
    if (UsersInfoWriteInProgress || (ImmediateTxs.empty() && UserActs.empty() && DistrTxs.empty()) || TxInProgress) {
        return;
    }

    Y_VERIFY(PendingUsersInfo.empty());
    Y_VERIFY(Replies.empty());
    Y_VERIFY(AffectedUsers.empty());

    ContinueProcessTxsAndUserActs(ctx);
}

void TPartition::ContinueProcessTxsAndUserActs(const TActorContext& ctx)
{
    if (!DistrTxs.empty()) {
        ProcessDistrTxs(ctx);

        if (TxInProgress) {
            return;
        }
    }

    ProcessUserActs(ctx);
    ProcessImmediateTxs(ctx);

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);
    request->Record.SetCookie(SET_OFFSET_COOKIE);

    if (TxIdHasChanged) {
        AddCmdWriteTxMeta(request->Record,
                          *PlanStep, *TxId);
    }
    AddCmdWriteUserInfos(request->Record);
    AddCmdWriteConfig(request->Record);

    ctx.Send(Tablet, request.Release());
    UsersInfoWriteInProgress = true;
}

void TPartition::RemoveDistrTx()
{
    Y_VERIFY(!DistrTxs.empty());

    DistrTxs.pop_front();
}

void TPartition::ProcessDistrTxs(const TActorContext& ctx)
{
    Y_VERIFY(!TxInProgress);

    while (!TxInProgress && !DistrTxs.empty()) {
        ProcessDistrTx(ctx);
    }
}

bool TPartition::BeginTransaction(const TEvPQ::TEvTxCalcPredicate& tx,
                                  const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    bool predicate = true;

    for (auto& operation : tx.Operations) {
        const TString& consumer = operation.GetConsumer();

        if (AffectedUsers.contains(consumer) && !GetPendingUserIfExists(consumer)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "' has been removed");
            predicate = false;
            break;
        }

        if (!UsersInfoStorage->GetIfExists(consumer)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Unknown consumer '" << consumer << "'");
            predicate = false;
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
            predicate = false;
        } else if (userInfo.Offset != (i64)operation.GetBegin()) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "'" <<
                        " Bad request (gap) " <<
                        " Offset " << userInfo.Offset <<
                        " Begin " << operation.GetBegin());
            predicate = false;
        } else if (operation.GetEnd() > EndOffset) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE,
                        "Partition " << Partition <<
                        " Consumer '" << consumer << "'" <<
                        " Bad request (behind the last offset) " <<
                        " EndOffset " << EndOffset <<
                        " End " << operation.GetEnd());
            predicate = false;
        }

        if (!predicate) {
            if (!isAffectedConsumer) {
                AffectedUsers.erase(consumer);
            }
            break;
        }
    }

    return predicate;
}

bool TPartition::BeginTransaction(const TEvPQ::TEvProposePartitionConfig& event)
{
    ChangeConfig =
        MakeSimpleShared<TEvPQ::TEvChangePartitionConfig>(TopicConverter,
                                                          event.Config);
    SendChangeConfigReply = false;
    return true;
}

void TPartition::EndTransaction(const TEvPQ::TEvTxCommit& event,
                                const TActorContext& ctx)
{
    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(event) < GetStepAndTxId(*PlanStep, *TxId)) {
            ctx.Send(Tablet, MakeCommitDone(event.Step, event.TxId).Release());
            return;
        }
    }

    Y_VERIFY(TxInProgress);

    Y_VERIFY(!DistrTxs.empty());
    TTransaction& t = DistrTxs.front();

    if (t.Tx) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.Tx));
        Y_VERIFY(t.Predicate.Defined() && *t.Predicate);

        for (auto& operation : t.Tx->Operations) {
            TUserInfoBase& userInfo = GetOrCreatePendingUser(operation.GetConsumer());

            Y_VERIFY(userInfo.Offset == (i64)operation.GetBegin());

            userInfo.Offset = operation.GetEnd();
            userInfo.Session = "";
        }

        ChangePlanStepAndTxId(t.Tx->Step, t.Tx->TxId);

        ScheduleReplyCommitDone(t.Tx->Step, t.Tx->TxId);
    } else if (t.ProposeConfig) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.ProposeConfig));
        Y_VERIFY(t.Predicate.Defined() && *t.Predicate);

        BeginChangePartitionConfig(t.ProposeConfig->Config, ctx);

        ChangePlanStepAndTxId(t.ProposeConfig->Step, t.ProposeConfig->TxId);

        ScheduleReplyCommitDone(t.ProposeConfig->Step, t.ProposeConfig->TxId);
    } else {
        Y_VERIFY(t.ChangeConfig);
    }

    RemoveDistrTx();
}

void TPartition::EndTransaction(const TEvPQ::TEvTxRollback& event,
                                const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    if (PlanStep.Defined() && TxId.Defined()) {
        if (GetStepAndTxId(event) <= GetStepAndTxId(*PlanStep, *TxId)) {
            return;
        }
    }

    Y_VERIFY(TxInProgress);

    Y_VERIFY(!DistrTxs.empty());
    TTransaction& t = DistrTxs.front();

    if (t.Tx) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.Tx));
        Y_VERIFY(t.Predicate.Defined());

        ChangePlanStepAndTxId(t.Tx->Step, t.Tx->TxId);
    } else if (t.ProposeConfig) {
        Y_VERIFY(GetStepAndTxId(event) == GetStepAndTxId(*t.ProposeConfig));
        Y_VERIFY(t.Predicate.Defined());

        ChangePlanStepAndTxId(t.ProposeConfig->Step, t.ProposeConfig->TxId);
    } else {
        Y_VERIFY(t.ChangeConfig);
    }


    RemoveDistrTx();
}

void TPartition::BeginChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                            const TActorContext& ctx)
{
    TSet<TString> hasReadRule;

    for (auto& [consumer, info] : UsersInfoStorage->GetAll()) {
        hasReadRule.insert(consumer);
    }

    TSet<TString> important;
    for (const auto& importantUser : config.GetPartitionConfig().GetImportantClientId()) {
        important.insert(importantUser);
    }

    for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
        const auto& consumer = config.GetReadRules(i);
        auto& userInfo = GetOrCreatePendingUser(consumer, 0);

        TInstant ts = i < config.ReadFromTimestampsMsSize() ? TInstant::MilliSeconds(config.GetReadFromTimestampsMs(i)) : TInstant::Zero();
        if (!ts) {
            ts += TDuration::MilliSeconds(1);
        }
        userInfo.ReadFromTimestamp = ts;
        userInfo.Important = important.contains(consumer);

        ui64 rrGen = i < config.ReadRuleGenerationsSize() ? config.GetReadRuleGenerations(i) : 0;
        if (userInfo.ReadRuleGeneration != rrGen) {
            TEvPQ::TEvSetClientInfo act(0, consumer, 0, "", 0, 0,
                                        TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, rrGen);

            ProcessUserAct(act, ctx);
        }
        hasReadRule.erase(consumer);
    }

    for (auto& consumer : hasReadRule) {
        GetOrCreatePendingUser(consumer);
        TEvPQ::TEvSetClientInfo act(0, consumer,
                                    0, "", 0, 0, TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0);

        ProcessUserAct(act, ctx);
    }
}

void TPartition::EndChangePartitionConfig(const NKikimrPQ::TPQTabletConfig& config,
                                          NPersQueue::TTopicConverterPtr topicConverter,
                                          const TActorContext& ctx)
{
    Config = config;
    TopicConverter = topicConverter;

    Y_VERIFY(Config.GetPartitionConfig().GetTotalPartitions() > 0);

    UsersInfoStorage->UpdateConfig(Config);

    WriteQuota->UpdateConfig(Config.GetPartitionConfig().GetBurstSize(), Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond());
    if (AppData(ctx)->PQConfig.GetQuotingConfig().GetPartitionReadQuotaIsTwiceWriteQuota()) {
        for (auto& userInfo : UsersInfoStorage->GetAll()) {
            userInfo.second.ReadQuota.UpdateConfig(Config.GetPartitionConfig().GetBurstSize() * 2, Config.GetPartitionConfig().GetWriteSpeedInBytesPerSecond() * 2);
        }
    }

    for (const auto& readQuota : Config.GetPartitionConfig().GetReadQuota()) {
        auto& userInfo = UsersInfoStorage->GetOrCreate(readQuota.GetClientId(), ctx);
        userInfo.ReadQuota.UpdateConfig(readQuota.GetBurstSize(), readQuota.GetSpeedInBytesPerSecond());
    }

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
}

TString TPartition::GetKeyConfig() const
{
    return Sprintf("_config_%u", Partition);
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

void TPartition::ProcessDistrTx(const TActorContext& ctx)
{
    Y_VERIFY(!TxInProgress);

    Y_VERIFY(!DistrTxs.empty());
    TTransaction& t = DistrTxs.front();

    if (t.Tx) {
        t.Predicate = BeginTransaction(*t.Tx, ctx);

        ctx.Send(Tablet,
                 MakeHolder<TEvPQ::TEvTxCalcPredicateResult>(t.Tx->Step,
                                                             t.Tx->TxId,
                                                             Partition,
                                                             *t.Predicate).Release());

        TxInProgress = true;
    } else if (t.ProposeConfig) {
        t.Predicate = BeginTransaction(*t.ProposeConfig);

        ctx.Send(Tablet,
                 MakeHolder<TEvPQ::TEvProposePartitionConfigResult>(t.ProposeConfig->Step,
                                                                    t.ProposeConfig->TxId,
                                                                    Partition).Release());

        TxInProgress = true;
    } else {
        Y_VERIFY(!ChangeConfig);

        ChangeConfig = t.ChangeConfig;
        SendChangeConfigReply = t.SendReply;
        BeginChangePartitionConfig(ChangeConfig->Config, ctx);

        RemoveDistrTx();
    }
}

void TPartition::ProcessImmediateTxs(const TActorContext& ctx)
{
    Y_VERIFY(!UsersInfoWriteInProgress);

    while (!ImmediateTxs.empty() && (AffectedUsers.size() < MAX_USERS)) {
        ProcessImmediateTx(ImmediateTxs.front()->Record, ctx);

        RemoveImmediateTx();
    }
}

void TPartition::ProcessImmediateTx(const NKikimrPQ::TEvProposeTransaction& tx,
                                    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Y_VERIFY(tx.GetTxBodyCase() == NKikimrPQ::TEvProposeTransaction::kData);
    Y_VERIFY(tx.HasData());

    for (auto& operation : tx.GetData().GetOperations()) {
        Y_VERIFY(operation.HasBegin() && operation.HasEnd() && operation.HasConsumer());

        Y_VERIFY(operation.GetBegin() <= (ui64)Max<i64>(), "Unexpected begin offset: %" PRIu64, operation.GetBegin());
        Y_VERIFY(operation.GetEnd() <= (ui64)Max<i64>(), "Unexpected end offset: %" PRIu64, operation.GetEnd());

        const TString& user = operation.GetConsumer();

        if (!PendingUsersInfo.contains(user) && AffectedUsers.contains(user)) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED);
            return;
        }

        TUserInfoBase& userInfo = GetOrCreatePendingUser(user);

        if (operation.GetBegin() > operation.GetEnd()) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST);
            return;
        }

        if (userInfo.Offset != (i64)operation.GetBegin()) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::ABORTED);
            return;
        }

        if (operation.GetEnd() > EndOffset) {
            ScheduleReplyPropose(tx,
                                 NKikimrPQ::TEvProposeTransactionResult::BAD_REQUEST);
            return;
        }

        userInfo.Offset = operation.GetEnd();
        userInfo.Session = "";
    }

    ScheduleReplyPropose(tx,
                         NKikimrPQ::TEvProposeTransactionResult::COMPLETE);
}

void TPartition::ProcessUserActs(const TActorContext& ctx)
{
    Y_VERIFY(!UsersInfoWriteInProgress);

    while (!UserActs.empty() && (AffectedUsers.size() < MAX_USERS)) {
        ProcessUserAct(*UserActs.front(), ctx);

        RemoveUserAct();
    }
}

void TPartition::ProcessUserAct(TEvPQ::TEvSetClientInfo& act,
                                const TActorContext& ctx)
{
    Y_VERIFY(!UsersInfoWriteInProgress);

    const TString& user = act.ClientId;
    const bool strictCommitOffset = (act.Type == TEvPQ::TEvSetClientInfo::ESCI_OFFSET && act.Strict);

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

    TUserInfoBase& userInfo = GetOrCreatePendingUser(user);

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE) {
        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition
                    << " user " << user << " drop request"
        );

        EmulatePostProcessUserAct(act, userInfo, ctx);

        return;
    }

    if (act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && act.SessionId == userInfo.Session) { //this is retry of current request, answer ok
        auto *ui = UsersInfoStorage->GetIfExists(userInfo.User);
        auto ts = ui ? GetTime(*ui, userInfo.Offset) : std::make_pair<TInstant, TInstant>(TInstant::Zero(), TInstant::Zero());

        ScheduleReplyGetClientOffsetOk(act.Cookie,
                                       userInfo.Offset,
                                       ts.first, ts.second);

        return;
    }

    if (act.Type != TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && act.Type != TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE
            && !act.SessionId.empty() && userInfo.Session != act.SessionId //request to wrong session
            && (act.Type != TEvPQ::TEvSetClientInfo::ESCI_DROP_SESSION || !userInfo.Session.empty()) //but allow DropSession request when session is already dropped - for idempotence
            || (act.Type == TEvPQ::TEvSetClientInfo::ESCI_CREATE_SESSION && !userInfo.Session.empty()
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

    Y_VERIFY(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);

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

    EmulatePostProcessUserAct(act, userInfo, ctx);
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
        } else if (dropSession || strictCommitOffset) {
            userInfo.Session = "";
            userInfo.Generation = 0;
            userInfo.Step = 0;
        }

        Y_VERIFY(offset <= (ui64)Max<i64>(), "Unexpected Offset: %" PRIu64, offset);
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
                                      NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode)
{
    Replies.emplace_back(ActorIdFromProto(event.GetSourceActor()),
                         MakeReplyPropose(event,
                                          statusCode).Release());
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

void TPartition::AddCmdWriteTxMeta(NKikimrClient::TKeyValueRequest& request,
                                   ui64 step, ui64 txId)
{
    TKeyPrefix ikey(TKeyPrefix::TypeTxMeta, Partition);

    NKikimrPQ::TPartitionTxMeta meta;
    meta.SetPlanStep(step);
    meta.SetTxId(txId);

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
                        userInfo->Offset, userInfo->Generation, userInfo->Step, userInfo->Session,
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
    Y_VERIFY(ChangeConfig->Config.SerializeToString(&data));

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
        auto [p, _] = PendingUsersInfo.emplace(user, UsersInfoStorage->CreateUserInfo(user,
                                                                                      readRuleGeneration));

        if (ui) {
            p->second.Session = ui->Session;
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
    NKikimrClient::TResponse& resp = response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    return response;
}

THolder<TEvPQ::TEvProxyResponse> TPartition::MakeReplyGetClientOffsetOk(const ui64 dst,
                                                                        const i64 offset,
                                                                        const TInstant writeTimestamp, const TInstant createTimestamp)
{
    auto response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;

    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);

    auto user = resp.MutablePartitionResponse()->MutableCmdGetClientOffsetResult();
    if (offset > -1)
        user->SetOffset(offset);
    if (writeTimestamp)
        user->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
    if (createTimestamp) {
        Y_VERIFY(writeTimestamp);
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
                                                                                NKikimrPQ::TEvProposeTransactionResult::EStatus statusCode)
{
    auto response = MakeHolder<TEvPersQueue::TEvProposeTransactionResult>();

    response->Record.SetOrigin(TabletID);
    response->Record.SetStatus(statusCode);
    response->Record.SetTxId(event.GetTxId());

    return response;
}

THolder<TEvPQ::TEvTxCommitDone> TPartition::MakeCommitDone(ui64 step, ui64 txId)
{
    return MakeHolder<TEvPQ::TEvTxCommitDone>(step, txId, Partition);
}

void TPartition::ScheduleUpdateAvailableSize(const TActorContext& ctx) {
    ctx.Schedule(UPDATE_AVAIL_SIZE_INTERVAL, new TEvPQ::TEvUpdateAvailableSize());
}

void TPartition::BecomeIdle(const TActorContext&) {
    Become(&TThis::StateIdle);
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
    Channel = (Channel + 1) % Config.GetPartitionConfig().GetNumChannels();

    return res;
}

void TPartition::Handle(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx) {
    const ui64 cookie = ev->Cookie;
    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Got quota." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << ": " << ev->Get()->Result << "." <<
            " Cookie: " << cookie
    );
    // Check
    if (Y_UNLIKELY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Success)) {
        // We set deadline == inf in quota request.
        Y_VERIFY(ev->Get()->Result != TEvQuota::TEvClearance::EResult::Deadline);
        LOG_ERROR_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Got quota error." <<
                " Topic: \"" << TopicName() << "\"." <<
                " Partition " << Partition << ": " << ev->Get()->Result
        );
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        return;
    }

    // Search for proper request
    Y_VERIFY(TopicQuotaRequestCookie == cookie);
    TopicQuotaRequestCookie = 0;
    Y_ASSERT(!WaitingForPreviousBlobQuota());

    // Metrics
    TopicQuotaWaitTimeForCurrentBlob = StartTopicQuotaWaitTimeForCurrentBlob ? TActivationContext::Now() - StartTopicQuotaWaitTimeForCurrentBlob : TDuration::Zero();
    if (TopicWriteQuotaWaitCounter) {
        TopicWriteQuotaWaitCounter->IncFor(TopicQuotaWaitTimeForCurrentBlob.MilliSeconds());
    }
    // Reset quota wait time
    StartTopicQuotaWaitTimeForCurrentBlob = TInstant::Zero();

    if (CurrentStateFunc() == &TThis::StateIdle)
        HandleWrites(ctx);
}

size_t TPartition::GetQuotaRequestSize(const TEvKeyValue::TEvRequest& request) {
    if (Config.GetMeteringMode() == NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS) {
        return 0;
    }
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
        Register(new TMirrorer(Tablet, SelfId(), TopicConverter, Partition, IsLocalDC,  EndOffset, Config.GetPartitionConfig().GetMirrorFrom(), TabletCounters)),
        TabletCounters
    );
}

bool IsQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig,
                      bool isLocalDC)
{
    const auto& quotingConfig = pqConfig.GetQuotingConfig();
    return isLocalDC && !pqConfig.GetTopicsAreFirstClassCitizen() && quotingConfig.GetEnableQuoting();
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
            if (CurrentStateFunc() == &TThis::StateIdle) {
                HandleWrites(ctx);
            }
        }
    }
}


} // namespace NKikimr::NPQ
