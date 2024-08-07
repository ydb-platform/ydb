#include "event_helpers.h"
#include "mirrorer.h"
#include "partition_log.h"
#include "partition_util.h"
#include "partition.h"
#include "read.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/feature_flags.h>
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

namespace NKikimr::NPQ {

static const ui32 BATCH_UNPACK_SIZE_BORDER = 500_KB;
static const ui32 MAX_INLINE_SIZE = 1000;

static constexpr NPersQueue::NErrorCode::EErrorCode InactivePartitionErrorCode = NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_INACTIVE;

void TPartition::ReplyOwnerOk(const TActorContext& ctx, const ui64 dst, const TString& cookie, ui64 seqNo) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ReplyOwnerOk. Partition: " << Partition);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = *response->Response;
    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto* r = resp.MutablePartitionResponse()->MutableCmdGetOwnershipResult();
    r->SetOwnerCookie(cookie);
    r->SetStatus(PartitionConfig ? PartitionConfig->GetStatus() : NKikimrPQ::ETopicPartitionStatus::Active);
    r->SetSeqNo(seqNo);
    if (IsSupportive()) {
        r->SetSupportivePartition(Partition.InternalPartitionId);
    }

    ctx.Send(Tablet, response.Release());
}

void TPartition::ReplyWrite(
    const TActorContext& ctx, const ui64 dst, const TString& sourceId, const ui64 seqNo, const ui16 partNo, const ui16 totalParts,
    const ui64 offset, const TInstant writeTimestamp, bool already, const ui64 maxSeqNo,
    const TDuration partitionQuotedTime, const TDuration topicQuotedTime, const TDuration queueTime, const TDuration writeTime) {

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ReplyWrite. Partition: " << Partition);

    Y_ABORT_UNLESS(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);
    Y_ABORT_UNLESS(seqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, seqNo);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = *response->Response;
    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto write = resp.MutablePartitionResponse()->AddCmdWriteResult();
    write->SetSourceId(sourceId);
    write->SetSeqNo(seqNo);
    write->SetWriteTimestampMS(writeTimestamp.MilliSeconds());
    if (totalParts > 1)
        write->SetPartNo(partNo);
    write->SetAlreadyWritten(already);
    if (already)
        write->SetMaxSeqNo(maxSeqNo);
    write->SetOffset(offset);

    write->SetPartitionQuotedTimeMs(partitionQuotedTime.MilliSeconds());
    write->SetTopicQuotedTimeMs(topicQuotedTime.MilliSeconds());
    write->SetTotalTimeInPartitionQueueMs(queueTime.MilliSeconds());
    write->SetWriteTimeMs(writeTime.MilliSeconds());

    ctx.Send(Tablet, response.Release());
}

void TPartition::UpdateAvailableSize(const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);
    ScheduleUpdateAvailableSize(ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvUpdateAvailableSize::TPtr&, const TActorContext& ctx)
{
    UpdateAvailableSize(ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvUpdateAvailableSize::TPtr&, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleOnWrite TEvUpdateAvailableSize.");

    UpdateAvailableSize(ctx);
}

void TPartition::ProcessChangeOwnerRequest(TAutoPtr<TEvPQ::TEvChangeOwner> ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::ProcessChangeOwnerRequest.");

    auto &owner = ev->Owner;
    auto it = Owners.find(owner);
    if (it == Owners.end()) {
        if (ev->RegisterIfNotExists) {
            Owners[owner];
            it = Owners.find(owner);
        } else {
            return ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED, "SourceId isn't registered");
        }
    }

    if (it->second.NeedResetOwner || ev->Force) { //change owner

        Y_ABORT_UNLESS(ReservedSize >= it->second.ReservedSize);
        ReservedSize -= it->second.ReservedSize;

        it->second.GenerateCookie(owner, ev->PipeClient, ev->Sender, TopicName(), Partition, ctx);//will change OwnerCookie
        //cookie is generated. but answer will be sent when all inflight writes will be done - they in the same queue 'Requests'
        EmplacePendingRequest(TOwnershipMsg{ev->Cookie, it->second.OwnerCookie}, ctx);
        TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
        UpdateWriteBufferIsFullState(ctx.Now());
        ProcessReserveRequests(ctx);
    } else {
        it->second.WaitToChangeOwner.push_back(THolder<TEvPQ::TEvChangeOwner>(ev.Release()));
    }
}


THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator TPartition::DropOwner(THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator& it, const TActorContext& ctx) {
    PQ_LOG_D("TPartition::DropOwner.");

    Y_ABORT_UNLESS(ReservedSize >= it->second.ReservedSize);
    ReservedSize -= it->second.ReservedSize;
    UpdateWriteBufferIsFullState(ctx.Now());
    TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
    for (auto& ev : it->second.WaitToChangeOwner) { //this request maybe could be done right now
        WaitToChangeOwner.push_back(THolder<TEvPQ::TEvChangeOwner>(ev.Release()));
    }
    auto jt = it;
    ++jt;
    Owners.erase(it);
    return jt;
}

void TPartition::Handle(TEvPQ::TEvChangeOwner::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleOnWrite TEvChangeOwner.");

    bool res = OwnerPipes.insert(ev->Get()->PipeClient).second;
    Y_ABORT_UNLESS(res);
    WaitToChangeOwner.push_back(ev->Release());
    ProcessChangeOwnerRequests(ctx);
}

void TPartition::ProcessReserveRequests(const TActorContext& ctx) {
    PQ_LOG_T("TPartition::ProcessReserveRequests.");

    const ui64 maxWriteInflightSize = Config.GetPartitionConfig().GetMaxWriteInflightSize();

    while (!ReserveRequests.empty()) {
        const TString& ownerCookie = ReserveRequests.front()->OwnerCookie;
        const TStringBuf owner = TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie);
        const ui64& size = ReserveRequests.front()->Size;
        const ui64& cookie = ReserveRequests.front()->Cookie;
        const bool& lastRequest = ReserveRequests.front()->LastRequest;

        if (!IsActive()) {
            ReplyError(ctx, cookie, NPersQueue::NErrorCode::OVERLOAD, "ReserveRequest to inactive partition");
            ReserveRequests.pop_front();
            continue;
        }

        auto it = Owners.find(owner);
        if (ClosedInternalPartition) {
            ReplyError(ctx, cookie, NPersQueue::NErrorCode::BAD_REQUEST, "ReserveRequest to closed supportive partition");
            ReserveRequests.pop_front();
            continue;
        }
        if (it == Owners.end() || it->second.OwnerCookie != ownerCookie) {
            ReplyError(ctx, cookie, NPersQueue::NErrorCode::BAD_REQUEST, "ReserveRequest from dead ownership session");
            ReserveRequests.pop_front();
            continue;
        }

        const ui64 currentSize = ReservedSize + WriteInflightSize + WriteCycleSize;
        if (currentSize != 0 && currentSize + size > maxWriteInflightSize) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reserve processing: maxWriteInflightSize riched. Partition: " << Partition);
            break;
        }

        if (WaitingForSubDomainQuota(ctx, currentSize)) {
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reserve processing: SubDomainOutOfSpace. Partition: " << Partition);
            break;
        }

        it->second.AddReserveRequest(size, lastRequest);
        ReservedSize += size;

        ReplyOk(ctx, cookie);

        ReserveRequests.pop_front();
    }
    UpdateWriteBufferIsFullState(ctx.Now());
    TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
}

void TPartition::UpdateWriteBufferIsFullState(const TInstant& now) {
    WriteBufferIsFullCounter.UpdateWorkingTime(now);
    WriteBufferIsFullCounter.UpdateState(ReservedSize + WriteInflightSize + WriteCycleSize >= Config.GetPartitionConfig().GetBorderWriteInflightSize());
}

void TPartition::Handle(TEvPQ::TEvReserveBytes::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleOnWrite TEvReserveBytes.");

    const TString& ownerCookie = ev->Get()->OwnerCookie;
    TStringBuf owner = TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie);
    const ui64& messageNo = ev->Get()->MessageNo;

    auto it = Owners.find(owner);
    if (it == Owners.end() || it->second.OwnerCookie != ownerCookie) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST, "ReserveRequest from dead ownership session");
        return;
    }

    if (messageNo != it->second.NextMessageNo) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "reorder in reserve requests, waiting " << it->second.NextMessageNo << ", but got " << messageNo);
        DropOwner(it, ctx);
        ProcessChangeOwnerRequests(ctx);
        return;
    }

    ++it->second.NextMessageNo;
    ReserveRequests.push_back(ev->Release());
    ProcessReserveRequests(ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::AnswerCurrentWrites(const TActorContext& ctx) {
    PQ_LOG_T("TPartition::AnswerCurrentWrites. Responses.size()=" << Responses.size());
    const auto now = ctx.Now();

    ui64 offset = EndOffset;
    while (!Responses.empty()) {
        const auto& response = Responses.front();

        const TDuration queueTime = response.QueueTime;
        const TDuration writeTime = now - response.WriteTimeBaseline;

        if (response.IsWrite()) {
            const auto& writeResponse = response.GetWrite();
            const TString& s = writeResponse.Msg.SourceId;
            const ui64& seqNo = writeResponse.Msg.SeqNo;
            const ui16& partNo = writeResponse.Msg.PartNo;
            const ui16& totalParts = writeResponse.Msg.TotalParts;
            const TMaybe<ui64>& wrOffset = writeResponse.Offset;

            bool already = false;

            SourceIdCounter.Use(s, now);
            auto it = SourceIdStorage.GetInMemorySourceIds().find(s);

            ui64 maxSeqNo = 0;
            ui64 maxOffset = 0;

            if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
                maxSeqNo = std::max(it->second.SeqNo, writeResponse.InitialSeqNo.value_or(0));
                maxOffset = it->second.Offset;
                if (maxSeqNo >= seqNo && !writeResponse.Msg.DisableDeduplication) {
                    already = true;
                }
            } else if (writeResponse.InitialSeqNo) {
                maxSeqNo = writeResponse.InitialSeqNo.value();
                if (maxSeqNo >= seqNo && !writeResponse.Msg.DisableDeduplication) {
                    already = true;
                }
            }

            if (!already) {
                if (wrOffset) {
                    Y_ABORT_UNLESS(*wrOffset >= offset);
                    offset = *wrOffset;
                }
            }

            if (!already && partNo + 1 == totalParts) {
                if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
                    Y_ABORT_UNLESS(!writeResponse.Msg.HeartbeatVersion);
                    TabletCounters.Cumulative()[COUNTER_PQ_SID_CREATED].Increment(1);
                    SourceIdStorage.RegisterSourceId(s, seqNo, offset, CurrentTimestamp);
                } else if (const auto& hbVersion = writeResponse.Msg.HeartbeatVersion) {
                    SourceIdStorage.RegisterSourceId(s, it->second.Updated(
                        seqNo, offset, CurrentTimestamp, THeartbeat{*hbVersion, writeResponse.Msg.Data}
                    ));
                } else {
                    SourceIdStorage.RegisterSourceId(s, it->second.Updated(seqNo, offset, CurrentTimestamp));
                }

                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_OK].Increment(1);
            }
            ReplyWrite(
                ctx, writeResponse.Cookie, s, seqNo, partNo, totalParts,
                already ? maxOffset : offset, CurrentTimestamp, already, maxSeqNo,
                PartitionQuotaWaitTimeForCurrentBlob, TopicQuotaWaitTimeForCurrentBlob, queueTime, writeTime
            );
            LOG_DEBUG_S(
                ctx,
                NKikimrServices::PERSQUEUE,
                "Answering for message sourceid: '" << EscapeC(s) <<
                "', Topic: '" << TopicName() <<
                "', Partition: " << Partition <<
                ", SeqNo: " << seqNo << ", partNo: " << partNo <<
                ", Offset: " << offset << " is " << (already ? "already written" : "stored on disk")
            );

            if (PartitionWriteQuotaWaitCounter && !writeResponse.Internal) {
                PartitionWriteQuotaWaitCounter->IncFor(PartitionQuotaWaitTimeForCurrentBlob.MilliSeconds());
            }
            if (!already && partNo + 1 == totalParts && !writeResponse.Msg.HeartbeatVersion)
                ++offset;
        } else if (response.IsOwnership()) {
            const auto& r = response.GetOwnership();
            const TString& ownerCookie = r.OwnerCookie;
            auto it = Owners.find(TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie));
            if (it != Owners.end() && it->second.OwnerCookie == ownerCookie) {
                auto sit = SourceIdStorage.GetInMemorySourceIds().find(NSourceIdEncoding::EncodeSimple(it->first));
                auto seqNo = sit == SourceIdStorage.GetInMemorySourceIds().end() ? 0 : sit->second.SeqNo;
                ReplyOwnerOk(ctx, response.GetCookie(), ownerCookie, seqNo);
            } else {
                ReplyError(ctx, response.GetCookie(), NPersQueue::NErrorCode::WRONG_COOKIE, "new GetOwnership request is dropped already");
            }
        } else if (response.IsRegisterMessageGroup()) {
            const auto& body = response.GetRegisterMessageGroup().Body;

            TMaybe<TPartitionKeyRange> keyRange;
            if (body.KeyRange) {
                keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
            }

            Y_ABORT_UNLESS(body.AssignedOffset);
            SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange));
            ReplyOk(ctx, response.GetCookie());
        } else if (response.IsDeregisterMessageGroup()) {
            const auto& body = response.GetDeregisterMessageGroup().Body;

            SourceIdStorage.DeregisterSourceId(body.SourceId);
            ReplyOk(ctx, response.GetCookie());
        } else if (response.IsSplitMessageGroup()) {
            const auto& split = response.GetSplitMessageGroup();

            for (const auto& body : split.Deregistrations) {
                SourceIdStorage.DeregisterSourceId(body.SourceId);
            }

            for (const auto& body : split.Registrations) {
                TMaybe<TPartitionKeyRange> keyRange;
                if (body.KeyRange) {
                    keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
                }

                Y_ABORT_UNLESS(body.AssignedOffset);
                SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange), true);
            }

            ReplyOk(ctx, response.GetCookie());
        } else {
            Y_ABORT("Unexpected message");
        }
        Responses.pop_front();
    }
    TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
    PartitionQuotaWaitTimeForCurrentBlob = TDuration::Zero();
}

void TPartition::SyncMemoryStateWithKVState(const TActorContext& ctx) {
    PQ_LOG_T("TPartition::SyncMemoryStateWithKVState.");

    if (!CompactedKeys.empty())
        HeadKeys.clear();

    if (NewHeadKey.Size > 0) {
        while (!HeadKeys.empty() &&
            (HeadKeys.back().Key.GetOffset() > NewHeadKey.Key.GetOffset() || HeadKeys.back().Key.GetOffset() == NewHeadKey.Key.GetOffset()
                                                                       && HeadKeys.back().Key.GetPartNo() >= NewHeadKey.Key.GetPartNo())) {
                HeadKeys.pop_back();
        }
        HeadKeys.push_back(NewHeadKey);
        NewHeadKey = TDataKey{TKey{}, 0, TInstant::Zero(), 0};
    }

    if (CompactedKeys.empty() && NewHead.PackedSize == 0) { //Nothing writed at all
        return;
    }

    Y_ABORT_UNLESS(EndOffset == Head.GetNextOffset());

    if (!CompactedKeys.empty() || Head.PackedSize == 0) { //has compactedkeys or head is already empty
        Head.PackedSize = 0;
        Head.Offset = NewHead.Offset;
        Head.PartNo = NewHead.PartNo; //no partNo at this point
        Head.Batches.clear();
    }

    while (!CompactedKeys.empty()) {
        const auto& ck = CompactedKeys.front();
        BodySize += ck.second;
        Y_ABORT_UNLESS(!ck.first.IsHead());
        ui64 lastOffset = DataKeysBody.empty() ? 0 : (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
        Y_ABORT_UNLESS(lastOffset <= ck.first.GetOffset());
        if (DataKeysBody.empty()) {
            StartOffset = ck.first.GetOffset() + (ck.first.GetPartNo() > 0 ? 1 : 0);
        } else {
            if (lastOffset < ck.first.GetOffset()) {
                GapOffsets.push_back(std::make_pair(lastOffset, ck.first.GetOffset()));
                GapSize += ck.first.GetOffset() - lastOffset;
            }
        }
        DataKeysBody.push_back({ck.first, ck.second, ctx.Now(), DataKeysBody.empty() ? 0 : DataKeysBody.back().CumulativeSize + DataKeysBody.back().Size});

        CompactedKeys.pop_front();
    } // head cleared, all data moved to body

    //append Head with newHead
    while (!NewHead.Batches.empty()) {
        Head.Batches.push_back(NewHead.Batches.front());
        NewHead.Batches.pop_front();
    }
    Head.PackedSize += NewHead.PackedSize;

    if (Head.PackedSize > 0 && DataKeysBody.empty()) {
        StartOffset = Head.Offset + (Head.PartNo > 0 ? 1 : 0);
    }

    EndOffset = Head.GetNextOffset();
    NewHead.Clear();
    NewHead.Offset = EndOffset;

    CheckHeadConsistency();

    UpdateUserInfoEndOffset(ctx.Now());
}

void TPartition::OnHandleWriteResponse(const TActorContext& ctx)
{
    KVWriteInProgress = false;
    OnProcessTxsAndUserActsWriteComplete(ctx);
    HandleWriteResponse(ctx);
    ProcessTxsAndUserActs(ctx);

    if (DeletePartitionState == DELETION_IN_PROCESS) {
        DestroyActor(ctx);
    }
}

void TPartition::Handle(TEvPQ::TEvHandleWriteResponse::TPtr&, const TActorContext& ctx)
{
    PQ_LOG_T("TPartition::HandleOnWrite TEvHandleWriteResponse.");
    OnHandleWriteResponse(ctx);
}

void TPartition::UpdateAfterWriteCounters(bool writeComplete) {
    if (IsSupportive() == writeComplete) {
        // If supportive - update counters only prior to write, otherwise - only after writes;
        return;
    }
    if (BytesWrittenGrpc)
        BytesWrittenGrpc.Inc(WriteNewSizeInternal);
    if (BytesWrittenTotal)
        BytesWrittenTotal.Inc(WriteNewSize);

    if (BytesWrittenUncompressed)
        BytesWrittenUncompressed.Inc(WriteNewSizeUncompressed);
    if (BytesWrittenComp)
        BytesWrittenComp.Inc(WriteCycleSize);
    if (MsgsWrittenGrpc)
        MsgsWrittenGrpc.Inc(WriteNewMessagesInternal);
    if (MsgsWrittenTotal) {
        MsgsWrittenTotal.Inc(WriteNewMessages);
    }
}

void TPartition::HandleWriteResponse(const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleWriteResponse.");
    if (!HaveWriteMsg) {
        return;
    }
    HaveWriteMsg = false;

    const auto now = ctx.Now();

    for (auto& [sourceId, info] : TxSourceIdForPostPersist) {
        auto it = SourceIdStorage.GetInMemorySourceIds().find(sourceId);
        if (it.IsEnd()) {
            SourceIdStorage.RegisterSourceId(sourceId, info.SeqNo, info.Offset, now);
        } else {
            ui64 seqNo = std::max(info.SeqNo, it->second.SeqNo);
            SourceIdStorage.RegisterSourceId(sourceId, it->second.Updated(seqNo, info.Offset, now));
        }
    }
    TxSourceIdForPostPersist.clear();

    TxAffectedSourcesIds.clear();
    WriteAffectedSourcesIds.clear();
    TxAffectedConsumers.clear();
    SetOffsetAffectedConsumers.clear();
    if (UserActionAndTransactionEvents.empty()) {
        WriteInfosToTx.clear();
    }
    ui64 prevEndOffset = EndOffset;

    ui32 totalLatencyMs = (now - WriteCycleStartTime).MilliSeconds();
    ui32 writeLatencyMs = (now - WriteStartTime).MilliSeconds();

    WriteLatency.IncFor(writeLatencyMs, 1);
    if (writeLatencyMs >= AppData(ctx)->PQConfig.GetWriteLatencyBigMs()) {
        SLIBigLatency.Inc();
    }

    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_WRITE_CYCLE].IncrementFor(totalLatencyMs);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_CYCLE_BYTES_TOTAL].Increment(WriteCycleSize);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_OK].Increment(WriteNewSize);
    TabletCounters.Percentile()[COUNTER_PQ_WRITE_CYCLE_BYTES].IncrementFor(WriteCycleSize);
    TabletCounters.Percentile()[COUNTER_PQ_WRITE_NEW_BYTES].IncrementFor(WriteNewSize);

    UpdateAfterWriteCounters(true);

    //All ok
    for (auto& avg : AvgWriteBytes) {
        avg.Update(WriteNewSize, now);
    }
    for (auto& avg : AvgQuotaBytes) {
        avg.Update(WriteNewSize, now);
    }

    LOG_DEBUG_S(
        ctx, NKikimrServices::PERSQUEUE,
        "TPartition::HandleWriteResponse writeNewSize# " << WriteNewSize;
    );

    if (SupportivePartitionTimeLag) {
        SupportivePartitionTimeLag->UpdateTimestamp(now.MilliSeconds());
    }

    auto writeNewSizeFull = WriteNewSizeFull;

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeFull = 0;
    WriteNewSizeInternal = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewSizeUncompressedFull = 0;
    WriteNewMessages = 0;
    WriteNewMessagesInternal = 0;
    UpdateWriteBufferIsFullState(now);

    AnswerCurrentWrites(ctx);
    SyncMemoryStateWithKVState(ctx);

    if (SplitMergeEnabled(Config)) {
        SplitMergeAvgWriteBytes->Update(writeNewSizeFull, now);
        auto needScaling = CheckScaleStatus(ctx);
        ChangeScaleStatusIfNeeded(needScaling);
    }

    //if EndOffset changed there could be subscriptions witch could be completed
    TVector<std::pair<TReadInfo, ui64>> reads = Subscriber.GetReads(EndOffset);
    for (auto& read : reads) {
        Y_ABORT_UNLESS(EndOffset > read.first.Offset);
        ProcessRead(ctx, std::move(read.first), read.second, true);
    }
    //same for read requests
    ProcessHasDataRequests(ctx);

    ProcessTimestampsForNewData(prevEndOffset, ctx);
}

NKikimrPQ::EScaleStatus TPartition::CheckScaleStatus(const TActorContext& ctx) {
    const auto writeSpeedUsagePercent = SplitMergeAvgWriteBytes->GetValue() * 100.0 / Config.GetPartitionStrategy().GetScaleThresholdSeconds() / TotalPartitionWriteSpeed;
    const auto sourceIdWindow = TDuration::Seconds(std::min<ui32>(5, Config.GetPartitionStrategy().GetScaleThresholdSeconds()));
    const auto sourceIdCount = SourceIdCounter.Count(ctx.Now() - sourceIdWindow);

    LOG_DEBUG_S(
        ctx, NKikimrServices::PERSQUEUE,
        "TPartition::CheckScaleStatus"
            << " splitMergeAvgWriteBytes# " << SplitMergeAvgWriteBytes->GetValue()
            << " writeSpeedUsagePercent# " << writeSpeedUsagePercent
            << " scaleThresholdSeconds# " << Config.GetPartitionStrategy().GetScaleThresholdSeconds()
            << " totalPartitionWriteSpeed# " << TotalPartitionWriteSpeed
            << " sourceIdCount=" << sourceIdCount
            << " Topic: \"" << TopicName() << "\"." <<
        " Partition: " << Partition
    );

    auto splitEnabled = Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT
        || Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE;

    auto mergeEnabled = Config.GetPartitionStrategy().GetPartitionStrategyType() == ::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT_AND_MERGE;

    if (splitEnabled && writeSpeedUsagePercent >= Config.GetPartitionStrategy().GetScaleUpPartitionWriteSpeedThresholdPercent() && sourceIdCount > 1) {
        LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "TPartition::CheckScaleStatus NEED_SPLIT" << " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition
        );
        return NKikimrPQ::EScaleStatus::NEED_SPLIT;
    } else if (mergeEnabled && writeSpeedUsagePercent <= Config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent()) {
        LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "TPartition::CheckScaleStatus NEED_MERGE" << " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << " writeSpeedUsagePercent: " << writeSpeedUsagePercent <<
            " Threshold: " << Config.GetPartitionStrategy().GetScaleDownPartitionWriteSpeedThresholdPercent()
        );
        return NKikimrPQ::EScaleStatus::NEED_MERGE;
    }
    return NKikimrPQ::EScaleStatus::NORMAL;
}

void TPartition::ChangeScaleStatusIfNeeded(NKikimrPQ::EScaleStatus scaleStatus) {
    auto now = TInstant::Now();
    if (scaleStatus == ScaleStatus || LastScaleRequestTime + TDuration::Seconds(SCALE_REQUEST_REPEAT_MIN_SECONDS) > now) {
        return;
    }
    Send(Tablet, new TEvPQ::TEvPartitionScaleStatusChanged(Partition.OriginalPartitionId, scaleStatus));
    LastScaleRequestTime = now;
    ScaleStatus = scaleStatus;
}

void TPartition::HandleOnWrite(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::TEvWrite");

    if (!CanEnqueue()) {
        ReplyError(ctx, ev->Get()->Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return;
    }

    ui32 sz = std::accumulate(ev->Get()->Msgs.begin(), ev->Get()->Msgs.end(), 0u, [](ui32 sum, const TEvPQ::TEvWrite::TMsg& msg) {
        return sum + msg.Data.size();
    });

    bool mirroredPartition = Config.GetPartitionConfig().HasMirrorFrom();

    if (mirroredPartition && !ev->Get()->OwnerCookie.empty()) {
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "Write to mirrored topic is forbiden ");
        return;
    }

    ui64 decReservedSize = 0;
    TStringBuf owner;

    if (!mirroredPartition && !ev->Get()->IsDirectWrite) {
        owner = TOwnerInfo::GetOwnerFromOwnerCookie(ev->Get()->OwnerCookie);
        auto it = Owners.find(owner);

        if (it == Owners.end() || it->second.NeedResetOwner) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::WRONG_COOKIE,
                TStringBuilder() << "new GetOwnership request needed for owner " << owner);
            return;
        }

        if (it->second.SourceIdDeleted) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
                TStringBuilder() << "Yours maximum written sequence number for session was deleted, need to recreate session. "
                    << "Current count of sourceIds is " << SourceIdStorage.GetInMemorySourceIds().size() << " and limit is " << Config.GetPartitionConfig().GetSourceIdMaxCounts()
                    << ", current minimum sourceid timestamp(Ms) is " << SourceIdStorage.MinAvailableTimestamp(ctx.Now()).MilliSeconds()
                    << " and border timestamp(Ms) is " << ((ctx.Now() - TInstant::Seconds(Config.GetPartitionConfig().GetSourceIdLifetimeSeconds())).MilliSeconds()));
            return;
        }

        if (it->second.OwnerCookie != ev->Get()->OwnerCookie) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::WRONG_COOKIE,
                        TStringBuilder() << "incorrect ownerCookie " << ev->Get()->OwnerCookie << ", must be " << it->second.OwnerCookie);
            return;
        }

        if (ev->Get()->MessageNo != it->second.NextMessageNo) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                TStringBuilder() << "reorder in requests, waiting " << it->second.NextMessageNo << ", but got " << ev->Get()->MessageNo);
            DropOwner(it, ctx);
            return;
        }

        ++it->second.NextMessageNo;
        decReservedSize = it->second.DecReservedSize();
    }

    ReservedSize -= decReservedSize;
    TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);

    TMaybe<ui64> offset = ev->Get()->Offset;

    if (WriteInflightSize > Config.GetPartitionConfig().GetMaxWriteInflightSize()) {
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(ev->Get()->Msgs.size());
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(sz);

        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::OVERLOAD,
            TStringBuilder() << "try later. Write inflight limit reached. "
                << WriteInflightSize << " vs. maximum " <<  Config.GetPartitionConfig().GetMaxWriteInflightSize());
        return;
    }
    for (const auto& msg: ev->Get()->Msgs) {
        //this is checked in pq_impl when forming EvWrite request
        Y_ABORT_UNLESS(!msg.SourceId.empty() || ev->Get()->IsDirectWrite || msg.DisableDeduplication);
        Y_ABORT_UNLESS(!msg.Data.empty());

        if (msg.SeqNo > (ui64)Max<i64>()) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Request to write wrong SeqNo. Partition "
                << Partition << " sourceId '" << EscapeC(msg.SourceId) << "' seqno " << msg.SeqNo);

            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                TStringBuilder() << "wrong SeqNo " << msg.SeqNo);
            return;
        }

        ui32 sz = msg.Data.size() + msg.SourceId.size() + TClientBlob::OVERHEAD;

        if (sz > MAX_BLOB_PART_SIZE) {
            ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                TStringBuilder() << "too big message " << sz << " vs. maximum " << MAX_BLOB_PART_SIZE);
            return;
        }

        if (!mirroredPartition) {
            SourceIdStorage.RegisterSourceIdOwner(msg.SourceId, owner);
        }
    }

    ui64 size = 0;
    for (auto& msg: ev->Get()->Msgs) {
        size += msg.Data.size();
        bool needToChangeOffset = msg.PartNo + 1 == msg.TotalParts;
        EmplacePendingRequest(TWriteMsg{ev->Get()->Cookie, offset, std::move(msg), ev->Get()->InitialSeqNo}, ctx);
        if (offset && needToChangeOffset) {
            ++*offset;
        }
    }
    if (WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota(ctx)) {
        SetDeadlinesForWrites(ctx);
    }
    WriteInflightSize += size;

    // TODO: remove decReservedSize == 0
    Y_ABORT_UNLESS(size <= decReservedSize || decReservedSize == 0);
    UpdateWriteBufferIsFullState(ctx.Now());

}

void TPartition::HandleOnIdle(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleOnWrite TEvRegisterMessageGroup.");

    const auto& body = ev->Get()->Body;

    auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
    if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
        if (!it->second.Explicit) {
            return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                "Trying to register implicitly registered SourceId");
        }

        switch (it->second.State) {
        case TSourceIdInfo::EState::Registered:
            return ReplyOk(ctx, ev->Get()->Cookie);
        case TSourceIdInfo::EState::PendingRegistration:
            if (!body.AfterSplit) {
                return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                    "AfterSplit must be set");
            }
            break;
        default:
            return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::ERROR,
                TStringBuilder() << "Unknown state: " << static_cast<ui32>(it->second.State));
        }
    } else if (body.AfterSplit) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            "SourceId not found, registration cannot be completed");
    }

    EmplacePendingRequest(TRegisterMessageGroupMsg(*ev->Get()), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleOnWrite TEvDeregisterMessageGroup.");

    const auto& body = ev->Get()->Body;

    auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
    if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
            "SourceId doesn't exist");
    }

    EmplacePendingRequest(TDeregisterMessageGroupMsg(*ev->Get()), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::HandleOnWrite TEvSplitMessageGroup.");

    if (ev->Get()->Deregistrations.size() > 1) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "Currently, single deregistrations are supported");
    }

    TSplitMessageGroupMsg msg(ev->Get()->Cookie);

    for (auto& body : ev->Get()->Deregistrations) {
        auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
        if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
            msg.Deregistrations.push_back(std::move(body));
        } else {
            return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
                "SourceId doesn't exist");
        }
    }

    for (auto& body : ev->Get()->Registrations) {
        auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
        if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
            msg.Registrations.push_back(std::move(body));
        } else {
            if (!it->second.Explicit) {
                return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::BAD_REQUEST,
                    "Trying to register implicitly registered SourceId");
            }
        }
    }

    EmplacePendingRequest(std::move(msg), ctx);
}

std::pair<TKey, ui32> TPartition::Compact(const TKey& key, const ui32 size, bool headCleared) {
    std::pair<TKey, ui32> res({key, size});
    ui32 x = headCleared ? 0 : Head.PackedSize;
    Y_ABORT_UNLESS(std::accumulate(DataKeysHead.begin(), DataKeysHead.end(), 0u, [](ui32 sum, const TKeyLevel& level){return sum + level.Sum();}) == NewHead.PackedSize + x);
    for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
        auto jt = it; ++jt;
        if (it->NeedCompaction()) {
            res = it->Compact();
            if (jt != DataKeysHead.rend()) {
                jt->AddKey(res.first, res.second);
            }
        } else {
            Y_ABORT_UNLESS(jt == DataKeysHead.rend() || !jt->NeedCompaction()); //compact must start from last level, not internal
        }
        Y_ABORT_UNLESS(!it->NeedCompaction());
    }
    Y_ABORT_UNLESS(res.second >= size);
    Y_ABORT_UNLESS(res.first.GetOffset() < key.GetOffset() || res.first.GetOffset() == key.GetOffset() && res.first.GetPartNo() <= key.GetPartNo());
    return res;
}

void TPartition::StartProcessChangeOwnerRequests(const TActorContext& ctx)
{
    ctx.Send(ctx.SelfID, new TEvPQ::TEvProcessChangeOwnerRequests());
}

void TPartition::Handle(TEvPQ::TEvProcessChangeOwnerRequests::TPtr&, const TActorContext& ctx)
{
    ProcessChangeOwnerRequests(ctx);
}

void TPartition::ProcessChangeOwnerRequests(const TActorContext& ctx) {
    PQ_LOG_T("TPartition::ProcessChangeOwnerRequests.");

    while (!WaitToChangeOwner.empty()) {
        auto &ev = WaitToChangeOwner.front();
        if (OwnerPipes.find(ev->PipeClient) != OwnerPipes.end()) { //this is not request from dead pipe
            ProcessChangeOwnerRequest(ev.Release(), ctx);
        } else {
            ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::ERROR, "Pipe for GetOwnershipRequest is already dead");
        }
        WaitToChangeOwner.pop_front();
    }
    HandlePendingRequests(ctx);
}

void TPartition::CancelOneWriteOnWrite(const TActorContext& ctx,
                                       const TString& errorStr,
                                       const TWriteMsg& p,
                                       NPersQueue::NErrorCode::EErrorCode errorCode)
{
    ScheduleReplyError(p.Cookie, errorCode, errorStr);
    for (auto it = Owners.begin(); it != Owners.end();) {
        it = DropOwner(it, ctx);
    }
    StartProcessChangeOwnerRequests(ctx);
}

TPartition::EProcessResult TPartition::PreProcessRequest(TRegisterMessageGroupMsg& msg) {
    if (!CanWrite()) {
        ScheduleReplyError(msg.Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(msg.Cookie,
                           NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    if (TxAffectedSourcesIds.contains(msg.Body.SourceId)) {
        return EProcessResult::Blocked;
    }
    WriteAffectedSourcesIds.insert(msg.Body.SourceId);
    return EProcessResult::Continue;
}

void TPartition::ExecRequest(TRegisterMessageGroupMsg& msg, ProcessParameters& parameters) {

    auto& body = msg.Body;

    TMaybe<TPartitionKeyRange> keyRange;
    if (body.KeyRange) {
        keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
    }

    body.AssignedOffset = parameters.CurOffset;
    parameters.SourceIdBatch.RegisterSourceId(body.SourceId, body.SeqNo, parameters.CurOffset, CurrentTimestamp, std::move(keyRange));
}

TPartition::EProcessResult TPartition::PreProcessRequest(TDeregisterMessageGroupMsg& msg) {
    if (!CanWrite()) {
        ScheduleReplyError(msg.Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(msg.Cookie,
                           NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    if (TxAffectedSourcesIds.contains(msg.Body.SourceId)) {
        return EProcessResult::Blocked;
    }
    WriteAffectedSourcesIds.insert(msg.Body.SourceId);
    return EProcessResult::Continue;
}

void TPartition::ExecRequest(TDeregisterMessageGroupMsg& msg, ProcessParameters& parameters) {
    parameters.SourceIdBatch.DeregisterSourceId(msg.Body.SourceId);
}


TPartition::EProcessResult TPartition::PreProcessRequest(TSplitMessageGroupMsg& msg) {
    if (!CanWrite()) {
        ScheduleReplyError(msg.Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(msg.Cookie,
                           NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    for (auto& body : msg.Registrations) {
        if (TxAffectedSourcesIds.contains(body.SourceId)) {
            return EProcessResult::Blocked;
        }
        WriteAffectedSourcesIds.insert(body.SourceId);
    }
    for (auto& body : msg.Deregistrations) {
        if (TxAffectedSourcesIds.contains(body.SourceId)) {
            return EProcessResult::Blocked;
        }
        WriteAffectedSourcesIds.insert(body.SourceId);
    }
    return EProcessResult::Continue;

}


void TPartition::ExecRequest(TSplitMessageGroupMsg& msg, ProcessParameters& parameters) {
    for (auto& body : msg.Deregistrations) {
        parameters.SourceIdBatch.DeregisterSourceId(body.SourceId);
    }

    for (auto& body : msg.Registrations) {
        TMaybe<TPartitionKeyRange> keyRange;
        if (body.KeyRange) {
            keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
        }

        body.AssignedOffset = parameters.CurOffset;
        parameters.SourceIdBatch.RegisterSourceId(body.SourceId, body.SeqNo, parameters.CurOffset, CurrentTimestamp, std::move(keyRange), true);
    }
}

TPartition::EProcessResult TPartition::PreProcessRequest(TWriteMsg& p) {
    if (!CanWrite()) {
        ScheduleReplyError(p.Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(p.Cookie,
                            NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                            "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    if (TxAffectedSourcesIds.contains(p.Msg.SourceId)) {
        return EProcessResult::Blocked;
    }
    WriteAffectedSourcesIds.insert(p.Msg.SourceId);
    return EProcessResult::Continue;
}

void TPartition::AddCmdWrite(const std::optional<TPartitionedBlob::TFormedBlobInfo>& newWrite,
                             TEvKeyValue::TEvRequest* request,
                             const TActorContext& ctx)
{
    auto write = request->Record.AddCmdWrite();
    write->SetKey(newWrite->Key.ToString());
    write->SetValue(newWrite->Value);
    Y_ABORT_UNLESS(!newWrite->Key.IsHead());
    auto channel = GetChannel(NextChannel(newWrite->Key.IsHead(), newWrite->Value.Size()));
    write->SetStorageChannel(channel);
    write->SetTactic(AppData(ctx)->PQConfig.GetTactic());

    TKey resKey = newWrite->Key;
    resKey.SetType(TKeyPrefix::TypeData);
    write->SetKeyToCache(resKey.ToString());
    WriteCycleSize += newWrite->Value.size();
}

void TPartition::RenameFormedBlobs(const std::deque<TPartitionedBlob::TRenameFormedBlobInfo>& formedBlobs,
                                   ProcessParameters& parameters,
                                   ui32 curWrites,
                                   TEvKeyValue::TEvRequest* request,
                                   const TActorContext& ctx)
{
    for (ui32 i = 0; i < formedBlobs.size(); ++i) {
        const auto& x = formedBlobs[i];
        if (i + curWrites < formedBlobs.size()) { //this KV pair is already writed, rename needed
            auto rename = request->Record.AddCmdRename();
            rename->SetOldKey(x.OldKey.ToString());
            rename->SetNewKey(x.NewKey.ToString());
        }
        if (!DataKeysBody.empty() && CompactedKeys.empty()) {
            Y_ABORT_UNLESS(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() <= x.NewKey.GetOffset(),
                           "LAST KEY %s, HeadOffset %lu, NEWKEY %s",
                           DataKeysBody.back().Key.ToString().c_str(),
                           Head.Offset,
                           x.NewKey.ToString().c_str());
        }
        LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "writing blob: topic '" << TopicName() << "' partition " << Partition
                    << " " << x.OldKey.ToString() << " size " << x.Size << " WTime " << ctx.Now().MilliSeconds()
                   );

        CompactedKeys.emplace_back(x.NewKey, x.Size);
    }

    if (!formedBlobs.empty()) {
        parameters.HeadCleared = true;

        NewHead.Clear();
        NewHead.Offset = PartitionedBlob.GetOffset();
        NewHead.PartNo = PartitionedBlob.GetHeadPartNo();
        NewHead.PackedSize = 0;
    }
}

ui32 TPartition::RenameTmpCmdWrites(TEvKeyValue::TEvRequest* request)
{
    ui32 curWrites = 0;
    for (ui32 i = 0; i < request->Record.CmdWriteSize(); ++i) { //change keys for yet to be writed KV pairs
        TKey key(request->Record.GetCmdWrite(i).GetKey());
        if (key.GetType() == TKeyPrefix::TypeTmpData) {
            key.SetType(TKeyPrefix::TypeData);
            request->Record.MutableCmdWrite(i)->SetKey(TString(key.Data(), key.Size()));
            ++curWrites;
        }
    }
    return curWrites;
}

bool TPartition::ExecRequest(TWriteMsg& p, ProcessParameters& parameters, TEvKeyValue::TEvRequest* request) {
    if (!CanWrite()) {
        ScheduleReplyError(p.Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return false;
    }
    if (DiskIsFull) {
        ScheduleReplyError(p.Cookie,
                            NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                            "Disk is full");
        return false;
    }
    const auto& ctx = ActorContext();

    ui64& curOffset = parameters.CurOffset;
    auto& sourceIdBatch = parameters.SourceIdBatch;
    auto sourceId = sourceIdBatch.GetSource(p.Msg.SourceId);

    WriteInflightSize -= p.Msg.Data.size();

    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_RECEIVE_QUEUE].IncrementFor(ctx.Now().MilliSeconds() - p.Msg.ReceiveTimestamp);
    //check already written

    ui64 poffset = p.Offset ? *p.Offset : curOffset;

    LOG_TRACE_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicName() << "' partition " << Partition
                << " process write for '" << EscapeC(p.Msg.SourceId) << "'"
                << " DisableDeduplication=" << p.Msg.DisableDeduplication
                << " SeqNo=" << p.Msg.SeqNo
                << " LocalSeqNo=" << sourceId.SeqNo()
                << " InitialSeqNo=" << p.InitialSeqNo
    );

    if (!p.Msg.DisableDeduplication
        && ((sourceId.SeqNo() && *sourceId.SeqNo() >= p.Msg.SeqNo)
        || (p.InitialSeqNo && p.InitialSeqNo.value() >= p.Msg.SeqNo))) {
        if (poffset >= curOffset) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Already written message. Topic: '" << TopicName()
                        << "' Partition: " << Partition << " SourceId: '" << EscapeC(p.Msg.SourceId)
                        << "'. Message seqNo: " << p.Msg.SeqNo
                        << ". Committed seqNo: " << sourceId.CommittedSeqNo()
                        << ". Writing seqNo: " << sourceId.UpdatedSeqNo()
                        << ". EndOffset: " << EndOffset << ". CurOffset: " << curOffset << ". Offset: " << poffset
            );
            if (!p.Internal) {
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ALREADY].Increment(1);
                MsgsDiscarded.Inc();
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ALREADY].Increment(p.Msg.Data.size());
                BytesDiscarded.Inc(p.Msg.Data.size());
            }
        } else {
            if (!p.Internal) {
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_SMALL_OFFSET].Increment(1);
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_SMALL_OFFSET].Increment(p.Msg.Data.size());
            }
        }

        TString().swap(p.Msg.Data);
        return true;
    }

    if (const auto& hbVersion = p.Msg.HeartbeatVersion) {
        if (!sourceId.SeqNo()) {
            CancelOneWriteOnWrite(ctx,
                                    TStringBuilder() << "Cannot apply heartbeat on unknown sourceId: " << EscapeC(p.Msg.SourceId),
                                    p,
                                    NPersQueue::NErrorCode::BAD_REQUEST);
            return false;
        }
        if (!sourceId.Explicit()) {
            CancelOneWriteOnWrite(ctx,
                                    TStringBuilder() << "Cannot apply heartbeat on implcit sourceId: " << EscapeC(p.Msg.SourceId),
                                    p,
                                    NPersQueue::NErrorCode::BAD_REQUEST);
            return false;
        }

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition
                    << " process heartbeat sourceId '" << EscapeC(p.Msg.SourceId) << "'"
                    << " version " << *hbVersion
        );

        sourceId.Update(THeartbeat{*hbVersion, p.Msg.Data});

        return true;
    }

    if (poffset < curOffset) { //too small offset
        CancelOneWriteOnWrite(ctx,
                                TStringBuilder() << "write message sourceId: " << EscapeC(p.Msg.SourceId) <<
                                " seqNo: " << p.Msg.SeqNo <<
                                " partNo: " << p.Msg.PartNo <<
                                " has incorrect offset " << poffset <<
                                ", must be at least " << curOffset,
                                p,
                                NPersQueue::NErrorCode::EErrorCode::WRITE_ERROR_BAD_OFFSET);
        return false;
    }

    Y_ABORT_UNLESS(poffset >= curOffset);

    bool needCompactHead = poffset > curOffset;
    if (needCompactHead) { //got gap
        if (p.Msg.PartNo != 0) { //gap can't be inside of partitioned message
            CancelOneWriteOnWrite(ctx,
                                    TStringBuilder() << "write message sourceId: " << EscapeC(p.Msg.SourceId) <<
                                    " seqNo: " << p.Msg.SeqNo <<
                                    " partNo: " << p.Msg.PartNo <<
                                    " has gap inside partitioned message, incorrect offset " << poffset <<
                                    ", must be " << curOffset,
                                    p,
                                    NPersQueue::NErrorCode::BAD_REQUEST);
            return false;
        }
        curOffset = poffset;
    }

    if (p.Msg.PartNo == 0) { //create new PartitionedBlob
        //there could be parts from previous owner, clear them
        if (!parameters.OldPartsCleared) {
            parameters.OldPartsCleared = true;

            NPQ::AddCmdDeleteRange(*request, TKeyPrefix::TypeTmpData, Partition);
        }

        if (PartitionedBlob.HasFormedBlobs()) {
            //clear currently-writed blobs
            auto oldCmdWrite = request->Record.GetCmdWrite();
            request->Record.ClearCmdWrite();
            for (ui32 i = 0; i < (ui32)oldCmdWrite.size(); ++i) {
                TKey key(oldCmdWrite.Get(i).GetKey());
                if (key.GetType() != TKeyPrefix::TypeTmpData) {
                    request->Record.AddCmdWrite()->CopyFrom(oldCmdWrite.Get(i));
                }
            }
        }
        PartitionedBlob = TPartitionedBlob(Partition, curOffset, p.Msg.SourceId, p.Msg.SeqNo,
                                            p.Msg.TotalParts, p.Msg.TotalSize, Head, NewHead,
                                            parameters.HeadCleared, needCompactHead, MaxBlobSize);
    }

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicName() << "' partition " << Partition
                << " part blob processing sourceId '" << EscapeC(p.Msg.SourceId) <<
                "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo
    );
    TString s;
    if (!PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
        //this must not be happen - client sends gaps, fail this client till the end
        //now no changes will leak
        ctx.Send(Tablet, new TEvents::TEvPoisonPill());
        return false;
    }
    WriteNewSizeFull += p.Msg.SourceId.size() + p.Msg.Data.size();
    WriteNewSizeUncompressedFull += p.Msg.UncompressedSize + p.Msg.SourceId.size();
    if (!p.Internal) {
        WriteNewSize += p.Msg.SourceId.size() + p.Msg.Data.size();
        WriteNewSizeUncompressed += p.Msg.UncompressedSize + p.Msg.SourceId.size();
        WriteNewSizeInternal += p.Msg.External ? 0 : (p.Msg.SourceId.size() + p.Msg.Data.size());
    }
    if (p.Msg.PartNo == 0 && !p.Internal) {
        ++WriteNewMessages;
        if (!p.Msg.External)
            ++WriteNewMessagesInternal;
    }

    TMaybe<TPartData> partData;
    if (p.Msg.TotalParts > 1) { //this is multi-part message
        partData = TPartData(p.Msg.PartNo, p.Msg.TotalParts, p.Msg.TotalSize);
    }
    WriteTimestamp = ctx.Now();
    WriteTimestampEstimate = p.Msg.WriteTimestamp > 0 ? TInstant::MilliSeconds(p.Msg.WriteTimestamp) : WriteTimestamp;
    TClientBlob blob(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.Data, std::move(partData), WriteTimestampEstimate,
                        TInstant::MilliSeconds(p.Msg.CreateTimestamp == 0 ? curOffset : p.Msg.CreateTimestamp),
                        p.Msg.UncompressedSize, p.Msg.PartitionKey, p.Msg.ExplicitHashKey); //remove curOffset when LB will report CTime

    const ui64 writeLagMs =
        (WriteTimestamp - TInstant::MilliSeconds(p.Msg.CreateTimestamp)).MilliSeconds();
    WriteLagMs.Update(writeLagMs, WriteTimestamp);
    if (InputTimeLag && !p.Internal) {
        InputTimeLag->IncFor(writeLagMs, 1);
    } else if (SupportivePartitionTimeLag) {
        SupportivePartitionTimeLag->Insert(writeLagMs, 1);
    }
    if (p.Msg.PartNo == 0  && !p.Internal) {
        MessageSize.IncFor(p.Msg.TotalSize + p.Msg.SourceId.size(), 1);
    }
    bool lastBlobPart = blob.IsLastPart();

    //will return compacted tmp blob
    auto newWrite = PartitionedBlob.Add(std::move(blob));

    if (newWrite && !newWrite->Value.empty()) {
        AddCmdWrite(newWrite, request, ctx);

        LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Topic '" << TopicName() <<
                    "' partition " << Partition <<
                    " part blob sourceId '" << EscapeC(p.Msg.SourceId) <<
                    "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo <<
                    " result is " << newWrite->Key.ToString() <<
                    " size " << newWrite->Value.size()
                   );
    }

    if (lastBlobPart) {
        Y_ABORT_UNLESS(PartitionedBlob.IsComplete());
        ui32 curWrites = RenameTmpCmdWrites(request);
        Y_ABORT_UNLESS(curWrites <= PartitionedBlob.GetFormedBlobs().size());
        RenameFormedBlobs(PartitionedBlob.GetFormedBlobs(),
                          parameters,
                          curWrites,
                          request,
                          ctx);
        ui32 countOfLastParts = 0;
        for (auto& x : PartitionedBlob.GetClientBlobs()) {
            if (NewHead.Batches.empty() || NewHead.Batches.back().Packed) {
                NewHead.Batches.emplace_back(curOffset, x.GetPartNo(), TVector<TClientBlob>());
                NewHead.PackedSize += GetMaxHeaderSize(); //upper bound for packed size
            }
            if (x.IsLastPart()) {
                ++countOfLastParts;
            }
            Y_ABORT_UNLESS(!NewHead.Batches.back().Packed);
            NewHead.Batches.back().AddBlob(x);
            NewHead.PackedSize += x.GetBlobSize();
            if (NewHead.Batches.back().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                NewHead.Batches.back().Pack();
                NewHead.PackedSize += NewHead.Batches.back().GetPackedSize(); //add real packed size for this blob

                NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
                NewHead.PackedSize -= NewHead.Batches.back().GetUnpackedSize();
            }
        }

        Y_ABORT_UNLESS(countOfLastParts == 1);

        LOG_DEBUG_S(
                ctx, NKikimrServices::PERSQUEUE,
                "Topic '" << TopicName() << "' partition " << Partition
                    << " part blob complete sourceId '" << EscapeC(p.Msg.SourceId) << "' seqNo " << p.Msg.SeqNo
                    << " partNo " << p.Msg.PartNo << " FormedBlobsCount " << PartitionedBlob.GetFormedBlobs().size()
                    << " NewHead: " << NewHead
        );

        sourceId.Update(p.Msg.SeqNo, curOffset, CurrentTimestamp);

        ++curOffset;
        PartitionedBlob = TPartitionedBlob(Partition, 0, "", 0, 0, 0, Head, NewHead, true, false, MaxBlobSize);
    }

    TString().swap(p.Msg.Data);
    return true;
}

std::pair<TKey, ui32> TPartition::GetNewWriteKeyImpl(bool headCleared, bool needCompaction, ui32 HeadSize)
{
    TKey key(TKeyPrefix::TypeData, Partition, NewHead.Offset, NewHead.PartNo, NewHead.GetCount(), NewHead.GetInternalPartsCount(), !needCompaction);

    if (NewHead.PackedSize > 0)
        DataKeysHead[TotalLevels - 1].AddKey(key, NewHead.PackedSize);
    Y_ABORT_UNLESS(HeadSize + NewHead.PackedSize <= 3 * MaxSizeCheck);

    std::pair<TKey, ui32> res;

    if (needCompaction) { //compact all
        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
        if (!headCleared) { //compacted blob must contain both head and NewHead
            key = TKey(TKeyPrefix::TypeData, Partition, Head.Offset, Head.PartNo, NewHead.GetCount() + Head.GetCount(),
                        Head.GetInternalPartsCount() +  NewHead.GetInternalPartsCount(), false);
        } //otherwise KV blob is not from head (!key.IsHead()) and contains only new data from NewHead
        res = std::make_pair(key, HeadSize + NewHead.PackedSize);
    } else {
        res = Compact(key, NewHead.PackedSize, headCleared);
        Y_ABORT_UNLESS(res.first.IsHead());//may compact some KV blobs from head, but new KV blob is from head too
        Y_ABORT_UNLESS(res.second >= NewHead.PackedSize); //at least new data must be writed
    }
    Y_ABORT_UNLESS(res.second <= MaxBlobSize);
    return res;
}

std::pair<TKey, ui32> TPartition::GetNewWriteKey(bool headCleared) {
    bool needCompaction = false;
    ui32 HeadSize = headCleared ? 0 : Head.PackedSize;
    if (HeadSize + NewHead.PackedSize > 0 && HeadSize + NewHead.PackedSize
                                                        >= Min<ui32>(MaxBlobSize, Config.GetPartitionConfig().GetLowWatermark()))
        needCompaction = true;

    if (PartitionedBlob.IsInited()) { //has active partitioned blob - compaction is forbiden, head and newHead will be compacted when this partitioned blob is finished
        needCompaction = false;
    }

    Y_ABORT_UNLESS(NewHead.PackedSize > 0 || needCompaction); //smthing must be here

    return GetNewWriteKeyImpl(headCleared, needCompaction, HeadSize);
}

void TPartition::AddNewWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, bool headCleared, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::AddNewWriteBlob.");

    const auto& key = res.first;

    TString valueD;
    valueD.reserve(res.second);
    ui32 pp = Head.FindPos(key.GetOffset(), key.GetPartNo());
    if (pp < Max<ui32>() && key.GetOffset() < EndOffset) { //this batch trully contains this offset
        Y_ABORT_UNLESS(pp < Head.Batches.size());
        Y_ABORT_UNLESS(Head.Batches[pp].GetOffset() == key.GetOffset());
        Y_ABORT_UNLESS(Head.Batches[pp].GetPartNo() == key.GetPartNo());
        for (; pp < Head.Batches.size(); ++pp) { //TODO - merge small batches here
            Y_ABORT_UNLESS(Head.Batches[pp].Packed);
            Head.Batches[pp].SerializeTo(valueD);
        }
    }
    for (auto& b : NewHead.Batches) {
        Y_ABORT_UNLESS(b.Packed);
        b.SerializeTo(valueD);
    }

    Y_ABORT_UNLESS(res.second >= valueD.size());

    if (res.second > valueD.size() && res.first.IsHead()) { //change to real size if real packed size is smaller

        Y_ABORT("Can't be here right now, only after merging of small batches");

        for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
            if (it->KeysCount() > 0 ) {
                auto res2 = it->PopBack();
                Y_ABORT_UNLESS(res2 == res);
                res2.second = valueD.size();

                DataKeysHead[TotalLevels - 1].AddKey(res2.first, res2.second);

                res2 = Compact(res2.first, res2.second, headCleared);

                Y_ABORT_UNLESS(res2.first == res.first);
                Y_ABORT_UNLESS(res2.second == valueD.size());
                res = res2;
                break;
            }
        }
    }

    Y_ABORT_UNLESS(res.second == valueD.size() || res.first.IsHead());

    TClientBlob::CheckBlob(key, valueD);

    auto write = request->Record.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(valueD);

    if (!key.IsHead())
        write->SetKeyToCache(key.Data(), key.Size());

    bool isInline = key.IsHead() && valueD.size() < MAX_INLINE_SIZE;

    if (isInline)
        write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
    else {
        auto channel = GetChannel(NextChannel(key.IsHead(), valueD.size()));
        write->SetStorageChannel(channel);
        write->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    }

    //Need to clear all compacted blobs
    TKey k = CompactedKeys.empty() ? key : CompactedKeys.front().first;
    ClearOldHead(k.GetOffset(), k.GetPartNo(), request);

    if (!key.IsHead()) {
        if (!DataKeysBody.empty() && CompactedKeys.empty()) {
            Y_ABORT_UNLESS(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() <= key.GetOffset(),
                "LAST KEY %s, HeadOffset %lu, NEWKEY %s", DataKeysBody.back().Key.ToString().c_str(), Head.Offset, key.ToString().c_str());
        }
        CompactedKeys.push_back(res);
        NewHead.Clear();
        NewHead.Offset = res.first.GetOffset() + res.first.GetCount();
        NewHead.PartNo = 0;
    } else {
        Y_ABORT_UNLESS(NewHeadKey.Size == 0);
        NewHeadKey = {key, res.second, CurrentTimestamp, 0};
    }
    WriteCycleSize += write->GetValue().size();
    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::SetDeadlinesForWrites(const TActorContext& ctx) {
    PQ_LOG_T("TPartition::SetDeadlinesForWrites.");
    if (AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs() > 0 && QuotaDeadline == TInstant::Zero()) {
        QuotaDeadline = ctx.Now() + TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs());

        ctx.Schedule(QuotaDeadline, new TEvPQ::TEvQuotaDeadlineCheck());
    }
}

void TPartition::Handle(TEvPQ::TEvQuotaDeadlineCheck::TPtr&, const TActorContext& ctx) {
    PQ_LOG_T("TPartition::Handle TEvQuotaDeadlineCheck.");

    FilterDeadlinedWrites(ctx);
}

void TPartition::FilterDeadlinedWrites(const TActorContext& ctx) {
    if (QuotaDeadline == TInstant::Zero() || QuotaDeadline > ctx.Now()) {
        return;
    }
    PQ_LOG_T("TPartition::FilterDeadlinedWrites.");

    FilterDeadlinedWrites(ctx, PendingRequests);

    QuotaDeadline = TInstant::Zero();

    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::FilterDeadlinedWrites(const TActorContext& ctx, TMessageQueue& requests)
{
    TMessageQueue newRequests;
    for (auto& w : requests) {
        if (!w.IsWrite() || w.GetWrite().Msg.IgnoreQuotaDeadline) {
            newRequests.emplace_back(std::move(w));
            continue;
        }
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;

            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            WriteInflightSize -= msg.Data.size();
        }

        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::OVERLOAD, "quota exceeded");
    }
    requests = std::move(newRequests);
}

void TPartition::RemoveMessages(TMessageQueue& src, TMessageQueue& dst)
{
    for (auto& r : src) {
        dst.push_back(std::move(r));
    }
    src.clear();
}

void TPartition::RemoveMessagesToQueue(TMessageQueue& requests)
{
    for (auto& r : requests) {
        UserActionAndTransactionEvents.emplace_back(std::move(r));
    }
    requests.clear();
}

void TPartition::RemovePendingRequests(TMessageQueue& requests)
{
    RemoveMessages(PendingRequests, requests);
}

bool TPartition::RequestBlobQuota()
{
    if (!WriteQuotaTrackerActor) {
        return false;
    }

    size_t quotaSize = 0;
    for (auto& r : PendingRequests) {
        quotaSize += r.GetWriteSize();
    }

    if (!quotaSize) {
        return false;
    }

    RemovePendingRequests(QuotaWaitingRequests);
    RequestBlobQuota(quotaSize);

    return true;
}

void TPartition::HandlePendingRequests(const TActorContext& ctx)
{
    if (WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota(ctx)) {
        return;
    }
    if (RequestBlobQuota()) {
        return;
    }
    RemoveMessagesToQueue(PendingRequests);
    ProcessTxsAndUserActs(ctx);
}

void TPartition::BeginHandleRequests(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    Y_ABORT_UNLESS(Head.PackedSize + NewHead.PackedSize <= 2 * MaxSizeCheck);

    TInstant now = ctx.Now();
    WriteCycleStartTime = now;

    HaveData = false;
    HaveCheckDisk = false;

    if (!DiskIsFull) {
        return;
    }

    AddCheckDiskRequest(request, NumChannels);
    HaveCheckDisk = true;
}

void TPartition::EndHandleRequests(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    HaveDrop = CleanUp(request, ctx);

    ProcessReserveRequests(ctx);
    if (!HaveData && !HaveDrop && !HaveCheckDisk) { //no data writed/deleted
        return;
    }

    WritesTotal.Inc();

    UpdateAfterWriteCounters(false);

    AddMetaKey(request);
    WriteStartTime = TActivationContext::Now();
}

void TPartition::BeginProcessWrites(const TActorContext& ctx)
{
    SourceIdBatch.ConstructInPlace(SourceManager.CreateModificationBatch(ctx));

    HeadCleared = false;
}

void TPartition::EndProcessWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    if (HeadCleared) {
        Y_ABORT_UNLESS(!CompactedKeys.empty() || Head.PackedSize == 0);
        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
    }

    if (NewHead.PackedSize == 0) { //nothing added to head - just compaction or tmp part blobs writed
        if (!SourceIdBatch->HasModifications()) {
            HaveData = request->Record.CmdWriteSize() > 0
                || request->Record.CmdRenameSize() > 0
                || request->Record.CmdDeleteRangeSize() > 0;
            return;
        } else {
            SourceIdBatch->FillRequest(request);
            HaveData = true;
            return;
        }
    }

    SourceIdBatch->FillRequest(request);

    std::pair<TKey, ui32> res = GetNewWriteKey(HeadCleared);
    const auto& key = res.first;

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Add new write blob: topic '" << TopicName() << "' partition " << Partition
                << " compactOffset " << key.GetOffset() << "," << key.GetCount()
                << " HeadOffset " << Head.Offset << " endOffset " << EndOffset << " curOffset "
                << NewHead.GetNextOffset() << " " << key.ToString()
                << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );
    AddNewWriteBlob(res, request, HeadCleared, ctx);

    HaveData = true;
}

void TPartition::BeginAppendHeadWithNewWrites(const TActorContext& ctx)
{
    Parameters.ConstructInPlace(*SourceIdBatch);
    Parameters->CurOffset = PartitionedBlob.IsInited() ? PartitionedBlob.GetOffset() : EndOffset;

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewMessages = 0;
    UpdateWriteBufferIsFullState(ctx.Now());
    CurrentTimestamp = ctx.Now();

    NewHead.Offset = EndOffset;
    NewHead.PartNo = 0;
    NewHead.PackedSize = 0;

    Y_ABORT_UNLESS(NewHead.Batches.empty());

    Parameters->OldPartsCleared = false;
    Parameters->HeadCleared = (Head.PackedSize == 0);
}

void TPartition::EndAppendHeadWithNewWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    if (const auto heartbeat = SourceIdBatch->CanEmitHeartbeat()) {
        if (heartbeat->Version > LastEmittedHeartbeat) {
            LOG_INFO_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Topic '" << TopicName() << "' partition " << Partition
                        << " emit heartbeat " << heartbeat->Version
            );

            auto hbMsg = TWriteMsg{Max<ui64>() /* cookie */, Nothing(), TEvPQ::TEvWrite::TMsg{
                .SourceId = NSourceIdEncoding::EncodeSimple(ToString(TabletID)),
                .SeqNo = 0, // we don't use SeqNo because we disable deduplication
                .PartNo = 0,
                .TotalParts = 1,
                .TotalSize = static_cast<ui32>(heartbeat->Data.size()),
                .CreateTimestamp = CurrentTimestamp.MilliSeconds(),
                .ReceiveTimestamp = CurrentTimestamp.MilliSeconds(),
                .DisableDeduplication = true,
                .WriteTimestamp = CurrentTimestamp.MilliSeconds(),
                .Data = heartbeat->Data,
                .UncompressedSize = 0,
                .PartitionKey = {},
                .ExplicitHashKey = {},
                .External = false,
                .IgnoreQuotaDeadline = true,
                .HeartbeatVersion = std::nullopt,
            }, std::nullopt};

            WriteInflightSize += heartbeat->Data.size();
            ExecRequest(hbMsg, *Parameters, request);

            LastEmittedHeartbeat = heartbeat->Version;
        }
    }


    UpdateWriteBufferIsFullState(ctx.Now());

    if (!NewHead.Batches.empty() && !NewHead.Batches.back().Packed) {
        NewHead.Batches.back().Pack();
        NewHead.PackedSize += NewHead.Batches.back().GetPackedSize(); //add real packed size for this blob

        NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
        NewHead.PackedSize -= NewHead.Batches.back().GetUnpackedSize();
    }

    Y_ABORT_UNLESS((Parameters->HeadCleared ? 0 : Head.PackedSize) + NewHead.PackedSize <= MaxBlobSize); //otherwise last PartitionedBlob.Add must compact all except last cl
    MaxWriteResponsesSize = Max<ui32>(MaxWriteResponsesSize, Responses.size());

    HeadCleared = Parameters->HeadCleared;
}

void TPartition::RequestQuotaForWriteBlobRequest(size_t dataSize, ui64 cookie) {
    LOG_DEBUG_S(
            TActivationContext::AsActorContext(), NKikimrServices::PERSQUEUE,
            "Send write quota request." <<" Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << "." <<
            " Amount: " << dataSize << "." <<
            " Cookie: " << cookie
    );
    Send(WriteQuotaTrackerActor, new TEvPQ::TEvRequestQuota(cookie, nullptr));
}

bool TPartition::WaitingForPreviousBlobQuota() const {
    return TopicQuotaRequestCookie != 0;
}

bool TPartition::WaitingForSubDomainQuota(const TActorContext& /*ctx*/, const ui64 withSize) const {
    if (!SubDomainOutOfSpace || !AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota()) {
        return false;
    }

    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == Config.GetMeteringMode()) {
        // We allow one message to be written even when the SubDomainOutOfSpace.
        return withSize > 0 || Size() > 0;
    }

    return UserDataSize() + withSize > ReserveSize();
}

void TPartition::RequestBlobQuota(size_t quotaSize)
{
    PQ_LOG_T("TPartition::RequestBlobQuota.");

    Y_ABORT_UNLESS(!WaitingForPreviousBlobQuota());

    TopicQuotaRequestCookie = NextTopicWriteQuotaRequestCookie++;
    BlobQuotaSize = quotaSize;
    RequestQuotaForWriteBlobRequest(quotaSize, TopicQuotaRequestCookie);
}

void TPartition::ConsumeBlobQuota()
{
    if (!WriteQuotaTrackerActor) {
        return;
    }

    Y_ABORT_UNLESS(TopicQuotaRequestCookie != 0);
    Send(WriteQuotaTrackerActor, new TEvPQ::TEvConsumed(BlobQuotaSize, TopicQuotaRequestCookie, {}));
}

} // namespace NKikimr::NPQ
