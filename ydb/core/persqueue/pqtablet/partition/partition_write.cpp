#include "autopartitioning_manager.h"
#include "partition_util.h"
#include "partition.h"

#include <ydb/core/persqueue/pqtablet/cache/read.h>
#include <ydb/core/persqueue/pqtablet/common/constants.h>
#include <ydb/core/persqueue/pqtablet/common/event_helpers.h>
#include <ydb/core/persqueue/pqtablet/common/logging.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>

#include <ydb/core/protos/counters_pq.pb.h>
#include <ydb/core/protos/msgbus.pb.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/quoter/public/quoter.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/lib/base/msgbus.h>
#include <library/cpp/html/pcdata/pcdata.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/folder/path.h>
#include <util/string/escape.h>
#include <util/system/byteorder.h>

namespace NKikimr::NPQ {

static const TDuration SubDomainQuotaWaitDurationMs = TDuration::Seconds(60);

static constexpr NPersQueue::NErrorCode::EErrorCode InactivePartitionErrorCode = NPersQueue::NErrorCode::WRITE_ERROR_PARTITION_INACTIVE;

void TPartition::ReplyOwnerOk(const TActorContext& ctx, const ui64 dst, const TString& cookie, ui64 seqNo, NWilson::TSpan& span) {
    LOG_D("TPartition::ReplyOwnerOk. Partition: " << Partition);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst, false);
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

    ctx.Send(TabletActorId, response.Release());
    span.EndOk();
}

void TPartition::ReplyWrite(
    const TActorContext& ctx, const ui64 dst, const TString& sourceId, const ui64 seqNo, const ui16 partNo, const ui16 totalParts,
    const ui64 offset, const TInstant writeTimestamp, bool already, const ui64 maxSeqNo,
    const TDuration partitionQuotedTime, const TDuration topicQuotedTime, const TDuration queueTime, const TDuration writeTime, NWilson::TSpan& span) {

    LOG_D("TPartition::ReplyWrite. Partition: " << Partition);

    PQ_ENSURE(offset <= (ui64)Max<i64>())("Offset is too big", offset);
    PQ_ENSURE(seqNo <= (ui64)Max<i64>())("SeqNo is too big", seqNo);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst, false);
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

    write->SetWrittenInTx(IsSupportive());

    ctx.Send(TabletActorId, response.Release());
    span.EndOk();
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
    LOG_T("TPartition::HandleOnWrite TEvUpdateAvailableSize.");

    UpdateAvailableSize(ctx);
}

void TPartition::ProcessChangeOwnerRequest(TAutoPtr<TEvPQ::TEvChangeOwner> ev, const TActorContext& ctx) {
    LOG_T("TPartition::ProcessChangeOwnerRequest.");

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

        PQ_ENSURE(ReservedSize >= it->second.ReservedSize);
        ReservedSize -= it->second.ReservedSize;

        it->second.GenerateCookie(owner, ev->PipeClient, ev->Sender, TopicName(), Partition, ctx);//will change OwnerCookie
        //cookie is generated. but answer will be sent when all inflight writes will be done - they in the same queue 'Requests'
        EmplacePendingRequest(TOwnershipMsg{ev->Cookie, it->second.OwnerCookie}, {}, ctx);
        TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
        UpdateWriteBufferIsFullState(ctx.Now());
        ProcessReserveRequests(ctx);
    } else {
        it->second.WaitToChangeOwner.push_back(THolder<TEvPQ::TEvChangeOwner>(ev.Release()));
    }
}


THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator TPartition::DropOwner(THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator& it, const TActorContext& ctx) {
    LOG_D("TPartition::DropOwner.");

    PQ_ENSURE(ReservedSize >= it->second.ReservedSize);
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
    LOG_T("TPartition::HandleOnWrite TEvChangeOwner.");

    bool res = OwnerPipes.insert(ev->Get()->PipeClient).second;
    PQ_ENSURE(res);
    WaitToChangeOwner.push_back(ev->Release());
    ProcessChangeOwnerRequests(ctx);
}

void TPartition::ProcessReserveRequests(const TActorContext& ctx) {
    LOG_T("TPartition::ProcessReserveRequests.");

    const ui64 maxWriteInflightSize = Config.GetPartitionConfig().GetMaxWriteInflightSize();

    while (!ReserveRequests.empty()) {
        const TString& ownerCookie = ReserveRequests.front()->OwnerCookie;
        const TStringBuf owner = TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie);
        const ui64& size = ReserveRequests.front()->Size;
        const ui64& cookie = ReserveRequests.front()->Cookie;
        const bool& lastRequest = ReserveRequests.front()->LastRequest;

        if (!IsActive()) {
            ReplyOk(ctx, cookie);
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
            LOG_D("Reserve processing: maxWriteInflightSize riched. Partition: " << Partition);
            break;
        }

        if (WaitingForSubDomainQuota(currentSize)) {
            LOG_D("Reserve processing: SubDomainOutOfSpace. Partition: " << Partition);
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
    LOG_T("TPartition::HandleOnWrite TEvReserveBytes.");

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

ui64 CalculateReplyOffset(bool already, bool kafkaDeduplication, ui64 maxOffset, ui64 offset, ui64 maxSeqNo, ui64 seqNo) {
    if (!already) {
        // If it's not a duplicate message, return its offset.
        return offset;
    }

    if (!kafkaDeduplication) {
        // If it's a duplicate message, return the maxOffset, if we're not dealing with Kafka protocol.
        return maxOffset;
    }

    // At this point we're working with Kafka protocol deduplication.
    // SeqNos in Kafka are of int32 type and may overflow, i.e. the next seqno after int32max is 0.

    // Try to return offset of the message, given the maxOffset, maxSeqNo and seqNo.
    // The basic formula is (maxOffset - (maxSeqNo - seqNo)), but we need to handle various overflows.

    if (maxSeqNo < seqNo) {
        maxSeqNo += 1ul << 31;
    }

    auto diff = maxSeqNo - seqNo;

    if (maxOffset < diff) {
        return 0;
    }

    return maxOffset - diff;
}

void TPartition::AnswerCurrentWrites(const TActorContext& ctx) {
    LOG_T("TPartition::AnswerCurrentWrites. Responses.size()=" << Responses.size());
    const auto now = ctx.Now();

    ui64 offset = BlobEncoder.EndOffset;
    while (!Responses.empty()) {
        auto& response = Responses.front();

        const TDuration queueTime = response.QueueTime;
        const TDuration writeTime = now - response.WriteTimeBaseline;

        if (response.IsWrite()) {
            const auto& writeResponse = response.GetWrite();
            const TString& s = writeResponse.Msg.SourceId;
            const ui64& seqNo = writeResponse.Msg.SeqNo;
            const ui16& partNo = writeResponse.Msg.PartNo;
            const ui16& totalParts = writeResponse.Msg.TotalParts;
            const TMaybe<i16>& producerEpoch = writeResponse.Msg.ProducerEpoch;
            const TMaybe<ui64>& wrOffset = writeResponse.Offset;

            bool already = false;

            auto it = SourceIdStorage.GetInMemorySourceIds().find(s);

            ui64 maxSeqNo = 0;
            ui64 maxOffset = 0;

            if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
                maxSeqNo = std::max(it->second.SeqNo, writeResponse.InitialSeqNo.value_or(0));
                maxOffset = it->second.Offset;
                already = maxSeqNo >= seqNo && !writeResponse.Msg.DisableDeduplication ||
                    (
                        writeResponse.Msg.EnableKafkaDeduplication &&
                        writeResponse.Msg.ProducerEpoch.Defined() &&
                        writeResponse.Msg.ProducerEpoch == it->second.ProducerEpoch &&
                        NKafka::IsDuplicate(maxSeqNo, seqNo)
                    );
            } else if (writeResponse.InitialSeqNo) {
                maxSeqNo = writeResponse.InitialSeqNo.value();
                if (maxSeqNo >= seqNo && !writeResponse.Msg.DisableDeduplication) {
                    already = true;
                }
            }

            already = already || writeResponse.DeduplicatedByMessageId;

            if (!already) {
                if (wrOffset) {
                    PQ_ENSURE(*wrOffset >= offset);
                    offset = *wrOffset;
                }
            }

            if (!already && partNo + 1 == totalParts) {
                if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
                    PQ_ENSURE(!writeResponse.Msg.HeartbeatVersion);
                    TabletCounters.Cumulative()[COUNTER_PQ_SID_CREATED].Increment(1);
                    SourceIdStorage.RegisterSourceId(s, seqNo, offset, CurrentTimestamp, producerEpoch);
                } else if (const auto& hbVersion = writeResponse.Msg.HeartbeatVersion) {
                    SourceIdStorage.RegisterSourceId(s, it->second.Updated(
                        seqNo, offset, CurrentTimestamp, THeartbeat{*hbVersion, writeResponse.Msg.Data}, producerEpoch));
                } else {
                    SourceIdStorage.RegisterSourceId(s, it->second.Updated(
                        seqNo, offset, CurrentTimestamp, producerEpoch));
                }

                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_OK].Increment(1);
            }

            ui64 replyOffset = CalculateReplyOffset(already, writeResponse.Msg.EnableKafkaDeduplication, maxOffset, offset, maxSeqNo, seqNo);

            ReplyWrite(
                ctx, writeResponse.Cookie, s, seqNo, partNo, totalParts,
                replyOffset, CurrentTimestamp, already, maxSeqNo,
                PartitionQuotaWaitTimeForCurrentBlob, TopicQuotaWaitTimeForCurrentBlob, queueTime, writeTime, response.Span
            );

            LOG_D("Answering for message sourceid: '" << EscapeC(s)
                    << "', Topic: '" << TopicName()
                    << "', Partition: " << Partition
                    << ", SeqNo: " << seqNo << ", partNo: " << partNo
                    << ", Offset: " << offset << " is " << (already ? "already written" : "stored on disk")
            );

            if (PartitionWriteQuotaWaitCounter && !writeResponse.Internal) {
                PartitionWriteQuotaWaitCounter->IncFor(PartitionQuotaWaitTimeForCurrentBlob.MilliSeconds());
            }
            if (!already && partNo + 1 == totalParts && !writeResponse.Msg.HeartbeatVersion) {
                ++offset;
            }
        } else if (response.IsOwnership()) {
            const auto& r = response.GetOwnership();
            const TString& ownerCookie = r.OwnerCookie;
            auto it = Owners.find(TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie));
            if (it != Owners.end() && it->second.OwnerCookie == ownerCookie) {
                auto sit = SourceIdStorage.GetInMemorySourceIds().find(NSourceIdEncoding::EncodeSimple(it->first));
                auto seqNo = sit == SourceIdStorage.GetInMemorySourceIds().end() ? 0 : sit->second.SeqNo;
                ReplyOwnerOk(ctx, response.GetCookie(), ownerCookie, seqNo, response.Span);
            } else {
                ReplyError(ctx, response.GetCookie(), NPersQueue::NErrorCode::WRONG_COOKIE, "new GetOwnership request is dropped already", response.Span);
            }
        } else if (response.IsRegisterMessageGroup()) {
            const auto& body = response.GetRegisterMessageGroup().Body;

            TMaybe<TPartitionKeyRange> keyRange;
            if (body.KeyRange) {
                keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
            }

            PQ_ENSURE(body.AssignedOffset);
            SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange));
            ReplyOk(ctx, response.GetCookie(), response.Span);
        } else if (response.IsDeregisterMessageGroup()) {
            const auto& body = response.GetDeregisterMessageGroup().Body;

            SourceIdStorage.DeregisterSourceId(body.SourceId);
            ReplyOk(ctx, response.GetCookie(), response.Span);
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

                PQ_ENSURE(body.AssignedOffset);
                SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange), true);
            }

            ReplyOk(ctx, response.GetCookie(), response.Span);
        } else {
            Y_ABORT("Unexpected message");
        }
        Responses.pop_front();
    }
    TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
    PartitionQuotaWaitTimeForCurrentBlob = TDuration::Zero();
}

void TPartition::SyncMemoryStateWithKVState(const TActorContext& ctx) {
    LOG_T("TPartition::SyncMemoryStateWithKVState.");

    BlobEncoder.SyncHeadKeys();
    BlobEncoder.SyncNewHeadKey();

    if (BlobEncoder.IsNothingWritten()) { //Nothing writed at all
        return;
    }

    PQ_ENSURE(BlobEncoder.EndOffset == BlobEncoder.Head.GetNextOffset())("EndOffset", GetEndOffset())("NextOffset", BlobEncoder.Head.GetNextOffset());

    // a) !CompactedKeys.empty() && NewHead.PackedSize == 0
    // b) !CompactedKeys.empty() && NewHead.PackedSize != 0
    // c)  CompactedKeys.empty() && NewHead.PackedSize != 0
    if (!BlobEncoder.CompactedKeys.empty() || BlobEncoder.Head.PackedSize == 0) { //has compactedkeys or head is already empty
        BlobEncoder.SyncHeadFromNewHead();
    }

    BlobEncoder.SyncDataKeysBody(ctx.Now(),
                                 [this](const TString& key){ return MakeBlobKeyToken(key); },
                                 BlobEncoder.StartOffset,
                                 GapOffsets,
                                 GapSize);
    BlobEncoder.SyncHeadFastWrite(BlobEncoder.StartOffset, BlobEncoder.EndOffset);

    EndWriteTimestamp = PendingWriteTimestamp;

    BlobEncoder.ResetNewHead(BlobEncoder.EndOffset);

    CheckHeadConsistency();

    UpdateUserInfoEndOffset(ctx.Now());
}

void TPartition::OnHandleWriteResponse(const TActorContext& ctx)
{
    KVWriteInProgress = false;

    for (auto& span : TxForPersistSpans) {
        span.End();
    }
    TxForPersistSpans.clear();

    if (DeletePartitionState == DELETION_IN_PROCESS) {
        // before deleting an supportive partition, it is necessary to summarize its work
        HandleWakeup(ctx);
    }

    CurrentPersistRequestSpan.End();
    CurrentPersistRequestSpan = NWilson::TSpan();
    OnProcessTxsAndUserActsWriteComplete(ctx);
    HandleWriteResponse(ctx);
    ProcessTxsAndUserActs(ctx);
    TryRunCompaction();

    MessageIdDeduplicator.Commit();

    if (DeletePartitionState == DELETION_IN_PROCESS) {
        DestroyActor(ctx);
    }
}

void TPartition::Handle(TEvPQ::TEvHandleWriteResponse::TPtr&, const TActorContext& ctx)
{
    LOG_D("Received TPartition::Handle TEvHandleWriteResponse.");
    OnHandleWriteResponse(ctx);
}

void TPartition::UpdateAfterWriteCounters(bool writeComplete) {
    if (IsSupportive() == writeComplete) {
        // If supportive - update counters only prior to write, otherwise - only after writes;
        return;
    }

    if (BytesWrittenPerPartition) {
        BytesWrittenPerPartition->Add(WriteNewSize);
    }
    BytesWrittenGrpc.Inc(WriteNewSizeInternal);
    BytesWrittenTotal.Inc(WriteNewSize);
    BytesWrittenUncompressed.Inc(WriteNewSizeUncompressed);
    if (BytesWrittenComp) {
        BytesWrittenComp.Inc(WriteCycleSize);
    }

    if (MessagesWrittenPerPartition) {
        MessagesWrittenPerPartition->Add(WriteNewMessages);
    }
    MsgsWrittenGrpc.Inc(WriteNewMessagesInternal);
    MsgsWrittenTotal.Inc(WriteNewMessages);
}

void TPartition::HandleWriteResponse(const TActorContext& ctx) {
    LOG_T("TPartition::HandleWriteResponse.");
    if (!HaveWriteMsg) {
        return;
    }
    HaveWriteMsg = false;

    const auto now = ctx.Now();

    for (auto& [sourceId, info] : TxSourceIdForPostPersist) {
        auto it = SourceIdStorage.GetInMemorySourceIds().find(sourceId);
        if (it.IsEnd()) {
            SourceIdStorage.RegisterSourceId(sourceId, info.SeqNo, info.Offset, now, info.KafkaProducerEpoch);
        } else {
            ui64 seqNo = std::max(info.SeqNo, it->second.SeqNo);
            SourceIdStorage.RegisterSourceId(sourceId, it->second.Updated(seqNo, info.Offset, now, info.KafkaProducerEpoch));
        }
    }
    TxSourceIdForPostPersist.clear();
    TxInflightMaxSeqNoPerSourceId.clear();

    if (UserActionAndTransactionEvents.empty()) {
        WriteInfosToTx.clear();
    }
    ui64 prevEndOffset = BlobEncoder.EndOffset;

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
    UpdateAvgWriteBytes(WriteNewSize, now);
    UpdateAvgWriteBytes(WriteNewSizeFromSupportivePartitions, now);

    for (auto& avg : AvgQuotaBytes) {
        avg.Update(WriteNewSize, now);
        avg.Update(WriteNewSizeFromSupportivePartitions, now);
    }

    LOG_D("TPartition::HandleWriteResponse " <<
             "writeNewSize# " << WriteNewSize <<
             " WriteNewSizeFromSupportivePartitions# " << WriteNewSizeFromSupportivePartitions);

    if (SupportivePartitionTimeLag) {
        SupportivePartitionTimeLag->UpdateTimestamp(now.MilliSeconds());
    }

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeFull = 0;
    WriteNewSizeInternal = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewSizeUncompressedFull = 0;
    WriteNewMessages = 0;
    WriteNewMessagesInternal = 0;
    WriteNewSizeFromSupportivePartitions = 0;
    UpdateWriteBufferIsFullState(now);

    AnswerCurrentWrites(ctx);
    SyncMemoryStateWithKVState(ctx);
    NotifyEndOffsetChanged();

    ChangeScaleStatusIfNeeded(AutopartitioningManager->GetScaleStatus(ScaleStatus));

    //if EndOffset changed there could be subscriptions witch could be completed
    TVector<std::pair<TReadInfo, ui64>> reads = Subscriber.GetReads(BlobEncoder.EndOffset);
    for (auto& read : reads) {
        PQ_ENSURE(BlobEncoder.EndOffset > read.first.Offset);
        ProcessRead(ctx, std::move(read.first), read.second, true);
    }
    //same for read requests
    ProcessHasDataRequests(ctx);

    ProcessTimestampsForNewData(prevEndOffset, ctx);
}

void TPartition::UpdateAvgWriteBytes(ui64 size, const TInstant& now)
{
    for (auto& avg : AvgWriteBytes) {
        avg.Update(size, now);
    }
}

void TPartition::ChangeScaleStatusIfNeeded(NKikimrPQ::EScaleStatus scaleStatus) {
    auto now = TInstant::Now();
    if (scaleStatus == ScaleStatus || LastScaleRequestTime + TDuration::Seconds(SCALE_REQUEST_REPEAT_MIN_SECONDS) > now) {
        return;
    }

    ScaleStatus = scaleStatus;
    SplitBoundary.Clear();
    LastScaleRequestTime = now;

    auto ev = MakeHolder<TEvPQ::TEvPartitionScaleStatusChanged>(Partition.OriginalPartitionId, ScaleStatus);
    if (ScaleStatus == NKikimrPQ::EScaleStatus::NEED_SPLIT) {
        auto splitBoundary = AutopartitioningManager->SplitBoundary();
        if (splitBoundary) {
            ev->Record.SetSplitBoundary(*splitBoundary);
            SplitBoundary = *splitBoundary;
        }
    }
    Send(TabletActorId, std::move(ev));
}

void TPartition::HandleOnWrite(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    LOG_D("Received TPartition::TEvWrite");

    if (!CanEnqueue()) {
        ReplyError(ctx, ev->Get()->Cookie, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return;
    }

    ui32 sz = std::accumulate(ev->Get()->Msgs.begin(), ev->Get()->Msgs.end(), 0u, [](ui32 sum, const TEvPQ::TEvWrite::TMsg& msg) {
        return sum + msg.Data.size();
    });

    bool mirroredPartition = MirroringEnabled(Config);

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
        PQ_ENSURE(!msg.SourceId.empty() || ev->Get()->IsDirectWrite || msg.DisableDeduplication);
        PQ_ENSURE(!msg.Data.empty());

        if (msg.SeqNo > (ui64)Max<i64>()) {
            LOG_E( "Request to write wrong SeqNo. Partition "
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
        TString sourceId = msg.SourceId;
        bool needToChangeOffset = msg.PartNo + 1 == msg.TotalParts;
        EmplacePendingRequest(TWriteMsg{ev->Get()->Cookie, offset, std::move(msg), ev->Get()->InitialSeqNo}, NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId(ev->TraceId), "Topic.Partition.WriteMessage"), ctx);
        PendingRequests.back().Span.Attribute("source_id", std::move(sourceId));
        if (KVWriteInProgress) { // We are currently writing previous blob
            PendingRequests.back().WaitPreviousWriteSpan = NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId(PendingRequests.back().Span.GetTraceId()), "Topic.Partition.WaitPreviousWrite");
        }
        if (offset && needToChangeOffset) {
            ++*offset;
        }
    }
    if (WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota()) {
        SetDeadlinesForWrites(ctx);
    }
    WriteInflightSize += size;

    // TODO: remove decReservedSize == 0
    PQ_ENSURE(size <= decReservedSize || decReservedSize == 0);
    UpdateWriteBufferIsFullState(ctx.Now());

}

void TPartition::HandleOnIdle(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    LOG_T("TPartition::HandleOnWrite TEvRegisterMessageGroup.");

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

    EmplacePendingRequest(TRegisterMessageGroupMsg(*ev->Get()), NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId(ev->TraceId), "Topic.Partition.RegisterMessageGroup"), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    LOG_T("TPartition::HandleOnWrite TEvDeregisterMessageGroup.");

    const auto& body = ev->Get()->Body;

    auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
    if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
            "SourceId doesn't exist");
    }

    EmplacePendingRequest(TDeregisterMessageGroupMsg(*ev->Get()), NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId(ev->TraceId), "Topic.Partition.DeregisterMessageGroup"), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx)
{
    HandleOnWrite(ev, ctx);
    HandlePendingRequests(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx) {
    LOG_T("TPartition::HandleOnWrite TEvSplitMessageGroup.");

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

    EmplacePendingRequest(std::move(msg), NWilson::TSpan(TWilsonTopic::TopicDetailed, NWilson::TTraceId(ev->TraceId), "Topic.Partition.SplitMessageGroup"), ctx);
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
    LOG_T("TPartition::ProcessChangeOwnerRequests.");

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
    ScheduleReplyError(p.Cookie, false, errorCode, errorStr);
    for (auto it = Owners.begin(); it != Owners.end();) {
        it = DropOwner(it, ctx);
    }
    StartProcessChangeOwnerRequests(ctx);
}

TPartition::EProcessResult TPartition::PreProcessRequest(TRegisterMessageGroupMsg& msg,
                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    if (!CanWrite()) {
        ScheduleReplyError(msg.Cookie, false, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(msg.Cookie, false,
                           NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    if (TxAffectedSourcesIds.contains(msg.Body.SourceId)) {
        return EProcessResult::Blocked;
    }
    affectedSourceIdsAndConsumers.WriteSourcesIds.push_back(msg.Body.SourceId);
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

TPartition::EProcessResult TPartition::PreProcessRequest(TDeregisterMessageGroupMsg& msg,
                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    if (!CanWrite()) {
        ScheduleReplyError(msg.Cookie, false, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(msg.Cookie, false, NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    if (TxAffectedSourcesIds.contains(msg.Body.SourceId)) {
        return EProcessResult::Blocked;
    }
    affectedSourceIdsAndConsumers.WriteSourcesIds.push_back(msg.Body.SourceId);
    return EProcessResult::Continue;
}

void TPartition::ExecRequest(TDeregisterMessageGroupMsg& msg, ProcessParameters& parameters) {
    parameters.SourceIdBatch.DeregisterSourceId(msg.Body.SourceId);
}


TPartition::EProcessResult TPartition::PreProcessRequest(TSplitMessageGroupMsg& msg,
                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    if (!CanWrite()) {
        ScheduleReplyError(msg.Cookie, false, InactivePartitionErrorCode,
            TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }
    if (DiskIsFull) {
        ScheduleReplyError(msg.Cookie, false,
                           NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }
    for (auto& body : msg.Registrations) {
        if (TxAffectedSourcesIds.contains(body.SourceId)) {
            return EProcessResult::Blocked;
        }
        affectedSourceIdsAndConsumers.WriteSourcesIds.push_back(body.SourceId);
    }
    for (auto& body : msg.Deregistrations) {
        if (TxAffectedSourcesIds.contains(body.SourceId)) {
            return EProcessResult::Blocked;
        }
        affectedSourceIdsAndConsumers.WriteSourcesIds.push_back(body.SourceId);
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

TPartition::EProcessResult TPartition::PreProcessRequest(TWriteMsg& p,
                                                         TAffectedSourceIdsAndConsumers& affectedSourceIdsAndConsumers)
{
    if (!CanWrite()) {
        WriteInflightSize -= p.Msg.Data.size();
        ScheduleReplyError(p.Cookie, false, InactivePartitionErrorCode,
                           TStringBuilder() << "Write to inactive partition " << Partition.OriginalPartitionId);
        return EProcessResult::ContinueDrop;
    }

    if (DiskIsFull) {
        WriteInflightSize -= p.Msg.Data.size();
        ScheduleReplyError(p.Cookie, false,
                           NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL,
                           "Disk is full");
        return EProcessResult::ContinueDrop;
    }

    if (TxAffectedSourcesIds.contains(p.Msg.SourceId)) {
        return EProcessResult::Blocked;
    }
    auto inflightMaxSeqNo = TxInflightMaxSeqNoPerSourceId.find(p.Msg.SourceId);

    if (!inflightMaxSeqNo.IsEnd()) {
        if (SeqnoViolation(inflightMaxSeqNo->second.KafkaProducerEpoch, inflightMaxSeqNo->second.SeqNo, p.Msg.ProducerEpoch, p.Msg.SeqNo)) {
            return EProcessResult::Blocked;
        }
    }
    affectedSourceIdsAndConsumers.WriteSourcesIds.push_back(p.Msg.SourceId);

    return EProcessResult::Continue;
}

struct TPartitionsPrivateAddCmdWriteTag {};

void TPartition::AddCmdWrite(const std::optional<TPartitionedBlob::TFormedBlobInfo>& newWrite,
                             TEvKeyValue::TEvRequest* request,
                             TInstant creationUnixTime,
                             const TActorContext& ctx,
                             bool includeToWriteCycle) {
    // Y_ASSERT(creationUnixTime > 0);  // TODO: remove test cases from the 1970s and return check
    AddCmdWriteImpl(newWrite, request, creationUnixTime, ctx, includeToWriteCycle, {});
}

void TPartition::AddCmdWriteImpl(const std::optional<TPartitionedBlob::TFormedBlobInfo>& newWrite,
                             TEvKeyValue::TEvRequest* request,
                             TInstant creationUnixTime,
                             const TActorContext& ctx,
                             bool includeToWriteCycle,
                             TPartitionsPrivateAddCmdWriteTag)
{
    auto write = request->Record.AddCmdWrite();
    write->SetKey(newWrite->Key.Data(), newWrite->Key.Size());
    write->SetValue(newWrite->Value);
    if (creationUnixTime != TInstant::Zero()) {
        // note: The time is rounded to second precision.
        write->SetCreationUnixTime(creationUnixTime.Seconds());
    }
    //PQ_ENSURE(newWrite->Key.IsFastWrite());
    auto channel = GetChannel(NextChannel(newWrite->Key.HasSuffix(), newWrite->Value.size()));
    write->SetStorageChannel(channel);
    write->SetTactic(AppData(ctx)->PQConfig.GetTactic());

    TKey resKey = newWrite->Key;
    resKey.SetType(TKeyPrefix::TypeData);
    if (includeToWriteCycle)
        WriteCycleSize += newWrite->Value.size();
}

void TPartition::AddCmdWriteWithDeferredTimestamp(const std::optional<TPartitionedBlob::TFormedBlobInfo>& newWrite,
                             TEvKeyValue::TEvRequest* request,
                             const TActorContext& ctx,
                             bool includeToWriteCycle)
{
    AddCmdWriteImpl(newWrite, request, TInstant::Zero(), ctx, includeToWriteCycle, {});
}

void TPartition::RenameFormedBlobs(const std::deque<TPartitionedBlob::TRenameFormedBlobInfo>& formedBlobs,
                                   TProcessParametersBase& parameters,
                                   ui32 curWrites,
                                   TEvKeyValue::TEvRequest* request,
                                   TPartitionBlobEncoder& zone,
                                   const TActorContext& ctx)
{
    for (ui32 i = 0; i < formedBlobs.size(); ++i) {
        const auto& x = formedBlobs[i];
        if (i + curWrites < formedBlobs.size()) { //this KV pair is already writed, rename needed
            auto rename = request->Record.AddCmdRename();
            rename->SetOldKey(x.OldKey.ToString());
            rename->SetNewKey(x.NewKey.ToString());
            if (x.CreationUnixTime != TInstant::Zero()) {
                rename->SetCreationUnixTime(x.CreationUnixTime.Seconds());
            }
        }
        if (!zone.DataKeysBody.empty() && zone.CompactedKeys.empty()) {
            PQ_ENSURE(zone.DataKeysBody.back().Key.GetOffset() + zone.DataKeysBody.back().Key.GetCount() <= x.NewKey.GetOffset())
                ("LAST KEY", zone.DataKeysBody.back().Key.ToString())
                ("HeadOffset", zone.Head.Offset)
                ("NEWKEY", x.NewKey.ToString());
        }
        LOG_D("writing blob: topic '" << TopicName() << "' partition " << Partition <<
                 " old key " << x.OldKey.ToString() << " new key " << x.NewKey.ToString() <<
                 " size " << x.Size << " WTime " << ctx.Now().MilliSeconds());

        zone.CompactedKeys.emplace_back(x.NewKey, x.Size);
    }

    if (!formedBlobs.empty()) {
        parameters.HeadCleared = true;

        // zone.ResetNewHead ???
        zone.NewHead.Clear();
        zone.NewHead.Offset = zone.PartitionedBlob.GetOffset();
        zone.NewHead.PartNo = zone.PartitionedBlob.GetHeadPartNo();
        zone.NewHead.PackedSize = 0;
    }
}

ui32 TPartition::RenameTmpCmdWrites(TEvKeyValue::TEvRequest* request)
{
    ui32 curWrites = 0;
    for (ui32 i = 0; i < request->Record.CmdWriteSize(); ++i) { //change keys for yet to be writed KV pairs
        const auto& k = request->Record.GetCmdWrite(i).GetKey();
        if ((k.front() != TKeyPrefix::TypeTmpData) &&
            (k.front() != TKeyPrefix::ServiceTypeTmpData)) {
            continue;
        }
        // оптимизация. можно не создавать ключ. достаточно проверить первый символ и перевести с учётом типа партиции
        auto key = TKey::FromString(k);
        if (key.GetType() == TKeyPrefix::TypeTmpData) {
            key.SetType(TKeyPrefix::TypeData);
            request->Record.MutableCmdWrite(i)->SetKey(key.Data(), key.Size());
            ++curWrites;
        }
    }
    return curWrites;
}

void TPartition::TryCorrectStartOffset(TMaybe<ui64> offset)
{
    auto isEncoderEmpty = [](TPartitionBlobEncoder& encoder) {
        return (!encoder.Head.GetCount() && !encoder.NewHead.GetCount() && encoder.IsEmpty());
    };

    if (isEncoderEmpty(CompactionBlobEncoder) && isEncoderEmpty(BlobEncoder) && offset) {
        BlobEncoder.StartOffset = *offset;
        CompactionBlobEncoder.StartOffset = *offset;
    }
}

bool TPartition::ExecRequest(TWriteMsg& p, ProcessParameters& parameters, TEvKeyValue::TEvRequest* request) {
    Y_DEBUG_ABORT_UNLESS(WriteInflightSize >= p.Msg.Data.size(),
                         "PQ %" PRIu64 ", Partition {%" PRIu32 ", %" PRIu32 "}, WriteInflightSize=%" PRIu64 ", p.Msg.Data.size=%" PRISZT,
                         TabletId, Partition.OriginalPartitionId, Partition.InternalPartitionId,
                         WriteInflightSize, p.Msg.Data.size());
    WriteInflightSize -= p.Msg.Data.size();

    const auto& ctx = ActorContext();

    ui64& curOffset = parameters.CurOffset;
    auto& sourceIdBatch = parameters.SourceIdBatch;
    auto sourceId = sourceIdBatch.GetSource(p.Msg.SourceId);

    AutopartitioningManager->OnWrite(p.Msg.SourceId, p.Msg.Data.size());

    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_RECEIVE_QUEUE].IncrementFor(ctx.Now().MilliSeconds() - p.Msg.ReceiveTimestamp);
    //check already written

    ui64 poffset = p.Offset.GetOrElse(curOffset);

    LOG_T("Topic '" << TopicName() << "' partition " << Partition
            << " process write for '" << EscapeC(p.Msg.SourceId) << "'"
            << " DisableDeduplication=" << p.Msg.DisableDeduplication
            << " SeqNo=" << p.Msg.SeqNo
            << " LocalSeqNo=" << sourceId.SeqNo()
            << " InitialSeqNo=" << p.InitialSeqNo
            << " EnableKafkaDeduplication=" << p.Msg.EnableKafkaDeduplication
            << " ProducerEpoch=" << p.Msg.ProducerEpoch
    );

    if (p.Msg.EnableKafkaDeduplication &&
        sourceId.ProducerEpoch().has_value() && sourceId.ProducerEpoch().value().Defined() &&
        p.Msg.ProducerEpoch.Defined()
    ) {
        auto lastEpoch = *sourceId.ProducerEpoch().value();
        auto lastSeqNo = *sourceId.SeqNo();
        auto messageEpoch = *p.Msg.ProducerEpoch;
        auto messageSeqNo = p.Msg.SeqNo;

        if (auto res = NKafka::CheckDeduplication(lastEpoch, lastSeqNo, messageEpoch, messageSeqNo); res != NKafka::ECheckDeduplicationResult::OK) {
            auto [error, message] = NKafka::MakeDeduplicationError(
                res, TopicName(), Partition.OriginalPartitionId, p.Msg.SourceId, poffset,
                lastEpoch, lastSeqNo, messageEpoch, messageSeqNo
            );

            switch (res) {
            case NKafka::ECheckDeduplicationResult::OK:
                // Continue processing the message.
                break;
            case NKafka::ECheckDeduplicationResult::DUPLICATE_SEQUENCE_NUMBER:
                return true;

            case NKafka::ECheckDeduplicationResult::INVALID_PRODUCER_EPOCH:
                [[fallthrough]];
            case NKafka::ECheckDeduplicationResult::OUT_OF_ORDER_SEQUENCE_NUMBER: {
                CancelOneWriteOnWrite(ctx, message, p, error);
                return false;
            }
            }
        }
    }

    if (!p.Msg.DisableDeduplication &&
        ((sourceId.SeqNo() && *sourceId.SeqNo() >= p.Msg.SeqNo) || (p.InitialSeqNo && p.InitialSeqNo.value() >= p.Msg.SeqNo))
    ) {
        if (poffset >= curOffset) {
            LOG_D("Already written message. Topic: '" << TopicName()
                    << "' Partition: " << Partition << " SourceId: '" << EscapeC(p.Msg.SourceId)
                    << "'. Message seqNo: " << p.Msg.SeqNo
                    << ". InitialSeqNo: " << p.InitialSeqNo
                    << ". Committed seqNo: " << sourceId.CommittedSeqNo()
                    << ". Writing seqNo: " << sourceId.UpdatedSeqNo()
                    << ". EndOffset: " << BlobEncoder.EndOffset << ". CurOffset: " << curOffset << ". Offset: " << poffset
            );
            Y_ENSURE(!p.Internal); // No Already for transactions;
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ALREADY].Increment(1);
            MsgsDiscarded.Inc();
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ALREADY].Increment(p.Msg.Data.size());
            BytesDiscarded.Inc(p.Msg.Data.size());
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

        LOG_D("Topic '" << TopicName() << "' partition " << Partition
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

    PQ_ENSURE(poffset >= curOffset);

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

    auto deduplicationResult = DeduplicateByMessageId(p.Msg, curOffset);
    if (deduplicationResult) {
        LOG_D("Deduplicate message " << p.Msg.SeqNo << " by MessageDeduplicationId");
        p.DeduplicatedByMessageId = true;
        p.Offset = deduplicationResult.value();

        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ALREADY].Increment(1);
        MsgsDiscarded.Inc();
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ALREADY].Increment(p.Msg.Data.size());
        BytesDiscarded.Inc(p.Msg.Data.size());

        return true;
    }

    if (p.Msg.PartNo == 0) { //create new PartitionedBlob
        //there could be parts from previous owner, clear them
        if (!parameters.OldPartsCleared) {
            parameters.OldPartsCleared = true;

            NPQ::AddCmdDeleteRange(*request, TKeyPrefix::TypeTmpData, Partition);
        }

        if (BlobEncoder.PartitionedBlob.HasFormedBlobs()) {
            //clear currently-writed blobs
            auto oldCmdWrite = request->Record.GetCmdWrite();
            request->Record.ClearCmdWrite();
            for (ui32 i = 0; i < (ui32)oldCmdWrite.size(); ++i) {
                auto key = TKey::FromString(oldCmdWrite.Get(i).GetKey());
                if (key.GetType() != TKeyPrefix::TypeTmpData) {
                    request->Record.AddCmdWrite()->CopyFrom(oldCmdWrite.Get(i));
                }
            }
        }
        BlobEncoder.NewPartitionedBlob(Partition,
                                       curOffset,
                                       p.Msg.SourceId,
                                       p.Msg.SeqNo,
                                       p.Msg.TotalParts,
                                       p.Msg.TotalSize,
                                       parameters.HeadCleared,
                                       needCompactHead,
                                       MaxBlobSize);
    }

    LOG_D("Topic '" << TopicName() << "' partition " << Partition
            << " part blob processing sourceId '" << EscapeC(p.Msg.SourceId)
            << "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo
    );
    TString s;
    if (!BlobEncoder.PartitionedBlob.IsNextPart(p.Msg.SourceId, p.Msg.SeqNo, p.Msg.PartNo, &s)) {
        //this must not be happen - client sends gaps, fail this client till the end
        //now no changes will leak
        ctx.Send(TabletActorId, new TEvents::TEvPoisonPill());

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

    // Empty partition may will be filling from offset great than zero from mirror actor if source partition old and was clean by retantion time
    TryCorrectStartOffset(p.Offset);

    TMaybe<TPartData> partData;
    if (p.Msg.TotalParts > 1) { //this is multi-part message
        partData = TPartData(p.Msg.PartNo, p.Msg.TotalParts, p.Msg.TotalSize);
    }
    WriteTimestamp = ctx.Now();
    WriteTimestampEstimate = p.Msg.WriteTimestamp > 0 ? TInstant::MilliSeconds(p.Msg.WriteTimestamp) : WriteTimestamp;
    TClientBlob blob(TString{p.Msg.SourceId}, p.Msg.SeqNo, std::move(p.Msg.Data), partData, WriteTimestampEstimate,
                     TInstant::MilliSeconds(p.Msg.CreateTimestamp == 0 ? curOffset : p.Msg.CreateTimestamp),
                     p.Msg.UncompressedSize, std::move(p.Msg.PartitionKey), std::move(p.Msg.ExplicitHashKey)); //remove curOffset when LB will report CTime

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
    auto newWrite = BlobEncoder.PartitionedBlob.Add(std::move(blob));

    if (newWrite && !newWrite->Value.empty()) {
        newWrite->Key.SetFastWrite();
        AddCmdWriteWithDeferredTimestamp(newWrite, request, ctx);

        LOG_D("Topic '" << TopicName() <<
                "' partition " << Partition <<
                " part blob sourceId '" << EscapeC(p.Msg.SourceId) <<
                "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo <<
                " result is " << newWrite->Key.ToString() <<
                " size " << newWrite->Value.size()
        );
    }

    if (lastBlobPart) {
        PQ_ENSURE(BlobEncoder.PartitionedBlob.IsComplete());
        ui32 curWrites = RenameTmpCmdWrites(request);
        PQ_ENSURE(curWrites <= BlobEncoder.PartitionedBlob.GetFormedBlobs().size());
        RenameFormedBlobs(BlobEncoder.PartitionedBlob.GetFormedBlobs(),
                          parameters,
                          curWrites,
                          request,
                          BlobEncoder,
                          ctx);

        // Here we put together everything we want to burn to a disc. The list of keys for the finished
        // blocks will be in BlobEncoder.CompactedKeys, and the remaining small blobs in BlobEncoder.NewHead.
        // The key for them will appear later in the TPartition::EndProcessWrites function.

        ui32 countOfLastParts = 0;
        for (auto& x : BlobEncoder.PartitionedBlob.GetClientBlobs()) {
            if (BlobEncoder.NewHead.GetBatches().empty() || BlobEncoder.NewHead.GetLastBatch().Packed) {
                BlobEncoder.NewHead.AddBatch(TBatch(curOffset, x.GetPartNo()));
                BlobEncoder.NewHead.PackedSize += GetMaxHeaderSize(); //upper bound for packed size
            }

            if (x.IsLastPart()) {
                ++countOfLastParts;
            }

            PQ_ENSURE(!BlobEncoder.NewHead.GetLastBatch().Packed);
            BlobEncoder.NewHead.AddBlob(x);
            BlobEncoder.NewHead.PackedSize += x.GetSerializedSize();
            if (BlobEncoder.NewHead.GetLastBatch().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                BlobEncoder.PackLastBatch();
            }
        }

        PQ_ENSURE(countOfLastParts == 1);

        LOG_D("Topic '" << TopicName() << "' partition " << Partition
                << " part blob complete sourceId '" << EscapeC(p.Msg.SourceId) << "' seqNo " << p.Msg.SeqNo
                << " partNo " << p.Msg.PartNo << " FormedBlobsCount " << BlobEncoder.PartitionedBlob.GetFormedBlobs().size()
                << " NewHead: " << BlobEncoder.NewHead
        );

        sourceId.Update(p.Msg.SeqNo, curOffset, CurrentTimestamp, p.Msg.ProducerEpoch);

        ++curOffset;
        BlobEncoder.ClearPartitionedBlob(Partition, MaxBlobSize);
    }
    return true;
}

std::pair<TKey, ui32> TPartition::GetNewFastWriteKeyImpl(bool headCleared, ui32 headSize)
{
    TKey key = BlobEncoder.KeyForFastWrite(TKeyPrefix::TypeData, Partition);

    BlobEncoder.DataKeysHead[TotalLevels - 1].AddKey(key, BlobEncoder.NewHead.PackedSize);
    PQ_ENSURE(headSize + BlobEncoder.NewHead.PackedSize <= 3 * MaxSizeCheck);

    auto res = BlobEncoder.Compact(key, headCleared);
    PQ_ENSURE(res.first.HasSuffix());//may compact some KV blobs from head, but new KV blob is from head too
    PQ_ENSURE(res.second >= BlobEncoder.NewHead.PackedSize); //at least new data must be writed
    PQ_ENSURE(res.second <= MaxBlobSize);

    return res;
}

std::pair<TKey, ui32> TPartition::GetNewFastWriteKey(bool headCleared)
{
    ui32 headSize = headCleared ? 0 : BlobEncoder.Head.PackedSize;

    PQ_ENSURE(BlobEncoder.NewHead.PackedSize > 0); // smthing must be here

    return GetNewFastWriteKeyImpl(headCleared, headSize);
}

void TPartition::AddNewFastWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    LOG_T("TPartition::AddNewFastWriteBlob");

    const auto& key = res.first;

    TString valueD = BlobEncoder.SerializeForKey(key, res.second, BlobEncoder.EndOffset, PendingWriteTimestamp);

    auto write = request->Record.AddCmdWrite();
    write->SetKey(key.Data(), key.Size());
    write->SetValue(valueD);

    bool isInline = key.HasSuffix() && valueD.size() < MAX_INLINE_SIZE;

    if (isInline) {
        write->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
    } else {
        auto channel = GetChannel(NextChannel(key.HasSuffix(), valueD.size()));
        write->SetStorageChannel(channel);
        write->SetTactic(AppData(ctx)->PQConfig.GetTactic());
    }

    PQ_ENSURE(BlobEncoder.NewHeadKey.Size == 0);
    BlobEncoder.NewHeadKey = {key, res.second, PendingWriteTimestamp, 0, MakeBlobKeyToken(key.ToString())};

    WriteCycleSize += write->GetValue().size();
    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::SetDeadlinesForWrites(const TActorContext& ctx) {
    LOG_T("TPartition::SetDeadlinesForWrites.");
    auto quotaWaitDurationMs = TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs());
    if (SubDomainOutOfSpace) {
        quotaWaitDurationMs = quotaWaitDurationMs ? std::min(quotaWaitDurationMs, SubDomainQuotaWaitDurationMs) : SubDomainQuotaWaitDurationMs;
    }
    if (quotaWaitDurationMs > TDuration::Zero() && QuotaDeadline == TInstant::Zero()) {
        QuotaDeadline = ctx.Now() + quotaWaitDurationMs;

        ctx.Schedule(QuotaDeadline, new TEvPQ::TEvQuotaDeadlineCheck());
    }
}

void TPartition::Handle(TEvPQ::TEvQuotaDeadlineCheck::TPtr&, const TActorContext& ctx) {
    LOG_T("TPartition::Handle TEvQuotaDeadlineCheck.");

    FilterDeadlinedWrites(ctx);
}

void TPartition::FilterDeadlinedWrites(const TActorContext& ctx) {
    if (QuotaDeadline == TInstant::Zero() || QuotaDeadline > ctx.Now()) {
        return;
    }
    LOG_T("TPartition::FilterDeadlinedWrites.");

    FilterDeadlinedWrites(ctx, PendingRequests);

    QuotaDeadline = TInstant::Zero();

    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::FilterDeadlinedWrites(const TActorContext& ctx, TMessageQueue& requests)
{
    TMessageQueue newRequests;
    for (auto& w : requests) {
        if (!w.IsWrite() || (w.GetWrite().Msg.IgnoreQuotaDeadline && !SubDomainOutOfSpace)) {
            newRequests.emplace_back(std::move(w));
            continue;
        }
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;

            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            Y_DEBUG_ABORT_UNLESS(WriteInflightSize >= msg.Data.size(),
                                 "PQ %" PRIu64 ", Partition {%" PRIu32 ", %" PRIu32 "}, WriteInflightSize=%" PRIu64 ", msg.Data.size=%" PRISZT,
                                 TabletId, Partition.OriginalPartitionId, Partition.InternalPartitionId,
                                 WriteInflightSize, msg.Data.size());
            WriteInflightSize -= msg.Data.size();
        }

        TString errorMsg = SubDomainOutOfSpace ? "database size exceeded" : "quota exceeded";
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::OVERLOAD, errorMsg);
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
    size_t deduplicationIdQuotaSize = 0;
    for (auto& r : PendingRequests) {
        quotaSize += r.GetWriteSize();
        if (r.IsWrite()) {
            auto& write = r.GetWrite();
            if (write.Msg.MessageDeduplicationId && write.Msg.PartNo == 0) {
                ++deduplicationIdQuotaSize;
            }
        }
    }

    if (!quotaSize) {
        return false;
    }

    for (auto& r : PendingRequests) {
        r.WaitQuotaSpan = NWilson::TSpan(TWilsonTopic::TopicDetailed, r.Span.GetTraceId(), "Topic.Partition.WaitQuota");
        r.WaitQuotaSpan.Attribute("quota", static_cast<i64>(quotaSize));
    }

    RemovePendingRequests(QuotaWaitingRequests);
    RequestBlobQuota(quotaSize, deduplicationIdQuotaSize);

    return true;
}

void TPartition::HandlePendingRequests(const TActorContext& ctx)
{
    if (WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota() || NeedDeletePartition) {
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
    PQ_ENSURE(BlobEncoder.Head.PackedSize + BlobEncoder.NewHead.PackedSize <= 2 * MaxSizeCheck);

    TInstant now = ctx.Now();
    WriteCycleStartTime = now;

    BlobEncoder.HaveData = false;
    HaveCheckDisk = false;

    if (!DiskIsFull) {
        return;
    }

    AddCheckDiskRequest(request, NumChannels);
    HaveCheckDisk = true;
}

void TPartition::EndHandleRequests(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    ProcessReserveRequests(ctx);
    if (!BlobEncoder.HaveData && !HaveDrop && !HaveCheckDisk) { //no data writed/deleted
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

    BlobEncoder.HeadCleared = false;
}

void TPartition::EndProcessWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx)
{
    if (BlobEncoder.HeadCleared) {
        PQ_ENSURE(!BlobEncoder.CompactedKeys.empty() || BlobEncoder.Head.PackedSize == 0);
        for (ui32 i = 0; i < TotalLevels; ++i) {
            BlobEncoder.DataKeysHead[i].Clear();
        }
    }

    if (BlobEncoder.NewHead.PackedSize == 0) { //nothing added to head - just compaction or tmp part blobs writed
        if (!SourceIdBatch->HasModifications()) {
            BlobEncoder.HaveData = request->Record.CmdWriteSize() > 0
                || request->Record.CmdRenameSize() > 0
                || request->Record.CmdDeleteRangeSize() > 0;
            return;
        } else {
            SourceIdBatch->FillRequest(request);
            BlobEncoder.HaveData = true;
            return;
        }
    }

    SourceIdBatch->FillRequest(request);

    std::pair<TKey, ui32> res = GetNewFastWriteKey(BlobEncoder.HeadCleared);
    const auto& key = res.first;

    LOG_D("Add new write blob: topic '" << TopicName() << "' partition " << Partition
            << " compactOffset " << key.GetOffset() << "," << key.GetCount()
            << " HeadOffset " << BlobEncoder.Head.Offset << " endOffset " << BlobEncoder.EndOffset << " curOffset "
            << BlobEncoder.NewHead.GetNextOffset() << " " << key.ToString()
            << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );
    AddNewFastWriteBlob(res, request, ctx);

    BlobEncoder.HaveData = true;
}

void TPartition::BeginAppendHeadWithNewWrites(const TActorContext& ctx)
{
    Parameters.ConstructInPlace(*SourceIdBatch);
    Parameters->CurOffset = BlobEncoder.PartitionedBlob.IsInited() ? BlobEncoder.PartitionedBlob.GetOffset() : BlobEncoder.EndOffset;

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewMessages = 0;
    UpdateWriteBufferIsFullState(ctx.Now());
    CurrentTimestamp = ctx.Now();

    // BlobEncoder.ResetNewHead ???
    BlobEncoder.NewHead.Offset = BlobEncoder.EndOffset;
    BlobEncoder.NewHead.PartNo = 0;
    BlobEncoder.NewHead.PackedSize = 0;

    PQ_ENSURE(BlobEncoder.NewHead.GetBatches().empty());

    Parameters->OldPartsCleared = false;
    Parameters->HeadCleared = (BlobEncoder.Head.PackedSize == 0);
}

void TPartition::EndAppendHeadWithNewWrites(const TActorContext& ctx)
{
    if (const auto heartbeat = SourceIdBatch->CanEmitHeartbeat()) {
        if (heartbeat->Version > LastEmittedHeartbeat) {
            LOG_I("Topic '" << TopicName() << "' partition " << Partition
                    << " emit heartbeat " << heartbeat->Version);

            auto hbMsg = TWriteMsg{Max<ui64>() /* cookie */, Nothing(), TEvPQ::TEvWrite::TMsg{
                .SourceId = NSourceIdEncoding::EncodeSimple(ToString(TabletId)),
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
            ExecRequest(hbMsg, *Parameters, PersistRequest.Get());

            LastEmittedHeartbeat = heartbeat->Version;
        }
    }


    UpdateWriteBufferIsFullState(ctx.Now());

    if (!BlobEncoder.IsLastBatchPacked()) {
        BlobEncoder.PackLastBatch();
    }

    PQ_ENSURE((Parameters->HeadCleared ? 0 : BlobEncoder.Head.PackedSize) + BlobEncoder.NewHead.PackedSize <= MaxBlobSize); //otherwise last PartitionedBlob.Add must compact all except last cl
    BlobEncoder.MaxWriteResponsesSize = Max<ui32>(BlobEncoder.MaxWriteResponsesSize, Responses.size());

    BlobEncoder.HeadCleared = Parameters->HeadCleared;
}

void TPartition::RequestQuotaForWriteBlobRequest(size_t dataSize, ui64 cookie) {
    LOG_D("Send write quota request." <<" Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << "." <<
            " Amount: " << dataSize << "." <<
            " Cookie: " << cookie
    );
    Send(WriteQuotaTrackerActor, new TEvPQ::TEvRequestQuota(cookie, nullptr));
}

bool TPartition::WaitingForPreviousBlobQuota() const {
    return TopicQuotaRequestCookie != 0;
}

bool TPartition::WaitingForSubDomainQuota(const ui64 withSize) const {
    if (!SubDomainOutOfSpace || !AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota()) {
        return false;
    }

    if (NKikimrPQ::TPQTabletConfig::METERING_MODE_REQUEST_UNITS == Config.GetMeteringMode()) {
        // We allow one message to be written even when the SubDomainOutOfSpace.
        return withSize > 0 || Size() > 0;
    }

    return UserDataSize() + withSize > ReserveSize();
}

void TPartition::RequestBlobQuota(size_t quotaSize, size_t deduplicationIdQuotaSize)
{
    LOG_T("TPartition::RequestBlobQuota.");

    PQ_ENSURE(!WaitingForPreviousBlobQuota());

    TopicQuotaRequestCookie = NextTopicWriteQuotaRequestCookie++;
    BlobQuotaSize = quotaSize;
    DeduplicationIdQuotaSize = deduplicationIdQuotaSize;
    RequestQuotaForWriteBlobRequest(quotaSize, TopicQuotaRequestCookie);
}

void TPartition::ConsumeBlobQuota()
{
    if (!WriteQuotaTrackerActor) {
        return;
    }

    PQ_ENSURE(TopicQuotaRequestCookie != 0);
    Send(WriteQuotaTrackerActor, new TEvPQ::TEvConsumed(BlobQuotaSize, DeduplicationIdQuotaSize, TopicQuotaRequestCookie, {}));
}

} // namespace NKikimr::NPQ
