#include "event_helpers.h"
#include "mirrorer.h"
#include "partition_util.h"
#include "partition.h"
#include "read.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/path.h>
#include <ydb/core/quoter/public/quoter.h>
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
static const ui32 MAX_WRITE_CYCLE_SIZE = 16_MB;
static const ui32 MAX_INLINE_SIZE = 1000;

void TPartition::ReplyOwnerOk(const TActorContext& ctx, const ui64 dst, const TString& cookie) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ReplyOwnerOk. Partition: " << Partition);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;
    resp.SetStatus(NMsgBusProxy::MSTATUS_OK);
    resp.SetErrorCode(NPersQueue::NErrorCode::OK);
    resp.MutablePartitionResponse()->MutableCmdGetOwnershipResult()->SetOwnerCookie(cookie);
    ctx.Send(Tablet, response.Release());
}

void TPartition::ReplyWrite(
    const TActorContext& ctx, const ui64 dst, const TString& sourceId, const ui64 seqNo, const ui16 partNo, const ui16 totalParts,
    const ui64 offset, const TInstant writeTimestamp, bool already, const ui64 maxSeqNo,
    const TDuration partitionQuotedTime, const TDuration topicQuotedTime, const TDuration queueTime, const TDuration writeTime) {

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ReplyWrite. Partition: " << Partition);

    Y_VERIFY(offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, offset);
    Y_VERIFY(seqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, seqNo);

    THolder<TEvPQ::TEvProxyResponse> response = MakeHolder<TEvPQ::TEvProxyResponse>(dst);
    NKikimrClient::TResponse& resp = response->Response;
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

void TPartition::HandleOnIdle(TEvPQ::TEvUpdateAvailableSize::TPtr&, const TActorContext& ctx) {
    UpdateAvailableSize(ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvUpdateAvailableSize::TPtr&, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvUpdateAvailableSize. Partition: " << Partition);

    UpdateAvailableSize(ctx);
}

void TPartition::CancelAllWritesOnIdle(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::CancelAllWritesOnIdle. Partition: " << Partition);

    for (const auto& w : Requests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::WRITE_ERROR_DISK_IS_FULL, "Disk is full");
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            WriteInflightSize -= msg.Data.size();
        }
    }

    UpdateWriteBufferIsFullState(ctx.Now());
    Requests.clear();
    Y_VERIFY(Responses.empty());

    WriteCycleSize = 0;

    ProcessReserveRequests(ctx);
}

void TPartition::FailBadClient(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::FailBadClient. Partition: " << Partition);

    for (auto it = Owners.begin(); it != Owners.end();) {
        it = DropOwner(it, ctx);
    }
    Y_VERIFY(Owners.empty());
    Y_VERIFY(ReservedSize == 0);

    for (const auto& w : Requests) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::BAD_REQUEST, "previous write request failed");
        if (w.IsWrite()) {
            const auto& msg = w.GetWrite().Msg;
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(msg.Data.size() + msg.SourceId.size());
            WriteInflightSize -= msg.Data.size();
        }
    }
    UpdateWriteBufferIsFullState(ctx.Now());
    Requests.clear();
    for (const auto& w : Responses) {
        ReplyError(ctx, w.GetCookie(), NPersQueue::NErrorCode::BAD_REQUEST, "previous write request failed");
        if (w.IsWrite())
            TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
    }
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(WriteNewSize);
    Responses.clear();

    ProcessChangeOwnerRequests(ctx);
    ProcessReserveRequests(ctx);
}

void TPartition::ProcessChangeOwnerRequest(TAutoPtr<TEvPQ::TEvChangeOwner> ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ProcessChangeOwnerRequest. Partition: " << Partition);

    auto &owner = ev->Owner;
    auto it = Owners.find(owner);
    if (it == Owners.end()) {
        Owners[owner];
        it = Owners.find(owner);
    }
    if (it->second.NeedResetOwner || ev->Force) { //change owner
        Y_VERIFY(ReservedSize >= it->second.ReservedSize);
        ReservedSize -= it->second.ReservedSize;

        it->second.GenerateCookie(owner, ev->PipeClient, ev->Sender, TopicName(), Partition, ctx);//will change OwnerCookie
        //cookie is generated. but answer will be sent when all inflight writes will be done - they in the same queue 'Requests'
        EmplaceRequest(TOwnershipMsg{ev->Cookie, it->second.OwnerCookie}, ctx);
        TabletCounters.Simple()[COUNTER_PQ_TABLET_RESERVED_BYTES_SIZE].Set(ReservedSize);
        UpdateWriteBufferIsFullState(ctx.Now());
        ProcessReserveRequests(ctx);
    } else {
        it->second.WaitToChangeOwner.push_back(THolder<TEvPQ::TEvChangeOwner>(ev.Release()));
    }
}


THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator TPartition::DropOwner(THashMap<TString, NKikimr::NPQ::TOwnerInfo>::iterator& it, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::DropOwner. Partition: " << Partition);

    Y_VERIFY(ReservedSize >= it->second.ReservedSize);
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
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvChangeOwner. Partition: " << Partition);

    bool res = OwnerPipes.insert(ev->Get()->PipeClient).second;
    Y_VERIFY(res);
    WaitToChangeOwner.push_back(ev->Release());
    ProcessChangeOwnerRequests(ctx);
}

void TPartition::ProcessReserveRequests(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ProcessReserveRequests. Partition: " << Partition);

    const ui64 maxWriteInflightSize = Config.GetPartitionConfig().GetMaxWriteInflightSize();

    while (!ReserveRequests.empty()) {
        const TString& ownerCookie = ReserveRequests.front()->OwnerCookie;
        const TStringBuf owner = TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie);
        const ui64& size = ReserveRequests.front()->Size;
        const ui64& cookie = ReserveRequests.front()->Cookie;
        const bool& lastRequest = ReserveRequests.front()->LastRequest;

        auto it = Owners.find(owner);
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
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvReserveBytes. Partition: " << Partition);

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

void TPartition::HandleOnIdle(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::AnswerCurrentWrites(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::AnswerCurrentWrites. Partition: " << Partition);

    ui64 offset = EndOffset;
    while (!Responses.empty()) {
        const auto& response = Responses.front();

        const TDuration quotedTime = response.QuotedTime;
        const TDuration queueTime = response.QueueTime;
        const TDuration writeTime = ctx.Now() - response.WriteTimeBaseline;

        if (response.IsWrite()) {
            const auto& writeResponse = response.GetWrite();
            const TString& s = writeResponse.Msg.SourceId;
            const ui64& seqNo = writeResponse.Msg.SeqNo;
            const ui16& partNo = writeResponse.Msg.PartNo;
            const ui16& totalParts = writeResponse.Msg.TotalParts;
            const TMaybe<ui64>& wrOffset = writeResponse.Offset;

            bool already = false;

            auto it = SourceIdStorage.GetInMemorySourceIds().find(s);

            ui64 maxSeqNo = 0;
            ui64 maxOffset = 0;

            if (it != SourceIdStorage.GetInMemorySourceIds().end()) {
                maxSeqNo = it->second.SeqNo;
                maxOffset = it->second.Offset;
                if (it->second.SeqNo >= seqNo && !writeResponse.Msg.DisableDeduplication) {
                    already = true;
                }
            }

            if (!already) {
                if (wrOffset) {
                    Y_VERIFY(*wrOffset >= offset);
                    offset = *wrOffset;
                }
            }
            if (!already && partNo + 1 == totalParts) {
                if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
                    TabletCounters.Cumulative()[COUNTER_PQ_SID_CREATED].Increment(1);
                    SourceIdStorage.RegisterSourceId(s, writeResponse.Msg.SeqNo, offset, CurrentTimestamp);
                } else {
                    SourceIdStorage.RegisterSourceId(s, it->second.Updated(writeResponse.Msg.SeqNo, offset, CurrentTimestamp));
                }

                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_OK].Increment(1);
            }
            ReplyWrite(
                ctx, writeResponse.Cookie, s, seqNo, partNo, totalParts,
                already ? maxOffset : offset, CurrentTimestamp, already, maxSeqNo,
                quotedTime, TopicQuotaWaitTimeForCurrentBlob, queueTime, writeTime
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
            if (PartitionWriteQuotaWaitCounter) {
                PartitionWriteQuotaWaitCounter->IncFor(quotedTime.MilliSeconds());
            }

            if (!already && partNo + 1 == totalParts)
                ++offset;
        } else if (response.IsOwnership()) {
            const TString& ownerCookie = response.GetOwnership().OwnerCookie;
            auto it = Owners.find(TOwnerInfo::GetOwnerFromOwnerCookie(ownerCookie));
            if (it != Owners.end() && it->second.OwnerCookie == ownerCookie) {
                ReplyOwnerOk(ctx, response.GetCookie(), ownerCookie);
            } else {
                ReplyError(ctx, response.GetCookie(), NPersQueue::NErrorCode::WRONG_COOKIE, "new GetOwnership request is dropped already");
            }
        } else if (response.IsRegisterMessageGroup()) {
            const auto& body = response.GetRegisterMessageGroup().Body;

            TMaybe<TPartitionKeyRange> keyRange;
            if (body.KeyRange) {
                keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
            }

            Y_VERIFY(body.AssignedOffset);
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

                Y_VERIFY(body.AssignedOffset);
                SourceIdStorage.RegisterSourceId(body.SourceId, body.SeqNo, *body.AssignedOffset, CurrentTimestamp, std::move(keyRange), true);
            }

            ReplyOk(ctx, response.GetCookie());
        } else {
            Y_FAIL("Unexpected message");
        }
        Responses.pop_front();
    }
    TopicQuotaWaitTimeForCurrentBlob = TDuration::Zero();
}

void TPartition::SyncMemoryStateWithKVState(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::SyncMemoryStateWithKVState. Partition: " << Partition);

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

    Y_VERIFY(EndOffset == Head.GetNextOffset());

    if (!CompactedKeys.empty() || Head.PackedSize == 0) { //has compactedkeys or head is already empty
        Head.PackedSize = 0;
        Head.Offset = NewHead.Offset;
        Head.PartNo = NewHead.PartNo; //no partNo at this point
        Head.Batches.clear();
    }

    while (!CompactedKeys.empty()) {
        const auto& ck = CompactedKeys.front();
        BodySize += ck.second;
        Y_VERIFY(!ck.first.IsHead());
        ui64 lastOffset = DataKeysBody.empty() ? 0 : (DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount());
        Y_VERIFY(lastOffset <= ck.first.GetOffset());
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

void TPartition::Handle(TEvPQ::TEvHandleWriteResponse::TPtr&, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvHandleWriteResponse. Partition: " << Partition);

    HandleWriteResponse(ctx);
}

void TPartition::HandleWriteResponse(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleWriteResponse. Partition: " << Partition);

    Y_VERIFY(CurrentStateFunc() == &TThis::StateWrite);
    ui64 prevEndOffset = EndOffset;

    ui32 totalLatencyMs = (ctx.Now() - WriteCycleStartTime).MilliSeconds();
    ui32 writeLatencyMs = (ctx.Now() - WriteStartTime).MilliSeconds();

    WriteLatency.IncFor(writeLatencyMs, 1);
    if (writeLatencyMs >= AppData(ctx)->PQConfig.GetWriteLatencyBigMs()) {
        SLIBigLatency.Inc();
    }

    TabletCounters.Percentile()[COUNTER_LATENCY_PQ_WRITE_CYCLE].IncrementFor(totalLatencyMs);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_CYCLE_BYTES_TOTAL].Increment(WriteCycleSize);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_OK].Increment(WriteNewSize);
    TabletCounters.Percentile()[COUNTER_PQ_WRITE_CYCLE_BYTES].IncrementFor(WriteCycleSize);
    TabletCounters.Percentile()[COUNTER_PQ_WRITE_NEW_BYTES].IncrementFor(WriteNewSize);
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
    if (MsgsWrittenTotal)
        MsgsWrittenTotal.Inc(WriteNewMessages);

    //All ok
    auto now = ctx.Now();
    const auto& quotingConfig = AppData()->PQConfig.GetQuotingConfig();
    if (quotingConfig.GetTopicWriteQuotaEntityToLimit() == NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE) {
        WriteQuota->Exaust(WriteNewSize, now);
    } else {
        WriteQuota->Exaust(WriteCycleSize, now);
    }
    for (auto& avg : AvgWriteBytes) {
        avg.Update(WriteNewSize, now);
    }
    for (auto& avg : AvgQuotaBytes) {
        avg.Update(WriteNewSize, now);
    }

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeInternal = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewMessages = 0;
    WriteNewMessagesInternal = 0;
    UpdateWriteBufferIsFullState(now);

    AnswerCurrentWrites(ctx);
    SyncMemoryStateWithKVState(ctx);

    //if EndOffset changed there could be subscriptions witch could be completed
    TVector<std::pair<TReadInfo, ui64>> reads = Subscriber.GetReads(EndOffset);
    for (auto& read : reads) {
        Y_VERIFY(EndOffset > read.first.Offset);
        ProcessRead(ctx, std::move(read.first), read.second, true);
    }
    //same for read requests
    ProcessHasDataRequests(ctx);

    ProcessTimestampsForNewData(prevEndOffset, ctx);

    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvWrite::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvWrite. Partition: " << Partition);

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
        Y_VERIFY(!msg.SourceId.empty() || ev->Get()->IsDirectWrite || msg.DisableDeduplication);
        Y_VERIFY(!msg.Data.empty());

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
        EmplaceRequest(TWriteMsg{ev->Get()->Cookie, offset, std::move(msg)}, ctx);
        if (offset && needToChangeOffset)
            ++*offset;
    }
    WriteInflightSize += size;

    // TODO: remove decReservedSize == 0
    Y_VERIFY(size <= decReservedSize || decReservedSize == 0);
    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::HandleOnIdle(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvRegisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvRegisterMessageGroup. Partition: " << Partition);

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

    EmplaceRequest(TRegisterMessageGroupMsg(*ev->Get()), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvDeregisterMessageGroup::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvDeregisterMessageGroup. Partition: " << Partition);

    const auto& body = ev->Get()->Body;

    auto it = SourceIdStorage.GetInMemorySourceIds().find(body.SourceId);
    if (it == SourceIdStorage.GetInMemorySourceIds().end()) {
        return ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::SOURCEID_DELETED,
            "SourceId doesn't exist");
    }
    
    EmplaceRequest(TDeregisterMessageGroupMsg(*ev->Get()), ctx);
}

void TPartition::HandleOnIdle(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx) {
    HandleOnWrite(ev, ctx);
    HandleWrites(ctx);
}

void TPartition::HandleOnWrite(TEvPQ::TEvSplitMessageGroup::TPtr& ev, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleOnWrite TEvSplitMessageGroup. Partition: " << Partition);

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

    EmplaceRequest(std::move(msg), ctx);
}

std::pair<TKey, ui32> TPartition::Compact(const TKey& key, const ui32 size, bool headCleared) {
    std::pair<TKey, ui32> res({key, size});
    ui32 x = headCleared ? 0 : Head.PackedSize;
    Y_VERIFY(std::accumulate(DataKeysHead.begin(), DataKeysHead.end(), 0u, [](ui32 sum, const TKeyLevel& level){return sum + level.Sum();}) == NewHead.PackedSize + x);
    for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
        auto jt = it; ++jt;
        if (it->NeedCompaction()) {
            res = it->Compact();
            if (jt != DataKeysHead.rend()) {
                jt->AddKey(res.first, res.second);
            }
        } else {
            Y_VERIFY(jt == DataKeysHead.rend() || !jt->NeedCompaction()); //compact must start from last level, not internal
        }
        Y_VERIFY(!it->NeedCompaction());
    }
    Y_VERIFY(res.second >= size);
    Y_VERIFY(res.first.GetOffset() < key.GetOffset() || res.first.GetOffset() == key.GetOffset() && res.first.GetPartNo() <= key.GetPartNo());
    return res;
}


void TPartition::ProcessChangeOwnerRequests(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ProcessChangeOwnerRequests. Partition: " << Partition);

    while (!WaitToChangeOwner.empty()) {
        auto &ev = WaitToChangeOwner.front();
        if (OwnerPipes.find(ev->PipeClient) != OwnerPipes.end()) { //this is not request from dead pipe
            ProcessChangeOwnerRequest(ev.Release(), ctx);
        } else {
            ReplyError(ctx, ev->Cookie, NPersQueue::NErrorCode::ERROR, "Pipe for GetOwnershipRequest is already dead");
        }
        WaitToChangeOwner.pop_front();
    }
    if (CurrentStateFunc() == &TThis::StateIdle) {
        HandleWrites(ctx);
    }
}

void TPartition::CancelAllWritesOnWrite(const TActorContext& ctx, TEvKeyValue::TEvRequest* request, const TString& errorStr, const TWriteMsg& p, TSourceIdWriter& sourceIdWriter, NPersQueue::NErrorCode::EErrorCode errorCode) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::CancelAllWritesOnWrite. Partition: " << Partition);

    ReplyError(ctx, p.Cookie, errorCode, errorStr);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ERROR].Increment(1);
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ERROR].Increment(p.Msg.Data.size() + p.Msg.SourceId.size());
    FailBadClient(ctx);
    NewHead.Clear();
    NewHead.Offset = EndOffset;
    sourceIdWriter.Clear();
    request->Record.Clear();
    PartitionedBlob = TPartitionedBlob(Partition, 0, "", 0, 0, 0, Head, NewHead, true, false, MaxBlobSize);
    CompactedKeys.clear();

    WriteCycleSize = 0;
}

bool TPartition::AppendHeadWithNewWrites(TEvKeyValue::TEvRequest* request, const TActorContext& ctx,
                                         TSourceIdWriter& sourceIdWriter) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::AppendHeadWithNewWrites. Partition: " << Partition);

    ui64 curOffset = PartitionedBlob.IsInited() ? PartitionedBlob.GetOffset() : EndOffset;

    WriteCycleSize = 0;
    WriteNewSize = 0;
    WriteNewSizeUncompressed = 0;
    WriteNewMessages = 0;
    UpdateWriteBufferIsFullState(ctx.Now());
    CurrentTimestamp = ctx.Now();

    NewHead.Offset = EndOffset;
    NewHead.PartNo = 0;
    NewHead.PackedSize = 0;

    Y_VERIFY(NewHead.Batches.empty());

    bool oldPartsCleared = false;
    bool headCleared = (Head.PackedSize == 0);


    //TODO: Process here not TClientBlobs, but also TBatches from LB(LB got them from pushclient too)
    //Process is following: if batch contains already written messages or only one client message part -> unpack it and process as several TClientBlobs
    //otherwise write this batch as is to head;

    while (!Requests.empty() && WriteCycleSize < MAX_WRITE_CYCLE_SIZE) { //head is not too big
        auto pp = Requests.front();
        Requests.pop_front();

        if (!pp.IsWrite()) {
            if (pp.IsRegisterMessageGroup()) {
                auto& body = pp.GetRegisterMessageGroup().Body;

                TMaybe<TPartitionKeyRange> keyRange;
                if (body.KeyRange) {
                    keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
                }

                body.AssignedOffset = curOffset;
                sourceIdWriter.RegisterSourceId(body.SourceId, body.SeqNo, curOffset, CurrentTimestamp, std::move(keyRange));
            } else if (pp.IsDeregisterMessageGroup()) {
                sourceIdWriter.DeregisterSourceId(pp.GetDeregisterMessageGroup().Body.SourceId);
            } else if (pp.IsSplitMessageGroup()) {
                for (auto& body : pp.GetSplitMessageGroup().Deregistrations) {
                    sourceIdWriter.DeregisterSourceId(body.SourceId);
                }

                for (auto& body : pp.GetSplitMessageGroup().Registrations) {
                    TMaybe<TPartitionKeyRange> keyRange;
                    if (body.KeyRange) {
                        keyRange = TPartitionKeyRange::Parse(*body.KeyRange);
                    }

                    body.AssignedOffset = curOffset;
                    sourceIdWriter.RegisterSourceId(body.SourceId, body.SeqNo, curOffset, CurrentTimestamp, std::move(keyRange), true);
                }
            } else {
                Y_VERIFY(pp.IsOwnership());
            }

            EmplaceResponse(std::move(pp), ctx);
            continue;
        }

        Y_VERIFY(pp.IsWrite());
        auto& p = pp.GetWrite();

        WriteInflightSize -= p.Msg.Data.size();

        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_RECEIVE_QUEUE].IncrementFor(ctx.Now().MilliSeconds() - p.Msg.ReceiveTimestamp);
        //check already written

        ui64 poffset = p.Offset ? *p.Offset : curOffset;

        auto it_inMemory = SourceIdStorage.GetInMemorySourceIds().find(p.Msg.SourceId);
        auto it_toWrite = sourceIdWriter.GetSourceIdsToWrite().find(p.Msg.SourceId);
        if (!p.Msg.DisableDeduplication && (it_inMemory != SourceIdStorage.GetInMemorySourceIds().end() && it_inMemory->second.SeqNo >= p.Msg.SeqNo || (it_toWrite != sourceIdWriter.GetSourceIdsToWrite().end() && it_toWrite->second.SeqNo >= p.Msg.SeqNo))) {
            bool isWriting = (it_toWrite != sourceIdWriter.GetSourceIdsToWrite().end());
            bool isCommitted = (it_inMemory != SourceIdStorage.GetInMemorySourceIds().end());

            if (poffset >= curOffset) {
                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "Already written message. Topic: '" << TopicName()
                            << "' Partition: " << Partition << " SourceId: '" << EscapeC(p.Msg.SourceId)
                            << "'. Message seqNo = " << p.Msg.SeqNo
                            << ". Committed seqNo = " << (isCommitted ? it_inMemory->second.SeqNo : 0)
                            << (isWriting ? ". Writing seqNo: " : ". ") << (isWriting ? it_toWrite->second.SeqNo : 0)
                            << " EndOffset " << EndOffset << " CurOffset " << curOffset << " offset " << poffset
                );

                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_ALREADY].Increment(1);
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_ALREADY].Increment(p.Msg.Data.size());
            } else {
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_SMALL_OFFSET].Increment(1);
                TabletCounters.Cumulative()[COUNTER_PQ_WRITE_BYTES_SMALL_OFFSET].Increment(p.Msg.Data.size());
            }

            TString().swap(p.Msg.Data);
            EmplaceResponse(std::move(pp), ctx);
            continue;
        }

        if (poffset < curOffset) { //too small offset
            CancelAllWritesOnWrite(ctx, request,
                                    TStringBuilder() << "write message sourceId: " << EscapeC(p.Msg.SourceId) << " seqNo: " << p.Msg.SeqNo
                                        << " partNo: " << p.Msg.PartNo << " has incorrect offset " << poffset << ", must be at least " << curOffset,
                                        p, sourceIdWriter, NPersQueue::NErrorCode::EErrorCode::WRITE_ERROR_BAD_OFFSET);
            return false;
        }

        Y_VERIFY(poffset >= curOffset);

        bool needCompactHead = poffset > curOffset;
        if (needCompactHead) { //got gap
            if (p.Msg.PartNo != 0) { //gap can't be inside of partitioned message
                CancelAllWritesOnWrite(ctx, request,
                                        TStringBuilder() << "write message sourceId: " << EscapeC(p.Msg.SourceId) << " seqNo: " << p.Msg.SeqNo
                                            << " partNo: " << p.Msg.PartNo << " has gap inside partitioned message, incorrect offset "
                                            << poffset << ", must be " << curOffset,
                                            p, sourceIdWriter);
                return false;
            }
            curOffset = poffset;
        }

        if (p.Msg.PartNo == 0) { //create new PartitionedBlob
            //there could be parts from previous owner, clear them
            if (!oldPartsCleared) {
                oldPartsCleared = true;
                auto del = request->Record.AddCmdDeleteRange();
                auto range = del->MutableRange();
                TKeyPrefix from(TKeyPrefix::TypeTmpData, Partition);
                range->SetFrom(from.Data(), from.Size());
                TKeyPrefix to(TKeyPrefix::TypeTmpData, Partition + 1);
                range->SetTo(to.Data(), to.Size());
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
                                               headCleared, needCompactHead, MaxBlobSize);
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
            CancelAllWritesOnWrite(ctx, request, s, p, sourceIdWriter);
            //now no changes will leak
            return false;
        }

        WriteNewSize += p.Msg.SourceId.size() + p.Msg.Data.size();
        WriteNewSizeInternal += p.Msg.External ? 0 : (p.Msg.SourceId.size() + p.Msg.Data.size());
        WriteNewSizeUncompressed += p.Msg.UncompressedSize + p.Msg.SourceId.size();
        if (p.Msg.PartNo == 0) {
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
        if (InputTimeLag) {
            InputTimeLag->IncFor(writeLagMs, 1);
            if (p.Msg.PartNo == 0) {
                MessageSize->IncFor(p.Msg.TotalSize + p.Msg.SourceId.size(), 1);
            }
        }

        bool lastBlobPart = blob.IsLastPart();

        //will return compacted tmp blob
        auto newWrite = PartitionedBlob.Add(std::move(blob));

        if (newWrite && !newWrite->second.empty()) {
            auto write = request->Record.AddCmdWrite();
            write->SetKey(newWrite->first.Data(), newWrite->first.Size());
            write->SetValue(newWrite->second);
            Y_VERIFY(!newWrite->first.IsHead());
            auto channel = GetChannel(NextChannel(newWrite->first.IsHead(), newWrite->second.Size()));
            write->SetStorageChannel(channel);
            write->SetTactic(AppData(ctx)->PQConfig.GetTactic());

            TKey resKey = newWrite->first;
            resKey.SetType(TKeyPrefix::TypeData);
            write->SetKeyToCache(resKey.Data(), resKey.Size());
            WriteCycleSize += newWrite->second.size();

            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Topic '" << TopicName() <<
                        "' partition " << Partition <<
                        " part blob sourceId '" << EscapeC(p.Msg.SourceId) <<
                        "' seqNo " << p.Msg.SeqNo << " partNo " << p.Msg.PartNo <<
                        " result is " << TStringBuf(newWrite->first.Data(), newWrite->first.Size()) <<
                        " size " << newWrite->second.size()
            );
        }

        if (lastBlobPart) {
            Y_VERIFY(PartitionedBlob.IsComplete());
            ui32 curWrites = 0;
            for (ui32 i = 0; i < request->Record.CmdWriteSize(); ++i) { //change keys for yet to be writed KV pairs
                TKey key(request->Record.GetCmdWrite(i).GetKey());
                if (key.GetType() == TKeyPrefix::TypeTmpData) {
                    key.SetType(TKeyPrefix::TypeData);
                    request->Record.MutableCmdWrite(i)->SetKey(TString(key.Data(), key.Size()));
                    ++curWrites;
                }
            }
            Y_VERIFY(curWrites <= PartitionedBlob.GetFormedBlobs().size());
            auto formedBlobs = PartitionedBlob.GetFormedBlobs();
            for (ui32 i = 0; i < formedBlobs.size(); ++i) {
                const auto& x = formedBlobs[i];
                if (i + curWrites < formedBlobs.size()) { //this KV pair is already writed, rename needed
                    auto rename = request->Record.AddCmdRename();
                    TKey key = x.first;
                    rename->SetOldKey(TString(key.Data(), key.Size()));
                    key.SetType(TKeyPrefix::TypeData);
                    rename->SetNewKey(TString(key.Data(), key.Size()));
                }
                if (!DataKeysBody.empty() && CompactedKeys.empty()) {
                    Y_VERIFY(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() <= x.first.GetOffset(),
                        "LAST KEY %s, HeadOffset %lu, NEWKEY %s", DataKeysBody.back().Key.ToString().c_str(), Head.Offset, x.first.ToString().c_str());
                }
                LOG_DEBUG_S(
                        ctx, NKikimrServices::PERSQUEUE,
                        "writing blob: topic '" << TopicName() << "' partition " << Partition
                            << " " << x.first.ToString() << " size " << x.second << " WTime " << ctx.Now().MilliSeconds()
                );

                CompactedKeys.push_back(x);
                CompactedKeys.back().first.SetType(TKeyPrefix::TypeData);
            }
            if (PartitionedBlob.HasFormedBlobs()) { //Head and newHead are cleared
                headCleared = true;
                NewHead.Clear();
                NewHead.Offset = PartitionedBlob.GetOffset();
                NewHead.PartNo = PartitionedBlob.GetHeadPartNo();
                NewHead.PackedSize = 0;
            }
            ui32 countOfLastParts = 0;
            for (auto& x : PartitionedBlob.GetClientBlobs()) {
                if (NewHead.Batches.empty() || NewHead.Batches.back().Packed) {
                    NewHead.Batches.emplace_back(curOffset, x.GetPartNo(), TVector<TClientBlob>());
                    NewHead.PackedSize += GetMaxHeaderSize(); //upper bound for packed size
                }
                if (x.IsLastPart()) {
                    ++countOfLastParts;
                }
                Y_VERIFY(!NewHead.Batches.back().Packed);
                NewHead.Batches.back().AddBlob(x);
                NewHead.PackedSize += x.GetBlobSize();
                if (NewHead.Batches.back().GetUnpackedSize() >= BATCH_UNPACK_SIZE_BORDER) {
                    NewHead.Batches.back().Pack();
                    NewHead.PackedSize += NewHead.Batches.back().GetPackedSize(); //add real packed size for this blob

                    NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
                    NewHead.PackedSize -= NewHead.Batches.back().GetUnpackedSize();
                }
            }

            Y_VERIFY(countOfLastParts == 1);

            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "Topic '" << TopicName() << "' partition " << Partition
                        << " part blob complete sourceId '" << EscapeC(p.Msg.SourceId) << "' seqNo " << p.Msg.SeqNo
                        << " partNo " << p.Msg.PartNo << " FormedBlobsCount " << PartitionedBlob.GetFormedBlobs().size()
                        << " NewHead: " << NewHead
            );

            if (it_inMemory == SourceIdStorage.GetInMemorySourceIds().end()) {
                sourceIdWriter.RegisterSourceId(p.Msg.SourceId, p.Msg.SeqNo, curOffset, CurrentTimestamp);
            } else {
                sourceIdWriter.RegisterSourceId(p.Msg.SourceId, it_inMemory->second.Updated(p.Msg.SeqNo, curOffset, CurrentTimestamp));
            }

            ++curOffset;
            PartitionedBlob = TPartitionedBlob(Partition, 0, "", 0, 0, 0, Head, NewHead, true, false, MaxBlobSize);
        }
        TString().swap(p.Msg.Data);
        EmplaceResponse(std::move(pp), ctx);
    }

    UpdateWriteBufferIsFullState(ctx.Now());

    if (!NewHead.Batches.empty() && !NewHead.Batches.back().Packed) {
        NewHead.Batches.back().Pack();
        NewHead.PackedSize += NewHead.Batches.back().GetPackedSize(); //add real packed size for this blob

        NewHead.PackedSize -= GetMaxHeaderSize(); //instead of upper bound
        NewHead.PackedSize -= NewHead.Batches.back().GetUnpackedSize();
    }

    Y_VERIFY((headCleared ? 0 : Head.PackedSize) + NewHead.PackedSize <= MaxBlobSize); //otherwise last PartitionedBlob.Add must compact all except last cl
    MaxWriteResponsesSize = Max<ui32>(MaxWriteResponsesSize, Responses.size());

    return headCleared;
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

    Y_VERIFY(NewHead.PackedSize > 0 || needCompaction); //smthing must be here

    TKey key(TKeyPrefix::TypeData, Partition, NewHead.Offset, NewHead.PartNo, NewHead.GetCount(), NewHead.GetInternalPartsCount(), !needCompaction);

    if (NewHead.PackedSize > 0)
        DataKeysHead[TotalLevels - 1].AddKey(key, NewHead.PackedSize);
    Y_VERIFY(HeadSize + NewHead.PackedSize <= 3 * MaxSizeCheck);

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
        Y_VERIFY(res.first.IsHead());//may compact some KV blobs from head, but new KV blob is from head too
        Y_VERIFY(res.second >= NewHead.PackedSize); //at least new data must be writed
    }
    Y_VERIFY(res.second <= MaxBlobSize);
    return res;
}

void TPartition::AddNewWriteBlob(std::pair<TKey, ui32>& res, TEvKeyValue::TEvRequest* request, bool headCleared, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::AddNewWriteBlob. Partition: " << Partition);

    const auto& key = res.first;

    TString valueD;
    valueD.reserve(res.second);
    ui32 pp = Head.FindPos(key.GetOffset(), key.GetPartNo());
    if (pp < Max<ui32>() && key.GetOffset() < EndOffset) { //this batch trully contains this offset
        Y_VERIFY(pp < Head.Batches.size());
        Y_VERIFY(Head.Batches[pp].GetOffset() == key.GetOffset());
        Y_VERIFY(Head.Batches[pp].GetPartNo() == key.GetPartNo());
        for (; pp < Head.Batches.size(); ++pp) { //TODO - merge small batches here
            Y_VERIFY(Head.Batches[pp].Packed);
            Head.Batches[pp].SerializeTo(valueD);
        }
    }
    for (auto& b : NewHead.Batches) {
        Y_VERIFY(b.Packed);
        b.SerializeTo(valueD);
    }

    Y_VERIFY(res.second >= valueD.size());

    if (res.second > valueD.size() && res.first.IsHead()) { //change to real size if real packed size is smaller

        Y_FAIL("Can't be here right now, only after merging of small batches");

        for (auto it = DataKeysHead.rbegin(); it != DataKeysHead.rend(); ++it) {
            if (it->KeysCount() > 0 ) {
                auto res2 = it->PopBack();
                Y_VERIFY(res2 == res);
                res2.second = valueD.size();

                DataKeysHead[TotalLevels - 1].AddKey(res2.first, res2.second);

                res2 = Compact(res2.first, res2.second, headCleared);

                Y_VERIFY(res2.first == res.first);
                Y_VERIFY(res2.second == valueD.size());
                res = res2;
                break;
            }
        }
    }

    Y_VERIFY(res.second == valueD.size() || res.first.IsHead());

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
            Y_VERIFY(DataKeysBody.back().Key.GetOffset() + DataKeysBody.back().Key.GetCount() <= key.GetOffset(),
                "LAST KEY %s, HeadOffset %lu, NEWKEY %s", DataKeysBody.back().Key.ToString().c_str(), Head.Offset, key.ToString().c_str());
        }
        CompactedKeys.push_back(res);
        NewHead.Clear();
        NewHead.Offset = res.first.GetOffset() + res.first.GetCount();
        NewHead.PartNo = 0;
    } else {
        Y_VERIFY(NewHeadKey.Size == 0);
        NewHeadKey = {key, res.second, CurrentTimestamp, 0};
    }
    WriteCycleSize += write->GetValue().size();
    UpdateWriteBufferIsFullState(ctx.Now());
}

void TPartition::SetDeadlinesForWrites(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::SetDeadlinesForWrites. Partition: " << Partition);

    if (AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs() > 0 && QuotaDeadline == TInstant::Zero()) {
        QuotaDeadline = ctx.Now() + TDuration::MilliSeconds(AppData(ctx)->PQConfig.GetQuotingConfig().GetQuotaWaitDurationMs());

        ctx.Schedule(QuotaDeadline, new TEvPQ::TEvQuotaDeadlineCheck());
    }
}

void TPartition::Handle(TEvPQ::TEvQuotaDeadlineCheck::TPtr&, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::Handle TEvQuotaDeadlineCheck. Partition: " << Partition);

    FilterDeadlinedWrites(ctx);
}

bool TPartition::ProcessWrites(TEvKeyValue::TEvRequest* request, TInstant now, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::ProcessWrites. Partition: " << Partition);

    FilterDeadlinedWrites(ctx);

    if (!WriteQuota->CanExaust(now)) { // Waiting for partition quota.
        SetDeadlinesForWrites(ctx);
        return false;
    }

    if (WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota(ctx)) { // Waiting for topic quota.
        SetDeadlinesForWrites(ctx);

        if (StartTopicQuotaWaitTimeForCurrentBlob == TInstant::Zero() && !Requests.empty()) {
            StartTopicQuotaWaitTimeForCurrentBlob = now;
        }
        return false;
    }

    QuotaDeadline = TInstant::Zero();

    if (Requests.empty())
        return false;

    Y_VERIFY(request->Record.CmdWriteSize() == 0);
    Y_VERIFY(request->Record.CmdRenameSize() == 0);
    Y_VERIFY(request->Record.CmdDeleteRangeSize() == 0);
    const auto format = AppData(ctx)->PQConfig.GetEnableProtoSourceIdInfo()
        ? ESourceIdFormat::Proto
        : ESourceIdFormat::Raw;
    TSourceIdWriter sourceIdWriter(format);

    bool headCleared = AppendHeadWithNewWrites(request, ctx, sourceIdWriter);

    if (headCleared) {
        Y_VERIFY(!CompactedKeys.empty() || Head.PackedSize == 0);
        for (ui32 i = 0; i < TotalLevels; ++i) {
            DataKeysHead[i].Clear();
        }
    }

    if (NewHead.PackedSize == 0) { //nothing added to head - just compaction or tmp part blobs writed
        if (sourceIdWriter.GetSourceIdsToWrite().empty()) {
            return request->Record.CmdWriteSize() > 0
                || request->Record.CmdRenameSize() > 0
                || request->Record.CmdDeleteRangeSize() > 0;
        } else {
            sourceIdWriter.FillRequest(request, Partition);
            return true;
        }
    }

    sourceIdWriter.FillRequest(request, Partition);

    std::pair<TKey, ui32> res = GetNewWriteKey(headCleared);
    const auto& key = res.first;

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "writing blob: topic '" << TopicName() << "' partition " << Partition
                << " compactOffset " << key.GetOffset() << "," << key.GetCount()
                << " HeadOffset " << Head.Offset << " endOffset " << EndOffset << " curOffset "
                << NewHead.GetNextOffset() << " " << key.ToString()
                << " size " << res.second << " WTime " << ctx.Now().MilliSeconds()
    );

    AddNewWriteBlob(res, request, headCleared, ctx);
    return true;
}

void TPartition::FilterDeadlinedWrites(const TActorContext& ctx) {
    if (QuotaDeadline == TInstant::Zero() || QuotaDeadline > ctx.Now())
        return;

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::FilterDeadlinedWrites. Partition: " << Partition);

    std::deque<TMessage> newRequests;
    for (auto& w : Requests) {
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
    Requests = std::move(newRequests);
    QuotaDeadline = TInstant::Zero();

    UpdateWriteBufferIsFullState(ctx.Now());
}


void TPartition::HandleWrites(const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::HandleWrites. Partition: " << Partition);

    Become(&TThis::StateWrite);

    THolder<TEvKeyValue::TEvRequest> request(new TEvKeyValue::TEvRequest);

    Y_VERIFY(Head.PackedSize + NewHead.PackedSize <= 2 * MaxSizeCheck);
    
    TInstant now = ctx.Now();
    WriteCycleStartTime = now;

    bool haveData = false;
    bool haveCheckDisk = false;

    if (!Requests.empty() && DiskIsFull) {
        CancelAllWritesOnIdle(ctx);
        AddCheckDiskRequest(request.Get(), NumChannels);
        haveCheckDisk = true;
    } else {
        haveData = ProcessWrites(request.Get(), now, ctx);
    }
    bool haveDrop = CleanUp(request.Get(), ctx);

    ProcessReserveRequests(ctx);
    if (!haveData && !haveDrop && !haveCheckDisk) { //no data writed/deleted
        if (!Requests.empty()) { //there could be change ownership requests that
            bool res = ProcessWrites(request.Get(), now, ctx);
            Y_VERIFY(!res);
        }
        Y_VERIFY(Requests.empty() || !WriteQuota->CanExaust(now) || WaitingForPreviousBlobQuota() || WaitingForSubDomainQuota(ctx)); //in this case all writes must be processed or no quota left
        AnswerCurrentWrites(ctx); //in case if all writes are already done - no answer will be called on kv write, no kv write at all
        BecomeIdle(ctx);
        return;
    }

    WritesTotal.Inc();
    WriteBlobWithQuota(ctx, std::move(request));
}

void TPartition::RequestQuotaForWriteBlobRequest(size_t dataSize, ui64 cookie) {
    LOG_DEBUG_S(
            TActivationContext::AsActorContext(), NKikimrServices::PERSQUEUE,
            "Send write quota request." <<
            " Topic: \"" << TopicName() << "\"." <<
            " Partition: " << Partition << "." <<
            " Amount: " << dataSize << "." <<
            " Cookie: " << cookie
    );

    Send(MakeQuoterServiceID(),
        new TEvQuota::TEvRequest(
            TEvQuota::EResourceOperator::And,
            { TEvQuota::TResourceLeaf(TopicWriteQuoterPath, TopicWriteQuotaResourcePath, dataSize) },
            TDuration::Max()),
        0,
        cookie);
}

bool TPartition::WaitingForPreviousBlobQuota() const {
    return TopicQuotaRequestCookie != 0;
}

bool TPartition::WaitingForSubDomainQuota(const TActorContext& ctx, const ui64 withSize) const {
    return SubDomainOutOfSpace && AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota() && MeteringDataSize(ctx) + withSize > ReserveSize();
}

void TPartition::WriteBlobWithQuota(const TActorContext& ctx, THolder<TEvKeyValue::TEvRequest>&& request) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "TPartition::WriteBlobWithQuota. Partition: " << Partition);
    
    // Request quota and write blob.
    // Mirrored topics are not quoted in local dc.
    const bool skip = !IsQuotingEnabled() || TopicWriteQuotaResourcePath.empty();
    if (size_t quotaRequestSize = skip ? 0 : GetQuotaRequestSize(*request)) {
        // Request with data. We should check before attempting to write data whether we have enough quota.
        Y_VERIFY(!WaitingForPreviousBlobQuota());

        TopicQuotaRequestCookie = NextTopicWriteQuotaRequestCookie++;
        RequestQuotaForWriteBlobRequest(quotaRequestSize, TopicQuotaRequestCookie);
    }

    AddMetaKey(request.Get());

    WriteStartTime = TActivationContext::Now();
    // Write blob
#if 1
    // PQ -> CacheProxy -> KV
    Send(BlobCache, request.Release());
#else
    Send(Tablet, request.Release());
#endif
}

} // namespace NKikimr::NPQ
