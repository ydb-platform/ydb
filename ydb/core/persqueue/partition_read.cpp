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

#define VERIFY_RESULT_BLOB(blob, pos) \
    Y_VERIFY(!blob.Data.empty(), "Empty data. SourceId: %s, SeqNo: %" PRIu64, blob.SourceId.data(), blob.SeqNo); \
    Y_VERIFY(blob.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, blob.SeqNo);

namespace NKikimr::NPQ {

static const ui32 MAX_USER_ACTS = 1000;

void TPartition::FillReadFromTimestamps(const NKikimrPQ::TPQTabletConfig& config, const TActorContext& ctx) {
    TSet<TString> hasReadRule;

    for (auto& [consumer, userInfo] : UsersInfoStorage->GetAll()) {
        userInfo.ReadFromTimestamp = TInstant::Zero();
        userInfo.HasReadRule = false;
        hasReadRule.insert(consumer);
    }
    for (ui32 i = 0; i < config.ReadRulesSize(); ++i) {
        const auto& consumer = config.GetReadRules(i);
        auto& userInfo = UsersInfoStorage->GetOrCreate(consumer, ctx, 0);
        userInfo.HasReadRule = true;
        ui64 rrGen = i < config.ReadRuleGenerationsSize() ? config.GetReadRuleGenerations(i) : 0;
        if (userInfo.ReadRuleGeneration != rrGen) {
            THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer, 0, "", 0, 0,
                                                TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, rrGen);
            //
            // TODO(abcdef): заменить на вызов ProcessUserAct
            //
            AddUserAct(event.Release());
            userInfo.Session = "";
            userInfo.Offset = 0;
            if (userInfo.Important) {
                userInfo.Offset = StartOffset;
            }
            userInfo.Step = userInfo.Generation = 0;
        }
        hasReadRule.erase(consumer);
        TInstant ts = i < config.ReadFromTimestampsMsSize() ? TInstant::MilliSeconds(config.GetReadFromTimestampsMs(i)) : TInstant::Zero();
        if (!ts) ts += TDuration::MilliSeconds(1);
        if (!userInfo.ReadFromTimestamp || userInfo.ReadFromTimestamp > ts)
            userInfo.ReadFromTimestamp = ts;
    }
    for (auto& consumer : hasReadRule) {
        auto& userInfo = UsersInfoStorage->GetOrCreate(consumer, ctx);
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(0, consumer,
                                                                               0, "", 0, 0, TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0);
        if (!userInfo.Important && userInfo.LabeledCounters) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfo.LabeledCounters->GetGroup()));
        }
        userInfo.Session = "";
        userInfo.Offset = 0;
        userInfo.Step = userInfo.Generation = 0;
        //
        // TODO(abcdef): заменить на вызов ProcessUserAct
        //
        AddUserAct(event.Release());
    }
}

void TPartition::ProcessHasDataRequests(const TActorContext& ctx) {
    if (!InitDone)
        return;
    for (auto it = HasDataRequests.begin(); it != HasDataRequests.end();) {
        if (it->Offset < EndOffset) {
            TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());
            res->Record.SetEndOffset(EndOffset);
            res->Record.SetSizeLag(GetSizeLag(it->Offset));
            res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
            if (it->Cookie)
                res->Record.SetCookie(*(it->Cookie));
            ctx.Send(it->Sender, res.Release());
            if (!it->ClientId.empty()) {
                auto& userInfo = UsersInfoStorage->GetOrCreate(it->ClientId, ctx);
                userInfo.ForgetSubscription(ctx.Now());
            }
            it = HasDataRequests.erase(it);
        } else {
            break;
        }
    }
    for (auto it = HasDataDeadlines.begin(); it != HasDataDeadlines.end();) {
        if (it->Deadline <= ctx.Now()) {
            auto jt = HasDataRequests.find(it->Request);
            if (jt != HasDataRequests.end()) {
                TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());
                res->Record.SetEndOffset(EndOffset);
                res->Record.SetSizeLag(0);
                res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
                if (it->Request.Cookie)
                    res->Record.SetCookie(*(it->Request.Cookie));
                ctx.Send(it->Request.Sender, res.Release());
                if (!it->Request.ClientId.empty()) {
                    auto& userInfo = UsersInfoStorage->GetOrCreate(it->Request.ClientId, ctx);
                    userInfo.ForgetSubscription(ctx.Now());
                }
                HasDataRequests.erase(jt);
            }
            it = HasDataDeadlines.erase(it);
        } else {
            break;
        }
    }
}

void TPartition::UpdateAvailableSize(const TActorContext& ctx) {
    FilterDeadlinedWrites(ctx);

    auto now = ctx.Now();
    WriteQuota->Update(now);
    for (auto& [consumer, userInfo] : UsersInfoStorage->GetAll()) {
        while (true) {
            if (!userInfo.ReadQuota.CanExaust(now) && !userInfo.ReadRequests.empty()) {
                break;
            }
            if (!userInfo.ReadRequests.empty()) {
                auto ri(std::move(userInfo.ReadRequests.front().first));
                auto cookie = userInfo.ReadRequests.front().second;
                userInfo.ReadRequests.pop_front();
                ProcessRead(ctx, std::move(ri), cookie, false);
            } else
                break;
        }
    }
    ScheduleUpdateAvailableSize(ctx);
}

void TPartition::Handle(TEvPersQueue::TEvHasDataInfo::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    Y_VERIFY(record.HasSender());

    TActorId sender = ActorIdFromProto(record.GetSender());
    if (InitDone && EndOffset > (ui64)record.GetOffset()) { //already has data, answer right now
        TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());
        res->Record.SetEndOffset(EndOffset);
        res->Record.SetSizeLag(GetSizeLag(record.GetOffset()));
        res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
        if (record.HasCookie())
            res->Record.SetCookie(record.GetCookie());
        ctx.Send(sender, res.Release());
        return;
    } else {
        THasDataReq req{++HasDataReqNum, (ui64)record.GetOffset(), sender, record.HasCookie() ? TMaybe<ui64>(record.GetCookie()) : TMaybe<ui64>(),
                                                                                        record.HasClientId() && InitDone ? record.GetClientId() : ""};
        THasDataDeadline dl{TInstant::MilliSeconds(record.GetDeadline()), req};
        auto res = HasDataRequests.insert(req);
        HasDataDeadlines.insert(dl);
        Y_VERIFY(res.second);

        if (InitDone && record.HasClientId() && !record.GetClientId().empty()) {
            auto& userInfo = UsersInfoStorage->GetOrCreate(record.GetClientId(), ctx);
            ++userInfo.Subscriptions;
            userInfo.UpdateReadOffset((i64)EndOffset - 1, ctx.Now(), ctx.Now(), ctx.Now());
            userInfo.UpdateReadingTimeAndState(ctx.Now());
        }
    }
}

void TPartition::Handle(NReadSpeedLimiterEvents::TEvCounters::TPtr& ev, const TActorContext& /*ctx*/) {
    auto userInfo = UsersInfoStorage->GetIfExists(ev->Get()->User);
    if (userInfo && userInfo->ReadSpeedLimiter) {
        auto diff = ev->Get()->Counters.MakeDiffForAggr(userInfo->ReadSpeedLimiter->Baseline);
        TabletCounters.Populate(*diff.Get());
        ev->Get()->Counters.RememberCurrentStateAsBaseline(userInfo->ReadSpeedLimiter->Baseline);
    }
}

void TPartition::InitUserInfoForImportantClients(const TActorContext& ctx) {
    TSet<TString> important;
    for (const auto& importantUser : Config.GetPartitionConfig().GetImportantClientId()) {
        important.insert(importantUser);
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(importantUser);
        if (userInfo && !userInfo->Important && userInfo->LabeledCounters) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfo->LabeledCounters->GetGroup()));
            userInfo->SetImportant(true);
            continue;
        }
        if (!userInfo) {
            userInfo = &UsersInfoStorage->Create(ctx, importantUser, 0, true, "", 0, 0, 0, 0, TInstant::Zero());
        }
        if (userInfo->Offset < (i64)StartOffset)
            userInfo->Offset = StartOffset;
        ReadTimestampForOffset(importantUser, *userInfo, ctx);
    }
    for (auto& [consumer, userInfo] : UsersInfoStorage->GetAll()) {
        if (!important.contains(consumer) && userInfo.Important && userInfo.LabeledCounters) {
            ctx.Send(
                Tablet,
                new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfo.LabeledCounters->GetGroup())
            );
            userInfo.SetImportant(false);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TOffsetsResponse::TPartResult result;
    result.SetPartition(Partition);
    result.SetStartOffset(StartOffset);
    result.SetEndOffset(EndOffset);
    result.SetErrorCode(NPersQueue::NErrorCode::OK);
    result.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());

    if (!ev->Get()->ClientId.empty()) {
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(ev->Get()->ClientId);
        if (userInfo) {
            i64 offset = Max<i64>(userInfo->Offset, 0);
            result.SetClientOffset(userInfo->Offset);
            TInstant tmp = userInfo->GetWriteTimestamp() ? userInfo->GetWriteTimestamp() : GetWriteTimeEstimate(offset);
            result.SetWriteTimestampMS(tmp.MilliSeconds());
            result.SetCreateTimestampMS(userInfo->GetCreateTimestamp().MilliSeconds());
            result.SetClientReadOffset(userInfo->GetReadOffset());
            tmp = userInfo->GetReadWriteTimestamp() ? userInfo->GetReadWriteTimestamp() : GetWriteTimeEstimate(userInfo->GetReadOffset());
            result.SetReadWriteTimestampMS(tmp.MilliSeconds());
            result.SetReadCreateTimestampMS(userInfo->GetReadCreateTimestamp().MilliSeconds());
        }
    }
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionOffsetsResponse(result));
}

void TPartition::HandleOnInit(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TOffsetsResponse::TPartResult result;
    result.SetPartition(Partition);
    result.SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
    result.SetErrorReason("partition is not ready yet");
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionOffsetsResponse(result));
}

std::pair<TInstant, TInstant> TPartition::GetTime(const TUserInfo& userInfo, ui64 offset) const {
    TInstant wtime = userInfo.WriteTimestamp > TInstant::Zero() ? userInfo.WriteTimestamp : GetWriteTimeEstimate(offset);
    return std::make_pair(wtime, userInfo.CreateTimestamp);
}

void TPartition::Handle(TEvPQ::TEvGetClientOffset::TPtr& ev, const TActorContext& ctx) {
    auto& userInfo = UsersInfoStorage->GetOrCreate(ev->Get()->ClientId, ctx);
    Y_VERIFY(userInfo.Offset >= -1, "Unexpected Offset: %" PRIi64, userInfo.Offset);
    ui64 offset = Max<i64>(userInfo.Offset, 0);
    auto ts = GetTime(userInfo, offset);
    TabletCounters.Cumulative()[COUNTER_PQ_GET_CLIENT_OFFSET_OK].Increment(1);
    ReplyGetClientOffsetOk(ctx, ev->Get()->Cookie, userInfo.Offset, ts.first, ts.second);
}

void TPartition::Handle(TEvPQ::TEvSetClientInfo::TPtr& ev, const TActorContext& ctx) {
    if (size_t count = GetUserActCount(ev->Get()->ClientId); count > MAX_USER_ACTS) {
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::OVERLOAD,
            TStringBuilder() << "too big inflight: " << count);
        return;
    }

    const ui64& offset = ev->Get()->Offset;
    Y_VERIFY(offset <= (ui64)Max<i64>(), "Unexpected Offset: %" PRIu64, offset);

    AddUserAct(ev->Release());

    ProcessTxsAndUserActs(ctx);
}

template <typename T> // TCmdReadResult
static void AddResultBlob(T* read, const TClientBlob& blob, ui64 offset) {
    auto cc = read->AddResult();
    cc->SetOffset(offset);
    cc->SetData(blob.Data);
    cc->SetSourceId(blob.SourceId);
    cc->SetSeqNo(blob.SeqNo);
    cc->SetWriteTimestampMS(blob.WriteTimestamp.MilliSeconds());
    cc->SetCreateTimestampMS(blob.CreateTimestamp.MilliSeconds());
    cc->SetUncompressedSize(blob.UncompressedSize);
    cc->SetPartitionKey(blob.PartitionKey);
    cc->SetExplicitHash(blob.ExplicitHashKey);

    if (blob.PartData) {
        cc->SetPartNo(blob.PartData->PartNo);
        cc->SetTotalParts(blob.PartData->TotalParts);
        if (blob.PartData->PartNo == 0)
            cc->SetTotalSize(blob.PartData->TotalSize);
    }
}

template <typename T>
static void AddResultDebugInfo(const TEvPQ::TEvBlobResponse* response, T* readResult) {
    ui64 cachedSize = 0;
    ui32 cachedBlobs = 0;
    ui32 diskBlobs = 0;
    for (auto blob : response->GetBlobs()) {
        if (blob.Cached) {
            ++cachedBlobs;
            cachedSize += blob.Size;
        } else
            ++diskBlobs;
    }
    if (cachedSize)
        readResult->SetBlobsCachedSize(cachedSize);
    if (cachedBlobs)
        readResult->SetBlobsFromCache(cachedBlobs);
    if (diskBlobs)
        readResult->SetBlobsFromDisk(diskBlobs);
}

TReadAnswer TReadInfo::FormAnswer(
    const TActorContext& ctx,
    const TEvPQ::TEvBlobResponse& blobResponse,
    const ui64 endOffset,
    const ui32 partition,
    TUserInfo* userInfo,
    const ui64 cookie,
    const ui64 sizeLag,
    const TActorId& tablet,
    const NKikimrPQ::TPQTabletConfig::EMeteringMode meteringMode
) {
    Y_UNUSED(meteringMode);
    Y_UNUSED(partition);
    THolder<TEvPQ::TEvProxyResponse> answer = MakeHolder<TEvPQ::TEvProxyResponse>(cookie);
    NKikimrClient::TResponse& res = answer->Response;
    const TEvPQ::TEvBlobResponse* response = &blobResponse;

    if (HasError(blobResponse)) {
        return TReadAnswer{
            blobResponse.Error.ErrorStr.size(),
            MakeHolder<TEvPQ::TEvError>(blobResponse.Error.ErrorCode, blobResponse.Error.ErrorStr, cookie)
        };
    }

    res.SetStatus(NMsgBusProxy::MSTATUS_OK);
    res.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto readResult = res.MutablePartitionResponse()->MutableCmdReadResult();
    readResult->SetWaitQuotaTimeMs(WaitQuotaTime.MilliSeconds());
    readResult->SetMaxOffset(endOffset);
    readResult->SetRealReadOffset(Offset);
    readResult->SetReadFromTimestampMs(ReadTimestampMs);
    Y_VERIFY(endOffset <= (ui64)Max<i64>(), "Max offset is too big: %" PRIu64, endOffset);

    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "FormAnswer " << Blobs.size());

    AddResultDebugInfo(response, readResult);

    ui32 cnt = 0;
    ui32 size = 0;

    ui32 lastBlobSize = 0;
    const TVector<TRequestedBlob>& blobs = response->GetBlobs();

    auto updateUsage = [&](const TClientBlob& blob) {
        size += blob.GetBlobSize();
        lastBlobSize += blob.GetBlobSize();
        if (blob.IsLastPart()) {
            bool messageSkippingBehaviour = AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
                    ReadTimestampMs > blob.WriteTimestamp.MilliSeconds();
            ++cnt;
            if (messageSkippingBehaviour) {
                --cnt;
                size -= lastBlobSize;
            }
            lastBlobSize = 0;
            return (size >= Size || cnt >= Count);
        }
        return !AppData()->PQConfig.GetTopicsAreFirstClassCitizen() && (size >= Size || cnt >= Count);
    };

    Y_VERIFY(blobs.size() == Blobs.size());
    response->Check();
    bool needStop = false;
    for (ui32 pos = 0; pos < blobs.size() && !needStop; ++pos) {
        Y_VERIFY(Blobs[pos].Offset == blobs[pos].Offset, "Mismatch %" PRIu64 " vs %" PRIu64, Blobs[pos].Offset, blobs[pos].Offset);
        Y_VERIFY(Blobs[pos].Count == blobs[pos].Count, "Mismatch %" PRIu32 " vs %" PRIu32, Blobs[pos].Count, blobs[pos].Count);

        ui64 offset = blobs[pos].Offset;
        ui32 count = blobs[pos].Count;
        ui16 partNo = blobs[pos].PartNo;
        ui16 internalPartsCount = blobs[pos].InternalPartsCount;
        const TString& blobValue = blobs[pos].Value;

        if (blobValue.empty()) { // this is ok. Means that someone requested too much data or retention race
            LOG_DEBUG(ctx, NKikimrServices::PERSQUEUE, "Not full answer here!");
            ui64 answerSize = answer->Response.ByteSize();
            if (userInfo && Destination != 0) {
                userInfo->ReadDone(ctx, ctx.Now(), answerSize, cnt, ClientDC,
                        tablet);
            }
            readResult->SetSizeLag(sizeLag - size);
            return {answerSize, std::move(answer)};
        }
        Y_VERIFY(blobValue.size() == blobs[pos].Size, "value for offset %" PRIu64 " count %u size must be %u, but got %u",
                                                        offset, count, blobs[pos].Size, (ui32)blobValue.size());

        if (offset > Offset || (offset == Offset && partNo > PartNo)) { // got gap
            Offset = offset;
            PartNo = partNo;
        }
        Y_VERIFY(offset <= Offset);
        Y_VERIFY(offset < Offset || partNo <= PartNo);
        TKey key(TKeyPrefix::TypeData, 0, offset, partNo, count, internalPartsCount, false);
        for (TBlobIterator it(key, blobValue); it.IsValid() && !needStop; it.Next()) {
            TBatch batch = it.GetBatch();
            auto& header = batch.Header;
            batch.Unpack();

            ui32 pos = 0;
            if (header.GetOffset() > Offset || header.GetOffset() == Offset && header.GetPartNo() >= PartNo) {
                pos = 0;
            } else {
                pos = batch.FindPos(Offset, PartNo);
            }
            offset += header.GetCount();

            if (pos == Max<ui32>()) // this batch does not contain data to read, skip it
                continue;


            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "FormAnswer processing batch offset "
                << (offset - header.GetCount()) <<  " totakecount " << count << " count " << header.GetCount() << " size " << header.GetPayloadSize() << " from pos " << pos << " cbcount " << batch.Blobs.size());

            ui32 i = 0;
            for (i = pos; i < batch.Blobs.size(); ++i) {
                TClientBlob &res = batch.Blobs[i];
                VERIFY_RESULT_BLOB(res, i);

                Y_VERIFY(PartNo == res.GetPartNo(), "pos %" PRIu32 " i %" PRIu32 " Offset %" PRIu64 " PartNo %" PRIu16 " offset %" PRIu64 " partNo %" PRIu16,
                         pos, i, Offset, PartNo, offset, res.GetPartNo());

                if (userInfo) {
                    userInfo->AddTimestampToCache(
                                                  Offset, res.WriteTimestamp, res.CreateTimestamp,
                                                  Destination != 0, ctx.Now()
                                              );
                }

                AddResultBlob(readResult, res, Offset);

                if (res.IsLastPart()) {
                    PartNo = 0;
                    ++Offset;
                } else {
                    ++PartNo;
                }
                if (updateUsage(res)) {
                    break;
                }
            }

            if (i != batch.Blobs.size()) {//not fully processed batch - next definetely will not be processed
                needStop = true;
            }
        }
    }

    if (!needStop && cnt < Count && size < Size) { // body blobs are fully processed and need to take more data
        if (CachedOffset > Offset) {
            lastBlobSize = 0;
            Offset = CachedOffset;
        }

        for (const auto& writeBlob : Cached) {
            VERIFY_RESULT_BLOB(writeBlob, 0u);

            readResult->SetBlobsCachedSize(readResult->GetBlobsCachedSize() + writeBlob.GetBlobSize());

            if (userInfo) {
                userInfo->AddTimestampToCache(
                    Offset, writeBlob.WriteTimestamp, writeBlob.CreateTimestamp,
                    Destination != 0, ctx.Now()
                );
            }
            AddResultBlob(readResult, writeBlob, Offset);

            if (writeBlob.IsLastPart()) {
                ++Offset;
            }
            if (updateUsage(writeBlob)) {
                break;
            }
        }
    }
    Y_VERIFY(Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, Offset);
    ui64 answerSize = answer->Response.ByteSize();
    if (userInfo && Destination != 0) {
        userInfo->ReadDone(ctx, ctx.Now(), answerSize, cnt, ClientDC,
                        tablet);

    }
    readResult->SetSizeLag(sizeLag - size);
    return {answerSize, std::move(answer)};
}

void TPartition::Handle(TEvPQ::TEvReadTimeout::TPtr& ev, const TActorContext& ctx) {
    auto res = Subscriber.OnTimeout(ev);
    if (!res)
        return;
    TReadAnswer answer(res->FormAnswer(ctx, res->Offset, Partition, nullptr, res->Destination, 0, Tablet, Config.GetMeteringMode()));
    ctx.Send(Tablet, answer.Event.Release());
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, " waiting read cookie " << ev->Get()->Cookie
        << " partition " << Partition << " read timeout for " << res->User << " offset " << res->Offset);
    auto& userInfo = UsersInfoStorage->GetOrCreate(res->User, ctx);

    userInfo.ForgetSubscription(ctx.Now());
    OnReadRequestFinished(std::move(res.GetRef()), answer.Size);
}


TVector<TRequestedBlob> TPartition::GetReadRequestFromBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, ui32* rcount, ui32* rsize) {
    Y_VERIFY(rcount && rsize);
    ui32& count = *rcount;
    ui32& size = *rsize;
    count = size = 0;
    TVector<TRequestedBlob> blobs;
    if (!DataKeysBody.empty() && (Head.Offset > startOffset || Head.Offset == startOffset && Head.PartNo > partNo)) { //will read smth from body
        auto it = std::upper_bound(DataKeysBody.begin(), DataKeysBody.end(), std::make_pair(startOffset, partNo),
            [](const std::pair<ui64, ui16>& offsetAndPartNo, const TDataKey& p) { return offsetAndPartNo.first < p.Key.GetOffset() || offsetAndPartNo.first == p.Key.GetOffset() && offsetAndPartNo.second < p.Key.GetPartNo();});
        if (it == DataKeysBody.begin()) //could be true if data is deleted or gaps are created
            return blobs;
        Y_VERIFY(it != DataKeysBody.begin()); //always greater, startoffset can't be less that StartOffset
        Y_VERIFY(it == DataKeysBody.end() || it->Key.GetOffset() > startOffset || it->Key.GetOffset() == startOffset && it->Key.GetPartNo() > partNo);
        --it;
        Y_VERIFY(it->Key.GetOffset() < startOffset || (it->Key.GetOffset() == startOffset && it->Key.GetPartNo() <= partNo));
        ui32 cnt = 0;
        ui32 sz = 0;
        if (startOffset > it->Key.GetOffset() + it->Key.GetCount()) { //there is a gap
            ++it;
            if (it != DataKeysBody.end()) {
                cnt = it->Key.GetCount();
                sz = it->Size;
            }
        } else {
            Y_VERIFY(it->Key.GetCount() >= (startOffset - it->Key.GetOffset()));
            cnt = it->Key.GetCount() - (startOffset - it->Key.GetOffset()); //don't count all elements from first blob
            sz = (cnt == it->Key.GetCount() ? it->Size : 0); //not readed client blobs can be of ~8Mb, so don't count this size at all
        }
        while (it != DataKeysBody.end() && (size < maxSize && count < maxCount || count == 0)) { //count== 0 grants that blob with offset from ReadFromTimestamp will be readed
            size += sz;
            count += cnt;
            TRequestedBlob reqBlob(it->Key.GetOffset(), it->Key.GetPartNo(), it->Key.GetCount(),
                                   it->Key.GetInternalPartsCount(), it->Size, TString(), it->Key);
            blobs.push_back(reqBlob);

            ++it;
            if (it == DataKeysBody.end())
                break;
            sz = it->Size;
            cnt = it->Key.GetCount();
        }
    }
    return blobs;
}

TVector<TClientBlob> TPartition::GetReadRequestFromHead(const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, const ui64 readTimestampMs, ui32* rcount, ui32* rsize, ui64* insideHeadOffset) {
    ui32& count = *rcount;
    ui32& size = *rsize;
    TVector<TClientBlob> res;
    std::optional<ui64> firstAddedBlobOffset{};
    ui32 pos = 0;
    if (startOffset > Head.Offset || startOffset == Head.Offset && partNo > Head.PartNo) {
        pos = Head.FindPos(startOffset, partNo);
        Y_VERIFY(pos != Max<ui32>());
    }
    ui32 lastBlobSize = 0;
    for (;pos < Head.Batches.size(); ++pos) {

        TVector<TClientBlob> blobs;
        Head.Batches[pos].UnpackTo(&blobs);
        ui32 i = 0;
        ui64 offset = Head.Batches[pos].GetOffset();
        ui16 pno = Head.Batches[pos].GetPartNo();
        for (; i < blobs.size(); ++i) {

            ui64 curOffset = offset;

            Y_VERIFY(pno == blobs[i].GetPartNo());
            bool skip = offset < startOffset || offset == startOffset &&
                blobs[i].GetPartNo() < partNo;
            if (blobs[i].IsLastPart()) {
                ++offset;
                pno = 0;
            } else {
                ++pno;
            }
            if (skip) continue;
            if (blobs[i].IsLastPart()) {
                bool messageSkippingBehaviour = AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
                        readTimestampMs > blobs[i].WriteTimestamp.MilliSeconds();
                ++count;
                if (messageSkippingBehaviour) { //do not count in limits; message will be skippend in proxy
                    --count;
                    size -= lastBlobSize;
                }
                lastBlobSize = 0;

                if (count > maxCount) // blob is counted already
                    break;
                if (size > maxSize)
                    break;
            }
            size += blobs[i].GetBlobSize();
            lastBlobSize += blobs[i].GetBlobSize();
            res.push_back(blobs[i]);

            if (!firstAddedBlobOffset)
                firstAddedBlobOffset = curOffset;

        }
        if (i < blobs.size()) // already got limit
            break;
    }
    *insideHeadOffset = firstAddedBlobOffset.value_or(*insideHeadOffset);
    return res;
}

void TPartition::Handle(TEvPQ::TEvRead::TPtr& ev, const TActorContext& ctx) {
    auto read = ev->Get();

    if (read->Count == 0) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
        ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::BAD_REQUEST, "no infinite flows allowed - count is not set or 0");
        return;
    }
    if (read->Offset < StartOffset) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_SMALL_OFFSET].Increment(1);
        read->Offset = StartOffset;
        if (read->PartNo > 0) {
            LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                        "I was right, there could be rewinds and deletions at once! Topic " << TopicConverter->GetClientsideName() <<
                        " partition " << Partition <<
                        " readOffset " << read->Offset <<
                        " readPartNo " << read->PartNo <<
                        " startOffset " << StartOffset);
            ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET,
                       "client requested not from first part, and this part is lost");
            return;
        }
    }
    if (read->Offset > EndOffset || read->Offset == EndOffset && read->PartNo > 0) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_BIG_OFFSET].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
        LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE,
                    "reading from too big offset - topic " << TopicConverter->GetClientsideName() <<
                    " partition " << Partition <<
                    " client " << read->ClientId <<
                    " EndOffset " << EndOffset <<
                    " offset " << read->Offset);
        ReplyError(ctx, read->Cookie, NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET,
                                      TStringBuilder() << "trying to read from future. ReadOffset " <<
                                      read->Offset << ", " << read->PartNo << " EndOffset " << EndOffset);
        return;
    }

    const TString& user = read->ClientId;

    Y_VERIFY(read->Offset <= EndOffset);

    auto& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);

    if (!read->SessionId.empty()) {
        if (userInfo.Session != read->SessionId) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_NO_SESSION].Increment(1);
            TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
            ReplyError(ctx, read->Cookie, NPersQueue::NErrorCode::READ_ERROR_NO_SESSION,
                TStringBuilder() << "no such session '" << read->SessionId << "'");
            return;
        }
    }

    if (userInfo.ReadSpeedLimiter) {
        Send(userInfo.ReadSpeedLimiter->Actor, new NReadSpeedLimiterEvents::TEvRequest(ev.Release()));
    } else {
        DoRead(ev.Release(), TDuration::Zero(), ctx);
    }
}

void TPartition::Handle(NReadSpeedLimiterEvents::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    DoRead(ev->Get()->ReadRequest.Release(), ev->Get()->WaitTime, ctx);
}

void TPartition::DoRead(TEvPQ::TEvRead::TPtr ev, TDuration waitQuotaTime, const TActorContext& ctx) {
    auto read = ev->Get();
    const TString& user = read->ClientId;
    auto& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);

    ui64 offset = read->Offset;
    if (read->PartNo == 0 && (read->MaxTimeLagMs > 0 || read->ReadTimestampMs > 0 || userInfo.ReadFromTimestamp > TInstant::MilliSeconds(1))) {
        TInstant timestamp = read->MaxTimeLagMs > 0 ? ctx.Now() - TDuration::MilliSeconds(read->MaxTimeLagMs) : TInstant::Zero();
        timestamp = Max(timestamp, TInstant::MilliSeconds(read->ReadTimestampMs));
        timestamp = Max(timestamp, userInfo.ReadFromTimestamp);
        offset = Max(GetOffsetEstimate(DataKeysBody, timestamp, Min(Head.Offset, EndOffset - 1)), offset);
        userInfo.ReadOffsetRewindSum += offset - read->Offset;
    }

    TReadInfo info(user, read->ClientDC, offset, read->PartNo, read->Count, read->Size, read->Cookie, read->ReadTimestampMs, waitQuotaTime);

    ui64 cookie = Cookie++;

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "read cookie " << cookie << " Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                << " user " << user
                << " offset " << read->Offset << " count " << read->Count << " size " << read->Size << " endOffset " << EndOffset
                << " max time lag " << read->MaxTimeLagMs << "ms effective offset " << offset
    );


    if (offset == EndOffset) {
        if (read->Timeout > 30000) {
            LOG_DEBUG_S(
                    ctx, NKikimrServices::PERSQUEUE,
                    "too big read timeout " << " Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                        << " user " << read->ClientId << " offset " << read->Offset << " count " << read->Count
                        << " size " << read->Size << " endOffset " << EndOffset << " max time lag " << read->MaxTimeLagMs
                        << "ms effective offset " << offset
            );
            read->Timeout = 30000;
        }
        Subscriber.AddSubscription(std::move(info), read->Timeout, cookie, ctx);
        ++userInfo.Subscriptions;
        userInfo.UpdateReadOffset((i64)offset - 1, userInfo.WriteTimestamp, userInfo.CreateTimestamp, ctx.Now());

        return;
    }

    Y_VERIFY(offset < EndOffset);

    ProcessRead(ctx, std::move(info), cookie, false);
}

void TPartition::OnReadRequestFinished(TReadInfo&& info, ui64 answerSize) {
    auto userInfo = UsersInfoStorage->GetIfExists(info.User);
    Y_VERIFY(userInfo);

    if (Config.GetMeteringMode() != NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY) {
        return;
    }

    if (userInfo->ReadSpeedLimiter) {
        Send(
            userInfo->ReadSpeedLimiter->Actor,
            new NReadSpeedLimiterEvents::TEvConsumed(answerSize, info.Destination)
        );
    }
}

void TPartition::ReadTimestampForOffset(const TString& user, TUserInfo& userInfo, const TActorContext& ctx) {
    if (userInfo.ReadScheduled)
        return;
    userInfo.ReadScheduled = true;
    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition <<
            " user " << user << " readTimeStamp for offset " << userInfo.Offset << " initiated " <<
            " queuesize " << UpdateUserInfoTimestamp.size() << " startOffset " << StartOffset <<
            " ReadingTimestamp " << ReadingTimestamp << " rrg " << userInfo.ReadRuleGeneration
    );

    if (ReadingTimestamp) {
        UpdateUserInfoTimestamp.push_back(std::make_pair(user, userInfo.ReadRuleGeneration));
        return;
    }
    if (userInfo.Offset < (i64)StartOffset) {
        userInfo.ReadScheduled = false;
        auto now = ctx.Now();
        userInfo.CreateTimestamp = now - TDuration::Seconds(Max(86400, Config.GetPartitionConfig().GetLifetimeSeconds()));
        userInfo.WriteTimestamp = now - TDuration::Seconds(Max(86400, Config.GetPartitionConfig().GetLifetimeSeconds()));
        userInfo.ActualTimestamps = true;
        if (userInfo.ReadOffset + 1 < userInfo.Offset) {
            userInfo.ReadOffset = userInfo.Offset - 1;
            userInfo.ReadCreateTimestamp = userInfo.CreateTimestamp;
            userInfo.ReadWriteTimestamp = userInfo.WriteTimestamp;
        }

        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_OFFSET_IS_LOST].Increment(1);
        return;
    }

    if (userInfo.Offset >= (i64)EndOffset || StartOffset == EndOffset) {
        userInfo.ReadScheduled = false;
        return;
    }

    Y_VERIFY(!ReadingTimestamp);

    ReadingTimestamp = true;
    ReadingForUser = user;
    ReadingForOffset = userInfo.Offset;
    ReadingForUserReadRuleGeneration = userInfo.ReadRuleGeneration;

    for (const auto& user : UpdateUserInfoTimestamp) {
        Y_VERIFY(user.first != ReadingForUser || user.second != ReadingForUserReadRuleGeneration);
    }

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
            << " user " << user << " send read request for offset " << userInfo.Offset << " initiated "
            << " queuesize " << UpdateUserInfoTimestamp.size() << " startOffset " << StartOffset
            << " ReadingTimestamp " << ReadingTimestamp << " rrg " << ReadingForUserReadRuleGeneration
    );


    THolder<TEvPQ::TEvRead> event = MakeHolder<TEvPQ::TEvRead>(0, userInfo.Offset, 0, 1, "",
                                                               user, 0, MAX_BLOB_PART_SIZE * 2, 0, 0, "",
                                                               false);

    ctx.Send(ctx.SelfID, event.Release());
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_CACHE_MISS].Increment(1);
}

void TPartition::ProcessTimestampsForNewData(const ui64 prevEndOffset, const TActorContext& ctx) {
    for (auto& [consumer, userInfo] : UsersInfoStorage->GetAll()) {
        if (userInfo.Offset >= (i64)prevEndOffset && userInfo.Offset < (i64)EndOffset) {
            ReadTimestampForOffset(consumer, userInfo, ctx);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext& ctx) {
    ReadingTimestamp = false;
    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);
    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        LOG_INFO_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "'" <<
            " partition " << Partition <<
            " user " << ReadingForUser <<
            " readTimeStamp for other generation or no client info at all"
        );

        ProcessTimestampRead(ctx);
        return;
    }

    LOG_DEBUG_S(
            ctx, NKikimrServices::PERSQUEUE,
            "Topic '" << TopicConverter->GetClientsideName() << "'" <<
            " partition " << Partition <<
            " user " << ReadingForUser <<
            " readTimeStamp done, result " << userInfo->WriteTimestamp.MilliSeconds() <<
            " queuesize " << UpdateUserInfoTimestamp.size() <<
            " startOffset " << StartOffset
    );
    Y_VERIFY(userInfo->ReadScheduled);
    userInfo->ReadScheduled = false;
    Y_VERIFY(ReadingForUser != "");

    if (!userInfo->ActualTimestamps) {
        LOG_INFO_S(
            ctx,
            NKikimrServices::PERSQUEUE,
            "Reading Timestamp failed for offset " << ReadingForOffset << " ( "<< userInfo->Offset << " ) " << ev->Get()->Response.DebugString()
        );
        if (ev->Get()->Response.GetStatus() == NMsgBusProxy::MSTATUS_OK &&
            ev->Get()->Response.GetErrorCode() == NPersQueue::NErrorCode::OK &&
            ev->Get()->Response.GetPartitionResponse().HasCmdReadResult() &&
            ev->Get()->Response.GetPartitionResponse().GetCmdReadResult().ResultSize() > 0 &&
            (i64)ev->Get()->Response.GetPartitionResponse().GetCmdReadResult().GetResult(0).GetOffset() >= userInfo->Offset) {
                //offsets is inside gap - return timestamp of first record after gap
            const auto& res = ev->Get()->Response.GetPartitionResponse().GetCmdReadResult().GetResult(0);
            userInfo->WriteTimestamp = TInstant::MilliSeconds(res.GetWriteTimestampMS());
            userInfo->CreateTimestamp = TInstant::MilliSeconds(res.GetCreateTimestampMS());
            userInfo->ActualTimestamps = true;
            if (userInfo->ReadOffset + 1 < userInfo->Offset) {
                userInfo->ReadOffset = userInfo->Offset - 1;
                userInfo->ReadWriteTimestamp = userInfo->WriteTimestamp;
                userInfo->ReadCreateTimestamp = userInfo->CreateTimestamp;
            }
        } else {
            UpdateUserInfoTimestamp.push_back(std::make_pair(ReadingForUser, ReadingForUserReadRuleGeneration));
            userInfo->ReadScheduled = true;
        }
        TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_ERROR].Increment(1);
    }
    ProcessTimestampRead(ctx);
}


void TPartition::ProcessTimestampRead(const TActorContext& ctx) {
    ReadingForUser = "";
    ReadingForOffset = 0;
    ReadingForUserReadRuleGeneration = 0;
    while (!ReadingTimestamp && !UpdateUserInfoTimestamp.empty()) {
        TString user = UpdateUserInfoTimestamp.front().first;
        ui64 readRuleGeneration = UpdateUserInfoTimestamp.front().second;
        UpdateUserInfoTimestamp.pop_front();
        auto userInfo = UsersInfoStorage->GetIfExists(user);
        if (!userInfo || !userInfo->ReadScheduled || userInfo->ReadRuleGeneration != readRuleGeneration)
            continue;
        userInfo->ReadScheduled = false;
        if (userInfo->Offset == (i64)EndOffset)
            continue;
        ReadTimestampForOffset(user, *userInfo, ctx);
    }
    Y_VERIFY(ReadingTimestamp || UpdateUserInfoTimestamp.empty());
}

void TPartition::HandleSetOffsetResponse(ui64 cookie, const TActorContext& ctx) {
    Y_VERIFY(cookie == SET_OFFSET_COOKIE);


    if (ChangeConfig) {
        EndChangePartitionConfig(ChangeConfig->Config,
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
        } else {
            auto ui = UsersInfoStorage->GetIfExists(user);
            if (ui && ui->LabeledCounters) {
                ScheduleDropPartitionLabeledCounters(ui->LabeledCounters->GetGroup());
            }

            UsersInfoStorage->Remove(user, ctx);
        }
    }

    for (auto& [actor, reply] : Replies) {
        ctx.Send(actor, reply.release());
    }

    PendingUsersInfo.clear();
    Replies.clear();
    AffectedUsers.clear();

    UsersInfoWriteInProgress = false;

    TxIdHasChanged = false;

    if (ChangeConfig) {
        ReportCounters(ctx);
        ChangeConfig = nullptr;
    }


    ProcessTxsAndUserActs(ctx);
}

void TPartition::ProcessRead(const TActorContext& ctx, TReadInfo&& info, const ui64 cookie, bool subscription) {
    ui32 count = 0;
    ui32 size = 0;

    Y_VERIFY(!info.User.empty());
    auto& userInfo = UsersInfoStorage->GetOrCreate(info.User, ctx);

    if (subscription) {
        userInfo.ForgetSubscription(ctx.Now());
    }

    if (!userInfo.ReadQuota.CanExaust(ctx.Now())) {
        userInfo.ReadRequests.push_back({std::move(info), cookie});
        userInfo.UpdateReadingTimeAndState(ctx.Now());
        return;
    }
    TVector<TRequestedBlob> blobs = GetReadRequestFromBody(info.Offset, info.PartNo, info.Count, info.Size, &count, &size);
    info.Blobs = blobs;
    ui64 lastOffset = info.Offset + Min(count, info.Count);
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "read cookie " << cookie << " added " << info.Blobs.size()
                << " blobs, size " << size << " count " << count << " last offset " << lastOffset);

    if (blobs.empty() || blobs.back().Key == DataKeysBody.back().Key) { // read from head only when all blobs from body processed
        ui64 insideHeadOffset{0};
        info.Cached = GetReadRequestFromHead(info.Offset, info.PartNo, info.Count, info.Size, info.ReadTimestampMs, &count, &size, &insideHeadOffset);
        info.CachedOffset = insideHeadOffset;
    }
    if (info.Destination != 0) {
        ++userInfo.ActiveReads;
        userInfo.UpdateReadingTimeAndState(ctx.Now());
    }

    if (info.Blobs.empty()) { //all from head, answer right now
        LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Reading cookie " << cookie << ". All data is from uncompacted head.");

        TReadAnswer answer(info.FormAnswer(
            ctx, EndOffset, Partition, &UsersInfoStorage->GetOrCreate(info.User, ctx),
            info.Destination, GetSizeLag(info.Offset), Tablet, Config.GetMeteringMode()
        ));
        const auto& resp = dynamic_cast<TEvPQ::TEvProxyResponse*>(answer.Event.Get())->Response;
        if (info.IsSubscription) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_OK].Increment(1);
        }
        TabletCounters.Cumulative()[COUNTER_PQ_READ_HEAD_ONLY_OK].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_HEAD_ONLY].IncrementFor((ctx.Now() - info.Timestamp).MilliSeconds());
        TabletCounters.Cumulative()[COUNTER_PQ_READ_BYTES].Increment(resp.ByteSize());
        ctx.Send(info.Destination != 0 ? Tablet : ctx.SelfID, answer.Event.Release());
        OnReadRequestFinished(std::move(info), answer.Size);
        return;
    }

    const TString user = info.User;
    bool res = ReadInfo.insert({cookie, std::move(info)}).second;
    Y_VERIFY(res);

    THolder<TEvPQ::TEvBlobRequest> request(new TEvPQ::TEvBlobRequest(user, cookie, Partition,
                                                                     lastOffset, std::move(blobs)));

    ctx.Send(BlobCache, request.Release());
}

} // namespace NKikimr::NPQ
