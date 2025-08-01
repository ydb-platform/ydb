#include "event_helpers.h"
#include "mirrorer.h"
#include "partition_log.h"
#include "partition_util.h"
#include "partition_common.h"
#include "read.h"
#include "dread_cache_service/caching_service.h"

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
    Y_ABORT_UNLESS(blob.SeqNo <= (ui64)Max<i64>(), "SeqNo is too big: %" PRIu64, blob.SeqNo);

namespace NKikimr::NPQ {

TMaybe<TInstant> GetReadFrom(ui32 maxTimeLagMs, ui64 readTimestampMs, TInstant consumerReadFromTimestamp, const TActorContext& ctx) {
    if (!(maxTimeLagMs > 0 || readTimestampMs > 0 || consumerReadFromTimestamp > TInstant::MilliSeconds(1))) {
        return {};
    }

    TInstant timestamp = maxTimeLagMs > 0 ? ctx.Now() - TDuration::MilliSeconds(maxTimeLagMs) : TInstant::Zero();
    timestamp = Max(timestamp, TInstant::MilliSeconds(readTimestampMs));
    timestamp = Max(timestamp, consumerReadFromTimestamp);
    return timestamp;
}

ui64 TPartition::GetReadOffset(ui64 offset, TMaybe<TInstant> readTimestamp) const {
    if (!readTimestamp) {
        return offset;
    }
    auto estimatedOffset = GetOffsetEstimate(CompactionBlobEncoder.DataKeysBody, *readTimestamp, Max<ui64>());
    if (estimatedOffset == Max<ui64>()) {
        estimatedOffset = GetOffsetEstimate(BlobEncoder.DataKeysBody, *readTimestamp, Max<ui64>());
    }
    if (estimatedOffset == Max<ui64>()) {
        estimatedOffset = Min(BlobEncoder.Head.Offset, BlobEncoder.EndOffset - 1);
    }
    return Max(estimatedOffset, offset);
}

void TPartition::SendReadingFinished(const TString& consumer) {
    Send(Tablet, new TEvPQ::TEvReadingPartitionStatusRequest(consumer, Partition.OriginalPartitionId, TabletGeneration, ++PQRBCookie));
}

void TPartition::FillReadFromTimestamps(const TActorContext& ctx) {
    if (IsSupportive()) {
        return;
    }

    TSet<TString> hasReadRule;

    for (auto& [consumer, userInfo] : UsersInfoStorage->GetAll()) {
        userInfo.ReadFromTimestamp = TInstant::Zero();
        userInfo.HasReadRule = false;
        hasReadRule.insert(consumer);
    }

    for (auto& consumer : Config.GetConsumers()) {
        auto& userInfo = UsersInfoStorage->GetOrCreate(consumer.GetName(), ctx, 0);
        userInfo.HasReadRule = true;

        if (userInfo.ReadRuleGeneration != consumer.GetGeneration()) {
            THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(
                    0, consumer.GetName(), 0, "", 0, 0, 0, TActorId{}, TEvPQ::TEvSetClientInfo::ESCI_INIT_READ_RULE, consumer.GetGeneration()
            );
            //
            // TODO(abcdef): заменить на вызов ProcessUserAct
            //
            AddUserAct(event.Release());
            userInfo.Session = "";
            userInfo.Offset = 0;
            if (userInfo.Important) {
                userInfo.Offset = CompactionBlobEncoder.StartOffset;
            }
            userInfo.Step = userInfo.Generation = 0;
        }
        hasReadRule.erase(consumer.GetName());
        TInstant ts = TInstant::MilliSeconds(consumer.GetReadFromTimestampsMs());
        if (!ts) ts += TDuration::MilliSeconds(1);
        if (!userInfo.ReadFromTimestamp || userInfo.ReadFromTimestamp > ts)
            userInfo.ReadFromTimestamp = ts;
    }

    for (auto& consumer : hasReadRule) {
        auto& userInfo = UsersInfoStorage->GetOrCreate(consumer, ctx);
        if (userInfo.NoConsumer) {
            continue;
        }
        THolder<TEvPQ::TEvSetClientInfo> event = MakeHolder<TEvPQ::TEvSetClientInfo>(
                0, consumer, 0, "", 0, 0, 0, TActorId{}, TEvPQ::TEvSetClientInfo::ESCI_DROP_READ_RULE, 0
        );
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

TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> TPartition::MakeHasDataInfoResponse(ui64 lagSize, const TMaybe<ui64>& cookie, bool readingFinished) {
    TAutoPtr<TEvPersQueue::TEvHasDataInfoResponse> res(new TEvPersQueue::TEvHasDataInfoResponse());

    res->Record.SetEndOffset(BlobEncoder.EndOffset);
    res->Record.SetSizeLag(lagSize);
    res->Record.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());
    if (cookie) {
        res->Record.SetCookie(*cookie);
    }
    GetResultPostProcessor<NKikimrPQ::THasDataInfoResponse>()(readingFinished, res->Record);

    return res;
}

bool TPartition::ProcessHasDataRequest(const THasDataReq& request, const TActorContext& ctx) {
    auto sendResponse = [&](ui64 lagSize, bool readingFinished) {
        auto response = MakeHasDataInfoResponse(lagSize, request.Cookie, readingFinished);
        ctx.Send(request.Sender, response.Release());
    };

    if (!IsActive()) {
        if (request.Offset < BlobEncoder.EndOffset && (!request.ReadTimestamp || *request.ReadTimestamp <= EndWriteTimestamp)) {
            sendResponse(GetSizeLag(request.Offset), false);
        } else {
            sendResponse(0, true);

            auto now = ctx.Now();
            auto& userInfo = UsersInfoStorage->GetOrCreate(request.ClientId, ctx);
            userInfo.UpdateReadOffset((i64)BlobEncoder.EndOffset - 1, now, now, now, true);
        }
    } else if (request.Offset < BlobEncoder.EndOffset) {
        sendResponse(GetSizeLag(request.Offset), false);
    } else {
        return false;
    }

    return true;
}


void TPartition::ProcessHasDataRequests(const TActorContext& ctx) {
    if (!InitDone) {
        return;
    }
    auto now = ctx.Now();

    auto forgetSubscription = [&](const TString clientId) {
        if (!clientId.empty()) {
            auto& userInfo = UsersInfoStorage->GetOrCreate(clientId, ctx);
            userInfo.ForgetSubscription(BlobEncoder.EndOffset, now);
        }
    };

    for (auto request = HasDataRequests.begin(); request != HasDataRequests.end();) {
        if (!ProcessHasDataRequest(*request, ctx)) {
            break;
        }

        forgetSubscription(request->ClientId);
        request = HasDataRequests.erase(request);
    }

    for (auto it = HasDataDeadlines.begin(); it != HasDataDeadlines.end();) {
        if (it->Deadline <= now) {
            auto request = HasDataRequests.find(it->Request);
            if (request != HasDataRequests.end()) {
                auto response = MakeHasDataInfoResponse(0, request->Cookie);
                ctx.Send(request->Sender, response.Release());

                forgetSubscription(request->ClientId);
                HasDataRequests.erase(request);
            }
            it = HasDataDeadlines.erase(it);
        } else {
            break;
        }
    }
}

void TPartition::Handle(TEvPersQueue::TEvHasDataInfo::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    Y_ABORT_UNLESS(record.HasSender());

    auto now = ctx.Now();

    auto cookie = record.HasCookie() ? TMaybe<ui64>(record.GetCookie()) : TMaybe<ui64>();
    auto readTimestamp = GetReadFrom(record.GetMaxTimeLagMs(), record.GetReadTimestampMs(), TInstant::Zero(), ctx);
    TActorId sender = ActorIdFromProto(record.GetSender());

    THasDataReq req{++HasDataReqNum, (ui64)record.GetOffset(), sender, cookie,
        record.HasClientId() && InitDone ? record.GetClientId() : "", readTimestamp};

    if (!InitDone || !ProcessHasDataRequest(req, ctx)) {
        THasDataDeadline dl{TInstant::MilliSeconds(record.GetDeadline()), req};
        auto res = HasDataRequests.insert(std::move(req));
        HasDataDeadlines.insert(dl);
        Y_ABORT_UNLESS(res.second);

        if (InitDone && record.HasClientId() && !record.GetClientId().empty()) {
            auto& userInfo = UsersInfoStorage->GetOrCreate(record.GetClientId(), ctx);
            ++userInfo.Subscriptions;
            userInfo.UpdateReadOffset((i64)BlobEncoder.EndOffset - 1, now, now, now);
            userInfo.UpdateReadingTimeAndState(BlobEncoder.EndOffset, now);
        }
    }
}

void TPartition::Handle(NReadQuoterEvents::TEvAccountQuotaCountersUpdated::TPtr& ev, const TActorContext& /*ctx*/) {
    TabletCounters.Populate(*ev->Get()->AccountQuotaCounters.Get());
}

void TPartition::InitUserInfoForImportantClients(const TActorContext& ctx) {
    TSet<TString> important;
    for (const auto& consumer : Config.GetConsumers()) {
        if (!consumer.GetImportant()) {
            continue;
        }

        important.insert(consumer.GetName());

        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(consumer.GetName());
        if (userInfo && !userInfo->Important && userInfo->LabeledCounters) {
            ctx.Send(Tablet, new TEvPQ::TEvPartitionLabeledCountersDrop(Partition, userInfo->LabeledCounters->GetGroup()));
            userInfo->SetImportant(true);
            continue;
        }
        if (!userInfo) {
            userInfo = &UsersInfoStorage->Create(
                    ctx, consumer.GetName(), 0, true, "", 0, 0, 0, 0, 0, TInstant::Zero(), {}, false
            );
        }
        if (userInfo->Offset < (i64)CompactionBlobEncoder.StartOffset)
            userInfo->Offset = CompactionBlobEncoder.StartOffset;
        ReadTimestampForOffset(consumer.GetName(), *userInfo, ctx);
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
    result.SetPartition(Partition.InternalPartitionId);
    result.SetStartOffset(CompactionBlobEncoder.StartOffset);
    result.SetEndOffset(BlobEncoder.EndOffset);
    result.SetErrorCode(NPersQueue::NErrorCode::OK);
    result.SetWriteTimestampEstimateMS(WriteTimestampEstimate.MilliSeconds());

    if (!ev->Get()->ClientId.empty()) {
        TUserInfo* userInfo = UsersInfoStorage->GetIfExists(ev->Get()->ClientId);
        if (userInfo) {
            auto snapshot = CreateSnapshot(*userInfo);
            result.SetClientOffset(userInfo->Offset);
            result.SetWriteTimestampMS(snapshot.LastCommittedMessage.WriteTimestamp.MilliSeconds());
            result.SetCreateTimestampMS(snapshot.LastCommittedMessage.CreateTimestamp.MilliSeconds());
            result.SetClientReadOffset(userInfo->GetReadOffset());
            result.SetReadWriteTimestampMS(snapshot.LastReadMessage.WriteTimestamp.MilliSeconds());
            result.SetReadCreateTimestampMS(snapshot.LastReadMessage.CreateTimestamp.MilliSeconds());
        }
    }
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionOffsetsResponse(result, Partition));
}

void TPartition::HandleOnInit(TEvPQ::TEvPartitionOffsets::TPtr& ev, const TActorContext& ctx) {
    NKikimrPQ::TOffsetsResponse::TPartResult result;
    result.SetPartition(Partition.InternalPartitionId);
    result.SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
    result.SetErrorReason("partition is not ready yet");
    ctx.Send(ev->Get()->Sender, new TEvPQ::TEvPartitionOffsetsResponse(result, Partition));
}

std::pair<TInstant, TInstant> TPartition::GetTime(const TUserInfo& userInfo, ui64 offset) const {
    TInstant wtime = userInfo.WriteTimestamp > TInstant::Zero() ? userInfo.WriteTimestamp : GetWriteTimeEstimate(offset);
    return std::make_pair(wtime, userInfo.CreateTimestamp);
}

void TPartition::Handle(TEvPQ::TEvGetClientOffset::TPtr& ev, const TActorContext& ctx) {
    auto& userInfo = UsersInfoStorage->GetOrCreate(ev->Get()->ClientId, ctx);
    Y_ABORT_UNLESS(userInfo.Offset >= -1, "Unexpected Offset: %" PRIi64, userInfo.Offset);
    ui64 offset = Max<i64>(userInfo.Offset, 0);
    auto ts = GetTime(userInfo, offset);
    TabletCounters.Cumulative()[COUNTER_PQ_GET_CLIENT_OFFSET_OK].Increment(1);
    ReplyGetClientOffsetOk(ctx, ev->Get()->Cookie, userInfo.Offset, ts.first, ts.second, userInfo.AnyCommits, userInfo.CommittedMetadata);
}

void TPartition::Handle(TEvPQ::TEvSetClientInfo::TPtr& ev, const TActorContext& ctx) {
    if (size_t count = GetUserActCount(ev->Get()->ClientId); count > MAX_USER_ACTS) {
        TabletCounters.Cumulative()[COUNTER_PQ_SET_CLIENT_OFFSET_ERROR].Increment(1);
        ReplyError(ctx, ev->Get()->Cookie, NPersQueue::NErrorCode::OVERLOAD,
            TStringBuilder() << "too big inflight: " << count);
        return;
    }

    const ui64& offset = ev->Get()->Offset;
    Y_ABORT_UNLESS(offset <= (ui64)Max<i64>(), "Unexpected Offset: %" PRIu64, offset);

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

ui64 GetFirstHeaderOffset(const TKey& key, const TString& blob)
{
    TBlobIterator it(key, blob);
    Y_ABORT_UNLESS(it.IsValid());
    return it.GetBatch().GetOffset();
}

bool TReadInfo::UpdateUsage(const TClientBlob& blob,
                            ui32& cnt, ui32& size, ui32& lastBlobSize) const
{
    size += blob.GetBlobSize(AppData()->FeatureFlags.GetEnableTopicMessageKeySaving());
    lastBlobSize += blob.GetBlobSize(
        AppData()->FeatureFlags.GetEnableTopicMessageKeySaving());

    if (blob.IsLastPart()) {
        bool messageSkippingBehaviour =
            AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
            (ReadTimestampMs > blob.WriteTimestamp.MilliSeconds());

        ++cnt;

        if (messageSkippingBehaviour) {
            --cnt;
            size -= lastBlobSize;
        }
        lastBlobSize = 0;

        return cnt >= Count;
    }

    // For backward compatibility, we keep the behavior for older clients for non-FirstClassCitizen
    return !AppData()->PQConfig.GetTopicsAreFirstClassCitizen() && (cnt >= Count);
}

TMaybe<TReadAnswer> TReadInfo::AddBlobsFromBody(const TVector<NPQ::TRequestedBlob>& blobs,
                                                const ui32 begin, const ui32 end,
                                                TUserInfo* userInfo,
                                                const ui64 startOffset,
                                                const ui64 endOffset,
                                                const ui64 sizeLag,
                                                const TActorId& tablet,
                                                ui64 realReadOffset,
                                                NKikimrClient::TCmdReadResult* readResult,
                                                THolder<TEvPQ::TEvProxyResponse>& answer,
                                                bool& needStop,
                                                ui32& cnt, ui32& size, ui32& lastBlobSize,
                                                const TActorContext& ctx)
{
    Y_ABORT_UNLESS(begin <= end);
    Y_ABORT_UNLESS(end <= blobs.size());

    for (ui32 pos = begin; pos < end; ++pos) {
        Y_ABORT_UNLESS(Blobs[pos].Offset == blobs[pos].Offset, "Mismatch %" PRIu64 " vs %" PRIu64, Blobs[pos].Offset, blobs[pos].Offset);
        Y_ABORT_UNLESS(Blobs[pos].Count == blobs[pos].Count, "Mismatch %" PRIu32 " vs %" PRIu32, Blobs[pos].Count, blobs[pos].Count);

        ui64 offset = blobs[pos].Offset;
        ui32 count = blobs[pos].Count;
        ui16 partNo = blobs[pos].PartNo;
        ui16 internalPartsCount = blobs[pos].InternalPartsCount;
        const TString& blobValue = blobs[pos].Value;

        if (blobValue.empty()) { // this is ok. Means that someone requested too much data or retention race
            PQ_LOG_D( "Not full answer here!");
            ui64 answerSize = answer->Response->ByteSize();
            if (userInfo && Destination != 0) {
                userInfo->ReadDone(ctx, ctx.Now(), answerSize, cnt, ClientDC,
                        tablet, IsExternalRead, endOffset);
            }
            readResult->SetSizeLag(sizeLag - size);
            RealReadOffset = realReadOffset;
            LastOffset = Offset - 1;
            SizeEstimate = answerSize;
            readResult->SetSizeEstimate(SizeEstimate);
            readResult->SetLastOffset(LastOffset);
            readResult->SetStartOffset(startOffset);
            readResult->SetEndOffset(endOffset);
            return TReadAnswer{answerSize, std::move(answer)};
        }
        Y_ABORT_UNLESS(blobValue.size() <= blobs[pos].Size, "value for offset %" PRIu64 " count %u size must be %u, but got %u",
                                                        offset, count, blobs[pos].Size, (ui32)blobValue.size());

        if (offset > Offset || (offset == Offset && partNo > PartNo)) { // got gap
            Offset = offset;
            PartNo = partNo;
        }
        Y_ABORT_UNLESS(offset <= Offset);
        Y_ABORT_UNLESS(offset < Offset || partNo <= PartNo);
        auto key = TKey::ForBody(TKeyPrefix::TypeData, TPartitionId(0), offset, partNo, count, internalPartsCount);
        ui64 firstHeaderOffset = GetFirstHeaderOffset(key, blobValue);
        for (TBlobIterator it(key, blobValue); it.IsValid() && !needStop; it.Next()) {
            TBatch batch = it.GetBatch();
            auto& header = batch.Header;
            batch.Unpack();
            ui64 trueOffset = blobs[pos].Key.GetOffset() + (header.GetOffset() - firstHeaderOffset);

            ui32 pos = 0;
            if (trueOffset > Offset || trueOffset == Offset && header.GetPartNo() >= PartNo) {
                pos = 0;
            } else {
                ui64 trueSearchOffset = Offset - blobs[pos].Key.GetOffset() + firstHeaderOffset;
                pos = batch.FindPos(trueSearchOffset, PartNo);
            }
            offset += header.GetCount();

            if (pos == Max<ui32>()) // this batch does not contain data to read, skip it
                continue;


            PQ_LOG_D("FormAnswer processing batch offset " << (offset - header.GetCount()) <<  " totakecount " << count << " count " << header.GetCount()
                    << " size " << header.GetPayloadSize() << " from pos " << pos << " cbcount " << batch.Blobs.size());

            for (size_t i = pos; i < batch.Blobs.size(); ++i) {
                TClientBlob &res = batch.Blobs[i];
                VERIFY_RESULT_BLOB(res, i);

                Y_ABORT_UNLESS(PartNo == res.GetPartNo(), "pos %" PRIu32 " i %" PRIu64 " Offset %" PRIu64 " PartNo %" PRIu16 " offset %" PRIu64 " partNo %" PRIu16,
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

                if (UpdateUsage(res, cnt, size, lastBlobSize)) {
                    needStop = true;
                    break;
                }
            }
        }
    }

    return Nothing();
}

TReadAnswer TReadInfo::FormAnswer(
    const TActorContext& ctx,
    const TEvPQ::TEvBlobResponse& blobResponse,
    const ui64 startOffset,
    const ui64 endOffset,
    const TPartitionId& partition,
    TUserInfo* userInfo,
    const ui64 destination,
    const ui64 sizeLag,
    const TActorId& tablet,
    const NKikimrPQ::TPQTabletConfig::EMeteringMode meteringMode,
    const bool isActive,
    const std::function<void(bool readingFinished, NKikimrClient::TCmdReadResult& r)>& postProcessor
) {
    Y_UNUSED(meteringMode);
    Y_UNUSED(partition);
    auto answer = MakeHolder<TEvPQ::TEvProxyResponse>(destination);

    NKikimrClient::TResponse& res = *answer->Response;
    const TEvPQ::TEvBlobResponse* response = &blobResponse;
    if (HasError(blobResponse)) {
        Error = true;
        return TReadAnswer{
            blobResponse.Error.ErrorStr.size(),
            MakeHolder<TEvPQ::TEvError>(blobResponse.Error.ErrorCode, blobResponse.Error.ErrorStr, destination)
        };
    }

    res.SetStatus(NMsgBusProxy::MSTATUS_OK);
    res.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto readResult = res.MutablePartitionResponse()->MutableCmdReadResult();
    readResult->SetWaitQuotaTimeMs(WaitQuotaTime.MilliSeconds());
    readResult->SetStartOffset(startOffset);
    readResult->SetMaxOffset(endOffset);
    readResult->SetEndOffset(endOffset);
    readResult->SetRealReadOffset(Offset);
    ui64 realReadOffset = Offset;
    readResult->SetReadFromTimestampMs(ReadTimestampMs);

    Y_ABORT_UNLESS(endOffset <= (ui64)Max<i64>(), "Max offset is too big: %" PRIu64, endOffset);
    PQ_LOG_D("FormAnswer for " << Blobs.size() << " blobs");

    if (!isActive && response->GetBlobs().empty()) {
        postProcessor(true, *readResult);
    }

    AddResultDebugInfo(response, readResult);

    ui32 cnt = 0;
    ui32 size = 0;

    ui32 lastBlobSize = 0;
    const TVector<TRequestedBlob>& blobs = response->GetBlobs();
    const bool saveMsgKey = AppData()->FeatureFlags.GetEnableTopicMessageKeySaving();
    auto updateUsage = [&](const TClientBlob& blob) {
        size += blob.GetBlobSize(saveMsgKey);
        lastBlobSize += blob.GetBlobSize(saveMsgKey);
        if (blob.IsLastPart()) {
            bool messageSkippingBehaviour = AppData()->PQConfig.GetTopicsAreFirstClassCitizen() &&
                    ReadTimestampMs > blob.WriteTimestamp.MilliSeconds();
            ++cnt;
            if (messageSkippingBehaviour) {
                --cnt;
                size -= lastBlobSize;
            }
            lastBlobSize = 0;
            return cnt >= Count;
        }
        // For backward compatibility, we keep the behavior for older clients for non-FirstClassCitizen
        return !AppData()->PQConfig.GetTopicsAreFirstClassCitizen() && cnt >= Count;
    };

    Y_ABORT_UNLESS(blobs.size() == Blobs.size());
    response->Check();
    bool needStop = false;

    auto readAnswer = AddBlobsFromBody(blobs,
                                       0, CompactedBlobsCount,
                                       userInfo,
                                       startOffset,
                                       endOffset,
                                       sizeLag,
                                       tablet,
                                       realReadOffset,
                                       readResult,
                                       answer,
                                       needStop,
                                       cnt, size, lastBlobSize,
                                       ctx);
    if (readAnswer) {
        return std::move(*readAnswer);
    }

    if (!needStop && cnt < Count && size < Size) { // body blobs are fully processed and need to take more data
        if (CachedOffset > Offset) {
            lastBlobSize = 0;
            Offset = CachedOffset;
        }

        for (const auto& writeBlob : Cached) {
            VERIFY_RESULT_BLOB(writeBlob, 0u);

            readResult->SetBlobsCachedSize(readResult->GetBlobsCachedSize() + writeBlob.GetBlobSize(saveMsgKey));

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

    readAnswer = AddBlobsFromBody(blobs,
                                  CompactedBlobsCount, blobs.size(),
                                  userInfo,
                                  startOffset,
                                  endOffset,
                                  sizeLag,
                                  tablet,
                                  realReadOffset,
                                  readResult,
                                  answer,
                                  needStop,
                                  cnt, size, lastBlobSize,
                                  ctx);
    if (readAnswer) {
        return std::move(*readAnswer);
    }

    Y_ABORT_UNLESS(Offset <= (ui64)Max<i64>(), "Offset is too big: %" PRIu64, Offset);
    ui64 answerSize = answer->Response->ByteSize();
    if (userInfo && Destination != 0) {
        userInfo->ReadDone(ctx, ctx.Now(), answerSize, cnt, ClientDC,
                        tablet, IsExternalRead, endOffset);

    }
    readResult->SetSizeLag(sizeLag - size);
    RealReadOffset = realReadOffset;
    LastOffset = Offset - 1;
    SizeEstimate = answerSize;
    readResult->SetSizeEstimate(SizeEstimate);
    readResult->SetLastOffset(LastOffset);
    readResult->SetStartOffset(startOffset);
    readResult->SetEndOffset(endOffset);

    return {answerSize, std::move(answer)};
}

void TPartition::Handle(TEvPQ::TEvReadTimeout::TPtr& ev, const TActorContext& ctx) {
    auto res = Subscriber.OnTimeout(ev);
    if (!res)
        return;
    TReadAnswer answer = res->FormAnswer(
            ctx, nullptr, CompactionBlobEncoder.StartOffset, res->Offset, Partition, nullptr,
            res->Destination, 0, Tablet, Config.GetMeteringMode(), IsActive(), GetResultPostProcessor<NKikimrClient::TCmdReadResult>(res->User)
    );
    ctx.Send(Tablet, answer.Event.Release());
    PQ_LOG_D(" waiting read cookie " << ev->Get()->Cookie
        << " partition " << Partition << " read timeout for " << res->User << " offset " << res->Offset);
    auto& userInfo = UsersInfoStorage->GetOrCreate(res->User, ctx);

    userInfo.ForgetSubscription(BlobEncoder.EndOffset, ctx.Now());
    OnReadRequestFinished(res->Destination, answer.Size, res->User, ctx);
}

void CollectReadRequestFromBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount,
                                const ui32 maxSize, ui32* rcount, ui32* rsize, ui64 lastOffset,
                                TBlobKeyTokens* blobKeyTokens,
                                TPartitionBlobEncoder& zone,
                                TVector<TRequestedBlob>& result)
{
    Y_ABORT_UNLESS(rcount && rsize);
    auto blobs = zone.GetBlobsFromBody(startOffset,
                                       partNo,
                                       maxCount,
                                       maxSize,
                                       *rcount,
                                       *rsize,
                                       lastOffset,
                                       blobKeyTokens);
    std::move(blobs.begin(), blobs.end(), std::back_inserter(result));
}

void TPartition::GetReadRequestFromCompactedBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount,
                                                 const ui32 maxSize, ui32* rcount, ui32* rsize, ui64 lastOffset,
                                                 TBlobKeyTokens* blobKeyTokens,
                                                 TVector<TRequestedBlob>& blobs)
{
    CollectReadRequestFromBody(startOffset, partNo,
                               maxCount, maxSize,
                               rcount, rsize,
                               lastOffset,
                               blobKeyTokens,
                               CompactionBlobEncoder,
                               blobs);
}

void TPartition::GetReadRequestFromFastWriteBody(const ui64 startOffset, const ui16 partNo, const ui32 maxCount,
                                                 const ui32 maxSize, ui32* rcount, ui32* rsize, ui64 lastOffset,
                                                 TBlobKeyTokens* blobKeyTokens,
                                                 TVector<TRequestedBlob>& blobs)
{
    CollectReadRequestFromBody(startOffset, partNo,
                               maxCount, maxSize,
                               rcount, rsize,
                               lastOffset,
                               blobKeyTokens,
                               BlobEncoder,
                               blobs);
}

TVector<TClientBlob> TPartition::GetReadRequestFromHead(
        const ui64 startOffset, const ui16 partNo, const ui32 maxCount, const ui32 maxSize, const ui64 readTimestampMs, ui32* rcount,
        ui32* rsize, ui64* insideHeadOffset, ui64 lastOffset
) {
    Y_ABORT_UNLESS(rcount && rsize);
    return CompactionBlobEncoder.GetBlobsFromHead(startOffset,
                                                  partNo,
                                                  maxCount,
                                                  maxSize,
                                                  readTimestampMs,
                                                  *rcount,
                                                  *rsize,
                                                  *insideHeadOffset,
                                                  lastOffset);
}

void TPartition::Handle(TEvPQ::TEvRead::TPtr& ev, const TActorContext& ctx) {
    auto* read = ev->Get();

    if (read->Count == 0) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
        ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::BAD_REQUEST, "no infinite flows allowed - count is not set or 0");
        return;
    }
    if (read->Offset < CompactionBlobEncoder.StartOffset) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_SMALL_OFFSET].Increment(1);
        read->Offset = CompactionBlobEncoder.StartOffset;
        if (read->PartNo > 0) {
            TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
            PQ_LOG_ERROR(
                        "I was right, there could be rewinds and deletions at once! Topic " << TopicConverter->GetClientsideName() <<
                        " partition " << Partition <<
                        " readOffset " << read->Offset <<
                        " readPartNo " << read->PartNo <<
                        " startOffset " << CompactionBlobEncoder.StartOffset);
            ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::READ_ERROR_TOO_SMALL_OFFSET,
                       "client requested not from first part, and this part is lost");
            return;
        }
    }
    if (read->Offset > BlobEncoder.EndOffset || read->Offset == BlobEncoder.EndOffset && read->PartNo > 0) {
        TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_BIG_OFFSET].Increment(1);
        TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
        PQ_LOG_ERROR(
                    "reading from too big offset - topic " << TopicConverter->GetClientsideName() <<
                    " partition " << Partition <<
                    " client " << read->ClientId <<
                    " EndOffset " << BlobEncoder.EndOffset <<
                    " offset " << read->Offset);
        ReplyError(ctx, read->Cookie, NPersQueue::NErrorCode::READ_ERROR_TOO_BIG_OFFSET,
                                      TStringBuilder() << "trying to read from future. ReadOffset " <<
                                      read->Offset << ", " << read->PartNo << " EndOffset " << BlobEncoder.EndOffset);
        return;
    }

    Y_ABORT_UNLESS(read->Offset <= BlobEncoder.EndOffset);

    const TString& user = read->ClientId;
    auto& userInfo = UsersInfoStorage->GetOrCreate(user, ctx);
    if (!read->SessionId.empty() && !userInfo.NoConsumer) {
        if (userInfo.Session != read->SessionId) {
            TabletCounters.Cumulative()[COUNTER_PQ_READ_ERROR_NO_SESSION].Increment(1);
            TabletCounters.Percentile()[COUNTER_LATENCY_PQ_READ_ERROR].IncrementFor(0);
            ReplyError(ctx, read->Cookie, NPersQueue::NErrorCode::READ_ERROR_NO_SESSION,
                TStringBuilder() << "no such session '" << read->SessionId << "'");
            return;
        }
    }
    userInfo.ReadsInQuotaQueue++;
    Send(ReadQuotaTrackerActor,
            new TEvPQ::TEvRequestQuota(ev->Get()->Cookie, IEventHandle::Upcast(std::move(ev)))
    );
}

void TPartition::Handle(TEvPQ::TEvApproveReadQuota::TPtr& ev, const TActorContext& ctx) {
    DoRead(std::move(ev->Get()->ReadRequest), ev->Get()->WaitTime, ctx);
}

void TPartition::DoRead(TEvPQ::TEvRead::TPtr&& readEvent, TDuration waitQuotaTime, const TActorContext& ctx) {
    auto* read = readEvent->Get();
    const TString& user = read->ClientId;
    auto userInfo = UsersInfoStorage->GetIfExists(user);
    if (!userInfo) {
        ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::BAD_REQUEST, GetConsumerDeletedMessage(read->ClientId));
        Send(ReadQuotaTrackerActor, new TEvPQ::TEvConsumerRemoved(user));
        OnReadRequestFinished(read->Cookie, 0, user, ctx);
        return;
    }
    userInfo->ReadsInQuotaQueue--;
    ui64 offset = read->Offset;

    auto readTimestamp = GetReadFrom(read->MaxTimeLagMs, read->ReadTimestampMs, userInfo->ReadFromTimestamp, ctx);
    if (read->PartNo == 0 && readTimestamp) {
        offset = GetReadOffset(offset, readTimestamp);
        userInfo->ReadOffsetRewindSum += offset - read->Offset;
    }

    TReadInfo info(
            user, read->ClientDC, offset, read->LastOffset, read->PartNo, read->Count, read->Size, read->Cookie, read->ReadTimestampMs,
            waitQuotaTime, read->ExternalOperation, userInfo->PipeClient
    );

    ui64 cookie = NextReadCookie();

    PQ_LOG_D("read cookie " << cookie << " Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                << " user " << user
                << " offset " << read->Offset << " count " << read->Count << " size " << read->Size << " endOffset " << BlobEncoder.EndOffset
                << " max time lag " << read->MaxTimeLagMs << "ms effective offset " << offset);

    if (offset == BlobEncoder.EndOffset) {
        const ui32 maxTimeout = IsActive() ? 30000 : 1000;
        if (read->Timeout > maxTimeout) {
            if (IsActive()) {
                PQ_LOG_D("too big read timeout " << " Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
                            << " user " << read->ClientId << " offset " << read->Offset << " count " << read->Count
                            << " size " << read->Size << " endOffset " << BlobEncoder.EndOffset << " max time lag " << read->MaxTimeLagMs
                            << "ms effective offset " << offset
                );
            }
            read->Timeout = maxTimeout;
        }
        Subscriber.AddSubscription(std::move(info), read->Timeout, cookie, ctx);
        ++userInfo->Subscriptions;
        userInfo->UpdateReadOffset((i64)offset - 1, userInfo->WriteTimestamp, userInfo->CreateTimestamp, ctx.Now());

        return;
    }

    if (offset >= BlobEncoder.EndOffset) {
        ReplyError(ctx, read->Cookie,  NPersQueue::NErrorCode::BAD_REQUEST,
            TStringBuilder() << "Offset more than EndOffset. Offset=" << offset << ", EndOffset=" << BlobEncoder.EndOffset);
        return;
    }

    ProcessRead(ctx, std::move(info), cookie, false);
}

void TPartition::OnReadRequestFinished(ui64 cookie, ui64 answerSize, const TString& consumer, const TActorContext& ctx) {
    AvgReadBytes.Update(answerSize, ctx.Now());
    Send(ReadQuotaTrackerActor, new TEvPQ::TEvConsumed(answerSize, cookie, consumer));
}

void TPartition::ReadTimestampForOffset(const TString& user, TUserInfo& userInfo, const TActorContext& ctx) {
    if (userInfo.ReadScheduled)
        return;
    userInfo.ReadScheduled = true;
    PQ_LOG_D("Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition <<
            " user " << user << " readTimeStamp for offset " << userInfo.Offset << " initiated " <<
            " queuesize " << UpdateUserInfoTimestamp.size() << " startOffset " << CompactionBlobEncoder.StartOffset <<
            " ReadingTimestamp " << ReadingTimestamp << " rrg " << userInfo.ReadRuleGeneration
    );

    if (ReadingTimestamp) {
        UpdateUserInfoTimestamp.push_back(std::make_pair(user, userInfo.ReadRuleGeneration));
        return;
    }
    if (userInfo.Offset < (i64)CompactionBlobEncoder.StartOffset) {
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

    if (userInfo.Offset >= (i64)BlobEncoder.EndOffset || CompactionBlobEncoder.StartOffset == BlobEncoder.EndOffset) {
        userInfo.ReadScheduled = false;
        return;
    }

    Y_ABORT_UNLESS(!ReadingTimestamp);

    ReadingTimestamp = true;
    ReadingForUser = user;
    ReadingForOffset = userInfo.Offset;
    ReadingForUserReadRuleGeneration = userInfo.ReadRuleGeneration;

    for (const auto& user : UpdateUserInfoTimestamp) {
        Y_ABORT_UNLESS(user.first != ReadingForUser || user.second != ReadingForUserReadRuleGeneration);
    }

    PQ_LOG_D("Topic '" << TopicConverter->GetClientsideName() << "' partition " << Partition
            << " user " << user << " send read request for offset " << userInfo.Offset << " initiated "
            << " queuesize " << UpdateUserInfoTimestamp.size() << " startOffset " << CompactionBlobEncoder.StartOffset
            << " ReadingTimestamp " << ReadingTimestamp << " rrg " << ReadingForUserReadRuleGeneration
    );

    THolder<TEvPQ::TEvRead> event = MakeHolder<TEvPQ::TEvRead>(0, userInfo.Offset, 0, 0, 1, "",
                                                               user, 0, MAX_BLOB_PART_SIZE * 2, 0, 0, "",
                                                               false, TActorId{});

    ctx.Send(ctx.SelfID, event.Release());
    TabletCounters.Cumulative()[COUNTER_PQ_WRITE_TIMESTAMP_CACHE_MISS].Increment(1);
}

void TPartition::ProcessTimestampsForNewData(const ui64 prevEndOffset, const TActorContext& ctx) {
    for (auto& [consumer, userInfo] : UsersInfoStorage->GetAll()) {
        if (userInfo.Offset >= (i64)prevEndOffset && userInfo.Offset < (i64)BlobEncoder.EndOffset) {
            ReadTimestampForOffset(consumer, userInfo, ctx);
        }
    }
}

void TPartition::Handle(TEvPQ::TEvProxyResponse::TPtr& ev, const TActorContext& ctx) {
    ReadingTimestamp = false;

    auto userInfo = UsersInfoStorage->GetIfExists(ReadingForUser);

    if (!userInfo || userInfo->ReadRuleGeneration != ReadingForUserReadRuleGeneration) {
        PQ_LOG_I("Topic '" << TopicConverter->GetClientsideName() << "'" <<
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
            " startOffset " << CompactionBlobEncoder.StartOffset
    );
    Y_ABORT_UNLESS(userInfo->ReadScheduled);
    userInfo->ReadScheduled = false;
    Y_ABORT_UNLESS(ReadingForUser != "");

    if (!userInfo->ActualTimestamps) {
        LOG_INFO_S(
            ctx,
            NKikimrServices::PERSQUEUE,
            "Reading Timestamp failed for offset " << ReadingForOffset << " ( "<< userInfo->Offset << " ) "
                                                   << ev->Get()->Response->DebugString()
        );
        if (ev->Get()->Response->GetStatus() == NMsgBusProxy::MSTATUS_OK &&
            ev->Get()->Response->GetErrorCode() == NPersQueue::NErrorCode::OK &&
            ev->Get()->Response->GetPartitionResponse().HasCmdReadResult() &&
            ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().ResultSize() > 0 &&
            (i64)ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().GetResult(0).GetOffset() >= userInfo->Offset) {
                //offsets is inside gap - return timestamp of first record after gap
            const auto& res = ev->Get()->Response->GetPartitionResponse().GetCmdReadResult().GetResult(0);
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
        if (userInfo->Offset == (i64)BlobEncoder.EndOffset)
            continue;
        ReadTimestampForOffset(user, *userInfo, ctx);
    }
    Y_ABORT_UNLESS(ReadingTimestamp || UpdateUserInfoTimestamp.empty());
}

void TPartition::ProcessRead(const TActorContext& ctx, TReadInfo&& info, const ui64 cookie, bool subscription) {
    ui32 count = 0;
    ui32 size = 0;
    Y_ABORT_UNLESS(!info.User.empty());
    auto& userInfo = UsersInfoStorage->GetOrCreate(info.User, ctx);

    if (subscription) {
        userInfo.ForgetSubscription(BlobEncoder.EndOffset, ctx.Now());
    }

    TVector<TRequestedBlob> blobs;

    GetReadRequestFromCompactedBody(info.Offset, info.PartNo, info.Count, info.Size, &count, &size, info.LastOffset,
                                    &info.BlobKeyTokens, blobs);
    info.CompactedBlobsCount = blobs.size();
    GetReadRequestFromFastWriteBody(info.Offset, info.PartNo, info.Count, info.Size, &count, &size, info.LastOffset,
                                    &info.BlobKeyTokens, blobs);

    info.Blobs = blobs;
    ui64 lastOffset = blobs.empty() ? info.Offset : blobs.back().Key.GetOffset();

    PQ_LOG_D("read cookie " << cookie << " added " << info.Blobs.size()
                << " blobs, size " << size << " count " << count << " last offset " << lastOffset << ", current partition end offset: " << BlobEncoder.EndOffset);

    if (blobs.empty() ||
        ((info.CompactedBlobsCount > 0) && (blobs[info.CompactedBlobsCount - 1].Key == CompactionBlobEncoder.DataKeysBody.back().Key))) { // read from head only when all blobs from body processed
        ui64 insideHeadOffset = 0;
        info.Cached = GetReadRequestFromHead(
                info.Offset, info.PartNo, info.Count, info.Size, info.ReadTimestampMs, &count,
                &size, &insideHeadOffset, info.LastOffset
        );
        info.CachedOffset = insideHeadOffset;
    }

    Y_ABORT_UNLESS(info.BlobKeyTokens.Size() == info.Blobs.size());
    if (info.Destination != 0) {
        ++userInfo.ActiveReads;
        userInfo.UpdateReadingTimeAndState(BlobEncoder.EndOffset, ctx.Now());
    }

    if (info.Blobs.empty()) { //all from head, answer right now
        PQ_LOG_D("Reading cookie " << cookie << ". All data is from uncompacted head.");

        OnReadComplete(info, &UsersInfoStorage->GetOrCreate(info.User, ctx), nullptr, ctx);
        return;
    }

    bool res = ReadInfo.emplace(cookie, std::move(info)).second;
    PQ_LOG_D("Reading cookie " << cookie << ". Send blob request.");
    Y_ABORT_UNLESS(res);

    auto request = MakeHolder<TEvPQ::TEvBlobRequest>(cookie, Partition,
                                                     std::move(blobs));

    ctx.Send(BlobCache, request.Release());
}

TString TPartition::GetConsumerDeletedMessage(TStringBuf consumerName) {
    return TStringBuilder() << "cannot finish read request. Consumer " << consumerName << " is gone from partition";
}

} // namespace NKikimr::NPQ
