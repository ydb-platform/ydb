#include "keyvalue_flat_impl.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/util/log_priority_mute_checker.h>

namespace NKikimr {
namespace NKeyValue {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Storage request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TKeyValueStorageRequest : public TActorBootstrapped<TKeyValueStorageRequest> {
    using TBase = TActorBootstrapped<TKeyValueStorageRequest>;

    ui64 ReadRequestsSent;
    ui64 ReadRequestsReplied;
    ui64 WriteRequestsSent;
    ui64 WriteRequestsReplied;
    ui64 GetStatusRequestsSent;
    ui64 GetStatusRequestsReplied;
    ui64 RangeRequestsSent;
    ui64 RangeRequestsReplied;
    ui64 InFlightLimit;
    ui64 InFlightLimitSeq;
    ui64 InFlightQueries;
    ui64 InFlightRequestsLimit;
    ui64 NextInFlightBatchCookie;
    ui32 TabletGeneration;

    THolder<TIntermediate> IntermediateResults;

    TIntrusivePtr<TTabletStorageInfo> TabletInfo;

    THPTimer PutTimer;
    TInstant GetStatusSentAt;
    TInstant LastSuccess;
    const TDuration MuteDuration = TDuration::Seconds(5);
    TLogPriorityMuteChecker<NLog::PRI_DEBUG, NLog::PRI_ERROR> ErrorStateMuteChecker;

    THashMap<ui32, TInstant> TimeForNextSend;

    struct TReadQueueItem {
        TIntermediate::TRead *Read;
        TIntermediate::TRead::TReadItem *ReadItem;

        friend bool operator <(const TReadQueueItem& left, const TReadQueueItem& right) {
            return left.ReadItem->LogoBlobId < right.ReadItem->LogoBlobId;
        }
    };

    struct TInFlightBatch {
        TVector<TReadQueueItem> ReadQueue;
        TInstant SentAt;
    };

    TMap<ui64, TInFlightBatch> InFlightBatchByCookie;
    TDeque<TReadQueueItem> ReadItems;

    TStackVec<ui32, 16> YellowMoveChannels;
    TStackVec<ui32, 16> YellowStopChannels;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    TKeyValueStorageRequest(THolder<TIntermediate>&& intermediate, const TTabletStorageInfo *tabletInfo, ui32 tabletGeneration)
        : ReadRequestsSent(0)
        , ReadRequestsReplied(0)
        , WriteRequestsSent(0)
        , WriteRequestsReplied(0)
        , GetStatusRequestsSent(0)
        , GetStatusRequestsReplied(0)
        , RangeRequestsSent(0)
        , RangeRequestsReplied(0)
        , InFlightLimit(50)
        , InFlightLimitSeq(intermediate->SequentialReadLimit)
        , InFlightQueries(0)
        , InFlightRequestsLimit(10)
        , NextInFlightBatchCookie(1)
        , TabletGeneration(tabletGeneration)
        , IntermediateResults(std::move(intermediate))
        , TabletInfo(const_cast<TTabletStorageInfo*>(tabletInfo))
    {
        IntermediateResults->Stat.KeyvalueStorageRequestSentAt = TAppData::TimeProvider->Now();
    }

    void CheckYellow(const TStorageStatusFlags &statusFlags, ui32 currentGroup) {
        if (statusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            for (ui32 channel : xrange(TabletInfo->Channels.size())) {
                const ui32 group = TabletInfo->ChannelInfo(channel)->LatestEntry()->GroupID;
                if (currentGroup == group) {
                    YellowMoveChannels.push_back(channel);
                }
            }
            SortUnique(YellowMoveChannels);
        }
        if (statusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            for (ui32 channel : xrange(TabletInfo->Channels.size())) {
                const ui32 group = TabletInfo->ChannelInfo(channel)->LatestEntry()->GroupID;
                if (currentGroup == group) {
                    YellowStopChannels.push_back(channel);
                }
            }
            SortUnique(YellowStopChannels);
        }
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr &ev, const TActorContext &ctx) {
        const TDuration duration = TDuration::Seconds(PutTimer.Passed());
        IntermediateResults->Stat.PutLatencies.push_back(duration.MilliSeconds());

        auto groupId = ev->Get()->GroupId;
        CheckYellow(ev->Get()->StatusFlags, groupId);

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            TInstant now = TAppData::TimeProvider->Now();

            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " Unexpected EvPut result# " << NKikimrProto::EReplyStatus_Name(status).data();
            str << " Deadline# " << IntermediateResults->Deadline.MilliSeconds();
            str << " Now# " << now.MilliSeconds();
            str << " duration# " << duration;
            str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
            str << " LastSuccess# " << LastSuccess;
            str << " SinceLastSuccess# " << (now - LastSuccess).MilliSeconds();
            str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
            str << " ErrorReason# " << ev->Get()->ErrorReason;
            str << " Marker# KV24";

            NLog::EPriority logPriority = ErrorStateMuteChecker.Register(now, MuteDuration);

            ReplyErrorAndDie(ctx, str.Str(),
                status == NKikimrProto::TIMEOUT ? NMsgBusProxy::MSTATUS_TIMEOUT : NMsgBusProxy::MSTATUS_INTERNALERROR,
                logPriority);
            return;
        }
        ui64 cookie = ev->Cookie;
        ui64 writeIdx = cookie;
        if (writeIdx >= IntermediateResults->Writes.size() && writeIdx >= IntermediateResults->Commands.size()) {
            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " EvPut cookie# " << (ui64)cookie;
            str << " > writes#" << (ui32)IntermediateResults->Writes.size();
            str << " Marker# KV25";
            ReplyErrorAndDie(ctx, str.Str());
            return;
        }

        TIntermediate::TWrite *wr = nullptr;
        if (IntermediateResults->Writes.size()) {
            wr = &IntermediateResults->Writes[writeIdx];
        } else {
            wr = &std::get<TIntermediate::TWrite>(IntermediateResults->Commands[writeIdx]);
        }
        wr->StatusFlags.Merge(ev->Get()->StatusFlags.Raw);
        wr->Latency = duration;
        ++WriteRequestsReplied;
        IntermediateResults->Stat.GroupWrittenBytes[std::make_pair(ev->Get()->Id.Channel(), groupId)] += ev->Get()->Id.BlobSize();
        IntermediateResults->Stat.GroupWrittenIops[std::make_pair(ev->Get()->Id.Channel(), groupId)] += 1; // FIXME: count distinct blobs?
        UpdateRequest(ctx);
    }

    void Handle(TEvBlobStorage::TEvStatusResult::TPtr &ev, const TActorContext &ctx) {
        TInstant now = TAppData::TimeProvider->Now();
        ui64 durationMs = (now - GetStatusSentAt).MilliSeconds();
        IntermediateResults->Stat.GetStatusLatencies.push_back(durationMs);

        ui64 cookie = ev->Cookie; // groupId
        CheckYellow(ev->Get()->StatusFlags, cookie);

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " Unexpected EvStatusResult# " << NKikimrProto::EReplyStatus_Name(status).data();
            str << " Deadline# " << IntermediateResults->Deadline.MilliSeconds();
            str << " Now# " << now.MilliSeconds();
            str << " GetStatusSentAt# " << GetStatusSentAt.MilliSeconds();
            str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
            str << " LastSuccess# " << LastSuccess;
            str << " SinceLastSuccess# " << (now - LastSuccess).MilliSeconds();
            str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
            str << " ErrorReason# " << ev->Get()->ErrorReason;
            str << " Marker# KV28";
            ReplyErrorAndDie(ctx, str.Str(),
                status == NKikimrProto::TIMEOUT ? NMsgBusProxy::MSTATUS_TIMEOUT : NMsgBusProxy::MSTATUS_INTERNALERROR);
            return;
        }
        ui64 idx = cookie;
        if (idx >= IntermediateResults->GetStatuses.size()) {
            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " EvStatus cookie# " << (ui64)cookie;
            str << " > GetStatuses# " << (ui32)IntermediateResults->GetStatuses.size();
            str << " Marker# KV29";
            ReplyErrorAndDie(ctx, str.Str());
            return;
        }
        IntermediateResults->GetStatuses[idx].StatusFlags.Merge(ev->Get()->StatusFlags.Raw);
        ++GetStatusRequestsReplied;
        UpdateRequest(ctx);
    }

    void Handle(TEvBlobStorage::TEvRangeResult::TPtr &ev, const TActorContext &ctx) {
        TEvBlobStorage::TEvRangeResult *msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            auto &v = IntermediateResults->TrimLeakedBlobs->FoundBlobs;
            for (const auto &resp : msg->Responses) {
                v.push_back(resp.Id);
            }
        }
        ++RangeRequestsReplied;
        UpdateRequest(ctx);
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev, const TActorContext &ctx) {
        Y_VERIFY(!InFlightBatchByCookie.empty());

        // Find the corresponding request (as replies come in random order)
        auto foundIt = InFlightBatchByCookie.find(ev->Cookie);
        Y_VERIFY(foundIt != InFlightBatchByCookie.end(), "Cookie# %" PRIu64 " not found!", ev->Cookie);
        TInFlightBatch &request = foundIt->second;

        InFlightQueries -= request.ReadQueue.size();

        ui64 durationMs = (TAppData::TimeProvider->Now() - request.SentAt).MilliSeconds();
        IntermediateResults->Stat.GetLatencies.push_back(durationMs);

        auto resetReadItems = [&](NKikimrProto::EReplyStatus status) {
            Y_VERIFY(status != NKikimrProto::UNKNOWN);
            for (const auto& item : request.ReadQueue) {
                auto& readItem = *item.ReadItem;
                readItem.Status = status;
                readItem.InFlight = false;
            }
        };

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            TInstant now = TAppData::TimeProvider->Now();

            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " Unexpected EvGet result# " << NKikimrProto::EReplyStatus_Name(status).data();
            str << " Deadline# " << IntermediateResults->Deadline.MilliSeconds();
            str << " Now# " << now.MilliSeconds();
            str << " SentAt# " << request.SentAt.MilliSeconds();
            str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
            str << " LastSuccess# " << LastSuccess;
            str << " SinceLastSuccess# " << (now - LastSuccess).MilliSeconds();
            str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
            str << " ErrorReason# " << ev->Get()->ErrorReason;
            str << " Marker# KV26";
            resetReadItems(status);
            ReplyErrorAndDie(ctx, str.Str(),
                status == NKikimrProto::TIMEOUT ? NMsgBusProxy::MSTATUS_TIMEOUT : NMsgBusProxy::MSTATUS_INTERNALERROR);
            return;
        }
        if (ev->Get()->ResponseSz != request.ReadQueue.size()) {
            LOG_ERROR_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletInfo->TabletID
                << " Got# " << ev->Get()->Print(false));
            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " Unexpected EvGet ResponseSz# " << (ui32)ev->Get()->ResponseSz;
            str << " InFlightQueries# " << (ui32)InFlightQueries;
            str << " ReadQueue.size# " << request.ReadQueue.size();
            str << " Marker# KV27";
            resetReadItems(NKikimrProto::ERROR);
            ReplyErrorAndDie(ctx, str.Str());
            return;
        }

        Y_VERIFY(ev->Get()->ResponseSz == request.ReadQueue.size());
        auto groupId = ev->Get()->GroupId;
        decltype(request.ReadQueue)::iterator it = request.ReadQueue.begin();
        for (ui32 i = 0, num = ev->Get()->ResponseSz; i < num; ++i, ++it) {
            auto& response = ev->Get()->Responses[i];
            auto& read = *it->Read;
            auto& readItem = *it->ReadItem;

            if (response.Status == NKikimrProto::OK) {
                if (read.Value.size() != read.ValueSize) {
                    read.Value.resize(read.ValueSize);
                }
                Y_VERIFY(response.Buffer.size() == readItem.BlobSize);
                Y_VERIFY(readItem.ValueOffset + readItem.BlobSize <= read.ValueSize);
                Y_VERIFY(read.Value.IsDetached());
                response.Buffer.begin().ExtractPlainDataAndAdvance(read.Value.Detach() + readItem.ValueOffset, response.Buffer.size());
                IntermediateResults->Stat.GroupReadBytes[std::make_pair(response.Id.Channel(), groupId)] += response.Buffer.size();
                IntermediateResults->Stat.GroupReadIops[std::make_pair(response.Id.Channel(), groupId)] += 1; // FIXME: count distinct blobs?
            } else {
                TStringStream err;
                if (read.Message.size()) {
                    err << read.Message << Endl;
                }
                err << "BS EvGet query failed"
                    << " LogoBlobId# " << readItem.LogoBlobId.ToString()
                    << " Status# " << NKikimrProto::EReplyStatus_Name(response.Status)
                    << " Response# " << ev->Get()->ToString()
                    << " Marker# KV22";
                read.Message = err.Str();
            }

            Y_VERIFY(response.Status != NKikimrProto::UNKNOWN);
            readItem.Status = response.Status;
            readItem.InFlight = false;
        }

        InFlightBatchByCookie.erase(foundIt);
        // Don't use the request as it is no more!

        ++ReadRequestsReplied;

        UpdateRequest(ctx);
    }

    bool UpdateRequest(const TActorContext &ctx) {
        TInstant now = TAppData::TimeProvider->Now();
        if (IntermediateResults->Deadline <= now) {
            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " Deadline reached Deadline# " << IntermediateResults->Deadline.MilliSeconds();
            str << " Now# " << now.MilliSeconds();
            str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
            str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
            str << " Marker# KV30";
            ReplyErrorAndDie(ctx, str.Str(), NMsgBusProxy::MSTATUS_TIMEOUT);
            return true;
        }
        LastSuccess = now;
        bool sentSomething = false;
        while (SendSomeReadRequests(ctx)) {
            sentSomething = true;
        }
        if (sentSomething) {
            return false;
        }

        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletInfo->TabletID
                << " UpdateRequest ReadRequestsReplied# " << ReadRequestsReplied
                << " ReadRequestsSent# " << ReadRequestsSent
                << " WriteRequestsReplied # " << WriteRequestsReplied
                << " WriteRequestsSent# " << WriteRequestsSent
                << " GetStatusRequestsReplied # " << GetStatusRequestsReplied
                << " GetStatusRequestsSent# " << GetStatusRequestsSent
                << " Marker# KV45");
        if (ReadRequestsReplied == ReadRequestsSent &&
                WriteRequestsReplied == WriteRequestsSent &&
                GetStatusRequestsReplied == GetStatusRequestsSent &&
                RangeRequestsSent == RangeRequestsReplied) {
            for (auto& write : IntermediateResults->Writes) {
                if (write.Status == NKikimrProto::UNKNOWN) {
                    write.Status = NKikimrProto::OK;
                }
            }
            for (auto& getStatus : IntermediateResults->GetStatuses) {
                if (getStatus.Status == NKikimrProto::UNKNOWN) {
                    getStatus.Status = NKikimrProto::OK;
                }
            }
            for (auto& cmd : IntermediateResults->Commands) {
                if (!std::holds_alternative<TIntermediate::TWrite>(cmd)) {
                    continue;
                }
                auto& write = std::get<TIntermediate::TWrite>(cmd);
                if (write.Status == NKikimrProto::UNKNOWN) {
                    write.Status = NKikimrProto::OK;
                }
            }
            IntermediateResults->Stat.YellowStopChannels.reserve(YellowStopChannels.size());
            IntermediateResults->Stat.YellowStopChannels.insert(IntermediateResults->Stat.YellowStopChannels.end(),
                    YellowStopChannels.begin(), YellowStopChannels.end());
            IntermediateResults->Stat.YellowMoveChannels.reserve(YellowMoveChannels.size());
            IntermediateResults->Stat.YellowMoveChannels.insert(IntermediateResults->Stat.YellowMoveChannels.end(),
                    YellowMoveChannels.begin(), YellowMoveChannels.end());
            TActorId keyValueActorId = IntermediateResults->KeyValueActorId;
            ctx.Send(keyValueActorId, new TEvKeyValue::TEvIntermediate(std::move(IntermediateResults)));
            Die(ctx);
            return true;
        }
        return false;
    }

    template<typename Func>
    void TraverseReadItems(Func&& func) {
        auto traverseRead = [&](TIntermediate::TRead& read) {
            for (auto& readItem : read.ReadItems) {
                func(read, readItem);
            }
        };
        for (auto& rangeRead : IntermediateResults->RangeReads) {
            for (auto& read : rangeRead.Reads) {
                traverseRead(read);
            }
        }
        for (auto& read : IntermediateResults->Reads) {
            traverseRead(read);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        TInstant now = TAppData::TimeProvider->Now();

        TStringStream str;
        str << "KeyValue# " << TabletInfo->TabletID;
        str << " Deadline reached before answering the request.";
        str << " Deadline# " << IntermediateResults->Deadline.MilliSeconds();
        str << " Now# " << now.MilliSeconds();
        str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
        str << " LastSuccess# " << LastSuccess;
        str << " SinceLastSuccess# " << (now - LastSuccess).MilliSeconds();
        str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
        str << " Marker# KV312";
        ReplyErrorAndDie(ctx, str.Str(), NMsgBusProxy::MSTATUS_TIMEOUT);
        return;
    }

    void Bootstrap(const TActorContext &ctx) {
        // Check parameters and send requests
        if (IntermediateResults->Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (IntermediateResults->Deadline <= now) {
                TStringStream str;
                str << "KeyValue# " << TabletInfo->TabletID;
                str << " Deadline reached before processing request.";
                str << " Deadline# " << IntermediateResults->Deadline.MilliSeconds();
                str << " Now# " << now.MilliSeconds();
                str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
                str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
                str << " Marker# KV311";
                ReplyErrorAndDie(ctx, str.Str(), NMsgBusProxy::MSTATUS_TIMEOUT);
                return;
            }

            const TDuration timeout = IntermediateResults->Deadline - now;
            ctx.Schedule(timeout, new TEvents::TEvWakeup());
        }

        TraverseReadItems([&](TIntermediate::TRead& read, TIntermediate::TRead::TReadItem& readItem) {
                if (readItem.Status != NKikimrProto::SCHEDULED) {
                    ReadItems.push_back(TReadQueueItem{&read, &readItem});
                }
            });
        Sort(ReadItems.begin(), ReadItems.end());

        SendWriteRequests(ctx);
        SendGetStatusRequests(ctx);
        SendRangeRequests(ctx);

        if (UpdateRequest(ctx)) {
            return;
        }
        Become(&TThis::StateWait);
    }

    void ReplyErrorAndDie(const TActorContext &ctx, TString errorDescription,
            NMsgBusProxy::EResponseStatus status = NMsgBusProxy::MSTATUS_INTERNALERROR,
            NLog::EPriority logPriority = NLog::PRI_ERROR) {
        LOG_LOG_S(ctx, logPriority, NKikimrServices::KEYVALUE, errorDescription);

        THolder<TEvKeyValue::TEvResponse> response(new TEvKeyValue::TEvResponse);
        if (IntermediateResults->HasCookie) {
            response->Record.SetCookie(IntermediateResults->Cookie);
        }
        response->Record.SetErrorReason(errorDescription);
        response->Record.SetStatus(status);
        ctx.Send(IntermediateResults->RespondTo, response.Release());
        IntermediateResults->IsReplied = true;

        IntermediateResults->Stat.YellowStopChannels.reserve(YellowStopChannels.size());
        IntermediateResults->Stat.YellowStopChannels.insert(IntermediateResults->Stat.YellowStopChannels.end(),
                YellowStopChannels.begin(), YellowStopChannels.end());
        IntermediateResults->Stat.YellowMoveChannels.reserve(YellowMoveChannels.size());
        IntermediateResults->Stat.YellowMoveChannels.insert(IntermediateResults->Stat.YellowMoveChannels.end(),
                YellowMoveChannels.begin(), YellowMoveChannels.end());

        IntermediateResults->UpdateStat();
        TActorId keyValueActorId = IntermediateResults->KeyValueActorId;
        ctx.Send(keyValueActorId, new TEvKeyValue::TEvNotify(
            IntermediateResults->RequestUid,
            IntermediateResults->CreatedAtGeneration, IntermediateResults->CreatedAtStep,
            IntermediateResults->Stat, status));
        Die(ctx);
    }

    bool SendSomeReadRequests(const TActorContext &ctx) {
        if (InFlightBatchByCookie.size() >= InFlightRequestsLimit) {
            return false;
        }

        TInFlightBatch request;
        ui32 expectedResponseSize = 0;

        TLogoBlobID prevId;
        ui32 prevGroup = Max<ui32>();
        decltype(ReadItems)::iterator it;
        TVector<TReadQueueItem> skippedItems;
        NKikimrBlobStorage::EGetHandleClass handleClass = NKikimrBlobStorage::FastRead;
        bool isHandleClassSet = false;
        for (it = ReadItems.begin(); it != ReadItems.end(); ++it) {
            auto& readItem = *it->ReadItem;
            Y_VERIFY(!readItem.InFlight && readItem.Status == NKikimrProto::UNKNOWN);

            const TLogoBlobID& id = readItem.LogoBlobId;
            bool isSameChannel = (request.ReadQueue.empty() || id.Channel() == prevId.Channel());
            if (!isSameChannel) {
                skippedItems.push_back(std::move(*it));
                continue;
            }
            const ui32 group = TabletInfo->GroupFor(id.Channel(), id.Generation());
            Y_VERIFY(group != Max<ui32>(), "ReadItem Blob# %s is mapped to an invalid group (-1)!",
                    id.ToString().c_str());
            bool isSameGroup = (prevGroup == group || prevGroup == Max<ui32>());
            if (!isSameGroup) {
                skippedItems.push_back(std::move(*it));
                continue;
            }
            if (isHandleClassSet) {
                bool isSameClass = (handleClass == it->Read->HandleClass);
                if (!isSameClass) {
                    skippedItems.push_back(std::move(*it));
                    continue;
                }
            } else {
                handleClass = it->Read->HandleClass;
                isHandleClassSet = true;
            }

            bool isSeq = id.TabletID() == prevId.TabletID()
                && id.Generation() == prevId.Generation()
                && id.Step() == prevId.Step()
                && id.Cookie() == prevId.Cookie() + 1;
            prevId = id;
            prevGroup = group;

            if (InFlightQueries >= (isSeq ? InFlightLimitSeq : InFlightLimit)) {
                break;
            }

            // approximate expected response size
            expectedResponseSize += BlobProtobufHeaderMaxSize + readItem.BlobSize;
            if (expectedResponseSize > MaxProtobufSize) {
                break;
            }

            readItem.InFlight = true;
            request.ReadQueue.push_back(std::move(*it));
            ++InFlightQueries;
        }

        std::move(skippedItems.begin(), skippedItems.end(), ReadItems.erase(ReadItems.begin(), it - skippedItems.size()));

        if (request.ReadQueue.empty()) {
            return false;
        }

        const ui32 readQueryCount = request.ReadQueue.size();
        TArrayHolder <TEvBlobStorage::TEvGet::TQuery> readQueries(new TEvBlobStorage::TEvGet::TQuery[readQueryCount]);
        ui32 queryIdx = 0;

        prevGroup = Max<ui32>();
        for (const auto& rq : request.ReadQueue) {
            const auto& readItem = *rq.ReadItem;
            readQueries[queryIdx].Set(readItem.LogoBlobId, readItem.BlobOffset, readItem.BlobSize);
            ++queryIdx;

            const ui32 group = TabletInfo->GroupFor(readItem.LogoBlobId.Channel(), readItem.LogoBlobId.Generation());
            Y_VERIFY(group != Max<ui32>(), "Get Blob# %s is mapped to an invalid group (-1)!",
                    readItem.LogoBlobId.ToString().c_str());
            if (prevGroup != Max<ui32>()) {
                Y_VERIFY(prevGroup == group);
            } else {
                prevGroup = group;
            }
        }

        ++ReadRequestsSent;
        request.SentAt = TAppData::TimeProvider->Now();

        ui64 cookie = NextInFlightBatchCookie;
        ++NextInFlightBatchCookie;
        InFlightBatchByCookie[cookie] = std::move(request);

        Y_VERIFY(queryIdx == readQueryCount);

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(readQueries, readQueryCount, IntermediateResults->Deadline, handleClass, false);
        ev->ReaderTabletData = {TabletInfo->TabletID, TabletGeneration};
        SendToBSProxy(ctx, prevGroup, ev.release(), cookie);
        return true;
    }

    void SendGetStatusRequests(const TActorContext &ctx) {
        for (ui64 i = 0; i < IntermediateResults->GetStatuses.size(); ++i) {
            auto &getStatus = IntermediateResults->GetStatuses[i];
            if (getStatus.Status != NKikimrProto::OK) {
                Y_VERIFY(getStatus.Status == NKikimrProto::UNKNOWN);
                const ui32 groupId = TabletInfo->GroupFor(getStatus.LogoBlobId.Channel(), getStatus.LogoBlobId.Generation());
                Y_VERIFY(groupId != Max<ui32>(), "GetStatus Blob# %s is mapped to an invalid group (-1)!",
                        getStatus.LogoBlobId.ToString().c_str());
                SendToBSProxy(
                        ctx, groupId,
                        new TEvBlobStorage::TEvStatus(IntermediateResults->Deadline), i);
                ++GetStatusRequestsSent;
            }
        }
        GetStatusSentAt = TAppData::TimeProvider->Now();
    }

    void SendRangeRequests(const TActorContext &ctx) {
        if (auto& cmd = IntermediateResults->TrimLeakedBlobs) {
            for (const auto& item : cmd->ChannelGroupMap) {
                ui32 channel;
                ui32 groupId;
                std::tie(channel, groupId) = item;
                const ui64 tabletId = TabletInfo->TabletID;
                TLogoBlobID from(tabletId, 0, 0, channel, 0, 0);
                TLogoBlobID to(tabletId, Max<ui32>(), Max<ui32>(), channel, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
                auto request = MakeHolder<TEvBlobStorage::TEvRange>(tabletId, from, to, false, TInstant::Max(), true);
                SendToBSProxy(ctx, groupId, request.Release());
                ++RangeRequestsSent;
            }
        }
    }

    void SendWriteRequests(const TActorContext &ctx) {
        auto sendWrite = [&](ui32 i, auto &request) -> void {
            using Type = std::decay_t<decltype(request)>;
            if constexpr (std::is_same_v<Type, TIntermediate::TWrite>) {
                if (request.Status != NKikimrProto::SCHEDULED) {
                    Y_VERIFY(request.Status == NKikimrProto::UNKNOWN);

                    const TRcBuf& data = request.Data;
                    const TContiguousSpan whole = data.GetContiguousSpan();

                    ui64 offset = 0;
                    for (const TLogoBlobID& logoBlobId : request.LogoBlobIds) {
                        const TContiguousSpan chunk = whole.SubSpan(offset, logoBlobId.BlobSize());
                        THolder<TEvBlobStorage::TEvPut> put(
                            new TEvBlobStorage::TEvPut(
                                logoBlobId, TRcBuf(TRcBuf::Piece, chunk.data(), chunk.size(), data),
                                IntermediateResults->Deadline, request.HandleClass,
                                request.Tactic));
                        const ui32 groupId = TabletInfo->GroupFor(logoBlobId.Channel(), logoBlobId.Generation());
                        Y_VERIFY(groupId != Max<ui32>(), "Put Blob# %s is mapped to an invalid group (-1)!",
                                logoBlobId.ToString().c_str());
                        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletInfo->TabletID
                                << " Send TEvPut# " << put->ToString() << " to groupId# " << groupId
                                << " now# " << TAppData::TimeProvider->Now().MilliSeconds() << " Marker# KV60");

                        SendPutToGroup(ctx, groupId, TabletInfo.Get(), std::move(put), i);

                        ++WriteRequestsSent;
                        offset += logoBlobId.BlobSize();
                    }
                }
            }
        };

        for (ui64 i : IntermediateResults->WriteIndices) {
            auto &cmd = IntermediateResults->Commands[i];
            Y_VERIFY(std::holds_alternative<TIntermediate::TWrite>(cmd));
            auto& write = std::get<TIntermediate::TWrite>(cmd);
            sendWrite(i, write);
        }

        for (ui64 i = 0; i < IntermediateResults->Writes.size(); ++i) {
            sendWrite(i, IntermediateResults->Writes[i]);
        }
        PutTimer.Reset();
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvGetResult, Handle);
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvBlobStorage::TEvStatusResult, Handle);
            HFunc(TEvBlobStorage::TEvRangeResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }
};

IActor* CreateKeyValueStorageRequest(
        THolder<TIntermediate>&& intermediate,
        const TTabletStorageInfo *tabletInfo,
        ui32 tabletGeneration) {
    return new TKeyValueStorageRequest(std::move(intermediate), tabletInfo, tabletGeneration);
}

} // NKeyValue
} // NKikimr
