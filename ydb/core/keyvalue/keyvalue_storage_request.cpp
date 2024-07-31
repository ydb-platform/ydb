#include "keyvalue_flat_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/util/log_priority_mute_checker.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/public/lib/base/msgbus.h>

namespace NKikimr {
namespace NKeyValue {
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Storage request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TKeyValueStorageRequest : public TActorBootstrapped<TKeyValueStorageRequest> {
    using TBase = TActorBootstrapped<TKeyValueStorageRequest>;

    ui64 ReadRequestsSent = 0;
    ui64 ReadRequestsReplied = 0;
    ui64 WriteRequestsSent = 0;
    ui64 WriteRequestsReplied = 0;
    ui64 PatchRequestsSent = 0;
    ui64 PatchRequestsReplied = 0;
    ui64 GetStatusRequestsSent = 0;
    ui64 GetStatusRequestsReplied = 0;
    ui64 RangeRequestsSent = 0;
    ui64 RangeRequestsReplied = 0;
    ui64 InFlightLimit = 50;
    ui64 InFlightLimitSeq;
    ui64 InFlightQueries = 0;
    ui64 InFlightRequestsLimit = 10;
    ui64 NextInFlightBatchCookie = 1;
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

    NWilson::TSpan Span;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    TKeyValueStorageRequest(THolder<TIntermediate>&& intermediate, const TTabletStorageInfo *tabletInfo, ui32 tabletGeneration)
        : InFlightLimitSeq(intermediate->SequentialReadLimit)
        , TabletGeneration(tabletGeneration)
        , IntermediateResults(std::move(intermediate))
        , TabletInfo(const_cast<TTabletStorageInfo*>(tabletInfo))
        , Span(TWilsonTablet::TabletBasic, IntermediateResults->Span.GetTraceId(), "KeyValue.StorageRequest")
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
            str << " > commands#" << (ui32)IntermediateResults->Commands.size();
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

    void Handle(TEvBlobStorage::TEvPatchResult::TPtr &ev, const TActorContext &ctx) {
        auto groupId = ev->Get()->GroupId;
        CheckYellow(ev->Get()->StatusFlags, groupId.GetRawId());

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            TInstant now = TAppData::TimeProvider->Now();

            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " Unexpected EvPatch result# " << NKikimrProto::EReplyStatus_Name(status).data();
            str << " Deadline# " << IntermediateResults->Deadline.MilliSeconds();
            str << " Now# " << now.MilliSeconds();
            str << " GotAt# " << IntermediateResults->Stat.IntermediateCreatedAt.MilliSeconds();
            str << " LastSuccess# " << LastSuccess;
            str << " SinceLastSuccess# " << (now - LastSuccess).MilliSeconds();
            str << " EnqueuedAs# " << IntermediateResults->Stat.EnqueuedAs;
            str << " ErrorReason# " << ev->Get()->ErrorReason;
            str << " Marker# KV70";

            NLog::EPriority logPriority = ErrorStateMuteChecker.Register(now, MuteDuration);

            ReplyErrorAndDie(ctx, str.Str(),
                status == NKikimrProto::TIMEOUT ? NMsgBusProxy::MSTATUS_TIMEOUT : NMsgBusProxy::MSTATUS_INTERNALERROR,
                logPriority);
            return;
        }

        ui64 cookie = ev->Cookie;
        ui64 patchIdx = cookie;
        if (patchIdx >= IntermediateResults->Patches.size() && patchIdx >= IntermediateResults->Commands.size()) {
            TStringStream str;
            str << "KeyValue# " << TabletInfo->TabletID;
            str << " EvPatch cookie# " << (ui64)cookie;
            str << " > writes#" << (ui32)IntermediateResults->Patches.size();
            str << " > commands#" << (ui32)IntermediateResults->Commands.size();
            str << " Marker# KV71";
            ReplyErrorAndDie(ctx, str.Str());
            return;
        }

        TIntermediate::TPatch *patch = nullptr;
        if (IntermediateResults->Patches.size()) {
            patch = &IntermediateResults->Patches[patchIdx];
        } else {
            patch = &std::get<TIntermediate::TPatch>(IntermediateResults->Commands[patchIdx]);
        }
        patch->StatusFlags.Merge(ev->Get()->StatusFlags.Raw);
        ++PatchRequestsReplied;
        IntermediateResults->Stat.GroupWrittenBytes[std::make_pair(ev->Get()->Id.Channel(), groupId.GetRawId())] += ev->Get()->Id.BlobSize();
        IntermediateResults->Stat.GroupWrittenIops[std::make_pair(ev->Get()->Id.Channel(), groupId.GetRawId())] += 1;
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
        Y_ABORT_UNLESS(!InFlightBatchByCookie.empty());

        // Find the corresponding request (as replies come in random order)
        auto foundIt = InFlightBatchByCookie.find(ev->Cookie);
        Y_ABORT_UNLESS(foundIt != InFlightBatchByCookie.end(), "Cookie# %" PRIu64 " not found!", ev->Cookie);
        TInFlightBatch &request = foundIt->second;

        InFlightQueries -= request.ReadQueue.size();

        ui64 durationMs = (TAppData::TimeProvider->Now() - request.SentAt).MilliSeconds();
        IntermediateResults->Stat.GetLatencies.push_back(durationMs);

        auto resetReadItems = [&](NKikimrProto::EReplyStatus status) {
            Y_ABORT_UNLESS(status != NKikimrProto::UNKNOWN);
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

        Y_ABORT_UNLESS(ev->Get()->ResponseSz == request.ReadQueue.size());
        auto groupId = ev->Get()->GroupId;
        decltype(request.ReadQueue)::iterator it = request.ReadQueue.begin();
        for (ui32 i = 0, num = ev->Get()->ResponseSz; i < num; ++i, ++it) {
            auto& response = ev->Get()->Responses[i];
            auto& read = *it->Read;
            auto& readItem = *it->ReadItem;

            if (response.Status == NKikimrProto::OK) {
                Y_ABORT_UNLESS(response.Buffer.size() == readItem.BlobSize);
                Y_ABORT_UNLESS(readItem.ValueOffset + readItem.BlobSize <= read.ValueSize);
                IntermediateResults->Stat.GroupReadBytes[std::make_pair(response.Id.Channel(), groupId)] += response.Buffer.size();
                IntermediateResults->Stat.GroupReadIops[std::make_pair(response.Id.Channel(), groupId)] += 1; // FIXME: count distinct blobs?
                read.Value.Write(readItem.ValueOffset, std::move(response.Buffer));
            } else {
                Y_VERIFY_DEBUG_S(response.Status != NKikimrProto::NODATA, "NODATA received for TEvGet"
                    << " TabletId# " << TabletInfo->TabletID
                    << " Id# " << response.Id
                    << " Key# " << read.Key);

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

            Y_ABORT_UNLESS(response.Status != NKikimrProto::UNKNOWN);
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
                << " PatchRequestSent# " << PatchRequestsSent
                << " PatchRequestReplied# " << PatchRequestsReplied
                << " Marker# KV45");
        if (ReadRequestsReplied == ReadRequestsSent &&
                WriteRequestsReplied == WriteRequestsSent &&
                GetStatusRequestsReplied == GetStatusRequestsSent &&
                RangeRequestsSent == RangeRequestsReplied &&
                PatchRequestsSent == PatchRequestsReplied) {
            for (auto& write : IntermediateResults->Writes) {
                if (write.Status == NKikimrProto::UNKNOWN) {
                    write.Status = NKikimrProto::OK;
                }
            }
            for (auto& patch : IntermediateResults->Patches) {
                if (patch.Status == NKikimrProto::UNKNOWN) {
                    patch.Status = NKikimrProto::OK;
                }
            }
            for (auto& getStatus : IntermediateResults->GetStatuses) {
                if (getStatus.Status == NKikimrProto::UNKNOWN) {
                    getStatus.Status = NKikimrProto::OK;
                }
            }
            for (auto& cmd : IntermediateResults->Commands) {
                if (std::holds_alternative<TIntermediate::TWrite>(cmd)) {
                    auto& write = std::get<TIntermediate::TWrite>(cmd);
                    if (write.Status == NKikimrProto::UNKNOWN) {
                        write.Status = NKikimrProto::OK;
                    }
                }
                if (std::holds_alternative<TIntermediate::TPatch>(cmd)) {
                    auto& patch = std::get<TIntermediate::TPatch>(cmd);
                    if (patch.Status == NKikimrProto::UNKNOWN) {
                        patch.Status = NKikimrProto::OK;
                    }
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
            Span.EndOk();
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
        SendPatchRequests(ctx);
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
            IntermediateResults->Stat, status, std::move(IntermediateResults->RefCountsIncr)), 0, 0, Span.GetTraceId());

        Span.EndError(TStringBuilder() << status << ": " << errorDescription);
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
            Y_ABORT_UNLESS(!readItem.InFlight && readItem.Status == NKikimrProto::UNKNOWN);

            const TLogoBlobID& id = readItem.LogoBlobId;
            bool isSameChannel = (request.ReadQueue.empty() || id.Channel() == prevId.Channel());
            if (!isSameChannel) {
                skippedItems.push_back(std::move(*it));
                continue;
            }
            const ui32 group = TabletInfo->GroupFor(id.Channel(), id.Generation());
            Y_ABORT_UNLESS(group != Max<ui32>(), "ReadItem Blob# %s is mapped to an invalid group (-1)!",
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
            Y_ABORT_UNLESS(group != Max<ui32>(), "Get Blob# %s is mapped to an invalid group (-1)!",
                    readItem.LogoBlobId.ToString().c_str());
            if (prevGroup != Max<ui32>()) {
                Y_ABORT_UNLESS(prevGroup == group);
            } else {
                prevGroup = group;
            }
        }

        ++ReadRequestsSent;
        request.SentAt = TAppData::TimeProvider->Now();

        ui64 cookie = NextInFlightBatchCookie;
        ++NextInFlightBatchCookie;
        InFlightBatchByCookie[cookie] = std::move(request);

        Y_ABORT_UNLESS(queryIdx == readQueryCount);

        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(readQueries, readQueryCount, IntermediateResults->Deadline, handleClass, false);
        ev->ReaderTabletData = {TabletInfo->TabletID, TabletGeneration};
        SendToBSProxy(ctx, prevGroup, ev.release(), cookie, Span.GetTraceId());
        return true;
    }

    void SendGetStatusRequests(const TActorContext &ctx) {
        for (ui64 i = 0; i < IntermediateResults->GetStatuses.size(); ++i) {
            auto &getStatus = IntermediateResults->GetStatuses[i];
            if (getStatus.Status != NKikimrProto::OK) {
                Y_ABORT_UNLESS(getStatus.Status == NKikimrProto::UNKNOWN);
                SendToBSProxy(
                        ctx, getStatus.GroupId,
                        new TEvBlobStorage::TEvStatus(IntermediateResults->Deadline), i, Span.GetTraceId());
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
                SendToBSProxy(ctx, groupId, request.Release(), 0, Span.GetTraceId());
                ++RangeRequestsSent;
            }
        }
    }

    void SendWriteRequests(const TActorContext &ctx) {
        auto sendWrite = [&](ui32 i, auto &request) -> void {
            using Type = std::decay_t<decltype(request)>;
            if constexpr (std::is_same_v<Type, TIntermediate::TWrite>) {
                if (request.Status != NKikimrProto::SCHEDULED) {
                    Y_ABORT_UNLESS(request.Status == NKikimrProto::UNKNOWN);

                    const TRope& data = request.Data;
                    auto iter = data.begin();

                    for (const TLogoBlobID& logoBlobId : request.LogoBlobIds) {
                        const auto begin = iter;
                        iter += logoBlobId.BlobSize();
                        THolder<TEvBlobStorage::TEvPut> put(
                            new TEvBlobStorage::TEvPut(
                                logoBlobId, TRcBuf(TRope(begin, iter)),
                                IntermediateResults->Deadline, request.HandleClass,
                                request.Tactic));
                        const ui32 groupId = TabletInfo->GroupFor(logoBlobId.Channel(), logoBlobId.Generation());
                        Y_ABORT_UNLESS(groupId != Max<ui32>(), "Put Blob# %s is mapped to an invalid group (-1)!",
                                logoBlobId.ToString().c_str());
                        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletInfo->TabletID
                                << " Send TEvPut# " << put->ToString() << " to groupId# " << groupId
                                << " now# " << TAppData::TimeProvider->Now().MilliSeconds() << " Marker# KV60");

                        SendPutToGroup(ctx, groupId, TabletInfo.Get(), std::move(put), i, Span.GetTraceId());

                        ++WriteRequestsSent;
                    }
                }
            }
        };

        for (ui64 i : IntermediateResults->WriteIndices) {
            auto &cmd = IntermediateResults->Commands[i];
            Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TWrite>(cmd));
            auto& write = std::get<TIntermediate::TWrite>(cmd);
            sendWrite(i, write);
        }

        for (ui64 i = 0; i < IntermediateResults->Writes.size(); ++i) {
            sendWrite(i, IntermediateResults->Writes[i]);
        }
        PutTimer.Reset();
    }

    void SendPatchRequests(const TActorContext &ctx) {
        auto sendPatch = [&](ui32 i, auto &request) -> void {
            using Type = std::decay_t<decltype(request)>;
            if constexpr (std::is_same_v<Type, TIntermediate::TPatch>) {

                ui32 originalGroupId = TabletInfo->GroupFor(request.OriginalBlobId.Channel(), request.OriginalBlobId.Generation());

                TArrayHolder<TEvBlobStorage::TEvPatch::TDiff> diffs(new TEvBlobStorage::TEvPatch::TDiff[request.Diffs.size()]);
                for (ui32 diffIdx = 0; diffIdx < request.Diffs.size(); ++diffIdx) {
                    auto &diff = request.Diffs[diffIdx];
                    diffs[diffIdx].Buffer = TRcBuf(diff.Buffer);
                    diffs[diffIdx].Offset = diff.Offset;
                }

                THolder<TEvBlobStorage::TEvPatch> patch(
                    new TEvBlobStorage::TEvPatch(
                        originalGroupId, request.OriginalBlobId, request.PatchedBlobId, TLogoBlobID::MaxCookie,
                        std::move(diffs), request.Diffs.size(), IntermediateResults->Deadline));
                
                const ui32 groupId = TabletInfo->GroupFor(request.PatchedBlobId.Channel(), request.PatchedBlobId.Generation());
                Y_VERIFY_S(groupId != Max<ui32>(), "Patch Blob# " << request.PatchedBlobId.ToString() << " is mapped to an invalid group (-1)!");
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletInfo->TabletID
                        << " Send TEvPatch# " << patch->ToString() << " to groupId# " << groupId
                        << " now# " << TAppData::TimeProvider->Now().MilliSeconds() << " Marker# KV69");


                SendPatchToGroup(ctx, groupId, TabletInfo.Get(), std::move(patch), i, Span.GetTraceId());
                ++PatchRequestsSent;
            }
        };

        for (ui64 i : IntermediateResults->PatchIndices) {
            auto &cmd = IntermediateResults->Commands[i];
            Y_ABORT_UNLESS(std::holds_alternative<TIntermediate::TPatch>(cmd));
            auto& patch = std::get<TIntermediate::TPatch>(cmd);
            sendPatch(i, patch);
        }

        for (ui64 i = 0; i < IntermediateResults->Patches.size(); ++i) {
            sendPatch(i, IntermediateResults->Patches[i]);
        }
        PutTimer.Reset();
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvGetResult, Handle);
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvBlobStorage::TEvPatchResult, Handle);
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
