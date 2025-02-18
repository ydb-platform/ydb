#pragma once

#include "defs.h"
#include "dsproxy_mon.h"
#include "dsproxy_responsiveness.h"
#include "log_acc.h"
#include "group_sessions.h"

#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/storagepoolmon/storagepool_counters.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/blobstorage/base/batched_vec.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/base/transparent.h>
#include <ydb/core/blobstorage/backpressure/queue_backpressure_client.h>
#include <ydb/core/blobstorage/common/immediate_control_defaults.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/group_stat.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

constexpr ui32 TypicalPartsInBlob = 6;
constexpr ui32 TypicalDisksInSubring = 8;

constexpr ui32 MaxBatchedPutSize = 64 * 1024 - 512 - 5; // (MinREALHugeBlobInBytes - 1 - TDiskBlob::HugeBlobOverhead) for ssd and nvme

const TDuration ProxyConfigurationTimeout = TDuration::Seconds(20);
const ui32 ProxyRetryConfigurationInitialTimeout = 200;
const ui32 ProxyRetryConfigurationMaxTimeout = 5000;
const ui64 UnconfiguredBufferSizeLimit = 32 << 20;

const TDuration ProxyEstablishSessionsTimeout = TDuration::Seconds(100);

const ui64 DsPutWakeupMs = 60000;

const ui64 BufferSizeThreshold = 1 << 20;

const bool IsHandoffAccelerationEnabled = false;

const bool IsEarlyRequestAbortEnabled = false;
const bool IngressAsAReasonForErrorEnabled = false;

const ui32 BeginRequestSize = 10;
const ui32 MaxRequestSize = 1000;

const ui32 MaskSizeBits = 32;

constexpr bool WithMovingPatchRequestToStaticNode = true;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Common types
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TDiskDelayPrediction {
    ui64 PredictedNs;
    ui32 DiskIdx;

    bool operator<(const TDiskDelayPrediction& other) const {
        return PredictedNs > other.PredictedNs;
    }
};

using TDiskDelayPredictions = TStackVec<TDiskDelayPrediction, TypicalDisksInSubring>;

struct TEvDeathNote : public TEventLocal<TEvDeathNote, TEvBlobStorage::EvDeathNote> {
    TStackVec<std::pair<TDiskResponsivenessTracker::TDiskId, TDuration>, 16> Responsiveness;

    TEvDeathNote(const TStackVec<std::pair<TDiskResponsivenessTracker::TDiskId, TDuration>, 16> &responsiveness)
        : Responsiveness(responsiveness)
    {}
};

struct TEvAbortOperation : public TEventLocal<TEvAbortOperation, TEvBlobStorage::EvAbortOperation>
{};

struct TEvLatencyReport : public TEventLocal<TEvLatencyReport, TEvBlobStorage::EvLatencyReport> {
    TGroupStat::EKind Kind;
    TDuration Sample;

    TEvLatencyReport(TGroupStat::EKind kind, TDuration sample)
        : Kind(kind)
        , Sample(sample)
    {}
};

struct TNodeLayoutInfo : TThrRefBase {
    // indexed by NodeId
    TNodeLocation SelfLocation;
    TVector<TNodeLocation> LocationPerOrderNumber;

    TNodeLayoutInfo(const TNodeLocation& selfLocation, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
            std::unordered_map<ui32, TNodeLocation>& map)
        : SelfLocation(selfLocation)
        , LocationPerOrderNumber(info->GetTotalVDisksNum())
    {
        for (ui32 i = 0; i < LocationPerOrderNumber.size(); ++i) {
            LocationPerOrderNumber[i] = map[info->GetActorId(i).NodeId()];
        }
    }
};

using TNodeLayoutInfoPtr = TIntrusivePtr<TNodeLayoutInfo>;

inline TStoragePoolCounters::EHandleClass HandleClassToHandleClass(NKikimrBlobStorage::EGetHandleClass handleClass) {
    switch (handleClass) {
        case NKikimrBlobStorage::FastRead:
            return TStoragePoolCounters::EHandleClass::HcGetFast;
        case NKikimrBlobStorage::AsyncRead:
            return TStoragePoolCounters::EHandleClass::HcGetAsync;
        case NKikimrBlobStorage::Discover:
            return TStoragePoolCounters::EHandleClass::HcGetDiscover;
        case NKikimrBlobStorage::LowRead:
            return TStoragePoolCounters::EHandleClass::HcGetLow;
    }
    return TStoragePoolCounters::EHandleClass::HcCount;
}

inline TStoragePoolCounters::EHandleClass HandleClassToHandleClass(NKikimrBlobStorage::EPutHandleClass handleClass) {
    switch (handleClass) {
        case NKikimrBlobStorage::TabletLog:
            return TStoragePoolCounters::EHandleClass::HcPutTabletLog;
        case NKikimrBlobStorage::UserData:
            return TStoragePoolCounters::EHandleClass::HcPutUserData;
        case NKikimrBlobStorage::AsyncBlob:
            return TStoragePoolCounters::EHandleClass::HcPutAsync;
    }
    return TStoragePoolCounters::EHandleClass::HcCount;
}

NActors::NLog::EPriority PriorityForStatusOutbound(NKikimrProto::EReplyStatus status);
NActors::NLog::EPriority PriorityForStatusResult(NKikimrProto::EReplyStatus status);
NActors::NLog::EPriority PriorityForStatusInbound(NKikimrProto::EReplyStatus status);

inline void SetExecutionRelay(IEventBase& ev, std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay) {
    switch (const ui32 type = ev.Type()) {
#define XX(T) \
        case TEvBlobStorage::Ev##T: \
            static_cast<TEvBlobStorage::TEv##T&>(ev).ExecutionRelay = std::move(executionRelay); \
            break; \
        case TEvBlobStorage::Ev##T##Result: \
            static_cast<TEvBlobStorage::TEv##T##Result&>(ev).ExecutionRelay = std::move(executionRelay); \
            break; \
        //

        XX(Put)
        XX(Get)
        XX(Block)
        XX(Discover)
        XX(Range)
        XX(CollectGarbage)
        XX(Status)
        XX(Patch)
        XX(Assimilate)
#undef XX

        default:
            Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
    }
}

struct TAccelerationParams {
    double SlowDiskThreshold = DefaultSlowDiskThreshold;
    double PredictedDelayMultiplier = DefaultPredictedDelayMultiplier;
    ui32 MaxNumOfSlowDisks = DefaultMaxNumOfSlowDisks;
};

template<typename TDerived>
class TBlobStorageGroupRequestActor : public TActor<TDerived> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_GROUP_REQUEST;
    }

    struct TCommonParameters {
        TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        TIntrusivePtr<TGroupQueues> GroupQueues;
        TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
        TActorId Source = TActorId{};
        ui64 Cookie = 0;
        TMonotonic Now;
        TIntrusivePtr<TStoragePoolCounters>& StoragePoolCounters;
        ui32 RestartCounter;
        NWilson::TSpan Span;
        TDerived* Event = nullptr;
        std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay = nullptr;

        bool LogAccEnabled = false;
        TMaybe<TGroupStat::EKind> LatencyQueueKind = {};
    };

    struct TTypeSpecificParameters {
        NKikimrServices::EServiceKikimr LogComponent;
        const char* Name;
        NKikimrServices::TActivity::EType Activity;
    };

public:
    template<typename TGroupRequestParameters>
    TBlobStorageGroupRequestActor(TGroupRequestParameters& params)
        : TActor<TDerived>(&TThis::InitialStateFunc, params.TypeSpecific.Activity)
        , Info(std::move(params.Common.GroupInfo))
        , GroupQueues(std::move(params.Common.GroupQueues))
        , Mon(std::move(params.Common.Mon))
        , PoolCounters(params.Common.StoragePoolCounters)
        , LogCtx(params.TypeSpecific.LogComponent, params.Common.LogAccEnabled)
        , Span(std::move(params.Common.Span))
        , RestartCounter(params.Common.RestartCounter)
        , CostModel(GroupQueues->CostModel)
        , RequestStartTime(params.Common.Now)
        , Source(params.Common.Source)
        , Cookie(params.Common.Cookie)
        , LatencyQueueKind(params.Common.LatencyQueueKind)
        , RacingDomains(&Info->GetTopology())
        , ExecutionRelay(std::move(params.Common.ExecutionRelay))
    {
        TDerived::ActiveCounter(Mon)->Inc();
        Span
            .Attribute("GroupId", Info->GroupID.GetRawId())
            .Attribute("RestartCounter", RestartCounter);

        Y_ABORT_UNLESS(CostModel);
    }

    void Registered(TActorSystem *as, const TActorId& parentId) override {
        ProxyActorId = parentId;
        as->Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0,
            TActor<TBlobStorageGroupRequestActor<TDerived>>::SelfId(), parentId, nullptr, 0));
        TActor<TBlobStorageGroupRequestActor<TDerived>>::Registered(as, parentId);
    }

    STRICT_STFUNC(InitialStateFunc,
        cFunc(TEvents::TSystem::Bootstrap, Derived().Bootstrap);
    )

    template<typename T>
    void CountEvent(const T &ev) const {
        ERequestType request = TDerived::RequestType();
        Mon->CountEvent(request, ev);
    }

    TActorId GetVDiskActorId(const TVDiskIdShort &shortId) const {
        return Info->GetActorId(shortId);
    }

    template<typename TEvent>
    bool CheckForTermErrors(TAutoPtr<TEventHandle<TEvent>>& ev, bool suppressCommonErrors) {
        auto& record = ev->Get()->Record;
        auto& self = Derived();

        if (!record.HasStatus()) {
            return false; // we do not consider messages with missing status/vdisk fields
        }

        NKikimrProto::EReplyStatus status = record.GetStatus(); // obtain status from the reply

        if (status == NKikimrProto::NOTREADY) { // special case from BS_QUEUE -- when connection is not yet established
            record.SetStatus(NKikimrProto::ERROR); // rewrite this status as error for processing
            PostponedQ.emplace_back(ev.Release());
            CheckPostponedQueue();
            return true; // event has been processed early
        }

        if (!record.HasVDiskID()) {
            return false; // bad reply?
        }

        auto done = [&](NKikimrProto::EReplyStatus status, const TString& message) {
            ErrorReason = message;
            A_LOG_LOG_S(true, PriorityForStatusResult(status), "DSP10", "Query failed " << message);
            self.ReplyAndDie(status);
            return true;
        };

        // sanity check for matching group id
        const TVDiskID& vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        if (vdiskId.GroupID != Info->GroupID) {
            return done(NKikimrProto::ERROR, TStringBuilder() << "incorrect VDiskId# " << vdiskId << " GroupId# "
                << Info->GroupID);
        }

        // sanity check for correct VDisk generation ??? possible race
        Y_VERIFY_S(status == NKikimrProto::RACE || vdiskId.GroupGeneration <= Info->GroupGeneration ||
            TEvent::EventType == TEvBlobStorage::EvVStatusResult || TEvent::EventType == TEvBlobStorage::EvVAssimilateResult,
            "status# " << NKikimrProto::EReplyStatus_Name(status) << " vdiskId.GroupGeneration# " << vdiskId.GroupGeneration
            << " Info->GroupGeneration# " << Info->GroupGeneration << " Response# " << ev->Get()->ToString());

        if (status != NKikimrProto::RACE && status != NKikimrProto::BLOCKED && status != NKikimrProto::DEADLINE) {
            return false; // these statuses are non-terminal
        } else if (status != NKikimrProto::RACE) {
            if (suppressCommonErrors) {
                return false; // these errors will be handled in host code
            }
            // this status is terminal and we have nothing to do about it
            return done(status, TStringBuilder() << "status# " << NKikimrProto::EReplyStatus_Name(status) << " from# "
                << vdiskId.ToString());
        }

        A_LOG_INFO_S("DSP99", "Handing RACE response from " << vdiskId << " GroupGeneration# " << Info->GroupGeneration
            << " Response# " << SingleLineProto(record));

        // process the RACE status
        const TActorId& nodeWardenId = MakeBlobStorageNodeWardenID(self.SelfId().NodeId());
        if (vdiskId.GroupGeneration < Info->GroupGeneration) { // vdisk is older than our group
            RacingDomains |= {&Info->GetTopology(), vdiskId};
            if (RacingDomains.GetNumSetItems() <= 1) {
                record.SetStatus(NKikimrProto::ERROR);
                auto adjustStatus = [](auto *v) {
                    for (int i = 0; i < v->size(); ++i) {
                        auto *p = v->Mutable(i);
                        if (p->GetStatus() == NKikimrProto::RACE) {
                            p->SetStatus(NKikimrProto::ERROR);
                        }
                    }
                };
                if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvVGetResult>) {
                    adjustStatus(record.MutableResult());
                } else if constexpr (std::is_same_v<TEvent, TEvBlobStorage::TEvVMultiPutResult>) {
                    adjustStatus(record.MutableItems());
                }
                return false;
            }
        } else if (Info->GroupGeneration < vdiskId.GroupGeneration) { // our config is older that vdisk's one
            std::optional<NKikimrBlobStorage::TGroupInfo> group;
            if (record.HasRecentGroup()) {
                group = record.GetRecentGroup();
                if (group->GetGroupID() != Info->GroupID.GetRawId() || group->GetGroupGeneration() != vdiskId.GroupGeneration) {
                    return done(NKikimrProto::ERROR, "incorrect RecentGroup for RACE response");
                }
            }
            self.Send(nodeWardenId, new TEvBlobStorage::TEvUpdateGroupInfo(vdiskId.GroupID, vdiskId.GroupGeneration,
                std::move(group)));
        }

        // make NodeWarden restart the query just after proxy reconfiguration
        Y_DEBUG_ABORT_UNLESS(RestartCounter < 100);
        auto q = self.RestartQuery(RestartCounter + 1);
        if (q->Type() != TEvBlobStorage::EvBunchOfEvents) {
            SetExecutionRelay(*q, std::exchange(ExecutionRelay, {}));
        }
        ++*Mon->NodeMon->RestartHisto[Min<size_t>(Mon->NodeMon->RestartHisto.size() - 1, RestartCounter)];
        const TActorId& proxyId = MakeBlobStorageProxyID(Info->GroupID);
        TActivationContext::Send(new IEventHandle(nodeWardenId, Source, q.release(), 0, Cookie, &proxyId, Span.GetTraceId()));
        PassAway();
        return true;
    }

    bool ProcessEvent(TAutoPtr<IEventHandle>& ev, bool suppressCommonErrors = false) {
        switch (ev->GetTypeRewrite()) {
#define CHECK(T) case TEvBlobStorage::T::EventType: return CheckForTermErrors(reinterpret_cast<TEvBlobStorage::T::TPtr&>(ev), suppressCommonErrors)
            CHECK(TEvVPutResult);
            CHECK(TEvVMultiPutResult);
            CHECK(TEvVGetResult);
            CHECK(TEvVBlockResult);
            CHECK(TEvVGetBlockResult);
            CHECK(TEvVCollectGarbageResult);
            CHECK(TEvVGetBarrierResult);
            CHECK(TEvVStatusResult);
            CHECK(TEvVAssimilateResult);
#undef CHECK

            case TEvBlobStorage::EvProxySessionsState: {
                GroupQueues = ev->Get<TEvProxySessionsState>()->GroupQueues;
                return true;
            }

            case TEvAbortOperation::EventType: {
                if (IsEarlyRequestAbortEnabled) {
                    ErrorReason = "Request got EvAbortOperation, IsEarlyRequestAbortEnabled# true";
                    Derived().ReplyAndDie(NKikimrProto::ERROR);
                }
                return true;
            }

            case TEvents::TSystem::Poison: {
                ErrorReason = "Request got Poison";
                Derived().ReplyAndDie(NKikimrProto::ERROR);
                return true;
            }

            case TEvBlobStorage::EvDeadline: {
                ErrorReason = "Deadline timer hit";
                Derived().ReplyAndDie(NKikimrProto::DEADLINE);
                return true;
            }
        }

        return false;
    }

    template<typename TEv>
    void CountPut(const std::unique_ptr<TEv>& ev) {
        ++GeneratedSubrequests;
        GeneratedSubrequestBytes += ev->GetBufferBytes();
    }

    template<typename TEv>
    void CountPuts(const TDeque<std::unique_ptr<TEv>>& q) {
        for (const auto& item : q) {
            CountPut(item);
        }
    }

    template<typename... TOptions>
    void CountPuts(const TDeque<std::variant<TOptions...>>& q) {
        for (const auto& item : q) {
            std::visit([&](auto& item) { CountPut(item); }, item);
        }
    }

    template<typename T>
    void SendToQueue(std::unique_ptr<T> event, ui64 cookie, bool timeStatsEnabled = false) {
        if constexpr (
            !std::is_same_v<T, TEvBlobStorage::TEvVGetBlock>
            && !std::is_same_v<T, TEvBlobStorage::TEvVBlock>
            && !std::is_same_v<T, TEvBlobStorage::TEvVStatus>
            && !std::is_same_v<T, TEvBlobStorage::TEvVCollectGarbage>
            && !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate>
        ) {
            const ui64 cyclesPerUs = NHPTimer::GetCyclesPerSecond() / 1000000;
            event->Record.MutableTimestamps()->SetSentByDSProxyUs(GetCycleCountFast() / cyclesPerUs);
        }

        if constexpr (!std::is_same_v<T, TEvBlobStorage::TEvVStatus> && !std::is_same_v<T, TEvBlobStorage::TEvVAssimilate>) {
            event->MessageRelevanceTracker = MessageRelevanceTracker;
            ui64 cost;
            if constexpr (std::is_same_v<T, TEvBlobStorage::TEvVMultiPut>) {
                bool internalQueue;
                cost = CostModel->GetCost(*event, &internalQueue);
            } else {
                cost = CostModel->GetCost(*event);
            }
            *PoolCounters->DSProxyDiskCostCounter += cost;

            LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::BS_REQUEST_COST,
                    "DSProxy Request Type# " << TypeName(*event) << " Cost# " << cost);
        }

        const TActorId queueId = GroupQueues->Send(*this, Info->GetTopology(), std::move(event), cookie, Span.GetTraceId(),
            timeStatsEnabled);
        ++RequestsInFlight;
    }

    template<typename TPtr>
    void ProcessReplyFromQueue(const TPtr& /*ev*/) {
        Y_ABORT_UNLESS(RequestsInFlight);
        --RequestsInFlight;
        CheckPostponedQueue();
    }

    void SendToQueues(TDeque<std::unique_ptr<TEvBlobStorage::TEvVGet>> &vGets, bool timeStatsEnabled) {
        for (auto& request : vGets) {
            const ui64 messageCookie = request->Record.GetCookie();
            CountEvent(*request);
            SendToQueue(std::move(request), messageCookie, timeStatsEnabled);
        }
    }

    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVPut> &ev) {
        Y_ABORT_UNLESS(ev->Record.HasBlobID());
        return LogoBlobIDFromLogoBlobID(ev->Record.GetBlobID());
    }

    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVMultiPut> &ev) {
        Y_ABORT_UNLESS(ev->Record.ItemsSize());
        return LogoBlobIDFromLogoBlobID(ev->Record.GetItems(0).GetBlobID());
    }

    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVMovedPatch> &ev) {
        Y_ABORT_UNLESS(ev->Record.HasPatchedBlobId());
        return LogoBlobIDFromLogoBlobID(ev->Record.GetPatchedBlobId());
    }

    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVPatchStart> &ev) {
        Y_ABORT_UNLESS(ev->Record.HasOriginalBlobId());
        return LogoBlobIDFromLogoBlobID(ev->Record.GetOriginalBlobId());
    }

    TLogoBlobID GetBlobId(std::unique_ptr<TEvBlobStorage::TEvVPatchDiff> &ev) {
        Y_ABORT_UNLESS(ev->Record.HasPatchedPartBlobId());
        return LogoBlobIDFromLogoBlobID(ev->Record.GetPatchedPartBlobId());
    }

    template <typename TEvent>
    void SendToQueues(TDeque<std::unique_ptr<TEvent>> &events, bool timeStatsEnabled) {
        for (auto& request : events) {
            ui64 messageCookie = request->Record.GetCookie();
            CountEvent(*request);
            TLogoBlobID id = GetBlobId(request);
            TVDiskID vDiskId = VDiskIDFromVDiskID(request->Record.GetVDiskID());
            LWTRACK(DSProxyPutVPutIsSent, request->Orbit, Info->GetFailDomainOrderNumber(vDiskId),
                    Info->GroupID.GetRawId(), id.Channel(), id.PartId(), id.ToString(), id.BlobSize());
            SendToQueue(std::move(request), messageCookie, timeStatsEnabled);
        }
    }

    void SendToProxy(std::unique_ptr<IEventBase> event, ui64 cookie = 0, NWilson::TTraceId traceId = {}) {
        Derived().Send(ProxyActorId, event.release(), 0, cookie, std::move(traceId));
    }

    void SendResponseAndDie(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats, TActorId source,
            ui64 cookie) {
        SendResponse(std::move(ev), timeStats, source, cookie);
        PassAway();
    }

    void SendResponseAndDie(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats = nullptr) {
        SendResponseAndDie(std::move(ev), timeStats, Source, Cookie);
    }

    void PassAway() override {
        // ensure we didn't keep execution relay on occasion
        Y_VERIFY_DEBUG_S(!ExecutionRelay, LogCtx.RequestPrefix << " actor died without properly sending response");

        // ensure that we are dying for the first time
        Y_ABORT_UNLESS(!std::exchange(Dead, true));
        TDerived::ActiveCounter(Mon)->Dec();
        SendToProxy(std::make_unique<TEvDeathNote>(Responsiveness));
        TActorBootstrapped<TDerived>::PassAway();
    }

    void SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats, TActorId source, ui64 cookie,
            bool term = true) {
        const TInstant now = TActivationContext::Now();

        NKikimrProto::EReplyStatus status;
        TString errorReason;

        switch (ev->Type()) {
#define XX(T) \
            case TEvBlobStorage::Ev##T##Result: { \
                auto& msg = static_cast<TEvBlobStorage::TEv##T##Result&>(*ev); \
                status = msg.Status; \
                errorReason = msg.ErrorReason; \
                Mon->RespStat##T->Account(status); \
                break; \
            }

            XX(Put)
            XX(Get)
            XX(Block)
            XX(Discover)
            XX(Range)
            XX(CollectGarbage)
            XX(Status)
            XX(Patch)
            XX(Assimilate)
            default:
                Y_ABORT();
#undef XX
        }

        if (ExecutionRelay) {
            SetExecutionRelay(*ev, std::exchange(ExecutionRelay, {}));
            ExecutionRelayUsed = true;
        } else {
            Y_ABORT_UNLESS(!ExecutionRelayUsed);
        }

        // ensure that we are dying for the first time
        Y_ABORT_UNLESS(!Dead);
        if (RequestHandleClass && PoolCounters && FirstResponse) {
            PoolCounters->GetItem(*RequestHandleClass, RequestBytes).Register(
                RequestBytes, GeneratedSubrequests, GeneratedSubrequestBytes, Timer.Passed());
        }

        if (timeStats) {
            SendToProxy(std::make_unique<TEvTimeStats>(std::move(*timeStats)));
        }

        if (LatencyQueueKind) {
            SendToProxy(std::make_unique<TEvLatencyReport>(*LatencyQueueKind, TActivationContext::Monotonic() - RequestStartTime));
        }

        // KIKIMR-6737
        if (ev->Type() == TEvBlobStorage::EvGetResult) {
            static_cast<TEvBlobStorage::TEvGetResult&>(*ev).Sent = now;
        }

        if (term) {
            if (status == NKikimrProto::OK) {
                Span.EndOk();
            } else {
                Span.EndError(std::move(errorReason));
            }
        }

        // send the reply to original request sender
        Derived().Send(source, ev.release(), 0, cookie);
        FirstResponse = false;
    };

    void SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats = nullptr) {
        SendResponse(std::move(ev), timeStats, Source, Cookie);
    }

    static double GetStartTime(const NKikimrBlobStorage::TTimestamps& timestamps) {
        return timestamps.GetSentByDSProxyUs() / 1e6;
    }

    static double GetTotalTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps) {
        return double(timestamps.GetReceivedByDSProxyUs() - timestamps.GetSentByDSProxyUs())/1000.0;
    }

    static double GetVDiskTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps) {
        return double(timestamps.GetSentByVDiskUs() - timestamps.GetReceivedByVDiskUs())/1000.0;
    }

private:
    TDerived& Derived() {
        return static_cast<TDerived&>(*this);
    }

    void CheckPostponedQueue() {
        if (PostponedQ.size() == RequestsInFlight) {
            for (auto& ev : std::exchange(PostponedQ, {})) {
                TActivationContext::Send(ev.release());
            }
        }
    }

protected:
    using TThis = TDerived;

    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TIntrusivePtr<TStoragePoolCounters> PoolCounters;
    TLogContext LogCtx;
    NWilson::TSpan Span;
    TStackVec<std::pair<TDiskResponsivenessTracker::TDiskId, TDuration>, 16> Responsiveness;
    TString ErrorReason;
    TMaybe<TStoragePoolCounters::EHandleClass> RequestHandleClass;
    ui32 RequestBytes = 0;
    ui32 GeneratedSubrequests = 0;
    ui32 GeneratedSubrequestBytes = 0;
    bool Dead = false;
    const ui32 RestartCounter = 0;
    std::shared_ptr<const TCostModel> CostModel;
    const TMonotonic RequestStartTime;

private:
    const TActorId Source;
    const ui64 Cookie;
    std::shared_ptr<TMessageRelevanceTracker> MessageRelevanceTracker = std::make_shared<TMessageRelevanceTracker>();
    ui32 RequestsInFlight = 0;
    std::unique_ptr<IEventBase> Response;
    const TMaybe<TGroupStat::EKind> LatencyQueueKind;
    THPTimer Timer;
    std::deque<std::unique_ptr<IEventHandle>> PostponedQ;
    TBlobStorageGroupInfo::TGroupFailDomains RacingDomains; // a set of domains we've received RACE from
    TActorId ProxyActorId;
    std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay;
    bool ExecutionRelayUsed = false;
    bool FirstResponse = true;
};

void Encrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
        const TBlobStorageGroupInfo &info);

void EncryptInplace(TRope& rope, ui32 offset, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

void Decrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
        const TBlobStorageGroupInfo &info);
void DecryptInplace(TRope& rope, ui32 offset, ui32 shift, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

struct TBlobStorageGroupRangeParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvRange>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvRange>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_RANGE,
        .Name = "DSProxy.Range",
        .Activity = NKikimrServices::TActivity::BS_GROUP_RANGE,
    };
};
IActor* CreateBlobStorageGroupRangeRequest(TBlobStorageGroupRangeParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupPutParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvPut>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvPut>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_PUT,
        .Name = "DSProxy.Put",
        .Activity = NKikimrServices::TActivity::BS_PROXY_PUT_ACTOR,
    };
    bool TimeStatsEnabled;
    TDiskResponsivenessTracker::TPerDiskStatsPtr Stats;
    bool EnableRequestMod3x3ForMinLatency;
    TAccelerationParams AccelerationParams;
    TDuration LongRequestThreshold;
};
IActor* CreateBlobStorageGroupPutRequest(TBlobStorageGroupPutParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupMultiPutParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvPut>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvPut>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_PUT,
        .Name = "DSProxy.Put",
        .Activity = NKikimrServices::TActivity::BS_PROXY_PUT_ACTOR,
    };

    TBatchedVec<TEvBlobStorage::TEvPut::TPtr>& Events;
    bool TimeStatsEnabled;
    TDiskResponsivenessTracker::TPerDiskStatsPtr Stats;
    NKikimrBlobStorage::EPutHandleClass HandleClass;
    TEvBlobStorage::TEvPut::ETactic Tactic;
    bool EnableRequestMod3x3ForMinLatency;
    TAccelerationParams AccelerationParams;
    TDuration LongRequestThreshold;

    static ui32 CalculateRestartCounter(TBatchedVec<TEvBlobStorage::TEvPut::TPtr>& events) {
        ui32 maxRestarts = 0;
        for (const auto& ev : events) {
            maxRestarts = std::max(maxRestarts, ev->Get()->RestartCounter);
        }
        return maxRestarts;
    }
};
IActor* CreateBlobStorageGroupPutRequest(TBlobStorageGroupMultiPutParameters params);

struct TBlobStorageGroupGetParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvGet>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvGet>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_GET,
        .Name = "DSProxy.Get",
        .Activity = NKikimrServices::TActivity::BS_PROXY_GET_ACTOR,
    };
    TNodeLayoutInfoPtr NodeLayout;
    TAccelerationParams AccelerationParams;
    TDuration LongRequestThreshold;
};
IActor* CreateBlobStorageGroupGetRequest(TBlobStorageGroupGetParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupPatchParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvPatch>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvPatch>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_PATCH,
        .Name = "DSProxy.Patch",
        .Activity = NKikimrServices::TActivity::BS_PROXY_PATCH_ACTOR,
    };

    bool UseVPatch = false;
};
IActor* CreateBlobStorageGroupPatchRequest(TBlobStorageGroupPatchParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupMultiGetParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvGet>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvGet>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_MULTIGET,
        .Name = "DSProxy.MultiGet",
        .Activity = NKikimrServices::TActivity::BS_PROXY_MULTIGET_ACTOR,
    };
    bool UseVPatch = false;
};
IActor* CreateBlobStorageGroupMultiGetRequest(TBlobStorageGroupMultiGetParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupRestoreGetParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvGet>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvGet>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_INDEXRESTOREGET,
        .Name = "DSProxy.IndexRestoreGet",
        .Activity = NKikimrServices::TActivity::BS_PROXY_INDEXRESTOREGET_ACTOR,
    };
};
IActor* CreateBlobStorageGroupIndexRestoreGetRequest(TBlobStorageGroupRestoreGetParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupDiscoverParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvDiscover>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvDiscover>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_DISCOVER,
        .Name = "DSProxy.Discover",
        .Activity = NKikimrServices::TActivity::BS_GROUP_DISCOVER,
    };
};
IActor* CreateBlobStorageGroupDiscoverRequest(TBlobStorageGroupDiscoverParameters params, NWilson::TTraceId traceId);
IActor* CreateBlobStorageGroupMirror3dcDiscoverRequest(TBlobStorageGroupDiscoverParameters params, NWilson::TTraceId traceId);
IActor* CreateBlobStorageGroupMirror3of4DiscoverRequest(TBlobStorageGroupDiscoverParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupCollectGarbageParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvCollectGarbage>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvCollectGarbage>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_COLLECT,
        .Name = "DSProxy.CollectGarbage",
        .Activity = NKikimrServices::TActivity::BS_GROUP_COLLECT_GARBAGE,
    };
};
IActor* CreateBlobStorageGroupCollectGarbageRequest(TBlobStorageGroupCollectGarbageParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupMultiCollectParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvCollectGarbage>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvCollectGarbage>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_MULTICOLLECT,
        .Name = "DSProxy.MultiCollect",
        .Activity = NKikimrServices::TActivity::BS_PROXY_MULTICOLLECT_ACTOR,
    };
};
IActor* CreateBlobStorageGroupMultiCollectRequest(TBlobStorageGroupMultiCollectParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupBlockParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvBlock>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvBlock>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_BLOCK,
        .Name = "DSProxy.Block",
        .Activity = NKikimrServices::TActivity::BS_GROUP_BLOCK,
    };
};
IActor* CreateBlobStorageGroupBlockRequest(TBlobStorageGroupBlockParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupStatusParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvStatus>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvStatus>::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_STATUS,
        .Name = "DSProxy.Status",
        .Activity = NKikimrServices::TActivity::BS_PROXY_STATUS_ACTOR,
    };
};
IActor* CreateBlobStorageGroupStatusRequest(TBlobStorageGroupStatusParameters params, NWilson::TTraceId traceId);

struct TBlobStorageGroupAssimilateParameters {
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvAssimilate>::TCommonParameters Common;
    TBlobStorageGroupRequestActor<TEvBlobStorage::TEvAssimilate>
    ::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_ASSIMILATE,
        .Name = "DSProxy.Assimilate",
        .Activity = NKikimrServices::TActivity::BS_GROUP_ASSIMILATE,
    };
};
IActor* CreateBlobStorageGroupAssimilateRequest(TBlobStorageGroupAssimilateParameters params, NWilson::TTraceId traceId);

IActor* CreateBlobStorageGroupEjectedProxy(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon);

struct TBlobStorageProxyControlWrappers {
    TMemorizableControlWrapper EnablePutBatching;
    TMemorizableControlWrapper EnableVPatch;
    TMemorizableControlWrapper LongRequestThresholdMs = LongRequestThresholdDefaultControl;

#define DEVICE_TYPE_SEPECIFIC_MEMORIZABLE_CONTROLS(prefix)              \
    TMemorizableControlWrapper prefix = prefix##DefaultControl;         \
    TMemorizableControlWrapper prefix##HDD = prefix##DefaultControl;    \
    TMemorizableControlWrapper prefix##SSD = prefix##DefaultControl

    // Acceleration parameters
    DEVICE_TYPE_SEPECIFIC_MEMORIZABLE_CONTROLS(SlowDiskThreshold);
    DEVICE_TYPE_SEPECIFIC_MEMORIZABLE_CONTROLS(PredictedDelayMultiplier);

    TMemorizableControlWrapper MaxNumOfSlowDisks = MaxNumOfSlowDisksDefaultControl;
    TMemorizableControlWrapper MaxNumOfSlowDisksHDD = MaxNumOfSlowDisksHDDDefaultControl;
    TMemorizableControlWrapper MaxNumOfSlowDisksSSD = MaxNumOfSlowDisksDefaultControl;

#undef DEVICE_TYPE_SEPECIFIC_MEMORIZABLE_CONTROLS

};

struct TBlobStorageProxyParameters {
    bool UseActorSystemTimeInBSQueue = false;

    TBlobStorageProxyControlWrappers Controls;
};

IActor* CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>&& info,
    bool forceWaitAllDrives, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
    TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters, const TBlobStorageProxyParameters& params);

IActor* CreateBlobStorageGroupProxyUnconfigured(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
    const TBlobStorageProxyParameters& params);

}//NKikimr
