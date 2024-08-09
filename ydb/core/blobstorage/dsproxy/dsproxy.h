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

constexpr bool DefaultEnablePutBatching = true;
constexpr bool DefaultEnableVPatch = false;

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

#define DSPROXY_ENUM_EVENTS(XX) \
    XX(TEvBlobStorage::TEvPut) \
    XX(TEvBlobStorage::TEvGet) \
    XX(TEvBlobStorage::TEvBlock) \
    XX(TEvBlobStorage::TEvDiscover) \
    XX(TEvBlobStorage::TEvRange) \
    XX(TEvBlobStorage::TEvCollectGarbage) \
    XX(TEvBlobStorage::TEvStatus) \
    XX(TEvBlobStorage::TEvPatch) \
    XX(TEvBlobStorage::TEvAssimilate) \
//

#define DSPROXY_ENUM_DISK_EVENTS(XX) \
    XX(TEvBlobStorage::TEvVMovedPatch) \
    XX(TEvBlobStorage::TEvVPatchStart) \
    XX(TEvBlobStorage::TEvVPatchDiff) \
    XX(TEvBlobStorage::TEvVPatchXorDiff) \
    XX(TEvBlobStorage::TEvVPut) \
    XX(TEvBlobStorage::TEvVMultiPut) \
    XX(TEvBlobStorage::TEvVGet) \
    XX(TEvBlobStorage::TEvVBlock) \
    XX(TEvBlobStorage::TEvVGetBlock) \
    XX(TEvBlobStorage::TEvVCollectGarbage) \
    XX(TEvBlobStorage::TEvVGetBarrier) \
    XX(TEvBlobStorage::TEvVStatus) \
    XX(TEvBlobStorage::TEvVAssimilate) \
//

inline void SetExecutionRelay(IEventBase& ev, std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay) {
    switch (const ui32 type = ev.Type()) {
#define XX(T) \
        case T::EventType: \
            static_cast<T&>(ev).ExecutionRelay = std::move(executionRelay); \
            break; \
        case T##Result::EventType: \
            static_cast<T##Result&>(ev).ExecutionRelay = std::move(executionRelay); \
            break; \
        //

        DSPROXY_ENUM_EVENTS(XX)
#undef XX

        default:
            Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
    }
}

class TBlobStorageGroupRequestActor : public TActor<TBlobStorageGroupRequestActor> {
public:
    template<typename TEv>
    struct TCommonParameters {
        TIntrusivePtr<TBlobStorageGroupInfo> GroupInfo;
        TIntrusivePtr<TGroupQueues> GroupQueues;
        TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
        TActorId Source = TActorId{};
        ui64 Cookie = 0;
        TInstant Now;
        TIntrusivePtr<TStoragePoolCounters>& StoragePoolCounters;
        ui32 RestartCounter;
        NWilson::TTraceId TraceId = {};
        TEv* Event = nullptr;
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
        : TActor(&TThis::InitialStateFunc, params.TypeSpecific.Activity)
        , Info(std::move(params.Common.GroupInfo))
        , GroupQueues(std::move(params.Common.GroupQueues))
        , Mon(std::move(params.Common.Mon))
        , PoolCounters(params.Common.StoragePoolCounters)
        , LogCtx(params.TypeSpecific.LogComponent, params.Common.LogAccEnabled)
        , ParentSpan(TWilson::BlobStorage, std::move(params.Common.TraceId), params.TypeSpecific.Name)
        , RestartCounter(params.Common.RestartCounter)
        , CostModel(GroupQueues->CostModel)
        , Source(params.Common.Source)
        , Cookie(params.Common.Cookie)
        , LatencyQueueKind(params.Common.LatencyQueueKind)
        , RequestStartTime(params.Common.Now)
        , RacingDomains(&Info->GetTopology())
        , ExecutionRelay(std::move(params.Common.ExecutionRelay))
    {
        if (ParentSpan) {
            const NWilson::TTraceId& parentTraceId = ParentSpan.GetTraceId();
            Span = NWilson::TSpan(TWilson::BlobStorage, NWilson::TTraceId::NewTraceId(parentTraceId.GetVerbosity(),
                parentTraceId.GetTimeToLive()), ParentSpan.GetName());
            ParentSpan.Link(Span.GetTraceId());
            Span.Attribute("GroupId", Info->GroupID.GetRawId());
            Span.Attribute("RestartCounter", RestartCounter);
            params.Common.Event->ToSpan(Span);
        }

        Y_ABORT_UNLESS(CostModel);
    }

    virtual ~TBlobStorageGroupRequestActor() = default;

    virtual void Bootstrap() = 0;
    virtual void ReplyAndDie(NKikimrProto::EReplyStatus status) = 0;
    virtual std::unique_ptr<IEventBase> RestartQuery(ui32 counter) = 0;
    virtual ERequestType GetRequestType() const = 0;
    virtual ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const = 0;

    void Registered(TActorSystem *as, const TActorId& parentId) override;

    STFUNC(InitialStateFunc);

    void BootstrapImpl();

    void CountEvent(IEventBase *ev, ui32 type) const;

    void CountPut(ui32 bufferBytes);

    TActorId GetVDiskActorId(const TVDiskIdShort &shortId) const;

    bool CheckForTermErrors(bool suppressCommonErrors, const NProtoBuf::Message& record, ui32 type,
        NKikimrProto::EReplyStatus status, TVDiskID vdiskId, const NKikimrBlobStorage::TGroupInfo *group,
        bool& setErrorAndPostpone, bool& setRaceToError);
    bool ProcessEvent(TAutoPtr<IEventHandle>& ev, bool suppressCommonErrors = false);

    void SendToQueue(std::unique_ptr<IEventBase> event, ui64 cookie, bool timeStatsEnabled = false);

    void ProcessReplyFromQueue(IEventBase *ev);

    static TLogoBlobID GetBlobId(TEvBlobStorage::TEvVPut& ev);
    static TLogoBlobID GetBlobId(TEvBlobStorage::TEvVMultiPut& ev);
    static TLogoBlobID GetBlobId(TEvBlobStorage::TEvVMovedPatch& ev);
    static TLogoBlobID GetBlobId(TEvBlobStorage::TEvVPatchStart& ev);
    static TLogoBlobID GetBlobId(TEvBlobStorage::TEvVPatchDiff& ev);

    void SendToProxy(std::unique_ptr<IEventBase> event, ui64 cookie = 0, NWilson::TTraceId traceId = {});
    void SendResponseAndDie(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats, TActorId source, ui64 cookie);
    void SendResponseAndDie(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats = nullptr);

    void PassAway() override;

    void SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats, TActorId source,
        ui64 cookie, bool term = true);
    void SendResponse(std::unique_ptr<IEventBase>&& ev, TBlobStorageGroupProxyTimeStats *timeStats = nullptr);

    static double GetStartTime(const NKikimrBlobStorage::TTimestamps& timestamps);
    static double GetTotalTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps);
    static double GetVDiskTimeMs(const NKikimrBlobStorage::TTimestamps& timestamps);

private:
    void CheckPostponedQueue();

protected:
    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TIntrusivePtr<TStoragePoolCounters> PoolCounters;
    TLogContext LogCtx;
    NWilson::TSpan ParentSpan;
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

private:
    const TActorId Source;
    const ui64 Cookie;
    std::shared_ptr<TMessageRelevanceTracker> MessageRelevanceTracker = std::make_shared<TMessageRelevanceTracker>();
    ui32 RequestsInFlight = 0;
    std::unique_ptr<IEventBase> Response;
    const TMaybe<TGroupStat::EKind> LatencyQueueKind;
    const TInstant RequestStartTime;
    THPTimer Timer;
    std::deque<std::unique_ptr<IEventHandle>> PostponedQ;
    TBlobStorageGroupInfo::TGroupFailDomains RacingDomains; // a set of domains we've received RACE from
    TActorId ProxyActorId;
    std::shared_ptr<TEvBlobStorage::TExecutionRelay> ExecutionRelay;
    bool ExecutionRelayUsed = false;
};

void Encrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
        const TBlobStorageGroupInfo &info);

void EncryptInplace(TRope& rope, ui32 offset, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

void Decrypt(char *destination, const char *source, size_t shift, size_t sizeBytes, const TLogoBlobID &id,
        const TBlobStorageGroupInfo &info);
void DecryptInplace(TRope& rope, ui32 offset, ui32 shift, ui32 size, const TLogoBlobID& id, const TBlobStorageGroupInfo& info);

struct TBlobStorageGroupRangeParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvRange> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_RANGE,
        .Name = "DSProxy.Range",
        .Activity = NKikimrServices::TActivity::BS_GROUP_RANGE
        ,
    };
};
IActor* CreateBlobStorageGroupRangeRequest(TBlobStorageGroupRangeParameters params);

struct TBlobStorageGroupPutParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvPut> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_PUT,
        .Name = "DSProxy.Put",
        .Activity = NKikimrServices::TActivity::BS_PROXY_PUT_ACTOR,
    };
    bool TimeStatsEnabled;
    TDiskResponsivenessTracker::TPerDiskStatsPtr Stats;
    bool EnableRequestMod3x3ForMinLatency;
};
IActor* CreateBlobStorageGroupPutRequest(TBlobStorageGroupPutParameters params);

struct TBlobStorageGroupMultiPutParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvPut> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
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
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvGet> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_GET,
        .Name = "DSProxy.Get",
        .Activity = NKikimrServices::TActivity::BS_PROXY_GET_ACTOR,
    };
    TNodeLayoutInfoPtr NodeLayout;
};
IActor* CreateBlobStorageGroupGetRequest(TBlobStorageGroupGetParameters params);

struct TBlobStorageGroupPatchParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvPatch> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_PATCH,
        .Name = "DSProxy.Patch",
        .Activity = NKikimrServices::TActivity::BS_PROXY_PATCH_ACTOR,
    };

    bool UseVPatch = false;
};
IActor* CreateBlobStorageGroupPatchRequest(TBlobStorageGroupPatchParameters params);

struct TBlobStorageGroupMultiGetParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvGet> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_MULTIGET,
        .Name = "DSProxy.MultiGet",
        .Activity = NKikimrServices::TActivity::BS_PROXY_MULTIGET_ACTOR,
    };
    bool UseVPatch = false;
};
IActor* CreateBlobStorageGroupMultiGetRequest(TBlobStorageGroupMultiGetParameters params);

struct TBlobStorageGroupRestoreGetParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvGet> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_INDEXRESTOREGET,
        .Name = "DSProxy.IndexRestoreGet",
        .Activity = NKikimrServices::TActivity::BS_PROXY_INDEXRESTOREGET_ACTOR,
    };
};
IActor* CreateBlobStorageGroupIndexRestoreGetRequest(TBlobStorageGroupRestoreGetParameters params);

struct TBlobStorageGroupDiscoverParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvDiscover> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_DISCOVER,
        .Name = "DSProxy.Discover",
        .Activity = NKikimrServices::TActivity::BS_GROUP_DISCOVER,
    };
};
IActor* CreateBlobStorageGroupDiscoverRequest(TBlobStorageGroupDiscoverParameters params);
IActor* CreateBlobStorageGroupMirror3dcDiscoverRequest(TBlobStorageGroupDiscoverParameters params);
IActor* CreateBlobStorageGroupMirror3of4DiscoverRequest(TBlobStorageGroupDiscoverParameters params);

struct TBlobStorageGroupCollectGarbageParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvCollectGarbage> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_COLLECT,
        .Name = "DSProxy.CollectGarbage",
        .Activity = NKikimrServices::TActivity::BS_GROUP_COLLECT_GARBAGE,
    };
};
IActor* CreateBlobStorageGroupCollectGarbageRequest(TBlobStorageGroupCollectGarbageParameters params);

struct TBlobStorageGroupMultiCollectParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvCollectGarbage> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_MULTICOLLECT,
        .Name = "DSProxy.MultiCollect",
        .Activity = NKikimrServices::TActivity::BS_PROXY_MULTICOLLECT_ACTOR,
    };
};
IActor* CreateBlobStorageGroupMultiCollectRequest(TBlobStorageGroupMultiCollectParameters params);

struct TBlobStorageGroupBlockParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvBlock> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_BLOCK,
        .Name = "DSProxy.Block",
        .Activity = NKikimrServices::TActivity::BS_GROUP_BLOCK,
    };
};
IActor* CreateBlobStorageGroupBlockRequest(TBlobStorageGroupBlockParameters params);

struct TBlobStorageGroupStatusParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvStatus> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_STATUS,
        .Name = "DSProxy.Status",
        .Activity = NKikimrServices::TActivity::BS_PROXY_STATUS_ACTOR,
    };
};
IActor* CreateBlobStorageGroupStatusRequest(TBlobStorageGroupStatusParameters params);

struct TBlobStorageGroupAssimilateParameters {
    TBlobStorageGroupRequestActor::TCommonParameters<TEvBlobStorage::TEvAssimilate> Common;
    TBlobStorageGroupRequestActor::TTypeSpecificParameters TypeSpecific = {
        .LogComponent = NKikimrServices::BS_PROXY_ASSIMILATE,
        .Name = "DSProxy.Assimilate",
        .Activity = NKikimrServices::TActivity::BS_GROUP_ASSIMILATE,
    };
};
IActor* CreateBlobStorageGroupAssimilateRequest(TBlobStorageGroupAssimilateParameters params);

IActor* CreateBlobStorageGroupEjectedProxy(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon);

IActor* CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>&& info,
    bool forceWaitAllDrives, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
    TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters, const TControlWrapper &enablePutBatching,
    const TControlWrapper &enableVPatch);

IActor* CreateBlobStorageGroupProxyUnconfigured(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
    const TControlWrapper &enablePutBatching, const TControlWrapper &enableVPatch);

}//NKikimr
