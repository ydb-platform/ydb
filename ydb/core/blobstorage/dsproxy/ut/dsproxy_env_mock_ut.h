#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/dsproxy/dsproxy.h>
#include <ydb/core/blobstorage/dsproxy/dsproxy_nodemon.h>

#include <ydb/core/blobstorage/base/utility.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

inline TMaybe<TGroupStat::EKind> PutHandleClassToGroupStatKind(NKikimrBlobStorage::EPutHandleClass handleClass) {
    switch (handleClass) {
    case NKikimrBlobStorage::TabletLog:
        return  TGroupStat::EKind::PUT_TABLET_LOG;

    case NKikimrBlobStorage::UserData:
        return TGroupStat::EKind::PUT_USER_DATA;

    default:
        return {};
    }
}

struct TDSProxyEnv {
    TVector<TActorId> VDisks;
    TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TIntrusivePtr<TBlobStorageGroupProxyMon> Mon;
    TBSProxyContextPtr BSProxyCtxPtr;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> DynCounters;
    TIntrusivePtr<NKikimr::TStoragePoolCounters> StoragePoolCounters;
    TDiskResponsivenessTracker::TPerDiskStatsPtr PerDiskStatsPtr;
    TNodeLayoutInfoPtr NodeLayoutInfo;
    TIntrusivePtr<TGroupQueues> GroupQueues;
    ui64 GroupId;
    ui64 NodeIdx;
    TActorId RealProxyActorId;
    TActorId FakeProxyActorId;

    TActorId MakeVDiskId(ui32 failDomainIdx, ui32 driveIdx, ui32 drivesPerFailDomain) const {
        ui32 i = failDomainIdx * drivesPerFailDomain + driveIdx;
        return VDisks[i];
    }

    void Configure(TTestActorRuntime& runtime, TBlobStorageGroupType type, ui64 groupId, ui64 nodeIndex,
            TBlobStorageGroupInfo::EEncryptionMode encryptionMode = TBlobStorageGroupInfo::EEM_ENC_V1)
    {
        GroupId = groupId;
        NodeIdx = nodeIndex;

        ui32 vDiskCount = type.BlobSubgroupSize();
        VDisks.clear();
        for (ui32 i = 0; i < vDiskCount; ++i) {
            VDisks.push_back(runtime.AllocateEdgeActor(nodeIndex));
        }

        if (type.GetErasure() == TErasureType::ErasureMirror3dc) {
            Info = new TBlobStorageGroupInfo(type.GetErasure(), 1, 3, 3, &VDisks, encryptionMode);
        } else {
            Info = new TBlobStorageGroupInfo(type.GetErasure(), 1, type.BlobSubgroupSize(), 1, &VDisks, encryptionMode);
        }

        RealProxyActorId = MakeBlobStorageProxyID(groupId);
        TIntrusivePtr<TDsProxyNodeMon> nodeMon = new TDsProxyNodeMon(runtime.GetAppData(nodeIndex).Counters, true);
        TString name = Sprintf("%09" PRIu64, groupId);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> group = GetServiceCounters(
                runtime.GetAppData(0).Counters, "dsproxy")->GetSubgroup("blobstorageproxy", name);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> percentileGroup = GetServiceCounters(
                runtime.GetAppData(0).Counters, "dsproxy_percentile")->GetSubgroup("blobstorageproxy", name);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> overviewGroup = GetServiceCounters(
                runtime.GetAppData(0).Counters, "dsproxy_overview");
        BSProxyCtxPtr.Reset(new TBSProxyContext(group->GetSubgroup("subsystem", "memproxy")));
        Mon = new TBlobStorageGroupProxyMon(group, percentileGroup, overviewGroup, Info, nodeMon, false);
        TDsProxyPerPoolCounters perPoolCounters(runtime.GetAppData(nodeIndex).Counters);
        TIntrusivePtr<TStoragePoolCounters> storagePoolCounters = perPoolCounters.GetPoolCounters("pool_name");
        TControlWrapper enablePutBatching(DefaultEnablePutBatching, false, true);
        TControlWrapper enableVPatch(DefaultEnableVPatch, false, true);
        TControlWrapper slowDiskThreshold(DefaultSlowDiskThreshold * 1000, 1, 1000000);
        TControlWrapper predictedDelayMultiplier(DefaultPredictedDelayMultiplier * 1000, 1, 1000000);
        IActor *dsproxy = CreateBlobStorageGroupProxyConfigured(TIntrusivePtr(Info), true, nodeMon,
            std::move(storagePoolCounters), TBlobStorageProxyParameters{
                    .EnablePutBatching = enablePutBatching,
                    .EnableVPatch = enableVPatch,
                    .SlowDiskThreshold = slowDiskThreshold,
                    .PredictedDelayMultiplier = predictedDelayMultiplier,
                }
            );
        TActorId actorId = runtime.Register(dsproxy, nodeIndex);
        runtime.RegisterService(RealProxyActorId, actorId, nodeIndex);

        FakeProxyActorId = runtime.AllocateEdgeActor(0);
        TAutoPtr <IEventHandle> handle;
        runtime.Send(new IEventHandle(RealProxyActorId, FakeProxyActorId, new TEvRequestProxySessionsState));
        auto queues = runtime.GrabEdgeEventRethrow<TEvProxySessionsState>(handle);
        GroupQueues = queues->GroupQueues;
        NodeLayoutInfo = nullptr;
        DynCounters = new ::NMonitoring::TDynamicCounters();
        StoragePoolCounters = new NKikimr::TStoragePoolCounters(DynCounters, "", {});
        PerDiskStatsPtr = new TDiskResponsivenessTracker::TPerDiskStats;
    }

    void SetGroupGeneration(ui32 generation) {
        Info = new TBlobStorageGroupInfo(Info, TVDiskID(Info->GroupID, generation, 0, 0, 0), VDisks[0]);
    }

    std::unique_ptr<IActor> CreatePutRequestActor(TEvBlobStorage::TEvPut::TPtr &ev) {
        TMaybe<TGroupStat::EKind> kind = PutHandleClassToGroupStatKind(ev->Get()->HandleClass);
        return std::unique_ptr<IActor>(CreateBlobStorageGroupPutRequest(
                TBlobStorageGroupPutParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = GroupQueues,
                        .Mon = Mon,
                        .Source = ev->Sender,
                        .Cookie = ev->Cookie,
                        .Now =  TInstant::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = ev->Get()->RestartCounter,
                        .TraceId = std::move(ev->TraceId),
                        .Event = ev->Get(),
                        .ExecutionRelay = ev->Get()->ExecutionRelay,
                        .LatencyQueueKind = kind,
                    },
                    .TimeStatsEnabled = Mon->TimeStats.IsEnabled(),
                    .Stats = PerDiskStatsPtr,
                    .EnableRequestMod3x3ForMinLatency = false,
                }));
    }

    std::unique_ptr<IActor> CreatePutRequestActor(TBatchedVec<TEvBlobStorage::TEvPut::TPtr> &batched,
            TEvBlobStorage::TEvPut::ETactic tactic, NKikimrBlobStorage::EPutHandleClass handleClass)
    {
        TMaybe<TGroupStat::EKind> kind = PutHandleClassToGroupStatKind(handleClass);
        return std::unique_ptr<IActor>(CreateBlobStorageGroupPutRequest(
                TBlobStorageGroupMultiPutParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = GroupQueues,
                        .Mon = Mon,
                        .Now = TInstant::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = TBlobStorageGroupMultiPutParameters::CalculateRestartCounter(batched),
                        .LatencyQueueKind = kind,
                    },
                    .Events = batched,
                    .TimeStatsEnabled = Mon->TimeStats.IsEnabled(),
                    .Stats = PerDiskStatsPtr,
                    .HandleClass = handleClass,
                    .Tactic = tactic,
                    .EnableRequestMod3x3ForMinLatency = false,
                }));
    }

    std::unique_ptr<IActor> CreateGetRequestActor(TEvBlobStorage::TEvGet::TPtr &ev,
            NKikimrBlobStorage::EPutHandleClass handleClass)
    {
        TMaybe<TGroupStat::EKind> kind = PutHandleClassToGroupStatKind(handleClass);
        return std::unique_ptr<IActor>(CreateBlobStorageGroupGetRequest(
                TBlobStorageGroupGetParameters{
                    .Common = {
                        .GroupInfo = Info,
                        .GroupQueues = GroupQueues,
                        .Mon = Mon,
                        .Source = ev->Sender,
                        .Cookie = ev->Cookie,
                        .Now = TInstant::Now(),
                        .StoragePoolCounters = StoragePoolCounters,
                        .RestartCounter = ev->Get()->RestartCounter,
                        .TraceId = std::move(ev->TraceId),
                        .Event = ev->Get(),
                        .ExecutionRelay = ev->Get()->ExecutionRelay,
                        .LatencyQueueKind = kind,
                    },
                    .NodeLayout = TNodeLayoutInfoPtr(NodeLayoutInfo)
                }));
    }

    std::unique_ptr<IActor> CreatePatchRequestActor(TEvBlobStorage::TEvPatch::TPtr &ev, bool useVPatch = false) {
        return std::unique_ptr<IActor>(CreateBlobStorageGroupPatchRequest(
            TBlobStorageGroupPatchParameters{
                .Common = {
                    .GroupInfo = Info,
                    .GroupQueues = GroupQueues,
                    .Mon = Mon,
                    .Source = ev->Sender,
                    .Cookie = ev->Cookie,
                    .Now = TInstant::Now(),
                    .StoragePoolCounters = StoragePoolCounters,
                    .RestartCounter = ev->Get()->RestartCounter,
                    .TraceId = std::move(ev->TraceId),
                    .Event = ev->Get(),
                    .ExecutionRelay = ev->Get()->ExecutionRelay
                },
                .UseVPatch = useVPatch
            }));
    }
};

inline bool ScheduledFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event,
        TDuration delay, TInstant& deadline) {
    if (runtime.IsScheduleForActorEnabled(event->GetRecipientRewrite())) {
        deadline = runtime.GetTimeProvider()->Now() + delay;
        return false;
    }
    return true;
}

inline void SetupRuntime(TTestActorRuntime& runtime) {
    TAppPrepare app;
    app.ClearDomainsAndHive();
    runtime.SetScheduledEventFilter(&ScheduledFilterFunc);

    runtime.SetEventFilter([](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() == TEvBlobStorage::EvVCheckReadiness) {
            runtime.Send(new IEventHandle(
                    ev->Sender, ev->Recipient, new TEvBlobStorage::TEvVCheckReadinessResult(NKikimrProto::OK), 0,
                    ev->Cookie), 0, true);
            return true;
        }
        return false;
    });

    runtime.Initialize(app.Unwrap());
}

} // NKikimr
