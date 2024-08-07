#include "dsproxy_impl.h"

namespace NKikimr {

    std::atomic<TMonotonic> TBlobStorageGroupProxy::ThrottlingTimestamp;

    TBlobStorageGroupProxy::TBlobStorageGroupProxy(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            bool useActorSystemTimeInBSQueue, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters, const TControlWrapper &enablePutBatching,
            const TControlWrapper &enableVPatch, const TControlWrapper &slowDiskThreshold,
            const TControlWrapper &predictedDelayMultiplier)
        : GroupId(info->GroupID)
        , Info(std::move(info))
        , Topology(Info->PickTopology())
        , NodeMon(nodeMon)
        , StoragePoolCounters(std::move(storagePoolCounters))
        , IsEjected(false)
        , ForceWaitAllDrives(forceWaitAllDrives)
        , UseActorSystemTimeInBSQueue(useActorSystemTimeInBSQueue)
        , EnablePutBatching(enablePutBatching)
        , EnableVPatch(enableVPatch)
        , SlowDiskThreshold(slowDiskThreshold)
        , PredictedDelayMultiplier(predictedDelayMultiplier)
    {}

    TBlobStorageGroupProxy::TBlobStorageGroupProxy(ui32 groupId, bool isEjected, bool useActorSystemTimeInBSQueue,
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon, const TControlWrapper &enablePutBatching,
            const TControlWrapper &enableVPatch, const TControlWrapper &slowDiskThreshold,
            const TControlWrapper &predictedDelayMultiplier)
        : GroupId(TGroupId::FromValue(groupId))
        , NodeMon(nodeMon)
        , IsEjected(isEjected)
        , ForceWaitAllDrives(false)
        , UseActorSystemTimeInBSQueue(useActorSystemTimeInBSQueue)
        , EnablePutBatching(enablePutBatching)
        , EnableVPatch(enableVPatch)
        , SlowDiskThreshold(slowDiskThreshold)
        , PredictedDelayMultiplier(predictedDelayMultiplier)
    {}

    IActor* CreateBlobStorageGroupEjectedProxy(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon) {
        return new TBlobStorageGroupProxy(groupId, true, false, nodeMon, TControlWrapper(false, false, true),
                TControlWrapper(false, false, true), TControlWrapper(1000, 1, 1000000),
                TControlWrapper(1000, 1, 1000000));
    }

    IActor* CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            bool useActorSystemTimeInBSQueue, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters, const TControlWrapper &enablePutBatching,
            const TControlWrapper &enableVPatch, const TControlWrapper &slowDiskThreshold,
            const TControlWrapper &predictedDelayMultiplier) {
        Y_ABORT_UNLESS(info);
        return new TBlobStorageGroupProxy(std::move(info), forceWaitAllDrives, useActorSystemTimeInBSQueue, nodeMon,
                std::move(storagePoolCounters), enablePutBatching, enableVPatch, slowDiskThreshold, predictedDelayMultiplier);
    }

    IActor* CreateBlobStorageGroupProxyUnconfigured(ui32 groupId, bool useActorSystemInBSQueue, 
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon,const TControlWrapper &enablePutBatching,
            const TControlWrapper &enableVPatch, const TControlWrapper &slowDiskThreshold,
            const TControlWrapper &predictedDelayMultiplier) {
        return new TBlobStorageGroupProxy(groupId, false, useActorSystemInBSQueue, nodeMon,
                enablePutBatching, enableVPatch, slowDiskThreshold, predictedDelayMultiplier);
    }

    NActors::NLog::EPriority PriorityForStatusOutbound(NKikimrProto::EReplyStatus status) {
        switch (status) {
            case NKikimrProto::OK:
            case NKikimrProto::ALREADY:
                return NActors::NLog::EPriority::PRI_DEBUG;
            case NKikimrProto::NODATA:
                return NActors::NLog::EPriority::PRI_WARN;
            default:
                return NActors::NLog::EPriority::PRI_ERROR;
        }
    }

    NActors::NLog::EPriority PriorityForStatusResult(NKikimrProto::EReplyStatus status) {
        switch (status) {
            case NKikimrProto::OK:
            case NKikimrProto::ALREADY:
                return NActors::NLog::EPriority::PRI_DEBUG;
            case NKikimrProto::BLOCKED:
            case NKikimrProto::DEADLINE:
            case NKikimrProto::RACE:
            case NKikimrProto::ERROR:
                return NActors::NLog::EPriority::PRI_INFO;
            case NKikimrProto::NODATA:
                return NActors::NLog::EPriority::PRI_NOTICE;
            default:
                return NActors::NLog::EPriority::PRI_ERROR;
        }
    }

    NActors::NLog::EPriority PriorityForStatusInbound(NKikimrProto::EReplyStatus status) {
        switch (status) {
            case NKikimrProto::OK:
            case NKikimrProto::ALREADY:
                return NActors::NLog::EPriority::PRI_DEBUG;
            case NKikimrProto::NODATA:
            case NKikimrProto::ERROR:
            case NKikimrProto::VDISK_ERROR_STATE:
            case NKikimrProto::OUT_OF_SPACE:
                return NActors::NLog::EPriority::PRI_INFO;
            default:
                return NActors::NLog::EPriority::PRI_ERROR;
        }
    }

}//NKikimr
