#include "dsproxy_impl.h"

namespace NKikimr {

    std::atomic<TMonotonic> TBlobStorageGroupProxy::ThrottlingTimestamp;

    TBlobStorageGroupProxy::TBlobStorageGroupProxy(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon, TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters, 
            const TBlobStorageProxyParameters& params)
        : GroupId(info->GroupID)
        , Info(std::move(info))
        , Topology(Info->PickTopology())
        , NodeMon(nodeMon)
        , StoragePoolCounters(std::move(storagePoolCounters))
        , IsEjected(false)
        , ForceWaitAllDrives(forceWaitAllDrives)
        , UseActorSystemTimeInBSQueue(params.UseActorSystemTimeInBSQueue)
        , EnablePutBatching(params.EnablePutBatching)
        , EnableVPatch(params.EnableVPatch)
        , SlowDiskThreshold(params.SlowDiskThreshold)
        , PredictedDelayMultiplier(params.PredictedDelayMultiplier)
        , LongRequestThreshold(params.LongRequestThreshold)
    {}

    TBlobStorageGroupProxy::TBlobStorageGroupProxy(ui32 groupId, bool isEjected,TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            const TBlobStorageProxyParameters& params)
        : GroupId(TGroupId::FromValue(groupId))
        , NodeMon(nodeMon)
        , IsEjected(isEjected)
        , ForceWaitAllDrives(false)
        , UseActorSystemTimeInBSQueue(params.UseActorSystemTimeInBSQueue)
        , EnablePutBatching(params.EnablePutBatching)
        , EnableVPatch(params.EnableVPatch)
        , SlowDiskThreshold(params.SlowDiskThreshold)
        , PredictedDelayMultiplier(params.PredictedDelayMultiplier)
        , LongRequestThreshold(params.LongRequestThreshold)
    {}

    IActor* CreateBlobStorageGroupEjectedProxy(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon) {
        return new TBlobStorageGroupProxy(groupId, true, nodeMon, 
                TBlobStorageProxyParameters{
                    .EnablePutBatching = TControlWrapper(false, false, true),
                    .EnableVPatch = TControlWrapper(false, false, true),
                    .SlowDiskThreshold = TControlWrapper(2000, 1, 1000000),
                    .PredictedDelayMultiplier = TControlWrapper(1000, 1, 1000000),
                }
        );
    }

    IActor* CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon, TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters,
            const TBlobStorageProxyParameters& params) {
        Y_ABORT_UNLESS(info);
        return new TBlobStorageGroupProxy(std::move(info), forceWaitAllDrives, nodeMon,
                std::move(storagePoolCounters), params);
    }

    IActor* CreateBlobStorageGroupProxyUnconfigured(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            const TBlobStorageProxyParameters& params) {
        return new TBlobStorageGroupProxy(groupId, false, nodeMon, params);
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
