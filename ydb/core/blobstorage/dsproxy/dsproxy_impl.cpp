#include "dsproxy_impl.h"

namespace NKikimr {

    std::atomic<TMonotonic> TBlobStorageGroupProxy::ThrottlingTimestamp;

    TBlobStorageGroupProxy::TBlobStorageGroupProxy(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon, TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters,
            const TControlWrapper &enablePutBatching, const TControlWrapper &enableVPatch)
        : GroupId(info->GroupID)
        , Info(std::move(info))
        , Topology(Info->PickTopology())
        , NodeMon(nodeMon)
        , StoragePoolCounters(std::move(storagePoolCounters))
        , IsEjected(false)
        , ForceWaitAllDrives(forceWaitAllDrives)
        , EnablePutBatching(enablePutBatching)
        , EnableVPatch(enableVPatch)
    {}

    TBlobStorageGroupProxy::TBlobStorageGroupProxy(ui32 groupId, bool isEjected, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            const TControlWrapper &enablePutBatching, const TControlWrapper &enableVPatch)
        : GroupId(TGroupId::FromValue(groupId))
        , NodeMon(nodeMon)
        , IsEjected(isEjected)
        , ForceWaitAllDrives(false)
        , EnablePutBatching(enablePutBatching)
        , EnableVPatch(enableVPatch)
    {}

    IActor* CreateBlobStorageGroupEjectedProxy(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon) {
        return new TBlobStorageGroupProxy(groupId, true, nodeMon, TControlWrapper(false, false, true),
                TControlWrapper(false, false, true));
    }

    IActor* CreateBlobStorageGroupProxyConfigured(TIntrusivePtr<TBlobStorageGroupInfo>&& info, bool forceWaitAllDrives,
            TIntrusivePtr<TDsProxyNodeMon> &nodeMon, TIntrusivePtr<TStoragePoolCounters>&& storagePoolCounters,
            const TControlWrapper &enablePutBatching, const TControlWrapper &enableVPatch) {
        Y_ABORT_UNLESS(info);
        return new TBlobStorageGroupProxy(std::move(info), forceWaitAllDrives, nodeMon, std::move(storagePoolCounters),
                enablePutBatching, enableVPatch);
    }

    IActor* CreateBlobStorageGroupProxyUnconfigured(ui32 groupId, TIntrusivePtr<TDsProxyNodeMon> &nodeMon,
            const TControlWrapper &enablePutBatching, const TControlWrapper &enableVPatch) {
        return new TBlobStorageGroupProxy(groupId, false, nodeMon, enablePutBatching, enableVPatch);
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
