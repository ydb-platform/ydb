#pragma once

#include "cluster_info.h"
#include "config.h"

#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NKikimr::NCms {

struct TTaskInfo {
    TString TaskId;
    TString RequestId;
    TString Owner;
    TSet<TString> Permissions;
    bool HasSingleCompositeActionGroup = false;

    TString ToString() const {
        return TStringBuilder() << "{"
            << " TaskId: " << TaskId
            << " RequestId: " << RequestId
            << " Owner: " << Owner
            << " Permissions: [" << JoinSeq(", ", Permissions) << "]"
            << " HasSingleCompositeActionGroup: " << HasSingleCompositeActionGroup
            << " }";
    }
};

struct TCmsState : public TAtomicRefCount<TCmsState> {
    // Main state.
    THashMap<TString, TPermissionInfo> Permissions;
    THashMap<TString, TRequestInfo> ScheduledRequests;
    THashMap<TString, TNotificationInfo> Notifications;
    THashMap<TString, THashSet<NKikimrCms::EMarker>> HostMarkers;
    TDowntimes Downtimes;
    ui64 NextPermissionId = 0;
    ui64 NextRequestId = 0;
    ui64 NextNotificationId = 0;
    ui64 LastLogRecordTimestamp = 0;

    // State of Wall-E tasks.
    THashMap<TString, TTaskInfo> WalleTasks;
    THashMap<TString, TString> WalleRequests;

    THashMap<TString, TTaskInfo> MaintenanceTasks;
    THashMap<TString, TString> MaintenanceRequests;

    // CMS config.
    TCmsConfig Config;
    // CMS config proto cache
    NKikimrCms::TCmsConfig ConfigProto;

    // Cluster info. It's not initialized on state creation.
    // Updated by event from info collector by rewritting
    // pointer. Therefore pointer shouldnt be preserved
    // in local structures and should be accessed through
    // pointer to CMS state.
    TClusterInfoPtr ClusterInfo;
    THashMap<ui32, TString> InitialNodeTenants; // would be applyed to ClusterInfo at first update

    // Static info.
    ui64 CmsTabletId = 0;
    TActorId CmsActorId;
    TActorId BSControllerPipe;
    TActorId Sentinel;

    bool EnableCMSRequestPriorities = false;
    bool EnableSingleCompositeActionGroup = false;
};

using TCmsStatePtr = TIntrusivePtr<TCmsState>;

} // namespace NKikimr::NCms
