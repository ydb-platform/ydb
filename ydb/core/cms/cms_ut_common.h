#pragma once

#include "cms.h"
#include "ut_helpers.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/basics/helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <util/datetime/base.h>
#include <util/system/mutex.h>

namespace NKikimr::NCmsTest {

using NNodeWhiteboard::TTabletId;
using TNodeTenantsMap = THashMap<ui32, TVector<TString>>;

struct TFakeNodeInfo {
    struct TVDiskIDComparator {
        bool operator ()(const TVDiskID &a, const TVDiskID &b) const {
            return std::make_tuple(a.GroupID, a.FailRealm, a.FailDomain, a.VDisk)
                    < std::make_tuple(b.GroupID, b.FailRealm, b.FailDomain, b.VDisk);
        }
    };

    TMap<TTabletId, NKikimrWhiteboard::TTabletStateInfo> TabletStateInfo;
    TMap<TString, NKikimrWhiteboard::TNodeStateInfo> NodeStateInfo;
    TMap<ui32, NKikimrWhiteboard::TPDiskStateInfo> PDiskStateInfo;
    TMap<TVDiskID, NKikimrWhiteboard::TVDiskStateInfo, TVDiskIDComparator> VDiskStateInfo;
    NKikimrWhiteboard::TSystemStateInfo SystemStateInfo;
    bool Connected = true;
    bool VDisksMoved = false;
};

class TFakeNodeWhiteboardService : public TActorBootstrapped<TFakeNodeWhiteboardService> {
public:
    using TEvWhiteboard = NNodeWhiteboard::TEvWhiteboard;

    static inline NKikimrBlobStorage::TEvControllerConfigResponse Config;
    static inline NKikimrConfig::TBootstrap BootstrapConfig;
    static inline THashMap<ui32, TFakeNodeInfo> Info;
    static inline THashMap<NCms::TPDiskID, TVector<bool>, NCms::TPDiskIDHash> BSControllerResponsePatterns;
    static inline bool NoisyBSCPipe = false;
    static inline ui32 NoisyBSCPipeCounter = 0;
    static inline TMutex Mutex;

    void Bootstrap(const TActorContext &ctx) {
        Y_UNUSED(ctx);
        Become(&TFakeNodeWhiteboardService::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvControllerConfigRequest, Handle);
            HFunc(TEvWhiteboard::TEvTabletStateRequest, Handle);
            HFunc(TEvWhiteboard::TEvNodeStateRequest, Handle);
            HFunc(TEvWhiteboard::TEvPDiskStateRequest, Handle);
            HFunc(TEvWhiteboard::TEvVDiskStateRequest, Handle);
            HFunc(TEvWhiteboard::TEvSystemStateRequest, Handle);
            HFunc(NConsole::TEvConfigsDispatcher::TEvGetConfigRequest, Handle);
        }
    }

    void Handle(NConsole::TEvConfigsDispatcher::TEvGetConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvBlobStorage::TEvControllerConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvWhiteboard::TEvTabletStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvWhiteboard::TEvNodeStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvWhiteboard::TEvPDiskStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvWhiteboard::TEvVDiskStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvWhiteboard::TEvSystemStateRequest::TPtr &ev, const TActorContext &ctx);
};

struct TTestEnvOpts {
    ui32 NodeCount;
    ui32 VDisks;
    ui32 NToSelect;
    ui32 NRings;
    ui32 RingSize;
    ui32 DataCenterCount;
    TNodeTenantsMap Tenants;
    bool UseMirror3dcErasure;
    bool AdvanceCurrentTime;
    bool EnableSentinel;
    bool EnableCMSRequestPriorities;

    TTestEnvOpts() = default;

    TTestEnvOpts(ui32 nodeCount, 
            ui32 vdisks = 1,
            const TNodeTenantsMap &tenants = TNodeTenantsMap())
        : NodeCount(nodeCount)
        , VDisks(vdisks)
        , NToSelect(1)
        , NRings(1)
        , RingSize(nodeCount)
        , DataCenterCount(1)
        , Tenants(tenants)
        , UseMirror3dcErasure(false)
        , AdvanceCurrentTime(false)
        , EnableSentinel(false)
        , EnableCMSRequestPriorities(false)
    {
    }

    TTestEnvOpts& WithSentinel() {
        EnableSentinel = true;
        return *this;
    }

    TTestEnvOpts& WithoutSentinel() {
        EnableSentinel = false;
        return *this;
    }

    TTestEnvOpts& WithEnableCMSRequestPriorities() {
        EnableCMSRequestPriorities = true;
        return *this;
    }
};

class TCmsTestEnv : public TTestBasicRuntime {
public:
    TCmsTestEnv(ui32 nodeCount, ui32 vdisks = 1, const TNodeTenantsMap &tenants = {});
    TCmsTestEnv(ui32 nodeCount, const TNodeTenantsMap &tenants);
    TCmsTestEnv(const TTestEnvOpts &options);

    TActorId GetSender() { return Sender; }

    NCms::TPDiskID PDiskId(ui32 nodeIndex, ui32 pdiskIndex = 0);
    TString PDiskName(ui32 nodeIndex, ui32 pdiskIndex = 0);

    void RestartCms();
    void SendRestartCms();
    void SendToCms(IEventBase *event);
    void CreateDefaultCmsPipe();
    void DestroyDefaultCmsPipe();

    NKikimrCms::TCmsConfig GetCmsConfig();
    void SendCmsConfig(const NKikimrCms::TCmsConfig &config);
    void SetCmsConfig(const NKikimrCms::TCmsConfig &config);
    void SetLimits(ui32 tenantLimit, ui32 tenantRatioLimit, ui32 clusterLimit, ui32 clusterRatioLimit);

    void EnableSysNodeChecking(); 
    TIntrusiveConstPtr<NKikimr::TStateStorageInfo> GetStateStorageInfo();
    
    void UpdateNodeStartTime(ui32 nodeIndex, TInstant startTime) {
        TFakeNodeWhiteboardService::Info[GetNodeId(nodeIndex)].SystemStateInfo.SetStartTime(startTime.GetValue());
    }

    NKikimrCms::TClusterState RequestState(const NKikimrCms::TClusterStateRequest &request = {},
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);

    std::pair<TString, TVector<TString>> ExtractPermissions(const NKikimrCms::TPermissionResponse &response);

    template <typename... Ts>
    NKikimrCms::TPermissionResponse CheckPermissionRequest(
            const TString &user,
            bool partial,
            bool dry,
            bool schedule,
            bool defaultTenantPolicy,
            TDuration duration,
            NKikimrCms::EAvailabilityMode availabilityMode,
            i32 priority,
            NKikimrCms::TStatus::ECode code,
            Ts... actions)
    {
        auto req = MakePermissionRequest(user, partial, dry, schedule, actions...);
        if (!defaultTenantPolicy)
            req->Record.SetTenantPolicy(NKikimrCms::NONE);
        if (duration)
            req->Record.SetDuration(duration.GetValue());
        req->Record.SetAvailabilityMode(availabilityMode);
        if (priority)
            req->Record.SetPriority(priority);
        return CheckPermissionRequest(req, code);
    }

    template <typename... Ts>
    NKikimrCms::TPermissionResponse CheckPermissionRequest(
            const TString &user,
            bool partial,
            bool dry,
            bool schedule,
            bool defaultTenantPolicy,
            NKikimrCms::EAvailabilityMode availabilityMode,
            NKikimrCms::TStatus::ECode code,
            Ts... actions)
    {
        return CheckPermissionRequest(user, partial, dry, schedule,
                                      defaultTenantPolicy, TDuration::Zero(),
                                      availabilityMode, 0,
                                      code, actions...);
    }
    template <typename... Ts>
    NKikimrCms::TPermissionResponse CheckPermissionRequest(
            const TString &user,
            bool partial,
            bool dry,
            bool schedule,
            bool defaultTenantPolicy,
            TDuration duration,
            NKikimrCms::TStatus::ECode code,
            Ts... actions)
    {
        return CheckPermissionRequest(user, partial, dry, schedule, defaultTenantPolicy,
            duration, NKikimrCms::MODE_MAX_AVAILABILITY, 0, code, actions...);
    }

    template <typename... Ts>
    NKikimrCms::TPermissionResponse CheckPermissionRequest(
            const TString &user,
            bool partial,
            bool dry,
            bool schedule,
            bool defaultTenantPolicy,
            NKikimrCms::TStatus::ECode code,
            Ts... actions)
    {
        return CheckPermissionRequest(user, partial, dry, schedule, defaultTenantPolicy,
            NKikimrCms::MODE_MAX_AVAILABILITY, code, actions...);
    }

    template <typename... Ts>
    NKikimrCms::TPermissionResponse CheckPermissionRequest(
            const TString &user,
            bool partial,
            bool dry,
            bool schedule,
            bool defaultTenantPolicy,
            i32 priority,
            NKikimrCms::TStatus::ECode code,
            Ts... actions)
    {
        return CheckPermissionRequest(user, partial, dry, schedule, defaultTenantPolicy,
            TDuration::Zero(), NKikimrCms::MODE_MAX_AVAILABILITY, priority, code, actions...);
    }

    NKikimrCms::TPermissionResponse CheckPermissionRequest(TAutoPtr<NCms::TEvCms::TEvPermissionRequest> req,
        NKikimrCms::TStatus::ECode code);

    NKikimrCms::TManagePermissionResponse CheckManagePermissionRequest(
        const TString &user,
        NKikimrCms::TManagePermissionRequest::ECommand cmd,
        bool dry = false,
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);

    template <typename... Ts>
    NKikimrCms::TManagePermissionResponse CheckManagePermissionRequest(
            const TString &user,
            NKikimrCms::TManagePermissionRequest::ECommand cmd,
            bool dry,
            NKikimrCms::TStatus::ECode code,
            Ts... ids)
    {
        auto req = MakeManagePermissionRequest(user, cmd, dry, ids...);
        return CheckManagePermissionRequest(req, code);
    }

    template <typename... Ts>
    void CheckDonePermission(
            const TString &user,
            bool dry,
            NKikimrCms::TStatus::ECode code,
            Ts... ids)
    {
        CheckManagePermissionRequest(user, NKikimrCms::TManagePermissionRequest::DONE, dry, code, ids...);
    }

    NKikimrCms::TManagePermissionResponse CheckListPermissions(const TString &user, ui64 count);
    void CheckDonePermission(
        const TString &user,
        const TString &id,
        bool dry = false,
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);
    void CheckRejectPermission(
        const TString &user,
        const TString &id,
        bool dry = false,
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);
    NKikimrCms::TManagePermissionResponse CheckGetPermission(
        const TString &user,
        const TString &id,
        bool dry = false,
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);

    NKikimrCms::TManageRequestResponse CheckGetRequest(
        const TString &user,
        const TString &id,
        bool dry = false,
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);
    void CheckRejectRequest(
        const TString &user,
        const TString &id,
        bool dry = false,
        NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);
    NKikimrCms::TManageRequestResponse CheckListRequests(const TString &user, ui64 count);

    NKikimrCms::TPermissionResponse CheckRequest(
        const TString &user,
        TString id,
        bool dry,
        NKikimrCms::EAvailabilityMode availabilityMode,
        NKikimrCms::TStatus::ECode res,
        size_t count = 0);
    NKikimrCms::TPermissionResponse CheckRequest(
            const TString &user,
            TString id,
            bool dry,
            NKikimrCms::TStatus::ECode res,
            size_t count = 0)
    {
        return CheckRequest(user, id, dry, NKikimrCms::MODE_MAX_AVAILABILITY, res, count);
    }

    void CheckWalleStoreTaskIsFailed(NCms::TEvCms::TEvStoreWalleTask *req);

    template <typename... Ts>
    void CheckWalleCreateTask(
            const TString &id,
            const TString &action,
            bool dry,
            NKikimrCms::TStatus::ECode code,
            Ts... nodes)
    {
        auto req = MakeWalleCreateRequest(id, action, dry, nodes...);
        CheckWalleCreateTask(req, code);
    }

    template <typename... Ts>
    void CheckWalleListTasks(const TString &id, const TString &status, Ts... nodes) {
        auto task = MakeTaskInfo(id, status, nodes...);
        CheckWalleListTasks(task);
    }

    template <typename... Ts>
    void CheckWalleCheckTask(const TString &id, NKikimrCms::TStatus::ECode code, Ts... nodes) {
        auto task = MakeTaskInfo(id, "", nodes...);
        CheckWalleCheckTask(id, code, task);
    }

    void CheckWalleCheckTask(const TString &id, NKikimrCms::TStatus::ECode code);

    ui64 CountWalleTasks();

    void CheckWalleListTasks(size_t count);

    void CheckWalleRemoveTask(const TString &id, NKikimrCms::TStatus::ECode code = NKikimrCms::TStatus::OK);

    template <typename... Ts>
    TString CheckNotification(
            NKikimrCms::TStatus::ECode code,
            const TString &user,
            TInstant when,
            Ts... actions)
    {
        auto req = MakeNotification(user, when, actions...);
        return CheckNotification(req, code);
    }

    void CheckGetNotification(const TString &user, const TString &id, NKikimrCms::TStatus::ECode code);
    void CheckListNotifications(const TString &user, NKikimrCms::TStatus::ECode code, ui32 count);
    void CheckRejectNotification(const TString &user, const TString &id, NKikimrCms::TStatus::ECode code, bool dry = false);

    void WaitUpdatDiskStatus(ui32 statusEventsCount, NKikimrBlobStorage::EDriveStatus newStatus = NKikimrBlobStorage::ACTIVE);

    template <typename... Ts>
    void CheckSetMarker(
            NKikimrCms::EMarker marker,
            const TString &userToken,
            NKikimrCms::TStatus::ECode code,
            Ts ...args)
    {
        auto req = MakeSetMarkerRequest(marker, userToken, args...);
        return CheckSetMarker(req, code);
    }

    template <typename... Ts>
    void CheckResetMarker(
            NKikimrCms::EMarker marker,
            const TString &userToken,
            NKikimrCms::TStatus::ECode code,
            Ts ...args)
    {
        auto req = MakeResetMarkerRequest(marker, userToken, args...);
        return CheckResetMarker(req, code);
    }

    void EnableBSBaseConfig();
    void DisableBSBaseConfig();

    NKikimrCms::TGetLogTailResponse GetLogTail(
        ui32 type = 0,
        TInstant from = TInstant::Zero(),
        TInstant to = TInstant::Zero(),
        ui32 limit = 100,
        ui32 offset = 0);

    void AddBSCFailures(const NCms::TPDiskID &id, TVector<bool> &&failuresPattern);

    void EnableNoisyBSCPipe();

    const ui64 CmsId;

    void RegenerateBSConfig(NKikimrBlobStorage::TBaseConfig *config, const TTestEnvOpts &opts);

private:
    void SetupLogging();

    NKikimrCms::TManagePermissionResponse CheckManagePermissionRequest(
        TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest> req, NKikimrCms::TStatus::ECode code);
    NKikimrCms::TManageRequestResponse CheckManageRequestRequest(
        TAutoPtr<NCms::TEvCms::TEvManageRequestRequest> req, NKikimrCms::TStatus::ECode code);
    void CheckWalleCreateTask(TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> req, NKikimrCms::TStatus::ECode code);
    void CheckTasksEqual(const NKikimrCms::TWalleTaskInfo &l, const NKikimrCms::TWalleTaskInfo &r);
    void CheckWalleListTasks(const NKikimrCms::TWalleTaskInfo &task);
    void CheckWalleCheckTask(const TString &id, NKikimrCms::TStatus::ECode code, NKikimrCms::TWalleTaskInfo task);
    TString CheckNotification(TAutoPtr<NCms::TEvCms::TEvNotification> req, NKikimrCms::TStatus::ECode code);
    void CheckSetMarker(TAutoPtr<NCms::TEvCms::TEvSetMarkerRequest> req, NKikimrCms::TStatus::ECode code);
    void CheckResetMarker(TAutoPtr<NCms::TEvCms::TEvResetMarkerRequest> req, NKikimrCms::TStatus::ECode code);

    TActorId Sender;
    TActorId ClientId;
};

}
