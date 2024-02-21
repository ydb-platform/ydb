#pragma once

#include "audit_log.h"
#include "cms.h"
#include "config.h"
#include "logger.h"
#include "sentinel.h"
#include "services.h"
#include "walle.h"

#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/protos/counters_cms.pb.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>

#include <util/datetime/base.h>
#include <util/generic/queue.h>
#include <util/generic/stack.h>

namespace NKikimr::NCms {

using NConsole::TEvConsole;
using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

class TCms : public TActor<TCms>, public TTabletExecutedFlat {
public:
    struct TEvPrivate {
        enum EEv {
            EvClusterInfo = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvUpdateClusterInfo,
            EvCleanupExpired,
            EvCleanupWalle,
            EvLogAndSend,
            EvCleanupLog,
            EvStartCollecting,
            EvProcessQueue,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvClusterInfo : public TEventLocal<TEvClusterInfo, EvClusterInfo> {
            bool Success = true;
            TClusterInfoPtr Info;

            TString ToString() const override {
                return "TEvClusterInfo";
            }
        };

        struct TEvUpdateClusterInfo : public TEventLocal<TEvUpdateClusterInfo, EvUpdateClusterInfo> {};

        struct TEvStartCollecting : public TEventLocal<TEvStartCollecting, EvStartCollecting> {};

        struct TEvCleanupExpired : public TEventLocal<TEvCleanupExpired, EvCleanupExpired> {};

        struct TEvCleanupWalle : public TEventLocal<TEvCleanupWalle, EvCleanupWalle> {};

        struct TEvLogAndSend : public TEventLocal<TEvLogAndSend, EvLogAndSend> {
            NKikimrCms::TLogRecordData LogData;
            THolder<IEventHandle> Event;
        };

        struct TEvCleanupLog : public TEventLocal<TEvCleanupLog, EvCleanupLog> {};

        struct TEvProcessQueue : public TEventLocal<TEvProcessQueue, EvProcessQueue> {};
    };

    void PersistNodeTenants(TTransactionContext &txc, const TActorContext &ctx);

    using THostMarkers = TEvSentinel::TEvUpdateHostMarkers::THostMarkers;
    TVector<THostMarkers> SetHostMarker(const TString &host, NKikimrCms::EMarker marker, TTransactionContext &txc, const TActorContext &ctx);
    TVector<THostMarkers> ResetHostMarkers(const TString &host, TTransactionContext &txc, const TActorContext &ctx);
    void SentinelUpdateHostMarkers(TVector<THostMarkers> &&updateMarkers, const TActorContext &ctx);

    static void AddHostState(const TClusterInfoPtr &clusterInfo, const TNodeInfo &node, NKikimrCms::TClusterStateResponse &resp, TInstant timestamp);

private:
    using TActorBase = TActor<TCms>;
    using EStatusCode = NKikimrCms::TStatus::ECode;

    class TTxGetLogTail;
    class TTxInitScheme;
    class TTxLoadState;
    class TTxLogAndSend;
    class TTxLogCleanup;
    class TTxProcessNotification;
    class TTxRejectNotification;
    class TTxRemoveExpiredNotifications;
    class TTxRemoveRequest;
    class TTxRemovePermissions;
    template <typename TTable> class TTxRemoveTask;
    class TTxStorePermissions;
    class TTxStoreWalleTask;
    class TTxUpdateConfig;
    class TTxUpdateDowntimes;

    struct TActionOptions {
        TDuration PermissionDuration;
        NKikimrCms::ETenantPolicy TenantPolicy;
        NKikimrCms::EAvailabilityMode AvailabilityMode;
        bool PartialPermissionAllowed;

        TActionOptions(TDuration dur)
            : PermissionDuration(dur)
            , TenantPolicy(NKikimrCms::DEFAULT)
            , AvailabilityMode(NKikimrCms::MODE_MAX_AVAILABILITY)
            , PartialPermissionAllowed(false)
        {}
    };

    struct TRequestsQueueItem {
        TAutoPtr<IEventHandle> Request;
        TInstant ArrivedTime;

        TRequestsQueueItem(TAutoPtr<IEventHandle> req, TInstant arrivedTime)
            : Request(std::move(req))
            , ArrivedTime(arrivedTime)
        {}
    };

    ITransaction *CreateTxGetLogTail(TEvCms::TEvGetLogTailRequest::TPtr &ev);
    ITransaction *CreateTxInitScheme();
    ITransaction *CreateTxLoadState();
    ITransaction *CreateTxLogAndSend(TEvPrivate::TEvLogAndSend::TPtr &ev);
    ITransaction *CreateTxLogCleanup();
    ITransaction *CreateTxProcessNotification(TEvCms::TEvNotification::TPtr &ev);
    ITransaction *CreateTxRejectNotification(TEvCms::TEvManageNotificationRequest::TPtr &ev);
    ITransaction *CreateTxRemoveExpiredNotifications();
    ITransaction *CreateTxRemoveRequest(const TString &id, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp);
    ITransaction *CreateTxRemovePermissions(TVector<TString> ids, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp, bool expired = false);
    ITransaction *CreateTxRemoveWalleTask(const TString &id);
    ITransaction *CreateTxRemoveMaintenanceTask(const TString &id);
    ITransaction *CreateTxStorePermissions(THolder<IEventBase> req, TAutoPtr<IEventHandle> resp,
                                           const TString &owner, TAutoPtr<TRequestInfo> scheduled,
                                           const TMaybe<TString> &maintenanceTaskId = {});
    ITransaction *CreateTxStoreWalleTask(const TTaskInfo &task, THolder<IEventBase> req, TAutoPtr<IEventHandle> resp);
    ITransaction *CreateTxUpdateConfig(TEvCms::TEvSetConfigRequest::TPtr &ev);
    ITransaction *CreateTxUpdateConfig(TEvConsole::TEvConfigNotificationRequest::TPtr &ev);
    ITransaction *CreateTxUpdateDowntimes();

    static void AuditLog(const TActorContext &ctx, const TString &message) {
        NCms::AuditLog("CMS tablet", message, ctx);
    }

    static void AuditLog(const IEventBase *request, const IEventBase *response, const TActorContext &ctx) {
        return AuditLog(ctx, TStringBuilder() << "Reply"
            << ": request# " << request->ToString()
            << ", response# " << response->ToString());
    }

    static void Reply(const IEventBase *request, TAutoPtr<IEventHandle> response, const TActorContext &ctx) {
        AuditLog(request, response->GetBase(), ctx);
        ctx.Send(response);
    }

    template <typename TEvRequestPtr>
    static void Reply(TEvRequestPtr &request, THolder<IEventBase> response, const TActorContext &ctx) {
        AuditLog(request->Get(), response.Get(), ctx);
        ctx.Send(request->Sender, response.Release(), 0, request->Cookie);
    }

    template <typename TEvResponse, typename TEvRequestPtr>
    static void ReplyWithError(TEvRequestPtr &ev, EStatusCode code, const TString &reason, const TActorContext &ctx) {
        auto response = MakeHolder<TEvResponse>();
        response->Record.MutableStatus()->SetCode(code);
        response->Record.MutableStatus()->SetReason(reason);

        Reply<TEvRequestPtr>(ev, std::move(response), ctx);
    }

    STFUNC(StateInit) {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "StateInit event type: %" PRIx32 " event: %s",
                  ev->GetTypeRewrite(), ev->ToString().data());
        StateInitImpl(ev, SelfId());
    }

    template <typename TEvRequest, typename TEvResponse>
    void HandleNotSupported(typename TEvRequest::TPtr &ev, const TActorContext &ctx) {
        ReplyWithError<TEvResponse, typename TEvRequest::TPtr>(ev, NKikimrCms::TStatus::ERROR, NotSupportedReason, ctx);
    }

    STFUNC(StateNotSupported) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvCms::TEvClusterStateRequest, (HandleNotSupported<TEvCms::TEvClusterStateRequest,
                                                                      TEvCms::TEvClusterStateResponse>));
            HFunc(TEvCms::TEvPermissionRequest, (HandleNotSupported<TEvCms::TEvPermissionRequest,
                                                                    TEvCms::TEvPermissionResponse>));
            HFunc(TEvCms::TEvManageRequestRequest, (HandleNotSupported<TEvCms::TEvManageRequestRequest,
                                                                       TEvCms::TEvManageRequestResponse>));
            HFunc(TEvCms::TEvCheckRequest, (HandleNotSupported<TEvCms::TEvCheckRequest,
                                                               TEvCms::TEvPermissionResponse>));
            HFunc(TEvCms::TEvManagePermissionRequest, (HandleNotSupported<TEvCms::TEvManagePermissionRequest,
                                                                          TEvCms::TEvManagePermissionResponse>));
            HFunc(TEvCms::TEvConditionalPermissionRequest, (HandleNotSupported<TEvCms::TEvConditionalPermissionRequest,
                                                                               TEvCms::TEvPermissionResponse>));
            HFunc(TEvCms::TEvNotification, (HandleNotSupported<TEvCms::TEvNotification,
                                                               TEvCms::TEvNotificationResponse>));
            HFunc(TEvCms::TEvManageNotificationRequest, (HandleNotSupported<TEvCms::TEvManageNotificationRequest,
                                                                            TEvCms::TEvManageNotificationResponse>));
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "StateNotSupported unexpected event type: %" PRIx32 " event: %s",
                          ev->GetTypeRewrite(), ev->ToString().data());
            }
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvClusterInfo, Handle);
            HFunc(TEvPrivate::TEvLogAndSend, Handle);
            FFunc(TEvPrivate::EvUpdateClusterInfo, EnqueueRequest);
            CFunc(TEvPrivate::EvCleanupExpired, CleanupExpired);
            CFunc(TEvPrivate::EvCleanupLog, CleanupLog);
            CFunc(TEvPrivate::EvCleanupWalle, CleanupWalleTasks);
            cFunc(TEvPrivate::EvStartCollecting, StartCollecting);
            cFunc(TEvPrivate::EvProcessQueue, ProcessQueue);
            FFunc(TEvCms::EvClusterStateRequest, EnqueueRequest);
            HFunc(TEvCms::TEvPermissionRequest, CheckAndEnqueueRequest);
            HFunc(TEvCms::TEvManageRequestRequest, Handle);
            HFunc(TEvCms::TEvCheckRequest, CheckAndEnqueueRequest);
            HFunc(TEvCms::TEvManagePermissionRequest, Handle);
            HFunc(TEvCms::TEvConditionalPermissionRequest, CheckAndEnqueueRequest);
            HFunc(TEvCms::TEvNotification, CheckAndEnqueueRequest);
            HFunc(TEvCms::TEvManageNotificationRequest, Handle);
            HFunc(TEvCms::TEvWalleCreateTaskRequest, Handle);
            HFunc(TEvCms::TEvWalleListTasksRequest, Handle);
            HFunc(TEvCms::TEvWalleCheckTaskRequest, Handle);
            HFunc(TEvCms::TEvWalleRemoveTaskRequest, Handle);
            HFunc(TEvCms::TEvStoreWalleTask, Handle);
            HFunc(TEvCms::TEvRemoveWalleTask, Handle);
            // public api begin
            HFunc(TEvCms::TEvListClusterNodesRequest, Handle);
            HFunc(TEvCms::TEvCreateMaintenanceTaskRequest, Handle);
            HFunc(TEvCms::TEvRefreshMaintenanceTaskRequest, Handle);
            HFunc(TEvCms::TEvGetMaintenanceTaskRequest, Handle);
            HFunc(TEvCms::TEvListMaintenanceTasksRequest, Handle);
            HFunc(TEvCms::TEvDropMaintenanceTaskRequest, Handle);
            HFunc(TEvCms::TEvCompleteActionRequest, Handle);
            // public api end
            HFunc(TEvCms::TEvGetConfigRequest, Handle);
            HFunc(TEvCms::TEvSetConfigRequest, Handle);
            HFunc(TEvCms::TEvResetMarkerRequest, Handle);
            HFunc(TEvCms::TEvSetMarkerRequest, Handle);
            HFunc(TEvCms::TEvGetLogTailRequest, Handle);
            HFunc(TEvCms::TEvGetSentinelStateRequest, Handle);
            FFunc(TEvCms::EvGetClusterInfoRequest, EnqueueRequest);
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
            HFunc(TEvConsole::TEvReplaceConfigSubscriptionsResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            IgnoreFunc(TEvTabletPipe::TEvServerDisconnected);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse);
            IgnoreFunc(NConsole::TEvConfigsDispatcher::TEvRemoveConfigSubscriptionResponse);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_DEBUG(*TlsActivationContext, NKikimrServices::CMS, "StateWork unexpected event type: %" PRIx32 " event: %s",
                          ev->GetTypeRewrite(), ev->ToString().data());
            }
        }
    }

    void DefaultSignalTabletActive(const TActorContext &ctx) override;
    void OnActivateExecutor(const TActorContext &ctx) override;
    void OnDetach(const TActorContext &ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override;

    void Enqueue(TAutoPtr<IEventHandle> &ev) override;
    void ProcessInitQueue(const TActorContext &ctx);

    void SubscribeForConfig(const TActorContext &ctx);
    void AdjustInfo(TClusterInfoPtr &info, const TActorContext &ctx) const;
    bool CheckPermissionRequest(const NKikimrCms::TPermissionRequest &request,
        NKikimrCms::TPermissionResponse &response,
        NKikimrCms::TPermissionRequest &scheduled,
        const TActorContext &ctx);
    bool IsActionHostValid(const NKikimrCms::TAction &action, TErrorInfo &error) const;
    bool ParseServices(const NKikimrCms::TAction &action, TServices &services, TErrorInfo &error) const;

    bool CheckAccess(const TString &token,
        NKikimrCms::TStatus::ECode &code,
        TString &error,
        const TActorContext &ctx);
    bool CheckEvictVDisks(const NKikimrCms::TAction &action,
        TErrorInfo &error) const;
    bool CheckAction(const NKikimrCms::TAction &action,
        const TActionOptions &opts,
        TErrorInfo &error,
        const TActorContext &ctx) const;
    bool CheckActionShutdownNode(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        const TNodeInfo &node,
        TErrorInfo &error,
        const TActorContext &ctx) const;
    bool CheckActionRestartServices(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        TErrorInfo &error,
        const TActorContext &ctx) const;
    bool CheckActionShutdownHost(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        TErrorInfo &error,
        const TActorContext &ctx) const;
    bool CheckActionReplaceDevices(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        TErrorInfo &error) const;
    bool CheckSysTabletsNode(const TActionOptions &opts,
        const TNodeInfo &node,
        TErrorInfo &error) const;
    bool TryToLockNode(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        const TNodeInfo &node,
        TErrorInfo &error) const;
    bool TryToLockPDisk(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        const TPDiskInfo &pdisk,
        TErrorInfo &error) const;
    bool TryToLockVDisks(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        const TSet<TVDiskID> &vdisks,
        TErrorInfo &error) const;
    bool TryToLockVDisk(const TActionOptions &options,
        const TVDiskInfo &vdisk,
        TDuration duration,
        TErrorInfo &error) const;
    bool TryToLockStateStorageReplica(const NKikimrCms::TAction &action,
        const TActionOptions &options,
        const TNodeInfo &node,
        TErrorInfo &error,
        const TActorContext &ctx) const;
    void AcceptPermissions(NKikimrCms::TPermissionResponse &resp, const TString &requestId,
        const TString &owner, const TActorContext &ctx, bool check = false);
    void ScheduleUpdateClusterInfo(const TActorContext &ctx, bool now = false);
    void ScheduleCleanup(TInstant time, const TActorContext &ctx);
    void SchedulePermissionsCleanup(const TActorContext &ctx);
    void ScheduleNotificationsCleanup(const TActorContext &ctx);
    void CleanupExpired(const TActorContext &ctx);
    void CleanupLog(const TActorContext &ctx);
    void ScheduleLogCleanup(const TActorContext &ctx);
    void DoPermissionsCleanup(const TActorContext &ctx);
    void CleanupWalleTasks(const TActorContext &ctx);
    TVector<TString> FindEmptyTasks(const THashMap<TString, TTaskInfo> &tasks, const TActorContext &ctx);
    void RemoveEmptyTasks(const TActorContext &ctx);
    void StartCollecting();
    bool CheckNotificationDeadline(const NKikimrCms::TAction &action, TInstant time,
        TErrorInfo &error, const TActorContext &ctx) const;
    bool CheckNotificationRestartServices(const NKikimrCms::TAction &action, TInstant time,
        TErrorInfo &error, const TActorContext &ctx) const;
    bool CheckNotificationShutdownHost(const NKikimrCms::TAction &action, TInstant time,
        TErrorInfo &error, const TActorContext &ctx) const;
    bool CheckNotificationReplaceDevices(const NKikimrCms::TAction &action, TInstant time,
        TErrorInfo &error, const TActorContext &ctx) const;
    bool IsValidNotificationAction(const NKikimrCms::TAction &action, TInstant time,
        TErrorInfo &error, const TActorContext &ctx) const;
    TString AcceptNotification(const NKikimrCms::TNotification &notification, const TActorContext &ctx);
    bool CheckNotification(const NKikimrCms::TNotification &notification,
        NKikimrCms::TNotificationResponse &resp,
        const TActorContext &ctx) const;

    void Cleanup(const TActorContext &ctx);
    void Die(const TActorContext &ctx) override;

    void GetPermission(TEvCms::TEvManagePermissionRequest::TPtr &ev, bool all, const TActorContext &ctx);
    void RemovePermission(TEvCms::TEvManagePermissionRequest::TPtr &ev, bool done, const TActorContext &ctx);
    void GetRequest(TEvCms::TEvManageRequestRequest::TPtr &ev, bool all, const TActorContext &ctx);
    void RemoveRequest(TEvCms::TEvManageRequestRequest::TPtr &ev, const TActorContext &ctx);
    void GetNotifications(TEvCms::TEvManageNotificationRequest::TPtr &ev, bool all, const TActorContext &ctx);
    bool RemoveNotification(const TString &id, const TString &user, bool remove, TErrorInfo &error);

    void EnqueueRequest(TAutoPtr<IEventHandle> ev, const TActorContext &ctx);
    void CheckAndEnqueueRequest(TEvCms::TEvPermissionRequest::TPtr &ev, const TActorContext &ctx);
    void CheckAndEnqueueRequest(TEvCms::TEvCheckRequest::TPtr &ev, const TActorContext &ctx);
    void CheckAndEnqueueRequest(TEvCms::TEvConditionalPermissionRequest::TPtr &ev, const TActorContext &ctx);
    void CheckAndEnqueueRequest(TEvCms::TEvNotification::TPtr &ev, const TActorContext &ctx);
    void ProcessQueue();
    void ProcessRequest(TAutoPtr<IEventHandle> &ev);

    void AddPermissionExtensions(const NKikimrCms::TAction &action, NKikimrCms::TPermission &perm) const;
    void AddHostExtensions(const TString &host, NKikimrCms::TPermission &perm) const;

    void OnBSCPipeDestroyed(const TActorContext &ctx);

    void Handle(TEvPrivate::TEvClusterInfo::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvLogAndSend::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvUpdateClusterInfo::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvManageRequestRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvManagePermissionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvClusterStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvPermissionRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvCheckRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvNotification::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvManageNotificationRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvWalleCreateTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvWalleListTasksRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvWalleCheckTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvWalleRemoveTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvStoreWalleTask::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvRemoveWalleTask::TPtr &ev, const TActorContext &ctx);
    // public api begin
    void Handle(TEvCms::TEvListClusterNodesRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvCreateMaintenanceTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvRefreshMaintenanceTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvGetMaintenanceTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvListMaintenanceTasksRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvDropMaintenanceTaskRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvCompleteActionRequest::TPtr &ev, const TActorContext &ctx);
    // public api end
    void Handle(TEvCms::TEvGetConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvSetConfigRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvResetMarkerRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvSetMarkerRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvGetLogTailRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvGetSentinelStateRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvCms::TEvGetClusterInfoRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx);

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;

private:
    TStack<TInstant> ScheduledCleanups;
    TString NotSupportedReason;
    TQueue<TAutoPtr<IEventHandle>> InitQueue;

    TQueue<TRequestsQueueItem> Queue;
    TQueue<TRequestsQueueItem> NextQueue;

    TCmsStatePtr State;
    TLogger Logger;
    // Shortcut to State->ClusterInfo.
    TClusterInfoPtr ClusterInfo;
    ui64 ConfigSubscriptionId;
    TSchedulerCookieHolder WalleCleanupTimerCookieHolder;
    TSchedulerCookieHolder LogCleanupTimerCookieHolder;

    // State Storage
    TIntrusiveConstPtr<TStateStorageInfo> StateStorageInfo;
    THashMap<ui32, ui32> NodeToRing;
    THashSet<ui32> StateStorageNodes;

    // Monitoring
    THolder<class NKikimr::TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase *TabletCounters;

    TInstant InfoCollectorStartTime;

    bool EnableCMSRequestPriorities = false;

private:
    TString GenerateStat();
    void GenerateNodeState(IOutputStream&);

public:
    TCms(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , State(new TCmsState)
        , Logger(State)
        , ConfigSubscriptionId(0)
    {
        TabletCountersPtr.Reset(new TProtobufTabletCounters<
            ESimpleCounters_descriptor,
            ECumulativeCounters_descriptor,
            EPercentileCounters_descriptor,
            ETxTypes_descriptor>());
        TabletCounters = TabletCountersPtr.Get();
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_SERVICE;
    }
};

} // namespace NKikimr::NCms
