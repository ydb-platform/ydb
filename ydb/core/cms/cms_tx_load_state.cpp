#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/system/hostname.h>

namespace NKikimr::NCms {

namespace {

template<typename T>
bool ParseFromStringSafe(const TString& input, T* output) {
    google::protobuf::TextFormat::Parser parser;
    parser.AllowUnknownField(true);
    return parser.ParseFromString(input, output);
}

} // anonymous namespace

class TCms::TTxLoadState : public TTransactionBase<TCms> {
public:
    TTxLoadState(TCms *self)
        : TBase(self)
    {
    }

     TTxType GetTxType() const override { return TXTYPE_LOAD_STATE; } 

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxLoadState Execute");

        auto state = Self->State;
        NIceDb::TNiceDb db(txc.DB);

        if (!db.Precharge<Schema>())
            return false;

        auto paramRow = db.Table<Schema::Param>().Key(1).Select<Schema::Param::TColumns>();
        auto permissionRowset = db.Table<Schema::Permission>().Range().Select<Schema::Permission::TColumns>();
        auto requestRowset = db.Table<Schema::Request>().Range().Select<Schema::Request::TColumns>();
        auto walleTaskRowset = db.Table<Schema::WalleTask>().Range().Select<Schema::WalleTask::TColumns>();
        auto maintenanceTasksRowset = db.Table<Schema::MaintenanceTasks>().Range().Select<Schema::MaintenanceTasks::TColumns>();
        auto notificationRowset = db.Table<Schema::Notification>().Range().Select<Schema::Notification::TColumns>();
        auto nodeTenantRowset = db.Table<Schema::NodeTenant>().Range().Select<Schema::NodeTenant::TColumns>();
        auto hostMarkersRowset = db.Table<Schema::HostMarkers>().Range().Select<Schema::HostMarkers::TColumns>();
        auto logRowset = db.Table<Schema::LogRecords>().Range().Select<Schema::LogRecords::Timestamp>();

        if (!paramRow.IsReady()
            || !permissionRowset.IsReady()
            || !requestRowset.IsReady()
            || !walleTaskRowset.IsReady()
            || !maintenanceTasksRowset.IsReady()
            || !notificationRowset.IsReady()
            || !hostMarkersRowset.IsReady()
            || !logRowset.IsReady())
            return false;

        NKikimrCms::TCmsConfig config;
        if (paramRow.IsValid()) {
            state->NextPermissionId = paramRow.GetValueOrDefault<Schema::Param::NextPermissionID>(1);
            state->NextRequestId = paramRow.GetValueOrDefault<Schema::Param::NextRequestID>(1);
            state->NextNotificationId = paramRow.GetValueOrDefault<Schema::Param::NextNotificationID>(1);
            config = paramRow.GetValueOrDefault<Schema::Param::Config>(NKikimrCms::TCmsConfig());

            LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                        "Loaded config: " << config.ShortDebugString());
        } else {
            state->NextPermissionId = 1;
            state->NextRequestId = 1;
            state->NextNotificationId = 1;

            LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                        "Using default config");
        }
        state->ConfigProto = config;
        state->Config.Deserialize(config);

        if (!logRowset.EndOfSet())
            state->LastLogRecordTimestamp
                = Max<ui64>() - logRowset.GetValue<Schema::LogRecords::Timestamp>();

        state->WalleTasks.clear();
        state->WalleRequests.clear();
        state->MaintenanceTasks.clear();
        state->MaintenanceRequests.clear();
        state->Permissions.clear();
        state->ScheduledRequests.clear();
        state->Notifications.clear();

        while (!requestRowset.EndOfSet()) {
            TString id = requestRowset.GetValue<Schema::Request::ID>();
            TString owner = requestRowset.GetValue<Schema::Request::Owner>();
            ui64 order = requestRowset.GetValue<Schema::Request::Order>();
            i32 priority = requestRowset.GetValueOrDefault<Schema::Request::Priority>();
            TString requestStr = requestRowset.GetValue<Schema::Request::Content>();

            TRequestInfo request;
            request.RequestId = id;
            request.Owner = owner;
            request.Order = order;
            request.Priority = priority;
            ParseFromStringSafe(requestStr, &request.Request);

            LOG_DEBUG(ctx, NKikimrServices::CMS, "Loaded request %s owned by %s: %s",
                      id.data(), owner.data(), requestStr.data());

            state->ScheduledRequests.emplace(id, request);

            if (!requestRowset.Next())
                return false;
        }

        while (!walleTaskRowset.EndOfSet()) {
            TString taskId = walleTaskRowset.GetValue<Schema::WalleTask::TaskID>();
            TString requestId = walleTaskRowset.GetValue<Schema::WalleTask::RequestID>();

            TTaskInfo task;
            task.TaskId = taskId;
            task.RequestId = requestId;
            state->WalleRequests.emplace(requestId, taskId);
            state->WalleTasks.emplace(taskId, task);

            LOG_DEBUG(ctx, NKikimrServices::CMS, "Loaded Wall-E task %s mapped to request %s",
                      taskId.data(), requestId.data());

            if (!walleTaskRowset.Next())
                return false;
        }

        while (!maintenanceTasksRowset.EndOfSet()) {
            TString taskId = maintenanceTasksRowset.GetValue<Schema::MaintenanceTasks::TaskID>();
            TString requestId = maintenanceTasksRowset.GetValue<Schema::MaintenanceTasks::RequestID>();
            TString owner = maintenanceTasksRowset.GetValue<Schema::MaintenanceTasks::Owner>();

            state->MaintenanceRequests.emplace(requestId, taskId);
            state->MaintenanceTasks.emplace(taskId, TTaskInfo{
                .TaskId = taskId,
                .RequestId = requestId,
                .Owner = owner,
            });

            LOG_DEBUG(ctx, NKikimrServices::CMS, "Loaded maintenance task %s mapped to request %s",
                      taskId.data(), requestId.data());

            if (!maintenanceTasksRowset.Next())
                return false;
        }

        while (!permissionRowset.EndOfSet()) {
            TString id = permissionRowset.GetValue<Schema::Permission::ID>();
            TString requestId = permissionRowset.GetValue<Schema::Permission::RequestID>();
            TString owner = permissionRowset.GetValue<Schema::Permission::Owner>();
            TString actionStr = permissionRowset.GetValue<Schema::Permission::Action>();
            ui64 deadline = permissionRowset.GetValue<Schema::Permission::Deadline>();

            TPermissionInfo permission;
            permission.PermissionId = id;
            permission.RequestId = requestId;
            permission.Owner = owner;
            ParseFromStringSafe(actionStr, &permission.Action);
            permission.Deadline = TInstant::MicroSeconds(deadline);

            LOG_DEBUG(ctx, NKikimrServices::CMS, "Loaded permission %s owned by %s valid until %s: %s",
                      id.data(), owner.data(), TInstant::MicroSeconds(deadline).ToStringLocalUpToSeconds().data(), actionStr.data());

            state->Permissions.emplace(id, permission);

            if (state->WalleRequests.contains(requestId)) {
                const auto &taskId = state->WalleRequests[requestId];
                state->WalleTasks[taskId].Permissions.insert(id);

                LOG_DEBUG(ctx, NKikimrServices::CMS, "Added permission %s to Wall-E task %s",
                          id.data(), taskId.data());
            }

            if (state->MaintenanceRequests.contains(requestId)) {
                const auto &taskId = state->MaintenanceRequests[requestId];
                state->MaintenanceTasks[taskId].Permissions.insert(id);

                LOG_DEBUG(ctx, NKikimrServices::CMS, "Added permission %s to maintenance task %s",
                          id.data(), taskId.data());
            }

            if (!permissionRowset.Next())
                return false;
        }

        while (!notificationRowset.EndOfSet()) {
            TString id = notificationRowset.GetValue<Schema::Notification::ID>();
            TString owner = notificationRowset.GetValue<Schema::Notification::Owner>();
            TString notificationStr = notificationRowset.GetValue<Schema::Notification::NotificationProto>();

            TNotificationInfo notification;
            notification.NotificationId = id;
            notification.Owner = owner;
            ParseFromStringSafe(notificationStr, &notification.Notification);

            LOG_DEBUG(ctx, NKikimrServices::CMS, "Loaded notification %s owned by %s: %s",
                      id.data(), owner.data(), notificationStr.data());

            state->Notifications.emplace(id, notification);

            if (!notificationRowset.Next())
                return false;
        }

        while (!nodeTenantRowset.EndOfSet()) {
            ui32 nodeId = nodeTenantRowset.GetValue<Schema::NodeTenant::NodeId>();
            TString tenant = nodeTenantRowset.GetValue<Schema::NodeTenant::Tenant>();

            LOG_DEBUG(ctx, NKikimrServices::CMS,
                      "Loaded node tenant '%s' for node %" PRIu32,
                      tenant.data(), nodeId);
            state->InitialNodeTenants[nodeId] = tenant;

            if (!nodeTenantRowset.Next())
                return false;
        }

        while (!hostMarkersRowset.EndOfSet()) {
            TString host = hostMarkersRowset.GetValue<Schema::HostMarkers::Host>();
            TVector<NKikimrCms::EMarker> markers = hostMarkersRowset.GetValue<Schema::HostMarkers::Markers>();

            state->HostMarkers[host].insert(markers.begin(), markers.end());

            if (!hostMarkersRowset.Next())
                return false;
        }

        if (!state->Downtimes.DbLoadState(txc, ctx))
            return false;

        Self->CleanupWalleTasks(ctx);

        NKikimrCms::TLogRecordData data;
        data.SetRecordType(NKikimrCms::TLogRecordData::CMS_LOADED);
        auto &rec = *data.MutableCmsLoaded();
        rec.SetHost(FQDNHostName());
        rec.SetNodeId(Self->SelfId().NodeId());
        rec.SetVersion(Sprintf("%s:%s", GetArcadiaSourceUrl(), GetArcadiaLastChange()));
        Self->Logger.DbLogData(data, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxLoadState Complete");
        Self->Become(&TCms::StateWork);
        Self->SignalTabletActive(ctx);
        Self->SchedulePermissionsCleanup(ctx);
        Self->ScheduleNotificationsCleanup(ctx);
        Self->ScheduleLogCleanup(ctx);
        Self->ScheduleUpdateClusterInfo(ctx, true);
        Self->ProcessInitQueue(ctx);
    }
};

ITransaction *TCms::CreateTxLoadState() {
    return new TTxLoadState(this);
}

} // namespace NKikimr::NCms
