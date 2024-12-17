#pragma once

#include "cms.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>
#include <ydb/library/aclib/aclib.h>

namespace NKikimr::NCmsTest {

inline void AddServices(NKikimrCms::TAction &action, const TString &service) {
    *action.AddServices() = service;
}

template <typename... Ts>
void AddServices(NKikimrCms::TAction &action, const TString &service, Ts... services) {
    AddServices(action, service);
    AddServices(action, services...);
}

inline void AddDevices(NKikimrCms::TAction &action, const TString &device) {
    *action.AddDevices() = device;
}

template <typename... Ts>
void AddDevices(NKikimrCms::TAction &action, const TString &device, Ts... devices) {
    AddDevices(action, device);
    AddDevices(action, devices...);
}

inline NKikimrCms::TAction MakeAction(NKikimrCms::TAction::EType type, const TString &host, ui64 duration) {
    NKikimrCms::TAction action;
    action.SetType(type);
    action.SetHost(host);
    action.SetDuration(duration);
    return action;
}

inline NKikimrCms::TAction MakeAction(NKikimrCms::TAction::EType type, ui32 nodeId, ui64 duration) {
    return MakeAction(type, ToString(nodeId), duration);
}

template <typename... Ts>
NKikimrCms::TAction MakeAction(NKikimrCms::TAction::EType type, const TString &host, ui64 duration, Ts... items) {
    NKikimrCms::TAction action = MakeAction(type, host, duration);
    if (type == NKikimrCms::TAction::START_SERVICES
        || type == NKikimrCms::TAction::RESTART_SERVICES
        || type == NKikimrCms::TAction::STOP_SERVICES)
        AddServices(action, items...);
    else
        AddDevices(action, items...);
    return action;
}

template <typename... Ts>
NKikimrCms::TAction MakeAction(NKikimrCms::TAction::EType type, ui32 nodeId, ui64 duration, Ts... items) {
    return MakeAction(type, ToString(nodeId), duration, items...);
}

inline void AddActions(TAutoPtr<NCms::TEvCms::TEvPermissionRequest> &event, const NKikimrCms::TAction &action) {
    event->Record.AddActions()->CopyFrom(action);
}

template <typename... Ts>
void AddActions(TAutoPtr<NCms::TEvCms::TEvPermissionRequest> &event, const NKikimrCms::TAction &action, Ts... actions) {
    AddActions(event, action);
    AddActions(event, actions...);
}

struct TRequestOptions {
    TString User;
    bool Partial;
    bool DryRun;
    bool Schedule;
    bool EvictVDisks;

    explicit TRequestOptions(const TString &user, bool partial, bool dry, bool schedule)
        : User(user)
        , Partial(partial)
        , DryRun(dry)
        , Schedule(schedule)
        , EvictVDisks(false)
    {}

    explicit TRequestOptions(const TString &user)
        : TRequestOptions(user, false, false, false)
    {}

    TRequestOptions& WithEvictVDisks() {
        EvictVDisks = true;
        return *this;
    }
};

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvPermissionRequest> MakePermissionRequest(const TRequestOptions &opts, Ts... actions) {
    TAutoPtr<NCms::TEvCms::TEvPermissionRequest> event = new NCms::TEvCms::TEvPermissionRequest;
    event->Record.SetUser(opts.User);
    event->Record.SetPartialPermissionAllowed(opts.Partial);
    event->Record.SetDryRun(opts.DryRun);
    event->Record.SetSchedule(opts.Schedule);
    event->Record.SetEvictVDisks(opts.EvictVDisks);
    AddActions(event, actions...);

    return event;
}

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvPermissionRequest> MakePermissionRequest(const TString &user, bool partial, bool dry, bool schedule, Ts... actions) {
    return MakePermissionRequest(TRequestOptions(user, partial, dry, schedule), actions...);
}

inline void AddPermissions(TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest> &ev, const TString &id) {
    *ev->Record.AddPermissions() = id;
}

template <typename... Ts>
void AddPermissions(TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest> &ev, const TString &id, Ts... ids) {
    AddPermissions(ev, id);
    AddPermissions(ev, ids...);
}

inline TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest>
MakeManagePermissionRequest(const TString &user, NKikimrCms::TManagePermissionRequest::ECommand cmd, bool dry) {
    TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest> event = new NCms::TEvCms::TEvManagePermissionRequest;
    event->Record.SetUser(user);
    event->Record.SetCommand(cmd);
    event->Record.SetDryRun(dry);
    return event;
}

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvManagePermissionRequest> MakeManagePermissionRequest(
        const TString &user,
        NKikimrCms::TManagePermissionRequest::ECommand cmd,
        bool dry,
        Ts... ids)
{
    auto event = MakeManagePermissionRequest(user, cmd, dry);
    AddPermissions(event, ids...);
    return event;
}

inline TAutoPtr<NCms::TEvCms::TEvManageRequestRequest> MakeManageRequestRequest(
        const TString &user,
        NKikimrCms::TManageRequestRequest::ECommand cmd,
        bool dry)
{
    TAutoPtr<NCms::TEvCms::TEvManageRequestRequest> event = new NCms::TEvCms::TEvManageRequestRequest;
    event->Record.SetUser(user);
    event->Record.SetCommand(cmd);
    event->Record.SetDryRun(dry);
    return event;
}

inline TAutoPtr<NCms::TEvCms::TEvManageRequestRequest> MakeManageRequestRequest(
        const TString &user,
        NKikimrCms::TManageRequestRequest::ECommand cmd,
        const TString &id,
        bool dry)
{
    TAutoPtr<NCms::TEvCms::TEvManageRequestRequest> event = new NCms::TEvCms::TEvManageRequestRequest;
    event->Record.SetUser(user);
    event->Record.SetCommand(cmd);
    event->Record.SetRequestId(id);
    event->Record.SetDryRun(dry);
    return event;
}

inline TAutoPtr<NCms::TEvCms::TEvCheckRequest> MakeCheckRequest(
        const TString &user,
        const TString &id,
        bool dry,
        NKikimrCms::EAvailabilityMode availabilityMode = NKikimrCms::MODE_MAX_AVAILABILITY)
{
    TAutoPtr<NCms::TEvCms::TEvCheckRequest> event = new NCms::TEvCms::TEvCheckRequest;
    event->Record.SetUser(user);
    event->Record.SetRequestId(id);
    event->Record.SetDryRun(dry);
    event->Record.SetAvailabilityMode(availabilityMode);
    return event;
}

inline void AddHosts(TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> &ev, ui32 node) {
    *ev->Record.AddHosts() = ToString(node);
}

template <typename... Ts>
void AddHosts(TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> &ev, ui32 node, Ts... nodes) {
    AddHosts(ev, node);
    AddHosts(ev, nodes...);
}

inline void AddHosts(TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> &ev, const TString &host) {
    *ev->Record.AddHosts() = host;
}

template <typename... Ts>
void AddHosts(TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> &ev, const TString &host, Ts... hosts) {
    AddHosts(ev, host);
    AddHosts(ev, hosts...);
}

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> MakeWalleCreateRequest(
        const TString &id,
        const TString &action,
        bool dry,
        Ts... hosts)
{
    TAutoPtr<NCms::TEvCms::TEvWalleCreateTaskRequest> res = new NCms::TEvCms::TEvWalleCreateTaskRequest;
    res->Record.SetTaskId(id);
    res->Record.SetType("automated");
    res->Record.SetIssuer("UT");
    res->Record.SetAction(action);
    res->Record.SetDryRun(dry);
    AddHosts(res, hosts...);
    return res;
}

template <typename T>
void AddHosts(NKikimrCms::TWalleTaskInfo &task, T node) {
    *task.AddHosts() = ToString(node);
}

template <typename T, typename... Ts>
void AddHosts(NKikimrCms::TWalleTaskInfo &task, T node, Ts... nodes) {
    AddHosts(task, node);
    AddHosts(task, nodes...);
}

template <typename... Ts>
NKikimrCms::TWalleTaskInfo MakeTaskInfo(const TString &id, const TString &status, Ts... nodes) {
    NKikimrCms::TWalleTaskInfo task;
    task.SetTaskId(id);
    task.SetStatus(status);
    AddHosts(task, nodes...);
    return task;
}

inline void AddActions(TAutoPtr<NCms::TEvCms::TEvNotification> &event, const NKikimrCms::TAction &action) {
    event->Record.AddActions()->CopyFrom(action);
}

template <typename... Ts>
void AddActions(TAutoPtr<NCms::TEvCms::TEvNotification> &event, const NKikimrCms::TAction &action, Ts... actions) {
    AddActions(event, action);
    AddActions(event, actions...);
}

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvNotification> MakeNotification(const TString &user, TInstant when, Ts... actions) {
    TAutoPtr<NCms::TEvCms::TEvNotification> event = new NCms::TEvCms::TEvNotification;
    if (user)
        event->Record.SetUser(user);
    event->Record.SetTime(when.GetValue());
    AddActions(event, actions...);
    return event;
}

inline void AddItems(NKikimrCms::TItems &) {
}

inline void AddItem(NKikimrCms::TItems &items, const TString &host) {
    items.AddHosts(host);
}

inline void AddItem(NKikimrCms::TItems &items, ui32 nodeId) {
    items.AddNodes(nodeId);
}

inline void AddItem(NKikimrCms::TItems &items, const NCms::TPDiskID &pdisk) {
    auto &rec = *items.AddPDisks();
    rec.SetNodeId(pdisk.NodeId);
    rec.SetDiskId(pdisk.DiskId);
}

inline void AddItem(NKikimrCms::TItems &items, const TVDiskID &vdisk) {
    VDiskIDFromVDiskID(vdisk, items.AddVDisks());
}

template <typename T, typename... Ts>
void AddItems(NKikimrCms::TItems &items, T item, Ts... args) {
    AddItem(items, item);
    AddItems(items, args...);
}

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvSetMarkerRequest> MakeSetMarkerRequest(NKikimrCms::EMarker marker, const TString &token, Ts... args) {
    TAutoPtr<NCms::TEvCms::TEvSetMarkerRequest> event = new NCms::TEvCms::TEvSetMarkerRequest;
    event->Record.SetMarker(marker);
    AddItems(*event->Record.MutableItems(), args...);
    if (token) {
        NACLib::TUserToken t(token, {});
        event->Record.SetUserToken(t.SerializeAsString());
    }
    return event;
}

template <typename... Ts>
TAutoPtr<NCms::TEvCms::TEvResetMarkerRequest> MakeResetMarkerRequest(NKikimrCms::EMarker marker, const TString &token, Ts... args) {
    TAutoPtr<NCms::TEvCms::TEvResetMarkerRequest> event = new NCms::TEvCms::TEvResetMarkerRequest;
    event->Record.SetMarker(marker);
    AddItems(*event->Record.MutableItems(), args...);
    if (token) {
        NACLib::TUserToken t(token, {});
        event->Record.SetUserToken(t.SerializeAsString());
    }
    return event;
}

struct TIsUpdateStatusConfigRequest {
public:
    TIsUpdateStatusConfigRequest(NKikimrBlobStorage::EDriveStatus status)
        : Status(status)
    {
    }

    bool operator()(IEventHandle &ev) {
        if (ev.GetTypeRewrite() == TEvBlobStorage::EvControllerConfigRequest) {
            auto &rec = ev.Get<TEvBlobStorage::TEvControllerConfigRequest>()->Record;
            if (rec.GetRequest().CommandSize()
                && rec.GetRequest().GetCommand(0).HasUpdateDriveStatus()
                && rec.GetRequest().GetCommand(0).GetUpdateDriveStatus().GetStatus() == Status)
                return true;
        }
        return false;
    }

private:
    NKikimrBlobStorage::EDriveStatus Status;
};

inline NKikimrWhiteboard::TSystemStateInfo MakeSystemStateInfo(const TString &version, const TVector<TString>& roles = {}) {
    NKikimrWhiteboard::TSystemStateInfo result;

    result.SetVersion(version);
    for (const auto &role : roles) {
        result.AddRoles(role);
    }

    return result;
}

inline void AddActionsToGroup(
        Ydb::Maintenance::ActionGroup &group,
        const Ydb::Maintenance::Action &action) 
{
    group.add_actions()->CopyFrom(action);
}

template <typename... Ts>
void AddActionsToGroup(
        Ydb::Maintenance::ActionGroup &group,
        const Ydb::Maintenance::Action &action, Ts... actions) 
{
    AddActionsToGroup(group, action);
    AddActionsToGroup(group, actions...);
}

template <typename... Ts>
Ydb::Maintenance::ActionGroup MakeActionGroup(Ts... actions) 
{
    Ydb::Maintenance::ActionGroup group;
    AddActionsToGroup(group, actions...);
    return group;
}

inline Ydb::Maintenance::ActionGroup MakeActionGroup(const Ydb::Maintenance::Action &action) 
{
    Ydb::Maintenance::ActionGroup group;
    AddActionsToGroup(group, action);
    return group;
}

inline Ydb::Maintenance::Action MakeLockAction(ui32 nodeId, TDuration duration) {
    Ydb::Maintenance::Action action;
    auto *lockAction = action.mutable_lock_action();

    lockAction->mutable_scope()->set_node_id(nodeId);
    lockAction->mutable_duration()->set_seconds(static_cast<i64>(duration.Seconds()));

    return action;
}

inline void AddActionGroups(
        Ydb::Maintenance::CreateMaintenanceTaskRequest &req,
        const Ydb::Maintenance::ActionGroup &actionGroup) 
{
    req.add_action_groups()->CopyFrom(actionGroup);
}

template <typename... Ts>
void AddActionGroups(
        Ydb::Maintenance::CreateMaintenanceTaskRequest &req,
        const Ydb::Maintenance::ActionGroup &actionGroup, Ts... actionGroups) 
{
    AddActionGroups(req, actionGroup);
    AddActionGroups(req, actionGroups...);
}

}
