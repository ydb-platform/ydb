#include "cms_impl.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>
#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/time_util.h>

#include <util/string/cast.h>

namespace NKikimr::NCms {

using TimeUtil = ::google::protobuf::util::TimeUtil;

namespace {

    NKikimrCms::EAvailabilityMode ConvertAvailabilityMode(Ydb::Maintenance::AvailabilityMode mode) {
        switch (mode) {
        case Ydb::Maintenance::AVAILABILITY_MODE_STRONG:
            return NKikimrCms::MODE_MAX_AVAILABILITY;
        case Ydb::Maintenance::AVAILABILITY_MODE_WEAK:
            return NKikimrCms::MODE_KEEP_AVAILABLE;
        case Ydb::Maintenance::AVAILABILITY_MODE_FORCE:
            return NKikimrCms::MODE_FORCE_RESTART;
        default:
            Y_ABORT("unreachable");
        }
    }

    Ydb::Maintenance::AvailabilityMode ConvertAvailabilityMode(NKikimrCms::EAvailabilityMode mode) {
        switch (mode) {
        case NKikimrCms::MODE_MAX_AVAILABILITY:
            return Ydb::Maintenance::AVAILABILITY_MODE_STRONG;
        case NKikimrCms::MODE_KEEP_AVAILABLE:
            return Ydb::Maintenance::AVAILABILITY_MODE_WEAK;
        case NKikimrCms::MODE_FORCE_RESTART:
            return Ydb::Maintenance::AVAILABILITY_MODE_FORCE;
        default:
            Y_ABORT("unreachable");
        }
    }

    void ConvertAction(const NKikimrCms::TAction& cmsAction, Ydb::Maintenance::LockAction& action) {
        *action.mutable_duration() = TimeUtil::MicrosecondsToDuration(cmsAction.GetDuration());

        ui32 nodeId;
        if (TryFromString(cmsAction.GetHost(), nodeId)) {
            action.mutable_scope()->set_node_id(nodeId);
        } else {
            action.mutable_scope()->set_host(cmsAction.GetHost());
        }
    }

    void ConvertAction(const NKikimrCms::TAction& cmsAction, Ydb::Maintenance::ActionState& actionState) {
        ConvertAction(cmsAction, *actionState.mutable_action()->mutable_lock_action());
        // FIXME: specify action_uid
        actionState.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_PENDING);
        actionState.set_reason(Ydb::Maintenance::ActionState::ACTION_REASON_UNSPECIFIED); // FIXME: specify
    }

    void ConvertActionUid(const TString& taskUid, const TString& permissionId,
            Ydb::Maintenance::ActionUid& actionUid)
    {
        actionUid.set_task_uid(taskUid);
        actionUid.set_action_id(permissionId);
    }

    void ConvertPermission(const TString& taskUid, const NKikimrCms::TPermission& permission,
            Ydb::Maintenance::ActionState& actionState)
    {
        ConvertAction(permission.GetAction(), *actionState.mutable_action()->mutable_lock_action());
        ConvertActionUid(taskUid, permission.GetId(), *actionState.mutable_action_uid());

        actionState.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_PERFORMED);
        actionState.set_reason(Ydb::Maintenance::ActionState::ACTION_REASON_OK);
        *actionState.mutable_deadline() = TimeUtil::MicrosecondsToTimestamp(permission.GetDeadline());
    }

    void ConvertPermission(const TString& taskUid, const TPermissionInfo& permission,
            Ydb::Maintenance::ActionState& actionState)
    {
        NKikimrCms::TPermission protoPermission;
        permission.CopyTo(protoPermission);
        ConvertPermission(taskUid, protoPermission, actionState);
    }

} // anonymous

template <typename TDerived, typename TEvRequest, typename TEvResponse>
class TAdapterActor: public TActorBootstrapped<TDerived> {
protected:
    using TBase = TAdapterActor<TDerived, TEvRequest, TEvResponse>;

    TCmsStatePtr GetCmsState() const {
        Y_ABORT_UNLESS(CmsState);
        return CmsState;
    }

    void Reply(THolder<TEvResponse>&& ev) {
        this->Send(Request->Sender, std::move(ev));
        this->PassAway();
    }

    void Reply(Ydb::StatusIds::StatusCode code, const TString& error = {}) {
        auto ev = MakeHolder<TEvResponse>();
        ev->Record.SetStatus(code);

        if (error) {
            auto& issue = *ev->Record.AddIssues();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(error);
        }

        Reply(std::move(ev));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::CMS_API_ADAPTER;
    }

    explicit TAdapterActor(typename TEvRequest::TPtr& ev, const TActorId& cmsActorId, TCmsStatePtr cmsState = nullptr)
        : Request(ev)
        , CmsActorId(cmsActorId)
        , CmsState(cmsState)
    {
    }

protected:
    typename TEvRequest::TPtr Request;
    const TActorId CmsActorId;

private:
    const TCmsStatePtr CmsState;

}; // TAdapterActor

class TListClusterNodes: public TAdapterActor<
        TListClusterNodes,
        TEvCms::TEvListClusterNodesRequest,
        TEvCms::TEvListClusterNodesResponse>
{
    static Ydb::Maintenance::ItemState ConvertNodeState(NKikimrCms::EState state) {
        switch (state) {
        case NKikimrCms::UNKNOWN:
            return Ydb::Maintenance::ITEM_STATE_UNSPECIFIED;
        case NKikimrCms::UP:
            return Ydb::Maintenance::ITEM_STATE_UP;
        case NKikimrCms::RESTART:
            return Ydb::Maintenance::ITEM_STATE_MAINTENANCE;
        case NKikimrCms::DOWN:
            return Ydb::Maintenance::ITEM_STATE_DOWN;
        default:
            Y_ABORT("unreachable");
        }
    }

    static void ConvertNode(const TNodeInfo& in, Ydb::Maintenance::Node& out) {
        out.set_node_id(in.NodeId);
        out.set_host(in.Host);
        out.set_port(in.IcPort);
        out.set_state(ConvertNodeState(in.State));
        out.set_version(in.Version);
        *out.mutable_start_time() = TimeUtil::MicrosecondsToTimestamp(in.StartTime.GetValue());

        auto& location = *out.mutable_location();
        location.set_data_center(in.Location.GetDataCenterId());
        location.set_module(in.Location.GetModuleId());
        location.set_rack(in.Location.GetRackId());
        location.set_unit(in.Location.GetUnitId());

        if (in.Services & EService::DynamicNode) {
            out.mutable_dynamic()->set_tenant(in.Tenant);
        } else {
            out.mutable_storage();
        }
    }

public:
    using TBase::TBase;

    void Bootstrap() {
        Send(CmsActorId, new TEvCms::TEvGetClusterInfoRequest());
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCms::TEvGetClusterInfoResponse, Handle);
        }
    }

    void Handle(TEvCms::TEvGetClusterInfoResponse::TPtr& ev) {
        auto clusterInfo = ev->Get()->Info;

        if (clusterInfo->IsOutdated()) {
            return Reply(Ydb::StatusIds::UNAVAILABLE, "Cannot collect cluster info");
        }

        auto response = MakeHolder<TEvCms::TEvListClusterNodesResponse>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        for (const auto& [_, node] : clusterInfo->AllNodes()) {
            ConvertNode(*node, *response->Record.MutableResult()->add_nodes());
        }

        Reply(std::move(response));
    }

}; // TListClusterNodes

class TCompositeActionGroupHandler {
protected:
    template <typename TResult>
    Ydb::Maintenance::ActionGroupStates* GetActionGroupState(TResult& result) const {
        if (HasSingleCompositeActionGroup && !result.action_group_states().empty()) {
            return result.mutable_action_group_states(0);
        }
        return result.add_action_group_states();
    }

protected:
    bool HasSingleCompositeActionGroup = false;
};

template <typename TDerived, typename TEvRequest>
class TPermissionResponseProcessor  
    : public TAdapterActor<TDerived, TEvRequest, TEvCms::TEvMaintenanceTaskResponse>  
    , public TCompositeActionGroupHandler
{
protected:
    using TBase = TPermissionResponseProcessor<TDerived, TEvRequest>;

public:
    using TAdapterActor<TDerived, TEvRequest, TEvCms::TEvMaintenanceTaskResponse>::TAdapterActor;

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCms::TEvPermissionResponse, Handle);
        }
    }

    void Handle(TEvCms::TEvPermissionResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus().GetCode()) {
        case NKikimrCms::TStatus::ALLOW:
        case NKikimrCms::TStatus::ALLOW_PARTIAL:
        case NKikimrCms::TStatus::DISALLOW_TEMP:
            break;
        case NKikimrCms::TStatus::ERROR_TEMP:
            return this->Reply(Ydb::StatusIds::UNAVAILABLE, record.GetStatus().GetReason());
        case NKikimrCms::TStatus::DISALLOW:
        case NKikimrCms::TStatus::WRONG_REQUEST:
        case NKikimrCms::TStatus::ERROR:
        case NKikimrCms::TStatus::NO_SUCH_HOST:
        case NKikimrCms::TStatus::NO_SUCH_DEVICE:
        case NKikimrCms::TStatus::NO_SUCH_SERVICE:
            return this->Reply(Ydb::StatusIds::BAD_REQUEST, record.GetStatus().GetReason());
        case NKikimrCms::TStatus::UNAUTHORIZED:
            return this->Reply(Ydb::StatusIds::UNAUTHORIZED, record.GetStatus().GetReason());
        case NKikimrCms::TStatus::OK:
        case NKikimrCms::TStatus::UNKNOWN:
            return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetStatus().GetReason());
        }

        const auto& taskUid = static_cast<const TDerived*>(this)->GetTaskUid();

        auto response = MakeHolder<TEvCms::TEvMaintenanceTaskResponse>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        auto& result = *response->Record.MutableResult();
        result.set_task_uid(taskUid);

        if (record.GetDeadline()) {
            *result.mutable_retry_after() = TimeUtil::MicrosecondsToTimestamp(record.GetDeadline());
        }

        THashSet<TString> permissionsSeen;
        // performed actions: new permissions
        for (const auto& permission : record.GetPermissions()) {
            permissionsSeen.insert(permission.GetId());
            ConvertPermission(taskUid, permission, *GetActionGroupState(result)->add_action_states());
        }

        auto cmsState = this->GetCmsState();
        // performed actions: existing permissions
        if (cmsState->MaintenanceTasks.contains(taskUid)) {
            const auto& task = cmsState->MaintenanceTasks.at(taskUid);
            for (const auto& id : task.Permissions) {
                if (!cmsState->Permissions.contains(id) || permissionsSeen.contains(id)) {
                    continue;
                }

                ConvertPermission(taskUid, cmsState->Permissions.at(id),
                    *GetActionGroupState(result)->add_action_states());
            }
        }

        // pending actions
        if (cmsState->ScheduledRequests.contains(record.GetRequestId())) {
            const auto& request = cmsState->ScheduledRequests.at(record.GetRequestId());
            for (const auto& action : request.Request.GetActions()) {
                ConvertAction(action, *GetActionGroupState(result)->add_action_states());
            }
        }

        this->Reply(std::move(response));
    }

}; // TPermissionResponseProcessor

class TCreateMaintenanceTask: public TPermissionResponseProcessor<
        TCreateMaintenanceTask,
        TEvCms::TEvCreateMaintenanceTaskRequest>
{
    bool ValidateScope(const Ydb::Maintenance::ActionScope& scope) {
        switch (scope.scope_case()) {
        case Ydb::Maintenance::ActionScope::kNodeId:
        case Ydb::Maintenance::ActionScope::kHost:
            return true;
        default:
            Reply(Ydb::StatusIds::BAD_REQUEST, "Unknown scope");
            return false;
        }
    }

    bool ValidateAction(const Ydb::Maintenance::Action& action) {
        switch (action.action_case()) {
        case Ydb::Maintenance::Action::kLockAction:
            return ValidateScope(action.lock_action().scope());
        default:
            Reply(Ydb::StatusIds::BAD_REQUEST, "Unknown action");
            return false;
        }
    }

    bool ValidateRequest(const Ydb::Maintenance::CreateMaintenanceTaskRequest& request) {
        switch (request.task_options().availability_mode()) {
        case Ydb::Maintenance::AVAILABILITY_MODE_STRONG:
        case Ydb::Maintenance::AVAILABILITY_MODE_WEAK:
        case Ydb::Maintenance::AVAILABILITY_MODE_FORCE:
            break;
        default:
            Reply(Ydb::StatusIds::BAD_REQUEST, "Unknown availability mode");
            return false;
        }

        for (const auto& group : request.action_groups()) {
            if (group.actions().size() < 1) {
                Reply(Ydb::StatusIds::BAD_REQUEST, "Empty actions");
                return false;
            } 
            
            if (!GetCmsState()->EnableSingleCompositeActionGroup && group.actions().size() > 1) {
                Reply(Ydb::StatusIds::UNSUPPORTED, "Feature flag EnableSingleCompositeActionGroup is off");
                return false;
            } 

            if (request.action_groups().size() > 1 && group.actions().size() > 1) {
                Reply(Ydb::StatusIds::UNSUPPORTED, TStringBuilder()
                    << "A task can have either a single composite action group or many action groups"
                    << " with only one action");
                return false;
            }

            for (const auto& action : group.actions()) {
                if (!ValidateAction(action)) {
                    return false;
                }
            }
        }

        return true;
    }

    static void ConvertAction(const Ydb::Maintenance::LockAction& action, NKikimrCms::TAction& cmsAction) {
        cmsAction.SetType(NKikimrCms::TAction::SHUTDOWN_HOST);
        cmsAction.SetDuration(TimeUtil::DurationToMicroseconds(action.duration()));

        const auto& scope = action.scope();
        switch (scope.scope_case()) {
        case Ydb::Maintenance::ActionScope::kNodeId:
            cmsAction.SetHost(ToString(scope.node_id()));
            break;
        case Ydb::Maintenance::ActionScope::kHost:
            cmsAction.SetHost(scope.host());
            break;
        default:
            Y_ABORT("unreachable");
        }
    }

    void ConvertRequest(const TString& user, const Ydb::Maintenance::CreateMaintenanceTaskRequest& request,
            NKikimrCms::TPermissionRequest& cmsRequest)
    {
        const auto& opts = request.task_options();

        cmsRequest.SetMaintenanceTaskId(opts.task_uid());
        cmsRequest.SetUser(user);
        cmsRequest.SetDryRun(opts.dry_run());
        cmsRequest.SetReason(opts.description());
        cmsRequest.SetAvailabilityMode(ConvertAvailabilityMode(opts.availability_mode()));
        cmsRequest.SetSchedule(true);

        i32 priority = opts.priority();
        if (priority != 0) {
            cmsRequest.SetPriority(priority);
        }

        HasSingleCompositeActionGroup = request.action_groups().size() == 1 
                                        && request.action_groups(0).actions().size() > 1;
        cmsRequest.SetPartialPermissionAllowed(!HasSingleCompositeActionGroup);

        for (const auto& group : request.action_groups()) {
            Y_ABORT_UNLESS(HasSingleCompositeActionGroup || group.actions().size() == 1);
            for (const auto& action : group.actions()) {
                if (action.has_lock_action()) {
                    ConvertAction(action.lock_action(), *cmsRequest.AddActions());
                } else {
                    Y_ABORT("unreachable");
                }
            }
        }
    }

public:
    using TBase::TBase;

    void Bootstrap() {
        const auto& user = Request->Get()->Record.GetUserSID();
        const auto& request = Request->Get()->Record.GetRequest();

        if (!ValidateRequest(request)) {
            return;
        }

        auto cmsRequest = MakeHolder<TEvCms::TEvPermissionRequest>();
        ConvertRequest(user, request, cmsRequest->Record);

        Send(CmsActorId, std::move(cmsRequest));
        Become(&TThis::StateWork);
    }

    const TString& GetTaskUid() const {
        return Request->Get()->Record.GetRequest().task_options().task_uid();
    }

    // using processor's handler

}; // TCreateMaintenanceTask

class TRefreshMaintenanceTask: public TPermissionResponseProcessor<
        TRefreshMaintenanceTask,
        TEvCms::TEvRefreshMaintenanceTaskRequest>
{
public:
    using TBase::TBase;

    void Bootstrap() {
        const auto& taskUid = GetTaskUid();
        auto cmsState = GetCmsState();

        auto tit = cmsState->MaintenanceTasks.find(taskUid);
        if (tit == cmsState->MaintenanceTasks.end()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Task not found");
        }

        const auto& task = tit->second;
        HasSingleCompositeActionGroup = task.HasSingleCompositeActionGroup;

        auto rit = cmsState->ScheduledRequests.find(task.RequestId);
        if (rit == cmsState->ScheduledRequests.end()) {
            auto response = MakeHolder<TEvCms::TEvMaintenanceTaskResponse>();
            response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

            auto& result = *response->Record.MutableResult();
            result.set_task_uid(taskUid);

            // performed actions
            for (const auto& id : task.Permissions) {
                if (!cmsState->Permissions.contains(id)) {
                    continue;
                }

                ConvertPermission(taskUid, cmsState->Permissions.at(id),
                    *GetActionGroupState(result)->add_action_states());
            }

            if (result.action_group_states().empty()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Task has no active actions");
            }

            return Reply(std::move(response));
        }

        const auto& request = rit->second;

        auto cmsRequest = MakeHolder<TEvCms::TEvCheckRequest>();
        cmsRequest->Record.SetUser(task.Owner);
        cmsRequest->Record.SetRequestId(task.RequestId);
        cmsRequest->Record.SetAvailabilityMode(request.Request.GetAvailabilityMode());

        Send(CmsActorId, std::move(cmsRequest));
        Become(&TThis::StateWork);
    }

    const TString& GetTaskUid() const {
        return Request->Get()->Record.GetRequest().task_uid();
    }

    // using processor's handler

}; // TRefreshMaintenanceTask

class TGetMaintenanceTask  
    : public TAdapterActor<  
        TGetMaintenanceTask,  
        TEvCms::TEvGetMaintenanceTaskRequest,  
        TEvCms::TEvGetMaintenanceTaskResponse>  
    , public TCompositeActionGroupHandler 
{
public:
    using TBase::TBase;

    void Bootstrap() {
        const auto& taskUid = GetTaskUid();
        auto cmsState = GetCmsState();

        auto it = cmsState->MaintenanceTasks.find(taskUid);
        if (it == cmsState->MaintenanceTasks.end()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Task not found");
        }

        const auto& task = it->second;
        HasSingleCompositeActionGroup = task.HasSingleCompositeActionGroup;

        if (!cmsState->ScheduledRequests.contains(task.RequestId)) {
            auto response = MakeHolder<TEvCms::TEvGetMaintenanceTaskResponse>();
            response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

            auto& result = *response->Record.MutableResult();
            result.mutable_task_options()->set_task_uid(taskUid);

            // performed actions
            for (const auto& id : task.Permissions) {
                if (!cmsState->Permissions.contains(id)) {
                    continue;
                }

                ConvertPermission(taskUid, cmsState->Permissions.at(id),
                    *GetActionGroupState(result)->add_action_states());
            }

            return Reply(std::move(response));
        }

        auto cmsRequest = MakeHolder<TEvCms::TEvManageRequestRequest>();
        cmsRequest->Record.SetUser(task.Owner);
        cmsRequest->Record.SetRequestId(task.RequestId);
        cmsRequest->Record.SetCommand(NKikimrCms::TManageRequestRequest::GET);

        Send(CmsActorId, std::move(cmsRequest));
        Become(&TThis::StateWork);
    }

    const TString& GetTaskUid() const {
        return Request->Get()->Record.GetRequest().task_uid();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCms::TEvManageRequestResponse, Handle);
        }
    }

    void Handle(TEvCms::TEvManageRequestResponse::TPtr& ev) {
        const auto& taskUid = GetTaskUid();
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus().GetCode()) {
        case NKikimrCms::TStatus::OK:
            break;
        case NKikimrCms::TStatus::WRONG_REQUEST:
            return Reply(Ydb::StatusIds::BAD_REQUEST, record.GetStatus().GetReason());
        default:
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetStatus().GetReason());
        }

        auto response = MakeHolder<TEvCms::TEvGetMaintenanceTaskResponse>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        auto& result = *response->Record.MutableResult();
        for (const auto& request : record.GetRequests()) {
            auto& opts = *result.mutable_task_options();
            opts.set_task_uid(taskUid);
            opts.set_description(request.GetReason());
            opts.set_availability_mode(ConvertAvailabilityMode(request.GetAvailabilityMode()));
            opts.set_priority(request.GetPriority());

            // pending actions
            for (const auto& action : request.GetActions()) {
                ConvertAction(action, *GetActionGroupState(result)->add_action_states());
            }
        }

        auto cmsState = GetCmsState();
        // performed actions
        if (cmsState->MaintenanceTasks.contains(taskUid)) {
            const auto& task = cmsState->MaintenanceTasks.at(taskUid);
            for (const auto& id : task.Permissions) {
                if (!cmsState->Permissions.contains(id)) {
                    continue;
                }

                ConvertPermission(taskUid, cmsState->Permissions.at(id),
                    *GetActionGroupState(result)->add_action_states());
            }
        }

        Reply(std::move(response));
    }

}; // TGetMaintenanceTask

class TListMaintenanceTasks: public TAdapterActor<
        TListMaintenanceTasks,
        TEvCms::TEvListMaintenanceTasksRequest,
        TEvCms::TEvListMaintenanceTasksResponse>
{
public:
    using TBase::TBase;

    void Bootstrap() {
        const auto& user = Request->Get()->Record.GetRequest().user();

        auto response = MakeHolder<TEvCms::TEvListMaintenanceTasksResponse>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        auto cmsState = GetCmsState();
        for (const auto& [taskUid, task] : cmsState->MaintenanceTasks) {
            if (!user || user == task.Owner) {
                response->Record.MutableResult()->add_tasks_uids(taskUid);
            }
        }

        Reply(std::move(response));
    }

}; // TListMaintenanceTasks

class TDropMaintenanceTask: public TAdapterActor<
        TDropMaintenanceTask,
        TEvCms::TEvDropMaintenanceTaskRequest,
        TEvCms::TEvManageMaintenanceTaskResponse>
{
    void DropRequest(const TTaskInfo& task) {
        auto cmsRequest = MakeHolder<TEvCms::TEvManageRequestRequest>();
        cmsRequest->Record.SetUser(task.Owner);
        cmsRequest->Record.SetRequestId(task.RequestId);
        cmsRequest->Record.SetCommand(NKikimrCms::TManageRequestRequest::REJECT);

        Send(CmsActorId, std::move(cmsRequest));
    }

    void DropPermissions(const TTaskInfo& task) {
        auto cmsRequest = MakeHolder<TEvCms::TEvManagePermissionRequest>();
        cmsRequest->Record.SetUser(task.Owner);
        cmsRequest->Record.SetCommand(NKikimrCms::TManagePermissionRequest::REJECT);

        for (const auto& id : task.Permissions) {
            cmsRequest->Record.AddPermissions(id);
        }

        Send(CmsActorId, std::move(cmsRequest));
    }

public:
    using TBase::TBase;

    void Bootstrap() {
        auto cmsState = GetCmsState();

        auto it = cmsState->MaintenanceTasks.find(GetTaskUid());
        if (it == cmsState->MaintenanceTasks.end()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Task not found");
        }

        const auto& task = it->second;
        if (cmsState->ScheduledRequests.contains(task.RequestId)) {
            DropRequest(task);
        } else {
            DropPermissions(task);
        }

        Become(&TThis::StateWork);
    }

    const TString& GetTaskUid() const {
        return Request->Get()->Record.GetRequest().task_uid();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCms::TEvManageRequestResponse, HandleDropRequest);
            hFunc(TEvCms::TEvManagePermissionResponse, Handle<TEvCms::TEvManagePermissionResponse>);
        }
    }

    void HandleDropRequest(TEvCms::TEvManageRequestResponse::TPtr& ev) {
        auto cmsState = GetCmsState();
        if (cmsState->MaintenanceTasks.contains(GetTaskUid())) {
            DropPermissions(cmsState->MaintenanceTasks.at(GetTaskUid()));
        } else {
            Handle<TEvCms::TEvManageRequestResponse>(ev);
        }
    }

    template <typename TEvResponse>
    void Handle(typename TEvResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus().GetCode()) {
        case NKikimrCms::TStatus::OK:
            return Reply(Ydb::StatusIds::SUCCESS);
        case NKikimrCms::TStatus::WRONG_REQUEST:
            return Reply(Ydb::StatusIds::BAD_REQUEST, record.GetStatus().GetReason());
        default:
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetStatus().GetReason());
        }
    }

}; // TDropMaintenanceTask

class TCompleteAction: public TAdapterActor<
        TCompleteAction,
        TEvCms::TEvCompleteActionRequest,
        TEvCms::TEvManageActionResponse>
{
public:
    using TBase::TBase;

    void Bootstrap() {
        auto cmsRequest = MakeHolder<TEvCms::TEvManagePermissionRequest>();
        cmsRequest->Record.SetCommand(NKikimrCms::TManagePermissionRequest::DONE);

        auto cmsState = GetCmsState();
        for (const auto& actionUid : Request->Get()->Record.GetRequest().action_uids()) {
            auto it = cmsState->MaintenanceTasks.find(actionUid.task_uid());
            if (it == cmsState->MaintenanceTasks.end()) {
                return Reply(Ydb::StatusIds::BAD_REQUEST, "Task not found");
            }

            const auto& task = it->second;
            if (!cmsState->ScheduledRequests.contains(task.RequestId)) {
                if (!task.Permissions.contains(actionUid.action_id())) {
                    return Reply(Ydb::StatusIds::BAD_REQUEST, "Action not found");
                }
            }

            if (cmsRequest->Record.HasUser() && cmsRequest->Record.GetUser() != task.Owner) {
                return Reply(Ydb::StatusIds::UNSUPPORTED, "Cannot complete actions of multiple owners at once");
            } else {
                cmsRequest->Record.SetUser(task.Owner);
            }

            cmsRequest->Record.AddPermissions(actionUid.action_id());
        }

        Send(CmsActorId, std::move(cmsRequest));
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCms::TEvManagePermissionResponse, Handle);
        }
    }

    void Handle(TEvCms::TEvManagePermissionResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus().GetCode()) {
        case NKikimrCms::TStatus::OK:
            break;
        case NKikimrCms::TStatus::WRONG_REQUEST:
            return Reply(Ydb::StatusIds::BAD_REQUEST, record.GetStatus().GetReason());
        default:
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetStatus().GetReason());
        }

        auto response = MakeHolder<TEvCms::TEvManageActionResponse>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        for (const auto& actionUid : Request->Get()->Record.GetRequest().action_uids()) {
            auto& actionStatus = *response->Record.MutableResult()->add_action_statuses();
            *actionStatus.mutable_action_uid() = actionUid;
            actionStatus.set_status(Ydb::StatusIds::SUCCESS);
        }

        Reply(std::move(response));
    }

}; // TCompleteAction

void TCms::Handle(TEvCms::TEvListClusterNodesRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TListClusterNodes(ev, SelfId()));
}

void TCms::Handle(TEvCms::TEvCreateMaintenanceTaskRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TCreateMaintenanceTask(ev, SelfId(), State));
}

void TCms::Handle(TEvCms::TEvRefreshMaintenanceTaskRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TRefreshMaintenanceTask(ev, SelfId(), State));
}

void TCms::Handle(TEvCms::TEvGetMaintenanceTaskRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TGetMaintenanceTask(ev, SelfId(), State));
}

void TCms::Handle(TEvCms::TEvListMaintenanceTasksRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TListMaintenanceTasks(ev, SelfId(), State));
}

void TCms::Handle(TEvCms::TEvDropMaintenanceTaskRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TDropMaintenanceTask(ev, SelfId(), State));
}

void TCms::Handle(TEvCms::TEvCompleteActionRequest::TPtr& ev, const TActorContext& ctx) {
    ctx.RegisterWithSameMailbox(new TCompleteAction(ev, SelfId(), State));
}

}
