#include "cms_impl.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/draft/ydb_maintenance.pb.h>
#include <yql/essentials/public/issue/protos/issue_severity.pb.h>

#include <ydb/core/base/hive.h>
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
        case Ydb::Maintenance::AVAILABILITY_MODE_SMART:
            return NKikimrCms::MODE_SMART_AVAILABILITY;
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
        case NKikimrCms::MODE_SMART_AVAILABILITY:
            return Ydb::Maintenance::AVAILABILITY_MODE_SMART;
        default:
            Y_ABORT("unreachable");
        }
    }

    static bool ConvertToPDiskId(const TString &name, TPDiskID &id) {
        int size;

        if (sscanf(name.data(), "pdisk-%" SCNu32 "-%" SCNu32 "%n", &id.NodeId, &id.DiskId, &size) != 2) {
            return false;
        }

        if (size != static_cast<int>(name.size())) {
            return false;
        }

        return true;
    }

    template <typename T>
    concept CApiAction = std::same_as<T, Ydb::Maintenance::LockAction>
                      || std::same_as<T, Ydb::Maintenance::DrainAction>
                      || std::same_as<T, Ydb::Maintenance::CordonAction>;

    template <CApiAction TApiAction>
    static void ConvertAction(const NKikimrCms::TAction& cmsAction, TApiAction& action) {
        if constexpr (std::is_same_v<TApiAction, Ydb::Maintenance::LockAction>) {
            *action.mutable_duration() = TimeUtil::MicrosecondsToDuration(cmsAction.GetDuration());
        }

        if (cmsAction.DevicesSize() > 0) {
            Y_ABORT_UNLESS(cmsAction.DevicesSize() == 1);
            auto& device = cmsAction.GetDevices()[0];
            auto* pdisk = action.mutable_scope()->mutable_pdisk();
            TPDiskID id;
            if (ConvertToPDiskId(device, id)) {
                auto* pdiskId = pdisk->mutable_pdisk_id();
                pdiskId->set_node_id(id.NodeId);
                pdiskId->set_pdisk_id(id.DiskId);
            } else {
                auto* pdiskLocation = pdisk->mutable_pdisk_location();
                pdiskLocation->set_host(cmsAction.GetHost());
                pdiskLocation->set_path(device);
            }
        } else {
            ui32 nodeId;
            if (TryFromString(cmsAction.GetHost(), nodeId)) {
                action.mutable_scope()->set_node_id(nodeId);
            } else {
                action.mutable_scope()->set_host(cmsAction.GetHost());
            }
        }
    }

    Ydb::Maintenance::ActionState::ActionReason ConvertReason(NKikimrCms::TAction::TIssue::EType cmsActionIssueType) {
        using EIssueType = NKikimrCms::TAction::TIssue;
        switch (cmsActionIssueType) {
            case EIssueType::UNKNOWN:
                return Ydb::Maintenance::ActionState::ACTION_REASON_UNSPECIFIED;
            case EIssueType::GENERIC:
                return Ydb::Maintenance::ActionState::ACTION_REASON_GENERIC;
            case EIssueType::TOO_MANY_UNAVAILABLE_VDISKS:
                return Ydb::Maintenance::ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_VDISKS;
            case EIssueType::TOO_MANY_UNAVAILABLE_STATE_STORAGE_RINGS:
                return Ydb::Maintenance::ActionState::ACTION_REASON_TOO_MANY_UNAVAILABLE_STATE_STORAGE_RINGS;
            case EIssueType::DISABLED_NODES_LIMIT_REACHED:
                return Ydb::Maintenance::ActionState::ACTION_REASON_DISABLED_NODES_LIMIT_REACHED;
            case EIssueType::TENANT_DISABLED_NODES_LIMIT_REACHED:
                return Ydb::Maintenance::ActionState::ACTION_REASON_TENANT_DISABLED_NODES_LIMIT_REACHED;
            case EIssueType::SYS_TABLETS_NODE_LIMIT_REACHED:
                return Ydb::Maintenance::ActionState::ACTION_REASON_SYS_TABLETS_NODE_LIMIT_REACHED;
        }
        return Ydb::Maintenance::ActionState::ACTION_REASON_UNSPECIFIED;
    }

    void ConvertAction(const NKikimrCms::TAction& cmsAction, Ydb::Maintenance::ActionState& actionState) {
        // FIXME: specify action_uid
        actionState.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_PENDING);
        actionState.set_reason(ConvertReason(cmsAction.GetIssue().GetType()));
        actionState.set_details(cmsAction.GetIssue().GetMessage());

        switch (cmsAction.GetType()) {
        case NKikimrCms::TAction::DRAIN_NODE:
            return ConvertAction(cmsAction, *actionState.mutable_action()->mutable_drain_action());
        case NKikimrCms::TAction::CORDON_NODE:
            return ConvertAction(cmsAction, *actionState.mutable_action()->mutable_cordon_action());
        default:
            return ConvertAction(cmsAction, *actionState.mutable_action()->mutable_lock_action());
        }
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
        ConvertActionUid(taskUid, permission.GetId(), *actionState.mutable_action_uid());

        actionState.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_PERFORMED);
        actionState.set_reason(Ydb::Maintenance::ActionState::ACTION_REASON_OK);
        *actionState.mutable_deadline() = TimeUtil::MicrosecondsToTimestamp(permission.GetDeadline());

        switch (permission.GetAction().GetType()) {
        case NKikimrCms::TAction::DRAIN_NODE:
            return ConvertAction(permission.GetAction(), *actionState.mutable_action()->mutable_drain_action());
        case NKikimrCms::TAction::CORDON_NODE:
            return ConvertAction(permission.GetAction(), *actionState.mutable_action()->mutable_cordon_action());
        default:
            return ConvertAction(permission.GetAction(), *actionState.mutable_action()->mutable_lock_action());
        }
    }

    void ConvertPermission(const TString& taskUid, const TPermissionInfo& permission,
            Ydb::Maintenance::ActionState& actionState)
    {
        NKikimrCms::TPermission protoPermission;
        permission.CopyTo(protoPermission);
        ConvertPermission(taskUid, protoPermission, actionState);
    }

    void ConvertInstant(const TInstant& instant, google::protobuf::Timestamp& protoValue) {
        protoValue.set_seconds(instant.Seconds());
        protoValue.set_nanos(instant.NanoSecondsOfSecond());
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

class THiveInteractor {
public:
    explicit THiveInteractor(IActorOps* actorOps)
        : ActorOps(actorOps)
    {
    }

protected:
    TActorId HivePipe(const TActorId& self) {
        if (!HivePipeActor) {
            const auto hiveId = AppData()->DomainsInfo->GetHive();
            NTabletPipe::TClientConfig config;
            config.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
            HivePipeActor = ActorOps->Register(NTabletPipe::CreateClient(self, hiveId, config));
        }
        return HivePipeActor;
    }

    void Close(const TActorId& self) {
        NTabletPipe::CloseAndForgetClient(TActorIdentity(self), HivePipeActor);
    }

    void Shutdown(const TActorContext& ctx) {
        NTabletPipe::ShutdownClient(ctx, HivePipeActor);
    }

    ui64 NewCookie() {
        return ++Cookie;
    }

private:
    IActorOps* const ActorOps;
    TActorId HivePipeActor;
    ui64 Cookie = 0;
};

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

        if (const auto bridgePileName = in.Location.GetBridgePileName(); bridgePileName) {
            location.set_bridge_pile_name(*bridgePileName);
        }

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
    using TActionIdx = std::pair<int, int>; // [group idx, action idx]

    template <typename TResult>
    Ydb::Maintenance::ActionGroupStates* GetActionGroupState(TResult& result) const {
        if (HasSingleCompositeActionGroup && !result.action_group_states().empty()) {
            return result.mutable_action_group_states(0);
        }
        return result.add_action_group_states();
    }

    template <typename TResult>
    static TActionIdx GetLastIdx(const TResult& result) {
        Y_ABORT_UNLESS(!result.action_group_states().empty());
        const int groupIdx = result.action_group_states_size() - 1;
        const int actionIdx = result.action_group_states(groupIdx).action_states_size() - 1;
        return {groupIdx, actionIdx};
    }

    template <typename TResult>
    static Ydb::Maintenance::ActionState* GetActionState(TResult& result, TActionIdx idx) {
        return result.mutable_action_group_states(idx.first)->mutable_action_states(idx.second);
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

            ConvertInstant(task.CreateTime, *result.mutable_create_time());
            ConvertInstant(task.LastRefreshTime, *result.mutable_last_refresh_time());

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

class TCreateMaintenanceTask
    : public TPermissionResponseProcessor<
        TCreateMaintenanceTask,
        TEvCms::TEvCreateMaintenanceTaskRequest>
    , public THiveInteractor
{
    using EActionCase = Ydb::Maintenance::Action::ActionCase;
    using EScopeCase = Ydb::Maintenance::ActionScope::ScopeCase;

    template <EActionCase>
    static std::vector<EScopeCase> SupportedScopes();

    template <>
    std::vector<EScopeCase> SupportedScopes<EActionCase::kLockAction>() {
        return {
            EScopeCase::kNodeId,
            EScopeCase::kHost,
            EScopeCase::kPdisk,
        };
    }

    template <>
    std::vector<EScopeCase> SupportedScopes<EActionCase::kDrainAction>() {
        return {
            EScopeCase::kNodeId,
        };
    }

    template <>
    std::vector<EScopeCase> SupportedScopes<EActionCase::kCordonAction>() {
        return {
            EScopeCase::kNodeId,
        };
    }

    template <EActionCase Action>
    bool ValidateScope(const Ydb::Maintenance::ActionScope& scope) {
        const auto supportedScopes = SupportedScopes<Action>();
        const auto it = std::find(supportedScopes.begin(), supportedScopes.end(), scope.scope_case());
        if (it == supportedScopes.end()) {
            Reply(Ydb::StatusIds::BAD_REQUEST, "Unsupported scope");
            return false;
        }
        return true;
    }

    bool ValidateAction(const Ydb::Maintenance::Action& action) {
        switch (action.action_case()) {
        case EActionCase::kLockAction:
            return ValidateScope<EActionCase::kLockAction>(action.lock_action().scope());
        case EActionCase::kDrainAction:
            return ValidateScope<EActionCase::kDrainAction>(action.drain_action().scope());
        case EActionCase::kCordonAction:
            return ValidateScope<EActionCase::kCordonAction>(action.cordon_action().scope());
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
        case Ydb::Maintenance::AVAILABILITY_MODE_SMART:
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

    static void ConvertPDiskId(const Ydb::Maintenance::ActionScope_PDiskId& pdiskId, TString& out) {
        out = Sprintf("pdisk-%" PRIu32 "-%" PRIu32, pdiskId.node_id(), pdiskId.pdisk_id());
    }

    static void ConvertScope(const Ydb::Maintenance::ActionScope& scope, NKikimrCms::TAction& cmsAction) {
        switch (scope.scope_case()) {
        case Ydb::Maintenance::ActionScope::kNodeId:
            cmsAction.SetHost(ToString(scope.node_id()));
            break;
        case Ydb::Maintenance::ActionScope::kHost:
            cmsAction.SetHost(scope.host());
            break;
        case Ydb::Maintenance::ActionScope::kPdisk: {
            auto& pdisk = scope.pdisk();
            if (pdisk.has_pdisk_id()) {
                ConvertPDiskId(pdisk.pdisk_id(), *cmsAction.add_devices());
            } else if (pdisk.has_pdisk_location()) {
                auto& pdiskLocation = pdisk.pdisk_location();
                cmsAction.SetHost(pdiskLocation.host());
                *cmsAction.add_devices() = pdiskLocation.path();
            }
            break;
        }
        default:
            Y_ABORT("unreachable");
        }
    }

    static void ConvertAction(const Ydb::Maintenance::LockAction& action, NKikimrCms::TAction& cmsAction) {
        switch (action.scope().scope_case()) {
        case Ydb::Maintenance::ActionScope::kPdisk:
            cmsAction.SetType(NKikimrCms::TAction::REPLACE_DEVICES);
            break;
        default:
            cmsAction.SetType(NKikimrCms::TAction::SHUTDOWN_HOST);
        }

        cmsAction.SetDuration(TimeUtil::DurationToMicroseconds(action.duration()));

        ConvertScope(action.scope(), cmsAction);
    }

    static void ConvertAction(const Ydb::Maintenance::DrainAction& action, NKikimrCms::TAction& cmsAction) {
        cmsAction.SetType(NKikimrCms::TAction::DRAIN_NODE);
        cmsAction.SetDuration(TDuration::Max().GetValue());

        ConvertScope(action.scope(), cmsAction);
    }

    static void ConvertAction(const Ydb::Maintenance::CordonAction& action, NKikimrCms::TAction& cmsAction) {
        cmsAction.SetType(NKikimrCms::TAction::CORDON_NODE);
        cmsAction.SetDuration(TDuration::Max().GetValue());

        ConvertScope(action.scope(), cmsAction);
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
                const int actionNo = cmsRequest.ActionsSize();
                if (action.has_lock_action()) {
                    ConvertAction(action.lock_action(), *cmsRequest.AddActions());
                } else if (action.has_drain_action()) {
                    ConvertAction(action.drain_action(), *cmsRequest.AddActions());
                    const auto nodeId = action.drain_action().scope().node_id();
                    NTabletPipe::SendData(SelfId(), HivePipe(SelfId()), new TEvHive::TEvDrainNode(nodeId), actionNo);
                    PendingHiveActions.insert(actionNo);
                } else if (action.has_cordon_action()) {
                    ConvertAction(action.cordon_action(), *cmsRequest.AddActions());
                    const auto nodeId = action.cordon_action().scope().node_id();
                    NTabletPipe::SendData(SelfId(), HivePipe(SelfId()), new TEvHive::TEvSetDown(nodeId));
                    PendingHiveActions.insert(actionNo);
                } else {
                    Y_ABORT("unreachable");
                }
            }
        }
    }

    void CheckPendingHiveActions() {
        if (PendingHiveActions.empty()) {
            Send(CmsActorId, std::move(CmsRequest));
            Become(&TThis::StateWork);
        }
    }

    void Handle(TEvHive::TEvDrainNodeAck::TPtr& ev) {
        const int actionNo = ev->Cookie;
        if (!PendingHiveActions.erase(actionNo)) {
            return;
        }
        const ui64 drainSeqNo = ev->Get()->Record.GetSeqNo();
        CmsRequest->Record.MutableActions(actionNo)->SetMaintenanceTaskContext(ToString(drainSeqNo));
        CheckPendingHiveActions();
    }

    void Handle(TEvHive::TEvDrainNodeResult::TPtr& ev) {
        const auto status = ev->Get()->Record.GetStatus();
        if (status != NKikimrProto::OK) {
            Reply(Ydb::StatusIds::GENERIC_ERROR, "Drain failed");
        }
    }

    void Handle(TEvHive::TEvSetDownReply::TPtr& ev) {
        const int actionNo = ev->Cookie;
        if (!PendingHiveActions.erase(actionNo)) {
            return;
        }
        const auto status = ev->Get()->Record.GetStatus();
        if (status != NKikimrProto::OK) {
            Reply(Ydb::StatusIds::GENERIC_ERROR, "Cordon failed");
        }
        CheckPendingHiveActions();
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        THiveInteractor::Close(SelfId());
        Reply(Ydb::StatusIds::UNAVAILABLE, "Hive request failed");
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) {
        const TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            THiveInteractor::Close(SelfId());
            Reply(Ydb::StatusIds::UNAVAILABLE, "Hive request failed");
        }
    }

private:
    THolder<TEvCms::TEvPermissionRequest> CmsRequest = MakeHolder<TEvCms::TEvPermissionRequest>();
    std::unordered_set<int> PendingHiveActions;

public:
    TCreateMaintenanceTask(TEvCms::TEvCreateMaintenanceTaskRequest::TPtr& ev, const TActorId& cmsActorId, TCmsStatePtr cmsState = nullptr)
        : TBase(ev, cmsActorId, cmsState)
        , THiveInteractor(this)
    {
    }

    void Bootstrap() {
        const auto& user = Request->Get()->Record.GetUserSID();
        const auto& request = Request->Get()->Record.GetRequest();

        if (!ValidateRequest(request)) {
            return;
        }

        ConvertRequest(user, request, CmsRequest->Record);

        Become(&TThis::StateWaitHive);

        CheckPendingHiveActions();
    }

    const TString& GetTaskUid() const {
        return Request->Get()->Record.GetRequest().task_options().task_uid();
    }

    void PassAway() override {
        THiveInteractor::Shutdown(TActivationContext::AsActorContext());
        TBase::PassAway();
    }

    STFUNC(StateWaitHive) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHive::TEvDrainNodeAck, Handle);
            hFunc(TEvHive::TEvDrainNodeResult, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvHive::TEvSetDownReply, Handle);
        }
    }

    // using processor's handler for StateWork

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
            ConvertInstant(task.CreateTime, *result.mutable_create_time());
            ConvertInstant(task.LastRefreshTime, *result.mutable_last_refresh_time());

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
    , public THiveInteractor
{
public:
    TGetMaintenanceTask(TEvCms::TEvGetMaintenanceTaskRequest::TPtr& ev, const TActorId& cmsActorId, TCmsStatePtr cmsState = nullptr)
        : TBase(ev, cmsActorId, cmsState)
        , THiveInteractor(this)
    {
    }

    void Bootstrap() {
        const auto& taskUid = GetTaskUid();
        auto cmsState = GetCmsState();

        auto it = cmsState->MaintenanceTasks.find(taskUid);
        if (it == cmsState->MaintenanceTasks.end()) {
            return Reply(Ydb::StatusIds::BAD_REQUEST, "Task not found");
        }

        const auto& task = it->second;
        HasSingleCompositeActionGroup = task.HasSingleCompositeActionGroup;
        Response->Record.SetStatus(Ydb::StatusIds::SUCCESS);

        auto& result = *Response->Record.MutableResult();
        result.mutable_task_options()->set_task_uid(taskUid);
        ConvertInstant(task.CreateTime, *result.mutable_create_time());
        ConvertInstant(task.LastRefreshTime, *result.mutable_last_refresh_time());
        ConvertPermissions(task, result);


        if (cmsState->ScheduledRequests.contains(task.RequestId)) {
            auto cmsRequest = MakeHolder<TEvCms::TEvManageRequestRequest>();
            cmsRequest->Record.SetUser(task.Owner);
            cmsRequest->Record.SetRequestId(task.RequestId);
            cmsRequest->Record.SetCommand(NKikimrCms::TManageRequestRequest::GET);

            Send(CmsActorId, std::move(cmsRequest));
            WaitingForCms = true;
        }

        Become(&TThis::StateWork);

        MaybeReply();
    }

    void ConvertPermissions(const TTaskInfo& task, Ydb::Maintenance::GetMaintenanceTaskResult& result) {
        auto cmsState = GetCmsState();
        const auto& taskUid = GetTaskUid();
        for (const auto& id : task.Permissions) {
            if (!cmsState->Permissions.contains(id)) {
                continue;
            }

            const auto& permission = cmsState->Permissions.at(id);
            ConvertPermission(taskUid, permission, *GetActionGroupState(result)->add_action_states());
            if (permission.Action.GetType() == NKikimrCms::TAction::DRAIN_NODE) {
                TActionIdx actionIdx = GetLastIdx(result);
                ui64 cookie = NewCookie();
                PendingDrainActions[cookie] = actionIdx;
                ui32 nodeId = FromString(permission.Action.GetHost());

                NTabletPipe::SendData(SelfId(), HivePipe(SelfId()), new TEvHive::TEvRequestDrainInfo(nodeId), cookie);
            }
        }
    }

    const TString& GetTaskUid() const {
        return Request->Get()->Record.GetRequest().task_uid();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvCms::TEvManageRequestResponse, Handle);
            hFunc(TEvHive::TEvResponseDrainInfo, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
        }
    }

    void MaybeReply() {
        if (PendingDrainActions.empty() && !WaitingForCms) {
            Reply(std::move(Response));
        }
    }

    void Handle(TEvCms::TEvManageRequestResponse::TPtr& ev) {
        WaitingForCms = false;
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

        auto& result = *Response->Record.MutableResult();
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

        MaybeReply();
    }

    void Handle(TEvHive::TEvResponseDrainInfo::TPtr& ev) {
        const auto cookie = ev->Cookie;
        auto it = PendingDrainActions.find(cookie);
        if (it == PendingDrainActions.end()) {
            return;
        }
        const TActionIdx actionIdx = it->second;
        PendingDrainActions.erase(it);

        auto& result = *Response->Record.MutableResult();
        auto& actionInfo = *GetActionState(result, actionIdx);
        const auto& record = ev->Get()->Record;

        const auto nodeId = record.GetNodeId();
        if (nodeId == 0) {
            actionInfo.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_UNSPECIFIED);
        }

        auto cmsState = GetCmsState();
        const auto permissionId = actionInfo.action_uid().action_id();
        auto cmsIt = cmsState->Permissions.find(permissionId);
        if (cmsIt == cmsState->Permissions.end()) {
            return MaybeReply();
        }
        const ui64 expectedSeqNo = FromString(cmsIt->second.Action.GetMaintenanceTaskContext());
        const ui64 actualSeqNo = record.GetDrainSeqNo();
        if (actualSeqNo > expectedSeqNo || (actualSeqNo == expectedSeqNo && !record.HasDrainInProgress())) {
            actionInfo.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_PERFORMED);
        } else if (actualSeqNo == expectedSeqNo && record.HasDrainInProgress()) {
            actionInfo.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_IN_PROGRESS);
            if (record.GetDrainInProgress().HasProgress()) {
                actionInfo.set_details(Sprintf("Progress: %.2f%%", record.GetDrainInProgress().GetProgress()));
            }
        } else {
            actionInfo.set_status(Ydb::Maintenance::ActionState::ACTION_STATUS_UNSPECIFIED);
        }

        MaybeReply();
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        THiveInteractor::Close(SelfId());
        Reply(Ydb::StatusIds::UNAVAILABLE, "Hive request failed");
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) {
        TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            THiveInteractor::Close(SelfId());
            Reply(Ydb::StatusIds::UNAVAILABLE, "Hive request failed");
        }
    }

    void PassAway() override {
        THiveInteractor::Close(SelfId());
        TBase::PassAway();
    }

private:
    THolder<TEvCms::TEvGetMaintenanceTaskResponse> Response = MakeHolder<TEvCms::TEvGetMaintenanceTaskResponse>();
    std::unordered_map<ui64, TActionIdx> PendingDrainActions;
    bool WaitingForCms = false;

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

class TDropMaintenanceTask
    : public TAdapterActor<
        TDropMaintenanceTask,
        TEvCms::TEvDropMaintenanceTaskRequest,
        TEvCms::TEvManageMaintenanceTaskResponse>
    , THiveInteractor
{
    void DropRequest(const TTaskInfo& task) {
        auto cmsRequest = MakeHolder<TEvCms::TEvManageRequestRequest>();
        cmsRequest->Record.SetUser(task.Owner);
        cmsRequest->Record.SetRequestId(task.RequestId);
        cmsRequest->Record.SetCommand(NKikimrCms::TManageRequestRequest::REJECT);

        Send(CmsActorId, std::move(cmsRequest));
        ++Requests;
    }

    void DropPermissions(const TTaskInfo& task) {
        auto cmsRequest = MakeHolder<TEvCms::TEvManagePermissionRequest>();
        cmsRequest->Record.SetUser(task.Owner);
        cmsRequest->Record.SetCommand(NKikimrCms::TManagePermissionRequest::REJECT);

        for (const auto& id : task.Permissions) {
            cmsRequest->Record.AddPermissions(id);
            const auto& permission = GetCmsState()->Permissions.at(id);
            if (permission.Action.GetType() == NKikimrCms::TAction::DRAIN_NODE ||
                permission.Action.GetType() == NKikimrCms::TAction::CORDON_NODE) 
            {
                const ui32 nodeId = FromString(permission.Action.GetHost());
                NTabletPipe::SendData(SelfId(), HivePipe(SelfId()), new TEvHive::TEvSetDown(nodeId, false));
                ++Requests;
            }
        }

        Send(CmsActorId, std::move(cmsRequest));
        ++Requests;
    }

public:
    TDropMaintenanceTask(TEvCms::TEvDropMaintenanceTaskRequest::TPtr& ev, const TActorId& cmsActorId, TCmsStatePtr cmsState = nullptr)
        : TBase(ev, cmsActorId, cmsState)
        , THiveInteractor(this)
    {
    }

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
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvHive::TEvSetDownReply, Handle);
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
            return MaybeReply();
        case NKikimrCms::TStatus::WRONG_REQUEST:
            return Reply(Ydb::StatusIds::BAD_REQUEST, record.GetStatus().GetReason());
        default:
            return Reply(Ydb::StatusIds::INTERNAL_ERROR, record.GetStatus().GetReason());
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        Reply(Ydb::StatusIds::UNAVAILABLE, "Hive request failed");
        THiveInteractor::Close(SelfId());
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev) {
        const TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            THiveInteractor::Close(SelfId());
            Reply(Ydb::StatusIds::UNAVAILABLE, "Hive request failed");
        }
    }

    void Handle(TEvHive::TEvSetDownReply::TPtr &ev) {
        if (ev->Get()->Record.GetStatus() == NKikimrProto::OK) {
            return MaybeReply();
        } else {
            return Reply(Ydb::StatusIds::GENERIC_ERROR, "Node not found");
        }
    }

    void MaybeReply() {
        if (--Requests == 0) {
            return Reply(Ydb::StatusIds::SUCCESS);
        }
    }

    void PassAway() override {
        THiveInteractor::Shutdown(TActivationContext::AsActorContext());
        TBase::PassAway();
    }

private:
    i64 Requests = 0;

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
