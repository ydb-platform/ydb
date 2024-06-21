#include "cms_impl.h"
#include "erasure_checkers.h"
#include "info_collector.h"
#include "node_checkers.h"
#include "scheme.h"
#include "sentinel.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/cms/console/config_helpers.h>
#include <ydb/core/erasure/erasure.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config_units.pb.h>
#include <ydb/core/protos/counters_cms.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

namespace NKikimr::NCms {

using namespace NNodeWhiteboard;
using namespace NKikimrCms;

void TCms::DefaultSignalTabletActive(const TActorContext &)
{
    // must be empty
}

void TCms::OnActivateExecutor(const TActorContext &ctx)
{
    if (AppData(ctx)->DomainsInfo->Domains.size() > 1) {
        NotSupportedReason = "Multiple domains are not supported.";
        Become(&TThis::StateNotSupported);
        SignalTabletActive(ctx);
        return;
    }

    EnableCMSRequestPriorities = AppData(ctx)->FeatureFlags.GetEnableCMSRequestPriorities();

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr.Release());

    State->CmsTabletId = TabletID();
    State->CmsActorId = SelfId();

    SubscribeForConfig(ctx);

    Execute(CreateTxInitScheme(), ctx);
}

void TCms::OnDetach(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS, "TCms::OnDetach");

    Die(ctx);
}

void TCms::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, NKikimrServices::CMS, "OnTabletDead: %" PRIu64, TabletID());

    Die(ctx);
}

namespace {
    struct TNodeVDisksStatus {
        ui32 Up;
        ui32 Down;
        ui32 Restart;
    };

    void CalculateNodeVDisksStatus(const TClusterInfoPtr clusterInfo, const TNodeInfoPtr node,
                                   THashMap<ui32, TNodeVDisksStatus>& nodeVDisksStatus) {
        ui32 up = 0;
        ui32 down = 0;
        ui32 restart = 0;
        for (const auto& vdiskID : node->VDisks) {
            const auto& vdisk = clusterInfo->VDisk(vdiskID);
            switch (vdisk.State) {
                case NKikimrCms::EState::UNKNOWN:
                    break;
                case NKikimrCms::EState::DOWN:
                    ++down;
                    break;
                case NKikimrCms::EState::RESTART:
                    ++restart;
                    break;
                case NKikimrCms::EState::UP:
                    ++up;
                    break;
            }
        }
        nodeVDisksStatus[node->NodeId].Up = up;
        nodeVDisksStatus[node->NodeId].Down = down;
        nodeVDisksStatus[node->NodeId].Restart = restart;
    }
} // namespace

void TCms::GenerateNodeState(IOutputStream& out)
{
    THashMap<ui32, TNodeVDisksStatus> nodeVDisksStatusMap;

    ui32 totalVDisksUp = 0;
    ui32 totalVDisksRestart = 0;
    ui32 totalVDisksDown = 0;

    for (const auto& node: ClusterInfo->AllNodes()) {
        CalculateNodeVDisksStatus(ClusterInfo, node.second, nodeVDisksStatusMap);
        totalVDisksUp += nodeVDisksStatusMap[node.first].Up;
        totalVDisksDown += nodeVDisksStatusMap[node.first].Down;
        totalVDisksRestart += nodeVDisksStatusMap[node.first].Restart;
    }

    const auto& nodeState = ClusterInfo->ClusterNodes->GetNodeToState();
    HTML(out) {
        TAG(TH3) {
            out << "Nodes with state";
        }
        TAG(TH4) {
            out << "ClusterInfo last update timestamp: " << ClusterInfo->GetTimestamp();
        }
        TAG(TH4) {
            out << "Total VDisks State. UP: " << totalVDisksUp << ", Restart = " << totalVDisksRestart << ", Down = " << totalVDisksDown;
        }
        TABLE_SORTABLE() {
            TABLEHEAD() {
                TABLER() {
                    TABLED() {
                        out << "NodeID";
                    }
                    TABLED() {
                        out << "Host";
                    }
                    TABLED() {
                        out << "State";
                    }
                    TABLED() {
                        out << "InMemoryState";
                    }
                    TABLED() {
                        out << "Tenant";
                    }
                    TABLED() {
                        out << "VDisksUp";
                    }
                    TABLED() {
                        out << "VDisksDown";
                    }
                    TABLED() {
                        out << "VDisksRestart";
                    }
                }
            }
            TABLEBODY() {
                for (const auto& node : ClusterInfo->AllNodes()) {
                    auto currentInMemoryState = INodesChecker::NODE_STATE_UNSPECIFIED;
                    if (nodeState.contains(node.first)) {
                        currentInMemoryState = nodeState.at(node.first);
                    }
                    TABLER() {
                        TABLED() {
                            out << node.first;
                        }
                        TABLED() {
                            out << node.second->Host;
                        }
                        TABLED() {
                            out << node.second->State;
                        }
                        TABLED() {
                            out << currentInMemoryState;
                        }
                        TABLED() {
                            out << node.second->Tenant;
                        }
                        if (node.second->VDisks) {
                            TABLED() {
                                out << nodeVDisksStatusMap[node.first].Up;
                            }
                            TABLED() {
                                out << nodeVDisksStatusMap[node.first].Down;
                            }
                            TABLED() {
                                out << nodeVDisksStatusMap[node.first].Restart;
                            }
                        }
                    }
                }
            }
        }
    }
}

TString TCms::GenerateStat()
{
    TStringStream str;
    HTML(str) {
        TAG(TH2) { str << "Cluster management system tablet";}
        GenerateNodeState(str);
    }
    return str.Str();
}

bool TCms::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx)
{
    if (!ev) {
        return true;
    }

    ScheduleUpdateClusterInfo(ctx, true);

    TString str = GenerateStat();
    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(std::move(str)));
    return true;
}

void TCms::Enqueue(TAutoPtr<IEventHandle> &ev)
{
    InitQueue.push(ev);
}

void TCms::ProcessInitQueue(const TActorContext &ctx)
{
    while (!InitQueue.empty()) {
        TAutoPtr<IEventHandle> &ev = InitQueue.front();
        ctx.Send(ev.Release());
        InitQueue.pop();
    }
}

void TCms::SubscribeForConfig(const TActorContext &ctx)
{
    NConsole::SubscribeViaConfigDispatcher(ctx, {(ui32)NKikimrConsole::TConfigItem::CmsConfigItem, 
        (ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem}, ctx.SelfID);
}

void TCms::AdjustInfo(TClusterInfoPtr &info, const TActorContext &ctx) const
{
    for (const auto &entry : State->Permissions)
        info->AddLocks(entry.second, &ctx);
    for (const auto &entry : State->ScheduledRequests)
        info->ScheduleActions(entry.second, &ctx);
    for (const auto &entry : State->Notifications)
        info->AddExternalLocks(entry.second, &ctx);
    for (const auto &entry : State->HostMarkers)
        info->SetHostMarkers(entry.first, entry.second);
}

namespace {
    THashMap<NKikimrCms::TStatus::ECode, ui32> BuildCodesRateMap(std::initializer_list<NKikimrCms::TStatus::ECode> l) {
        ui32 nextCodeRate = 0;
        THashMap<NKikimrCms::TStatus::ECode, ui32> m;
        for (auto it = l.begin(); it != l.end(); ++it, ++nextCodeRate) {
            m[*it] = nextCodeRate;
        }
        return m;
    }
}

bool TCms::CheckPermissionRequest(const TPermissionRequest &request,
                                  TPermissionResponse &response,
                                  TPermissionRequest &scheduled,
                                  const TActorContext &ctx)
{
    static THashMap<EStatusCode, ui32> CodesRate = BuildCodesRateMap({
        TStatus::DISALLOW_TEMP,
        TStatus::ERROR_TEMP,
        TStatus::DISALLOW,
        TStatus::WRONG_REQUEST,
        TStatus::ERROR,
        TStatus::NO_SUCH_HOST,
        TStatus::NO_SUCH_DEVICE,
        TStatus::ALLOW_PARTIAL,
        TStatus::ALLOW,
        TStatus::UNKNOWN,
    });
    bool allowPartial = request.GetPartialPermissionAllowed();
    bool schedule = (request.GetSchedule() || request.GetEvictVDisks()) && !request.GetDryRun();

    if (request.GetEvictVDisks() && request.ActionsSize() > 1) {
        response.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
        response.MutableStatus()->SetReason("Cannot perform several actions and evict vdisks");
        return false;
    }

    response.MutableStatus()->SetCode(TStatus::ALLOW);
    if (schedule) {
        scheduled.SetUser(request.GetUser());
        scheduled.SetPartialPermissionAllowed(allowPartial);
        scheduled.SetSchedule(request.GetSchedule());
        scheduled.SetEvictVDisks(request.GetEvictVDisks());
        scheduled.SetReason(request.GetReason());
        if (request.HasDuration())
            scheduled.SetDuration(request.GetDuration());
        scheduled.SetTenantPolicy(request.GetTenantPolicy());
        scheduled.SetAvailabilityMode(request.GetAvailabilityMode());
        if (request.HasPriority()) {
            scheduled.SetPriority(request.GetPriority());
        }
    }

    LOG_INFO_S(ctx, NKikimrServices::CMS,
                "Check request: " << request.ShortDebugString());

    switch (request.GetAvailabilityMode()) {
    case MODE_MAX_AVAILABILITY:
    case MODE_KEEP_AVAILABLE:
    case MODE_FORCE_RESTART:
        break;
    default:
        response.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
        response.MutableStatus()
            ->SetReason(Sprintf("Unsupported availability mode: %s",
                                EAvailabilityMode_Name(request.GetAvailabilityMode()).data()));
        return false;
    };

    auto point = ClusterInfo->PushRollbackPoint();
    for (const auto &action : request.GetActions()) {
        TDuration permissionDuration = State->Config.DefaultPermissionDuration;
        if (request.HasDuration())
            permissionDuration = TDuration::MicroSeconds(request.GetDuration());
        if (action.HasDuration())
            permissionDuration = TDuration::MicroSeconds(action.GetDuration());

        TActionOptions opts(permissionDuration);
        opts.TenantPolicy = request.GetTenantPolicy();
        opts.AvailabilityMode = request.GetAvailabilityMode();
        opts.PartialPermissionAllowed = allowPartial;

        TErrorInfo error;

        LOG_DEBUG(ctx, NKikimrServices::CMS, "Checking action: %s", action.ShortDebugString().data());

        bool prepared = !request.GetEvictVDisks();
        if (!prepared) {
            prepared = CheckEvictVDisks(action, error);
        }

        if (prepared && CheckAction(action, opts, error, ctx)) {
            LOG_DEBUG(ctx, NKikimrServices::CMS, "Result: ALLOW");

            auto *permission = response.AddPermissions();
            permission->MutableAction()->CopyFrom(action);
            permission->SetDeadline(error.Deadline.GetValue());
            AddPermissionExtensions(action, *permission);

            ClusterInfo->AddTempLocks(action, &ctx);
        } else {
            LOG_DEBUG(ctx, NKikimrServices::CMS, "Result: %s (reason: %s)",
                      ToString(error.Code).data(), error.Reason.data());

            if (CodesRate[response.GetStatus().GetCode()] > CodesRate[error.Code]) {
                response.MutableStatus()->SetCode(error.Code);
                response.MutableStatus()->SetReason(error.Reason);
                if (error.Code == TStatus::DISALLOW_TEMP
                    || error.Code == TStatus::ERROR_TEMP)
                    response.SetDeadline(error.Deadline.GetValue());
            }

            if (!allowPartial)
                break;

            if (schedule)
                scheduled.AddActions()->CopyFrom(action);
        }
    }
    ClusterInfo->RollbackLocks(point);

    // Handle partial permission and reject cases. Partial permission requires
    // removal of rejected action status. Reject means we have to clear all
    // permissions.
    if (response.PermissionsSize() < request.ActionsSize()) {
        if (response.PermissionsSize() && allowPartial) {
            response.MutableStatus()->SetCode(TStatus::ALLOW_PARTIAL);
            response.MutableStatus()->ClearReason();
            response.ClearDeadline();
        } else {
            response.ClearPermissions();
        }
    }

    // Scheduled actions were collected in the actions check loop for partial
    // permissions only. Process other cases here: schedule all actions on
    // temporary errors and nothing on other errors.
    if (schedule && response.GetStatus().GetCode() != TStatus::ALLOW_PARTIAL) {
        if (response.GetStatus().GetCode() == TStatus::DISALLOW_TEMP
            || response.GetStatus().GetCode() == TStatus::ERROR_TEMP)
            scheduled.MutableActions()->CopyFrom(request.GetActions());
        else
            scheduled.ClearActions();
    }

    return response.GetStatus().GetCode() == TStatus::ALLOW
        || response.GetStatus().GetCode() == TStatus::ALLOW_PARTIAL;
}

bool TCms::IsActionHostValid(const TAction &action, TErrorInfo &error) const
{
    if (!ClusterInfo->HasNode(action.GetHost())
        && ActionRequiresHost(action)) {
        error.Code = TStatus::NO_SUCH_HOST;
        error.Reason = Sprintf("Unknown host '%s'", action.GetHost().data());
        return false;
    }

    if (ClusterInfo->HasNode(action.GetHost())
        && action.GetType() == TAction::ADD_HOST) {
        error.Code = TStatus::WRONG_REQUEST;
        error.Reason = Sprintf("Host '%s' already exists", action.GetHost().data());
        return false;
    }
#if 0
    if (ActionRequiresHost(action)
        && ClusterInfo->NodesCount(action.GetHost()) > 1) {
        error.Code = TStatus::WRONG_REQUEST;
        error.Reason = Sprintf("Multiple nodes on host '%s'", ~action.GetHost());
        return false;
    }
#endif
    return true;
}

bool TCms::ParseServices(const TAction &action, TServices &services, TErrorInfo &error) const {
    for (const auto &service : action.GetServices()) {
        EService value;
        if (!TryFromString(service, value)) {
            error.Code = TStatus::WRONG_REQUEST;
            error.Reason = Sprintf("Invalid service '%s' (supported services: %s)",
                                   service.data(), GetEnumAllNames<EService>().data());
            return false;
        }

        services |= value;
    }

    return true;
}

void TCms::AddPermissionExtensions(const TAction& action, TPermission& perm) const
{
    switch (action.GetType()) {
        case TAction::RESTART_SERVICES:
        case TAction::SHUTDOWN_HOST:
        case TAction::REBOOT_HOST:
            AddHostExtensions(action.GetHost(), perm);
            break;
        default:
            break;
    }
}

void TCms::AddHostExtensions(const TString& host, TPermission& perm) const
{
    auto * ext = perm.AddExtentions();
    ext->SetType(HostInfo);

    for (const TNodeInfo * node : ClusterInfo->HostNodes(host)) {
        auto * host = ext->AddHosts();
        host->SetName(node->Host);
        host->SetState(node->State);
        host->SetNodeId(node->NodeId);
        host->SetInterconnectPort(node->IcPort);
    }
}

bool TCms::CheckAccess(const TString &token,
                       TStatus::ECode &code,
                       TString &error,
                       const TActorContext &ctx)
{
    auto *appData = AppData(ctx);

    if (appData->AdministrationAllowedSIDs.empty())
        return true;

    if (token) {
        NACLib::TUserToken userToken(token);
        for (auto &sid : appData->AdministrationAllowedSIDs)
            if (userToken.IsExist(sid))
                return true;
    }

    code = TStatus::UNAUTHORIZED;
    error = "You don't have permission for this operation."
        " Contact service admin for cluster management operations.";

    return false;
}

bool TCms::CheckEvictVDisks(const TAction &action, TErrorInfo &error) const {
    if (!State->Sentinel) {
        error.Code = TStatus::ERROR;
        error.Reason = "Unable to evict vdisks while Sentinel (self heal) is disabled";
        return false;
    }

    switch (action.GetType()) {
        case TAction::RESTART_SERVICES:
        case TAction::SHUTDOWN_HOST:
        case TAction::REBOOT_HOST:
            break;
        default:
            error.Code = TStatus::WRONG_REQUEST;
            error.Reason = TStringBuilder() << "Unable to evict vdisks to perform action: " << action.GetType();
            return false;
    }

    for (const auto node : ClusterInfo->HostNodes(action.GetHost())) {
        if (!node->VDisks.empty()) {
            error.Code = TStatus::DISALLOW_TEMP;
            error.Reason = TStringBuilder() << "VDisks eviction from host " << action.GetHost() << " has not yet been completed";
            return false;
        }
    }

    return true;
}

bool TCms::CheckAction(const TAction &action, const TActionOptions &opts, TErrorInfo &error, const TActorContext &ctx) const {
    if (!IsActionHostValid(action, error))
        return false;

    switch (action.GetType()) {
        case TAction::RESTART_SERVICES:
            return CheckActionRestartServices(action, opts, error, ctx);
        case TAction::SHUTDOWN_HOST:
        case TAction::REBOOT_HOST:
            return CheckActionShutdownHost(action, opts, error, ctx);
        case TAction::REPLACE_DEVICES:
            return CheckActionReplaceDevices(action, opts.PermissionDuration, error);
        case TAction::START_SERVICES:
        case TAction::STOP_SERVICES:
        case TAction::ADD_HOST:
        case TAction::DECOMMISSION_HOST:
        case TAction::ADD_DEVICES:
        case TAction::REMOVE_DEVICES:
            error.Code = TStatus::ERROR;
            error.Reason = TStringBuilder() << "Unsupported action: " << action.GetType();
            return false;
        default:
            error.Code = TStatus::WRONG_REQUEST;
            error.Reason = TStringBuilder() << "Unknown action: " << static_cast<int>(action.GetType());
            return false;
    }
}

bool TCms::CheckActionShutdownNode(const NKikimrCms::TAction &action,
                                   const TActionOptions &opts,
                                   const TNodeInfo &node,
                                   TErrorInfo &error,
                                   const TActorContext &ctx) const
{
    if (!TryToLockNode(action, opts, node, error)) {
        return false;
    }

    if (!TryToLockVDisks(action, opts, node.VDisks, error)) {
        return false;
    }

    // node is not locked
    if (!TryToLockStateStorageReplica(action, opts, node, error, ctx)) {
        return false;
    }

    if (!AppData(ctx)->DisableCheckingSysNodesCms &&
        !CheckSysTabletsNode(opts, node, error)) {
        return false;
    }

    return true;
}

bool TCms::CheckActionRestartServices(const TAction &action,
                                      const TActionOptions &opts,
                                      TErrorInfo &error,
                                      const TActorContext &ctx) const
{
    TServices services;
    if (!ParseServices(action, services, error))
        return false;

    if (!services) {
        error.Code =  TStatus::WRONG_REQUEST;
        error.Reason = "Empty services list";
        return false;
    }

    bool found = false;
    for (const auto node : ClusterInfo->HostNodes(action.GetHost())) {
        if (node->Services & services) {
            found = true;
            if (!CheckActionShutdownNode(action, opts, *node, error, ctx)) {
                return false;
            }
        }
    }

    if (!found) {
        error.Code =  TStatus::NO_SUCH_SERVICE;
        error.Reason = Sprintf("No such services: %s on host %s",
            JoinSeq(", ", action.GetServices()).c_str(), action.GetHost().c_str());
        return false;
    }

    error.Deadline = TActivationContext::Now() + opts.PermissionDuration;
    return true;
}

bool TCms::CheckActionShutdownHost(const TAction &action,
                                   const TActionOptions &opts,
                                   TErrorInfo &error,
                                   const TActorContext &ctx) const
{
    const bool forciblyAllow = action.GetType() == TAction::REBOOT_HOST;
    for (const auto node : ClusterInfo->HostNodes(action.GetHost())) {
        if (!CheckActionShutdownNode(action, opts, *node, error, ctx)) {
            if (forciblyAllow && node->State == DOWN) {
                continue;
            }
            return false;
        }
    }

    error.Deadline = TActivationContext::Now() + opts.PermissionDuration;
    return true;
}

bool TCms::TryToLockStateStorageReplica(const TAction& action,
                                        const TActionOptions& opts,
                                        const TNodeInfo& node,
                                        TErrorInfo& error,
                                        const TActorContext &ctx) const
{
    TInstant defaultDeadline = TActivationContext::Now() + State->Config.DefaultRetryTime;

    if (!ClusterInfo->IsStateStorageinfoReceived()) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = "Did not received state storage configuration";
        error.Deadline = defaultDeadline;
        return false;
    }

    if (!ClusterInfo->IsStateStorageReplicaNode(node.NodeId)) {
        return true;
    }

    const ui32 nToSelect = ClusterInfo->StateStorageInfo->NToSelect;
    const ui32 currentRing = ClusterInfo->GetRingId(node.NodeId);
    ui8 currentRingState = TStateStorageRingInfo::Unknown;
    ui32 restartRings = 0;
    ui32 lockedRings = 0;
    ui32 disabledRings = 0;
    auto now = AppData(ctx)->TimeProvider->Now();
    TDuration duration = TDuration::MicroSeconds(action.GetDuration()) + opts.PermissionDuration;
    for (auto ringInfo : ClusterInfo->StateStorageRings) {
        auto state = ringInfo->CountState(now, State->Config.DefaultRetryTime, duration);
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS, "Ring: " << ringInfo->RingId
                                                                 << "; State: " << TStateStorageRingInfo::RingStateToString(state));

        if (ringInfo->RingId == currentRing) {
            if (state == TStateStorageRingInfo::Disabled) {
                return true;
            }
            currentRingState = state;
            continue;
        }
        switch (state) {
            case TStateStorageRingInfo::Ok:
                break;
            case TStateStorageRingInfo::Locked:
                ++lockedRings;
                break;
            case TStateStorageRingInfo::Restart:
                ++restartRings;
                break;
            case TStateStorageRingInfo::Disabled:
                ++disabledRings;
                break;
            default:
                break;
        }
    }
    Y_ABORT_UNLESS(currentRingState != TStateStorageRingInfo::Unknown);

    // Add current ring to restart rings
    ++restartRings;

    switch (opts.AvailabilityMode) {
        case MODE_MAX_AVAILABILITY:
            if (restartRings + lockedRings > 1) {
                error.Code = TStatus::DISALLOW_TEMP;
                error.Reason = TStringBuilder() << "Too many unavailable state storage rings"
                                                << ". Restarting rings: "
                                                    << (currentRingState == TStateStorageRingInfo::Restart ? restartRings : restartRings - 1)
                                                << ". Temporary (for a 2 minutes) locked rings: "
                                                    << (currentRingState == TStateStorageRingInfo::Locked ? lockedRings + 1 : lockedRings)
                                                << ". Maximum allowed number of unavailable rings for this mode: " << 1;
                error.Deadline = defaultDeadline;
                return false;
            }
            break;
        case MODE_KEEP_AVAILABLE:
            if (restartRings + lockedRings + disabledRings > (nToSelect - 1) / 2) {
                error.Code = TStatus::DISALLOW_TEMP;
                error.Reason = TStringBuilder() << "Too many unavailable state storage rings"
                                                << ". Restarting rings: "
                                                    << (currentRingState == TStateStorageRingInfo::Restart ? restartRings : restartRings - 1)
                                                << ". Temporary (for a 2 minutes) locked rings: "
                                                    << (currentRingState == TStateStorageRingInfo::Locked ? lockedRings + 1 : lockedRings)
                                                << ". Disabled rings: " << disabledRings
                                                << ". Maximum allowed number of unavailable rings for this mode: " << (nToSelect - 1) / 2;
                error.Deadline = defaultDeadline;
                return false;
            }
            break;
        case MODE_FORCE_RESTART:
            break;
        default:
            error.Code = TStatus::WRONG_REQUEST;
            error.Reason = Sprintf("Unknown availability mode: %s (%" PRIu32 ")",
                                   EAvailabilityMode_Name(opts.AvailabilityMode).data(),
                                   static_cast<ui32>(opts.AvailabilityMode));
            error.Deadline = defaultDeadline;
            return false;
    }

    return true;
}

bool TCms::CheckSysTabletsNode(const TActionOptions &opts,
                               const TNodeInfo &node,
                               TErrorInfo &error) const
{
    if (node.Services & EService::DynamicNode || node.PDisks.size()) {
        return true;
    }

    for (auto &tabletType : ClusterInfo->NodeToTabletTypes[node.NodeId]) {
        if (!ClusterInfo->SysNodesCheckers[tabletType]->TryToLockNode(node.NodeId, opts.AvailabilityMode, error.Reason)) {
            error.Code = TStatus::DISALLOW_TEMP;
            error.Deadline = TActivationContext::Now() + State->Config.DefaultRetryTime;
            return false;
        }
    }

    return true;
}

bool TCms::TryToLockNode(const TAction& action,
                         const TActionOptions& opts,
                         const TNodeInfo& node,
                         TErrorInfo& error) const
{
    TDuration duration = TDuration::MicroSeconds(action.GetDuration());
    duration += opts.PermissionDuration;

    if (!ClusterInfo->ClusterNodes->TryToLockNode(node.NodeId, opts.AvailabilityMode, error.Reason)) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Deadline = TActivationContext::Now() + State->Config.DefaultRetryTime;
        return false;
    }

    if (node.Tenant
        && opts.TenantPolicy != NONE
        && !ClusterInfo->TenantNodesChecker[node.Tenant]->TryToLockNode(node.NodeId, opts.AvailabilityMode, error.Reason))
    {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Deadline = TActivationContext::Now() + State->Config.DefaultRetryTime;
        return false;
    }

    return true;
}

bool TCms::TryToLockPDisk(const TAction &action,
                          const TActionOptions& opts,
                          const TPDiskInfo &pdisk,
                          TErrorInfo &error) const
{
    if (!TryToLockVDisks(action, opts, pdisk.VDisks, error))
        return false;

    error.Deadline = TActivationContext::Now() + opts.PermissionDuration;
    return true;
}

bool TCms::TryToLockVDisks(const TAction &action,
                           const TActionOptions& opts,
                           const TSet<TVDiskID> &vdisks,
                           TErrorInfo &error) const
{
    TDuration duration = TDuration::MicroSeconds(action.GetDuration());
    duration += opts.PermissionDuration;

    auto res = true;
    auto point = ClusterInfo->PushRollbackPoint();
    for (const auto &vdId : vdisks) {
        const auto &vdisk = ClusterInfo->VDisk(vdId);
        if (TryToLockVDisk(opts, vdisk, duration, error)) {
            ClusterInfo->AddVDiskTempLock(vdId, action);
        } else {
            res = false;
            break;
        }
    }
    ClusterInfo->RollbackLocks(point);

    return res;
}

bool TCms::TryToLockVDisk(const TActionOptions& opts,
                          const TVDiskInfo &vdisk,
                          TDuration duration,
                          TErrorInfo &error) const
{
    if (vdisk.IsLocked(error, State->Config.DefaultRetryTime, TActivationContext::Now(), duration))
       return false;

    if (vdisk.NodeId
        && ClusterInfo->Node(vdisk.NodeId)
        .IsLocked(error, State->Config.DefaultRetryTime, TActivationContext::Now(), duration))
        return false;

    if (vdisk.PDiskId
        && ClusterInfo->PDisk(vdisk.PDiskId)
        .IsLocked(error, State->Config.DefaultRetryTime, TActivationContext::Now(), duration))
        return false;

    for (auto groupId : vdisk.BSGroups) {
        const auto &group = ClusterInfo->BSGroup(groupId);
        TInstant defaultDeadline = TActivationContext::Now() + State->Config.DefaultRetryTime;

        if (group.Erasure.GetErasure() == TErasureType::ErasureSpeciesCount) {
            error.Code = TStatus::ERROR;
            error.Reason = Sprintf("Affected group %u has unknown erasure type", groupId);
            error.Deadline = defaultDeadline;
            return false;
        }

        if (opts.AvailabilityMode != MODE_FORCE_RESTART
            && !group.Erasure.ParityParts()) {
            error.Code = TStatus::DISALLOW;
            error.Reason = Sprintf("Affected group %u has no parity parts", groupId);
            error.Deadline = defaultDeadline;
            return false;
        }

        auto counters = CreateErasureCounter(ClusterInfo->BSGroup(groupId).Erasure.GetErasure(), vdisk, groupId);
        counters->CountGroupState(ClusterInfo, State->Config.DefaultRetryTime, duration, error);

        switch (opts.AvailabilityMode) {
        case MODE_MAX_AVAILABILITY:
            if (!counters->CheckForMaxAvailability(ClusterInfo, error, defaultDeadline, opts.PartialPermissionAllowed)) {
                return false;
            }
            break;
        case MODE_KEEP_AVAILABLE:
            if (!counters->CheckForKeepAvailability(ClusterInfo, error, defaultDeadline, opts.PartialPermissionAllowed)) {
                return false;
            }
            break;
        case MODE_FORCE_RESTART:
            if ( counters->GroupAlreadyHasLockedDisks() && opts.PartialPermissionAllowed) {
                error.Code = TStatus::DISALLOW_TEMP;
                error.Reason = "You cannot get two or more disks from the same group at the same time"
                               " without specifying the PartialPermissionAllowed parameter";
                error.Deadline = defaultDeadline;
                return false;
            }
            // Any number of down disks is OK for this mode.
            break;
        default:
            error.Code = TStatus::WRONG_REQUEST;
            error.Reason = Sprintf("Unknown availability mode: %s (%" PRIu32 ")",
                                   EAvailabilityMode_Name(opts.AvailabilityMode).data(),
                                   static_cast<ui32>(opts.AvailabilityMode));
            error.Deadline = defaultDeadline;
            return false;
        }
    }

    return true;
}

bool TCms::CheckActionReplaceDevices(const TAction &action,
                                     const TActionOptions &opts,
                                     TErrorInfo &error) const
{
    auto point = ClusterInfo->PushRollbackPoint();
    bool res = true;
    TDuration duration = TDuration::MicroSeconds(action.GetDuration());
    duration += opts.PermissionDuration;

    for (const auto &device : action.GetDevices()) {
        if (ClusterInfo->HasPDisk(device)) {
            const auto &pdisk = ClusterInfo->PDisk(device);
            if (TryToLockPDisk(action, opts, pdisk, error))
                ClusterInfo->AddPDiskTempLock(pdisk.PDiskId, action);
            else {
                res = false;
                break;
            }
        } else if (ClusterInfo->HasPDisk(action.GetHost(), device)) {
            const auto &pdisk = ClusterInfo->PDisk(action.GetHost(), device);
            if (TryToLockPDisk(action, opts, pdisk, error))
                ClusterInfo->AddPDiskTempLock(pdisk.PDiskId, action);
            else {
                res = false;
                break;
            }
        } else if (ClusterInfo->HasVDisk(device)) {
            const auto &vdisk = ClusterInfo->VDisk(device);
            if (TryToLockVDisk(opts, vdisk, duration, error))
                ClusterInfo->AddVDiskTempLock(vdisk.VDiskId, action);
            else {
                res = false;
                break;
            }
        } else {
            error.Code = TStatus::NO_SUCH_DEVICE;
            error.Reason = Sprintf("Unknown device %s (use cluster state command"
                                   " to get list of known devices)", device.data());
            res = false;
        }
    }
    ClusterInfo->RollbackLocks(point);

    if (res)
        error.Deadline = TActivationContext::Now() + opts.PermissionDuration;

    return res;
}

void TCms::AcceptPermissions(TPermissionResponse &resp, const TString &requestId,
                             const TString &owner, const TActorContext &ctx, bool check)
{
    auto acceptTaskPermission = [](auto &tasks, auto &requests, const TString &requestId, const TString &permissionId) {
        auto reqIt = requests.find(requestId);
        if (reqIt == requests.end()) {
            return;
        }

        auto taskIt = tasks.find(reqIt->second);
        if (taskIt == tasks.end()) {
            return;
        }

        taskIt->second.Permissions.insert(permissionId);
    };

    for (size_t i = 0; i < resp.PermissionsSize(); ++i) {
        auto &permission = *resp.MutablePermissions(i);
        permission.SetId(owner + "-p-" + ToString(State->NextPermissionId++));
        State->Permissions.emplace(permission.GetId(), TPermissionInfo(permission, requestId, owner));
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS, "Accepting permission"
            << ": id# " << permission.GetId()
            << ", requestId# " << requestId
            << ", owner# " << owner);
        ClusterInfo->AddLocks(permission, requestId, owner, &ctx);

        if (!check) {
            continue;
        }

        acceptTaskPermission(State->WalleTasks, State->WalleRequests, requestId, permission.GetId());
        acceptTaskPermission(State->MaintenanceTasks, State->MaintenanceRequests, requestId, permission.GetId());
    }
}

void TCms::ScheduleUpdateClusterInfo(const TActorContext &ctx, bool now)
{
    ctx.Schedule(now ? TDuration::Zero() : TDuration::Minutes(1),
                 new TEvPrivate::TEvUpdateClusterInfo());
}

void TCms::ScheduleCleanup(TInstant time, const TActorContext &ctx)
{
    // Don't schedule event in the past or earlier then already
    // scheduled one. Also limit events rate.
    auto now = ctx.Now();
    time = Max(time, now + TDuration::Seconds(1));
    if (!ScheduledCleanups.empty()
        && ScheduledCleanups.top() <= (time + TDuration::Seconds(1)))
        return;

    LOG_DEBUG_S(ctx, NKikimrServices::CMS, "Schedule cleanup at " << time);

    ScheduledCleanups.push(time);
    ctx.Schedule(time - now, new TEvPrivate::TEvCleanupExpired);

}

void TCms::SchedulePermissionsCleanup(const TActorContext &ctx)
{
    if (State->Permissions.empty())
        return;

    TInstant earliest = TInstant::Max();
    for (const auto &entry : State->Permissions) {
        const TDuration duration = TDuration::MicroSeconds(entry.second.Action.GetDuration());
        const TDuration doubleDuration = ((TDuration::Max() / 2) >= duration ? (2 * duration) : TDuration::Max());
        const TInstant deadline = entry.second.Deadline;
        earliest = Min(earliest, deadline + doubleDuration);
    }

    ScheduleCleanup(earliest, ctx);
}

void TCms::ScheduleNotificationsCleanup(const TActorContext &ctx)
{
    if (State->Notifications.empty())
        return;

    TInstant earliest = TInstant::Max();
    for (const auto &entry : State->Notifications) {
        TInstant start = TInstant::MicroSeconds(entry.second.Notification.GetTime());
        for (const auto &action : entry.second.Notification.GetActions()) {
            TDuration duration = TDuration::MicroSeconds(action.GetDuration());
            Y_ABORT_UNLESS(duration);
            earliest = Min(earliest, start + duration);
        }
    }

    ScheduleCleanup(earliest, ctx);
}

void TCms::CleanupLog(const TActorContext &ctx)
{
    Execute(CreateTxLogCleanup(), ctx);
}

void TCms::ScheduleLogCleanup(const TActorContext &ctx)
{
    LogCleanupTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
    CreateLongTimer(ctx, TDuration::Minutes(10),
                    new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvPrivate::TEvCleanupLog),
                    AppData(ctx)->SystemPoolId,
                    LogCleanupTimerCookieHolder.Get());
}

void TCms::CleanupExpired(const TActorContext &ctx)
{
    DoPermissionsCleanup(ctx);
    Execute(CreateTxRemoveExpiredNotifications(), ctx);

    SchedulePermissionsCleanup(ctx);
    ScheduleNotificationsCleanup(ctx);
}

void TCms::DoPermissionsCleanup(const TActorContext &ctx)
{
    ScheduledCleanups.pop();

    TVector<TString> ids;
    auto now = ctx.Now();
    for (const auto &entry : State->Permissions) {
        const TDuration duration = TDuration::MicroSeconds(entry.second.Action.GetDuration());
        const TDuration doubleDuration = ((TDuration::Max() / 2) >= duration ? (2 * duration) : TDuration::Max());
        const TInstant deadline(entry.second.Deadline);
        if ((deadline + doubleDuration) <= now)
            ids.push_back(entry.first);
    }

    Execute(CreateTxRemovePermissions(std::move(ids), nullptr, nullptr, true), ctx);
}

void TCms::CleanupWalleTasks(const TActorContext &ctx)
{
    LOG_DEBUG_S(ctx, NKikimrServices::CMS, "Running CleanupWalleTasks");

    // Wall-E tasks are updated separately from its request and
    // permissions which means we might have some Wall-E requests
    // not attached to Wall-E tasks and Wall-E tasks with no
    // request and permissions. Cleanup the mess here.
    TVector<TString> requestsToRemove;
    for (const auto &entry : State->ScheduledRequests) {
        const auto &request = entry.second;
        if (request.Owner == WALLE_CMS_USER
            && !State->WalleRequests.contains(request.RequestId))
            requestsToRemove.push_back(request.RequestId);
    }

    for (const auto &requestId : requestsToRemove) {
        Execute(CreateTxRemoveRequest(requestId, nullptr, nullptr), ctx);
    }

    TVector<TString> permissionsToRemove;
    for (const auto &entry : State->Permissions) {
        const auto &permission = entry.second;
        if (permission.Owner == WALLE_CMS_USER
            && !State->WalleRequests.contains(permission.RequestId))
            permissionsToRemove.push_back(permission.PermissionId);
    }

    if (!permissionsToRemove.empty())
        Execute(CreateTxRemovePermissions(permissionsToRemove, nullptr, nullptr), ctx);

    RemoveEmptyTasks(ctx);

    WalleCleanupTimerCookieHolder.Reset(ISchedulerCookie::Make2Way());
    CreateLongTimer(ctx, State->Config.DefaultWalleCleanupPeriod,
                    new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvPrivate::TEvCleanupWalle),
                    AppData(ctx)->SystemPoolId,
                    WalleCleanupTimerCookieHolder.Get());
}

TVector<TString> TCms::FindEmptyTasks(const THashMap<TString, TTaskInfo> &tasks, const TActorContext &ctx)
{
    TVector<TString> tasksToRemove;
    for (const auto &entry : tasks) {
        const auto &task = entry.second;
        if (!State->ScheduledRequests.contains(task.RequestId) && task.Permissions.empty()) {
            LOG_DEBUG(ctx, NKikimrServices::CMS, "Found empty task %s", task.TaskId.data());
            tasksToRemove.push_back(task.TaskId);
        }
    }

    return tasksToRemove;
}

void TCms::RemoveEmptyTasks(const TActorContext &ctx)
{
    for (auto &id : FindEmptyTasks(State->WalleTasks, ctx))
        Execute(CreateTxRemoveWalleTask(id), ctx);
    for (auto &id : FindEmptyTasks(State->MaintenanceTasks, ctx))
        Execute(CreateTxRemoveMaintenanceTask(id), ctx);
}

void TCms::Cleanup(const TActorContext &ctx)
{
    LOG_DEBUG(ctx, NKikimrServices::CMS, "TCms::Cleanup");

    NConsole::UnsubscribeViaConfigDispatcher(ctx, ctx.SelfID);

    if (State->Sentinel)
        ctx.Send(State->Sentinel, new TEvents::TEvPoisonPill);
}

void TCms::Die(const TActorContext& ctx)
{
    Cleanup(ctx);
    TActorBase::Die(ctx);
}

void TCms::AddHostState(const TClusterInfoPtr &clusterInfo, const TNodeInfo &node, TClusterStateResponse &resp, TInstant timestamp)
{
    auto *host = resp.MutableState()->AddHosts();
    host->SetName(node.Host);
    host->SetState(node.State);
    host->SetNodeId(node.NodeId);
    host->SetInterconnectPort(node.IcPort);
    host->SetTimestamp(timestamp.GetValue());
    host->SetStartTimeSeconds(node.StartTime.Seconds());
    node.Location.Serialize(host->MutableLocation(), false);
    for (auto marker : node.Markers) {
        host->AddMarkers(marker);
    }
    if (node.State == UP || node.VDisks || node.PDisks) {
        for (const auto flag : GetEnumAllValues<EService>()) {
            if (!(node.Services & flag)) {
                continue;
            }

            auto* service = host->AddServices();
            service->SetName(ToString(flag));
            service->SetState(node.State);
            if (node.State == UP) {
                service->SetVersion(node.Version);
            }
            service->SetTimestamp(timestamp.GetValue());
        }

        for (const auto &vdId : node.VDisks) {
            const auto &vdisk = clusterInfo->VDisk(vdId);
            auto *device = host->AddDevices();
            device->SetName(vdisk.GetDeviceName());
            device->SetState(vdisk.State);
            device->SetTimestamp(timestamp.GetValue());
            for (auto marker : vdisk.Markers) {
                device->AddMarkers(marker);
            }
        }

        for (const auto &pdId : node.PDisks) {
            const auto &pdisk = clusterInfo->PDisk(pdId);
            auto *device = host->AddDevices();
            device->SetName(pdisk.GetDeviceName());
            device->SetState(pdisk.State);
            device->SetTimestamp(timestamp.GetValue());
            for (auto marker : pdisk.Markers) {
                device->AddMarkers(marker);
            }
        }
    }
}

void TCms::GetPermission(TEvCms::TEvManagePermissionRequest::TPtr &ev, bool all, const TActorContext &ctx)
{
    TAutoPtr<TEvCms::TEvManagePermissionResponse> resp = new TEvCms::TEvManagePermissionResponse;
    const auto &rec = ev->Get()->Record;
    const TString &user = rec.GetUser();

    LOG_INFO(ctx, NKikimrServices::CMS, "Get %s permissions for %s",
              all ? "all" : "selected", user.data());

    resp->Record.MutableStatus()->SetCode(TStatus::OK);
    if (all) {
        for (const auto &entry : State->Permissions) {
            const auto &permission = entry.second;
            if (permission.Owner == user)
                permission.CopyTo(*resp->Record.AddPermissions());
        }
    } else {
        for (const auto &id : rec.GetPermissions()) {
            auto it = State->Permissions.find(id);
            if (it == State->Permissions.end()) {
                resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
                resp->Record.MutableStatus()->SetReason("Unknown permission " + id);
                resp->Record.ClearPermissions();
                break;
            }

            const auto &permission = it->second;
            if (permission.Owner != user) {
                resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
                resp->Record.MutableStatus()->SetReason(Sprintf("Permission %s doesn't belong to %s", id.data(), user.data()));
                resp->Record.ClearPermissions();
                break;
            }

            permission.CopyTo(*resp->Record.AddPermissions());
        }
    }

    LOG_DEBUG(ctx, NKikimrServices::CMS, "Resulting status: %s %s",
              TStatus::ECode_Name(resp->Record.GetStatus().GetCode()).data(), resp->Record.GetStatus().GetReason().data());

    Reply(ev, std::move(resp), ctx);
}

void TCms::RemovePermission(TEvCms::TEvManagePermissionRequest::TPtr &ev, bool done, const TActorContext &ctx)
{
    TAutoPtr<TEvCms::TEvManagePermissionResponse> resp = new TEvCms::TEvManagePermissionResponse;
    const auto &rec = ev->Get()->Record;
    const TString &user = rec.GetUser();

    LOG_INFO(ctx, NKikimrServices::CMS, "User %s %s permissions %s",
              user.data(), done ? "is done with" : "rejected", ToString(rec.GetPermissions()).data());

    TVector<TString> ids;
    resp->Record.MutableStatus()->SetCode(TStatus::OK);
    for (const auto &id : rec.GetPermissions()) {
        auto it = State->Permissions.find(id);
        if (it == State->Permissions.end()) {
            resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
            resp->Record.MutableStatus()->SetReason("Unknown permission " + id);
            break;
        }

        const auto &permission = it->second;
        if (permission.Owner != user) {
            resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
            resp->Record.MutableStatus()->SetReason(Sprintf("Permission %s doesn't belong to %s", id.data(), user.data()));
            break;
        }

        ids.push_back(id);
    }

    LOG_DEBUG(ctx, NKikimrServices::CMS, "Resulting status: %s %s",
              TStatus::ECode_Name(resp->Record.GetStatus().GetCode()).data(), resp->Record.GetStatus().GetReason().data());

    if (!rec.GetDryRun() && resp->Record.GetStatus().GetCode() == TStatus::OK) {
        auto handle = new IEventHandle(ev->Sender, SelfId(), resp.Release(), 0, ev->Cookie);
        Execute(CreateTxRemovePermissions(std::move(ids), std::move(ev->Release()), handle), ctx);
    } else {
        Reply(ev, std::move(resp), ctx);
    }
}

void TCms::GetRequest(TEvCms::TEvManageRequestRequest::TPtr &ev, bool all, const TActorContext &ctx)
{
    TAutoPtr<TEvCms::TEvManageRequestResponse> resp = new TEvCms::TEvManageRequestResponse;
    const auto &rec = ev->Get()->Record;
    const TString &user = rec.GetUser();

    LOG_INFO(ctx, NKikimrServices::CMS, "Get %s requests for %s",
              all ? "all" : "selected", user.data());

    resp->Record.MutableStatus()->SetCode(TStatus::OK);
    if (all) {
        for (const auto &entry : State->ScheduledRequests) {
            const auto &request = entry.second;
            if (request.Owner == user)
                request.CopyTo(*resp->Record.AddRequests());
        }
    } else {
        auto &id = rec.GetRequestId();
        auto it = State->ScheduledRequests.find(id);
        if (it == State->ScheduledRequests.end()) {
            resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
            resp->Record.MutableStatus()->SetReason("Unknown request " + id);
        } else {
            const auto &request = it->second;
            if (request.Owner != user) {
                resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
                resp->Record.MutableStatus()->SetReason(Sprintf("Request %s doesn't belong to %s", id.data(), user.data()));
            } else
                request.CopyTo(*resp->Record.AddRequests());
        }
    }

    LOG_DEBUG(ctx, NKikimrServices::CMS, "Resulting status: %s %s",
              TStatus::ECode_Name(resp->Record.GetStatus().GetCode()).data(), resp->Record.GetStatus().GetReason().data());

    Reply(ev, std::move(resp), ctx);
}

void TCms::RemoveRequest(TEvCms::TEvManageRequestRequest::TPtr &ev, const TActorContext &ctx)
{
    TAutoPtr<TEvCms::TEvManageRequestResponse> resp = new TEvCms::TEvManageRequestResponse;
    const auto &rec = ev->Get()->Record;
    const TString &user = rec.GetUser();
    const TString &id = rec.GetRequestId();

    LOG_INFO(ctx, NKikimrServices::CMS, "User %s removes request %s", user.data(), id.data());

    resp->Record.MutableStatus()->SetCode(TStatus::OK);
    auto it = State->ScheduledRequests.find(id);
    if (it == State->ScheduledRequests.end()) {
        resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
        resp->Record.MutableStatus()->SetReason("Unknown request " + id);
    } else {
        const auto &request = it->second;

        if (request.Owner != user) {
            resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
            resp->Record.MutableStatus()->SetReason(Sprintf("Request %s doesn't belong to %s", id.data(), user.data()));
        }

        if (request.Request.GetEvictVDisks() && request.Request.ActionsSize() < 1) {
            resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
            resp->Record.MutableStatus()->SetReason(
                Sprintf("Request %s used to evict vdisks and cannot be deleted while permission is valid", id.data()));
        }
    }

    LOG_DEBUG(ctx, NKikimrServices::CMS, "Resulting status: %s %s",
              TStatus::ECode_Name(resp->Record.GetStatus().GetCode()).data(), resp->Record.GetStatus().GetReason().data());

    if (!rec.GetDryRun() && resp->Record.GetStatus().GetCode() == TStatus::OK) {
        auto handle = new IEventHandle(ev->Sender, SelfId(), resp.Release(), 0, ev->Cookie);
        Execute(CreateTxRemoveRequest(id, std::move(ev->Release()), handle), ctx);
    } else {
        Reply(ev, std::move(resp), ctx);
    }
}

void TCms::GetNotifications(TEvCms::TEvManageNotificationRequest::TPtr &ev, bool all,
                            const TActorContext &ctx)
{
    TAutoPtr<TEvCms::TEvManageNotificationResponse> resp
        = new TEvCms::TEvManageNotificationResponse;
    const auto &rec = ev->Get()->Record;
    const TString &user = rec.GetUser();

    LOG_INFO(ctx, NKikimrServices::CMS, "Get %s notifications for %s",
              all ? "all" : "selected", user.data());

    resp->Record.MutableStatus()->SetCode(TStatus::OK);
    if (all) {
        for (const auto &entry : State->Notifications) {
            const auto &notification = entry.second;
            if (notification.Owner == user)
                notification.CopyTo(*resp->Record.AddNotifications());
        }
    } else {
        auto &id = rec.GetNotificationId();
        auto it = State->Notifications.find(id);
        if (it == State->Notifications.end()) {
            resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
            resp->Record.MutableStatus()->SetReason("Unknown notification " + id);
        } else {
            const auto &notification = it->second;
            if (notification.Owner != user) {
                resp->Record.MutableStatus()->SetCode(TStatus::WRONG_REQUEST);
                resp->Record.MutableStatus()->SetReason(Sprintf("Notification %s doesn't belong to %s", id.data(), user.data()));
            } else
                notification.CopyTo(*resp->Record.AddNotifications());
        }
    }

    LOG_DEBUG(ctx, NKikimrServices::CMS, "Resulting status: %s %s",
              ToString(resp->Record.GetStatus().GetCode()).data(), resp->Record.GetStatus().GetReason().data());

    Reply(ev, std::move(resp), ctx);
}

bool TCms::RemoveNotification(const TString &id, const TString &user, bool remove, TErrorInfo &error)
{
    auto it = State->Notifications.find(id);
    if (it == State->Notifications.end()) {
        error.Code = TStatus::WRONG_REQUEST;
        error.Reason = "Unknown notification " + id;
        return false;
    }

    const auto &notification = it->second;
    if (notification.Owner != user) {
        error.Code = TStatus::WRONG_REQUEST;
        error.Reason = Sprintf("Notification %s doesn't belong to %s", id.data(), user.data());
        return false;
    }

    if (remove)
        State->Notifications.erase(it);

    return true;
}

void TCms::EnqueueRequest(TAutoPtr<IEventHandle> ev, const TActorContext &ctx)
{
    if (Queue.empty() && NextQueue.empty()) {
        ctx.Schedule(TDuration::MilliSeconds(100), new TEvPrivate::TEvStartCollecting);
    }

    NextQueue.emplace(ev, ctx.Now());
    TabletCounters->Simple()[COUNTER_REQUESTS_QUEUE_SIZE].Add(1);
}

void TCms::StartCollecting()
{
    if (!Queue.empty()) {
        return;
    }

    std::swap(NextQueue, Queue);

    InfoCollectorStartTime = TActivationContext::Now();

    auto collector = CreateInfoCollector(SelfId(), State->Config.InfoCollectionTimeout);
    Register(collector);
}

void TCms::CheckAndEnqueueRequest(TEvCms::TEvPermissionRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    if (!rec.GetUser()) {
        return ReplyWithError<TEvCms::TEvPermissionResponse>(
            ev, TStatus::WRONG_REQUEST, "Missing user in request", ctx);
    }

    if (rec.HasMaintenanceTaskId()) {
        if (State->MaintenanceTasks.contains(rec.GetMaintenanceTaskId())) {
            return ReplyWithError<TEvCms::TEvPermissionResponse>(
                ev, TStatus::WRONG_REQUEST, "Maintenance task already exists", ctx);
        }
    }

    if (rec.HasPriority() && !EnableCMSRequestPriorities) {
        if (rec.GetUser() == WALLE_CMS_USER) {
            rec.ClearPriority();
        } else {
            return ReplyWithError<TEvCms::TEvPermissionResponse>(
                ev, TStatus::WRONG_REQUEST, "Unsupported: feature flag EnableCMSRequestPriorities is off", ctx);
        }
    }

    if (-100 > rec.GetPriority() || rec.GetPriority() > 100) {
        return ReplyWithError<TEvCms::TEvPermissionResponse>(
            ev, TStatus::WRONG_REQUEST, "Priority value is out of range", ctx);
    }

    EnqueueRequest(ev.Release(), ctx);
}

void TCms::CheckAndEnqueueRequest(TEvCms::TEvCheckRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    if (!rec.GetUser()) {
        return ReplyWithError<TEvCms::TEvPermissionResponse>(
            ev, TStatus::WRONG_REQUEST, "Missing user in request", ctx);
    }

    auto it = State->ScheduledRequests.find(rec.GetRequestId());
    if (it == State->ScheduledRequests.end()) {
        auto reason = Sprintf("Unknown request %s", rec.GetRequestId().data());
        return ReplyWithError<TEvCms::TEvPermissionResponse>(ev, TStatus::WRONG_REQUEST, reason, ctx);
    }

    if (it->second.Owner != rec.GetUser()) {
        auto reason = Sprintf("Request %s doesn't belong to %s", rec.GetRequestId().data(), rec.GetUser().data());
        return ReplyWithError<TEvCms::TEvPermissionResponse>(ev, TStatus::WRONG_REQUEST, reason, ctx);
    }

    EnqueueRequest(ev.Release(), ctx);
}

void TCms::CheckAndEnqueueRequest(TEvCms::TEvConditionalPermissionRequest::TPtr &ev, const TActorContext &ctx)
{
    ReplyWithError<TEvCms::TEvPermissionResponse>(ev, TStatus::ERROR, "Not supported", ctx);
}

void TCms::CheckAndEnqueueRequest(TEvCms::TEvNotification::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    if (!rec.GetUser()) {
        return ReplyWithError<TEvCms::TEvNotificationResponse>(
            ev, TStatus::WRONG_REQUEST, "Missing user in request", ctx);
    }

    EnqueueRequest(ev.Release(), ctx);
}

void TCms::PersistNodeTenants(TTransactionContext& txc, const TActorContext& ctx)
{
    NIceDb::TNiceDb db(txc.DB);

    for (const auto& pr : ClusterInfo->AllNodes()) {
        ui32 nodeId = pr.second->NodeId;
        TString tenant = pr.second->Tenant;

        auto row = db.Table<Schema::NodeTenant>().Key(nodeId);
        row.Update(NIceDb::TUpdate<Schema::NodeTenant::Tenant>(tenant));

        LOG_TRACE(ctx, NKikimrServices::CMS,
                  "Persist node %" PRIu32 " tenant '%s'",
                  nodeId, tenant.data());
    }
}

TVector<TCms::THostMarkers> TCms::SetHostMarker(const TString &host, NKikimrCms::EMarker marker, TTransactionContext &txc, const TActorContext &ctx) {
    if (State->HostMarkers.contains(host)) {
        return {};
    }

    AuditLog(ctx, TStringBuilder() << "Add host marker"
        << ": host# " << host
        << ", marker# " << marker);

    State->HostMarkers[host] = {marker};
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::HostMarkers>().Key(host).Update(
        NIceDb::TUpdate<Schema::HostMarkers::Markers>(TVector<NKikimrCms::EMarker>{marker})
    );

    TVector<TCms::THostMarkers> updateMarkers;
    if (ClusterInfo) {
        for (const auto node : ClusterInfo->HostNodes(host)) {
            updateMarkers.push_back({
                .NodeId = node->NodeId,
                .Markers = {marker},
            });
        }
    }

    return updateMarkers;
}

TVector<TCms::THostMarkers> TCms::ResetHostMarkers(const TString &host, TTransactionContext &txc, const TActorContext &ctx) {
    if (!State->HostMarkers.contains(host)) {
        return {};
    }

    AuditLog(ctx, TStringBuilder() << "Reset host markers"
        << ": host# " << host);

    State->HostMarkers.erase(host);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::HostMarkers>().Key(host).Delete();

    TVector<TCms::THostMarkers> updateMarkers;
    if (ClusterInfo) {
        for (const auto node : ClusterInfo->HostNodes(host)) {
            updateMarkers.push_back({
                .NodeId = node->NodeId,
                .Markers = {},
            });
        }
    }

    return updateMarkers;
}

void TCms::SentinelUpdateHostMarkers(TVector<TCms::THostMarkers> &&updateMarkers, const TActorContext &ctx) {
    if (updateMarkers) {
        ctx.Send(State->Sentinel, new TEvSentinel::TEvUpdateHostMarkers(std::move(updateMarkers)));
    }
}

void TCms::ProcessQueue()
{
    // To avoid getting stuck in the processing queue for too long,
    // we'll process queue by one.
    if (!Queue.empty()) {
        TabletCounters->Percentile()[COUNTER_LATENCY_REQUEST_QUEUING].IncrementFor((TActivationContext::Now() - Queue.front().ArrivedTime).MilliSeconds());
        TabletCounters->Simple()[COUNTER_REQUESTS_QUEUE_SIZE].Sub(1);

        ProcessRequest(Queue.front().Request);
        Queue.pop();
    }

    if (!Queue.empty()) {
        Send(SelfId(), new TEvPrivate::TEvProcessQueue);
    } else if (!NextQueue.empty()) {
        // Process events received while collecting and processing queue
        StartCollecting();
    }
}

void TCms::ProcessRequest(TAutoPtr<IEventHandle> &ev)
{
    TRACE_EVENT(NKikimrServices::CMS);
    switch (ev->GetTypeRewrite()) {
        HFuncTraced(TEvPrivate::TEvUpdateClusterInfo, Handle);
        HFuncTraced(TEvCms::TEvClusterStateRequest, Handle);
        HFuncTraced(TEvCms::TEvPermissionRequest, Handle);
        HFuncTraced(TEvCms::TEvCheckRequest, Handle);
        HFuncTraced(TEvCms::TEvNotification, Handle);
        HFuncTraced(TEvCms::TEvResetMarkerRequest, Handle);
        HFuncTraced(TEvCms::TEvSetMarkerRequest, Handle);
        HFuncTraced(TEvCms::TEvGetClusterInfoRequest, Handle);

    default:
        Y_ABORT("Unexpected request type");
    }
}

void TCms::OnBSCPipeDestroyed(const TActorContext &ctx)
{
    LOG_WARN(ctx, NKikimrServices::CMS, "BS Controller connection error");

    if (State->BSControllerPipe) {
        NTabletPipe::CloseClient(ctx, State->BSControllerPipe);
        State->BSControllerPipe = TActorId();
    }

    if (State->Sentinel)
        ctx.Send(State->Sentinel, new TEvSentinel::TEvBSCPipeDisconnected);
}

void TCms::Handle(TEvCms::TEvGetClusterInfoRequest::TPtr &ev, const TActorContext &ctx) {
    TAutoPtr<TEvCms::TEvGetClusterInfoResponse> resp = new TEvCms::TEvGetClusterInfoResponse;
    resp->Info = ClusterInfo;

    ctx.Send(ev->Sender, resp.Release());
}

void TCms::Handle(TEvPrivate::TEvClusterInfo::TPtr &ev, const TActorContext &ctx)
{
    TabletCounters->Percentile()[COUNTER_LATENCY_INFO_COLLECTOR].IncrementFor((ctx.Now() - InfoCollectorStartTime).MilliSeconds());

    if (!ev->Get()->Success) {
        LOG_NOTICE_S(ctx, NKikimrServices::CMS,
                     "Couldn't collect cluster state.");

        if (!ClusterInfo) {
            State->ClusterInfo = new TClusterInfo;
            ClusterInfo = State->ClusterInfo;
        }

        ClusterInfo->SetOutdated(true);
        ProcessQueue();
        return;
    }

    auto info = ev->Get()->Info;
    info->SetOutdated(false);

    if (ClusterInfo) {
        info->MigrateOldInfo(ClusterInfo);
    } else {
        info->ApplyDowntimes(State->Downtimes);
    }

    // We need to generate NodeCheckers after MigrateOldInfo to get
    // all the information about the tenants on the disconnected nodes
    info->GenerateTenantNodesCheckers();

    if (!AppData(ctx)->DisableCheckingSysNodesCms)
        info->GenerateSysTabletsNodesCheckers();

    AdjustInfo(info, ctx);

    State->ClusterInfo = info;
    ClusterInfo = info;

    ui32 tenantLimit = State->Config.TenantLimits.GetDisabledNodesLimit();
    ui32 tenantRatioLimit = State->Config.TenantLimits.GetDisabledNodesRatioLimit();
    ui32 clusterLimit = State->Config.ClusterLimits.GetDisabledNodesLimit();
    ui32 clusterRatioLimit = State->Config.ClusterLimits.GetDisabledNodesRatioLimit();
    ClusterInfo->ApplyNodeLimits(clusterLimit, clusterRatioLimit, tenantLimit, tenantRatioLimit);

    ClusterInfo->UpdateDowntimes(State->Downtimes, ctx);
    Execute(CreateTxUpdateDowntimes(), ctx);

    if (State->InitialNodeTenants) {
        ClusterInfo->ApplyInitialNodeTenants(ctx, State->InitialNodeTenants);
        State->InitialNodeTenants.clear();
    }

    if (State->Config.SentinelConfig.Enable && !State->Sentinel)
        State->Sentinel = RegisterWithSameMailbox(CreateSentinel(State));

    info->DebugDump(ctx);

    TabletCounters->Simple()[COUNTER_BOOTSTRAP_DIFFERS].Set(ClusterInfo->IsLocalBootConfDiffersFromConsole);

    ProcessQueue();
}

void TCms::Handle(TEvPrivate::TEvLogAndSend::TPtr &ev, const TActorContext &ctx)
{
    Execute(CreateTxLogAndSend(ev), ctx);
}

void TCms::Handle(TEvPrivate::TEvUpdateClusterInfo::TPtr &/*ev*/, const TActorContext &ctx)
{
    if (State->ClusterInfo->IsOutdated()) {
        ScheduleUpdateClusterInfo(ctx);
    }
}

void TCms::Handle(TEvCms::TEvManageRequestRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    if (!rec.GetUser()) {
        return ReplyWithError<TEvCms::TEvManageRequestResponse>(
            ev, TStatus::WRONG_REQUEST, "Missing user in request", ctx);
    }

    switch (rec.GetCommand()) {
    case TManageRequestRequest::LIST:
        GetRequest(ev, true, ctx);
        return;
    case TManageRequestRequest::GET:
        GetRequest(ev, false, ctx);
        return;
    case TManageRequestRequest::REJECT:
        RemoveRequest(ev, ctx);
        return;
    default:
        return ReplyWithError<TEvCms::TEvManageRequestResponse>(
            ev, TStatus::WRONG_REQUEST, "Unknown command", ctx);
    }
}

void TCms::Handle(TEvCms::TEvManagePermissionRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    if (!rec.GetUser()) {
        return ReplyWithError<TEvCms::TEvManagePermissionResponse>(
            ev, TStatus::WRONG_REQUEST, "Missing user in request", ctx);
    }

    switch (rec.GetCommand()) {
    case TManagePermissionRequest::LIST:
        GetPermission(ev, true, ctx);
        return;
    case TManagePermissionRequest::GET:
        GetPermission(ev, false, ctx);
        return;
    case TManagePermissionRequest::DONE:
        RemovePermission(ev, true, ctx);
        return;
    case TManagePermissionRequest::REJECT:
        RemovePermission(ev, false, ctx);
        return;
    case TManagePermissionRequest::EXTEND:
        return ReplyWithError<TEvCms::TEvManagePermissionResponse>(
            ev, TStatus::ERROR, "Not supported", ctx);
    default:
        return ReplyWithError<TEvCms::TEvManagePermissionResponse>(
            ev, TStatus::WRONG_REQUEST, "Unknown command", ctx);
    }
}

void TCms::Handle(TEvCms::TEvClusterStateRequest::TPtr &ev,
                  const TActorContext &ctx)
{
    if (ClusterInfo->IsOutdated()) {
        return ReplyWithError<TEvCms::TEvClusterStateResponse>(
            ev, TStatus::ERROR_TEMP, "Cannot collect cluster state", ctx);
    }

    TAutoPtr<TEvCms::TEvClusterStateResponse> resp = new TEvCms::TEvClusterStateResponse;

    auto &rec = ev->Get()->Record;
    if (rec.HostsSize() > 0) {
        for (const auto &host : rec.GetHosts()) {
            if (ClusterInfo->NodesCount(host) >= 1) {
                for (const TNodeInfo *node : ClusterInfo->HostNodes(host)) {
                    AddHostState(ClusterInfo, *node, resp->Record, ClusterInfo->GetTimestamp());
                }
            } else {
                return ReplyWithError<TEvCms::TEvClusterStateResponse>(
                    ev, TStatus::NO_SUCH_HOST, "Unknown host " + host, ctx);
            }
        }
    } else {
        for (const auto &entry : ClusterInfo->AllNodes())
            AddHostState(ClusterInfo, *entry.second, resp->Record, ClusterInfo->GetTimestamp());
    }

    resp->Record.MutableStatus()->SetCode(TStatus::OK);
    resp->Record.MutableState()->SetTimestamp(ClusterInfo->GetTimestamp().GetValue());

    Reply(ev, std::move(resp), ctx);
}

void TCms::Handle(TEvCms::TEvPermissionRequest::TPtr &ev,
                  const TActorContext &ctx)
{
    if (ClusterInfo->IsOutdated()) {
        return ReplyWithError<TEvCms::TEvPermissionResponse>(
            ev, TStatus::ERROR_TEMP, "Cannot collect cluster state", ctx);
    }

    TAutoPtr<TEvCms::TEvPermissionResponse> resp = new TEvCms::TEvPermissionResponse;
    TRequestInfo scheduled;
    auto &rec = ev->Get()->Record;

    auto requestStartTime = TInstant::Now();

    auto actions(std::move(*rec.MutableActions()));
    rec.ClearActions();

    THashSet<TString> hostNames;
    for (const auto &action : actions) {
        if (action.HasTenant()) {
            for (const TNodeInfo * node : ClusterInfo->TenantNodes(action.GetTenant())) {
                if (!hostNames.insert(node->Host).second) {
                    continue;
                }

                TAction &hostAction = *rec.MutableActions()->Add();
                hostAction.CopyFrom(action);
                hostAction.ClearTenant();
                hostAction.SetHost(node->Host);
            }
        } else {
            rec.MutableActions()->Add()->CopyFrom(action);
        }
    }

    if (rec.GetEvictVDisks()) {
        for (const auto &action : rec.GetActions()) {
            if (State->HostMarkers.contains(action.GetHost())) {
                return ReplyWithError<TEvCms::TEvPermissionResponse>(
                    ev, TStatus::WRONG_REQUEST, TStringBuilder() << "VDisks of host '" << action.GetHost() << "' are being evicted", ctx);
            }
            for (const auto node : ClusterInfo->HostNodes(action.GetHost())) {
                if (State->HostMarkers.contains(ToString(node->NodeId))) {
                    return ReplyWithError<TEvCms::TEvPermissionResponse>(
                        ev, TStatus::WRONG_REQUEST, TStringBuilder() << "VDisks of node '" << node->NodeId << "' are being evicted", ctx);
                }
            }
        }
    }

    ClusterInfo->LogManager.PushRollbackPoint();
    const i32 priority = rec.GetPriority();
    for (const auto &[_, scheduledRequest] : State->ScheduledRequests) {
        if (scheduledRequest.Priority < priority) {
            for (const auto &action : scheduledRequest.Request.GetActions()) {
                ClusterInfo->LogManager.ApplyAction(action, ClusterInfo);
            }
        }
    }
    ClusterInfo->DeactivateScheduledLocks(priority);
    bool ok = CheckPermissionRequest(rec, resp->Record, scheduled.Request, ctx);
    ClusterInfo->ReactivateScheduledLocks();
    ClusterInfo->LogManager.RollbackOperations();

    // Schedule request if required.
    if (rec.GetDryRun()) {
        Reply(ev, std::move(resp), ctx);
    } else {
        TString user = rec.GetUser();
        auto reqId = user + "-r-" + ToString(State->NextRequestId++);
        resp->Record.SetRequestId(reqId);

        TAutoPtr<TRequestInfo> copy;
        if (scheduled.Request.ActionsSize() || scheduled.Request.GetEvictVDisks()) {
            scheduled.Owner = user;
            scheduled.Order = State->NextRequestId - 1;
            scheduled.Priority = priority;
            scheduled.RequestId = reqId;
            ClusterInfo->ScheduleActions(scheduled, &ctx);

            copy = new TRequestInfo(scheduled);
            State->ScheduledRequests.emplace(reqId, std::move(scheduled));
        } else if (user == WALLE_CMS_USER || rec.HasMaintenanceTaskId()) {
            scheduled.Owner = user;
            scheduled.RequestId = reqId;

            copy = new TRequestInfo(scheduled);
        }

        if (ok)
            AcceptPermissions(resp->Record, reqId, user, ctx);

        TMaybe<TString> maintenanceTaskId;
        if (rec.HasMaintenanceTaskId()) {
            maintenanceTaskId.ConstructInPlace(rec.GetMaintenanceTaskId());
        }

        auto handle = new IEventHandle(ev->Sender, SelfId(), resp.Release(), 0, ev->Cookie);
        Execute(CreateTxStorePermissions(std::move(ev->Release()), handle, user, std::move(copy), maintenanceTaskId), ctx);
    }

    TabletCounters->Percentile()[COUNTER_LATENCY_PERMISSION_REQUEST].IncrementFor((TInstant::Now() - requestStartTime).MilliSeconds());
}

void TCms::Handle(TEvCms::TEvCheckRequest::TPtr &ev, const TActorContext &ctx)
{
    if (ClusterInfo->IsOutdated()) {
        return ReplyWithError<TEvCms::TEvPermissionResponse>(
            ev, TStatus::ERROR_TEMP, "Cannot collect cluster state", ctx);
    }

    auto &rec = ev->Get()->Record;
    auto it = State->ScheduledRequests.find(rec.GetRequestId());

    // Have to check request existence again because it could be
    // deleted after previous event check.
    if (it == State->ScheduledRequests.end()) {
        auto reason = Sprintf("Unknown request %s", rec.GetRequestId().data());
        return ReplyWithError<TEvCms::TEvPermissionResponse>(
            ev, TStatus::WRONG_REQUEST, reason, ctx);
    }

    TString user = rec.GetUser();
    auto &request = it->second;
    TAutoPtr<TEvCms::TEvPermissionResponse> resp = new TEvCms::TEvPermissionResponse;
    TRequestInfo scheduled;

    auto requestStartTime = TInstant::Now();

    ClusterInfo->LogManager.PushRollbackPoint();
    for (const auto &scheduled_request : State->ScheduledRequests) {
        if (scheduled_request.second.Priority < request.Priority) {
            for (const auto &action : scheduled_request.second.Request.GetActions())
                ClusterInfo->LogManager.ApplyAction(action, ClusterInfo);
        }
    }

    ClusterInfo->DeactivateScheduledLocks(request.Priority);
    request.Request.SetAvailabilityMode(rec.GetAvailabilityMode());
    bool ok = CheckPermissionRequest(request.Request, resp->Record, scheduled.Request, ctx);
    ClusterInfo->ReactivateScheduledLocks();
    ClusterInfo->LogManager.RollbackOperations();

    // Schedule request if required.
    if (rec.GetDryRun()) {
        Reply(ev, std::move(resp), ctx);
    } else {
        TAutoPtr<TRequestInfo> copy;
        auto order = request.Order;
        auto priority = request.Priority;

        ClusterInfo->UnscheduleActions(request.RequestId);
        State->ScheduledRequests.erase(it);
        if (scheduled.Request.ActionsSize() || scheduled.Request.GetEvictVDisks()) {
            scheduled.Owner = user;
            scheduled.Order = order;
            scheduled.Priority = priority;
            scheduled.RequestId = rec.GetRequestId();
            resp->Record.SetRequestId(scheduled.RequestId);

            ClusterInfo->ScheduleActions(scheduled, &ctx);
            
            copy = new TRequestInfo(scheduled);
            State->ScheduledRequests.emplace(rec.GetRequestId(), std::move(scheduled));
        } else {
            scheduled.RequestId = rec.GetRequestId();
            scheduled.Owner = user;
            copy = new TRequestInfo(scheduled);
        }

        if (ok)
            AcceptPermissions(resp->Record, rec.GetRequestId(), user, ctx, true);

        auto handle = new IEventHandle(ev->Sender, SelfId(), resp.Release(), 0, ev->Cookie);
        Execute(CreateTxStorePermissions(std::move(ev->Release()), handle, user, std::move(copy)), ctx);
    }

    TabletCounters->Percentile()[COUNTER_LATENCY_CHECK_REQUEST].IncrementFor((TInstant::Now() - requestStartTime).MilliSeconds());
}

bool TCms::CheckNotificationDeadline(const TAction &action, TInstant time,
                                     TErrorInfo &error, const TActorContext &ctx) const
{
    if (time + TDuration::MicroSeconds(action.GetDuration()) < ctx.Now()) {
        error.Code =  TStatus::WRONG_REQUEST;
        error.Reason = "Action already finished";
        return false;
    }

    return true;
}

bool TCms::CheckNotificationRestartServices(const TAction &action, TInstant time,
                                            TErrorInfo &error, const TActorContext &ctx) const
{
    TServices services;
    if (!ParseServices(action, services, error))
        return false;

    if (!services) {
        error.Code =  TStatus::WRONG_REQUEST;
        error.Reason = "Empty services list";
        return false;
    }

    if (!CheckNotificationDeadline(action, time, error, ctx))
        return false;

    return true;
}

bool TCms::CheckNotificationShutdownHost(const TAction &action, TInstant time,
                                         TErrorInfo &error, const TActorContext &ctx) const
{
    if (!CheckNotificationDeadline(action, time, error, ctx))
        return false;

    return true;
}

bool TCms::CheckNotificationReplaceDevices(const TAction &action, TInstant time,
                                           TErrorInfo &error, const TActorContext &ctx) const
{
    for (const auto &device : action.GetDevices()) {
        if (!ClusterInfo->HasPDisk(device)
                && !ClusterInfo->HasPDisk(action.GetHost(), device)
                && !ClusterInfo->HasVDisk(device)) {
            error.Code = TStatus::NO_SUCH_DEVICE;
            error.Reason = Sprintf("Unknown device %s (use cluster state command"
                                   " to get list of known devices)", device.data());
            return false;
        }
    }

    if (!CheckNotificationDeadline(action, time, error, ctx))
        return false;

    return true;
}

bool TCms::IsValidNotificationAction(const TAction &action, TInstant time,
                                     TErrorInfo &error, const TActorContext &ctx) const
{
    if (!IsActionHostValid(action, error))
        return false;

    switch (action.GetType()) {
        case TAction::RESTART_SERVICES:
            return CheckNotificationRestartServices(action, time, error, ctx);
        case TAction::SHUTDOWN_HOST:
        case TAction::REBOOT_HOST:
            return CheckNotificationShutdownHost(action, time, error, ctx);
        case TAction::REPLACE_DEVICES:
            return CheckNotificationReplaceDevices(action, time, error, ctx);
        case TAction::START_SERVICES:
        case TAction::STOP_SERVICES:
        case TAction::ADD_HOST:
        case TAction::DECOMMISSION_HOST:
        case TAction::ADD_DEVICES:
        case TAction::REMOVE_DEVICES:
            error.Code = TStatus::ERROR;
            error.Reason = TStringBuilder() << "Unsupported action: " << action.GetType();
            return false;
        default:
            error.Code = TStatus::WRONG_REQUEST;
            error.Reason = TStringBuilder() << "Unknown action: " << static_cast<int>(action.GetType());
            return false;
    }
}

TString TCms::AcceptNotification(const TNotification &notification,
                                 const TActorContext &ctx)
{
    TString id = notification.GetUser() + "-n-" + ToString(State->NextNotificationId++);
    TNotificationInfo info;

    info.NotificationId = id;
    info.Owner = notification.GetUser();
    info.Notification = notification;

    ClusterInfo->AddExternalLocks(info, &ctx);
    State->Notifications.emplace(id, std::move(info));

    return id;
}

bool TCms::CheckNotification(const TNotification &notification,
                             TNotificationResponse &resp,
                             const TActorContext &ctx) const
{
    TInstant time = TInstant::MicroSeconds(notification.GetTime());

    resp.MutableStatus()->SetCode(TStatus::OK);
    for (const auto &action : notification.GetActions()) {
        TErrorInfo error;

        LOG_DEBUG(ctx, NKikimrServices::CMS, "Processing notification for action: %s",
                  action.ShortDebugString().data());

        if (!IsValidNotificationAction(action, time, error, ctx)) {
            resp.MutableStatus()->SetCode(error.Code);
            resp.MutableStatus()->SetReason(error.Reason);
            break;
        }
    }

    return resp.GetStatus().GetCode() == TStatus::OK;
}

void TCms::Handle(TEvCms::TEvNotification::TPtr &ev, const TActorContext &ctx)
{
    if (ClusterInfo->IsOutdated()) {
        return ReplyWithError<TEvCms::TEvNotificationResponse>(
            ev, TStatus::ERROR_TEMP, "Cannot collect cluster state", ctx);
    }

    Execute(CreateTxProcessNotification(ev), ctx);
}

void TCms::Handle(TEvCms::TEvManageNotificationRequest::TPtr &ev, const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;

    LOG_INFO(ctx, NKikimrServices::CMS, "Notification management request: %s",
              rec.ShortDebugString().data());

    if (!rec.GetUser()) {
        return ReplyWithError<TEvCms::TEvManageNotificationResponse>(
            ev, TStatus::WRONG_REQUEST, "Missing user in request", ctx);
    }

    switch (rec.GetCommand()) {
    case TManageNotificationRequest::LIST:
        GetNotifications(ev, true, ctx);
        return;
    case TManageNotificationRequest::GET:
        GetNotifications(ev, false, ctx);
        return;
    case TManageNotificationRequest::REJECT:
        Execute(CreateTxRejectNotification(ev), ctx);
        return;
    default:
        return ReplyWithError<TEvCms::TEvManageNotificationResponse>(
            ev, TStatus::WRONG_REQUEST, "Unknown command", ctx);
    }
}

void TCms::Handle(TEvCms::TEvWalleCreateTaskRequest::TPtr &ev, const TActorContext &ctx)
{
    auto adapter = CreateWalleAdapter(ev, SelfId());
    ctx.RegisterWithSameMailbox(adapter);
}

void TCms::Handle(TEvCms::TEvWalleListTasksRequest::TPtr &ev, const TActorContext &ctx)
{
    auto adapter = CreateWalleAdapter(ev, State);
    ctx.RegisterWithSameMailbox(adapter);
}

void TCms::Handle(TEvCms::TEvWalleCheckTaskRequest::TPtr &ev, const TActorContext &ctx)
{
    auto adapter = CreateWalleAdapter(ev, State, SelfId());
    ctx.RegisterWithSameMailbox(adapter);
}

void TCms::Handle(TEvCms::TEvWalleRemoveTaskRequest::TPtr &ev, const TActorContext &ctx)
{
    auto adapter = CreateWalleAdapter(ev, State, SelfId());
    ctx.RegisterWithSameMailbox(adapter);
}

void TCms::Handle(TEvCms::TEvStoreWalleTask::TPtr &ev, const TActorContext &ctx)
{
    auto event = ev->Get();

    auto handle = new IEventHandle(ev->Sender, SelfId(), new TEvCms::TEvWalleTaskStored(event->Task.TaskId), 0, ev->Cookie);
    Execute(CreateTxStoreWalleTask(event->Task, std::move(ev->Release()), handle), ctx);
}

void TCms::Handle(TEvCms::TEvRemoveWalleTask::TPtr &ev, const TActorContext &ctx)
{
    TString id = ev->Get()->TaskId;
    TAutoPtr<TEvCms::TEvWalleTaskRemoved> resp = new TEvCms::TEvWalleTaskRemoved(id);

    if (State->WalleTasks.contains(id)) {
        auto &task = State->WalleTasks.find(id)->second;
        auto handle = new IEventHandle(ev->Sender, SelfId(), resp.Release(), 0, ev->Cookie);
        if (State->ScheduledRequests.contains(task.RequestId)) {
            Execute(CreateTxRemoveRequest(task.RequestId, std::move(ev->Release()), handle), ctx);
        } else {
            TVector<TString> ids(task.Permissions.begin(), task.Permissions.end());
            Execute(CreateTxRemovePermissions(ids, std::move(ev->Release()), handle), ctx);
        }
    } else {
        Reply(ev, std::move(resp), ctx);
    }
}

void TCms::Handle(TEvCms::TEvGetConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    TAutoPtr<TEvCms::TEvGetConfigResponse> response
        = new TEvCms::TEvGetConfigResponse;
    State->Config.Serialize(*response->Record.MutableConfig());
    response->Record.MutableStatus()->SetCode(TStatus::OK);

    Reply(ev, std::move(response), ctx);
}

void TCms::Handle(TEvCms::TEvSetConfigRequest::TPtr &ev, const TActorContext &ctx)
{
    Execute(CreateTxUpdateConfig(ev), ctx);
}

void TCms::Handle(TEvCms::TEvResetMarkerRequest::TPtr &ev, const TActorContext &ctx)
{
    return ReplyWithError<TEvCms::TEvResetMarkerResponse>(
        ev, TStatus::ERROR, "Unsupported action", ctx);
}

void TCms::Handle(TEvCms::TEvSetMarkerRequest::TPtr &ev, const TActorContext &ctx)
{
    return ReplyWithError<TEvCms::TEvSetMarkerResponse>(
        ev, TStatus::ERROR, "Unsupported action", ctx);
}

void TCms::Handle(TEvCms::TEvGetLogTailRequest::TPtr &ev, const TActorContext &ctx)
{
    Execute(CreateTxGetLogTail(ev), ctx);
}

void TCms::Handle(TEvCms::TEvGetSentinelStateRequest::TPtr &ev, const TActorContext &ctx)
{
    if (State->Sentinel) {
        ctx.Send(ev->Forward(State->Sentinel));
    } else {
        auto Response = MakeHolder<TEvCms::TEvGetSentinelStateResponse>();
        auto &rec = Response->Record;
        rec.MutableStatus()->SetCode(NKikimrCms::TStatus::ERROR);
        ctx.Send(ev->Sender, Response.Release());
    }
}

void TCms::Handle(TEvConsole::TEvConfigNotificationRequest::TPtr &ev,
                  const TActorContext &ctx)
{   
    const auto& appConfig = ev->Get()->Record.GetConfig();
    if (appConfig.HasFeatureFlags()) {
        EnableCMSRequestPriorities = appConfig.GetFeatureFlags().GetEnableCMSRequestPriorities();
    }

    if (ev->Get()->Record.HasLocal() && ev->Get()->Record.GetLocal()) {
        Execute(CreateTxUpdateConfig(ev), ctx);
    } else {
        // ignore and immediately ack messages from old persistent console subscriptions
        auto response = MakeHolder<TEvConsole::TEvConfigNotificationResponse>();
        response->Record.MutableConfigId()->CopyFrom(ev->Get()->Record.GetConfigId());
        ctx.Send(ev->Sender, response.Release(), 0, ev->Cookie);
    }
}

void TCms::Handle(TEvConsole::TEvReplaceConfigSubscriptionsResponse::TPtr &ev,
                  const TActorContext &ctx)
{
    auto &rec = ev->Get()->Record;
    if (rec.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::CMS,
                    "Cannot subscribe for config updates: " << rec.GetStatus().GetCode()
                    << " " << rec.GetStatus().GetReason());

        SubscribeForConfig(ctx);
        return;
    }

    ConfigSubscriptionId = rec.GetSubscriptionId();

    LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                "Got config subscription id=" << ConfigSubscriptionId);
}

void TCms::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev,
                  const TActorContext &ctx)
{
    auto *msg = ev->Get();
    if (msg->ClientId == State->BSControllerPipe)
        OnBSCPipeDestroyed(ctx);
}

void TCms::Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev,
                  const TActorContext &ctx)
{
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();
    if (msg->ClientId == State->BSControllerPipe && msg->Status != NKikimrProto::OK)
        OnBSCPipeDestroyed(ctx);
}

IActor *CreateCms(const TActorId &tablet, TTabletStorageInfo *info)
{
    return new TCms(tablet, info);
}

} // namespace NKikimr::NCms
