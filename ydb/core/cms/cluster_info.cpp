#include "cluster_info.h"
#include "cms_state.h"
#include "node_checkers.h"

#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

#if defined BLOG_D || defined BLOG_I || defined BLOG_ERROR
#error log macro definition clash
#endif

#define BLOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::CMS, stream)
#define BLOG_ERROR(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::CMS, stream)

namespace NKikimr::NCms {

using namespace NNodeWhiteboard;
using namespace NKikimrCms;

bool TLockableItem::IsLocked(TErrorInfo &error, TDuration defaultRetryTime,
                             TInstant now, TDuration duration) const
{
    if (State == RESTART) {
        Y_ABORT_UNLESS(Lock.Defined());
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = Sprintf("%s is restarting (permission %s owned by %s)",
                               PrettyItemName().data(), Lock->PermissionId.data(), Lock->Owner.data());
        error.Deadline = Lock->ActionDeadline;
        return true;
    }

    if (Lock.Defined()) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = Sprintf("%s has planned shutdown (permission %s owned by %s)",
                               PrettyItemName().data(), Lock->PermissionId.data(), Lock->Owner.data());
        error.Deadline = Lock->ActionDeadline;
        return true;
    }

    for (auto &lock : ExternalLocks) {
        // External locks are sorted by start time.
        if (lock.LockStart > now + duration)
            break;

        if (lock.LockDeadline < now)
            continue;

        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = Sprintf("%s has planned shutdown (notification %s owned by %s)",
                               PrettyItemName().data(), lock.NotificationId.data(), lock.Owner.data());
        error.Deadline = lock.LockDeadline;
        return true;
    }

    if (!ScheduledLocks.empty() && ScheduledLocks.begin()->Priority < DeactivatedLocksPriority) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = Sprintf("%s has scheduled action %s owned by %s (priority %" PRIi32 " vs %" PRIi32 ")",
                               PrettyItemName().data(), ScheduledLocks.begin()->RequestId.data(),
                               ScheduledLocks.begin()->Owner.data(), ScheduledLocks.begin()->Priority,
                               DeactivatedLocksPriority);
        error.Deadline = now + defaultRetryTime;
        return true;
    }

    if (!TempLocks.empty()) {
        error.Code = TStatus::DISALLOW;
        error.Reason = Sprintf("%s has temporary lock", PrettyItemName().data());
        error.Deadline = now + defaultRetryTime;
        error.RollbackPoint = TempLocks.back().RollbackPoint;
        return true;
    }

    return false;
}

bool TLockableItem::IsDown(TErrorInfo &error, TInstant defaultDeadline) const
{
    if (State == RESTART) {
        Y_ABORT_UNLESS(Lock.Defined());
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = Sprintf("%s is restarting (permission %s owned by %s)",
                               PrettyItemName().data(), Lock->PermissionId.data(), Lock->Owner.data());
        error.Deadline = Lock->ActionDeadline;
        return true;
    }

    if (State != UP) {
        error.Code = TStatus::DISALLOW_TEMP;
        error.Reason = Sprintf("%s is down", PrettyItemName().data());
        error.Deadline = defaultDeadline;
        return true;
    }

    return false;
}

void TLockableItem::RollbackLocks(ui64 point)
{
    for (auto it = TempLocks.begin(); it != TempLocks.end(); ++it)
        if (it->RollbackPoint >= point) {
            TempLocks.erase(it, TempLocks.end());
            break;
        }
}

void TLockableItem::ReactivateScheduledLocks()
{
    DeactivatedLocksPriority = Max<i32>();
}

void TLockableItem::DeactivateScheduledLocks(i32 priority)
{
    DeactivatedLocksPriority = priority;
}

void TLockableItem::RemoveScheduledLocks(const TString &requestId)
{
    ScheduledLocks.remove_if([&requestId](auto &lock) {
            return lock.RequestId == requestId;
        });
}

void TLockableItem::MigrateOldInfo(const TLockableItem &old)
{
    Downtime = old.Downtime;
    if (State != NKikimrCms::UP)
        Downtime.AddDowntime(old.Timestamp, Timestamp, "known downtime");
    Downtime.CleanupOldSegments(Timestamp);
}

void TLockableItem::DebugLocksDump(IOutputStream &ss, const TString &prefix) const
{
    if (Lock.Defined())
        ss << prefix << "Locked by permission " << Lock->PermissionId << Endl;

    for (auto &lock : ExternalLocks)
        ss << prefix << "External lock by notifications " << lock.NotificationId;

    for (auto &lock : ScheduledLocks)
        ss << prefix << "Scheduled lock by request " << lock.RequestId;

    for (auto &lock : TempLocks)
        ss << prefix << "Temporary lock at point " << lock.RollbackPoint;
}

void TNodeInfo::MigrateOldInfo(const TLockableItem &old)
{
    TLockableItem::MigrateOldInfo(old);
    if (auto * oldNode = dynamic_cast<const TNodeInfo *>(&old)) {
        if (oldNode->State == UP) {
            PreviousTenant = oldNode->Tenant;
        } else {
            PreviousTenant = oldNode->PreviousTenant;
        }

        if (!HasTenantInfo || State != UP)
            Tenant = PreviousTenant;
        HasTenantInfo = true;
    }
}

TString TPDiskInfo::ItemName() const
{
    return Sprintf("PDisk %s", PDiskId.ToString().data());
}

TString TPDiskInfo::PrettyItemName() const
{
    TStringBuilder name;

    name << ItemName();
    if (Host || Path) {
        name << " (";

        if (Host) {
            name << Host;
        }
        if (Path) {
            name << ":" << Path;
        }

        name << ")";
    }

    return name;
}

TString TPDiskInfo::GetDeviceName() const
{
    return Sprintf("pdisk-%" PRIu32 "-%" PRIu32, PDiskId.NodeId, PDiskId.DiskId);
}

bool TPDiskInfo::NameToId(const TString &name, TPDiskID &id)
{
    int size;

    if (sscanf(name.data(), "pdisk-%" SCNu32 "-%" SCNu32 "%n", &id.NodeId, &id.DiskId, &size) != 2)
        return false;

    if (size != static_cast<int>(name.size()))
        return false;

    return true;
}

bool TPDiskInfo::IsDeviceName(const TString &name)
{
    TPDiskID id;
    return NameToId(name, id);
}

TPDiskID TPDiskInfo::NameToId(const TString &name)
{
    TPDiskID id;
    NameToId(name, id);
    return id;
}

void TPDiskInfo::MigrateOldInfo(const TLockableItem &old)
{
    TLockableItem::MigrateOldInfo(old);
    if (auto *oldPDisk = dynamic_cast<const TPDiskInfo *>(&old)) {
        if (!NodeId)
            NodeId = oldPDisk->NodeId;
    }
}

TString TVDiskInfo::ItemName() const
{
    return Sprintf("VDisk %s", VDiskId.ToString().data());
}

TString TVDiskInfo::PrettyItemName() const
{
    TStringBuilder name;

    name << ItemName();
    if (Host || Path) {
        name << " (";

        if (Host) {
            name << Host;
        }
        if (Path) {
            name << ":" << Path;
        }

        name << ")";
    }

    return name;
}

TString TVDiskInfo::GetDeviceName() const
{
    return Sprintf("vdisk-%u-%u-%u-%u-%u", VDiskId.GroupID.GetRawId(), VDiskId.GroupGeneration,
                   VDiskId.FailRealm, VDiskId.FailDomain, VDiskId.VDisk);
}

bool TVDiskInfo::NameToId(const TString &name, TVDiskID &id)
{
    ui32 group, gen, ring, domain, vdisk;
    int size;

    if (sscanf(name.data(), "vdisk-%" SCNu32 "-%" SCNu32 "-%" SCNu32 "-%" SCNu32 "-%" SCNu32 "%n",
               &group, &gen, &ring, &domain, &vdisk, &size) != 5)
        return false;

    if (size != static_cast<int>(name.size()))
        return false;

    id = TVDiskID(TGroupId::FromValue(group), gen, ring, domain, vdisk);

    return true;
}

bool TVDiskInfo::IsDeviceName(const TString &name)
{
    TVDiskID id;
    return NameToId(name, id);
}

TVDiskID TVDiskInfo::NameToId(const TString &name)
{
    TVDiskID id;
    NameToId(name, id);
    return id;
}

void TVDiskInfo::MigrateOldInfo(const TLockableItem &old)
{
    TLockableItem::MigrateOldInfo(old);
    if (auto *oldVDisk = dynamic_cast<const TVDiskInfo *>(&old)) {
        if (!NodeId)
            NodeId = oldVDisk->NodeId;
        if (!PDiskId)
            PDiskId = oldVDisk->PDiskId;
    }
}

TStateStorageRingInfo::RingState TStateStorageRingInfo::CountState(TInstant now,
                                                                   TDuration retryTime,
                                                                   TDuration duration) const
{
    if (IsDisabled) {
        return Disabled;
    }

    ui32 unavailableReplicas = 0;
    bool hasTimeout = false;
    TErrorInfo error;
    for (auto &node : Replicas) {
        if (node->IsDown(error, now + retryTime)
            || node->IsLocked(error, retryTime, now, duration)) {
            ++unavailableReplicas;
            continue;
        }

        if (now <= node->StartTime + Timeout) {
            hasTimeout = true;
        }
    }

    if (unavailableReplicas > 0) {
        return Restart;
    }

    if (hasTimeout) {
        return Locked;
    }

    return Ok;
}

void TClusterInfo::SetTimestamp(TInstant timestamp)
{
    Timestamp = timestamp;
    for (auto &entry : LockableItems)
        entry.second->Timestamp = timestamp;
}

void TClusterInfo::AddNode(const TEvInterconnect::TNodeInfo &info, const TActorContext *ctx)
{
    TNodeInfoPtr &node = Nodes[info.NodeId];
    if (!node)
        node = new TNodeInfo;

    TString oldHost = node->Host;
    node->NodeId = info.NodeId;
    node->Host = info.Host;
    node->Address = info.Address;
    node->IcPort = info.Port;
    node->Location = info.Location;
    node->State = NKikimrCms::UNKNOWN;

    if (ctx) {
        const auto maxStaticNodeId = AppData(*ctx)->DynamicNameserviceConfig->MaxStaticNodeId;
        if (node->NodeId <= maxStaticNodeId) {
            node->Services |= EService::Storage;
        } else {
            node->Services |= EService::DynamicNode;
        }
    }

    auto range = HostNameToNodeId.equal_range(oldHost);
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second == node->NodeId) {
            HostNameToNodeId.erase(it);
            break;
        }
    }

    node->AddNodeGroup(ClusterNodes);

    HostNameToNodeId.emplace(node->Host, node->NodeId);
    LockableItems[node->ItemName()] = node;
}

void TClusterInfo::SetNodeState(ui32 nodeId, NKikimrCms::EState state, const NKikimrWhiteboard::TSystemStateInfo &info)
{
    if (!HasNode(nodeId))
        return;

    auto &node = NodeRef(nodeId);
    node.State = state;
    node.StartTime = TInstant::MilliSeconds(info.GetStartTime());
    node.Version = info.GetVersion();

    node.Services = TServices();
    for (const auto& role : info.GetRoles()) {
        EService value;
        if (TryFromWhiteBoardRole(role, value)) {
            node.Services |= value;
        }
    }

    node.UpdateNodeState();
}

void TClusterInfo::ClearNode(ui32 nodeId)
{
    if (!HasNode(nodeId))
        return;

    auto &node = NodeRef(nodeId);
    for (auto tablet : node.Tablets)
        Tablets.erase(tablet);
    node.Tablets.clear();
    node.HasTenantInfo = false;
    node.State = NKikimrCms::DOWN;
    node.UpdateNodeState();
}

void TClusterInfo::ApplyInitialNodeTenants(const TActorContext& ctx, const THashMap<ui32, TString>& nodeTenants)
{
    for (const auto& pr : nodeTenants) {
        ui32 nodeId = pr.first;
        TString tenant = pr.second;

        if (!HasNode(nodeId)) {
            LOG_ERROR(ctx, NKikimrServices::CMS,
                      "Forgoten node tenant '%s' at node %" PRIu32 ". Node is unknown.",
                      tenant.data(), nodeId);
            continue;
        }

        TNodeInfo& node = NodeRef(nodeId);
        node.PreviousTenant = tenant;

        LOG_DEBUG(ctx, NKikimrServices::CMS,
                  "Initial node tenant '%s' at node %" PRIu32,
                  tenant.data(), nodeId);
    }
}

void TClusterInfo::AddTablet(ui32 nodeId, const NKikimrWhiteboard::TTabletStateInfo &info)
{
    if (!HasNode(nodeId))
        return;

    TTabletInfo &tablet = Tablets[info.GetTabletId()];
    tablet.TabletId = info.GetTabletId();
    tablet.Type = info.GetType();
    tablet.State = info.GetState();
    tablet.Leader = info.GetLeader();
    tablet.NodeId = nodeId;

    auto &node = NodeRef(nodeId);
    node.Tablets.insert(tablet.TabletId);
}

void TClusterInfo::AddPDisk(const NKikimrBlobStorage::TBaseConfig::TPDisk &info)
{
    ui32 nodeId = info.GetNodeId();
    ui32 pdiskId = info.GetPDiskId();
    auto &path = info.GetPath();

    if (!HasNode(nodeId))
        return;

    TPDiskID id  = {nodeId, pdiskId};
    TPDiskInfoPtr &pdisk = PDisks[id];
    if (!pdisk)
        pdisk = new TPDiskInfo;

    pdisk->PDiskId = id;
    pdisk->NodeId = nodeId;
    pdisk->Path = path;

    auto &node = NodeRef(nodeId);
    pdisk->Host = node.Host;
    node.PDisks.insert(id);

    LockableItems[pdisk->ItemName()] = pdisk;
}

void TClusterInfo::UpdatePDiskState(const TPDiskID &id, const NKikimrWhiteboard::TPDiskStateInfo &info)
{
    if (!HasPDisk(id)) {
        BLOG_ERROR("Cannot update state for unknown PDisk " << id.ToString());
        return;
    }

    auto &pdisk = PDiskRef(id);
    pdisk.State = info.GetState() == NKikimrBlobStorage::TPDiskState::Normal ? UP : DOWN;
}

void TClusterInfo::AddVDisk(const NKikimrBlobStorage::TBaseConfig::TVSlot &info)
{
    ui32 nodeId = info.GetVSlotId().GetNodeId();
    Y_DEBUG_ABORT_UNLESS(HasNode(nodeId));
    if (!HasNode(nodeId)) {
        BLOG_ERROR("Got VDisk info from BSC base config for unknown node " << nodeId);
        return;
    }

    TVDiskID vdiskId(TGroupId::FromProto(&info, &NKikimrBlobStorage::TBaseConfig::TVSlot::GetGroupId),
                     info.GetGroupGeneration(),
                     info.GetFailRealmIdx(),
                     info.GetFailDomainIdx(),
                     info.GetVDiskIdx());
    TVDiskInfoPtr &vdisk = VDisks[vdiskId];
    if (!vdisk)
        vdisk = new TVDiskInfo;

    vdisk->VDiskId = vdiskId;
    vdisk->PDiskId = {nodeId, info.GetVSlotId().GetPDiskId()};
    vdisk->NodeId = nodeId;
    vdisk->SlotId = info.GetVSlotId().GetVSlotId();

    Y_DEBUG_ABORT_UNLESS(HasPDisk(vdisk->PDiskId));
    if (!HasPDisk(vdisk->PDiskId)) {
        BLOG_ERROR("Got VDisk info from BSC base config for unknown PDisk " << vdisk->PDiskId.ToString());
        PDisks.emplace(vdisk->PDiskId, new TPDiskInfo(vdisk->PDiskId));
    }

    auto &pdisk = PDiskRef(vdisk->PDiskId);
    vdisk->Path = pdisk.Path;
    pdisk.VDisks.insert(vdisk->VDiskId);
    pdisk.VSlots[vdisk->SlotId] = vdisk->VDiskId;

    auto &node = NodeRef(nodeId);
    vdisk->Host = node.Host;
    node.VDisks.insert(vdisk->VDiskId);

    LockableItems[vdisk->ItemName()] = vdisk;
}

void TClusterInfo::UpdateVDiskState(const TVDiskID &id, const NKikimrWhiteboard::TVDiskStateInfo &info)
{
    if (!HasVDisk(id)) {
        if (IsStaticGroupVDisk(id)) {
            return;
        }

        BLOG_ERROR("Cannot update state for unknown VDisk " << id.ToString());
        return;
    }

    auto &vdisk = VDiskRef(id);
    if (info.GetVDiskState() == NKikimrWhiteboard::OK && info.GetReplicated())
        vdisk.State = UP;
    else
        vdisk.State = DOWN;
}

void TClusterInfo::AddBSGroup(const NKikimrBlobStorage::TBaseConfig::TGroup &info)
{
    TBSGroupInfo bsgroup;
    bsgroup.GroupId = info.GetGroupId();
    if (info.GetErasureSpecies())
        bsgroup.Erasure = {TErasureType::ErasureSpeciesByName(info.GetErasureSpecies())};
    for (const auto &vdisk : info.GetVSlotId()) {
        TPDiskID pdiskId = {vdisk.GetNodeId(), vdisk.GetPDiskId()};
        Y_DEBUG_ABORT_UNLESS(HasPDisk(pdiskId));
        if (!HasPDisk(pdiskId)) {
            BLOG_ERROR("Group " << bsgroup.GroupId << " refers unknown pdisk " << pdiskId.ToString());
            return;
        }

        auto &pdisk = PDiskRef(pdiskId);
        Y_DEBUG_ABORT_UNLESS(pdisk.VSlots.contains(vdisk.GetVSlotId()));
        if (!pdisk.VSlots.contains(vdisk.GetVSlotId())) {
            BLOG_ERROR("Group " << bsgroup.GroupId << " refers unknown slot " <<
                        vdisk.GetVSlotId() << " in disk " << pdiskId.ToString());
            return;
        }

        bsgroup.VDisks.insert(pdisk.VSlots.at(vdisk.GetVSlotId()));
    }

    for (auto &vdisk : bsgroup.VDisks)
        VDiskRef(vdisk).BSGroups.insert(bsgroup.GroupId);
    BSGroups[bsgroup.GroupId] = std::move(bsgroup);
}

void TClusterInfo::AddNodeTenants(ui32 nodeId, const NKikimrTenantPool::TTenantPoolStatus &info)
{
    if (!HasNode(nodeId))
        return;

    auto& node = NodeRef(nodeId);
    TString nodeTenant;

    for (const auto& slot : info.GetSlots()) {
        TString slotTenant = slot.GetAssignedTenant();
        Y_ABORT_UNLESS(slotTenant.empty() || nodeTenant.empty() || slotTenant == nodeTenant);
        if (!slotTenant.empty())
            nodeTenant = slotTenant;
    }

    node.Tenant = nodeTenant;
    node.HasTenantInfo = true;

    TenantToNodeId.emplace(nodeTenant, nodeId);
}

void TClusterInfo::AddNodeTempLock(ui32 nodeId, const NKikimrCms::TAction &action)
{
    auto &node = NodeRef(nodeId);
    node.TempLocks.push_back({RollbackPoint, action});
}

void TClusterInfo::AddPDiskTempLock(TPDiskID pdiskId, const NKikimrCms::TAction &action)
{
    auto &pdisk = PDiskRef(pdiskId);
    pdisk.TempLocks.push_back({RollbackPoint, action});
}

void TClusterInfo::AddVDiskTempLock(TVDiskID vdiskId, const NKikimrCms::TAction &action)
{
    auto &vdisk = VDiskRef(vdiskId);
    vdisk.TempLocks.push_back({RollbackPoint, action});
}

static TServices MakeServices(const NKikimrCms::TAction &action) {
    TServices services;

    if (action.GetType() != TAction::RESTART_SERVICES) {
        return services;
    }

    for (const auto &service : action.GetServices()) {
        EService value;
        if (TryFromString(service, value)) {
            services |= value;
        }
    }

    return services;
}

void TClusterInfo::ApplyActionWithoutLog(const NKikimrCms::TAction &action)
{
    if (ActionRequiresHost(action) && !HasNode(action.GetHost())) {
        return;
    }

    switch (action.GetType()) {
    case TAction::RESTART_SERVICES:
    case TAction::SHUTDOWN_HOST:
    case TAction::REBOOT_HOST:
        if (auto nodes = NodePtrs(action.GetHost(), MakeServices(action))) {
            for (const auto node : nodes) {
                for (auto &nodeGroup: node->NodeGroups) {
                    if (!nodeGroup->IsNodeLocked(node->NodeId)) {
                        nodeGroup->LockNode(node->NodeId);
                    }
                }
            }
        }
        break;
    case TAction::REPLACE_DEVICES:
        for (const auto &device : action.GetDevices()) {
            if (HasPDisk(device)) {
                auto pdisk = &PDiskRef(device);
                for (auto &nodeGroup: NodeRef(pdisk->NodeId).NodeGroups) {
                    if (!nodeGroup->IsNodeLocked(pdisk->NodeId)) {
                        nodeGroup->LockNode(pdisk->NodeId);
                    }
                }
            } else if (HasVDisk(device)) {
                auto vdisk = &VDiskRef(device);
                for (auto &nodeGroup: NodeRef(vdisk->NodeId).NodeGroups) {
                    if (!nodeGroup->IsNodeLocked(vdisk->NodeId)) {
                        nodeGroup->LockNode(vdisk->NodeId);
                    }
                } 
            }
        }
        break;

    default:
        break;
    }
}

void TClusterInfo::ApplyNodeLimits(ui32 clusterLimit, ui32 clusterRatioLimit, ui32 tenantLimit, ui32 tenantRatioLimit)
{
    ClusterNodes->ApplyLimits(clusterLimit, clusterRatioLimit);

    for (auto &[_, tenantChecker] : TenantNodesChecker) {
        tenantChecker->ApplyLimits(tenantLimit, tenantRatioLimit);
    }
}

TSet<TLockableItem *> TClusterInfo::FindLockedItems(const NKikimrCms::TAction &action,
                                                    const TActorContext *ctx)
{
    TSet<TLockableItem *> res;

    if (ActionRequiresHost(action) && !HasNode(action.GetHost())) {
        if (ctx)
            LOG_ERROR(*ctx, NKikimrServices::CMS, "FindLockedItems: unknown host %s",
                      action.GetHost().data());
        return res;
    }

    switch (action.GetType()) {
    case TAction::RESTART_SERVICES:
    case TAction::SHUTDOWN_HOST:
    case TAction::REBOOT_HOST:
        if (auto nodes = NodePtrs(action.GetHost(), MakeServices(action))) {
            for (const auto node : nodes) {
                res.insert(node);
            }
        } else if (ctx) {
            LOG_ERROR_S(*ctx, NKikimrServices::CMS,
                        "FindLockedItems: unknown host " << action.GetHost());
        }
        break;

    case TAction::REPLACE_DEVICES:
        for (const auto &device : action.GetDevices()) {
            TLockableItem *item = nullptr;

            if (HasPDisk(device))
                item = &PDiskRef(device);
            else if (HasVDisk(device))
                item = &VDiskRef(device);

            if (item)
                res.insert(item);
            else if (ctx)
                LOG_ERROR(*ctx, NKikimrServices::CMS, "FindLockedItems: unknown device %s", device.data());
        }
        break;

    default:
        if (ctx) {
            LOG_ERROR(*ctx, NKikimrServices::CMS, "FindLockedItems: action %s is not supported",
                      TAction::EType_Name(action.GetType()).data());
        }
        break;
    }

    return res;
}

ui64 TClusterInfo::AddLocks(const TPermissionInfo &permission, const TActorContext *ctx)
{
    TInstant deadline(permission.Deadline);
    TDuration duration = TDuration::MicroSeconds(permission.Action.GetDuration());
    Y_UNUSED(duration);
    auto items = FindLockedItems(permission.Action, ctx);
    ui64 locks = 0;

    for (auto item : items) {
        bool lock = false;

        if (deadline > Timestamp)
            lock = true;

        if (item->State == DOWN
            && (permission.Action.GetType() == TAction::RESTART_SERVICES
                || permission.Action.GetType() == TAction::SHUTDOWN_HOST
                || permission.Action.GetType() == TAction::REBOOT_HOST
                || permission.Action.GetType() == TAction::REPLACE_DEVICES)) {
            item->State = RESTART;
            lock = true;
        }

        if (lock) {
            if (ctx)
                LOG_INFO(*ctx, NKikimrServices::CMS, "Adding lock for %s (permission %s until %s)",
                          item->PrettyItemName().data(), permission.PermissionId.data(),
                          permission.Deadline.ToStringLocalUpToSeconds().data());
            item->AddLock(permission);
            ++locks;
        }
    }

    ApplyActionWithoutLog(permission.Action);

    return locks;
}

ui64 TClusterInfo::AddExternalLocks(const TNotificationInfo &notification, const TActorContext *ctx)
{
    ui64 locks = 0;
    for (const auto &action : notification.Notification.GetActions()) {
        auto items = FindLockedItems(action, ctx);
        ApplyActionWithoutLog(action);

        for (auto item : items) {
            if (ctx)
                LOG_INFO(*ctx, NKikimrServices::CMS, "Adding external lock for %s",
                          item->PrettyItemName().data());

            item->AddExternalLock(notification, action);
        }

        locks += items.size();
    }

    return locks;
}

void TClusterInfo::SetHostMarkers(const TString &hostName, const THashSet<NKikimrCms::EMarker> &markers) {
    for (auto node : NodePtrs(hostName)) {
        node->Markers.insert(markers.begin(), markers.end());
    }
}

void TClusterInfo::ResetHostMarkers(const TString &hostName) {
    for (auto node : NodePtrs(hostName)) {
        node->Markers.clear();
    }
}

void TClusterInfo::ApplyDowntimes(const TDowntimes &downtimes)
{
    for (auto &pr : downtimes.NodeDowntimes) {
        if (!HasNode(pr.first))
            continue;
        NodeRef(pr.first).Downtime = pr.second;
    }
    for (auto &pr : downtimes.PDiskDowntimes) {
        if (!HasPDisk(pr.first))
            continue;
        PDiskRef(pr.first).Downtime = pr.second;
    }
}

void TClusterInfo::UpdateDowntimes(TDowntimes &downtimes, const TActorContext &ctx)
{
    downtimes.CleanupOld(ctx.Now());

    for (auto &pr : Nodes) {
        if (pr.second->State != NKikimrCms::UP)
            downtimes.NodeDowntimes[pr.first] = pr.second->Downtime;
    }
    for (auto &pr : PDisks) {
        if (pr.second->State != NKikimrCms::UP)
            downtimes.PDiskDowntimes[pr.first] = pr.second->Downtime;
    }
}

ui64 TClusterInfo::AddTempLocks(const NKikimrCms::TAction &action, const TActorContext *ctx)
{
    auto items = FindLockedItems(action, ctx);

    LogManager.ApplyAction(action, this);

    for (auto item : items)
        item->TempLocks.push_back({RollbackPoint, action});

    return items.size();
}

ui64 TClusterInfo::ScheduleActions(const TRequestInfo &request, const TActorContext *ctx)
{
    ui64 locks = 0;
    for (const auto &action : request.Request.GetActions()) {
        auto items = FindLockedItems(action, ctx);

        for (auto item : items)
            item->ScheduleLock({action, request.Owner, request.RequestId, request.Priority});

        locks += items.size();
    }

    return locks;
}

void TClusterInfo::UnscheduleActions(const TString &requestId)
{
    for (auto &entry : LockableItems)
        entry.second->RemoveScheduledLocks(requestId);
}

void TClusterInfo::DeactivateScheduledLocks(i32 priority)
{
    for (auto &entry : LockableItems)
        entry.second->DeactivateScheduledLocks(priority);
}

void TClusterInfo::ReactivateScheduledLocks()
{
    for (auto &entry : LockableItems)
        entry.second->ReactivateScheduledLocks();
}

void TClusterInfo::RollbackLocks(ui64 point)
{
    if (!point || point > RollbackPoint)
        return;

    for (auto &entry : LockableItems)
        entry.second->RollbackLocks(point);
    RollbackPoint = point - 1;

    LogManager.RollbackOperations();
}

void TClusterInfo::MigrateOldInfo(TClusterInfoPtr old)
{
    for (auto &entry : LockableItems) {
        auto it = old->LockableItems.find(entry.first);
        if (it != old->LockableItems.end())
            entry.second->MigrateOldInfo(*it->second);
    }
}

void TClusterInfo::ApplyStateStorageInfo(TIntrusiveConstPtr<TStateStorageInfo> info) {
    StateStorageInfoReceived = true;
    for (ui32 ringId = 0; ringId < info->Rings.size(); ++ringId) {
        auto &ring = info->Rings[ringId];
        TStateStorageRingInfoPtr ringInfo = MakeIntrusive<TStateStorageRingInfo>();
        ringInfo->RingId = ringId;
        if (ring.IsDisabled)
            ringInfo->SetDisabled();

        for(auto replica : ring.Replicas) {
            CheckNodeExistenceWithVerify(replica.NodeId());
            ringInfo->AddNode(Nodes[replica.NodeId()]);
            StateStorageReplicas.insert(replica.NodeId());
            StateStorageNodeToRingId[replica.NodeId()] = ringId;
        }

        StateStorageRings.push_back(ringInfo);
    }
}

void TClusterInfo::GenerateTenantNodesCheckers() {
    for (auto &[nodeId, nodeInfo] : Nodes) {
        if (nodeInfo->Tenant) {
            if (!TenantNodesChecker.contains(nodeInfo->Tenant))
                TenantNodesChecker[nodeInfo->Tenant] = TSimpleSharedPtr<TNodesLimitsCounterBase>(new TTenantLimitsCounter(nodeInfo->Tenant, 0, 0));

            nodeInfo->AddNodeGroup(TenantNodesChecker[nodeInfo->Tenant]);
        }
    }
}

void TClusterInfo::GenerateSysTabletsNodesCheckers() {
    for (auto tablet : BootstrapConfig.GetTablet()) {
        SysNodesCheckers[tablet.GetType()] = TSimpleSharedPtr<TSysTabletsNodesCounter>(new TSysTabletsNodesCounter(tablet.GetType()));

        for (auto nodeId : tablet.GetNode()) {
            if (!HasNode(nodeId)) {
                BLOG_ERROR(TStringBuilder() << "Got node " << nodeId
                                            << " with system tablet, which exists in configuration, "
                                               "but does not exist in cluster.");
                continue;
            }
            NodeToTabletTypes[nodeId].push_back(tablet.GetType());
            NodeRef(nodeId).AddNodeGroup(SysNodesCheckers[tablet.GetType()]);
        }
    }
}

void TClusterInfo::DebugDump(const TActorContext &ctx) const
{
    LOG_DEBUG_S(ctx, NKikimrServices::CMS,
                "Timestamp: " << Timestamp.ToStringLocalUpToSeconds());
    for (auto &entry: Nodes) {
        TStringStream ss;
        auto &node = *entry.second;
        ss << "Node {" << Endl
           << "  Id: " << (ui32)node.NodeId << Endl
           << "  Host: " << node.Host << Endl
           << "  Address: " << node.Address << Endl
           << "  Version: " << node.Version << Endl
           << "  State: " << EState_Name(node.State) << Endl;
        for (auto pd : node.PDisks)
            ss << "  PDisk: " << pd.NodeId << ":" << pd.DiskId << Endl;
        for (auto &vd : node.VDisks)
            ss << "  VDisk: " << vd.ToString() << Endl;
        node.DebugLocksDump(ss, "  ");
        ss << "}" << Endl;
        LOG_TRACE(ctx, NKikimrServices::CMS, ss.Str());
    }
    for (auto &entry: PDisks) {
        TStringStream ss;
        auto &pdisk = *entry.second;
        ss << "PDisk {" << Endl
           << "  Id: " << pdisk.PDiskId.NodeId << ":" << pdisk.PDiskId.DiskId << Endl
           << "  NodeId: " << pdisk.NodeId << Endl
           << "  State: " << EState_Name(pdisk.State) << Endl;
        pdisk.DebugLocksDump(ss, "  ");
        ss << "}" << Endl;
        LOG_TRACE(ctx, NKikimrServices::CMS, ss.Str());
    }
    for (auto &entry: VDisks) {
        TStringStream ss;
        auto &vdisk = *entry.second;
        ss << "VDisk {" << Endl
           << "  Id: " << vdisk.VDiskId.ToString() << Endl
           << "  NodeId: " << vdisk.NodeId << Endl
           << "  State: " << EState_Name(vdisk.State) << Endl
           << "  PDisk: " << vdisk.PDiskId.NodeId << ":" << vdisk.PDiskId.DiskId << Endl;
        for (auto id : vdisk.BSGroups)
            ss << "  BSGroup: " << id << Endl;
        vdisk.DebugLocksDump(ss, "  ");
        ss << "}" << Endl;
        LOG_TRACE(ctx, NKikimrServices::CMS, ss.Str());
    }
    for (auto &entry: BSGroups) {
        TStringStream ss;
        auto &group = entry.second;
        ss << "BSGroup {" << Endl
           << "  Id: " << group.GroupId << Endl;
        if (group.Erasure.GetErasure() == TErasureType::ErasureSpeciesCount)
            ss << "  Erasure: UNKNOWN" << Endl;
        else
            ss << "  Erasure: " << group.Erasure.ToString() << Endl;
        for (auto &vd : group.VDisks)
            ss << "  VDisk: " << vd.ToString() << Endl;
        ss << "}" << Endl;
        LOG_TRACE(ctx, NKikimrServices::CMS, ss.Str());
    }
}

void TOperationLogManager::ApplyAction(const NKikimrCms::TAction &action,
                                      TClusterInfoPtr clusterState)
{
    switch (action.GetType()) {
    case NKikimrCms::TAction::RESTART_SERVICES:
    case NKikimrCms::TAction::SHUTDOWN_HOST:
    case NKikimrCms::TAction::REBOOT_HOST:
        if (auto nodes = clusterState->NodePtrs(action.GetHost(), MakeServices(action))) {
            for (const auto node : nodes) {
                for (auto &nodeGroup: node->NodeGroups) {
                    if (!nodeGroup->IsNodeLocked(node->NodeId)) {
                        AddNodeLockOperation(node->NodeId, nodeGroup);
                    }
                }     
            }
        }
        break;
    case NKikimrCms::TAction::REPLACE_DEVICES:
        for (const auto &device : action.GetDevices()) {
            if (clusterState->HasPDisk(device)) {
                auto pdisk = &clusterState->PDisk(device);
                for (auto &nodeGroup: clusterState->NodeRef(pdisk->NodeId).NodeGroups) {
                    if (!nodeGroup->IsNodeLocked(pdisk->NodeId)) {
                        AddNodeLockOperation(pdisk->NodeId, nodeGroup);
                    }
                }       
            } else if (clusterState->HasVDisk(device)) {
                auto vdisk = &clusterState->VDisk(device);
                for (auto &nodeGroup: clusterState->NodeRef(vdisk->NodeId).NodeGroups) {
                    if (!nodeGroup->IsNodeLocked(vdisk->NodeId)) {
                        AddNodeLockOperation(vdisk->NodeId, nodeGroup);
                    }
                }     
            }
        }
        break;

    default:
        break;
    }
}
} // namespace NKikimr::NCms
