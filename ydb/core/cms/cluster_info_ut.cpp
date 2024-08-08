#include "cluster_info.h"
#include "ut_helpers.h"

#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>

namespace NKikimr {
namespace NCmsTest {

using namespace NCms;
using namespace NNodeWhiteboard;
using namespace NKikimrWhiteboard;
using namespace NKikimrBlobStorage;
using namespace NKikimrCms;

TTabletStateInfo MakeTabletInfo(ui64 id, TTabletTypes::EType type,
                                TTabletStateInfo::ETabletState state, bool leader)
{
    TTabletStateInfo tablet;
    tablet.SetTabletId(id);
    tablet.SetType(type);
    tablet.SetState(state);
    tablet.SetLeader(leader);
    return tablet;
}

TBaseConfig::TPDisk MakePDiskConfig(ui32 nodeId, ui32 id)
{
    TBaseConfig::TPDisk pdisk;
    pdisk.SetNodeId(nodeId);
    pdisk.SetPDiskId(id);
    pdisk.SetPath(Sprintf("/pdisk%" PRIu32 ".data", id));
    pdisk.SetType(ROT);
    pdisk.SetGuid(id);
    return pdisk;
}

TPDiskStateInfo MakePDiskInfo(ui32 id)
{
    auto now = Now();
    TPDiskStateInfo pdisk;
    pdisk.SetPDiskId(id);
    pdisk.SetCreateTime(now.GetValue());
    pdisk.SetChangeTime(now.GetValue());
    pdisk.SetPath(Sprintf("/pdisk%" PRIu32 ".data", id));
    pdisk.SetGuid(id);
    pdisk.SetAvailableSize(100ULL << 30);
    pdisk.SetTotalSize(100ULL << 30);
    pdisk.SetState(NKikimrBlobStorage::TPDiskState::Normal);
    return pdisk;
}

TBaseConfig::TVSlot MakeVSlotConfig(ui32 nodeId, const TVDiskID &id, ui32 pdisk, ui32 slot)
{
    TBaseConfig::TVSlot vdisk;
    vdisk.MutableVSlotId()->SetNodeId(nodeId);
    vdisk.MutableVSlotId()->SetPDiskId(pdisk);
    vdisk.MutableVSlotId()->SetVSlotId(slot);
    vdisk.SetGroupId(id.GroupID);
    vdisk.SetGroupGeneration(id.GroupGeneration);
    vdisk.SetFailRealmIdx(id.FailRealm);
    vdisk.SetFailDomainIdx(id.FailDomain);
    vdisk.SetVDiskIdx(id.VDisk);
    return vdisk;
}

TVDiskStateInfo MakeVDiskInfo(const TVDiskID &id, ui32 pdisk, ui32 slot)
{
    auto now = Now();
    TVDiskStateInfo vdisk;
    VDiskIDFromVDiskID(id, vdisk.MutableVDiskId());
    vdisk.SetCreateTime(now.GetValue());
    vdisk.SetChangeTime(now.GetValue());
    vdisk.SetPDiskId(pdisk);
    vdisk.SetVDiskSlotId(slot);
    vdisk.SetVDiskState(OK);
    vdisk.SetReplicated(true);
    return vdisk;
}

void AddVDisks(TBaseConfig::TGroup &group, ui32 nodeId, ui32 pdiskId, ui32 slotId)
{
    auto &slot = *group.AddVSlotId();
    slot.SetNodeId(nodeId);
    slot.SetPDiskId(pdiskId);
    slot.SetVSlotId(slotId);
}

template<typename... Ts>
void AddVDisks(TBaseConfig::TGroup &group, ui32 nodeId, ui32 pdiskId, ui32 slotId, Ts... vdisks)
{
    AddVDisks(group, nodeId, pdiskId, slotId);
    AddVDisks(group, vdisks...);
}

template<typename... Ts>
TBaseConfig::TGroup MakeBSGroup(ui32 id, const TString &erasure, Ts... vdisks)
{
    TBaseConfig::TGroup group;
    group.SetGroupId(id);
    group.SetErasureSpecies(erasure);
    AddVDisks(group, vdisks...);
    return group;
}

void CheckNode(const TNodeInfo &node, ui32 id, const TString &host,
               const TString &address, NKikimrCms::EState state)
{
    UNIT_ASSERT_VALUES_EQUAL(node.NodeId, id);
    UNIT_ASSERT_VALUES_EQUAL(node.Host, host);
    UNIT_ASSERT_VALUES_EQUAL(node.Address, address);
    UNIT_ASSERT_VALUES_EQUAL(node.State, state);
}

void CheckPDisk(const TPDiskInfo &pdisk, ui32 id, ui32 nodeId, NKikimrCms::EState state,
                size_t vdisks = 0)
{
    UNIT_ASSERT_VALUES_EQUAL(pdisk.PDiskId.DiskId, id);
    UNIT_ASSERT_VALUES_EQUAL(pdisk.PDiskId.NodeId, nodeId);
    UNIT_ASSERT_VALUES_EQUAL(pdisk.NodeId, nodeId);
    UNIT_ASSERT_VALUES_EQUAL(pdisk.State, state);
    UNIT_ASSERT_VALUES_EQUAL(pdisk.VDisks.size(), vdisks);
}

void CheckVDiskGroups(const TVDiskInfo &vdisk, ui32 group)
{
    UNIT_ASSERT(vdisk.BSGroups.contains(group));
}

template<typename... Ts>
void CheckVDiskGroups(const TVDiskInfo &vdisk, ui32 group, Ts... groups)
{
    CheckVDiskGroups(vdisk, group);
    CheckVDiskGroups(vdisk, groups...);
}

template<typename... Ts>
void CheckVDisk(const TVDiskInfo &vdisk, size_t size, Ts... groups)
{
    UNIT_ASSERT_VALUES_EQUAL(vdisk.BSGroups.size(), size);
    CheckVDiskGroups(vdisk, groups...);
}

void CheckVDisk(const TVDiskInfo &vdisk, TVDiskID id, ui32 nodeId, NKikimrCms::EState state,
                ui32 pdisk, size_t groups = 0)
{
    UNIT_ASSERT_VALUES_EQUAL(vdisk.VDiskId, id);
    UNIT_ASSERT_VALUES_EQUAL(vdisk.NodeId, nodeId);
    UNIT_ASSERT_VALUES_EQUAL(vdisk.State, state);
    UNIT_ASSERT_VALUES_EQUAL(vdisk.PDiskId.NodeId, nodeId);
    UNIT_ASSERT_VALUES_EQUAL(vdisk.PDiskId.DiskId, pdisk);
    UNIT_ASSERT_VALUES_EQUAL(vdisk.BSGroups.size(), groups);
}

template<typename... Ts>
void CheckVDisk(const TVDiskInfo &vdisk, TVDiskID id, ui32 nodeId, NKikimrCms::EState state,
                ui32 pdisk, size_t groupSize, Ts... groups)
{
    CheckVDisk(vdisk, id, nodeId, state, pdisk, groupSize);
    CheckVDiskGroups(vdisk, groups...);
}

void CheckBSGroupVDisks(const TBSGroupInfo &group, TVDiskID vdisk)
{
    UNIT_ASSERT(group.VDisks.contains(vdisk));
}

template<typename... Ts>
void CheckBSGroupVDisks(const TBSGroupInfo &group, TVDiskID vdisk, Ts... vdisks)
{
    CheckBSGroupVDisks(group, vdisk);
    CheckBSGroupVDisks(group, vdisks...);
}

template<typename... Ts>
void CheckBSGroup(const TBSGroupInfo &group, ui32 id, TErasureType::EErasureSpecies erasure,
                  size_t size, Ts... vdisks)
{
    UNIT_ASSERT_VALUES_EQUAL(group.GroupId, id);
    UNIT_ASSERT_VALUES_EQUAL(group.Erasure.GetErasure(), erasure);
    UNIT_ASSERT_VALUES_EQUAL(group.VDisks.size(), size);
    CheckBSGroupVDisks(group, vdisks...);
}

void AddActions(TRequestInfo &request, const NKikimrCms::TAction &action)
{
    request.Request.AddActions()->CopyFrom(action);
}

template<typename... Ts>
void AddActions(TRequestInfo &request, const NKikimrCms::TAction &action, Ts... actions)
{
    AddActions(request, action);
    AddActions(request, actions...);
}

template<typename... Ts>
TRequestInfo MakeRequest(const TString &id, const TString &owner, i32 priority, Ts... actions)
{
    TRequestInfo res;
    res.RequestId = id;
    res.Owner = owner;
    res.Priority = priority;
    AddActions(res, actions...);
    return res;
}

template<typename I>
void CheckScheduledLocks(I pos, I end, const TString &id, const TString &owner, i32 priority)
{
    UNIT_ASSERT(pos != end);
    UNIT_ASSERT_VALUES_EQUAL(pos->RequestId, id);
    UNIT_ASSERT_VALUES_EQUAL(pos->Owner, owner);
    UNIT_ASSERT_VALUES_EQUAL(pos->Priority, priority);
    UNIT_ASSERT(++pos == end);
}

template<typename I, typename... Ts>
void CheckScheduledLocks(I pos, I end, const TString &id, const TString &owner, i32 priority, Ts... locks)
{
    UNIT_ASSERT(pos != end);
    UNIT_ASSERT_VALUES_EQUAL(pos->RequestId, id);
    UNIT_ASSERT_VALUES_EQUAL(pos->Owner, owner);
    UNIT_ASSERT_VALUES_EQUAL(pos->Priority, priority);
    CheckScheduledLocks(++pos, end, locks...);
}

template<typename... Ts>
void CheckScheduledLocks(const TLockableItem &item, Ts... locks)
{
    CheckScheduledLocks(item.ScheduledLocks.begin(), item.ScheduledLocks.end(), locks...);
}

Y_UNIT_TEST_SUITE(TClusterInfoTest) {
    Y_UNIT_TEST(DeviceId) {
        UNIT_ASSERT(!TPDiskInfo::IsDeviceName("pdisk"));
        UNIT_ASSERT(!TPDiskInfo::IsDeviceName("disk-1"));
        UNIT_ASSERT(!TPDiskInfo::IsDeviceName("pdisk-"));
        UNIT_ASSERT(!TPDiskInfo::IsDeviceName("pdisk-1l"));
        UNIT_ASSERT(!TPDiskInfo::IsDeviceName("pdisk-1-1l"));
        UNIT_ASSERT(TPDiskInfo::IsDeviceName("pdisk-1-1"));
        UNIT_ASSERT(TPDiskInfo::NameToId("pdisk-1-1") == NCms::TPDiskID(1, 1));

        UNIT_ASSERT(!TVDiskInfo::IsDeviceName("vdisk"));
        UNIT_ASSERT(!TVDiskInfo::IsDeviceName("vdisk-1-2-3-4"));
        UNIT_ASSERT(!TVDiskInfo::IsDeviceName("disk-1-2-3-4-5"));
        UNIT_ASSERT(!TVDiskInfo::IsDeviceName("vdisk-1-2-3-4-5-6"));
        UNIT_ASSERT(TVDiskInfo::IsDeviceName("vdisk-1-2-3-4-5"));
        UNIT_ASSERT_VALUES_EQUAL(TVDiskInfo::NameToId("vdisk-1-2-3-4-5"), TVDiskID(1, 2, 3, 4, 5));
    }

    Y_UNIT_TEST(FillInfo) {
        TEvInterconnect::TNodeInfo node1 =
            { 1, "::1", "test1", "test1", 1, TNodeLocation() };
        TEvInterconnect::TNodeInfo node2 =
            { 1, "::1", "test2", "test2", 1, TNodeLocation() };
        TEvInterconnect::TNodeInfo node3 =
            { 2, "::2", "localhost", "localhost", 1, TNodeLocation() };
        TEvInterconnect::TNodeInfo node4 =
            { 3, "::2", "localhost", "localhost", 1, TNodeLocation() };

        TClusterInfoPtr cluster(new TClusterInfo);
        cluster->AddNode(node1, nullptr);
        UNIT_ASSERT(cluster->HasNode(1));
        UNIT_ASSERT(!cluster->HasNode(2));
        UNIT_ASSERT(cluster->HasNode("test1"));
        UNIT_ASSERT(!cluster->HasNode("test2"));
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("test1").size(), 1);
        CheckNode(cluster->Node(1), 1, "test1", "::1", EState::UNKNOWN);

        cluster->AddNode(node2, nullptr);
        UNIT_ASSERT(cluster->HasNode(1));
        UNIT_ASSERT(!cluster->HasNode("test1"));
        UNIT_ASSERT(cluster->HasNode("test2"));
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("test2").size(), 1);
        CheckNode(cluster->Node(1), 1, "test2", "::1", EState::UNKNOWN);

        cluster->AddNode(node3, nullptr);
        UNIT_ASSERT(cluster->HasNode(2));
        UNIT_ASSERT(cluster->HasNode("localhost"));
        UNIT_ASSERT_VALUES_EQUAL(cluster->NodesCount("localhost"), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("localhost").size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("localhost")[0]->NodeId, 2);
        CheckNode(cluster->Node(2), 2, "localhost", "::2", EState::UNKNOWN);

        cluster->AddNode(node4, nullptr);
        UNIT_ASSERT(cluster->HasNode(3));
        UNIT_ASSERT_VALUES_EQUAL(cluster->NodesCount("localhost"), 2);

        cluster->AddPDisk(MakePDiskConfig(1, 1));
        cluster->UpdatePDiskState(NCms::TPDiskID(1, 1), MakePDiskInfo(1));
        UNIT_ASSERT(cluster->HasPDisk(NCms::TPDiskID(1, 1)));
        UNIT_ASSERT(!cluster->HasPDisk(NCms::TPDiskID(1, 2)));
        UNIT_ASSERT(!cluster->HasPDisk(NCms::TPDiskID(2, 1)));
        CheckPDisk(cluster->PDisk(NCms::TPDiskID(1, 1)), 1, 1, UP, 0);
        UNIT_ASSERT(cluster->Node(1).PDisks.contains(NCms::TPDiskID(1, 1)));

        cluster->AddVDisk(MakeVSlotConfig(1, {0, 1, 0, 0, 0}, 1, 0));
        cluster->UpdateVDiskState({0, 1, 0, 0, 0}, MakeVDiskInfo({0, 1, 0, 0, 0}, 1, 0));
        UNIT_ASSERT(cluster->HasVDisk({0, 1, 0, 0, 0}));
        UNIT_ASSERT(!cluster->HasVDisk({0, 1, 0, 1, 0}));
        CheckVDisk(cluster->VDisk({0, 1, 0, 0, 0}), {0, 1, 0, 0, 0}, 1, UP, 1, 0);
        UNIT_ASSERT_VALUES_EQUAL(cluster->PDisk(NCms::TPDiskID(1, 1)).VDisks.size(), 1);
        UNIT_ASSERT(cluster->PDisk(NCms::TPDiskID(1, 1)).VDisks.contains(TVDiskID(0, 1, 0, 0, 0)));

        cluster->AddPDisk(MakePDiskConfig(2, 2));
        cluster->UpdatePDiskState(NCms::TPDiskID(2, 2), MakePDiskInfo(2));
        UNIT_ASSERT(cluster->HasPDisk(NCms::TPDiskID(2, 2)));
        CheckPDisk(cluster->PDisk(NCms::TPDiskID(2, 2)), 2, 2, UP, 0);

        cluster->AddVDisk(MakeVSlotConfig(2, {0, 1, 0, 1, 0}, 2, 0));
        cluster->UpdateVDiskState({0, 1, 0, 1, 0}, MakeVDiskInfo({0, 1, 0, 1, 0}, 2, 0));
        UNIT_ASSERT(cluster->HasVDisk({0, 1, 0, 1, 0}));
        UNIT_ASSERT(cluster->HasPDisk(NCms::TPDiskID(2, 2)));
        CheckPDisk(cluster->PDisk(NCms::TPDiskID(2, 2)), 2, 2, UP, 1);
        UNIT_ASSERT(cluster->PDisk(NCms::TPDiskID(2, 2)).VDisks.contains(TVDiskID(0, 1, 0, 1, 0)));

        cluster->AddBSGroup(MakeBSGroup(1, "none", 1, 1, 0, 2, 2, 0));
        UNIT_ASSERT(cluster->HasBSGroup(1));
        UNIT_ASSERT(!cluster->HasBSGroup(2));
        CheckBSGroup(cluster->BSGroup(1), 1, TErasureType::ErasureNone, 2,
                     TVDiskID(0, 1, 0, 0, 0), TVDiskID(0, 1, 0, 1, 0));
        CheckVDisk(cluster->VDisk({0, 1, 0, 0, 0}), 1, 1);
        CheckVDisk(cluster->VDisk({0, 1, 0, 1, 0}), 1, 1);

        cluster->AddPDisk(MakePDiskConfig(3, 3));
        cluster->UpdatePDiskState(NCms::TPDiskID(3, 3), MakePDiskInfo(3));
        UNIT_ASSERT(cluster->HasPDisk(NCms::TPDiskID(3, 3)));
        CheckPDisk(cluster->PDisk(NCms::TPDiskID(3, 3)), 3, 3, UP, 0);

        cluster->AddVDisk(MakeVSlotConfig(3, {0, 1, 0, 2, 0}, 3, 0));
        cluster->UpdateVDiskState({0, 1, 0, 2, 0}, MakeVDiskInfo({0, 1, 0, 2, 0}, 3, 0));
        UNIT_ASSERT(cluster->HasVDisk({0, 1, 0, 2, 0}));
        CheckVDisk(cluster->VDisk({0, 1, 0, 2, 0}), TVDiskID(0, 1, 0, 2, 0), 3, UP, 3, 0);

        cluster->AddBSGroup(MakeBSGroup(2, "none", 1, 1, 0, 3, 3, 0));
        UNIT_ASSERT(cluster->HasBSGroup(2));
        CheckBSGroup(cluster->BSGroup(2), 2, TErasureType::ErasureNone, 2,
                     TVDiskID(0, 1, 0, 0, 0), TVDiskID(0, 1, 0, 2, 0));
        CheckVDisk(cluster->VDisk({0, 1, 0, 0, 0}), 2, 1, 2);
        UNIT_ASSERT(cluster->HasVDisk({0, 1, 0, 2, 0}));
        CheckVDisk(cluster->VDisk({0, 1, 0, 2, 0}), TVDiskID(0, 1, 0, 2, 0), 3, UP, 3, 1, 2);

        cluster->AddTablet(1, MakeTabletInfo(1, TTabletTypes::Hive, TTabletStateInfo::Active, true));
        UNIT_ASSERT(cluster->HasTablet(1));
        UNIT_ASSERT(!cluster->HasTablet(2));
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(1).Tablets.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Tablet(1).TabletId, 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Tablet(1).Type, TTabletTypes::Hive);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Tablet(1).State, TTabletStateInfo::Active);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Tablet(1).Leader, true);

        auto now = Now();
        cluster->SetTimestamp(now);
        TPermissionInfo permission;
        permission.PermissionId = "1";
        permission.Owner = "user";
        permission.Action = MakeAction(TAction::SHUTDOWN_HOST, "test1", 60000000);
        permission.Deadline = now - TDuration::Seconds(61);
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddLocks(permission, nullptr), 0);

        permission.Action.SetHost("test2");
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddLocks(permission, nullptr), 0);

        permission.Deadline = now + TDuration::Seconds(60);
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddLocks(permission, nullptr), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("test2").size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("test2")[0]->Lock->Action.GetType(), TAction::SHUTDOWN_HOST);
        UNIT_ASSERT_VALUES_EQUAL(cluster->HostNodes("test2")[0]->Lock->ActionDeadline, now + TDuration::Minutes(2));

        permission.Action.SetHost("2");
        permission.Deadline = now - TDuration::Seconds(30);
        cluster->SetNodeState(2, DOWN, MakeSystemStateInfo("1"));
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddLocks(permission, nullptr), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(2).State, EState::RESTART);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(2).Lock->ActionDeadline, now + TDuration::Seconds(30));

        cluster->ClearNode(1);
        UNIT_ASSERT(!cluster->HasTablet(1));
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(1).Tablets.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(1).State, DOWN);

        auto point1 = cluster->PushRollbackPoint();
        auto action1 = MakeAction(TAction::SHUTDOWN_HOST, 3, 60000000);
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddTempLocks(action1, nullptr), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(3).TempLocks.size(), 1);

        cluster->RollbackLocks(point1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->Node(3).TempLocks.size(), 0);

        auto point2 = cluster->PushRollbackPoint();
        auto action2 = MakeAction(TAction::REPLACE_DEVICES, 3, 60000000,
                                  "pdisk-1-1", "vdisk-0-1-0-1-0");
        permission.Action = action2;
        permission.Deadline = now + TDuration::Seconds(60);
        UNIT_ASSERT(!cluster->PDisk(NCms::TPDiskID(1, 1)).Lock.Defined());
        UNIT_ASSERT(!cluster->VDisk(TVDiskID(0, 1, 0, 1, 0)).Lock.Defined());
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddLocks(permission, nullptr), 2);
        UNIT_ASSERT(cluster->PDisk(NCms::TPDiskID(1, 1)).Lock.Defined());
        UNIT_ASSERT(cluster->VDisk(TVDiskID(0, 1, 0, 1, 0)).Lock.Defined());
        UNIT_ASSERT_VALUES_EQUAL(cluster->AddTempLocks(action2, nullptr), 2);
        UNIT_ASSERT_VALUES_EQUAL(cluster->PDisk(NCms::TPDiskID(1, 1)).TempLocks.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cluster->VDisk(TVDiskID(0, 1, 0, 1, 0)).TempLocks.size(), 1);
        cluster->RollbackLocks(point2);
        UNIT_ASSERT(cluster->PDisk(NCms::TPDiskID(1, 1)).Lock.Defined());
        UNIT_ASSERT(cluster->VDisk(TVDiskID(0, 1, 0, 1, 0)).Lock.Defined());
        UNIT_ASSERT_VALUES_EQUAL(cluster->PDisk(NCms::TPDiskID(1, 1)).TempLocks.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cluster->VDisk(TVDiskID(0, 1, 0, 1, 0)).TempLocks.size(), 0);

        auto request1 = MakeRequest("request-1", "user-1", 1,
                                    MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                               "pdisk-1-1", "vdisk-0-1-0-1-0"),
                                    MakeAction(TAction::SHUTDOWN_HOST, 1, 60000000));
        auto request2 = MakeRequest("request-2", "user-2", 2,
                                    MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                               "pdisk-1-1", "vdisk-0-1-0-1-0"),
                                    MakeAction(TAction::SHUTDOWN_HOST, 3, 60000000));
        auto request3 = MakeRequest("request-3", "user-3", 3,
                                    MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                               "pdisk-1-1", "vdisk-0-1-0-1-0"));
        auto request4 = MakeRequest("request-4", "user-4", 4,
                                    MakeAction(TAction::REPLACE_DEVICES, 1, 60000000,
                                               "pdisk-1-1", "vdisk-0-1-0-1-0"));
        UNIT_ASSERT_VALUES_EQUAL(cluster->ScheduleActions(request2, nullptr), 3);
        UNIT_ASSERT_VALUES_EQUAL(cluster->ScheduleActions(request4, nullptr), 2);
        UNIT_ASSERT_VALUES_EQUAL(cluster->ScheduleActions(request3, nullptr), 2);
        UNIT_ASSERT_VALUES_EQUAL(cluster->ScheduleActions(request1, nullptr), 3);

        CheckScheduledLocks(cluster->Node(1), "request-1", "user-1", 1);
        CheckScheduledLocks(cluster->Node(3), "request-2", "user-2", 2);
        CheckScheduledLocks(cluster->PDisk(NCms::TPDiskID(1, 1)), "request-1", "user-1", 1,
                            "request-2", "user-2", 2,
                            "request-3", "user-3", 3,
                            "request-4", "user-4", 4);
        CheckScheduledLocks(cluster->VDisk({0, 1, 0, 1, 0}),
                            "request-1", "user-1", 1,
                            "request-2", "user-2", 2,
                            "request-3", "user-3", 3,
                            "request-4", "user-4", 4);

        cluster->DeactivateScheduledLocks(request2.Priority);

        TErrorInfo error;
        UNIT_ASSERT(cluster->Node(1).IsLocked(error, TDuration(), Now(), TDuration()));
        UNIT_ASSERT(!cluster->Node(3).IsLocked(error, TDuration(), Now(), TDuration()));

        cluster->ReactivateScheduledLocks();

        UNIT_ASSERT(cluster->Node(1).IsLocked(error, TDuration(), Now(), TDuration()));
        UNIT_ASSERT(cluster->Node(3).IsLocked(error, TDuration(), Now(), TDuration()));

        cluster->UnscheduleActions(request3.RequestId);
        cluster->UnscheduleActions(request2.RequestId);

        CheckScheduledLocks(cluster->Node(1), "request-1", "user-1", 1);
        UNIT_ASSERT(cluster->Node(3).ScheduledLocks.empty());
        CheckScheduledLocks(cluster->PDisk(NCms::TPDiskID(1, 1)), "request-1", "user-1", 1,
                            "request-4", "user-4", 4);
        CheckScheduledLocks(cluster->VDisk({0, 1, 0, 1, 0}),
                            "request-1", "user-1", 1,
                            "request-4", "user-4", 4);

    }
}

} // NCmsTest
} // NKikimr
