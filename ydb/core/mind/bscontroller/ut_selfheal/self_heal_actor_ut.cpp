#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/util/actorsys_test/testactorsys.h>
#include <ydb/core/mind/bscontroller/self_heal.h>
#include <ydb/core/base/blobstorage_common.h>
#include <ydb/core/mind/bscontroller/impl.h>
#include <ydb/core/mind/bscontroller/layout_helpers.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;

using E = NKikimrBlobStorage::EVDiskStatus;

template<typename TCallback>
void RunTestCase(TCallback&& callback) {
    TTestActorSystem runtime(1);
    runtime.Start();
    const TActorId& parentId = runtime.AllocateEdgeActor(1);
    TBlobStorageController Controller({}, new TTabletStorageInfo(1, TTabletTypes::BSController));

    Controller.CreateEmptyHostRecordsMap();
    const TActorId& selfHealId = runtime.Register(Controller.CreateSelfHealActor(), parentId, {}, {}, 1);

    callback(selfHealId, parentId, runtime);
    runtime.Stop();
}

class TVDiskResponder : public TActor<TVDiskResponder> {
    const bool Replicated;

public:
    explicit TVDiskResponder(bool replicated = true)
        : TActor(&TThis::StateFunc)
        , Replicated(replicated)
    {}

    void Handle(TEvBlobStorage::TEvVStatus::TPtr ev) {
        Send(ev->Sender, new TEvBlobStorage::TEvVStatusResult(NKikimrProto::OK,
            VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()), true, Replicated, false, 1));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvBlobStorage::TEvVStatus, Handle);
    )
};

void RegisterDiskResponders(TTestActorSystem& runtime, const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        const std::set<ui32>& unreplicatedIndexes = {}) {
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        runtime.RegisterService(info->GetActorId(i),
            runtime.Register(new TVDiskResponder(!unreplicatedIndexes.count(i)), 1));
    }
}

TIntrusivePtr<TBlobStorageGroupInfo> CreateGroup(ui32 groupId = 0x82000000) {
    TVector<TActorId> actorIds;
    for (ui32 i = 0; i < 8; ++i) {
        actorIds.push_back(MakeBlobStorageVDiskID(1, 1000 + i, 1000));
    }
    return MakeIntrusive<TBlobStorageGroupInfo>(TBlobStorageGroupType::Erasure4Plus2Block, 1u, 0u, 1u, &actorIds,
        TBlobStorageGroupInfo::EEM_NONE, TBlobStorageGroupInfo::ELCP_INITIAL, TCypherKey(), TGroupId::FromValue(groupId));
}

TEvControllerUpdateSelfHealInfo::TGroupContent Convert(const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        const std::set<ui32>& faultyIndexes, const std::vector<E>& status,
        const std::set<ui32>& maintenanceIndexes = {}) {
    TEvControllerUpdateSelfHealInfo::TGroupContent res;
    res.Generation = info->GroupGeneration;
    res.Type = info->Type;
    res.Geometry = std::make_shared<TGroupGeometryInfo>(CreateGroupGeometry(TBlobStorageGroupType::Erasure4Plus2Block));
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        auto& x = res.VDisks[info->GetVDiskId(i)];
        x.Location = {1, 1000 + i, 1000};
        x.ReassignmentReason = ESelfHealReassignmentReason::None;
        if (faultyIndexes.count(i)) {
            x.ReassignmentReason = ESelfHealReassignmentReason::Faulty;
        } else if (maintenanceIndexes.count(i)) {
            x.ReassignmentReason = ESelfHealReassignmentReason::Maintenance;
        }
        x.UnavailabilityRisk = faultyIndexes.count(i);
        x.Decommitted = false;
        x.OnlyPhantomsRemain = false;
        x.IsReady = !x.UnavailabilityRisk;
        x.ReadySince = TMonotonic::Zero();
        x.VDiskStatus = i < status.size() ? status[i] : E::READY;
    }
    return res;
}

void ValidateCmd(const TActorId& parentId, TTestActorSystem& runtime, ui32 groupId, ui32 groupGeneration,
        ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx) {
    auto res = runtime.WaitForEdgeActorEvent({parentId});
    auto *m = res->Get<TEvBlobStorage::TEvControllerConfigRequest>();
    UNIT_ASSERT(m->SelfHeal);
    auto& record = m->Record;
    auto& request = record.GetRequest();
    UNIT_ASSERT_VALUES_EQUAL(request.CommandSize(), 1);
    auto& cmd = request.GetCommand(0);
    UNIT_ASSERT(cmd.HasReassignGroupDisk());
    auto& reassign = cmd.GetReassignGroupDisk();
    UNIT_ASSERT_VALUES_EQUAL(reassign.GetGroupId(), groupId);
    UNIT_ASSERT_VALUES_EQUAL(reassign.GetGroupGeneration(), groupGeneration);
    UNIT_ASSERT_VALUES_EQUAL(reassign.GetFailRealmIdx(), failRealmIdx);
    UNIT_ASSERT_VALUES_EQUAL(reassign.GetFailDomainIdx(), failDomainIdx);
    UNIT_ASSERT_VALUES_EQUAL(reassign.GetVDiskIdx(), vdiskIdx);
}

void ValidateNoCmd(const TActorId& parentId, TTestActorSystem& runtime) {
    runtime.Schedule(TDuration::Minutes(30),
        new IEventHandle(TEvents::TSystem::Wakeup, 0, parentId, {}, nullptr, 0), nullptr, 1);
    auto res = runtime.WaitForEdgeActorEvent({parentId});
    UNIT_ASSERT_EQUAL(res->GetTypeRewrite(), TEvents::TSystem::Wakeup);
}

Y_UNIT_TEST_SUITE(SelfHealActorTest) {

    Y_UNIT_TEST(SingleErrorDisk) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0}, {E::ERROR});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000000, 1, 0, 0, 0);
        });
    }

    Y_UNIT_TEST(NoMoreThanOneReplicating) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0}, {E::ERROR, E::REPLICATING});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);
        });
    }

    Y_UNIT_TEST(FaultyDiskBeforeMaintenanceDiskInGroup) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {1}, {}, {0});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000000, 1, 0, 1, 0);
        });
    }

    Y_UNIT_TEST(FaultyGroupBeforeMaintenanceGroup) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto maintenanceGroup = CreateGroup(0x82000000);
            auto faultyGroup = CreateGroup(0x82000001);
            RegisterDiskResponders(runtime, maintenanceGroup);

            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[maintenanceGroup->GroupID] = Convert(maintenanceGroup, {}, {}, {0});
            ev->GroupsToUpdate[faultyGroup->GroupID] = Convert(faultyGroup, {0}, {});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000001, 1, 0, 0, 0);
        });
    }

    Y_UNIT_TEST(MaintenanceWaitsForFullyReplicatedCachedState) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto content = Convert(info, {}, {}, {0});
            auto& replicatingVDisk = content.VDisks.at(info->GetVDiskId(1));
            replicatingVDisk.VDiskStatus = E::REPLICATING;
            replicatingVDisk.OnlyPhantomsRemain = true;
            replicatingVDisk.IsReady = false;

            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = std::move(content);
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);
        });
    }

    Y_UNIT_TEST(MaintenanceTreatsOnlyPhantomsAsUnreplicated) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto content = Convert(info, {}, {}, {0});
            auto& phantomOnlyVDisk = content.VDisks.at(info->GetVDiskId(1));
            phantomOnlyVDisk.OnlyPhantomsRemain = true;

            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = std::move(content);
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);
        });
    }

    Y_UNIT_TEST(MaintenanceWaitsForFullyReplicatedLiveState) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info, {1});
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {}, {}, {0});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);
        });
    }

    Y_UNIT_TEST(MaintenanceReassignsWhenOtherDisksAreFullyOperational) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {}, {}, {0});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000000, 1, 0, 0, 0);
        });
    }

    Y_UNIT_TEST(FaultyReassignmentKeepsQuorumBasedLiveCheck) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info, {1});
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0}, {});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000000, 1, 0, 0, 0);
        });
    }

}
