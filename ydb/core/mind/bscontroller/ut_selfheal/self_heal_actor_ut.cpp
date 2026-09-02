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
public:
    TVDiskResponder()
        : TActor(&TThis::StateFunc)
    {}

    void Handle(TEvBlobStorage::TEvVStatus::TPtr ev) {
        Send(ev->Sender, new TEvBlobStorage::TEvVStatusResult(NKikimrProto::OK,
            VDiskIDFromVDiskID(ev->Get()->Record.GetVDiskID()), true, true, false, 1));
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvBlobStorage::TEvVStatus, Handle);
    )
};

void RegisterDiskResponders(TTestActorSystem& runtime, const TIntrusivePtr<TBlobStorageGroupInfo>& info) {
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        runtime.RegisterService(info->GetActorId(i), runtime.Register(new TVDiskResponder, 1));
    }
}

TIntrusivePtr<TBlobStorageGroupInfo> CreateGroup(TBlobStorageGroupType::EErasureSpecies erasure =
        TBlobStorageGroupType::Erasure4Plus2Block) {
    const TBlobStorageGroupType groupType(erasure);
    const ui32 numFailRealms = erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 1;
    const ui32 numFailDomains = erasure == TBlobStorageGroupType::ErasureMirror3dc ? 3 : 0;
    const ui32 numVDisks = numFailRealms * (numFailDomains ? numFailDomains : groupType.BlobSubgroupSize());
    TVector<TActorId> actorIds;
    for (ui32 i = 0; i < numVDisks; ++i) {
        actorIds.push_back(MakeBlobStorageVDiskID(1, 1000 + i, 1000));
    }
    return MakeIntrusive<TBlobStorageGroupInfo>(groupType, 1u, numFailDomains, numFailRealms, &actorIds,
        TBlobStorageGroupInfo::EEM_NONE, TBlobStorageGroupInfo::ELCP_INITIAL, TCypherKey(),
        TGroupId::FromValue(0x82000000));
}

TEvControllerUpdateSelfHealInfo::TGroupContent Convert(const TIntrusivePtr<TBlobStorageGroupInfo>& info,
        std::set<ui32> faultyIndexes, std::vector<E> status, std::set<ui32> phantomsOnlyIndexes = {}) {
    TEvControllerUpdateSelfHealInfo::TGroupContent res;
    res.Generation = info->GroupGeneration;
    res.Type = info->Type;
    res.Geometry = std::make_shared<TGroupGeometryInfo>(CreateGroupGeometry(info->Type));
    for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
        auto& x = res.VDisks[info->GetVDiskId(i)];
        x.Location = {1, 1000 + i, 1000};
        x.ReassignmentPriority = faultyIndexes.count(i)
            ? ESelfHealReassignmentPriority::DriveStatus
            : ESelfHealReassignmentPriority::None;
        x.UnavailabilityRisk = faultyIndexes.count(i);
        x.Decommitted = false;
        x.VDiskStatus = i < status.size() ? status[i] : E::READY;
        x.OnlyPhantomsRemain = x.VDiskStatus == E::REPLICATING && phantomsOnlyIndexes.count(i);
        x.IsReady = x.VDiskStatus == E::READY;
        x.ReadySince = TMonotonic::Zero();
    }
    return res;
}

void ValidateCmd(const TActorId& parentId, TTestActorSystem& runtime, ui32 groupId, ui32 groupGeneration,
        ui32 failRealmIdx, ui32 failDomainIdx, ui32 vdiskIdx, bool isSelfHealReasonDecommit = false) {
    auto res = runtime.WaitForEdgeActorEvent({parentId});
    UNIT_ASSERT_EQUAL(res->GetTypeRewrite(), TEvBlobStorage::TEvControllerConfigRequest::EventType);
    auto *m = res->Get<TEvBlobStorage::TEvControllerConfigRequest>();
    UNIT_ASSERT(m->SelfHeal);
    auto& record = m->Record;
    auto& request = record.GetRequest();
    UNIT_ASSERT_VALUES_EQUAL(request.GetIsSelfHealReasonDecommit(), isSelfHealReasonDecommit);
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

    Y_UNIT_TEST(ReadyFaultyDiskWithPhantomsOnlyIsNotReassigned) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0}, {E::READY, E::REPLICATING}, {1});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);
        });
    }

    Y_UNIT_TEST(ReadyFaultyDiskWaitsForPhantomsOnlyReplicationToFinish) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);

            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0}, {E::READY, E::REPLICATING}, {1});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);

            auto update = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            update->VDiskStatusUpdate.push_back({
                .VDiskId = info->GetVDiskId(1),
                .OnlyPhantomsRemain = false,
                .IsReady = true,
                .VDiskStatus = E::READY,
            });
            runtime.Send(new IEventHandle(selfHealId, parentId, update.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000000, 1, 0, 0, 0);
        });
    }

    Y_UNIT_TEST(UnavailableFaultyDiskWithPhantomsOnlyIsReassigned) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0}, {E::ERROR, E::REPLICATING}, {1});
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, 0x82000000, 1, 0, 0, 0);
        });
    }

    Y_UNIT_TEST(DriveStatusHasHighestPriority) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup();
            RegisterDiskResponders(runtime, info);
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            auto& content = ev->GroupsToUpdate[info->GroupID].emplace(Convert(info, {2}, {}));
            content.VDisks.at(info->GetVDiskId(0)).ReassignmentPriority =
                ESelfHealReassignmentPriority::MaintenanceStatus;
            content.VDisks.at(info->GetVDiskId(1)).ReassignmentPriority =
                ESelfHealReassignmentPriority::DecommitStatus;
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            const TVDiskID expected = info->GetVDiskId(2);
            ValidateCmd(parentId, runtime, expected.GroupID.GetRawId(), expected.GroupGeneration,
                expected.FailRealm, expected.FailDomain, expected.VDisk);
        });
    }

    Y_UNIT_TEST(DecommitIsUrgentAndPropagatesReason) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup(TBlobStorageGroupType::ErasureMirror3dc);
            RegisterDiskResponders(runtime, info);
            std::vector<E> status(4, E::READY);
            status[3] = E::ERROR;
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            auto& content = ev->GroupsToUpdate[info->GroupID].emplace(Convert(info, {}, std::move(status)));
            const TVDiskID expected = info->GetVDiskId(4);
            content.VDisks.at(expected).ReassignmentPriority = ESelfHealReassignmentPriority::DecommitStatus;
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateCmd(parentId, runtime, expected.GroupID.GetRawId(), expected.GroupGeneration,
                expected.FailRealm, expected.FailDomain, expected.VDisk, true);
        });
    }

    Y_UNIT_TEST(MaintenanceWaitsForOtherNonReadyVDisk) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup(TBlobStorageGroupType::ErasureMirror3dc);
            RegisterDiskResponders(runtime, info);
            std::vector<E> status(4, E::READY);
            status[3] = E::ERROR;
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            auto& content = ev->GroupsToUpdate[info->GroupID].emplace(Convert(info, {}, std::move(status)));
            content.VDisks.at(info->GetVDiskId(4)).ReassignmentPriority =
                ESelfHealReassignmentPriority::MaintenanceStatus;
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            ValidateNoCmd(parentId, runtime);
        });
    }

    Y_UNIT_TEST(TriesEveryCandidateWithSamePriority) {
        RunTestCase([&](const TActorId& selfHealId, const TActorId& parentId, TTestActorSystem& runtime) {
            auto info = CreateGroup(TBlobStorageGroupType::ErasureMirror3dc);
            RegisterDiskResponders(runtime, info);
            std::vector<E> status(4, E::READY);
            status[3] = E::ERROR;
            auto ev = std::make_unique<TEvControllerUpdateSelfHealInfo>();
            ev->GroupsToUpdate[info->GroupID] = Convert(info, {0, 4}, std::move(status));
            runtime.Send(new IEventHandle(selfHealId, parentId, ev.release()), 1);
            const TVDiskID expected = info->GetVDiskId(4);
            ValidateCmd(parentId, runtime, expected.GroupID.GetRawId(), expected.GroupGeneration,
                expected.FailRealm, expected.FailDomain, expected.VDisk);
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

}
