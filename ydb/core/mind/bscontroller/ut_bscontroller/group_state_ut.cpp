#include <ydb/core/mind/bscontroller/group_state.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;

namespace {
using TFailRealm = NKikimrBlobStorage::TDataCenterState;

struct TVDiskReplicationStatus {
    bool Replicated = true;
    std::optional<bool> DurabilityReplicated = true;
    bool FailSecondRealm = false;
    std::optional<ui32> UnreplicatedRealm;
    bool ObsoleteGeneration = false;
};

class TGroupStateFixture {
public:
    TTestBasicRuntime Runtime;
    TActorId Controller;
    TActorId Actor;
    TGroupReplicationInfo Info;

    TGroupStateFixture() {
        TAppPrepare app;
        Runtime.Initialize(app.Unwrap());
        Controller = Runtime.AllocateEdgeActor();
        Actor = Runtime.Register(CreateGroupStateActor(Controller));
        Runtime.EnableScheduleForActor(Actor);
        for (ui32 realm = 0; realm < 3; ++realm) {
            for (ui32 domain = 0; domain < 3; ++domain) {
                Info.VDisks.push_back({TVDiskID(42, 1, realm, domain, 0), Runtime.AllocateEdgeActor()});
            }
        }
    }

    void Update(ui32 generation, EMirror3dcGroupState state, const TFailRealmStates& states) {
        Info.Generation = generation;
        Info.States = states;
        Info.GroupState = state;
        for (TGroupReplicationInfo::TVDisk& disk : Info.VDisks) {
            disk.Id.GroupGeneration = generation;
        }
        std::unique_ptr<TEvGroupState::TEvUpdate> event = std::make_unique<TEvGroupState::TEvUpdate>(42);
        event->Info = Info;
        Runtime.Send(new IEventHandle(Actor, Controller, event.release()), 0, true);
    }

    void ReplyRound(ui32 requests, const TVDiskReplicationStatus& status = {}) {
        TSet<TActorId> disks;
        for (const TGroupReplicationInfo::TVDisk& disk : Info.VDisks) {
            disks.insert(disk.ActorId);
        }
        for (ui32 index = 0; index < requests; ++index) {
            TEvBlobStorage::TEvVStatus::TPtr request = Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVStatus>(disks);
            TVDiskID disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
            UNIT_ASSERT_VALUES_EQUAL(disk.GroupGeneration, Info.Generation);
            const bool available = (!status.FailSecondRealm || disk.FailRealm != 1 || disk.FailDomain != 0)
                && (!status.UnreplicatedRealm || disk.FailRealm != *status.UnreplicatedRealm);
            if (status.ObsoleteGeneration) {
                --disk.GroupGeneration;
            }
            std::unique_ptr<TEvBlobStorage::TEvVStatusResult> result =
                std::make_unique<TEvBlobStorage::TEvVStatusResult>(NKikimrProto::OK, disk, true,
                    status.Replicated && available, false, 1);
            if (status.DurabilityReplicated) {
                result->Record.SetDurabilityReplicated(*status.DurabilityReplicated && available);
            }
            Runtime.Send(new IEventHandle(request->Sender, request->Recipient, result.release(), 0, request->Cookie), 0, true);
        }
    }

    void AssertNoRequests() {
        UNIT_ASSERT(Runtime.CaptureMailboxEvents(Controller.Hint(), Controller.NodeId()).empty());
        for (const TGroupReplicationInfo::TVDisk& disk : Info.VDisks) {
            UNIT_ASSERT(Runtime.CaptureMailboxEvents(disk.ActorId.Hint(), disk.ActorId.NodeId()).empty());
        }
    }

    TMirror3dcGroupConfiguration GetProposal() {
        TEvBlobStorage::TEvControllerConfigRequest::TPtr result =
            Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigRequest>(Controller);
        const NKikimrBlobStorage::TConfigRequest& request = result->Get()->Record.GetRequest();
        UNIT_ASSERT(!request.GetIgnoreGroupFailModelChecks());
        UNIT_ASSERT(!request.GetIgnoreDegradedGroupsChecks());
        UNIT_ASSERT(!request.GetIgnoreDisintegratedGroupsChecks());
        const NKikimrBlobStorage::TDefineGroupState& command =
            result->Get()->Record.GetRequest().GetCommand(0).GetDefineGroupState();
        UNIT_ASSERT_VALUES_EQUAL(command.GroupsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(command.GetGroups(0).GetExpectedGroupGeneration(), Info.Generation);
        TFailRealmStates states;
        for (ui32 realm = 0; realm < 3; ++realm) {
            states[realm] = static_cast<EMirror3dcFailRealmState>(command.GetGroups(0).GetFailRealmStates(realm));
        }
        UNIT_ASSERT(command.GetGroups(0).HasGroupState());
        return {command.GetGroups(0).GetGroupState(), states};
    }
};
}

Y_UNIT_TEST_SUITE(BsControllerGroupState) {
    Y_UNIT_TEST(StableFailureDoesNotStartReplication) {
        TGroupStateFixture fixture;
        fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_FAILURE, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED});
        fixture.AssertNoRequests();
    }
    Y_UNIT_TEST(PendingWaitsForAllTargetRealms) {
        TGroupStateFixture fixture;
        fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1_PENDING, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED});
        TVDiskReplicationStatus status;
        status.FailSecondRealm = true;
        fixture.ReplyRound(6, status);
        fixture.AssertNoRequests();
        fixture.ReplyRound(6);
        const auto next = fixture.GetProposal();
        UNIT_ASSERT(next.State == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1);
        UNIT_ASSERT((next.Realms == TFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}));
    }
    Y_UNIT_TEST(RecoveryPendingChangesPolicyWithoutChangingRealmTuple) {
        TGroupStateFixture fixture;
        fixture.Update(2, NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2_PENDING, {TFailRealm::EXCLUDED, TFailRealm::INCLUDED, TFailRealm::RECOVERING});
        fixture.ReplyRound(3);
        const auto next = fixture.GetProposal();
        UNIT_ASSERT(next.State == NKikimrBlobStorage::TGroupState::DC_SOLO_RECOVERY_INV2);
        UNIT_ASSERT(next.Realms == fixture.Info.States);
    }
    Y_UNIT_TEST(RecoveryQueriesOnlyR) {
        TGroupStateFixture fixture;
        fixture.Update(3, NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::INCLUDED});
        fixture.ReplyRound(3);
        const auto next = fixture.GetProposal();
        UNIT_ASSERT(next.State == NKikimrBlobStorage::TGroupState::NORMAL);
        UNIT_ASSERT((next.Realms == TFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED}));
    }
    Y_UNIT_TEST(StaleOrIncompleteStatusCannotFinishTransition) {
        for (unsigned mode = 0; mode < 3; ++mode) {
            TGroupStateFixture fixture;
            fixture.Update(4, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED});
            TVDiskReplicationStatus status;
            if (mode == 0) {
                status.ObsoleteGeneration = true;
            } else if (mode == 1) {
                status.DurabilityReplicated.reset();
            } else {
                status.DurabilityReplicated = false;
            }
            fixture.ReplyRound(3, status);
            fixture.AssertNoRequests();
        }
    }
    Y_UNIT_TEST(GroupUpdateKeepsOtherGroupsQueries) {
        TGroupStateFixture fixture;
        const TFailRealmStates initial{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, initial);
        TGroupReplicationInfo other = fixture.Info;
        for (TGroupReplicationInfo::TVDisk& disk : other.VDisks) {
            disk.Id.GroupID = TGroupId::FromValue(43);
            disk.ActorId = fixture.Runtime.AllocateEdgeActor();
        }
        std::unique_ptr<TEvGroupState::TEvUpdate> update = std::make_unique<TEvGroupState::TEvUpdate>(43);
        update->Info = other;
        fixture.Runtime.Send(new IEventHandle(fixture.Actor, fixture.Controller, update.release()), 0, true);
        TVector<TEvBlobStorage::TEvVStatus::TPtr> requests;
        for (const TGroupReplicationInfo* info : {&fixture.Info, &other}) {
            for (const TGroupReplicationInfo::TVDisk& disk : info->VDisks) {
                if (disk.Id.FailRealm != 2) {
                    requests.push_back(fixture.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvVStatus>(disk.ActorId));
                }
            }
        }
        fixture.Update(2, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, initial);
        // Late replies for group 42 are ignored; current replies for group 43 must still complete it.
        for (const TEvBlobStorage::TEvVStatus::TPtr& request : requests) {
            const TVDiskID disk = VDiskIDFromVDiskID(request->Get()->Record.GetVDiskID());
            std::unique_ptr<TEvBlobStorage::TEvVStatusResult> result =
                std::make_unique<TEvBlobStorage::TEvVStatusResult>(NKikimrProto::OK, disk, true, true, false, 1);
            result->Record.SetDurabilityReplicated(true);
            fixture.Runtime.Send(new IEventHandle(request->Sender, request->Recipient,
                result.release(), 0, request->Cookie), 0, true);
        }
        TEvBlobStorage::TEvControllerConfigRequest::TPtr proposal =
            fixture.Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigRequest>(fixture.Controller);
        const NKikimrBlobStorage::TDefineGroupState::TGroupDefinition& definition =
            proposal->Get()->Record.GetRequest().GetCommand(0).GetDefineGroupState().GetGroups(0);
        UNIT_ASSERT_VALUES_EQUAL(definition.GetGroupId(), 43);
        UNIT_ASSERT_VALUES_EQUAL(definition.GetExpectedGroupGeneration(), 1);
        fixture.ReplyRound(6);
        UNIT_ASSERT((fixture.GetProposal().Realms == TFailRealmStates{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}));
    }


}
