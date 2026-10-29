#include <ydb/core/mind/bscontroller/group_state.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NBsController;

namespace {
using TFailRealm = NKikimrBlobStorage::TGroupInfo::TFailRealm;

struct TVDiskReplicationStatus {
    bool Replicated = true;
    std::optional<bool> DurabilityReplicated = true;
    bool FailSecondRealm = false;
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

    void Update(ui32 generation, const TFailRealmStates& states) {
        Info.Generation = generation;
        Info.States = states;
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
            const bool available = !status.FailSecondRealm || disk.FailRealm != 1 || disk.FailDomain != 0;
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

    TFailRealmStates GetProposal() {
        TEvBlobStorage::TEvControllerConfigRequest::TPtr result =
            Runtime.GrabEdgeEventRethrow<TEvBlobStorage::TEvControllerConfigRequest>(Controller);
        const NKikimrBlobStorage::TDefineFailRealmStates& command =
            result->Get()->Record.GetRequest().GetCommand(0).GetDefineFailRealmStates();
        UNIT_ASSERT_VALUES_EQUAL(command.GroupsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(command.GetGroups(0).GetExpectedGroupGeneration(), Info.Generation);
        TFailRealmStates states;
        for (ui32 realm = 0; realm < 3; ++realm) {
            states[realm] = static_cast<EMirror3dcFailRealmState>(command.GetGroups(0).GetFailRealmStates(realm));
        }
        return states;
    }
};
}

Y_UNIT_TEST_SUITE(BsControllerGroupState) {
    Y_UNIT_TEST(FullReplicationStatusSupportsLegacyReplies) {
        TEvBlobStorage::TEvVStatusResult status(NKikimrProto::OK, TVDiskID(42, 1, 0, 0, 0), true, true, false, 1);
        UNIT_ASSERT(status.IsFullyReplicated());
        status.Record.SetDurabilityReplicated(false);
        UNIT_ASSERT(!status.IsFullyReplicated());
        status.Record.SetDurabilityReplicated(true);
        UNIT_ASSERT(status.IsFullyReplicated());
        status.Record.SetReplicated(false);
        UNIT_ASSERT(!status.IsFullyReplicated());
    }

    Y_UNIT_TEST(DurabilityTargets) {
        const TFailRealmStates oneCopy{TFailRealm::N0, TFailRealm::N0, TFailRealm::F};
        UNIT_ASSERT(GetReplicatedRealmState(oneCopy, 0) == TFailRealm::N1);
        UNIT_ASSERT(!GetReplicatedRealmState(oneCopy, 2));
        const TFailRealmStates twoCopies{TFailRealm::N1, TFailRealm::N1, TFailRealm::F};
        UNIT_ASSERT(GetReplicatedRealmState(twoCopies, 0) == TFailRealm::N2);
        const TFailRealmStates recovering{TFailRealm::N0, TFailRealm::R, TFailRealm::F};
        UNIT_ASSERT(GetReplicatedRealmState(recovering, 1) == TFailRealm::N1);
        const TFailRealmStates recoveryComplete{TFailRealm::N2, TFailRealm::N2, TFailRealm::R};
        UNIT_ASSERT(GetReplicatedRealmState(recoveryComplete, 2) == TFailRealm::N1);
        UNIT_ASSERT(!GetReplicatedRealmState(recoveryComplete, 0));
        UNIT_ASSERT(GetReplicatedRealmState({TFailRealm::N0, TFailRealm::F, TFailRealm::F}, 0) == TFailRealm::N1);
        UNIT_ASSERT(GetReplicatedRealmState({TFailRealm::N1, TFailRealm::F, TFailRealm::F}, 0) == TFailRealm::N2);
        UNIT_ASSERT(!GetReplicatedRealmState({TFailRealm::F, TFailRealm::F, TFailRealm::F}, 0));
    }

    Y_UNIT_TEST(WaitsForEveryDiskAndTracksChangedDurability) {
        TGroupStateFixture fixture;
        fixture.Update(1, {TFailRealm::N0, TFailRealm::N0, TFailRealm::F});
        fixture.ReplyRound(6, {.FailSecondRealm = true});
        UNIT_ASSERT((fixture.GetProposal() == TFailRealmStates{TFailRealm::N1, TFailRealm::N0, TFailRealm::F}));
        fixture.Update(2, {TFailRealm::N1, TFailRealm::N0, TFailRealm::F});
        // These replies carry the current query cookies, but an obsolete group generation.
        fixture.ReplyRound(3, {.ObsoleteGeneration = true});
        fixture.ReplyRound(3);
        UNIT_ASSERT((fixture.GetProposal() == TFailRealmStates{TFailRealm::N1, TFailRealm::N1, TFailRealm::F}));
        // D changed from 1+1 to 2+2. Completion of the old round cannot prove N2.
        fixture.Update(3, {TFailRealm::N1, TFailRealm::N1, TFailRealm::F});
        fixture.ReplyRound(6);
        UNIT_ASSERT((fixture.GetProposal() == TFailRealmStates{TFailRealm::N2, TFailRealm::N2, TFailRealm::F}));
        fixture.Runtime.Send(new IEventHandle(fixture.Actor, fixture.Controller, new TEvents::TEvPoison), 0, true);
    }

    Y_UNIT_TEST(RequiresOrdinaryAndDurabilityReplication) {
        TGroupStateFixture fixture;
        fixture.Update(1, {TFailRealm::N1, TFailRealm::N1, TFailRealm::F});
        fixture.ReplyRound(6, {.DurabilityReplicated = false});
        fixture.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(fixture.Runtime.CaptureMailboxEvents(fixture.Controller.Hint(), fixture.Controller.NodeId()).empty());
        // A VDisk that does not advertise the new field cannot confirm D.
        fixture.ReplyRound(6, {.DurabilityReplicated = std::nullopt});
        fixture.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(fixture.Runtime.CaptureMailboxEvents(fixture.Controller.Hint(), fixture.Controller.NodeId()).empty());
        fixture.ReplyRound(6, {.Replicated = false});
        fixture.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(fixture.Runtime.CaptureMailboxEvents(fixture.Controller.Hint(), fixture.Controller.NodeId()).empty());
        fixture.ReplyRound(6);
        UNIT_ASSERT((fixture.GetProposal() == TFailRealmStates{TFailRealm::N2, TFailRealm::N2, TFailRealm::F}));
    }

    Y_UNIT_TEST(RecoveringRealmPromotesAfterReplication) {
        TGroupStateFixture fixture;
        fixture.Update(1, {TFailRealm::N2, TFailRealm::R, TFailRealm::F});
        fixture.ReplyRound(3);
        UNIT_ASSERT((fixture.GetProposal() == TFailRealmStates{TFailRealm::N2, TFailRealm::N1, TFailRealm::F}));
    }
}
