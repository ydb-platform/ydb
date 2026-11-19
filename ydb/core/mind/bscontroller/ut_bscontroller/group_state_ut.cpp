#include <ydb/core/mind/bscontroller/group_state.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>
#include <ydb/core/blobstorage/syncer/syncer.h>
#include <ydb/library/actors/core/interconnect.h>
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
    std::optional<ui32> ReplicatingRealm;
    std::optional<TVDiskIdShort> ReplicatingDisk;
    std::optional<TVDiskIdShort> ErrorDisk;
    std::optional<bool> DurabilityReplicated = true;
    bool FailSecondRealm = false;
    std::optional<ui32> UnreplicatedRealm;
    bool ObsoleteGeneration = false;
    bool OnlyFirstRealm = false;
};

class TGroupStateFixture {
public:
    TTestBasicRuntime Runtime;
    TActorId Controller;
    TActorId Actor;
    TActorId Warden;
    TActorId WardenService;
    TGroupReplicationInfo Info;

    TGroupStateFixture() {
        TAppPrepare app;
        Runtime.Initialize(app.Unwrap());
        Controller = Runtime.AllocateEdgeActor();
        Warden = Runtime.AllocateEdgeActor();
        WardenService = MakeBlobStorageNodeWardenID(Warden.NodeId());
        Runtime.RegisterService(WardenService, Warden);
        Actor = Runtime.Register(CreateGroupStateActor(Controller));
        Runtime.EnableScheduleForActor(Actor);
        Info.Topology = std::make_shared<TBlobStorageGroupInfo::TTopology>(
            TBlobStorageGroupType(TBlobStorageGroupType::ErasureMirror3dc), 3, 3, 1, true);
        for (ui32 realm = 0; realm < 3; ++realm) {
            for (ui32 domain = 0; domain < 3; ++domain) {
                Info.VDisks.push_back({TVDiskID(42, 1, realm, domain, 0), Runtime.AllocateEdgeActor()});
            }
        }
    }

    void Update(ui32 generation, EMirror3dcGroupState state, const TFailRealmStates& states, ui32 groupId = 42) {
        Info.Generation = generation;
        Info.States = states;
        Info.GroupState = state;
        for (TGroupReplicationInfo::TVDisk& disk : Info.VDisks) {
            disk.Id = TVDiskID(groupId, generation, disk.Id.FailRealm, disk.Id.FailDomain, 0);
        }
        std::unique_ptr<TEvGroupState::TEvUpdate> event = std::make_unique<TEvGroupState::TEvUpdate>(groupId);
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
            if (!available || status.ObsoleteGeneration || (status.OnlyFirstRealm && disk.FailRealm != 0)) {
                --disk.GroupGeneration;
            }
            const TVDiskIdShort diskId(disk);
            const bool replicated = status.Replicated && available && status.ReplicatingRealm != disk.FailRealm
                && status.ReplicatingDisk != diskId;
            const auto replyStatus = status.ErrorDisk == diskId ? NKikimrProto::ERROR : NKikimrProto::OK;
            std::unique_ptr<TEvBlobStorage::TEvVStatusResult> result =
                std::make_unique<TEvBlobStorage::TEvVStatusResult>(replyStatus, disk, true, replicated, false, 1);
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

    NGroupSyncer::TEvRunMirror3dcRepair::TPtr GetRepair() {
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr request =
            Runtime.GrabEdgeEventRethrow<NGroupSyncer::TEvRunMirror3dcRepair>(Warden);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetGroupId(), 42);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetGeneration(), Info.Generation);
        UNIT_ASSERT(!request->Get()->Record.GetCancel());
        return request;
    }

    void ReplyRepair(const NGroupSyncer::TEvRunMirror3dcRepair::TPtr& request, NKikimrProto::EReplyStatus status) {
        std::unique_ptr<NGroupSyncer::TEvRunMirror3dcRepairResult> result =
            std::make_unique<NGroupSyncer::TEvRunMirror3dcRepairResult>();
        result->Record.SetGroupId(request->Get()->Record.GetGroupId());
        result->Record.SetGeneration(request->Get()->Record.GetGeneration());
        result->Record.SetStatus(status);
        Runtime.Send(new IEventHandle(request->Sender, Warden, result.release(), 0, request->Cookie), 0, true);
    }

    void AssertNoProposal() {
        UNIT_ASSERT(Runtime.CaptureMailboxEvents(Controller.Hint(), Controller.NodeId()).empty());
    }

    void AssertNoRepair() {
        TEventsList events = Runtime.CaptureMailboxEvents(Warden.Hint(), Warden.NodeId());
        for (const TAutoPtr<IEventHandle>& event : events) {
            UNIT_ASSERT_C(event->GetTypeRewrite() != NGroupSyncer::TEvRunMirror3dcRepair::EventType,
                event->GetTypeRewrite());
        }
        Runtime.PushMailboxEventsFront(Warden.Hint(), Warden.NodeId(), events);
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
    Y_UNIT_TEST(ConfigQuorumIntersectsEveryPreviousWrite) {
        using TInfo = TBlobStorageGroupInfo;
        auto top = std::make_shared<TInfo::TTopology>(TBlobStorageGroupType(TBlobStorageGroupType::ErasureMirror3dc), 3, 3, 1, true);
        for (const auto& edge : Mirror3dcAllowedGroupTransitions) {
            auto before = *GetMirror3dcRealmStates(edge.From);
            std::sort(before.begin(), before.end());
            do {
                auto layouts = *GetMirror3dcGroupLayouts(edge.From);
                auto writer = TMirror3dcWriteQuorumChecker::TryCreate(before, edge.From, layouts.Write, layouts.Ensured);
                UNIT_ASSERT(writer);
                auto after = *GetMirror3dcRealmStates(edge.To);
                std::sort(after.begin(), after.end());
                do {
                    if (!IsAllowedMirror3dcGroupTransition({edge.From, before}, {edge.To, after})) {
                        continue;
                    }
                    TInfo::TDynamicInfo dynamic(TGroupId::FromValue(42), 2);
                    dynamic.Mirror3dcState = {edge.To, after};
                    dynamic.ServiceIdForOrderNumber.resize(9);
                    TInfo info(top, std::move(dynamic), TString(), {}, NPDisk::DEVICE_TYPE_UNKNOWN);
                    for (ui32 mask = 0; mask < (1u << 9); ++mask) {
                        auto outstanding = TInfo::TSubgroupVDisks::CreateFromMask(top.get(), mask);
                        if (!writer->CheckQuorumForSubgroup(outstanding)) {
                            continue;
                        }
                        TInfo::TGroupVDisks unconfirmed(top.get());
                        for (ui32 i = 0; i < 9; ++i) {
                            if (outstanding[i]) {
                                ui32 realm = i % 3;
                                unconfirmed += {top.get(), TVDiskIdShort(realm, i / 3, 0)};
                            }
                        }
                        bool confirmed = TMirror3dcWriteQuorumChecker::CheckFailModelForGroup(info, unconfirmed);
                        UNIT_ASSERT_C(!confirmed, "old state# " << NKikimrBlobStorage::TGroupState::EState_Name(edge.From)
                            << " new state# " << NKikimrBlobStorage::TGroupState::EState_Name(edge.To)
                            << " unconfirmed disks# " << mask);
                    }
                } while (std::next_permutation(after.begin(), after.end()));
            } while (std::next_permutation(before.begin(), before.end()));
        }
    }

    Y_UNIT_TEST(StableFailureDoesNotStartReplication) {
        TGroupStateFixture fixture;
        fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED});
        fixture.AssertNoRequests();
        fixture.AssertNoRepair();
    }

    Y_UNIT_TEST(ConfigQuorumThenRepairThenTransition) {
        TGroupStateFixture fixture;
        fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_FAILURE,
            {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::LEAVING});
        TVDiskReplicationStatus status;
        status.OnlyFirstRealm = true;
        fixture.ReplyRound(9, status);
        fixture.AssertNoRepair();
        fixture.AssertNoProposal();
        // Repair Syncer checks placement after ordinary VDisk startup.
        status.OnlyFirstRealm = false;
        status.DurabilityReplicated = false;
        fixture.ReplyRound(6, status);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr repair = fixture.GetRepair();
        fixture.AssertNoProposal();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        // One incomplete Included realm cannot contain the next E=1+1 by itself.
        status.ReplicatingRealm = 1;
        fixture.ReplyRound(6, status);
        auto proposal = fixture.GetProposal();
        UNIT_ASSERT(proposal.State == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV1);
        UNIT_ASSERT(proposal.Realms[2] == TFailRealm::EXCLUDED);
    }

    Y_UNIT_TEST(SoloDisasterRequiresPendingSourcesBeforePromotion) {
        TGroupStateFixture fixture;
        fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER,
            {TFailRealm::INCLUDED, TFailRealm::LEAVING, TFailRealm::LEAVING});
        TVDiskReplicationStatus status;
        status.UnreplicatedRealm = 2;
        status.ReplicatingRealm = 1;
        fixture.ReplyRound(9, status);
        fixture.AssertNoRepair();
        fixture.AssertNoProposal();
        status.ReplicatingRealm.reset();
        fixture.ReplyRound(6, status);
        auto repair = fixture.GetRepair();
        fixture.AssertNoProposal();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.ReplyRound(3);
        auto proposal = fixture.GetProposal();
        UNIT_ASSERT(proposal.State == NKikimrBlobStorage::TGroupState::DC_SOLO_INV1);
        UNIT_ASSERT(proposal.Realms[0] == TFailRealm::INCLUDED);
        UNIT_ASSERT(proposal.Realms[1] == TFailRealm::EXCLUDED);
        UNIT_ASSERT(proposal.Realms[2] == TFailRealm::EXCLUDED);
    }

    Y_UNIT_TEST(SoloDisasterRechecksDiskHealthAfterSuccessfulRepair) {
        TGroupStateFixture fixture;
        fixture.Update(4, NKikimrBlobStorage::TGroupState::DC_SOLO_DISASTER,
            {TFailRealm::LEAVING, TFailRealm::INCLUDED, TFailRealm::LEAVING});
        fixture.ReplyRound(9);
        auto repair = fixture.GetRepair();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.AssertNoProposal();

        // E=1 in the next state: a single lost disk in the surviving realm can hide a copy.
        TVDiskReplicationStatus status;
        status.ReplicatingDisk.emplace(1, 2, 0);
        fixture.ReplyRound(3, status);
        fixture.AssertNoProposal();
        fixture.AssertNoRepair();

        // The first disk recovers, but a different disk now reports an error.
        status.ReplicatingDisk.reset();
        status.ErrorDisk.emplace(1, 0, 0);
        fixture.ReplyRound(3, status);
        fixture.AssertNoProposal();
        fixture.AssertNoRepair();

        fixture.ReplyRound(3, {.ObsoleteGeneration = true});
        fixture.AssertNoProposal();
        fixture.AssertNoRepair();

        fixture.ReplyRound(3);
        auto proposal = fixture.GetProposal();
        UNIT_ASSERT(proposal.State == NKikimrBlobStorage::TGroupState::DC_SOLO_INV1);
        UNIT_ASSERT(proposal.Realms == (TFailRealmStates{TFailRealm::EXCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED}));
        fixture.AssertNoRepair();
    }

    Y_UNIT_TEST(SoloRecoveryPendingWaitsForRecoveringVDiskStartup) {
        using S = NKikimrBlobStorage::TGroupState;
        for (auto [pending, ready] : {std::pair{S::DC_SOLO_RECOVERY_INV1_PENDING, S::DC_SOLO_RECOVERY_INV1},
                std::pair{S::DC_SOLO_RECOVERY_INV2_PENDING, S::DC_SOLO_RECOVERY_INV2}}) {
            TGroupStateFixture fixture;
            fixture.Update(3, pending, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::EXCLUDED});
            TVDiskReplicationStatus status;
            status.ReplicatingRealm = 1;
            fixture.ReplyRound(6, status);
            fixture.AssertNoProposal();
            fixture.AssertNoRepair();
            fixture.ReplyRound(3);
            auto proposal = fixture.GetProposal();
            UNIT_ASSERT(proposal.State == ready);
            UNIT_ASSERT(proposal.Realms == fixture.Info.States);
            fixture.AssertNoRepair();
        }
    }

    Y_UNIT_TEST(FailedRepairRetriesWithoutTransition) {
        TGroupStateFixture fixture;
        fixture.Update(2, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED});
        fixture.ReplyRound(3);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr repair = fixture.GetRepair();
        fixture.ReplyRepair(repair, NKikimrProto::ERROR);
        fixture.AssertNoProposal();
        repair = fixture.GetRepair();
        fixture.AssertNoRequests();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.ReplyRound(3);
        UNIT_ASSERT(fixture.GetProposal().State == NKikimrBlobStorage::TGroupState::DC_SOLO_INV2);
    }

    Y_UNIT_TEST(ConfirmedConfigWaitsForRepairSlotWithoutPollingAgain) {
        TGroupStateFixture fixture;
        for (ui32 groupId = 42; groupId < 59; ++groupId) {
            fixture.Update(1, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING,
                {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED}, groupId);
        }
        fixture.ReplyRound(17 * 3);
        TVector<NGroupSyncer::TEvRunMirror3dcRepair::TPtr> repairs;
        for (ui32 index = 0; index < 16; ++index) {
            repairs.push_back(fixture.Runtime.GrabEdgeEventRethrow<NGroupSyncer::TEvRunMirror3dcRepair>(fixture.Warden));
        }
        fixture.Runtime.SimulateSleep(TDuration::Seconds(6));
        fixture.AssertNoRepair();
        fixture.AssertNoRequests();
        fixture.ReplyRepair(repairs.front(), NKikimrProto::OK);
        fixture.ReplyRound(3);
        fixture.GetProposal();
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr next =
            fixture.Runtime.GrabEdgeEventRethrow<NGroupSyncer::TEvRunMirror3dcRepair>(fixture.Warden);
        for (const NGroupSyncer::TEvRunMirror3dcRepair::TPtr& repair : repairs) {
            UNIT_ASSERT(next->Get()->Record.GetGroupId() != repair->Get()->Record.GetGroupId());
        }
        fixture.AssertNoRequests();
    }

    Y_UNIT_TEST(GroupUpdateCancelsRepairAndIgnoresOldSuccess) {
        TGroupStateFixture fixture;
        TFailRealmStates realms{TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::EXCLUDED};
        fixture.Update(4, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, realms);
        fixture.ReplyRound(6);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr oldRepair = fixture.GetRepair();
        fixture.Update(5, NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2_PENDING, realms);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr cancel =
            fixture.Runtime.GrabEdgeEventRethrow<NGroupSyncer::TEvRunMirror3dcRepair>(fixture.Warden);
        UNIT_ASSERT(cancel->Get()->Record.GetCancel());
        UNIT_ASSERT_VALUES_EQUAL(cancel->Cookie, oldRepair->Cookie);
        UNIT_ASSERT_VALUES_EQUAL(cancel->Get()->Record.GetGeneration(), 4);
        fixture.ReplyRepair(oldRepair, NKikimrProto::OK);
        fixture.AssertNoProposal();
        fixture.ReplyRound(6);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr repair = fixture.GetRepair();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.ReplyRound(6);
        UNIT_ASSERT(fixture.GetProposal().State == NKikimrBlobStorage::TGroupState::DC_FAILURE_INV2);
    }

    Y_UNIT_TEST(DisconnectedRepairCannotFinishTransition) {
        TGroupStateFixture fixture;
        fixture.Update(8, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED});
        fixture.ReplyRound(3);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr repair = fixture.GetRepair();
        fixture.Runtime.Send(new IEventHandle(fixture.Actor, {},
            new TEvInterconnect::TEvNodeDisconnected(fixture.Warden.NodeId()), 0, repair->Cookie), 0, true);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr cancel =
            fixture.Runtime.GrabEdgeEventRethrow<NGroupSyncer::TEvRunMirror3dcRepair>(fixture.Warden);
        UNIT_ASSERT(cancel->Get()->Record.GetCancel());
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.AssertNoProposal();
    }

    Y_UNIT_TEST(UndeliveredRepairIsRetried) {
        TGroupStateFixture fixture;
        fixture.Update(9, NKikimrBlobStorage::TGroupState::DC_SOLO_INV2_PENDING, {TFailRealm::INCLUDED, TFailRealm::EXCLUDED, TFailRealm::EXCLUDED});
        fixture.ReplyRound(3);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr repair = fixture.GetRepair();
        fixture.Runtime.Send(new IEventHandle(fixture.Actor, {},
            new TEvents::TEvUndelivered(NGroupSyncer::TEvRunMirror3dcRepair::EventType, TEvents::TEvUndelivered::ReasonActorUnknown),
            0, repair->Cookie), 0, true);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr cancel =
            fixture.Runtime.GrabEdgeEventRethrow<NGroupSyncer::TEvRunMirror3dcRepair>(fixture.Warden);
        UNIT_ASSERT(cancel->Get()->Record.GetCancel());
        fixture.AssertNoProposal();
        repair = fixture.GetRepair();
        fixture.AssertNoRequests();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.ReplyRound(3);
        UNIT_ASSERT(fixture.GetProposal().State == NKikimrBlobStorage::TGroupState::DC_SOLO_INV2);
    }

    Y_UNIT_TEST(NormalDoesNotStartRepair) {
        TGroupStateFixture fixture;
        fixture.Update(7, NKikimrBlobStorage::TGroupState::NORMAL, {TFailRealm::INCLUDED, TFailRealm::INCLUDED, TFailRealm::INCLUDED});
        fixture.AssertNoRequests();
        fixture.AssertNoRepair();
    }

    Y_UNIT_TEST(RecoveryCompletesAfterRepair) {
        TGroupStateFixture fixture;
        fixture.Update(6, NKikimrBlobStorage::TGroupState::DC_RECOVERY_INV1, {TFailRealm::INCLUDED, TFailRealm::RECOVERING, TFailRealm::INCLUDED});
        fixture.ReplyRound(9);
        NGroupSyncer::TEvRunMirror3dcRepair::TPtr repair = fixture.GetRepair();
        fixture.ReplyRepair(repair, NKikimrProto::OK);
        fixture.ReplyRound(9);
        UNIT_ASSERT(fixture.GetProposal().State == NKikimrBlobStorage::TGroupState::NORMAL);
    }
}
