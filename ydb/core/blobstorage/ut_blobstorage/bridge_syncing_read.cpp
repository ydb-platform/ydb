#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    // Pile 1 rejoins and its part of the group is held at the DATA stage of the sync: reads go to both piles, while
    // only the part in pile 0 holds everything written to the group
    struct TPileBeingSynced {
        TEnvironmentSetup Env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        ui32 GroupId = 0;
        std::vector<ui32> PileGroups;
        const TString Data = "blob read while the pile is synced";
        const TLogoBlobID Id = TLogoBlobID(100500, 1, 1, 0, Data.size(), 0);

        bool HoldSyncers = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> HeldSyncers;
        THashSet<const IEventHandle*> ReleasedSyncers; // let go once, they pass the filter again on the way

        // the read under test: the reply of pile 0 is held until pile 1 has failed
        bool InterceptReads = false;
        bool UntrustedFailed = false;
        std::vector<std::pair<ui32, std::unique_ptr<IEventHandle>>> HeldTrusted;

        TPileBeingSynced() {
            Env.CreatePool();
            Env.Sim(TDuration::Seconds(30));
            GroupId = Env.GetGroups().front();
            PileGroups = GetPileGroups(Env, GroupId);

            SwitchPileState(Env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
            Env.Sim(TDuration::Seconds(30));
            Put();

            Env.Runtime->FilterFunction = [this](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                return Filter(nodeId, ev);
            };
            HoldSyncers = true;
            SwitchPileState(Env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
            WaitForPileState(Env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2);
            // let the sync of the group go stage after stage until it is at the DATA one
            for (ui32 i = 0; GetStageOfPile1() != NKikimrBridge::TGroupState::WRITE_KEEP_BARRIER_DONOTKEEP_DATA; ++i) {
                UNIT_ASSERT_C(i < 300, "the sync of the group didn't reach the DATA stage");
                for (auto& [nodeId, ev] : std::exchange(HeldSyncers, {})) {
                    ReleasedSyncers.insert(ev.get());
                    Env.Runtime->Send(ev.release(), nodeId);
                }
                Env.Sim(TDuration::Seconds(1));
            }
            UNIT_ASSERT(!PartIsSynced(1));
            UNIT_ASSERT(PartIsSynced(0));
        }

        void Put() {
            const TActorId edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, GroupId, new TEvBlobStorage::TEvPut(Id, Data, TInstant::Max()));
            });
            auto res = Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        }

        NKikimrBridge::TGroupState::TPile GetPart(ui32 pile) {
            return GetGroupInfo(Env, GroupId).GetBridgeGroupState().GetPile(pile);
        }

        NKikimrBridge::TGroupState::EStage GetStageOfPile1() {
            return GetPart(1).GetStage();
        }

        bool PartIsSynced(ui32 pile) {
            return GetPart(pile).GetStage() == NKikimrBridge::TGroupState::SYNCED;
        }

        bool IsToBridgeProxy(const IEventHandle& ev) {
            IActor *actor = Env.Runtime->GetActor(ev.GetRecipientRewrite());
            return actor && TypeName(*actor).Contains("TBridgedBlobStorageProxyActor");
        }

        template<typename TResult>
        bool InterceptRead(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            auto *msg = ev->Get<TResult>();
            if (msg->GroupId == PileGroups[0] && !UntrustedFailed) {
                HeldTrusted.emplace_back(nodeId, std::move(ev));
                return false;
            } else if (msg->GroupId == PileGroups[1]) {
                // the part being synced is degraded and fails the read
                msg->Status = NKikimrProto::ERROR;
                msg->ErrorReason = "the part being synced failed the read by the test";
                if constexpr (std::is_same_v<TResult, TEvBlobStorage::TEvGetResult>) {
                    msg->ResponseSz = 0;
                    msg->Responses.Reset();
                } else {
                    msg->Responses.clear();
                }
                UntrustedFailed = true;
            }
            return true;
        }

        bool Filter(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (HoldSyncers && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType &&
                    ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->BridgeProxyGroupId.GetRawId() == GroupId &&
                    !ReleasedSyncers.erase(ev.get())) {
                HeldSyncers.emplace_back(nodeId, std::move(ev));
                return false;
            }
            if (InterceptReads && ev->HasEvent() && IsToBridgeProxy(*ev)) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                    return InterceptRead<TEvBlobStorage::TEvGetResult>(nodeId, ev);
                } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvRangeResult) {
                    return InterceptRead<TEvBlobStorage::TEvRangeResult>(nodeId, ev);
                }
            }
            return true;
        }

        // pile 1 fails the read first, then the reply of pile 0 comes
        template<typename TResult>
        std::unique_ptr<TResult> Read(IEventBase *request) {
            InterceptReads = true;
            const TActorId edge = Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            Env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, GroupId, request);
            });
            for (ui32 i = 0; !UntrustedFailed || HeldTrusted.empty(); ++i) {
                UNIT_ASSERT_C(i < 300, "the read didn't reach both piles");
                Env.Sim(TDuration::MilliSeconds(100));
            }
            for (auto& [nodeId, ev] : std::exchange(HeldTrusted, {})) {
                Env.Runtime->Send(ev.release(), nodeId);
            }
            auto res = Env.WaitForEdgeActorEvent<TResult>(edge, false);
            InterceptReads = false;
            return std::unique_ptr<TResult>(res->Release().Release());
        }
    };

} // anonymous

Y_UNIT_TEST_SUITE(BridgeSyncingPileRead) {

    // A Get not restoring data takes the answer of pile 0, which holds everything; the error of the part being synced
    // coming first must not fail it
    Y_UNIT_TEST(GetSurvivesErrorOfPileBeingSynced) {
        TPileBeingSynced t;
        const auto res = t.Read<TEvBlobStorage::TEvGetResult>(new TEvBlobStorage::TEvGet(t.Id, 0, 0, TInstant::Max(),
            NKikimrBlobStorage::FastRead));
        CheckGetResult(*res, t.Data);
    }

    // The same for a Range
    Y_UNIT_TEST(RangeSurvivesErrorOfPileBeingSynced) {
        TPileBeingSynced t;
        const auto res = t.Read<TEvBlobStorage::TEvRangeResult>(new TEvBlobStorage::TEvRange(t.Id.TabletID(),
            TLogoBlobID(t.Id.TabletID(), 0, 0, 0, 0, 0),
            TLogoBlobID(t.Id.TabletID(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
            TLogoBlobID::MaxCookie), /*mustRestoreFirst=*/ false, TInstant::Max()));
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Responses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Id, t.Id);
        UNIT_ASSERT_VALUES_EQUAL(res->Responses[0].Buffer, t.Data);
    }

}
