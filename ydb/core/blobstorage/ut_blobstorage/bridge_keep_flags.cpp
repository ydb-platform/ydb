#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    TEnvironmentSetup::TSettings MakeTwoPileSettings() {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        };
    }

    // reads of the blob coming to the actor of the designated kind from the group lose Keep flags, as it happens when
    // the read comes from VDisks which don't know the flag yet
    auto MakeKeepFlagDropper(TEnvironmentSetup& env, TStringBuf actorTypeName, ui32 groupId, const TLogoBlobID& id,
            ui32& numDropped) {
        return [&env, actorTypeName, groupId, id, &numDropped](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent()) {
                if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains(actorTypeName)) {
                    auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                    for (ui32 i = 0; msg->GroupId == groupId && i < msg->ResponseSz; ++i) {
                        if (auto& r = msg->Responses[i]; r.Id == id && r.Status == NKikimrProto::OK && r.Keep) {
                            r.Keep = false;
                            ++numDropped;
                        }
                    }
                }
            }
            return true;
        };
    }

    void PutToPileGroup(TEnvironmentSetup& env, ui32 pileGroupId, ui32 nodeId, const TLogoBlobID& id, const TString& data) {
        auto res = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroupId, nodeId,
            new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
    }

    // VDisks apply the barrier once they know their quorum has it; the unkept blob behind it vanishes then
    void WaitForBarrierInEffect(TEnvironmentSetup& env, ui32 pileGroupId, ui32 nodeId, const TLogoBlobID& probe,
            const TString& data) {
        WaitUntil(env, [&] { return ReadFromPileGroup(env, pileGroupId, nodeId, probe, data) == NKikimrProto::NODATA; },
            TStringBuilder() << "barrier in effect in GroupId# " << pileGroupId, TDuration::Seconds(1), 300);
    }

    // nodeId: a node of the pile of pileGroupId
    void CollectGarbage(TEnvironmentSetup& env, TEvBlobStorage::TEvCollectGarbage *request,
            std::optional<ui32> pileGroupId = std::nullopt, ui32 groupId = 0, ui32 nodeId = 1) {
        std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> res;
        if (pileGroupId) {
            res = QueryPileGroup<TEvBlobStorage::TEvCollectGarbageResult>(env, *pileGroupId, nodeId, request);
        } else {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, request);
            });
            res.reset(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false)->Release().Release());
        }
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
    }

    // The blob and its Keep flag are in piles 0 and 1, pile 2 lacks the blob; the barrier is in all piles, with a probe
    // blob not kept in each to see it in effect
    struct TThreePileBlob {
        TEnvironmentSetup Env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        ui32 GroupId = 0;
        std::vector<ui32> PileGroups;
        const TString Data = "blob to be restored";
        const TLogoBlobID Id = TLogoBlobID(100500, 1, 1, 0, Data.size(), 0);

        // what the restore puts of the blob asked for
        ui32 NumRestorePuts = 0;
        ui32 NumRestorePutsWithKeep = 0;

        static ui32 NodeOf(ui32 pile) {
            return 1 + 8 * pile;
        }

        TThreePileBlob() {
            Env.CreatePool();
            Env.Sim(TDuration::Seconds(30));
            GroupId = Env.GetGroups().front();
            PileGroups = GetPileGroups(Env, GroupId);

            const TLogoBlobID probe(Id.TabletID(), 1, 2, 0, Data.size(), 0);
            for (ui32 pile : {0, 1}) {
                PutToPileGroup(Env, PileGroups[pile], NodeOf(pile), Id, Data);
                CollectGarbage(Env, new TEvBlobStorage::TEvCollectGarbage(Id.TabletID(), 1, 0, 0, false, 0, 0,
                    new TVector<TLogoBlobID>{Id}, nullptr, TInstant::Max(), false), PileGroups[pile], 0, NodeOf(pile));
            }
            for (ui32 pile : {0, 1, 2}) {
                PutToPileGroup(Env, PileGroups[pile], NodeOf(pile), probe, Data);
            }
            CollectGarbage(Env, new TEvBlobStorage::TEvCollectGarbage(Id.TabletID(), 1, 1, 0, true, 1, 10, nullptr,
                nullptr, TInstant::Max(), false), {}, GroupId);
            for (ui32 pile : {0, 1, 2}) {
                WaitForBarrierInEffect(Env, PileGroups[pile], NodeOf(pile), probe, Data);
            }
            for (ui32 pile : {0, 1}) {
                UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(Env, PileGroups[pile], NodeOf(pile), Id, Data), NKikimrProto::OK);
            }
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(Env, PileGroups[2], NodeOf(2), Id, Data), NKikimrProto::NODATA);
        }

        bool IsToBridgeProxy(const IEventHandle& ev) {
            IActor *actor = Env.Runtime->GetActor(ev.GetRecipientRewrite());
            return actor && TypeName(*actor).Contains("TBridgedBlobStorageProxyActor");
        }

        // counts the restore puts of the blob and the Keep flag they carry
        void CountRestorePut(IEventHandle& ev) {
            if (ev.GetTypeRewrite() == TEvBlobStorage::EvPut && ev.HasEvent()) {
                auto *msg = ev.Get<TEvBlobStorage::TEvPut>();
                if (msg->Id == Id && msg->WriteSource == TWriteSource::BridgeProxyRestorePut) {
                    ++NumRestorePuts;
                    NumRestorePutsWithKeep += msg->IssueKeepFlag;
                }
            }
        }

        // the copy restored into pile 2 is behind the barrier there, so it stays only if it is kept
        void CheckRestoredAndKept() {
            UNIT_ASSERT(NumRestorePuts);
            UNIT_ASSERT_VALUES_EQUAL(NumRestorePutsWithKeep, NumRestorePuts);
            const TLogoBlobID probe2(Id.TabletID(), 1, 3, 0, Data.size(), 0);
            PutToPileGroup(Env, PileGroups[2], NodeOf(2), probe2, Data);
            WaitForBarrierInEffect(Env, PileGroups[2], NodeOf(2), probe2, Data);
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(Env, PileGroups[2], NodeOf(2), Id, Data), NKikimrProto::OK);
        }
    };

} // anonymous

Y_UNIT_TEST_SUITE(BridgeKeepFlags) {

    // the blob missing in the pile being synced gets copied after barriers are; its Keep flag must be the one the source
    // pile has, not the one the read of the blob happened to see, otherwise the copy is behind the barrier unprotected
    Y_UNIT_TEST(SyncerKeepsSourceKeepFlag) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));

        // the blob is kept behind the barrier in pile 0 only, the probe blob there is not kept
        const TString data = "kept blob";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data);
        PutToPileGroup(env, pileGroups[0], 1, probe, data);
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, true, 1, 10,
            new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false), {}, groupId);
        WaitForBarrierInEffect(env, pileGroups[0], 1, probe, data);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[0], 1, id, data), NKikimrProto::OK);

        ui32 numDropped = 0;
        env.Runtime->FilterFunction = MakeKeepFlagDropper(env, "TSyncerActor", pileGroups[0], id, numDropped);
        RejoinPile(env, 1);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT(numDropped); // the syncer has copied the blob

        // the barrier has been copied to pile 1 too, the copy of the blob must be kept there
        const TLogoBlobID probe2(100500, 1, 3, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[1], 9, probe2, data);
        WaitForBarrierInEffect(env, pileGroups[1], 9, probe2, data);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
    }

    // the bridge proxy restoring the blob missing in a pile reads it after the query that found it missing; the Keep
    // flag known from that query must survive the read not seeing it
    Y_UNIT_TEST(RestoreKeepsKnownKeepFlag) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        // the blob and its Keep flag are in pile 0 only, the barrier is in both piles, as well as probe blobs not kept
        const TString data = "blob to be restored";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, false, 0, 0,
            new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false), pileGroups[0]);
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 1, 0, true, 1, 10, nullptr, nullptr,
            TInstant::Max(), false), {}, groupId);
        for (ui32 pile : {0, 1}) {
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[0], 1, id, data), NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::NODATA);

        ui32 numDropped = 0;
        env.Runtime->FilterFunction = MakeKeepFlagDropper(env, "TBridgedBlobStorageProxyActor", pileGroups[0], id,
            numDropped);
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(id.TabletID(), TLogoBlobID(id.TabletID(), 0, 0, 0, 0, 0),
                TLogoBlobID(id.TabletID(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie), /*mustRestoreFirst=*/ true, TInstant::Max(), /*isIndexOnly=*/ true));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), 1);
        UNIT_ASSERT(numDropped); // the blob has been read for restoration

        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
    }

    // A Get restoring the blob into pile 2 collects the responses of all piles first. Only the first pile to answer
    // shows the Keep flag, as if the other one didn't know it yet; a later response without the flag must not erase
    // it from the restore, whatever the order of the piles.
    Y_UNIT_TEST(GetRestoreKeepsKeepFlagOfAnyPile) {
        TThreePileBlob t;

        bool keepSeen = false;
        ui32 numDropped = 0;
        t.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            t.CountRestorePut(*ev);
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent() && t.IsToBridgeProxy(*ev)) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                    if (auto& r = msg->Responses[i]; r.Id == t.Id && r.Status == NKikimrProto::OK && r.Keep) {
                        if (std::exchange(keepSeen, true)) {
                            r.Keep = false;
                            ++numDropped;
                        }
                    }
                }
            }
            return true;
        };
        const TActorId edge = t.Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        t.Env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, t.GroupId, new TEvBlobStorage::TEvGet(t.Id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        });
        auto res = t.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        t.Env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
        UNIT_ASSERT(res->Get()->Responses[0].Keep);
        UNIT_ASSERT(numDropped); // the second pile to answer didn't show the flag

        t.CheckRestoredAndKept();
    }

    // A Range restoring the blob into pile 2: the first pile to answer doesn't show the Keep flag, a later one does.
    // The restore takes the flag from the later response; the read of the blob for the restore doesn't show it either.
    Y_UNIT_TEST(RangeRestoreKeepsKeepFlagOfLaterPile) {
        TThreePileBlob t;

        bool firstRangeSeen = false;
        ui32 numDropped = 0;
        t.Env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            t.CountRestorePut(*ev);
            if (!ev->HasEvent() || !t.IsToBridgeProxy(*ev)) {
                return true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvRangeResult) {
                for (auto& r : ev->Get<TEvBlobStorage::TEvRangeResult>()->Responses) {
                    if (r.Id == t.Id && r.Keep && !std::exchange(firstRangeSeen, true)) {
                        r.Keep = false;
                        ++numDropped;
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                    if (auto& r = msg->Responses[i]; r.Id == t.Id && r.Keep) {
                        r.Keep = false;
                        ++numDropped;
                    }
                }
            }
            return true;
        };
        const TActorId edge = t.Env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        t.Env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, t.GroupId, new TEvBlobStorage::TEvRange(t.Id.TabletID(),
                TLogoBlobID(t.Id.TabletID(), 0, 0, 0, 0, 0),
                TLogoBlobID(t.Id.TabletID(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
                TLogoBlobID::MaxCookie), /*mustRestoreFirst=*/ true, TInstant::Max(), /*isIndexOnly=*/ true));
        });
        auto res = t.Env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
        t.Env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), 1);
        UNIT_ASSERT(res->Get()->Responses[0].Keep);
        UNIT_ASSERT(firstRangeSeen);
        UNIT_ASSERT(numDropped);

        t.CheckRestoredAndKept();
    }

}
