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

    TEnvironmentSetup::TSettings MakeThreePileSettings() {
        return {
            .NodeCount = 24,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        };
    }

    void PutToPileGroup(TEnvironmentSetup& env, ui32 pileGroupId, ui32 nodeId, const TLogoBlobID& id, const TString& data,
            bool issueKeepFlag = false) {
        auto res = QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroupId, nodeId,
            new TEvBlobStorage::TEvPut(TEvBlobStorage::TEvPut::TParameters{
                .BlobId = id,
                .Buffer = TRope(data),
                .Deadline = TInstant::Max(),
                .IssueKeepFlag = issueKeepFlag,
            }));
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
    }

    // VDisks apply the barrier once they know their quorum has it; the unkept blob behind it vanishes then
    void WaitForBarrierInEffect(TEnvironmentSetup& env, ui32 pileGroupId, ui32 nodeId, const TLogoBlobID& probe,
            const TString& data) {
        WaitUntil(env, [&] { return ReadFromPileGroup(env, pileGroupId, nodeId, probe, data) == NKikimrProto::NODATA; },
            TStringBuilder() << "barrier in effect in GroupId# " << pileGroupId, TDuration::Seconds(1), 300);
    }

    void CollectGarbage(TEnvironmentSetup& env, TEvBlobStorage::TEvCollectGarbage *request,
            std::optional<ui32> pileGroupId = std::nullopt, ui32 groupId = 0) {
        std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> res;
        if (pileGroupId) {
            res = QueryPileGroup<TEvBlobStorage::TEvCollectGarbageResult>(env, *pileGroupId, 1, request);
        } else {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, request);
            });
            res.reset(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false)->Release().Release());
        }
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
    }

    enum class ERepairRead { Get, Range, Discover };

    // the blob is kept in piles 0 and 1 behind the barrier in effect in every pile and is missing in pile 2, so the read
    // with MustRestoreFirst restores it there; Get and Range see the flag in one pile only, which comes in the order
    // making the restoration take the response without it
    void CheckRepairKeepsKeepFlag(ERepairRead read) {
        TEnvironmentSetup env(MakeThreePileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        const TString data = "blob to be restored";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data, true);
        }
        for (ui32 pile : {0, 1, 2}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, true, 1, 10, nullptr, nullptr,
            TInstant::Max(), false), {}, groupId);
        for (ui32 pile : {0, 1, 2}) {
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }

        // the pile which loses the flag in its response and the one which comes first
        const ui32 lossyPileGroupId = pileGroups[read == ERepairRead::Range ? 0 : 1];
        std::map<ui32, std::unique_ptr<IEventHandle>> replies; // held responses of piles by pile group id
        std::vector<bool> restoreKeepFlags;
        bool hold = read != ERepairRead::Discover;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut && ev->HasEvent()) {
                if (auto *msg = ev->Get<TEvBlobStorage::TEvPut>(); msg->Id == id &&
                        msg->WriteSource == TWriteSource::BridgeProxyRestorePut) {
                    restoreKeepFlags.push_back(msg->IssueKeepFlag);
                }
            }
            if (!hold || !ev->HasEvent()) {
                return true;
            }
            if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); !actor ||
                    !TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                return true;
            }
            ui32 pileGroupId;
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && read == ERepairRead::Get) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg->ResponseSz != 1 || msg->Responses[0].Id != id) {
                    return true;
                }
                pileGroupId = msg->GroupId;
                if (pileGroupId == lossyPileGroupId) {
                    msg->Responses[0].Keep = false;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvRangeResult && read == ERepairRead::Range) {
                auto *msg = ev->Get<TEvBlobStorage::TEvRangeResult>();
                if (msg->From != id || msg->To != id) {
                    return true;
                }
                pileGroupId = msg->GroupId;
                if (pileGroupId == lossyPileGroupId) {
                    for (auto& r : msg->Responses) {
                        r.Keep = false;
                    }
                }
            } else {
                return true;
            }
            UNIT_ASSERT(!replies.contains(pileGroupId));
            replies.emplace(pileGroupId, std::move(ev));
            return false;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            switch (read) {
                case ERepairRead::Get:
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                        NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
                    break;
                case ERepairRead::Range:
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(id.TabletID(), id, id,
                        /*mustRestoreFirst=*/ true, TInstant::Max(), /*isIndexOnly=*/ false));
                    break;
                case ERepairRead::Discover:
                    SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvDiscover(id.TabletID(), 0, /*readBody=*/ true,
                        /*discoverBlockedGeneration=*/ false, TInstant::Max(), 0, false));
                    break;
            }
        });
        if (hold) {
            WaitUntil(env, [&] { return replies.size() == 3; }, "responses of all piles", TDuration::Seconds(1), 30);
            hold = false;
            for (ui32 pile : {0, 1, 2}) {
                env.Runtime->Send(replies.at(pileGroups[pile]).release(), 1);
                if (pile != 2) { // the result comes after the last one, it is waited for below
                    env.Sim(TDuration::MilliSeconds(10));
                }
            }
        }
        switch (read) {
            case ERepairRead::Get: {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
                CheckGetResult(*res->Get(), data);
                UNIT_ASSERT(res->Get()->Responses[0].Keep);
                break;
            }
            case ERepairRead::Range: {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses.size(), 1);
                UNIT_ASSERT(res->Get()->Responses[0].Keep);
                break;
            }
            case ERepairRead::Discover: {
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(edge, false);
                UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Id, id);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Buffer, data);
                break;
            }
        }
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT(!restoreKeepFlags.empty());
        for (bool keep : restoreKeepFlags) {
            UNIT_ASSERT(keep);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[2], 17, id, data), NKikimrProto::OK);
    }

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

    // the blob written to the pile being synced only, as the write which has failed in the other pile is, gets no
    // DoNotKeep flag from the syncer; the bridge proxy can restore the blob into the other pile, and the piles must
    // agree on its fate then
    Y_UNIT_TEST(SyncerLeavesTargetOnlyBlobUnflagged) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        const TString data = "blob in pile 1 only";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[1], 9, id, data);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        RejoinPile(env, 1);

        // the read restores the blob into pile 0
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        });
        CheckGetResult(*env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false)->Get(), data);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[0], 1, id, data), NKikimrProto::OK);

        // the blob is kept behind the barrier now, so both piles must keep it
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, true, 1, 10,
            new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false), {}, groupId);
        for (ui32 pile : {0, 1}) {
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, probe, data);
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data), NKikimrProto::OK);
        }

        const auto res = QueryPileGroup<TEvBlobStorage::TEvGetResult>(env, pileGroups[1], 9,
            new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead,
                /*mustRestoreFirst=*/ false, /*isIndexOnly=*/ true));
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 1);
        UNIT_ASSERT_C(res->Responses[0].Keep && !res->Responses[0].DoNotKeep, res->ToString());
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

    Y_UNIT_TEST(GetRestoreKeepsFlagOfAnyPile) {
        CheckRepairKeepsKeepFlag(ERepairRead::Get);
    }

    Y_UNIT_TEST(RangeRestoreKeepsFlagOfAnyPile) {
        CheckRepairKeepsKeepFlag(ERepairRead::Range);
    }

    Y_UNIT_TEST(DiscoverRestoreKeepsFlag) {
        CheckRepairKeepsKeepFlag(ERepairRead::Discover);
    }

    // the blob missing in the pile being synced gets written there without the Keep flag after the syncer has learned
    // the flag from the source pile, but before it checks the blob in the pile, as the bridge proxy restoring the blob
    // may do; the syncer must keep the blob anyway, otherwise it is behind the barrier unprotected
    Y_UNIT_TEST(SyncerKeepsBlobWrittenWithoutKeepFlag) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));

        // the blob is kept behind the barrier in pile 0 only
        const TString data = "kept blob";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data, true);
        PutToPileGroup(env, pileGroups[0], 1, probe, data);
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, true, 1, 10, nullptr, nullptr,
            TInstant::Max(), false), {}, groupId);
        WaitForBarrierInEffect(env, pileGroups[0], 1, probe, data);

        // hold the check of the blob in pile 1 the syncer makes at the data stage
        std::unique_ptr<IEventHandle> check;
        ui32 checkNodeId = 0;
        bool held = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (held || ev->GetTypeRewrite() != TEvBlobStorage::EvGet || !ev->HasEvent()) {
                return true;
            }
            if (IActor *actor = env.Runtime->GetActor(ev->Sender); !actor || !TypeName(*actor).Contains("TSyncerActor")) {
                return true;
            }
            auto *msg = ev->Get<TEvBlobStorage::TEvGet>();
            for (ui32 i = 0; i < msg->QuerySize; ++i) {
                if (msg->Queries[i].Id == id && msg->IsIndexOnly) {
                    check = std::move(ev);
                    checkNodeId = nodeId;
                    held = true;
                    return false;
                }
            }
            return true;
        };
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] { return held; }, "syncer checking the blob in pile 1", TDuration::Seconds(1), 300);

        PutToPileGroup(env, pileGroups[1], 9, id, data);
        env.Runtime->Send(check.release(), checkNodeId);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = {};

        // the barrier has been copied to pile 1, the blob must be kept there
        const TLogoBlobID probe2(100500, 1, 3, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[1], 9, probe2, data);
        WaitForBarrierInEffect(env, pileGroups[1], 9, probe2, data);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
    }


    // the blob put behind the barrier is reported so by the bridged group as by any other, which BlobDepot checks
    Y_UNIT_TEST(PutBeyondBarrierReported) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        const TString data = "blob";
        const TLogoBlobID probe(100500, 1, 1, 0, data.size(), 0);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(probe.TabletID(), 1, 0, 0, true, 1, 10, nullptr, nullptr,
            TInstant::Max(), false), {}, groupId);
        for (ui32 pile : {0, 1}) {
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }

        auto put = [&](const TLogoBlobID& id) {
            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            });
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            return res->Get()->WrittenBeyondBarrier;
        };
        UNIT_ASSERT(put(TLogoBlobID(probe.TabletID(), 1, 2, 0, data.size(), 0)));
        UNIT_ASSERT(!put(TLogoBlobID(probe.TabletID(), 1, 11, 0, data.size(), 0)));
    }

}
