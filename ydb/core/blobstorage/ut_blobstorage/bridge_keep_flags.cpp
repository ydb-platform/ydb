#include "bridge_common.h"
#include "bridge_pile_ops.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/library/testlib/helpers.h>

#include <util/system/type_name.h>

using namespace NBridgeTest;

namespace {

    TEnvironmentSetup::TSettings MakeTwoPileSettings(std::function<void(TVDiskConfig&)> prepareVDisk = {}) {
        return {
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .VDiskConfigPreprocessor = std::move(prepareVDisk),
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

    void CompactPile(TEnvironmentSetup& env, ui32 groupId) {
        const auto info = env.GetGroupInfo(groupId);
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            env.CompactVDisk(info->GetActorId(i));
        }
    }

    auto ReadFlags(TEnvironmentSetup& env, ui32 pile, const TLogoBlobID& id, bool verify = false) {
        auto get = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead,
            false, true);
        get->VerifyKeepFlags = verify;
        const ui32 node = env.GetGroupInfo(pile)->GetActorId(0).NodeId();
        auto res = QueryPileGroup<TEvBlobStorage::TEvGetResult>(env, pile, node, get.release());
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 1);
        return std::move(res->Responses[0]);
    }

    TActorId StartRepair(TEnvironmentSetup& env, ui32 groupId, const TLogoBlobID& id) {
        const auto edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/true));
        });
        return edge;
    }

    enum class EHoldRepair { TargetGetReply, RestorePut, SyncerPut, SyncerKeep, ReconcileKeep };

    void CheckDelayedRepair(EHoldRepair hold, bool compactSource = false) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const auto groupId = CreatePoolAndGetGroup(env);
        const auto piles = GetPileGroups(env, groupId);
        const auto source = piles[0], target = piles[1];
        const bool rejoin = hold == EHoldRepair::SyncerPut || hold == EHoldRepair::SyncerKeep;
        if (rejoin || hold == EHoldRepair::ReconcileKeep) {
            SwitchPileState(env, 1, rejoin ? NKikimrBridge::TClusterState::DISCONNECTED : NKikimrBridge::TClusterState::HOT_STANDBY);
            env.Sim(TDuration::Seconds(30));
        }

        const TString data = "released while the repair was in flight";
        const TLogoBlobID id(100500, 1, 1, 2, data.size(), 0);
        const TLogoBlobID probe(100500, 1, 2, 2, data.size(), 0);
        PutToPileGroup(env, source, 1, id, data, true);
        PutToPileGroup(env, target, 9, probe, data);
        if (hold == EHoldRepair::SyncerKeep || hold == EHoldRepair::ReconcileKeep) {
            PutToPileGroup(env, target, 9, id, data);
        }
        std::unique_ptr<IEventHandle> held;
        ui32 node = 0;
        bool sawSourceKeep = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!ev->HasEvent()) {
                return true;
            }
            if (hold == EHoldRepair::TargetGetReply && ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                const auto& msg = *ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg.ResponseSz != 1 || msg.Responses[0].Id != id) {
                    return true;
                }
                sawSourceKeep |= msg.GroupId == source && msg.Responses[0].Keep;
                if (msg.GroupId != target || msg.Responses[0].Status != NKikimrProto::NODATA) {
                    return true;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut &&
                    (hold == EHoldRepair::RestorePut || hold == EHoldRepair::SyncerPut)) {
                const auto& put = *ev->Get<TEvBlobStorage::TEvPut>();
                if (put.Id != id || !put.IssueKeepFlag || put.WriteSource != (rejoin ? TWriteSource::SyncerMergePut
                        : TWriteSource::BridgeProxyRestorePut)) {
                    return true;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage &&
                    (hold == EHoldRepair::SyncerKeep || hold == EHoldRepair::ReconcileKeep)) {
                const auto& gc = *ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                if (gc.WriteSource != TWriteSource::SyncerMergeGC || !gc.Keep || !IsIn(*gc.Keep, id)) {
                    return true;
                }
            } else {
                return true;
            }
            UNIT_ASSERT(!held);
            node = nodeId;
            held = std::move(ev);
            return false;
        };

        TActorId client;
        if (rejoin) {
            SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        } else if (hold == EHoldRepair::ReconcileKeep) {
            env.Runtime->Send(new IEventHandle(MakeBlobStorageNodeWardenID(1), {},
                new NStorage::TEvNodeWardenStandbyDirty(TGroupId::FromValue(target), /*data=*/false)), 1);
        } else {
            client = StartRepair(env, groupId, id);
        }
        WaitUntil(env, [&] { return held && (hold != EHoldRepair::TargetGetReply || sawSourceKeep); }, "delayed repair");
        env.Runtime->FilterFunction = {};
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, id.Channel(), true, 1, 10,
            nullptr, new TVector<TLogoBlobID>{id}, TInstant::Max(), false), {}, groupId);
        WaitForBarrierInEffect(env, source, 1, id, data);
        if (client) {
            WaitForBarrierInEffect(env, target, 9, probe, data);
            CompactPile(env, target);
            UNIT_ASSERT(!ReadFlags(env, target, id).DoNotKeep);
        }
        if (compactSource) {
            CompactPile(env, source);
            UNIT_ASSERT_VALUES_EQUAL(ReadFlags(env, source, id, true).Status, NKikimrProto::NODATA);
        }

        bool corrected = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->HasEvent() && ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                const auto& gc = *ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                if (gc.WriteSource == TWriteSource::BridgeProxyRestorePut && gc.DoNotKeep && IsIn(*gc.DoNotKeep, id)) {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, MakeBlobStorageProxyID(target));
                    UNIT_ASSERT_VALUES_EQUAL(gc.Channel, id.Channel());
                    UNIT_ASSERT(!gc.Collect);
                    corrected = true;
                }
            }
            return true;
        };
        ResendEvent(env, node, std::move(held));
        if (client) {
            const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(client, false);
            CheckGetResult(*res->Get(), data);
        }
        WaitUntil(env, [&] { return corrected; }, "flags-only DoNotKeep on the repaired pile");
        if (rejoin) {
            WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        }
        WaitForBarrierInEffect(env, target, 9, id, data);
        UNIT_ASSERT(!ReadFlags(env, target, id).Keep);
        env.Runtime->FilterFunction = {};
    }

    enum class ERepairRead { Get, Range, Discover };
    void CheckSyncerCopiesDoNotKeep(bool dropReadFlag) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        const TString data = "blob whose Keep has been released";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data, true);
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, false, 0, 0,
            nullptr, new TVector<TLogoBlobID>{id}, TInstant::Max(), false), {}, groupId);

        ui32 numDropped = 0;
        if (dropReadFlag) {
            env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent()) {
                    auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                    IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite());
                    if (msg->GroupId == pileGroups[0] && actor && TypeName(*actor).Contains("TSyncerActor")) {
                        for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                            auto& r = msg->Responses[i];
                            if (r.Id == id && r.Status == NKikimrProto::OK && r.DoNotKeep) {
                                r.DoNotKeep = false;
                                ++numDropped;
                            }
                        }
                    }
                }
                return true;
            };
        }
        RejoinPile(env, 1);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_C(!dropReadFlag || numDropped, "the source read must exercise stale GC metadata");

        // Copying the body must not reintroduce effective Keep, even if the data read missed DoNotKeep.
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 1, 0, true, 1, 10,
            nullptr, nullptr, TInstant::Max(), false), {}, groupId);
        for (ui32 pile : {0, 1}) {
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, probe, data);
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, id, data);
        }
    }

    void CheckSyncerCollectsObsoleteKeptBlob(std::optional<NKikimrProto::EReplyStatus> confirmationError = {},
            bool threePiles = false) {
        TEnvironmentSetup env(threePiles ? MakeThreePileSettings() : MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        const TString data = "blob collected while the target pile is disconnected";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        for (ui32 pile = 0; pile < pileGroups.size(); ++pile) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data, true);
        }

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, true, 1, 10,
            nullptr, new TVector<TLogoBlobID>{id}, TInstant::Max(), false), {}, groupId);
        WaitForBarrierInEffect(env, pileGroups[0], 1, id, data);
        if (threePiles) {
            WaitForBarrierInEffect(env, pileGroups[2], 17, id, data);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);

        TActorId failedSyncer;
        bool failureReported = false;
        ui32 releases = 0;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (!ev->HasEvent()) {
                return true;
            }
            if (confirmationError && ev->GetTypeRewrite() == TEvBlobStorage::EvGet &&
                    ev->Recipient == MakeBlobStorageProxyID(pileGroups[0])) {
                auto *msg = ev->Get<TEvBlobStorage::TEvGet>();
                IActor *actor = env.Runtime->GetActor(ev->Sender);
                if (msg->IsIndexOnly && msg->QuerySize == 1 && msg->Queries[0].Id == id && actor &&
                        TypeName(*actor).Contains("TSyncerActor")) {
                    UNIT_ASSERT(msg->ForceGroupGeneration);
                    failedSyncer = ev->Sender;
                    auto result = msg->MakeErrorResponse(*confirmationError, "source confirmation failed",
                        TGroupId::FromValue(pileGroups[0]));
                    env.Runtime->Send(new IEventHandle(ev->Sender, ev->Recipient, result.release(), 0, ev->Cookie), nodeId);
                    return false;
                }
            }
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType && ev->Sender == failedSyncer) {
                UNIT_ASSERT(ev->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason);
                failureReported = true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                if (msg->WriteSource == TWriteSource::SyncerMergeGC && msg->DoNotKeep &&
                        std::ranges::find(*msg->DoNotKeep, id) != msg->DoNotKeep->end()) {
                    UNIT_ASSERT(msg->ForceGroupGeneration);
                    ++releases;
                }
            }
            return true;
        };
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        if (confirmationError) {
            WaitUntil(env, [&] { return failureReported; }, "failed source confirmation",
                TDuration::Seconds(1), 300);
            UNIT_ASSERT_VALUES_EQUAL(releases, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
            confirmationError.reset();
        }
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_C(releases, "the syncer must release the obsolete target Keep");
        WaitForBarrierInEffect(env, pileGroups[1], 9, id, data);
    }

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

    void CheckGetMergeKeepsDoNotKeep(NKikimrProto::EReplyStatus otherStatus, bool reverseOrder) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        const TString data = "blob with flags split between piles";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data);
        }

        std::map<ui32, std::unique_ptr<IEventHandle>> replies;
        bool hold = true;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (hold && ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                        TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                    auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                    UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
                    auto& r = msg->Responses[0];
                    UNIT_ASSERT_VALUES_EQUAL(r.Id, id);
                    const bool firstPile = msg->GroupId == pileGroups[0];
                    r.Keep = firstPile;
                    r.DoNotKeep = !firstPile;
                    if (firstPile) {
                        r.Status = otherStatus;
                        r.Buffer = {};
                    }
                    UNIT_ASSERT(replies.emplace(msg->GroupId, std::move(ev)).second);
                    return false;
                }
            }
            return true;
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        });
        WaitUntil(env, [&] { return replies.size() == 2; }, "both pile replies", TDuration::Seconds(1), 30);
        hold = false;
        env.Runtime->Send(replies.at(pileGroups[reverseOrder ? 1 : 0]).release(), 1);
        env.Sim(TDuration::MilliSeconds(10));
        env.Runtime->Send(replies.at(pileGroups[reverseOrder ? 0 : 1]).release(), 1);
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        const auto& r = res->Get()->Responses[0];
        UNIT_ASSERT_VALUES_EQUAL(r.Id, id);
        UNIT_ASSERT_VALUES_EQUAL(r.Status, otherStatus == NKikimrProto::NODATA ? NKikimrProto::OK : NKikimrProto::ERROR);
        UNIT_ASSERT_C(!r.Keep, "DoNotKeep must suppress Keep from another pile");
        UNIT_ASSERT_C(r.DoNotKeep, "merge lost DoNotKeep from the other pile");
        env.Runtime->FilterFunction = {};
    }

    void CheckRepairFailurePreservesIdentity(std::optional<NKikimrProto::EReplyStatus> failedReadStatus) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);
        const TString data = "two blobs needing repair";
        const std::array<TLogoBlobID, 2> ids{{
            TLogoBlobID(100500, 1, 1, 0, data.size(), 0),
            TLogoBlobID(100500, 1, 2, 0, data.size(), 0),
        }};
        for (const auto& id : ids) {
            PutToPileGroup(env, pileGroups[0], 1, id, data, true);
        }

        bool failureInjected = false;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            const ui32 resultType = failedReadStatus ? TEvBlobStorage::EvGetResult : TEvBlobStorage::EvPutResult;
            if (ev->GetTypeRewrite() != resultType) {
                return true;
            }
            if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); actor &&
                    TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
                if (failedReadStatus) {
                    auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                    // The original query asks for both blobs; the repair reads one full blob at a time.
                    if (msg->ResponseSz == 1 && msg->Responses[0].Id == ids[0]) {
                        msg->Responses[0].Status = *failedReadStatus;
                        msg->Responses[0].Buffer = {};
                        msg->Responses[0].Keep = false;
                        msg->Responses[0].DoNotKeep = true;
                        failureInjected = true;
                    }
                } else {
                    if (auto *msg = ev->Get<TEvBlobStorage::TEvPutResult>(); msg->Id == ids[0]) {
                        msg->Status = NKikimrProto::ERROR;
                        failureInjected = true;
                    }
                }
            }
            return true;
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[2]);
            for (ui32 i = 0; i < ids.size(); ++i) {
                queries[i].Set(ids[i], 2, 3);
            }
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(queries, ids.size(), TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT(failureInjected);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, ids.size());
        for (ui32 i = 0; i < ids.size(); ++i) {
            const auto& r = res->Get()->Responses[i];
            UNIT_ASSERT_VALUES_EQUAL(r.Id, ids[i]);
            UNIT_ASSERT_VALUES_EQUAL(r.Shift, 2);
            UNIT_ASSERT_VALUES_EQUAL(r.RequestedSize, 3);
            const bool released = i == 0 && failedReadStatus.has_value();
            UNIT_ASSERT_VALUES_EQUAL(r.Keep, !released);
            UNIT_ASSERT_VALUES_EQUAL(r.DoNotKeep, released);
            UNIT_ASSERT_VALUES_EQUAL(r.Status, i ? NKikimrProto::OK : NKikimrProto::ERROR);
            UNIT_ASSERT_VALUES_EQUAL(r.Buffer.ConvertToString(), i ? data.substr(2, 3) : TString());
        }
        env.Runtime->FilterFunction = {};
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, ids[1], data), NKikimrProto::OK);
    }

} // anonymous

Y_UNIT_TEST_SUITE(BridgeKeepFlags) {

    Y_UNIT_TEST(QuorumReleaseAndAmbiguousCompaction) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const auto groupId = CreatePoolAndGetGroup(env);
        const auto piles = GetPileGroups(env, groupId);
        const auto info = env.GetGroupInfo(piles[0]);
        const auto survivor = info->GetVDiskId(0);
        const TString data = "payload kept in the other pile";
        const TLogoBlobID released(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID live(100500, 1, 1, 0, data.size(), 1);
        const TLogoBlobID probe(100500, 1, 2, 0, data.size(), 0);
        PutToPileGroup(env, piles[1], 9, live, data, true);
        PutToPileGroup(env, piles[0], 1, probe, data);
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(released.TabletID(), 1, 0, 0, true, 1, 10,
            new TVector<TLogoBlobID>{released}, nullptr, TInstant::Max(), false), piles[0]);
        WaitForBarrierInEffect(env, piles[0], 1, probe, data);
        env.Sim(TDuration::Seconds(30));
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVSyncResult ||
                    ev->GetTypeRewrite() == TEvBlobStorage::EvVSyncFullResult) {
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvVCollectGarbage) {
                const auto *actor = env.Runtime->GetActor(ev->GetRecipientRewrite());
                if (actor && actor->GetActivityType() == NKikimrServices::TActivity::BS_SKELETON_FRONT) {
                    const auto& rec = ev->Get<TEvBlobStorage::TEvVCollectGarbage>()->Record;
                    const auto disk = VDiskIDFromVDiskID(rec.GetVDiskID());
                    if (disk.GroupID == info->GroupID && rec.GetTabletId() == live.TabletID()) {
                        return rec.KeepSize() ? disk == survivor : disk != survivor;
                    }
                }
            }
            return true;
        };
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(released.TabletID(), 1, 1, 0, false, 0, 0,
            nullptr, new TVector<TLogoBlobID>{released}, TInstant::Max(), false), piles[0]);
        // A release quorum outweighs the last replica's Keep until the release records are compacted.
        auto item = ReadFlags(env, piles[0], released, true);
        UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
        UNIT_ASSERT(item.DoNotKeep && !item.Keep);
        CompactPile(env, piles[0]);
        const auto edge = env.Runtime->AllocateEdgeActor(2, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            auto *keep = new TEvBlobStorage::TEvCollectGarbage(live.TabletID(), Max<ui32>(), 0, 0, false, 0, 0,
                new TVector<TLogoBlobID>{live}, nullptr, TInstant::Max(), false, TWriteSource::SyncerMergeGC, false, true);
            keep->ForceGroupGeneration = info->GroupGeneration;
            SendToBSProxy(edge, piles[0], keep);
        });
        WaitUntil(env, [&] { return ReadFlags(env, piles[0], live).Keep; }, "first replica applying the live Keep");
        for (const auto& id : {released, live}) {
            // Both histories leave one visible Keep among empty replicas.
            UNIT_ASSERT(ReadFlags(env, piles[0], id).Keep);
            auto checked = ReadFlags(env, piles[0], id, true);
            UNIT_ASSERT_VALUES_EQUAL(checked.Status, NKikimrProto::UNKNOWN);
            UNIT_ASSERT(!checked.DoNotKeep && !checked.Keep);
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, piles[1], 9, live, data), NKikimrProto::OK);
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST_TWIN(DelayedRepairCannotReinstateReleasedKeep, delayPut) {
        CheckDelayedRepair(delayPut ? EHoldRepair::RestorePut : EHoldRepair::TargetGetReply);
    }

    Y_UNIT_TEST(DelayedRepairAfterSourceCompaction) {
        CheckDelayedRepair(EHoldRepair::RestorePut, true);
    }

    Y_UNIT_TEST_TWIN(SyncerRepairRacesWithRelease, flagOnly) {
        CheckDelayedRepair(flagOnly ? EHoldRepair::SyncerKeep : EHoldRepair::SyncerPut);
    }

    Y_UNIT_TEST(ReconcileKeepRacesWithRelease) {
        CheckDelayedRepair(EHoldRepair::ReconcileKeep);
    }

    Y_UNIT_TEST_TWIN(RepairPreservesKeepWithoutConfirmedRelease, unavailable) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const auto groupId = CreatePoolAndGetGroup(env);
        const auto piles = GetPileGroups(env, groupId);
        const TString data = "live kept blob";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, piles[0], 1, id, data, true);
        THashSet<ui64> attempts;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->HasEvent() && ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
                const auto& get = *ev->Get<TEvBlobStorage::TEvGet>();
                if (get.VerifyKeepFlags && get.Queries[0].Id == id) {
                    attempts.insert(ev->Cookie);
                    return !unavailable;
                }
            }
            if (ev->HasEvent() && ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage) {
                const auto& gc = *ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                UNIT_ASSERT(!gc.DoNotKeep || !IsIn(*gc.DoNotKeep, id));
            }
            return true;
        };
        const auto client = StartRepair(env, groupId, id);
        const auto response = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(client, false);
        CheckGetResult(*response->Get(), data);
        UNIT_ASSERT(attempts.size() <= 1); // the client is answered while a lost check is still outstanding
        env.Sim(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(attempts.size(), unavailable ? 3u : 1u);
        for (auto pile : piles) {
            auto item = ReadFlags(env, pile, id);
            UNIT_ASSERT_VALUES_EQUAL(item.Status, NKikimrProto::OK);
            UNIT_ASSERT(item.Keep && !item.DoNotKeep);
        }
        env.Runtime->FilterFunction = {};
    }

    Y_UNIT_TEST_TWIN(GetRepairReadFailurePreservesIdentityAndFlags, noData) {
        CheckRepairFailurePreservesIdentity(noData ? NKikimrProto::NODATA : NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(GetRepairPutFailurePreservesIdentityAndFlags) {
        CheckRepairFailurePreservesIdentity(std::nullopt);
    }

    Y_UNIT_TEST_TWIN(GetMergeKeepsDoNotKeepOnNodataReplacement, reverseOrder) {
        CheckGetMergeKeepsDoNotKeep(NKikimrProto::NODATA, reverseOrder);
    }

    Y_UNIT_TEST_TWIN(GetMergeKeepsDoNotKeepOnErrorReplacement, reverseOrder) {
        CheckGetMergeKeepsDoNotKeep(NKikimrProto::ERROR, reverseOrder);
    }

    Y_UNIT_TEST(SyncerCopiesDoNotKeepToMissingTarget) {
        CheckSyncerCopiesDoNotKeep(false);
    }

    Y_UNIT_TEST(SyncerKeepsDoNotKeepFromAssimilation) {
        CheckSyncerCopiesDoNotKeep(true);
    }

    Y_UNIT_TEST(SyncerCollectsObsoleteKeptBlob) {
        CheckSyncerCollectsObsoleteKeptBlob();
    }

    Y_UNIT_TEST(SyncerCollectsObsoleteKeptBlobAcrossSources) {
        CheckSyncerCollectsObsoleteKeptBlob({}, true);
    }

    Y_UNIT_TEST(SyncerDoesNotInferDeletionFromSuspendedSource) {
        TEnvironmentSetup env(MakeThreePileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto groups = GetPileGroups(env, groupId);
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));

        const TString data = "written after the assigned source is suspended";
        const TLogoBlobID id(100500, 1, 5, 0, data.size(), 0);
        enum { WAIT_BARRIER_SCAN, WAIT_BLOB_SCAN, HELD, RELEASED } scan = WAIT_BARRIER_SCAN;
        TActorId syncer;
        std::unique_ptr<IEventHandle> heldScan;
        ui32 scanNode = 0;
        bool sourceScanned = false;
        bool holdConfig = false;
        std::unique_ptr<IEventHandle> bscConfig;
        ui32 bscNode = 0;
        std::unique_ptr<IEventHandle> completion;
        ui32 releases = 0;
        std::set<ui32> confirmedSources;
        env.Runtime->FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (holdConfig && ev->GetTypeRewrite() == TEvNodeWardenStorageConfig::EventType) {
                const auto *actor = env.Runtime->GetActor(ev->GetRecipientRewrite());
                if (actor && TypeName(*actor).Contains("TBlobStorageController")) {
                    bscConfig = std::move(ev);
                    bscNode = node;
                    return false;
                }
            }
            if (scan == HELD && ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult &&
                    ev->GetRecipientRewrite() == syncer) {
                sourceScanned = true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilate &&
                    ev->Recipient == MakeBlobStorageProxyID(groups[1])) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvAssimilate>();
                if (scan == WAIT_BARRIER_SCAN && msg->SkipBlocksUpTo && !msg->SkipBarriersUpTo && msg->SkipBlobsUpTo) {
                    scan = WAIT_BLOB_SCAN;
                } else if (scan == WAIT_BLOB_SCAN && msg->SkipBlocksUpTo && msg->SkipBarriersUpTo && !msg->SkipBlobsUpTo) {
                    syncer = ev->Sender;
                    heldScan = std::move(ev);
                    scanNode = node;
                    scan = HELD;
                    return false;
                }
            }
            if (holdConfig && ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifySyncerFinished::EventType && ev->Sender == syncer) {
                completion = std::move(ev);
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->GetRecipientRewrite() == syncer) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                UNIT_ASSERT_VALUES_EQUAL(msg->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Id, id);
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Status,
                    msg->GroupId == groups[0] ? NKikimrProto::NODATA : NKikimrProto::OK);
                UNIT_ASSERT(!msg->Responses[0].DoNotKeep);
                confirmedSources.insert(msg->GroupId);
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvCollectGarbage && ev->Sender == syncer) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                if (msg->DoNotKeep && std::ranges::find(*msg->DoNotKeep, id) != msg->DoNotKeep->end()) {
                    ++releases;
                }
            }
            return true;
        };
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] { return sourceScanned; }, "old source scanned", TDuration::Seconds(1), 600);
        holdConfig = true;
        auto state = QueryStorageConfig(env).GetClusterState();
        TBridgePileId::FromPileIndex(2).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(2).CopyToProto(&state, &NKikimrBridge::TClusterState::SetPromotedPile);
        state.SetGeneration(state.GetGeneration() + 1);
        const auto promoted = InvokeDistconf(env, [&](auto& record) {
            record.MutableSwitchBridgeClusterState()->MutableNewClusterState()->CopyFrom(state);
        });
        UNIT_ASSERT_EQUAL_C(promoted.GetStatus(), NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK,
            promoted.GetErrorReason());
        SwitchPileState(env, 0, NKikimrBridge::TClusterState::SUSPENDED);
        env.Sim(TDuration::Seconds(10));
        const auto edge = env.Runtime->AllocateEdgeActor(17, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false)->Get()->Status,
            NKikimrProto::OK);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, false, 0, 0,
                new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false));
        });
        UNIT_ASSERT_VALUES_EQUAL(env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false)->Get()->Status,
            NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, groups[0], 1, id, data), NKikimrProto::NODATA);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, groups[2], 17, id, data), NKikimrProto::OK);

        scan = RELEASED;
        env.Runtime->Send(heldScan.release(), scanNode);
        WaitUntil(env, [&] { return bool(completion); }, "stale source attempt completion");
        UNIT_ASSERT_VALUES_EQUAL_C(releases, 0, "source suspension must not turn a live blob into garbage");
        UNIT_ASSERT(confirmedSources.contains(groups[0]) && confirmedSources.contains(groups[2]));
        UNIT_ASSERT(completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason);
        UNIT_ASSERT_VALUES_EQUAL(*completion->Get<NStorage::TEvNodeWardenNotifySyncerFinished>()->ErrorReason,
            "errors encountered during sync");
        holdConfig = false;
        UNIT_ASSERT(bscConfig);
        env.Runtime->Send(bscConfig.release(), bscNode);
        env.Runtime->Send(completion.release(), syncer.NodeId());
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = {};

        const TLogoBlobID probe(100500, 1, 6, 0, data.size(), 0);
        for (ui32 pile : {1, 2}) {
            PutToPileGroup(env, groups[pile], 1 + 8 * pile, probe, data);
            const auto collected = QueryPileGroup<TEvBlobStorage::TEvCollectGarbageResult>(env, groups[pile],
                1 + 8 * pile, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 1, 0, true, 1, 10,
                    nullptr, nullptr, TInstant::Max(), false));
            UNIT_ASSERT_VALUES_EQUAL(collected->Status, NKikimrProto::OK);
            WaitForBarrierInEffect(env, groups[pile], 1 + 8 * pile, probe, data);
            UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, groups[pile], 1 + 8 * pile, id, data), NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(SyncerRetriesFailedAbsenceCheck) {
        CheckSyncerCollectsObsoleteKeptBlob(NKikimrProto::ERROR);
    }

    Y_UNIT_TEST(SyncerRetriesRacingAbsenceCheck) {
        CheckSyncerCollectsObsoleteKeptBlob(NKikimrProto::RACE);
    }

    // Keep reaches the target during metadata synchronization while Put still goes only to the source. A target
    // scan seeing that new Keep must not interpret an earlier source scan's absence as a deletion.
    Y_UNIT_TEST(SyncerKeepsBlobWrittenBetweenScans) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = GetPileGroups(env, groupId);

        SwitchPileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        env.Sim(TDuration::Seconds(30));

        enum { WAIT_BARRIER_SCAN, WAIT_BLOB_SCAN, HELD, RELEASED } state = WAIT_BARRIER_SCAN;
        std::unique_ptr<IEventHandle> held;
        ui32 heldNodeId = 0;
        bool sourceScanFinished = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (state == HELD && ev->GetTypeRewrite() == TEvBlobStorage::EvAssimilateResult && ev->HasEvent() &&
                    ev->Recipient == held->Sender && ev->Get<TEvBlobStorage::TEvAssimilateResult>()->Status == NKikimrProto::OK) {
                sourceScanFinished = true;
            }
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvAssimilate || !ev->HasEvent() ||
                    ev->Recipient != MakeBlobStorageProxyID(pileGroups[1])) {
                return true;
            }
            const auto *msg = ev->Get<TEvBlobStorage::TEvAssimilate>();
            const bool barrierScan = msg->SkipBlocksUpTo && !msg->SkipBarriersUpTo && msg->SkipBlobsUpTo;
            const bool blobScan = msg->SkipBlocksUpTo && msg->SkipBarriersUpTo && !msg->SkipBlobsUpTo;
            if (state == WAIT_BARRIER_SCAN && barrierScan) {
                state = WAIT_BLOB_SCAN;
            } else if (state == WAIT_BLOB_SCAN && blobScan) {
                held = std::move(ev);
                heldNodeId = nodeId;
                state = HELD;
                return false;
            }
            return true;
        };
        SwitchPileState(env, 1, NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
        WaitUntil(env, [&] { return sourceScanFinished; }, "source scan while target scan is held",
            TDuration::Seconds(1), 600);

        const TString data = "kept blob written between sync scans";
        const TLogoBlobID id(100500, 1, 5, 0, data.size(), 0);
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        });
        auto put = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(put->Get()->Status, NKikimrProto::OK, put->Get()->ToString());
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 0, 0, false, 0, 0,
            new TVector<TLogoBlobID>{id}, nullptr, TInstant::Max(), false), {}, groupId);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::NODATA);

        state = RELEASED;
        env.Runtime->Send(held.release(), heldNodeId);
        WaitForPileState(env, 1, NKikimrBridge::TClusterState::SYNCHRONIZED);
        env.Runtime->FilterFunction = {};

        // First prove the barrier takes effect using an unkept probe, then check the kept blob in each physical group.
        const TLogoBlobID probe(100500, 1, 6, 0, data.size(), 0);
        for (ui32 pile : {0, 1}) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, probe, data);
        }
        CollectGarbage(env, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 1, 1, 0, true, 1, 10,
            nullptr, nullptr, TInstant::Max(), false), {}, groupId);
        for (ui32 pile : {0, 1}) {
            WaitForBarrierInEffect(env, pileGroups[pile], 1 + 8 * pile, probe, data);
            UNIT_ASSERT_VALUES_EQUAL_C(ReadFromPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data),
                NKikimrProto::OK, "kept blob lost in pile " << pile);
        }
    }

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

    Y_UNIT_TEST_TWIN(GetRestoreDoesNotReissueReleasedKeep, vetoInFirstRead) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        const TString data = "blob to be restored";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data);

        bool sawIndex = false;
        bool sawBody = false;
        ui32 restorePuts = 0;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (!ev->HasEvent()) {
                return true;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvPut) {
                const auto& put = *ev->Get<TEvBlobStorage::TEvPut>();
                if (put.Id == id && put.WriteSource == TWriteSource::BridgeProxyRestorePut) {
                    UNIT_ASSERT_C(!put.IssueKeepFlag, "repair must honor DoNotKeep from either read");
                    ++restorePuts;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult) {
                const IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite());
                auto& result = *ev->Get<TEvBlobStorage::TEvGetResult>();
                if (actor && TypeName(*actor).Contains("TBridgedBlobStorageProxyActor") &&
                        result.GroupId == pileGroups[0] && result.ResponseSz == 1) {
                    auto& response = result.Responses[0];
                    if (response.Id == id && response.Status == NKikimrProto::OK) {
                        const bool isBodyRead = !response.Buffer.empty();
                        (isBodyRead ? sawBody : sawIndex) = true;
                        response.DoNotKeep = isBodyRead != vetoInFirstRead;
                        response.Keep = !response.DoNotKeep;
                    }
                }
            }
            return true;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true, /*isIndexOnly=*/ true));
        });
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT(sawIndex && sawBody);
        UNIT_ASSERT_VALUES_EQUAL(restorePuts, 1);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        const auto& response = res->Get()->Responses[0];
        UNIT_ASSERT_VALUES_EQUAL(response.Status, NKikimrProto::OK);
        UNIT_ASSERT(response.DoNotKeep && !response.Keep);
        UNIT_ASSERT_VALUES_EQUAL(ReadFromPileGroup(env, pileGroups[1], 9, id, data), NKikimrProto::OK);
    }

    Y_UNIT_TEST(GetRestoreReusesFirstReplyBody) {
        TEnvironmentSetup env(MakeTwoPileSettings());
        const ui32 groupId = CreatePoolAndGetGroup(env);
        const auto pileGroups = GetPileGroups(env, groupId);
        const TString data = "full body for repair";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data);

        std::array<ui32, 2> getCounts{};
        std::array<std::unique_ptr<IEventHandle>, 2> replies;
        bool holdReplies = true;
        env.Runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet) {
                for (size_t i = 0; i < getCounts.size(); ++i) {
                    if (ev->Recipient == MakeBlobStorageProxyID(pileGroups[i])) {
                        ++getCounts[i];
                    }
                }
            } else if (holdReplies && ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent()) {
                const auto& result = *ev->Get<TEvBlobStorage::TEvGetResult>();
                for (size_t i = 0; i < replies.size(); ++i) {
                    if (result.GroupId == pileGroups[i] && result.ResponseSz == 1 && result.Responses[0].Id == id) {
                        UNIT_ASSERT(!replies[i]);
                        UNIT_ASSERT_VALUES_EQUAL(result.Responses[0].Status, i ? NKikimrProto::NODATA : NKikimrProto::OK);
                        replies[i] = std::move(ev);
                        return false;
                    }
                }
            }
            return true;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        });
        WaitUntil(env, [&] { return replies[0] && replies[1]; }, "both physical GET replies");
        holdReplies = false;
        env.Runtime->Send(replies[0].release(), 1);
        env.Sim(TDuration::MilliSeconds(10));
        env.Runtime->Send(replies[1].release(), 1);
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        env.Runtime->FilterFunction = {};

        CheckGetResult(*res->Get(), data);
        UNIT_ASSERT_VALUES_EQUAL(getCounts[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(getCounts[1], 1);
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
        TEnvironmentSetup env(MakeTwoPileSettings([](TVDiskConfig& config) {
            config.BarrierValidation = true;
        }));
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
