#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <span>

using NBridgeTest::GetBridgedLocation;
using NBridgeTest::PutToPileGroup;

namespace {

    template<typename T>
    T *FillInGroupGeneration(T *ev) {
        ev->ForceGroupGeneration = 1;
        return ev;
    }

    TActorId GetBridgeProxyActor(TEnvironmentSetup& env, ui32 groupId) {
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
        });
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edge);
        UNIT_ASSERT(res);
        return res->Sender;
    }

    void SendGet(TEnvironmentSetup& env, const TActorId& edge, ui32 groupId, std::span<const TLogoBlobID> ids) {
        env.Runtime->WrapInActorContext(edge, [&] {
            TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[ids.size()]);
            for (size_t i = 0; i < ids.size(); ++i) {
                queries[i].Set(ids[i]);
            }
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(queries, ids.size(), TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
    }

    enum class EAbsenceRead { Get, Range };

    // one blob is in pile 0 only, as the node knowing pile 1 is disconnected writes it, while the reading node takes both
    // piles for synchronized ones; pile 1 answers first, and its lack of the blob must not be the answer
    void CheckAbsenceNeedsAllPiles(EAbsenceRead read) {
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        env.CreatePool();
        env.Sim(TDuration::Seconds(30));
        const ui32 groupId = env.GetGroups().front();
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data = "blob data";
        const TLogoBlobID inBoth(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID inPile0(100500, 1, 2, 0, data.size(), 0);
        auto put = [&](ui32 pile, const TLogoBlobID& id) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data);
        };
        put(0, inBoth);
        put(1, inBoth);
        put(0, inPile0);

        // hold the answer of pile 0 until pile 1 has answered; take the result before it reaches the edge actor, as it
        // may come while the test is not waiting for it
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        std::unique_ptr<IEventHandle> held;
        ui32 heldNodeId = 0;
        bool pile1Answered = false;
        std::unique_ptr<IEventHandle> result;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge) {
                result = std::move(ev);
                return false;
            }
            ui32 pileGroupId;
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent()) {
                pileGroupId = ev->Get<TEvBlobStorage::TEvGetResult>()->GroupId;
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvRangeResult && ev->HasEvent()) {
                pileGroupId = ev->Get<TEvBlobStorage::TEvRangeResult>()->GroupId;
            } else {
                return true;
            }
            if (ev->GetRecipientRewrite() != bridgeProxy) {
                return true;
            }
            if (pileGroupId == pileGroups[0] && !pile1Answered) {
                held = std::move(ev);
                heldNodeId = nodeId;
                return false;
            } else if (pileGroupId == pileGroups[1]) {
                pile1Answered = true;
            }
            return true;
        };

        env.Runtime->WrapInActorContext(edge, [&] {
            if (read == EAbsenceRead::Get) {
                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[2]);
                q[0].Set(inBoth);
                q[1].Set(inPile0);
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(q, 2, TInstant::Max(),
                    NKikimrBlobStorage::FastRead));
            } else {
                SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvRange(inBoth.TabletID(),
                    TLogoBlobID(inBoth.TabletID(), 0, 0, 0, 0, 0),
                    TLogoBlobID(inBoth.TabletID(), Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                        TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie), /*mustRestoreFirst=*/ false, TInstant::Max()));
            }
        });
        NBridgeTest::WaitUntil(env, [&] { return held && pile1Answered; }, "answers of both piles",
            TDuration::MilliSeconds(100), 300);
        env.Runtime->Send(held.release(), heldNodeId);
        NBridgeTest::WaitUntil(env, [&] { return !!result; }, "result", TDuration::MilliSeconds(100), 300);
        env.Runtime->FilterFunction = {};

        if (read == EAbsenceRead::Get) {
            auto *msg = result->Get<TEvBlobStorage::TEvGetResult>();
            UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::OK, msg->ToString());
            UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 2);
            for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                const auto& r = msg->Responses[i];
                UNIT_ASSERT_VALUES_EQUAL_C(r.Status, NKikimrProto::OK, msg->ToString());
                UNIT_ASSERT_VALUES_EQUAL(r.Buffer.ConvertToString(), data);
            }
        } else {
            auto *msg = result->Get<TEvBlobStorage::TEvRangeResult>();
            UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::OK, msg->ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(msg->Responses.size(), 2, msg->ToString());
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Id, inBoth);
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[1].Id, inPile0);
            for (const auto& r : msg->Responses) {
                UNIT_ASSERT_VALUES_EQUAL(r.Buffer, data);
            }
        }
    }

    TEnvironmentSetup::TSettings MakeLocalPileFirstSettings(bool enabled = true, ui32 numPiles = 2) {
        TFeatureFlags featureFlags;
        featureFlags.SetEnableBridgeProxyLocalPileFirstGet(enabled);
        return {
            .NodeCount = 8 * numPiles,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .FeatureFlags = featureFlags,
            .SelfManagementConfig = true,
            .NumPiles = numPiles,
            .AutomaticBootstrap = true,
        };
    }

    void CheckLocalPileFirstGet(bool localHasBlob, bool mustRestoreFirst = false, bool isIndexOnly = false) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings()};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data = "blob data";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        for (ui32 pile = 0; pile < 2; ++pile) {
            if (pile == 0 && !localHasBlob) {
                continue;
            }
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data);
        }

        const bool localPileFirst = !mustRestoreFirst && !isIndexOnly;
        std::vector<ui32> requests;
        bool localReplied = false;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->HasEvent() && ev->Sender == bridgeProxy) {
                for (ui32 pileGroupId : pileGroups) {
                    if (ev->Recipient == MakeBlobStorageProxyID(pileGroupId)) {
                        if (localPileFirst && !localHasBlob && pileGroupId == pileGroups[1]) {
                            UNIT_ASSERT(localReplied);
                        }
                        requests.push_back(pileGroupId);
                    }
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent() &&
                    ev->GetRecipientRewrite() == bridgeProxy &&
                    ev->Get<TEvBlobStorage::TEvGetResult>()->GroupId == pileGroups[0]) {
                localReplied = true;
            }
            return true;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead, mustRestoreFirst, isIndexOnly));
        });
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        if (isIndexOnly) {
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Responses[0].Status, NKikimrProto::OK, res->Get()->ToString());
        } else {
            NBridgeTest::CheckGetResult(*res->Get(), data);
        }
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT_VALUES_EQUAL(requests.size(), localPileFirst && localHasBlob ? 1 : 2);
        if (localPileFirst) {
            UNIT_ASSERT(localReplied);
            UNIT_ASSERT_VALUES_EQUAL(requests.front(), pileGroups[0]);
            if (!localHasBlob) {
                UNIT_ASSERT_VALUES_EQUAL(requests.back(), pileGroups[1]);
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL(std::ranges::count(requests, pileGroups[0]), 1);
            UNIT_ASSERT_VALUES_EQUAL(std::ranges::count(requests, pileGroups[1]), 1);
        }
    }

}

Y_UNIT_TEST_SUITE(BridgeGet) {

    Y_UNIT_TEST(LocalPileFirst) {
        CheckLocalPileFirstGet(true);
    }

    Y_UNIT_TEST(LocalPileFirstFallsBackOnNoData) {
        CheckLocalPileFirstGet(false);
    }

    Y_UNIT_TEST(LocalPileFirstSkippedForIndexOnly) {
        CheckLocalPileFirstGet(true, false, true);
    }

    Y_UNIT_TEST(LocalPileFirstSkippedForMustRestoreFirst) {
        CheckLocalPileFirstGet(true, true, false);
    }

    Y_UNIT_TEST(LocalPileFirstRequestsOnlyRemainingBlobs) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings()};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);

        const TString data[] = {"first blob", "second blob"};
        const TLogoBlobID ids[] = {
            TLogoBlobID(100500, 1, 1, 0, data[0].size(), 0),
            TLogoBlobID(100500, 1, 2, 0, data[1].size(), 0),
        };
        for (ui32 pile = 0; pile < 2; ++pile) {
            for (ui32 i = 0; i < 2; ++i) {
                if (pile == 0 && i == 1) {
                    continue;
                }
                PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, ids[i], data[i]);
            }
        }

        std::vector<TLogoBlobID> fallbackQueries;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->HasEvent() &&
                    ev->Recipient == MakeBlobStorageProxyID(pileGroups[1])) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGet>();
                for (ui32 i = 0; i < msg->QuerySize; ++i) {
                    fallbackQueries.push_back(msg->Queries[i].Id);
                }
            }
            return true;
        };

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        SendGet(env, edge, groupId, ids);
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Status, NKikimrProto::OK, res->Get()->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 2);
        for (ui32 i = 0; i < 2; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Responses[i].Status, NKikimrProto::OK, res->Get()->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[i].Buffer.ConvertToString(), data[i]);
        }
        env.Sim(TDuration::MilliSeconds(100));
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT_VALUES_EQUAL(fallbackQueries.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fallbackQueries.front(), ids[1]);
    }

    Y_UNIT_TEST(LocalPileFirstFallbackMergesRemainingBlobs) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings(true, 3)};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data[] = {"data A", "data B1", "data C"};
        const TString conflictingData = "data B2";
        const TLogoBlobID ids[] = {
            TLogoBlobID(100500, 1, 1, 0, data[0].size(), 0),
            TLogoBlobID(100500, 1, 2, 0, data[1].size(), 0),
            TLogoBlobID(100500, 1, 3, 0, data[2].size(), 0),
        };
        PutToPileGroup(env, pileGroups[0], 1, ids[0], data[0]);
        PutToPileGroup(env, pileGroups[1], 9, ids[1], data[1]);
        PutToPileGroup(env, pileGroups[2], 17, ids[1], conflictingData);
        PutToPileGroup(env, pileGroups[2], 17, ids[2], data[2]);

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        std::unique_ptr<IEventHandle> held;
        std::unique_ptr<IEventHandle> result;
        ui32 heldNodeId = 0;
        bool firstFallbackAnswered = false;
        bool released = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge) {
                result = std::move(ev);
                return false;
            }
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvGetResult || !ev->HasEvent()) {
                return true;
            }
            if (ev->GetRecipientRewrite() != bridgeProxy) {
                return true;
            }
            auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
            if (msg->GroupId == pileGroups[1]) {
                UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 2);
                msg->Responses[0].Keep = true;
                firstFallbackAnswered = true;
            } else if (msg->GroupId == pileGroups[2] && !held && !released) {
                UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 2);
                msg->Responses[0].DoNotKeep = true;
                held = std::move(ev);
                heldNodeId = nodeId;
                return false;
            }
            return true;
        };

        SendGet(env, edge, groupId, ids);
        NBridgeTest::WaitUntil(env, [&] { return held && firstFallbackAnswered; }, "ordered fallback responses",
            TDuration::MilliSeconds(100), 300);
        env.Sim(TDuration::MilliSeconds(100));
        released = true;
        env.Runtime->Send(held.release(), heldNodeId);
        NBridgeTest::WaitUntil(env, [&] { return !!result; }, "merged response", TDuration::MilliSeconds(100), 300);
        env.Runtime->FilterFunction = {};

        auto *res = result->Get<TEvBlobStorage::TEvGetResult>();
        UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 3);
        for (ui32 i : {0, 2}) {
            UNIT_ASSERT_VALUES_EQUAL_C(res->Responses[i].Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Responses[i].Buffer.ConvertToString(), data[i]);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(res->Responses[1].Status, NKikimrProto::ERROR, res->ToString());
        UNIT_ASSERT(res->Responses[1].Keep);
        UNIT_ASSERT(res->Responses[1].DoNotKeep);
    }

    Y_UNIT_TEST(LocalPileFirstFullFallbackCompletesWithoutOtherPiles) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings(true, 3)};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data = "blob data";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[1], 9, id, data);
        PutToPileGroup(env, pileGroups[2], 17, id, data);

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        std::unique_ptr<IEventHandle> firstFallback;
        std::unique_ptr<IEventHandle> thirdPile;
        std::unique_ptr<IEventHandle> result;
        ui32 firstFallbackNodeId = 0;
        bool firstFallbackReleased = false;
        env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge) {
                result = std::move(ev);
                return false;
            }
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvGetResult || !ev->HasEvent() ||
                    ev->GetRecipientRewrite() != bridgeProxy) {
                return true;
            }
            auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
            if (msg->GroupId == pileGroups[0]) {
                msg->Responses[0].Keep = true;
            } else if (msg->GroupId == pileGroups[1] && !firstFallback && !firstFallbackReleased) {
                msg->Responses[0].DoNotKeep = true;
                firstFallback = std::move(ev);
                firstFallbackNodeId = nodeId;
                return false;
            } else if (msg->GroupId == pileGroups[2] && !thirdPile) {
                thirdPile = std::move(ev);
                return false;
            }
            return true;
        };

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        NBridgeTest::WaitUntil(env, [&] { return firstFallback && thirdPile; }, "both fallback responses",
            TDuration::MilliSeconds(100), 300);
        firstFallbackReleased = true;
        env.Runtime->Send(firstFallback.release(), firstFallbackNodeId);
        NBridgeTest::WaitUntil(env, [&] { return !!result; }, "response before the third pile replies",
            TDuration::MilliSeconds(100), 300);
        UNIT_ASSERT(thirdPile);
        env.Runtime->FilterFunction = {};
        thirdPile.reset();

        NBridgeTest::CheckGetResult(*result->Get<TEvBlobStorage::TEvGetResult>(), data);
        const auto& response = result->Get<TEvBlobStorage::TEvGetResult>()->Responses[0];
        UNIT_ASSERT(response.Keep);
        UNIT_ASSERT(response.DoNotKeep);
    }

    Y_UNIT_TEST(LocalPileFirstFullFallbackPreservesConflict) {
        for (bool blobInLastPile : {false, true}) {
            TEnvironmentSetup env{MakeLocalPileFirstSettings(true, 4)};
            const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
            const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
            const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

            const TString data[] = {"A1", "A2", "blob B"};
            const TLogoBlobID ids[] = {
                TLogoBlobID(100500, 1, 1, 0, data[0].size(), 0),
                TLogoBlobID(100500, 1, 2, 0, data[2].size(), 0),
            };
            PutToPileGroup(env, pileGroups[1], 9, ids[0], data[0]);
            PutToPileGroup(env, pileGroups[2], 17, ids[0], data[1]);
            PutToPileGroup(env, pileGroups[blobInLastPile ? 3 : 2], blobInLastPile ? 25 : 17, ids[1], data[2]);
            PutToPileGroup(env, pileGroups[3], 25, ids[0], data[0]);

            const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
            THashMap<ui32, std::pair<ui32, std::unique_ptr<IEventHandle>>> replies;
            bool captureReplies = true;
            std::unique_ptr<IEventHandle> result;
            env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetRecipientRewrite() == edge) {
                    result = std::move(ev);
                    return false;
                }
                if (!captureReplies || ev->GetTypeRewrite() != TEvBlobStorage::EvGetResult || !ev->HasEvent() ||
                        ev->GetRecipientRewrite() != bridgeProxy) {
                    return true;
                }
                const auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                for (ui32 pile = 1; pile < pileGroups.size(); ++pile) {
                    if (msg->GroupId == pileGroups[pile]) {
                        UNIT_ASSERT(!replies.contains(pile));
                        replies.emplace(pile, std::pair{nodeId, std::move(ev)});
                        return false;
                    }
                }
                return true;
            };

            SendGet(env, edge, groupId, ids);
            NBridgeTest::WaitUntil(env, [&] { return replies.size() == 3; }, "all fallback responses",
                TDuration::MilliSeconds(100), 300);
            captureReplies = false;
            auto release = [&](ui32 pile) {
                auto& [nodeId, reply] = replies.at(pile);
                env.Runtime->Send(reply.release(), nodeId);
            };
            release(1);
            env.Sim(TDuration::MilliSeconds(100));
            UNIT_ASSERT(!result);
            release(2);
            if (blobInLastPile) {
                env.Sim(TDuration::MilliSeconds(100));
                UNIT_ASSERT(!result);
                release(3);
            }
            NBridgeTest::WaitUntil(env, [&] { return !!result; }, "merged response",
                TDuration::MilliSeconds(100), 300);
            if (!blobInLastPile) {
                UNIT_ASSERT(replies.at(3).second);
            }
            env.Runtime->FilterFunction = {};
            replies.clear();

            auto *res = result->Get<TEvBlobStorage::TEvGetResult>();
            UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->ResponseSz, 2);
            UNIT_ASSERT_VALUES_EQUAL_C(res->Responses[0].Status, NKikimrProto::ERROR, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(res->Responses[1].Status, NKikimrProto::OK, res->ToString());
            UNIT_ASSERT_VALUES_EQUAL(res->Responses[1].Buffer.ConvertToString(), data[2]);
        }
    }

    Y_UNIT_TEST(LocalPileFirstSkippedForFiniteDeadline) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings()};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data = "blob data";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        for (ui32 pile = 0; pile < 2; ++pile) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data);
        }

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        std::unique_ptr<IEventHandle> heldLocal;
        bool remoteAnswered = false;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() != TEvBlobStorage::EvGetResult || !ev->HasEvent()) {
                return true;
            }
            if (ev->GetRecipientRewrite() != bridgeProxy) {
                return true;
            }
            const auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
            if (msg->GroupId == pileGroups[0] && !heldLocal) {
                heldLocal = std::move(ev);
                return false;
            }
            remoteAnswered |= msg->GroupId == pileGroups[1];
            return true;
        };

        const TInstant deadline = env.Runtime->GetClock() + TDuration::Seconds(5);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, deadline,
                NKikimrBlobStorage::FastRead));
        });
        const auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false, deadline);
        const TInstant replied = env.Runtime->GetClock();
        env.Runtime->FilterFunction = {};
        heldLocal.reset();

        UNIT_ASSERT_C(res, "remote response not received before the deadline");
        UNIT_ASSERT_C(replied <= deadline, "replied " << replied - deadline << " after deadline");
        UNIT_ASSERT(remoteAnswered);
        NBridgeTest::CheckGetResult(*res->Get(), data);
    }

    Y_UNIT_TEST(LocalPileFirstRestartsWhenDeferredPileDisconnects) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings()};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data = "blob data";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        PutToPileGroup(env, pileGroups[0], 1, id, data);

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        size_t getRequests = 0;
        std::unique_ptr<IEventHandle> heldLocal;
        std::unique_ptr<IEventHandle> result;
        env.Runtime->FilterFunction = [&](ui32 /*nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetRecipientRewrite() == edge) {
                result = std::move(ev);
                return false;
            }
            if (ev->GetTypeRewrite() == TEvBlobStorage::EvGet && ev->HasEvent()) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGet>();
                if (ev->Sender == bridgeProxy && msg->QuerySize == 1 && msg->Queries[0].Id == id) {
                    ++getRequests;
                }
            } else if (ev->GetTypeRewrite() == TEvBlobStorage::EvGetResult && ev->HasEvent() && !heldLocal) {
                const auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg->GroupId == pileGroups[0] && ev->GetRecipientRewrite() == bridgeProxy) {
                    heldLocal = std::move(ev);
                    return false;
                }
            }
            return true;
        };

        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::FastRead));
        });
        NBridgeTest::WaitUntil(env, [&] { return heldLocal && getRequests == 1; }, "stalled local read",
            TDuration::MilliSeconds(100), 300);
        NBridgeTest::SwitchBridgePileState(env, 1, NKikimrBridge::TClusterState::DISCONNECTED);
        NBridgeTest::WaitUntil(env, [&] { return getRequests == 2 && !!result; },
            "read restarted without the deferred pile", TDuration::MilliSeconds(100), 300);
        env.Runtime->FilterFunction = {};
        heldLocal.reset();

        UNIT_ASSERT_VALUES_EQUAL(getRequests, 2);
        NBridgeTest::CheckGetResult(*result->Get<TEvBlobStorage::TEvGetResult>(), data);
    }

    // the restoration is done on behalf of the read, so it is bound by the deadline of the read: the pile the blob is
    // restored into doesn't complete writes, and the read must end by its deadline anyway
    Y_UNIT_TEST(RestoreRespectsReadDeadline) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 2,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);

        const TString data = "blob to be restored";
        const TLogoBlobID id(100500, 1, 1, 0, data.size(), 0);
        const auto put = NBridgeTest::QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroups[0], 1,
            new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
        UNIT_ASSERT_VALUES_EQUAL_C(put->Status, NKikimrProto::OK, put->ToString());

        // VDisks of pile 1 run on nodes 9-16
        env.Runtime->FilterFunction = [](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return nodeId <= 8 || (ev->GetTypeRewrite() != TEvBlobStorage::EvVPut &&
                ev->GetTypeRewrite() != TEvBlobStorage::EvVMultiPut);
        };
        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TInstant deadline = env.Runtime->GetClock() + TDuration::Seconds(5);
        env.Runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, groupId, new TEvBlobStorage::TEvGet(id, 0, 0, deadline, NKikimrBlobStorage::FastRead,
                /*mustRestoreFirst=*/ true));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        const TInstant replied = env.Runtime->GetClock();
        env.Runtime->FilterFunction = {};

        UNIT_ASSERT_C(replied <= deadline + TDuration::Seconds(1), "replied " << replied - deadline << " after deadline");
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Get()->Responses[0].Status, NKikimrProto::ERROR, res->Get()->ToString());
    }

    Y_UNIT_TEST(PartRestorationAcrossBridge) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        auto& runtime = env.Runtime;
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);

        ui32 step = 1;
        std::vector<ui32> groupIds{
            groupId + 1,
            groupId + 2,
            groupId + 3,
        };
        auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        for (size_t originalGroupIndex = 0; originalGroupIndex < groupIds.size(); ++originalGroupIndex) {
            for (bool indexOnly : {true, false}) {
                for (bool mustRestoreFirst : {true}) {
                    Cerr << "originalGroupIndex# " << originalGroupIndex
                        << " indexOnly# " << indexOnly
                        << " mustRestoreFirst# " << mustRestoreFirst
                        << Endl;

                    TString data = "hello";
                    TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
                    runtime->WrapInActorContext(sender, [&] {
                        SendToBSProxy(sender, groupIds[originalGroupIndex], FillInGroupGeneration(
                            new TEvBlobStorage::TEvPut(id, data, TInstant::Max())));
                    });
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
                    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

                    for (size_t i = 0; i < groupIds.size(); ++i) {
                        Cerr << "*** reading from i# " << i << Endl;
                        runtime->WrapInActorContext(sender, [&] {
                            SendToBSProxy(sender, groupIds[i], FillInGroupGeneration(new TEvBlobStorage::TEvGet(id, 0, 0,
                                TInstant::Max(), NKikimrBlobStorage::FastRead)));
                        });
                        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
                        auto& m = *res->Get();
                        UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
                        UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
                        UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Status, i == originalGroupIndex ? NKikimrProto::OK : NKikimrProto::NODATA);
                    }

                    Cerr << "*** performing bridge get" << Endl;
                    runtime->WrapInActorContext(sender, [&] {
                        SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::FastRead, mustRestoreFirst, indexOnly));
                    });
                    auto res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
                    auto& m = *res1->Get();
                    UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
                    UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Status, NKikimrProto::OK);
                    if (!indexOnly) {
                        UNIT_ASSERT_EQUAL(m.Responses[0].Buffer, data);
                    }

                    for (size_t i = 0; i < groupIds.size(); ++i) {
                        Cerr << "*** reading from i# " << i << Endl;
                        runtime->WrapInActorContext(sender, [&] {
                            SendToBSProxy(sender, groupIds[i], FillInGroupGeneration(new TEvBlobStorage::TEvGet(id, 0, 0,
                                TInstant::Max(), NKikimrBlobStorage::FastRead)));
                        });
                        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
                        auto& m = *res->Get();
                        UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
                        UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
                        UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Status, NKikimrProto::OK);
                        UNIT_ASSERT_EQUAL(m.Responses[0].Buffer, data);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(PartRestorationAcrossBridgeOnDiscover) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        auto& runtime = env.Runtime;
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);

        std::vector<ui32> groupIds{
            groupId + 1,
            groupId + 2,
            groupId + 3,
        };
        auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        ui64 tabletId = 100500;
        for (bool readBody : {true, false})
        for (ui32 mask1 = 0; mask1 < 8; ++mask1)
        for (ui32 mask2 = 0; mask2 < 8; ++mask2)
        for (ui32 mask3 = 0; mask3 < 8; ++mask3) {
            Cerr << "readBody# " << readBody
                << " mask1# " << mask1
                << " mask2# " << mask2
                << " mask3# " << mask3
                << Endl;

            TLogoBlobID maxId;
            TString data = "hello";
            ui32 mask[] = {mask1, mask2, mask3};
            std::vector<TLogoBlobID> blobs;
            for (size_t i = 0; i < groupIds.size(); ++i) {
                for (int k = 0; k < 3; ++k) {
                    if (mask[i] & 1 << k) {
                        TLogoBlobID id(tabletId, k + 1, 1, 0, data.size(), 0);
                        runtime->WrapInActorContext(sender, [&] {
                            SendToBSProxy(sender, groupIds[i], FillInGroupGeneration(
                                new TEvBlobStorage::TEvPut(id, data, TInstant::Max())));
                        });
                        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
                        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                        maxId = Max(maxId, id);
                    }
                }
            }

            Cerr << "*** performing bridge discover maxId#" << maxId << Endl;
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvDiscover(tabletId, 0, readBody, true,
                    TInstant::Max(), 0, true));
            });
            auto res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(sender, false);
            auto& m = *res1->Get();
            UNIT_ASSERT_VALUES_EQUAL(m.Status, maxId ? NKikimrProto::OK : NKikimrProto::NODATA);
            UNIT_ASSERT(m.Id == maxId);
            if (readBody && maxId) {
                UNIT_ASSERT_EQUAL(m.Buffer, data);
            }

            for (size_t i = 0; maxId && i < groupIds.size(); ++i) {
                Cerr << "*** reading from i# " << i << Endl;
                runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, groupIds[i], FillInGroupGeneration(new TEvBlobStorage::TEvGet(m.Id, 0, 0,
                        TInstant::Max(), NKikimrBlobStorage::FastRead)));
                });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
                auto& m = *res->Get();
                UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
                UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Status, NKikimrProto::OK);
                UNIT_ASSERT_EQUAL(m.Responses[0].Buffer, data);
            }

            ++tabletId;
        }
    }

    Y_UNIT_TEST(PartRestorationAcrossBridgeOnRange) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 3,
            .AutomaticBootstrap = true,
        }};
        auto& runtime = env.Runtime;
        env.CreatePool();
        const ui32 groupId = env.GetGroups().front();
        auto info = env.GetGroupInfo(groupId);

        std::vector<ui32> groupIds{
            groupId + 1,
            groupId + 2,
            groupId + 3,
        };
        auto sender = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        ui64 tabletId = 100500;
        ui32 gen = 1;
        for (bool indexOnly : {true, false})
        for (bool reverse : {true, false})
        for (ui32 mask1 = 0; mask1 < 8; ++mask1)
        for (ui32 mask2 = 0; mask2 < 8; ++mask2)
        for (ui32 mask3 = 0; mask3 < 8; ++mask3) {
            Cerr << "indexOnly# " << indexOnly
                << " reverse# " << reverse
                << " mask1# " << mask1
                << " mask2# " << mask2
                << " mask3# " << mask3
                << Endl;

            TString data = "hello";
            ui32 mask[] = {mask1, mask2, mask3};
            std::vector<TLogoBlobID> blobs;
            for (size_t i = 0; i < groupIds.size(); ++i) {
                for (int k = 0; k < 3; ++k) {
                    if (mask[i] & 1 << k) {
                        TLogoBlobID id(tabletId, gen, k + 1, 0, data.size(), 0);
                        runtime->WrapInActorContext(sender, [&] {
                            SendToBSProxy(sender, groupIds[i], FillInGroupGeneration(new TEvBlobStorage::TEvPut(
                                id, data, TInstant::Max())));
                        });
                        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
                        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                        blobs.push_back(id);
                    }
                }
            }

            std::ranges::sort(blobs);
            const auto [b, e] = std::ranges::unique(blobs);
            blobs.erase(b, e);
            if (reverse) {
                std::ranges::reverse(blobs);
            }

            Cerr << "*** performing bridge range" << Endl;
            const TLogoBlobID min = TLogoBlobID(tabletId, gen, 0, 0, 0, 0);
            const TLogoBlobID max = TLogoBlobID(tabletId, gen, Max<ui32>(), TLogoBlobID::MaxChannel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie);
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvRange(tabletId, reverse ? max : min,
                    reverse ? min : max, true, TInstant::Max(), indexOnly));
            });
            auto res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(sender, false);
            auto& m = *res1->Get();
            UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(m.Responses.size(), blobs.size());
            for (size_t i = 0; i < blobs.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(m.Responses[i].Id, blobs[i]);
                if (!indexOnly) {
                    UNIT_ASSERT_VALUES_EQUAL(m.Responses[i].Buffer, data);
                }
            }

            for (size_t i = 0; i < groupIds.size(); ++i) {
                Cerr << "*** reading from i# " << i << Endl;
                for (size_t j = 0; j < blobs.size(); ++j) {
                    runtime->WrapInActorContext(sender, [&] {
                        SendToBSProxy(sender, groupIds[i], FillInGroupGeneration(new TEvBlobStorage::TEvGet(blobs[j], 0, 0,
                            TInstant::Max(), NKikimrBlobStorage::FastRead)));
                    });
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(sender, false);
                    auto& m = *res->Get();
                    UNIT_ASSERT_VALUES_EQUAL(m.Status, NKikimrProto::OK);
                    UNIT_ASSERT_VALUES_EQUAL(m.ResponseSz, 1);
                    UNIT_ASSERT_VALUES_EQUAL(m.Responses[0].Status, NKikimrProto::OK);
                    UNIT_ASSERT_EQUAL(m.Responses[0].Buffer, data);
                }
            }

            ++gen;
        }
    }


    Y_UNIT_TEST(GetAbsenceNeedsAllPiles) {
        CheckAbsenceNeedsAllPiles(EAbsenceRead::Get);
    }

    Y_UNIT_TEST(RangeAbsenceNeedsAllPiles) {
        CheckAbsenceNeedsAllPiles(EAbsenceRead::Range);
    }


    enum class EPileFailure { WholeRequest, EveryBlob };

    // both piles are synchronized, and pile 0 fails the read, answering first: the blobs pile 1 has are read anyway, and
    // the read lacking something in pile 1 fails the way pile 0 did
    void CheckFailedPileIsMadeUpFor(EPileFailure failure, bool localPileFirst = false) {
        TEnvironmentSetup env{MakeLocalPileFirstSettings(localPileFirst)};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);
        const TActorId bridgeProxy = GetBridgeProxyActor(env, groupId);

        const TString data = "blob data";
        const TLogoBlobID inBoth(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID inPile0(100500, 1, 2, 0, data.size(), 0);
        auto put = [&](ui32 pile, const TLogoBlobID& id) {
            PutToPileGroup(env, pileGroups[pile], 1 + 8 * pile, id, data);
        };
        put(0, inBoth);
        put(1, inBoth);
        put(0, inPile0);

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);

        // fail the answer of pile 0 and hold the answer of pile 1 until the one of pile 0 is delivered
        auto read = [&](std::initializer_list<TLogoBlobID> ids) {
            std::unique_ptr<IEventHandle> held;
            ui32 heldNodeId = 0;
            bool pile0Answered = false;
            bool released = false;
            std::unique_ptr<IEventHandle> result;
            env.Runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
                if (ev->GetRecipientRewrite() == edge) {
                    result = std::move(ev);
                    return false;
                }
                if (ev->GetTypeRewrite() != TEvBlobStorage::EvGetResult || !ev->HasEvent()) {
                    return true;
                }
                if (ev->GetRecipientRewrite() != bridgeProxy) {
                    return true;
                }
                auto *msg = ev->Get<TEvBlobStorage::TEvGetResult>();
                if (msg->GroupId == pileGroups[0]) {
                    UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::OK, msg->ToString());
                    if (failure == EPileFailure::WholeRequest) {
                        msg->Status = NKikimrProto::ERROR;
                        msg->ErrorReason = "injected failure";
                    }
                    for (ui32 i = 0; i < msg->ResponseSz; ++i) {
                        msg->Responses[i].Status = NKikimrProto::ERROR;
                        msg->Responses[i].Buffer = {};
                    }
                    pile0Answered = true;
                } else if (msg->GroupId == pileGroups[1] && !released) {
                    held = std::move(ev);
                    heldNodeId = nodeId;
                    return false;
                }
                return true;
            };
            SendGet(env, edge, groupId, std::span<const TLogoBlobID>(ids.begin(), ids.size()));
            NBridgeTest::WaitUntil(env, [&] { return held && pile0Answered; }, "answers of both piles",
                TDuration::MilliSeconds(100), 300);
            env.Sim(TDuration::MilliSeconds(100)); // let the answer of pile 0 be handled
            UNIT_ASSERT_C(!result, "the read is answered without pile 1");
            released = true;
            env.Runtime->Send(held.release(), heldNodeId);
            NBridgeTest::WaitUntil(env, [&] { return !!result; }, "result", TDuration::MilliSeconds(100), 300);
            env.Runtime->FilterFunction = {};
            return result;
        };

        {
            auto result = read({inBoth});
            auto *msg = result->Get<TEvBlobStorage::TEvGetResult>();
            UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::OK, msg->ToString());
            UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 1);
            UNIT_ASSERT_VALUES_EQUAL_C(msg->Responses[0].Status, NKikimrProto::OK, msg->ToString());
            UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Buffer.ConvertToString(), data);
        }

        {
            // pile 1 lacks the second blob, which is not the proof of its absence
            auto result = read({inBoth, inPile0});
            auto *msg = result->Get<TEvBlobStorage::TEvGetResult>();
            if (failure == EPileFailure::WholeRequest) {
                UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::ERROR, msg->ToString());
                UNIT_ASSERT_VALUES_EQUAL(msg->ErrorReason, "injected failure");
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(msg->Status, NKikimrProto::OK, msg->ToString());
                UNIT_ASSERT_VALUES_EQUAL(msg->ResponseSz, 2);
                UNIT_ASSERT_VALUES_EQUAL_C(msg->Responses[0].Status, NKikimrProto::OK, msg->ToString());
                UNIT_ASSERT_VALUES_EQUAL(msg->Responses[0].Buffer.ConvertToString(), data);
                UNIT_ASSERT_VALUES_EQUAL_C(msg->Responses[1].Status, NKikimrProto::ERROR, msg->ToString());
            }
        }
    }

    Y_UNIT_TEST(FailedPileIsMadeUpFor) {
        CheckFailedPileIsMadeUpFor(EPileFailure::WholeRequest);
    }

    Y_UNIT_TEST(FailedBlobsOfPileAreMadeUpFor) {
        CheckFailedPileIsMadeUpFor(EPileFailure::EveryBlob);
    }

    Y_UNIT_TEST(LocalPileFirstFailedPileIsMadeUpFor) {
        CheckFailedPileIsMadeUpFor(EPileFailure::WholeRequest, true);
    }

    Y_UNIT_TEST(LocalPileFirstFailedBlobsOfPileAreMadeUpFor) {
        CheckFailedPileIsMadeUpFor(EPileFailure::EveryBlob, true);
    }


    // the piles have different data of the same blob, as after a corruption; the reads comparing the answers of piles
    // fail rather than return either of them or stop the node
    Y_UNIT_TEST(DivergentPilesFailReads) {
        TEnvironmentSetup env{{
            .NodeCount = 16,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetBridgedLocation,
            .SelfManagementConfig = true,
            .NumPiles = 2,
            .AutomaticBootstrap = true,
        }};
        const ui32 groupId = NBridgeTest::CreatePoolAndGetGroup(env);
        const auto pileGroups = NBridgeTest::GetPileGroups(env, groupId);

        const TLogoBlobID id(100500, 1, 1, 0, 4, 0);
        for (ui32 pile : {0, 1}) {
            const auto res = NBridgeTest::QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroups[pile], 1 + 8 * pile,
                new TEvBlobStorage::TEvPut(id, TString(4, 'a' + pile), TInstant::Max()));
            UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
        }

        const TActorId edge = env.Runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        auto send = [&](IEventBase *ev) {
            env.Runtime->WrapInActorContext(edge, [&] {
                SendToBSProxy(edge, groupId, ev);
            });
        };

        send(new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(), NKikimrBlobStorage::FastRead, /*mustRestoreFirst=*/ true));
        auto get = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(get->Get()->ResponseSz, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(get->Get()->Responses[0].Status, NKikimrProto::ERROR, get->Get()->ToString());

        send(new TEvBlobStorage::TEvRange(id.TabletID(), id, id, /*mustRestoreFirst=*/ false, TInstant::Max()));
        auto range = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvRangeResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(range->Get()->Status, NKikimrProto::ERROR, range->Get()->ToString());

        send(new TEvBlobStorage::TEvDiscover(id.TabletID(), 0, /*readBody=*/ true, /*discoverBlockedGeneration=*/ false,
            TInstant::Max(), 0, false));
        auto discover = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvDiscoverResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL_C(discover->Get()->Status, NKikimrProto::ERROR, discover->Get()->ToString());
    }

}
