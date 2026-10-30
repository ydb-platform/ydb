#include "bridge_common.h"

#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

#include <util/system/type_name.h>

using NBridgeTest::GetBridgedLocation;

namespace {

    template<typename T>
    T *FillInGroupGeneration(T *ev) {
        ev->ForceGroupGeneration = 1;
        return ev;
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

        const TString data = "blob data";
        const TLogoBlobID inBoth(100500, 1, 1, 0, data.size(), 0);
        const TLogoBlobID inPile0(100500, 1, 2, 0, data.size(), 0);
        auto put = [&](ui32 pile, const TLogoBlobID& id) {
            const auto res = NBridgeTest::QueryPileGroup<TEvBlobStorage::TEvPutResult>(env, pileGroups[pile],
                1 + 8 * pile, new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
            UNIT_ASSERT_VALUES_EQUAL_C(res->Status, NKikimrProto::OK, res->ToString());
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
            if (IActor *actor = env.Runtime->GetActor(ev->GetRecipientRewrite()); !actor ||
                    !TypeName(*actor).Contains("TBridgedBlobStorageProxyActor")) {
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

}

Y_UNIT_TEST_SUITE(BridgeGet) {

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
