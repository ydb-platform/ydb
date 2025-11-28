#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>

namespace {

    TNodeLocation GetLocation(ui32 nodeId) {
        NActorsInterconnect::TNodeLocation location;
        if (1 <= nodeId && nodeId <= 8) {
            location.SetBridgePileName("pile_1");
        } else if (9 <= nodeId && nodeId <= 16) {
            location.SetBridgePileName("pile_2");
        } else if (17 <= nodeId && nodeId <= 24) {
            location.SetBridgePileName("pile_3");
        } else {
            Y_ABORT();
        }
        location.SetDataCenter("my_dc");
        location.SetRack(TStringBuilder() << "rack_" << nodeId);
        location.SetUnit(TStringBuilder() << "unit_" << nodeId);
        return TNodeLocation(location);
    }

    template<typename T>
    T *FillInGroupGeneration(T *ev) {
        ev->ForceGroupGeneration = 1;
        return ev;
    }

}

Y_UNIT_TEST_SUITE(BridgeGet) {

    Y_UNIT_TEST(PartRestorationAcrossBridge) {
        TEnvironmentSetup env{{
            .NodeCount = 8 * 3,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
            .LocationGenerator = GetLocation,
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
            .LocationGenerator = GetLocation,
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
            .LocationGenerator = GetLocation,
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

}
