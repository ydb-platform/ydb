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
                for (bool mustRestoreFirst : {true, false}) {
                    Cerr << "originalGroupIndex# " << originalGroupIndex
                        << " indexOnly# " << indexOnly
                        << " mustRestoreFirst# " << mustRestoreFirst
                        << Endl;

                    TString data = "hello";
                    TLogoBlobID id(100500, 1, step++, 0, data.size(), 0);
                    runtime->WrapInActorContext(sender, [&] {
                        SendToBSProxy(sender, groupIds[originalGroupIndex], new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
                    });
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
                    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

                    for (size_t i = 0; i < groupIds.size(); ++i) {
                        Cerr << "*** reading from i# " << i << Endl;
                        runtime->WrapInActorContext(sender, [&] {
                            SendToBSProxy(sender, groupIds[i], new TEvBlobStorage::TEvGet(id, 0, 0,
                                TInstant::Max(), NKikimrBlobStorage::FastRead));
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
                            SendToBSProxy(sender, groupIds[i], new TEvBlobStorage::TEvGet(id, 0, 0,
                                TInstant::Max(), NKikimrBlobStorage::FastRead));
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
        ui32 gen = 1;
        TLogoBlobID maxId;
        for (bool readBody : {true, false})
        for (ui32 pile1step = 0; pile1step <= groupIds.size(); ++pile1step)
        for (ui32 pile2step = 0; pile2step <= groupIds.size(); ++pile2step)
        for (ui32 pile3step = 0; pile3step <= groupIds.size(); ++pile3step) {
            Cerr << "pile1step# " << pile1step << " pile2step# " << pile2step << " pile3step# " << pile3step << Endl;

            TString data = "hello";
            ui32 steps[] = {pile1step, pile2step, pile3step};
            for (size_t i = 0; i < groupIds.size(); ++i) {
                if (!steps[i]) {
                    continue;
                }
                TLogoBlobID id(100500, gen, steps[i], 0, data.size(), 0);
                maxId = Max(maxId, id);
                runtime->WrapInActorContext(sender, [&] {
                    SendToBSProxy(sender, groupIds[i], new TEvBlobStorage::TEvPut(id, data, TInstant::Max()));
                });
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(sender, false);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            }

            ++gen;

            Cerr << "*** performing bridge discover" << Endl;
            runtime->WrapInActorContext(sender, [&] {
                SendToBSProxy(sender, info->GroupID, new TEvBlobStorage::TEvDiscover(100500, 0, readBody, true,
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
                    SendToBSProxy(sender, groupIds[i], new TEvBlobStorage::TEvGet(m.Id, 0, 0,
                        TInstant::Max(), NKikimrBlobStorage::FastRead));
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
