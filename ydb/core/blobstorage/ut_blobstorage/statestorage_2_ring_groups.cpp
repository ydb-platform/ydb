#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/util/json_util.h>
#include <ydb/core/base/statestorage_impl.h>

Y_UNIT_TEST_SUITE(TStateStorage2RingGroups) {
    class StateStorage2RingGroupsTest {
        public:
        TEnvironmentSetup Env;
        TTestActorSystem& Runtime;
        ui64 TabletId;
        TString Path;

        static TIntrusivePtr<TStateStorageInfo> StateStorageInfoGenerator(std::function<TActorId(ui32, ui32)> generateId, ui32 stateStorageNodeId) {
            ui32 numRingGroups = 2;
            ui32 numReplicas = 5;
            ui32 id = 0;
            auto info = MakeIntrusive<TStateStorageInfo>();
            info->RingGroups.resize(numRingGroups);
            for (ui32 rg : xrange(numRingGroups)) {
                auto& ringGroup = info->RingGroups[rg];
                ringGroup.NToSelect = numReplicas;
                ringGroup.Rings.resize(numReplicas);
                for (ui32 i = 0; i < numReplicas; ++i) {
                    ringGroup.Rings[i].Replicas.push_back(generateId(stateStorageNodeId, id++));
                }
            }
            return info;
        }

        StateStorage2RingGroupsTest(auto generator)
            : Env{{
                    .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
                    .SelfManagementConfig = true,
                    .StateStorageInfoGenerator = generator,
                }}
            , Runtime(*Env.Runtime)
            , Path("somePath")
        {
            Runtime.SetLogPriority(NKikimrServices::STATESTORAGE, NLog::PRI_DEBUG);
            Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
            Env.Sim(TDuration::Seconds(10));
            TabletId = Env.TabletId;
        }
    };

    Y_UNIT_TEST(TestStateStorageReplyOnce) {
        StateStorage2RingGroupsTest test(StateStorage2RingGroupsTest::StateStorageInfoGenerator);
        const TActorId proxy = MakeStateStorageProxyID();
        const auto edge1 = test.Runtime.AllocateEdgeActor(1);
        test.Runtime.WrapInActorContext(edge1, [&] {
            test.Runtime.Send(new IEventHandle(proxy, edge1, new TEvStateStorage::TEvLookup(test.TabletId, 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)), IEventHandle::FlagTrackDelivery));
        });
        test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge1, false);
        auto ev2 = test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge1);
        UNIT_ASSERT_EQUAL(ev2->Get()->Signature.Size(), 10);
    }

    Y_UNIT_TEST(TestStateStorageReplyOnceWriteOnly) {
        StateStorage2RingGroupsTest test([](auto generateId, auto stateStorageNodeId) {
            auto ssInfo = StateStorage2RingGroupsTest::StateStorageInfoGenerator(generateId, stateStorageNodeId);
            ssInfo->RingGroups[1].WriteOnly = true;
            return ssInfo;
        });
        const TActorId proxy = MakeStateStorageProxyID();
        const auto edge1 = test.Runtime.AllocateEdgeActor(1);
        test.Runtime.WrapInActorContext(edge1, [&] {
            test.Runtime.Send(new IEventHandle(proxy, edge1, new TEvStateStorage::TEvLookup(test.TabletId, 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)), IEventHandle::FlagTrackDelivery));
        });
        test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge1, false);
        test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge1, false);
        test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge1, false);
        auto ev2 = test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge1);
        UNIT_ASSERT_EQUAL(ev2->Get()->Signature.Size(), 10);
    }

    Y_UNIT_TEST(TestStateStorageReplyOnceWriteOnlyReverseEventsOrder) {
        StateStorage2RingGroupsTest test([](auto generateId, auto stateStorageNodeId) {
            auto ssInfo = StateStorage2RingGroupsTest::StateStorageInfoGenerator(generateId, stateStorageNodeId);
            ssInfo->RingGroups[0].WriteOnly = true;
            return ssInfo;
        });
        const TActorId proxy = MakeStateStorageProxyID();
        const auto edge1 = test.Runtime.AllocateEdgeActor(1);
        test.Runtime.WrapInActorContext(edge1, [&] {
            test.Runtime.Send(new IEventHandle(proxy, edge1, new TEvStateStorage::TEvLookup(test.TabletId, 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)), IEventHandle::FlagTrackDelivery));
        });
        test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge1, false);
        auto ev2 = test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge1);
        UNIT_ASSERT_EQUAL(ev2->Get()->Signature.Size(), 10);
    }
}
