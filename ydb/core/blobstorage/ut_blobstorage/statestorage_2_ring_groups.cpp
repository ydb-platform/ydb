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

        void CheckLookupReplyAndSignatures() {
            const TActorId proxy = MakeStateStorageProxyID();
            const auto edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(proxy, edge, new TEvStateStorage::TEvLookup(TabletId, 0,
                    TEvStateStorage::TProxyOptions::SigAsync), IEventHandle::FlagTrackDelivery));
            });

            auto reply = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge, false);
            auto signatures = reply->Get()->Signature;
            UNIT_ASSERT(signatures.Size() <= 10);
            while (signatures.Size() < 10) {
                auto update = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge, false);
                const auto& next = update->Get()->Signature;
                signatures.Merge(next);
                UNIT_ASSERT_VALUES_EQUAL(signatures.Size(), next.Size());
                UNIT_ASSERT(signatures.Size() <= 10);
            }
            Runtime.DestroyActor(edge);
        }
    };

    Y_UNIT_TEST(TestStateStorageReplyOnce) {
        StateStorage2RingGroupsTest test(StateStorage2RingGroupsTest::StateStorageInfoGenerator);
        test.CheckLookupReplyAndSignatures();
    }

    Y_UNIT_TEST(TestStateStorageReplyOnceWriteOnly) {
        StateStorage2RingGroupsTest test([](auto generateId, auto stateStorageNodeId) {
            auto ssInfo = StateStorage2RingGroupsTest::StateStorageInfoGenerator(generateId, stateStorageNodeId);
            ssInfo->RingGroups[1].WriteOnly = true;
            return ssInfo;
        });
        test.CheckLookupReplyAndSignatures();
    }

    Y_UNIT_TEST(TestStateStorageReplyOnceWriteOnlyReverseEventsOrder) {
        StateStorage2RingGroupsTest test([](auto generateId, auto stateStorageNodeId) {
            auto ssInfo = StateStorage2RingGroupsTest::StateStorageInfoGenerator(generateId, stateStorageNodeId);
            ssInfo->RingGroups[0].WriteOnly = true;
            return ssInfo;
        });
        test.CheckLookupReplyAndSignatures();
    }
}
