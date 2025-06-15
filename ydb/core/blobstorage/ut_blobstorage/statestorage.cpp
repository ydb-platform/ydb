#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/util/json_util.h>
#include <ydb/core/base/statestorage_impl.h>

Y_UNIT_TEST_SUITE(TStateStorageRingGroupState) {
    class StateStorageTest {
        public:
        TEnvironmentSetup Env;
        TTestActorSystem& Runtime;
        ui64 TabletId;

        StateStorageTest()
            : Env{{
                    .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
                    .SelfManagementConfig = true,
                }}
            , Runtime(*Env.Runtime)
        {
            Runtime.SetLogPriority(NKikimrServices::STATESTORAGE, NLog::PRI_DEBUG);
            Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
            Env.Sim(TDuration::Seconds(10));
            TabletId = Env.TabletId;
        }

        TAutoPtr<TEventHandle<NKikimr::NStorage::TEvNodeConfigInvokeOnRootResult>> SendRequest(const TString &cfg) {
            ui32 retry = 0;
            while (retry++ < 5) {
                Env.Sim(TDuration::Seconds(10));
                auto ev = std::make_unique<NKikimr::NStorage::TEvNodeConfigInvokeOnRoot>();
                const auto status = google::protobuf::util::JsonStringToMessage(cfg, &ev->Record);
                UNIT_ASSERT(status.ok());
                TActorId edge = Runtime.AllocateEdgeActor(1);
                const TActorId wardenId = MakeBlobStorageNodeWardenID(1);
                Runtime.WrapInActorContext(edge, [&] {
                    Runtime.Send(new IEventHandle(wardenId, edge, ev.release(), IEventHandle::FlagTrackDelivery));
                });
                auto res = Env.WaitForEdgeActorEvent<NKikimr::NStorage::TEvNodeConfigInvokeOnRootResult>(edge);
                if (res->Get()->Record.GetStatus() == NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK) {
                    return res;
                } else {
                    Cerr << "BadResponse: " << res->Get()->Record << Endl;
                }
            }
            UNIT_ASSERT(false);
            return nullptr;
        }

        auto ResolveReplicas() {
            const TActorId proxy = MakeStateStorageProxyID();
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(proxy, edge, new TEvStateStorage::TEvResolveReplicas(TabletId), IEventHandle::FlagTrackDelivery));
            });
            auto ev = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
            return ev;
        }

        void ChangeReplicaConfig(TActorId replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(replica.NodeId());
            TIntrusivePtr<TStateStorageInfo> info(new TStateStorageInfo());
            info->ClusterStateGeneration = gen;
            info->ClusterStateGuid = guid;
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvUpdateGroupConfig(info, nullptr, nullptr), IEventHandle::FlagTrackDelivery));
            });
            Env.Sim(TDuration::Seconds(10));
        }

        auto Lookup() {
            const TActorId proxy = MakeStateStorageProxyID();
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(proxy, edge, new TEvStateStorage::TEvLookup(TabletId, 0), IEventHandle::FlagTrackDelivery));
            });
            auto ev = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge);
            return ev;
        }

        auto ReplicaLookup(const TActorId &replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaLookup(TabletId, 0, gen, guid), IEventHandle::FlagTrackDelivery));
            });
            auto ev = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvReplicaInfo>(edge);
            return ev;
        }

    };

    Y_UNIT_TEST(TestProxyNotifyReplicaConfigChanged1) {
        StateStorageTest test;

        auto res = test.ResolveReplicas();
        const auto &replicas = res->Get()->GetPlainReplicas();
        UNIT_ASSERT(test.Lookup()->Get()->Status == NKikimrProto::EReplyStatus::OK);

        for (auto [gen, guid, res] : std::initializer_list<std::tuple<ui64, ui64, NKikimrProto::EReplyStatus>> {
            {0, 0, NKikimrProto::EReplyStatus::OK}
            , {0, 2, NKikimrProto::EReplyStatus::ERROR}
            , {0, 0, NKikimrProto::EReplyStatus::OK}
            , {1, 0, NKikimrProto::EReplyStatus::ERROR}
        }) {
            test.ChangeReplicaConfig(replicas[1], gen, guid);
            UNIT_ASSERT_EQUAL(test.Lookup()->Get()->Status, res); 
        }
    }

    Y_UNIT_TEST(TestProxyConfigMismatchNotSent) {
        StateStorageTest test;
        ui32 nw1Cnt = 0;
        test.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifyConfigMismatch::EventType) {
                nw1Cnt++;
            }
            return true;
        };
        UNIT_ASSERT_EQUAL(test.Lookup()->Get()->Status, NKikimrProto::EReplyStatus::OK);
        UNIT_ASSERT_EQUAL(nw1Cnt, 0);  
    }

    Y_UNIT_TEST(TestProxyConfigMismatch) {
        StateStorageTest test;

        auto res = test.ResolveReplicas();
        const auto &replicas = res->Get()->GetPlainReplicas();
        UNIT_ASSERT_EQUAL(test.Lookup()->Get()->Status, NKikimrProto::EReplyStatus::OK);
        ui32 nw1Cnt = 0;
        ui32 nw2Cnt = 0;
        test.ChangeReplicaConfig(replicas[1], 1, 2);
        test.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifyConfigMismatch::EventType) {
                auto* node = test.Runtime.GetNode(1);
                UNIT_ASSERT(node && node->ActorSystem);
                TActorId replicaId = node->ActorSystem->LookupLocalService(replicas[1]);
                auto msg = ev->Get<NStorage::TEvNodeWardenNotifyConfigMismatch>();
                if (ev->Sender == replicaId) {
                    UNIT_ASSERT_EQUAL(msg->ClusterStateGeneration, 3);
                    UNIT_ASSERT_EQUAL(msg->ClusterStateGuid, 4);
                    nw1Cnt++; // replica notify nodewarden
                } else {
                    UNIT_ASSERT_EQUAL(msg->ClusterStateGeneration, 1);
                    UNIT_ASSERT_EQUAL(msg->ClusterStateGuid, 2);
                    nw2Cnt++; // proxy notify nodewarden
                }
            }
            return true;
        };
        UNIT_ASSERT_EQUAL(test.Lookup()->Get()->Status, NKikimrProto::EReplyStatus::ERROR);
        UNIT_ASSERT_EQUAL(nw1Cnt, 0);  
        UNIT_ASSERT_EQUAL(nw2Cnt, 1);
        UNIT_ASSERT_EQUAL(test.ReplicaLookup(replicas[1], 3, 4)->Get()->Record.GetStatus(), NKikimrProto::EReplyStatus::OK);
        UNIT_ASSERT_EQUAL(nw1Cnt, 1);  
        UNIT_ASSERT_EQUAL(nw2Cnt, 1);
    }
}
