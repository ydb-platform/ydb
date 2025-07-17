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
        TString Path;

        StateStorageTest()
            : Env{{
                    .Erasure = TBlobStorageGroupType::ErasureMirror3dc,
                    .SelfManagementConfig = true,
                }}
            , Runtime(*Env.Runtime)
            , Path("somePath")
        {
            Runtime.SetLogPriority(NKikimrServices::STATESTORAGE, NLog::PRI_DEBUG);
            Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_DEBUG);
            Env.Sim(TDuration::Seconds(10));
            TabletId = Env.TabletId;
        }

        auto ResolveBoardReplicas() {
            const TActorId proxy = MakeStateStorageProxyID();
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(proxy, edge, new TEvStateStorage::TEvResolveBoard(Path), IEventHandle::FlagTrackDelivery));
            });
            auto ev = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvResolveReplicasList>(edge);
            return ev;
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

        void ChangeReplicaConfig(TActorId replica, ui64 gen, ui64 guid, bool board = false) {
            const TActorId edge = Runtime.AllocateEdgeActor(replica.NodeId());
            TIntrusivePtr<TStateStorageInfo> info(new TStateStorageInfo());
            info->ClusterStateGeneration = gen;
            info->ClusterStateGuid = guid;
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvUpdateGroupConfig(board ? nullptr : info, board ? info : nullptr, nullptr), IEventHandle::FlagTrackDelivery));
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

        auto BoardLookup(const TActorId &replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaBoardLookup(Path, 0, gen, guid), IEventHandle::FlagTrackDelivery));
            });
            auto ev = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvReplicaBoardInfo>(edge);
            return ev;
        }

        void BoardCleanup(const TActorId &replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaBoardCleanup(gen, guid), IEventHandle::FlagTrackDelivery));
            });
            Env.Sim(TDuration::Seconds(10));
        }


        void ReplicaCleanup(const TActorId &replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaCleanup(TabletId, TActorId(), gen, guid), IEventHandle::FlagTrackDelivery));
            });
            Env.Sim(TDuration::Seconds(10));
        }

        auto ReplicaDelete(const TActorId &replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaDelete(TabletId, gen, guid), IEventHandle::FlagTrackDelivery));
            });
            auto ev = Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvReplicaInfo>(edge);
            return ev;
        }

        void BoardUnsubscribe(const TActorId &replica, ui64 gen, ui64 guid) {
            const TActorId edge = Runtime.AllocateEdgeActor(1);
            Runtime.WrapInActorContext(edge, [&] {
                Runtime.Send(new IEventHandle(replica, edge, new TEvStateStorage::TEvReplicaBoardUnsubscribe(gen, guid), IEventHandle::FlagTrackDelivery));
            });
            Env.Sim(TDuration::Seconds(10));
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
        test.ReplicaDelete(replicas[1], 3, 4);
        UNIT_ASSERT_EQUAL(nw1Cnt, 2);
        UNIT_ASSERT_EQUAL(nw2Cnt, 1);
        test.ReplicaCleanup(replicas[1], 3, 4);
        UNIT_ASSERT_EQUAL(nw1Cnt, 3);
        UNIT_ASSERT_EQUAL(nw2Cnt, 1);
    }

    Y_UNIT_TEST(TestBoardConfigMismatch) {
        StateStorageTest test;
        auto res = test.ResolveBoardReplicas();
        const auto &replicas = res->Get()->GetPlainReplicas();
        ui32 nw1Cnt = 0;
        test.Runtime.FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NStorage::TEvNodeWardenNotifyConfigMismatch::EventType) {
                nw1Cnt++;
            }
            return true;
        };
        test.BoardLookup(replicas[1], 0, 0);
        UNIT_ASSERT_EQUAL(nw1Cnt, 0);
        test.BoardLookup(replicas[1], 1, 0);
        UNIT_ASSERT_EQUAL(nw1Cnt, 1);
        test.BoardLookup(replicas[1], 0, 1);
        UNIT_ASSERT_EQUAL(nw1Cnt, 2);
        ui64 guid = Max<ui64>();
        test.ChangeReplicaConfig(replicas[1], 3, guid, true);
        auto result = test.BoardLookup(replicas[1], 0, 0);
        UNIT_ASSERT_EQUAL(result->Get()->Record.GetClusterStateGeneration(), 3);
        UNIT_ASSERT_EQUAL(result->Get()->Record.GetClusterStateGuid(), guid);
        UNIT_ASSERT_EQUAL(nw1Cnt, 2);
        test.BoardCleanup(replicas[1], 5, 6);
        UNIT_ASSERT_EQUAL(nw1Cnt, 3);
        test.BoardUnsubscribe(replicas[1], 5, 6);
        UNIT_ASSERT_EQUAL(nw1Cnt, 4);
    }

    Y_UNIT_TEST(TestStateStorageUpdateSig) {
        StateStorageTest test;
        auto res = test.ResolveReplicas();
        const auto &replicas = res->Get()->GetPlainReplicas();
        ui32 rejectCnt = 0;
        const TActorId proxy = MakeStateStorageProxyID();
        const auto edge1 = test.Runtime.AllocateEdgeActor(1);
        std::unordered_set<ui32> processed;
        test.Runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaInfo::EventType) {
                ui32 cookie = ev->Get<TEvStateStorage::TEvReplicaInfo>()->Record.GetCookie();
                rejectCnt++;
                if (rejectCnt == 1) {
                    return false;
                }
                if (rejectCnt >= replicas.size()) {
                    return true;
                }
                if (rejectCnt < (replicas.size() / 2 + 2)) {
                    return true;
                }
                if(processed.insert(cookie).second) {
                    test.Runtime.Send(new IEventHandle(ev->Recipient, ev->Sender, new TEvents::TEvUndelivered(ev->GetTypeRewrite(), TEvents::TEvUndelivered::Disconnected), 0, cookie), node);
                    test.Runtime.Send(ev->Forward(ev->Recipient), node);
                }
                return false;
            }
            return true;
        };

        test.Runtime.WrapInActorContext(edge1, [&] {
            test.Runtime.Send(new IEventHandle(proxy, edge1, new TEvStateStorage::TEvLookup(test.TabletId, 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)), IEventHandle::FlagTrackDelivery));
        });
        test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge1, false);
        auto ev = test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvUpdateSignature>(edge1);
        for (auto replica : replicas) {
            UNIT_ASSERT(ev->Get()->Signature.GetReplicaSignature(replica) != Max<ui64>());
        }
        UNIT_ASSERT(rejectCnt > replicas.size());
        test.Runtime.FilterFunction = nullptr;
    }

    Y_UNIT_TEST(TestStateStorageDoubleReply) {
        StateStorageTest test;
        auto res = test.ResolveReplicas();
        const auto &replicas = res->Get()->GetPlainReplicas();
        const TActorId proxy = MakeStateStorageProxyID();
        const auto edge1 = test.Runtime.AllocateEdgeActor(1);
        std::unordered_set<ui32> processed;
        test.Runtime.FilterFunction = [&](ui32 node, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvStateStorage::TEvReplicaInfo::EventType) {
                ui32 cookie = ev->Get<TEvStateStorage::TEvReplicaInfo>()->Record.GetCookie();
                if(processed.insert(cookie).second) {
                    auto *duplicate = new TEvStateStorage::TEvReplicaInfo();
                    duplicate->Record = ev->Get<TEvStateStorage::TEvReplicaInfo>()->Record;
                    test.Runtime.Send(new IEventHandle(ev->Recipient, ev->Sender, duplicate, 0, cookie), node);
                    test.Runtime.Send(ev->Forward(ev->Recipient), node);
                    return false;
                }
                return true;
            }
            return true;
        };

        test.Runtime.WrapInActorContext(edge1, [&] {
            test.Runtime.Send(new IEventHandle(proxy, edge1, new TEvStateStorage::TEvLookup(test.TabletId, 0, TEvStateStorage::TProxyOptions(TEvStateStorage::TProxyOptions::SigAsync)), IEventHandle::FlagTrackDelivery));
        });
        auto ev = test.Runtime.WaitForEdgeActorEvent<TEvStateStorage::TEvInfo>(edge1);
        for (auto replica : replicas) {
            UNIT_ASSERT(ev->Get()->Signature.GetReplicaSignature(replica) != Max<ui64>());
        }
        test.Runtime.FilterFunction = nullptr;
    }
}
