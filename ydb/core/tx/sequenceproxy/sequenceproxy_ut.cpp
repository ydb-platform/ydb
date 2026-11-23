#include "sequenceproxy.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceshard/public/events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSequenceProxy {

using namespace NSequenceShard;

Y_UNIT_TEST_SUITE(SequenceProxy) {

    namespace {

        TTenantTestConfig::TTenantPoolConfig MakeDefaultTenantPoolConfig() {
            TTenantTestConfig::TTenantPoolConfig res = {
                // Static slots {tenant, {cpu, memory, network}}
                {
                    { {DOMAIN1_NAME, {1, 1, 1}} },
                },
                // NodeType
                "storage"
            };
            return res;
        }

        TTenantTestConfig MakeTenantTestConfig(bool fakeSchemeShard) {
            TTenantTestConfig res = {
                // Domains {name, schemeshard {{ subdomain_names }}}
                {{{DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()}}},
                // HiveId
                HIVE_ID,
                // FakeTenantSlotBroker
                true,
                // FakeSchemeShard
                fakeSchemeShard,
                // CreateConsole
                false,
                // Nodes {tenant_pool_config}
                {{
                    // Node0
                    {
                        MakeDefaultTenantPoolConfig()
                    },
                    // Node1
                    {
                        MakeDefaultTenantPoolConfig()
                    },
                }},
                // DataCenterCount
                1
            };
            return res;
        }

        void StartSchemeCache(TTestActorRuntime& runtime, const TString& root = DOMAIN1_NAME) {
            for (ui32 nodeIndex = 0; nodeIndex < runtime.GetNodeCount(); ++nodeIndex) {
                auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>();
                cacheConfig->Roots.emplace_back(1, SCHEME_SHARD1_ID, root);
                cacheConfig->Counters = new ::NMonitoring::TDynamicCounters();

                IActor* schemeCache = CreateSchemeBoardSchemeCache(cacheConfig.Get());
                TActorId schemeCacheId = runtime.Register(schemeCache, nodeIndex);
                runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId, nodeIndex);
            }
        }

#if 0
        void SimulateSleep(TTestActorRuntime& runtime, TDuration duration) {
            auto sender = runtime.AllocateEdgeActor();
            runtime.Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), duration);
            runtime.GrabEdgeEventRethrow<TEvents::TEvWakeup>(sender);
        }
#endif

        void CreateSequence(TTestActorRuntime& runtime, const TString& workingDir, const TString& scheme) {
            auto edge = runtime.AllocateEdgeActor(0);
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            auto* tx = request->Record.MutableTransaction()->MutableModifyScheme();
            tx->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
            tx->SetWorkingDir(workingDir);
            auto* op = tx->MutableSequence();
            bool parseResult = ::google::protobuf::TextFormat::ParseFromString(scheme, op);
            UNIT_ASSERT_C(parseResult, "protobuf parsing failed");
            runtime.Send(new IEventHandle(MakeTxProxyID(), edge, request.Release()));

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(edge);
            auto* msg = ev->Get();
            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
            Y_ABORT_UNLESS(status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);

            ui64 schemeShardTabletId = msg->Record.GetSchemeShardTabletId();
            auto notifyReq = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
            notifyReq->Record.SetTxId(msg->Record.GetTxId());
            runtime.SendToPipe(schemeShardTabletId, edge, notifyReq.Release());
            runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge);
        }

        void DropSequence(TTestActorRuntime& runtime, const TString& workingDir, const TString& name) {
            auto edge = runtime.AllocateEdgeActor(0);
            auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
            auto* tx = request->Record.MutableTransaction()->MutableModifyScheme();
            tx->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropSequence);
            tx->SetWorkingDir(workingDir);
            auto* op = tx->MutableDrop();
            op->SetName(name);
            runtime.Send(new IEventHandle(MakeTxProxyID(), edge, request.Release()));

            auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(edge);
            auto* msg = ev->Get();
            const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
            Y_ABORT_UNLESS(status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress);

            ui64 schemeShardTabletId = msg->Record.GetSchemeShardTabletId();
            auto notifyReq = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
            notifyReq->Record.SetTxId(msg->Record.GetTxId());
            runtime.SendToPipe(schemeShardTabletId, edge, notifyReq.Release());
            runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge);
        }

        TPathId DescribeSequence(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui64* sequenceShardId = nullptr) {
            TAutoPtr<IEventHandle> handle;

            auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            request->Record.MutableDescribePath()->SetPath(path);
            runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

            UNIT_ASSERT(reply->GetRecord().GetPathDescription().HasSequenceDescription());

            const auto& sequenceDescription = reply->GetRecord().GetPathDescription().GetSequenceDescription();

            if (sequenceShardId) {
                *sequenceShardId = sequenceDescription.GetSequenceShard();
            }

            return TPathId::FromProto(sequenceDescription.GetPathId());
        }

        void FreezeSequence(TTestActorRuntime& runtime, ui64 sequenceShardId, const TPathId& pathId) {
            auto sender = runtime.AllocateEdgeActor(0);
            auto clientId = runtime.ConnectToPipe(sequenceShardId, sender, 0, GetPipeConfigWithRetries());
            runtime.SendToPipe(clientId, sender, new TEvSequenceShard::TEvFreezeSequence(pathId), 0, 0);

            auto ev = runtime.GrabEdgeEventRethrow<TEvSequenceShard::TEvFreezeSequenceResult>(sender);
            runtime.ClosePipe(clientId, sender, 0);

            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(),
                NKikimrTxSequenceShard::TEvFreezeSequenceResult::SUCCESS);
        }

        void SendNextValRequest(TTestActorRuntime& runtime, const TActorId& sender, const TString& path) {
            auto request = MakeHolder<TEvSequenceProxy::TEvNextVal>(path);
            runtime.Send(new IEventHandle(MakeSequenceProxyServiceID(), sender, request.Release()));
        }

        void SendGetSequenceRequest(TTestActorRuntime& runtime, const TActorId& sender, const TPathId& pathId) {
            auto request = MakeHolder<TEvSequenceProxy::TEvGetSequence>(pathId);
            runtime.Send(new IEventHandle(MakeSequenceProxyServiceID(), sender, request.Release()));
        }

        i64 WaitNextValResult(TTestActorRuntime& runtime, const TActorId& sender, Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
            auto ev = runtime.GrabEdgeEventRethrow<TEvSequenceProxy::TEvNextValResult>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, expectedStatus);
            return msg->Status == Ydb::StatusIds::SUCCESS ? msg->Value : 0;
        }

        i64 WaitGetSequenceResult(TTestActorRuntime& runtime, const TActorId& sender, Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
            auto ev = runtime.GrabEdgeEventRethrow<TEvSequenceProxy::TEvGetSequenceResult>(sender);
            auto* msg = ev->Get();
            UNIT_ASSERT_VALUES_EQUAL(msg->Status, expectedStatus);
            if (msg->Status != Ydb::StatusIds::SUCCESS) {
                return 0;
            }
            UNIT_ASSERT_VALUES_EQUAL(msg->MinValue, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->MaxValue, 9223372036854775807LL);
            UNIT_ASSERT_VALUES_EQUAL(msg->StartValue, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Cache, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Increment, 1);
            UNIT_ASSERT_VALUES_EQUAL(msg->Cycle, false);
            UNIT_ASSERT_VALUES_EQUAL(msg->NextUsed, false);
            return msg->NextValue;
        }

        i64 DoNextVal(TTestActorRuntime& runtime, const TString& path, Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
            auto sender = runtime.AllocateEdgeActor(0);
            SendNextValRequest(runtime, sender, path);
            return WaitNextValResult(runtime, sender, expectedStatus);
        }

        i64 DoGetSequence(TTestActorRuntime& runtime, const TString& path, Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS) {
            auto sender = runtime.AllocateEdgeActor(0);
            auto pathId = DescribeSequence(runtime, sender, path);
            SendGetSequenceRequest(runtime, sender, pathId);
            return WaitGetSequenceResult(runtime, sender, expectedStatus);
        }

        struct TAdvanceResult {
            Ydb::StatusIds::StatusCode Status;
            TPathId PathId;
            i64 NextValue = 0;
            bool NextUsed = false;
        };

        void SendAdvanceRequest(TTestActorRuntime& runtime, const TActorId& sender, const TString& path,
                i64 observedMin, i64 observedMax) {
            auto request = MakeHolder<TEvSequenceProxy::TEvAdvanceSequence>(DOMAIN1_NAME, path, observedMin, observedMax);
            runtime.Send(new IEventHandle(MakeSequenceProxyServiceID(), sender, request.Release()));
        }

        void SendAdvanceRequest(TTestActorRuntime& runtime, const TActorId& sender, const TPathId& pathId,
                i64 observedMin, i64 observedMax) {
            auto request = MakeHolder<TEvSequenceProxy::TEvAdvanceSequence>(DOMAIN1_NAME, pathId, observedMin, observedMax);
            runtime.Send(new IEventHandle(MakeSequenceProxyServiceID(), sender, request.Release()));
        }

        TAdvanceResult WaitAdvanceResult(TTestActorRuntime& runtime, const TActorId& sender) {
            auto ev = runtime.GrabEdgeEventRethrow<TEvSequenceProxy::TEvAdvanceSequenceResult>(sender);
            const auto* msg = ev->Get();
            return { msg->Status, msg->PathId, msg->NextValue, msg->NextUsed };
        }

        TAdvanceResult DoAdvance(TTestActorRuntime& runtime, const TString& path, i64 observedMin, i64 observedMax) {
            auto sender = runtime.AllocateEdgeActor(0);
            SendAdvanceRequest(runtime, sender, path, observedMin, observedMax);
            return WaitAdvanceResult(runtime, sender);
        }

        TAdvanceResult DoAdvance(TTestActorRuntime& runtime, const TPathId& pathId, i64 observedMin, i64 observedMax) {
            auto sender = runtime.AllocateEdgeActor(0);
            SendAdvanceRequest(runtime, sender, pathId, observedMin, observedMax);
            return WaitAdvanceResult(runtime, sender);
        }

    } // namespace

    Y_UNIT_TEST(Basics) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(false));
        StartSchemeCache(runtime);
        auto sender = runtime.AllocateEdgeActor(0);

        CreateSequence(runtime, "/dc-1", R"(
            Name: "seq"
        )");

        i64 value = DoGetSequence(runtime, "/dc-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);

        value = DoNextVal(runtime, "/dc-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);

        value = DoGetSequence(runtime, "/dc-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 2);

        value = DoGetSequence(runtime, "/dc-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 2);


        DoNextVal(runtime, "/dc-1/noseq", Ydb::StatusIds::SCHEME_ERROR);

        SendGetSequenceRequest(runtime, sender, TPathId());
        WaitGetSequenceResult(runtime, sender, Ydb::StatusIds::SCHEME_ERROR);

        ui64 allocateEvents = 0;
        ui64 getSequenceEvents = 0;
        auto observerFunc = [&](auto& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvSequenceShard::TEvAllocateSequence::EventType:
                    ++allocateEvents;
                    break;
                case TEvSequenceShard::TEvGetSequence::EventType:
                    ++getSequenceEvents;
                    break;

                default:
                    break;
            }

            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserver = runtime.SetObserverFunc(observerFunc);

        auto pathId = DescribeSequence(runtime, sender, "/dc-1/seq");

        for (int i = 0; i < 7; ++i) {
            SendNextValRequest(runtime, sender, "/dc-1/seq");
        }
        for (int i = 0; i < 7; ++i) {
            value = WaitNextValResult(runtime, sender);
            UNIT_ASSERT_VALUES_EQUAL(value, 2 + i);
        }
        UNIT_ASSERT_C(allocateEvents < 7, "Too many TEvSequenceShard::TEvAllocateSequence events: " << allocateEvents);

        for (int i = 0; i < 7; ++i) {
            SendGetSequenceRequest(runtime, sender, pathId);
        }
        for (int i = 0; i < 7; ++i) {
            value = WaitGetSequenceResult(runtime, sender);
            UNIT_ASSERT_VALUES_EQUAL(value, 9);
        }
        UNIT_ASSERT_C(getSequenceEvents < 7, "Too many TEvSequenceShard::TEvGetSequence events: " << getSequenceEvents);
        ui64 prevGetSequenceEvents = getSequenceEvents;

        for (int i = 0; i < 7; ++i) {
            SendNextValRequest(runtime, sender, "/dc-1/seq");
            SendGetSequenceRequest(runtime, sender, pathId);
        }
        for (int i = 0; i < 7; ++i) {
            WaitGetSequenceResult(runtime, sender);
        }

        UNIT_ASSERT_C(getSequenceEvents < 7 + prevGetSequenceEvents, "Too many TEvSequenceShard::TEvGetSequence events: " << getSequenceEvents - prevGetSequenceEvents);
    }

    Y_UNIT_TEST(DropRecreate) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(false));
        StartSchemeCache(runtime);

        CreateSequence(runtime, "/dc-1", R"(
            Name: "seq"
        )");

        i64 value = DoNextVal(runtime, "/dc-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);

        DropSequence(runtime, "/dc-1", "seq");

        DoNextVal(runtime, "/dc-1/seq", Ydb::StatusIds::SCHEME_ERROR);

        CreateSequence(runtime, "/dc-1", R"(
            Name: "seq"
        )");

        value = DoNextVal(runtime, "/dc-1/seq");
        UNIT_ASSERT_VALUES_EQUAL(value, 1);
    }

    Y_UNIT_TEST(PipeOutdated) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(false));
        StartSchemeCache(runtime);
        auto sender = runtime.AllocateEdgeActor(0);

        CreateSequence(runtime, "/dc-1", R"(
            Name: "seq"
        )");

        auto pathId = DescribeSequence(runtime, sender, "/dc-1/seq");

        // The shard answers PIPE_OUTDATED when the request came through a pipe that is already
        // gone. That is a transient condition, so the proxy reports it as retriable.
        auto observerFunc = [&](auto& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvSequenceShard::TEvAllocateSequence::EventType: {
                    auto response = MakeHolder<TEvSequenceShard::TEvAllocateSequenceResult>();
                    response->Record.SetStatus(NKikimrTxSequenceShard::TEvAllocateSequenceResult::PIPE_OUTDATED);
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                case TEvSequenceShard::TEvGetSequence::EventType: {
                    auto response = MakeHolder<TEvSequenceShard::TEvGetSequenceResult>();
                    response->Record.SetStatus(NKikimrTxSequenceShard::TEvGetSequenceResult::PIPE_OUTDATED);
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                case TEvSequenceShard::TEvAdvanceSequence::EventType: {
                    auto response = MakeHolder<TEvSequenceShard::TEvAdvanceSequenceResult>();
                    response->Record.SetStatus(NKikimrTxSequenceShard::TEvAdvanceSequenceResult::PIPE_OUTDATED);
                    runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release()));
                    return TTestActorRuntime::EEventAction::DROP;
                }

                default:
                    return TTestActorRuntime::EEventAction::PROCESS;
            }
        };
        runtime.SetObserverFunc(observerFunc);

        SendNextValRequest(runtime, sender, "/dc-1/seq");
        WaitNextValResult(runtime, sender, Ydb::StatusIds::UNAVAILABLE);

        SendGetSequenceRequest(runtime, sender, pathId);
        WaitGetSequenceResult(runtime, sender, Ydb::StatusIds::UNAVAILABLE);

        SendAdvanceRequest(runtime, sender, pathId, 1, 10);
        UNIT_ASSERT_VALUES_EQUAL(WaitAdvanceResult(runtime, sender).Status, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(Advance) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(false));
        StartSchemeCache(runtime);
        auto sender = runtime.AllocateEdgeActor(0);

        CreateSequence(runtime, "/dc-1", R"(
            Name: "seq"
        )");

        ui64 sequenceShardId = 0;
        auto pathId = DescribeSequence(runtime, sender, "/dc-1/seq", &sequenceShardId);
        UNIT_ASSERT(sequenceShardId != 0);

        // The proxy resolves the sequence by name, like it does for TEvNextVal.
        auto result = DoAdvance(runtime, "/dc-1/seq", 1, 10);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result.PathId == pathId);
        UNIT_ASSERT_VALUES_EQUAL(result.NextValue, 10);
        UNIT_ASSERT(result.NextUsed);

        // Resolving by path id goes straight to the shard and skips the name lookup.
        result = DoAdvance(runtime, pathId, 1, 20);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(result.PathId == pathId);
        UNIT_ASSERT_VALUES_EQUAL(result.NextValue, 20);
        UNIT_ASSERT(result.NextUsed);

        // An unknown name and a null path id are reported as resolve errors.
        result = DoAdvance(runtime, "/dc-1/noseq", 1, 10);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SCHEME_ERROR);

        SendAdvanceRequest(runtime, sender, TPathId(), 1, 10);
        result = WaitAdvanceResult(runtime, sender);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(AdvanceFrozen) {
        TTenantTestRuntime runtime(MakeTenantTestConfig(false));
        StartSchemeCache(runtime);
        auto sender = runtime.AllocateEdgeActor(0);

        CreateSequence(runtime, "/dc-1", R"(
            Name: "seq"
        )");

        ui64 sequenceShardId = 0;
        auto pathId = DescribeSequence(runtime, sender, "/dc-1/seq", &sequenceShardId);
        UNIT_ASSERT(sequenceShardId != 0);

        // Sequences are frozen on tablet move until they are restored at the destination.
        // The proxy must report that as a retriable error instead of applying nothing.
        FreezeSequence(runtime, sequenceShardId, pathId);

        auto result = DoAdvance(runtime, "/dc-1/seq", 1, 10);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);

        result = DoAdvance(runtime, pathId, 1, 20);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
    }

} // Y_UNIT_TEST_SUITE(SequenceProxy)

} // namespace NSequenceProxy
} // namespace NKikimr
