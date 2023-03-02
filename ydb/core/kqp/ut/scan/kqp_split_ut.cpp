#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/generic/size_literals.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NKikimr {
namespace NKqp {


Y_UNIT_TEST_SUITE(KqpSplit) {
    static ui64 RunSchemeTx(
            TTestActorRuntimeBase& runtime,
            THolder<TEvTxUserProxy::TEvProposeTransaction>&& request,
            TActorId sender = {},
            bool viaActorSystem = false,
            TEvTxUserProxy::TEvProposeTransactionStatus::EStatus expectedStatus = TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress)
    {
        if (!sender) {
            sender = runtime.AllocateEdgeActor();
        }

        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()), 0, viaActorSystem);
        auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(sender);
        Cerr << (TStringBuilder() << "scheme op " << ev->Get()->Record.ShortDebugString()) << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetStatus(), expectedStatus);

        return ev->Get()->Record.GetTxId();
    }

    ui64 AsyncSplitTable(
            Tests::TServer* server,
            TActorId sender,
            const TString& path,
            ui64 sourceTablet,
            ui64 splitKey)
    {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetExecTimeoutPeriod(Max<ui64>());

        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpSplitMergeTablePartitions);

        auto& desc = *request->Record.MutableTransaction()->MutableModifyScheme()->MutableSplitMergeTablePartitions();
        desc.SetTablePath(path);
        desc.AddSourceTabletId(sourceTablet);
        desc.AddSplitBoundary()->MutableKeyPrefix()->AddTuple()->MutableOptional()->SetUint64(splitKey);

        return RunSchemeTx(*server->GetRuntime(), std::move(request), sender, true);
    }

    void WaitTxNotification(Tests::TServer* server, TActorId sender, ui64 txId) {
        auto &runtime = *server->GetRuntime();
        auto &settings = server->GetSettings();

        auto request = MakeHolder<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion>();
        request->Record.SetTxId(txId);
        auto tid = NKikimr::Tests::ChangeStateStorage(NKikimr::Tests::SchemeRoot, settings.Domain);
        runtime.SendToPipe(tid, sender, request.Release(), 0, GetPipeConfigWithRetries());
        runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(sender);
    }

    NKikimrScheme::TEvDescribeSchemeResult DescribeTable(Tests::TServer* server,
                                                        TActorId sender,
                                                        const TString &path)
    {
        auto &runtime = *server->GetRuntime();
        TAutoPtr<IEventHandle> handle;
        TVector<ui64> shards;

        auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        request->Record.MutableDescribePath()->SetPath(path);
        request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
        runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
        auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

        return *reply->MutableRecord();
    }

    TVector<ui64> GetTableShards(Tests::TServer* server,
                                TActorId sender,
                                const TString &path)
    {
        TVector<ui64> shards;
        auto lsResult = DescribeTable(server, sender, path);
        for (auto &part : lsResult.GetPathDescription().GetTablePartitions())
            shards.push_back(part.GetDatashardId());

        return shards;
    }

    i64 SetSplitMergePartCountLimit(TTestActorRuntime* runtime, i64 val) {
        TAtomic prev;
        runtime->GetAppData().Icb->SetValue("SchemeShard_SplitMergePartCountLimit", val, prev);
        return prev;
    }

    class TReplyPipeStub : public TActor<TReplyPipeStub> {
    public:
        TReplyPipeStub(TActorId owner, TActorId client)
            : TActor<TReplyPipeStub>(&TReplyPipeStub::State)
            , Owner(owner)
            , Client(client)
        {
        }

        STATEFN(State) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPipeCache::TEvForward, Handle);
                hFunc(TEvPipeCache::TEvUnlink, Handle);
                hFunc(TEvDataShard::TEvReadResult, Handle);
                default:
                    Handle(ev);
            }
        }

        void Handle(TAutoPtr<IEventHandle> ev) {
            Send(Client, ev->ReleaseBase());
        }

        void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
            if (ToSkip.fetch_sub(1) <= 0 && ToCapture.fetch_sub(1) > 0) {
                Cerr << "captured evreadresult -----------------------------------------------------------" << Endl;
                with_lock(CaptureLock) {
                    Captured.push_back(THolder(ev.Release()));
                }
                if (ToCapture.load() <= 0) {
                    ReadsReceived.Signal();
                }
                return;
            }
            Send(Client, ev->ReleaseBase());
        }

        void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
            Send(PipeCache, ev->Release());
        }

        void Handle(TEvPipeCache::TEvUnlink::TPtr& ev) {
            Send(PipeCache, ev->Release());
        }

        void SetupCapture(i64 skip, i64 capture) {
            ToCapture.store(capture);
            ToSkip.store(skip);
            ReadsReceived.Reset();
        }

        void SendCaptured(NActors::TTestActorRuntime* runtime) {
            TVector<THolder<IEventHandle>> tosend;
            with_lock(CaptureLock) {
                tosend.swap(Captured);
            }
            for (auto& ev : tosend) {
                ev->Rewrite(ev->GetTypeRewrite(), Client);
                runtime->Send(ev.Release());
            }
        }

    private:
        TActorId PipeCache = MakePipePeNodeCacheID(false);
        TActorId Owner;
        TActorId Client;

        TMutex CaptureLock;
        TVector<THolder<IEventHandle>> Captured;
        TManualEvent ReadsReceived;

        std::atomic<i64> ToCapture;
        std::atomic<i64> ToSkip;
    };

    class TReadActorPipeCacheStub : public TActor<TReadActorPipeCacheStub> {
    public:
        TReadActorPipeCacheStub()
            : TActor<TReadActorPipeCacheStub>(&TReadActorPipeCacheStub::State)
        {
            SkipAll();
            AllowResults();
        }

        void SetupResultsCapture(i64 skip, i64 capture = std::numeric_limits<i64>::max()) {
            ReverseSkip.store(skip);
            ReverseCapture.store(capture);
            for (auto& [_, pipe] : Pipes) {
                pipe->SetupCapture(ReverseSkip.load(), ReverseCapture.load());
            }
        }

        void AllowResults() {
            SetupResultsCapture(std::numeric_limits<i64>::max(), 0);
        }

        void SetupCapture(i64 skip, i64 capture = std::numeric_limits<i64>::max()) {
            ToCapture.store(capture);
            ToSkip.store(skip);
            ReadsReceived.Reset();
        }

        void SkipAll() {
            SetupCapture(std::numeric_limits<i64>::max(), 0);
        }

        void State(TAutoPtr<::NActors::IEventHandle> &ev, const ::NActors::TActorContext &ctx) {
            Y_UNUSED(ctx);
            if (ev->GetTypeRewrite() == TEvPipeCache::TEvForward::EventType) {
                auto* forw = reinterpret_cast<TEvPipeCache::TEvForward::TPtr*>(&ev);
                auto readtype = TEvDataShard::TEvRead::EventType;
                auto acktype = TEvDataShard::TEvReadAck::EventType;
                auto actual = forw->Get()->Get()->Ev->Type();
                bool isRead = actual == readtype || acktype;
                if (isRead && ToSkip.fetch_sub(1) <= 0 && ToCapture.fetch_sub(1) > 0) {
                    Cerr << "captured evread -----------------------------------------------------------" << Endl;
                    with_lock(CaptureLock) {
                        Captured.push_back(THolder(ev.Release()));
                    }
                    if (ToCapture.load() <= 0) {
                        ReadsReceived.Signal();
                    }
                    return;
                }
            }
            Forward(ev);
        }

        void Forward(TAutoPtr<::NActors::IEventHandle> ev) {
            TReplyPipeStub* pipe = Pipes[ev->Sender];
            if (pipe == nullptr) {
                pipe = Pipes[ev->Sender] = new TReplyPipeStub(SelfId(), ev->Sender);
                Register(pipe);
                for (auto& [_, pipe] : Pipes) {
                    pipe->SetupCapture(ReverseSkip.load(), ReverseCapture.load());
                }
            }
            auto id = pipe->SelfId();
            Send(id, ev->ReleaseBase());
        }

        void SendCaptured(NActors::TTestActorRuntime* runtime, bool sendResults = true) {
            TVector<THolder<IEventHandle>> tosend;
            with_lock(CaptureLock) {
                tosend.swap(Captured);
            }
            for (auto& ev : tosend) {
                TReplyPipeStub* pipe = Pipes[ev->Sender];
                if (pipe == nullptr) {
                    pipe = Pipes[ev->Sender] = new TReplyPipeStub(SelfId(), ev->Sender);
                    runtime->Register(pipe);
                    for (auto& [_, pipe] : Pipes) {
                        pipe->SetupCapture(ReverseSkip.load(), ReverseCapture.load());
                    }
                }
                auto id = pipe->SelfId();
                ev->Rewrite(ev->GetTypeRewrite(), id);
                runtime->Send(ev.Release());
            }
            if (sendResults) {
                for (auto& [_, pipe] : Pipes) {
                    pipe->SendCaptured(runtime);
                }
            }
        }

    public:
        TManualEvent ReadsReceived;
        std::atomic<i64> ToCapture;
        std::atomic<i64> ToSkip;

        std::atomic<i64> ReverseCapture;
        std::atomic<i64> ReverseSkip;

        TMutex CaptureLock;
        TVector<THolder<IEventHandle>> Captured;
        THashMap<TActorId, TReplyPipeStub*> Pipes;
    };

    TString ALL = ",101,102,103,201,202,203,301,302,303,401,402,403,501,502,503,601,602,603,701,702,703,801,802,803";
    TString Format(TVector<ui64> keys) {
        TStringBuilder res;
        for (auto k : keys) {
            res << "," << k;
        }
        return res;
    }

    void SendScanQuery(TTestActorRuntime* runtime, TActorId kqpProxy, TActorId sender, const TString& queryText) {
        auto ev = std::make_unique<NKqp::TEvKqp::TEvQueryRequest>();
        ev->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        ev->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        ev->Record.MutableRequest()->SetQuery(queryText);
        ev->Record.MutableRequest()->SetKeepSession(false);
        ActorIdToProto(sender, ev->Record.MutableRequestActorId());
        runtime->Send(new IEventHandle(kqpProxy, sender, ev.release()));
    };

    void CollectKeysTo(TVector<ui64>* collectedKeys, TTestActorRuntime* runtime, TActorId sender) {
        auto captureEvents =  [=](TTestActorRuntimeBase&, TAutoPtr<IEventHandle> &ev) {
            if (ev->GetTypeRewrite() == NKqp::TEvKqpExecuter::TEvStreamData::EventType) {
                auto& record = ev->Get<NKqp::TEvKqpExecuter::TEvStreamData>()->Record;
                for (auto& row : record.resultset().rows()) {
                    collectedKeys->push_back(row.items(0).uint64_value());
                }

                auto resp = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
                resp->Record.SetEnough(false);
                resp->Record.SetSeqNo(record.GetSeqNo());
                runtime->Send(new IEventHandle(ev->Sender, sender, resp.Release()));
                return true;
            }

            return false;
        };
        runtime->SetEventFilter(captureEvents);
    }

    enum class SortOrder {
        Descending,
        Ascending,
        Unspecified
    };

    TString OrderBy(SortOrder o) {
        if (o == SortOrder::Ascending) {
            return " ORDER BY Key ";
        }
        if (o == SortOrder::Descending) {
            return " ORDER BY Key DESC ";
        }
        return " ";
    }

    TVector<ui64> Canonize(TVector<ui64> collectedKeys, SortOrder o) {
        if (o == SortOrder::Unspecified) {
            Sort(collectedKeys);
        }
        if (o == SortOrder::Descending) {
            Reverse(collectedKeys.begin(), collectedKeys.end());
        }
        return collectedKeys;
    }

#define Y_UNIT_TEST_SORT(N, OPT)                                                                                   \
    template <SortOrder OPT>                                                                                       \
    struct TTestCase##N : public TCurrentTestCase {                                                                \
        TTestCase##N() : TCurrentTestCase() {                                                                      \
            if constexpr (OPT == SortOrder::Descending) { Name_ = #N "+Descending"; }                              \
            if constexpr (OPT == SortOrder::Ascending) { Name_ = #N "+Ascending"; }                                \
            if constexpr (OPT == SortOrder::Unspecified) { Name_ = #N "+Unspecified"; }                            \
        }                                                                                                          \
                                                                                                                   \
        static THolder<NUnitTest::TBaseTestCase> Create()  { return ::MakeHolder<TTestCase##N<Order>>();  }        \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<SortOrder::Ascending>::Create);                                     \
            TCurrentTest::AddTest(TTestCase##N<SortOrder::Descending>::Create);                                    \
            TCurrentTest::AddTest(TTestCase##N<SortOrder::Unspecified>::Create);                                   \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template <SortOrder OPT>                                                                                            \
    void TTestCase##N<OPT>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

    Y_UNIT_TEST_SORT(AfterResolve, Order) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        TFeatureFlags flags;
        flags.SetEnablePredicateExtractForScanQueries(true);
        settings.SetFeatureFlags(flags);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();

        auto& server = kikimr.GetTestServer();
        auto* runtime = server.GetRuntime();
        Y_UNUSED(runtime);
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        auto sender = runtime->AllocateEdgeActor();
        auto shards = GetTableShards(&server, sender, "/Root/KeyValueLargePartition");

        TVector<ui64> collectedKeys;
        CollectKeysTo(&collectedKeys, runtime, sender);

        auto* shim = new TReadActorPipeCacheStub();
        InterceptReadActorPipeCache(runtime->Register(shim));
        shim->SetupCapture(0, 1);
        SendScanQuery(runtime, kqpProxy, sender, "SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        SetSplitMergePartCountLimit(runtime, -1);
        {
            auto senderSplit = runtime->AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(&server, senderSplit, "/Root/KeyValueLargePartition", shards.at(0), 400);
            WaitTxNotification(&server, senderSplit, txId);
        }
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->SendCaptured(runtime);

        auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(collectedKeys, Order)), ALL);
    }

    Y_UNIT_TEST_SORT(AfterResult, Order) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        TFeatureFlags flags;
        flags.SetEnablePredicateExtractForScanQueries(true);
        settings.SetFeatureFlags(flags);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();

        auto& server = kikimr.GetTestServer();
        auto* runtime = server.GetRuntime();
        Y_UNUSED(runtime);
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        auto sender = runtime->AllocateEdgeActor();
        auto shards = GetTableShards(&server, sender, "/Root/KeyValueLargePartition");

        TVector<ui64> collectedKeys;
        CollectKeysTo(&collectedKeys, runtime, sender);

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(8);
        evread.SetMaxRows(8);
        InjectRangeEvReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(8);
        InjectRangeEvReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptReadActorPipeCache(runtime->Register(shim));
        SendScanQuery(runtime, kqpProxy, sender, "SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        SetSplitMergePartCountLimit(runtime, -1);
        {
            auto senderSplit = runtime->AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(&server, senderSplit, "/Root/KeyValueLargePartition", shards.at(0), 400);
            WaitTxNotification(&server, senderSplit, txId);
        }
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(runtime);

        auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(collectedKeys, Order)), ALL);
    }

    Y_UNIT_TEST_SORT(ChoosePartition, Order) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        TFeatureFlags flags;
        flags.SetEnablePredicateExtractForScanQueries(true);
        settings.SetFeatureFlags(flags);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();

        auto& server = kikimr.GetTestServer();
        auto* runtime = server.GetRuntime();
        Y_UNUSED(runtime);
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        auto sender = runtime->AllocateEdgeActor();
        auto shards = GetTableShards(&server, sender, "/Root/KeyValueLargePartition");

        TVector<ui64> collectedKeys;
        CollectKeysTo(&collectedKeys, runtime, sender);

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(8);
        evread.SetMaxRows(8);
        InjectRangeEvReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(8);
        InjectRangeEvReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(2, 1);
        shim->SetupResultsCapture(2);
        InterceptReadActorPipeCache(runtime->Register(shim));
        SendScanQuery(runtime, kqpProxy, sender, "SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        SetSplitMergePartCountLimit(runtime, -1);
        {
            auto senderSplit = runtime->AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(&server, senderSplit, "/Root/KeyValueLargePartition", shards.at(0), 400);
            WaitTxNotification(&server, senderSplit, txId);
        }
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(runtime);

        auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(collectedKeys, Order)), ALL);
    }


    Y_UNIT_TEST_SORT(BorderKeys, Order) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        TFeatureFlags flags;
        flags.SetEnablePredicateExtractForScanQueries(true);
        settings.SetFeatureFlags(flags);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();

        auto& server = kikimr.GetTestServer();
        auto* runtime = server.GetRuntime();
        Y_UNUSED(runtime);
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        auto sender = runtime->AllocateEdgeActor();
        auto shards = GetTableShards(&server, sender, "/Root/KeyValueLargePartition");

        TVector<ui64> collectedKeys;
        CollectKeysTo(&collectedKeys, runtime, sender);

        NKikimrTxDataShard::TEvRead evread;
        evread.SetMaxRowsInResult(12);
        evread.SetMaxRows(12);
        InjectRangeEvReadSettings(evread);

        NKikimrTxDataShard::TEvReadAck evreadack;
        evreadack.SetMaxRows(12);
        InjectRangeEvReadAckSettings(evreadack);

        auto* shim = new TReadActorPipeCacheStub();
        shim->SetupCapture(1, 1);
        shim->SetupResultsCapture(1);
        InterceptReadActorPipeCache(runtime->Register(shim));
        SendScanQuery(runtime, kqpProxy, sender, "SELECT Key FROM `/Root/KeyValueLargePartition`" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        SetSplitMergePartCountLimit(runtime, -1);
        {
            auto senderSplit = runtime->AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(&server, senderSplit, "/Root/KeyValueLargePartition", shards.at(0), 402);
            WaitTxNotification(&server, senderSplit, txId);

            shards = GetTableShards(&server, sender, "/Root/KeyValueLargePartition");

            txId = AsyncSplitTable(&server, senderSplit, "/Root/KeyValueLargePartition", shards.at(1), 404);
            WaitTxNotification(&server, senderSplit, txId);
        }
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->AllowResults();
        shim->SendCaptured(runtime);

        auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(collectedKeys, Order)), ALL);
    }

    Y_UNIT_TEST_SORT(AfterResolvePoints, Order) {
        TKikimrSettings settings;
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpScanQuerySourceRead(true);
        settings.SetDomainRoot(KikimrDefaultUtDomainRoot);
        TFeatureFlags flags;
        flags.SetEnablePredicateExtractForScanQueries(true);
        settings.SetFeatureFlags(flags);
        settings.SetAppConfig(appConfig);

        TKikimrRunner kikimr(settings);

        auto db = kikimr.GetTableClient();

        auto& server = kikimr.GetTestServer();
        auto* runtime = server.GetRuntime();
        Y_UNUSED(runtime);
        auto kqpProxy = MakeKqpProxyID(runtime->GetNodeId(0));

        auto sender = runtime->AllocateEdgeActor();
        auto shards = GetTableShards(&server, sender, "/Root/KeyValueLargePartition");

        TVector<ui64> collectedKeys;
        CollectKeysTo(&collectedKeys, runtime, sender);

        auto* shim = new TReadActorPipeCacheStub();
        InterceptReadActorPipeCache(runtime->Register(shim));
        shim->SetupCapture(0, 5);
        SendScanQuery(runtime, kqpProxy, sender,
            "PRAGMA Kikimr.OptEnablePredicateExtract=\"false\"; SELECT Key FROM `/Root/KeyValueLargePartition` where Key in (103, 302, 402, 502, 703)" + OrderBy(Order));

        shim->ReadsReceived.WaitI();
        Cerr << "starting split -----------------------------------------------------------" << Endl;
        SetSplitMergePartCountLimit(runtime, -1);
        {
            auto senderSplit = runtime->AllocateEdgeActor();
            ui64 txId = AsyncSplitTable(&server, senderSplit, "/Root/KeyValueLargePartition", shards.at(0), 400);
            WaitTxNotification(&server, senderSplit, txId);
        }
        Cerr << "resume evread -----------------------------------------------------------" << Endl;
        shim->SkipAll();
        shim->SendCaptured(runtime);

        auto reply = runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(Format(Canonize(collectedKeys, Order)), ",103,302,402,502,703");
    }
}


} // namespace NKqp
} // namespace NKikimr
