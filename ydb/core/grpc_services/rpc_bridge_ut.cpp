#include "rpc_bridge_base.h"
#include "service_bridge.h"
#include "local_rpc/local_rpc.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <functional>
#include <optional>

namespace NKikimr::NGRpcService {
namespace {

using TRpc = TGrpcRequestOperationCall<Ydb::Bridge::UpdateClusterStateRequest,
    Ydb::Bridge::UpdateClusterStateResponse>;

class TStartRequest : public TActorBootstrapped<TStartRequest>, public IFacilityProvider {
    std::unique_ptr<IRequestOpCtx> Request;
    using THandler = void (*)(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&);
    THandler Handler;

public:
    explicit TStartRequest(std::unique_ptr<IRequestOpCtx> request, THandler handler = DoUpdateClusterState)
        : Request(std::move(request))
        , Handler(handler) {
    }

    void Bootstrap() {
        Handler(std::move(Request), *this);
        PassAway();
    }

    ui64 GetChannelBufferSize() const override { return 0; }
    TActorId RegisterActor(IActor* actor) const override { return Register(actor); }
};

class TManagerProbe : public TActorBootstrapped<TManagerProbe> {
    ui32& Started;
    ui32& Finished;

public:
    TManagerProbe(ui32& started, ui32& finished)
        : Started(started)
        , Finished(finished)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

private:
    void HandleStarted() { ++Started; }
    void HandleFinished() { ++Finished; }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvGRpcServersManager::EvDisconnectRequestStarted, HandleStarted)
        cFunc(TEvGRpcServersManager::EvDisconnectRequestFinished, HandleFinished)
    )
};

struct TUpdateOptions {
    bool BadQuorum = false;
    std::optional<ui64> ExpectedGeneration;
    // the local snapshot of the receiving node
    ui64 ConfigGeneration = 1;
    NKikimrBridge::TClusterState::EPileState SecondaryState = NKikimrBridge::TClusterState::SYNCHRONIZED;
};

struct TSetup {
    TTestBasicRuntime Runtime;
    TActorId NodeWarden;
    ui32 Started = 0;
    ui32 Finished = 0;

    TSetup() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        NodeWarden = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeBlobStorageNodeWardenID(Runtime.GetNodeId(0)), NodeWarden);
        const auto manager = Runtime.Register(new TManagerProbe(Started, Finished));
        Runtime.RegisterService(MakeGRpcServersManagerId(Runtime.GetNodeId(0)), manager);
    }

    auto Start(TUpdateOptions options = {}) {
        Ydb::Bridge::UpdateClusterStateRequest request;
        NRpcService::SetRequestSyncOperationMode(request);
        auto *update = request.add_updates();
        update->set_pile_name("secondary");
        update->set_state(Ydb::Bridge::PileState::DISCONNECTED);
        if (options.BadQuorum) {
            request.add_quorum_piles("unknown");
        }
        if (options.ExpectedGeneration) {
            request.set_expected_generation(*options.ExpectedGeneration);
        }
        auto promise = NThreading::NewPromise<TRpc::TResponse>();
        using TCb = NRpcService::TPromiseWrapper<TRpc::TResponse>;
        auto ctx = std::make_unique<NRpcService::TLocalRpcCtx<TRpc, TCb>>(
            std::move(request), TCb(promise), "/Root", Nothing(), Nothing(), false);
        Runtime.Register(new TStartRequest(std::move(ctx)));
        const auto rpc = ReplyWithConfig(options.ConfigGeneration, options.SecondaryState);
        return std::pair{rpc, promise.GetFuture()};
    }

    auto StartGet(bool connectivity = true) {
        using TGetRpc = TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
            Ydb::Bridge::GetClusterStateResponse>;
        Ydb::Bridge::GetClusterStateRequest request;
        NRpcService::SetRequestSyncOperationMode(request);
        request.set_include_pile_connectivity(connectivity);
        auto promise = NThreading::NewPromise<TGetRpc::TResponse>();
        using TCb = NRpcService::TPromiseWrapper<TGetRpc::TResponse>;
        auto ctx = std::make_unique<NRpcService::TLocalRpcCtx<TGetRpc, TCb>>(
            std::move(request), TCb(promise), "/Root", Nothing(), Nothing(), false);
        Runtime.Register(new TStartRequest(std::move(ctx), DoGetClusterState));
        const auto rpc = ReplyWithConfig();
        return std::pair{rpc, promise.GetFuture()};
    }

    std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot> GrabInvoke(TActorId rpc,
            TDuration timeout = TDuration::Seconds(1))
    {
        // Wait for either the query or a terminal marker; an absent event must not leave the runtime idle forever.
        Runtime.Schedule(new IEventHandle(NodeWarden, {}, new TEvents::TEvWakeup()), timeout);
        std::optional<NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot> query;
        Runtime.WaitForEdgeEvents([&](auto&, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NStorage::TEvNodeConfigInvokeOnRoot::EventType && event->Sender == rpc) {
                query = event->Get<NStorage::TEvNodeConfigInvokeOnRoot>()->Record;
            } else if (event->GetTypeRewrite() != TEvents::TEvWakeup::EventType) {
                return false;
            }
            event.Reset();
            return true;
        }, {NodeWarden});
        return query;
    }

    void ReplyToInvoke(TActorId rpc, NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::EStatus status,
            const TString& errorReason = {})
    {
        auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        response->Record.SetStatus(status);
        if (errorReason) {
            response->Record.SetErrorReason(errorReason);
        }
        Runtime.Send(new IEventHandle(rpc, NodeWarden, response.release()), 0, true);
    }

    TActorId ReplyWithConfig(ui64 generation = 1,
            NKikimrBridge::TClusterState::EPileState secondaryState = NKikimrBridge::TClusterState::SYNCHRONIZED)
    {
        const auto query = Runtime.GrabEdgeEventIf<TEvNodeWardenQueryStorageConfig>(NodeWarden,
            [](const auto& ev) { return !ev->Get()->Subscribe; }, TDuration::Seconds(5));
        UNIT_ASSERT_C(query, "RPC did not request the storage configuration");
        const auto rpc = query->Sender;
        UNIT_ASSERT_C(Runtime.FindActor(rpc), "Config query sender has already stopped: " << rpc);

        auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
        config->MutableSelfManagementConfig()->SetEnabled(true);
        auto *state = config->MutableClusterState();
        state->SetGeneration(generation);
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPromotedPile);
        state->AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        state->AddPerPileState(secondaryState);
        auto bridge = std::make_shared<TBridgeInfo>();
        bridge->Piles.resize(2);
        for (ui32 i = 0; i != 2; ++i) {
            bridge->Piles[i].BridgePileId = TBridgePileId::FromPileIndex(i);
            bridge->Piles[i].Name = i ? "secondary" : "primary";
            bridge->Piles[i].State = i ? secondaryState : NKikimrBridge::TClusterState::SYNCHRONIZED;
            bridge->Piles[i].IsPrimary = i == 0;
        }
        bridge->PrimaryPile = &bridge->Piles[0];
        bridge->SelfNodePile = &bridge->Piles[1];
        Runtime.Send(new IEventHandle(rpc, NodeWarden, new TEvNodeWardenStorageConfig(config, true, bridge)), 0, true);
        return rpc;
    }
};

enum class ECompletion { Success, Error, Timeout, ClientLost };

void TestCompletion(ECompletion completion) {
    TSetup setup;
    auto [rpc, future] = setup.Start();
    const auto invoke = setup.Runtime.GrabEdgeEventIf<NStorage::TEvNodeConfigInvokeOnRoot>(setup.NodeWarden,
        [&](const auto& ev) { return ev->Sender == rpc; }, TDuration::Seconds(5));
    UNIT_ASSERT_C(invoke, "RPC did not invoke the cluster-state change");
    UNIT_ASSERT_VALUES_EQUAL(invoke->Sender, rpc);
    if (completion == ECompletion::Timeout) {
        setup.Runtime.Send(new IEventHandle(rpc, {}, new TEvents::TEvWakeup(TRpcRequestWithOperationParamsActor<
            TStartRequest, TRpc, true>::WakeupTagTimeout)), 0, true);
    } else if (completion == ECompletion::ClientLost) {
        setup.Runtime.Send(new IEventHandle(rpc, {}, new TRpcServices::TEvForgetOperation()), 0, true);
    } else {
        auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        response->Record.SetStatus(completion == ECompletion::Success
            ? NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK
            : NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR);
        setup.Runtime.Send(new IEventHandle(rpc, setup.NodeWarden, response.release()), 0, true);
    }
    setup.Runtime.WaitFuture(future);
    setup.Runtime.WaitFor("disconnect request completion", [&] { return setup.Finished == 1; }, TDuration::Seconds(1));
    UNIT_ASSERT_VALUES_EQUAL(setup.Started, 1);
    UNIT_ASSERT_VALUES_EQUAL(setup.Finished, 1);
}

}

namespace {

using TModeRpc = TGrpcRequestOperationCall<Ydb::Bridge::SetDatabaseBridgeModeRequest,
    Ydb::Bridge::SetDatabaseBridgeModeResponse>;

struct TModeSetup {
    TTestBasicRuntime Runtime;
    TActorId Edge;
    TActorId Rpc;

    TModeSetup() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Edge = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeTxProxyID(), Edge);
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->Type == TEvBlobStorage::TEvControllerConfigRequest::EventType) {
                event->Rewrite(event->Type, Edge);
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    auto Start(std::optional<bool> bridged = true, const TString& database = "/Root/db", const TString& retain = "",
            const TString& connectionDatabase = "/Root") {
        Ydb::Bridge::SetDatabaseBridgeModeRequest request;
        NRpcService::SetRequestSyncOperationMode(request);
        if (bridged) {
            request.set_bridge_mode(*bridged);
        }
        request.set_retain_pile_name(retain);
        request.set_database_path(database);
        auto promise = NThreading::NewPromise<TModeRpc::TResponse>();
        using TCb = NRpcService::TPromiseWrapper<TModeRpc::TResponse>;
        auto ctx = std::make_unique<NRpcService::TLocalRpcCtx<TModeRpc, TCb>>(
            std::move(request), TCb(promise), connectionDatabase, Nothing(), Nothing(), false);
        Runtime.Register(new TStartRequest(std::move(ctx), DoSetDatabaseBridgeMode));
        return promise.GetFuture();
    }

    void Describe(bool serverless = false, bool directory = false) {
        const auto navigate = Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvNavigate>(Edge);
        Rpc = navigate->Sender;
        UNIT_ASSERT_VALUES_EQUAL(navigate->Get()->Record.GetDescribePath().GetPath(), "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(navigate->Get()->Record.GetDatabaseName(), "/Root/db");
        auto reply = std::make_unique<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResultBuilder>();
        auto& record = reply->Record;
        record.SetStatus(NKikimrScheme::StatusSuccess);
        auto* description = record.MutablePathDescription();
        description->MutableSelf()->SetPathType(directory ? NKikimrSchemeOp::EPathTypeDir : NKikimrSchemeOp::EPathTypeSubDomain);
        auto* domain = description->MutableDomainDescription();
        domain->MutableDomainKey()->SetSchemeShard(100);
        domain->MutableDomainKey()->SetPathId(2);
        domain->MutableResourcesDomainKey()->CopyFrom(domain->GetDomainKey());
        if (serverless) {
            domain->MutableResourcesDomainKey()->SetPathId(3);
        }
        domain->AddStoragePools()->SetName("pool-a");
        domain->AddStoragePools()->SetName("pool-b");
        Runtime.Send(new IEventHandle(Rpc, Edge, reply.release()), 0, true);
    }

    auto GrabBscRequest() {
        return Runtime.GrabEdgeEvent<TEvBlobStorage::TEvControllerConfigRequest>(Edge);
    }

    void ReplyWithPools(std::initializer_list<TString> names = {"pool-a", "pool-b"}) {
        const auto read = GrabBscRequest();
        const auto& commands = read->Get()->Record.GetRequest().GetCommand();
        UNIT_ASSERT_VALUES_EQUAL(commands.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(commands.Get(0).GetReadStoragePool().NameSize(), 2);
        auto reply = std::make_unique<TEvBlobStorage::TEvControllerConfigResponse>();
        auto* response = reply->Record.MutableResponse();
        response->SetSuccess(true);
        auto* status = response->AddStatus();
        status->SetSuccess(true);
        ui64 id = 10;
        for (const auto& name : names) {
            auto* pool = status->AddStoragePool();
            pool->SetName(name);
            pool->SetBoxId(1);
            pool->SetStoragePoolId(id++);
            pool->SetItemConfigGeneration(42);
        }
        Runtime.Send(new IEventHandle(Rpc, Edge, reply.release()), 0, true);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(BridgeDatabaseMode) {
    Y_UNIT_TEST(RequiresTargetDatabaseAndExplicitMode) {
        TModeSetup setup;
        for (const auto& database : {TString(""), TString("/"), TString("relative/db")}) {
            auto future = setup.Start(true, database);
            UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        auto future = setup.Start(std::nullopt);
        UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(ConnectionDatabaseDoesNotSupplyTarget) {
        TModeSetup setup;
        auto future = setup.Start(true, "", "", "/Root/db");
        UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(RequiresAdministrator) {
        TModeSetup setup;
        setup.Runtime.GetAppData().AdministrationAllowedSIDs = {"admin"};
        auto future = setup.Start();
        UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(), Ydb::StatusIds::UNAUTHORIZED);
    }

    Y_UNIT_TEST(RetainPileRequiresOrdinaryStorage) {
        TModeSetup setup;
        auto future = setup.Start(true, "/Root/db", "secondary");
        UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(), Ydb::StatusIds::BAD_REQUEST);
    }

    Y_UNIT_TEST(RejectsServerlessAndNonDatabasePaths) {
        for (bool serverless : {false, true}) {
            TModeSetup setup;
            auto future = setup.Start();
            setup.Describe(serverless, !serverless);
            UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(),
                serverless ? Ydb::StatusIds::PRECONDITION_FAILED : Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(RejectsMissingAmbiguousAndUnexpectedPools) {
        for (const auto& names : {std::initializer_list<TString>{"pool-a"}, {"pool-a", "pool-a"}, {"pool-a", "other-db"}}) {
            TModeSetup setup;
            auto future = setup.Start();
            setup.Describe();
            setup.ReplyWithPools(names);
            UNIT_ASSERT_VALUES_EQUAL(setup.Runtime.WaitFuture(future).operation().status(), Ydb::StatusIds::PRECONDITION_FAILED);
        }
    }

    Y_UNIT_TEST(ChangesAllPoolsInOneRequestAndPropagatesFailure) {
        for (bool bridged : {false, true}) {
            for (bool success : {false, true}) {
                TModeSetup setup;
                auto future = setup.Start(bridged, "/Root/db", bridged ? "" : "secondary");
                setup.Describe();
                setup.ReplyWithPools();
                const auto change = setup.GrabBscRequest();
                const auto& commands = change->Get()->Record.GetRequest().GetCommand();
                UNIT_ASSERT_VALUES_EQUAL(commands.size(), 2);
                for (int i = 0; i < commands.size(); ++i) {
                    const auto& command = commands.Get(i).GetChangeStoragePoolBridgeMode();
                    UNIT_ASSERT_VALUES_EQUAL(command.GetBoxId(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(command.GetStoragePoolId(), 10 + i);
                    UNIT_ASSERT_VALUES_EQUAL(command.GetItemConfigGeneration(), 42);
                    UNIT_ASSERT_VALUES_EQUAL(command.GetBridgeMode(), bridged);
                    UNIT_ASSERT_VALUES_EQUAL(command.GetRetainPileName(), bridged ? "" : "secondary");
                }
                auto reply = std::make_unique<TEvBlobStorage::TEvControllerConfigResponse>();
                auto* response = reply->Record.MutableResponse();
                response->SetSuccess(success);
                response->AddStatus()->SetSuccess(true);
                auto* status = response->AddStatus();
                status->SetSuccess(success);
                if (!success) {
                    status->SetFailReason(NKikimrBlobStorage::TConfigResponse::TStatus::kItemConfigGenerationMismatch);
                    status->SetErrorDescription("generation mismatch");
                }
                setup.Runtime.Send(new IEventHandle(setup.Rpc, setup.Edge, reply.release()), 0, true);
                const auto result = setup.Runtime.WaitFuture(future);
                UNIT_ASSERT_VALUES_EQUAL(result.operation().status(), success ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::ABORTED);
                if (!success) {
                    UNIT_ASSERT_STRING_CONTAINS(result.DebugString(), "generation mismatch");
                }
            }
        }
    }

    Y_UNIT_TEST(CancellationWhileReadingPools) {
        for (bool timeout : {false, true}) {
            TModeSetup setup;
            auto future = setup.Start(true, "/Root/db", "", "");
            setup.Describe();
            setup.GrabBscRequest();
            if (timeout) {
                setup.Runtime.Send(new IEventHandle(setup.Rpc, {}, new TEvents::TEvWakeup(TRpcRequestWithOperationParamsActor<
                    TStartRequest, TModeRpc, true>::WakeupTagTimeout)), 0, true);
            } else {
                setup.Runtime.Send(new IEventHandle(setup.Rpc, {}, new TRpcServices::TEvForgetOperation()), 0, true);
            }
            setup.Runtime.WaitFuture(future);
            UNIT_ASSERT(!setup.Runtime.FindActor(setup.Rpc));
        }
    }
}

Y_UNIT_TEST_SUITE(BridgeRpcLifecycle) {
    Y_UNIT_TEST(InvalidQuorumDoesNotDelayServerStop) {
        TSetup setup;
        auto [rpc, future] = setup.Start({.BadQuorum = true});
        const auto response = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        setup.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(setup.Started, 0);
        UNIT_ASSERT_VALUES_EQUAL(setup.Finished, 0);
    }

    Y_UNIT_TEST(SuccessReleasesServer) { TestCompletion(ECompletion::Success); }
    Y_UNIT_TEST(FailureReleasesServer) { TestCompletion(ECompletion::Error); }
    Y_UNIT_TEST(TimeoutReleasesServer) { TestCompletion(ECompletion::Timeout); }
    Y_UNIT_TEST(ClientLossReleasesServer) { TestCompletion(ECompletion::ClientLost); }
}

Y_UNIT_TEST_SUITE(BridgeGetClusterStateConnectivity) {
    Y_UNIT_TEST(ConnectivityIsQueriedOnRequest) {
        TSetup setup;
        auto [rpc, future] = setup.StartGet();
        const auto invoke = setup.GrabInvoke(rpc);
        UNIT_ASSERT_C(invoke, "RPC did not query connectivity");
        UNIT_ASSERT(invoke->HasQueryPileConnectivity());
        UNIT_ASSERT(!future.HasValue());
        auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
        auto *report = response->Record.MutablePileConnectivity();
        report->SetReporterNodeId(9);
        report->SetHasScepter(false);
        report->SetBindingInProgress(true);
        report->SetClusterStateGeneration(2);
        for (ui32 i = 0; i != 2; ++i) {
            auto *pile = report->AddPiles();
            TBridgePileId::FromPileIndex(i).CopyToProto(pile,
                &NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::TPileConnectivity::TPile::SetBridgePileId);
            // Names belong to the reporter's snapshot, which can differ from the local registry.
            pile->SetPileName(i ? "secondary" : "reporter_primary");
            pile->SetNumStaticNodes(8);
            pile->SetNumConnectedStaticNodes(i ? 8 : 0);
            pile->SetHasNodeQuorum(i != 0);
            pile->SetRequiresConfigQuorum(true);
            pile->SetRequiresDataQuorum(i == 0);
        }
        setup.Runtime.Send(new IEventHandle(rpc, setup.NodeWarden, response.release()), 0, true);
        const auto reply = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Bridge::GetClusterStateResult result;
        UNIT_ASSERT(reply.operation().result().UnpackTo(&result));
        UNIT_ASSERT(result.has_pile_connectivity());
        UNIT_ASSERT_VALUES_EQUAL(result.generation(), 1);
        const auto& connectivity = result.pile_connectivity();
        UNIT_ASSERT_VALUES_EQUAL(connectivity.reporter_node_id(), 9);
        UNIT_ASSERT(!connectivity.reporter_is_root());
        UNIT_ASSERT(connectivity.binding_in_progress());
        UNIT_ASSERT_VALUES_EQUAL(connectivity.generation(), 2);
        UNIT_ASSERT_VALUES_EQUAL(connectivity.piles_size(), 2);
        for (ui32 i = 0; i != 2; ++i) {
            const auto& pile = connectivity.piles(i);
            UNIT_ASSERT_VALUES_EQUAL(pile.pile_name(), i ? "secondary" : "reporter_primary");
            UNIT_ASSERT_VALUES_EQUAL(pile.static_nodes(), 8);
            UNIT_ASSERT_VALUES_EQUAL(pile.connected_static_nodes(), i ? 8 : 0);
            UNIT_ASSERT_VALUES_EQUAL(pile.has_node_quorum(), i != 0);
            UNIT_ASSERT(pile.in_config_quorum());
            UNIT_ASSERT_VALUES_EQUAL(pile.in_data_quorum(), i == 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(setup.Started, 0);
        UNIT_ASSERT_VALUES_EQUAL(setup.Finished, 0);
    }

    Y_UNIT_TEST(NoConnectivityQueryByDefault) {
        TSetup setup;
        auto [rpc, future] = setup.StartGet(false);
        const auto reply = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Bridge::GetClusterStateResult result;
        UNIT_ASSERT(reply.operation().result().UnpackTo(&result));
        UNIT_ASSERT(!result.has_pile_connectivity());
        const auto invoke = setup.GrabInvoke(rpc, TDuration::MilliSeconds(10));
        UNIT_ASSERT(!invoke);
    }

    void TestError(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::EStatus status,
            Ydb::StatusIds::StatusCode expected)
    {
        TSetup setup;
        auto [rpc, future] = setup.StartGet();
        const auto invoke = setup.GrabInvoke(rpc);
        UNIT_ASSERT_C(invoke, "RPC did not query connectivity");
        auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        response->Record.SetStatus(status);
        response->Record.SetErrorReason("connectivity query failed");
        setup.Runtime.Send(new IEventHandle(rpc, setup.NodeWarden, response.release()), 0, true);
        const auto reply = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), expected);
    }

    Y_UNIT_TEST(RaceIsAborted) {
        TestError(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::RACE, Ydb::StatusIds::ABORTED);
    }

    Y_UNIT_TEST(NoQuorumIsUnavailable) {
        TestError(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::NO_QUORUM, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(ErrorIsReported) {
        TestError(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::ERROR, Ydb::StatusIds::GENERIC_ERROR);
    }

    void TestCancellation(bool timeout) {
        TSetup setup;
        auto [rpc, future] = setup.StartGet();
        const auto invoke = setup.GrabInvoke(rpc);
        UNIT_ASSERT_C(invoke, "RPC did not query connectivity");
        if (timeout) {
            using TGetRpc = TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
                Ydb::Bridge::GetClusterStateResponse>;
            setup.Runtime.Send(new IEventHandle(rpc, {}, new TEvents::TEvWakeup(TRpcRequestWithOperationParamsActor<
                TStartRequest, TGetRpc, true>::WakeupTagTimeout)), 0, true);
        } else {
            setup.Runtime.Send(new IEventHandle(rpc, {}, new TRpcServices::TEvForgetOperation()), 0, true);
        }
        setup.Runtime.WaitFuture(future);
        UNIT_ASSERT(!setup.Runtime.FindActor(rpc));
        // A late reply must not restart the completed request.
        auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
        response->Record.MutablePileConnectivity();
        setup.Runtime.Send(new IEventHandle(rpc, setup.NodeWarden, response.release()), 0, true);
        setup.Runtime.SimulateSleep(TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(setup.Started, 0);
        UNIT_ASSERT_VALUES_EQUAL(setup.Finished, 0);
    }

    Y_UNIT_TEST(TimeoutWhileWaiting) {
        TestCancellation(true);
    }

    Y_UNIT_TEST(ClientLostWhileWaiting) {
        TestCancellation(false);
    }

    Y_UNIT_TEST(MissingPayloadIsRejected) {
        TestError(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK, Ydb::StatusIds::GENERIC_ERROR);
    }
}

Y_UNIT_TEST_SUITE(BridgeUpdateClusterStateExpectedGeneration) {
    using EInvokeStatus = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    // The request is rejected by the receiving node: nothing is sent to distconf and no server is held.
    void TestRejectedLocally(TUpdateOptions options, Ydb::StatusIds::StatusCode expected) {
        TSetup setup;
        auto [rpc, future] = setup.Start(options);
        const auto invoke = setup.GrabInvoke(rpc, TDuration::MilliSeconds(10));
        UNIT_ASSERT_C(!invoke, "stale expected_generation must not reach distconf: "
            << (invoke ? invoke->ShortDebugString() : TString()));
        const auto reply = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), expected, reply.operation().DebugString());
        UNIT_ASSERT_VALUES_EQUAL(setup.Started, 0);
        UNIT_ASSERT_VALUES_EQUAL(setup.Finished, 0);
    }

    Y_UNIT_TEST(StaleGenerationIsAbortedWithoutInvoke) {
        TestRejectedLocally({.ExpectedGeneration = 10, .ConfigGeneration = 14}, Ydb::StatusIds::ABORTED);
    }

    Y_UNIT_TEST(LaggingNodeIsUnavailableWithoutInvoke) {
        TestRejectedLocally({.ExpectedGeneration = 20, .ConfigGeneration = 14}, Ydb::StatusIds::UNAVAILABLE);
    }

    Y_UNIT_TEST(StaleRequestIsAbortedBeforeValidation) {
        // the pile became suspended meanwhile: replan (ABORTED), not a malformed request (BAD_REQUEST)
        TestRejectedLocally({.ExpectedGeneration = 10, .ConfigGeneration = 14,
            .SecondaryState = NKikimrBridge::TClusterState::SUSPENDED}, Ydb::StatusIds::ABORTED);
    }

    // The request reaches distconf and the root answers with the given status.
    // The command is checked by the caller; returns the reply of the RPC.
    auto TestForwarded(TUpdateOptions options, EInvokeStatus::EStatus status, const TString& errorReason,
            const std::function<void(const NKikimrBlobStorage::TEvNodeConfigInvokeOnRoot::TSwitchBridgeClusterState&)>&
                checkCommand = {})
    {
        TSetup setup;
        auto [rpc, future] = setup.Start(options);
        const auto invoke = setup.GrabInvoke(rpc);
        UNIT_ASSERT_C(invoke, "RPC did not invoke the cluster-state change");
        UNIT_ASSERT(invoke->HasSwitchBridgeClusterState());
        if (checkCommand) {
            checkCommand(invoke->GetSwitchBridgeClusterState());
        }
        setup.ReplyToInvoke(rpc, status, errorReason);
        auto reply = setup.Runtime.WaitFuture(future);
        setup.Runtime.WaitFor("disconnect request completion", [&] { return setup.Finished == 1; },
            TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(setup.Started, 1);
        UNIT_ASSERT_VALUES_EQUAL(setup.Finished, 1);
        return reply;
    }

    Y_UNIT_TEST(MatchingGenerationIsForwarded) {
        const auto reply = TestForwarded({.ExpectedGeneration = 14, .ConfigGeneration = 14}, EInvokeStatus::OK, {},
            [](const auto& cmd) {
                UNIT_ASSERT_VALUES_EQUAL(cmd.GetNewClusterState().GetGeneration(), 15);
                UNIT_ASSERT(cmd.HasExpectedGeneration());
                UNIT_ASSERT_VALUES_EQUAL(cmd.GetExpectedGeneration(), 14);
            });
        UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), Ydb::StatusIds::SUCCESS,
            reply.operation().DebugString());
    }

    Y_UNIT_TEST(RootMismatchIsAborted) {
        const auto reply = TestForwarded({.ExpectedGeneration = 14, .ConfigGeneration = 14},
            EInvokeStatus::GENERATION_MISMATCH, "root generation mismatch");
        UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), Ydb::StatusIds::ABORTED,
            reply.operation().DebugString());
        UNIT_ASSERT_STRING_CONTAINS(reply.operation().DebugString(), "root generation mismatch");
    }

    Y_UNIT_TEST(UnknownOutcomeStaysInternalError) {
        for (const auto status : {EInvokeStatus::RACE, EInvokeStatus::ERROR}) {
            const auto reply = TestForwarded({.ExpectedGeneration = 14, .ConfigGeneration = 14},
                status, "outcome is unknown");
            UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), Ydb::StatusIds::INTERNAL_ERROR,
                reply.operation().DebugString());
        }
    }

    Y_UNIT_TEST(NoExpectedGenerationKeepsOldStamp) {
        const auto reply = TestForwarded({.ConfigGeneration = 14}, EInvokeStatus::ERROR, "failed",
            [](const auto& cmd) {
                UNIT_ASSERT_VALUES_EQUAL(cmd.GetNewClusterState().GetGeneration(), 15);
                UNIT_ASSERT(!cmd.HasExpectedGeneration());
            });
        UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), Ydb::StatusIds::INTERNAL_ERROR,
            reply.operation().DebugString());
    }
}

}
