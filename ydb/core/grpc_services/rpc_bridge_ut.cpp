#include "rpc_bridge_base.h"
#include "service_bridge.h"
#include "local_rpc/local_rpc.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/base/bridge_events.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/timestamp.pb.h>

#include <functional>
#include <algorithm>
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
    ui32& ConfigUpdates;

public:
    TManagerProbe(ui32& started, ui32& finished, ui32& configUpdates)
        : Started(started)
        , Finished(finished)
        , ConfigUpdates(configUpdates)
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

private:
    void HandleStarted() { ++Started; }
    void HandleFinished() { ++Finished; }
    void HandleConfig() { ++ConfigUpdates; }

    STRICT_STFUNC(StateFunc,
        cFunc(TEvGRpcServersManager::EvDisconnectRequestStarted, HandleStarted)
        cFunc(TEvGRpcServersManager::EvDisconnectRequestFinished, HandleFinished)
        cFunc(TEvNodeWardenStorageConfig::EventType, HandleConfig)
    )
};

struct TUpdateOptions {
    bool BadQuorum = false;
    std::optional<ui64> ExpectedGeneration;
    // the local snapshot of the receiving node
    ui64 ConfigGeneration = 1;
    NKikimrBridge::TClusterState::EPileState SecondaryState = NKikimrBridge::TClusterState::SYNCHRONIZED;
    Ydb::Bridge::PileState::State RequestedState = Ydb::Bridge::PileState::DISCONNECTED;
    bool PublishConfigBeforeReply = false;
};

struct TGetOptions {
    bool Connectivity = true;
    bool UnbridgedStatus = false;
    bool Bridged = true;
};

struct TSetup {
    TTestBasicRuntime Runtime;
    TActorId NodeWarden;
    ui32 Started = 0;
    ui32 Finished = 0;
    ui32 ConfigUpdates = 0;

    TSetup() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        NodeWarden = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeBlobStorageNodeWardenID(Runtime.GetNodeId(0)), NodeWarden);
        const auto manager = Runtime.Register(new TManagerProbe(Started, Finished, ConfigUpdates));
        Runtime.RegisterService(MakeGRpcServersManagerId(Runtime.GetNodeId(0)), manager);
    }

    auto Start(TUpdateOptions options = {}) {
        Ydb::Bridge::UpdateClusterStateRequest request;
        NRpcService::SetRequestSyncOperationMode(request);
        auto *update = request.add_updates();
        update->set_pile_name("secondary");
        update->set_state(options.RequestedState);
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

    auto StartGet(TGetOptions options = {}) {
        using TGetRpc = TGrpcRequestOperationCall<Ydb::Bridge::GetClusterStateRequest,
            Ydb::Bridge::GetClusterStateResponse>;
        Ydb::Bridge::GetClusterStateRequest request;
        NRpcService::SetRequestSyncOperationMode(request);
        request.set_include_pile_connectivity(options.Connectivity);
        request.set_include_unbridged_status(options.UnbridgedStatus);
        auto promise = NThreading::NewPromise<TGetRpc::TResponse>();
        using TCb = NRpcService::TPromiseWrapper<TGetRpc::TResponse>;
        auto ctx = std::make_unique<NRpcService::TLocalRpcCtx<TGetRpc, TCb>>(
            std::move(request), TCb(promise), "/Root", Nothing(), Nothing(), false);
        Runtime.Register(new TStartRequest(std::move(ctx), DoGetClusterState));
        const auto rpc = ReplyWithConfig(options.Bridged ? std::optional<ui64>(1) : std::nullopt);
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

    void PublishConfig(NKikimrBridge::TClusterState::EPileState secondaryState) {
        auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
        auto *state = config->MutableClusterState();
        state->SetGeneration(2);
        state->AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        state->AddPerPileState(secondaryState);
        auto bridge = std::make_shared<TBridgeInfo>();
        bridge->Piles.resize(2);
        bridge->Piles[0].State = NKikimrBridge::TClusterState::SYNCHRONIZED;
        bridge->Piles[1].State = secondaryState;
        bridge->SelfNodePile = &bridge->Piles[1];
        Runtime.Send(new IEventHandle(MakeGRpcServersManagerId(Runtime.GetNodeId(0)), NodeWarden,
            new TEvNodeWardenStorageConfig(config, true, bridge, config)), 0, true);
        Runtime.WaitFor("configuration publication", [&] { return ConfigUpdates == 1; }, TDuration::Seconds(1));
    }

    TActorId ReplyWithConfig(std::optional<ui64> generation = 1,
            NKikimrBridge::TClusterState::EPileState secondaryState = NKikimrBridge::TClusterState::SYNCHRONIZED)
    {
        const auto query = Runtime.GrabEdgeEventIf<TEvNodeWardenQueryStorageConfig>(NodeWarden,
            [](const auto& ev) { return !ev->Get()->Subscribe; }, TDuration::Seconds(5));
        UNIT_ASSERT_C(query, "RPC did not request the storage configuration");
        const auto rpc = query->Sender;
        UNIT_ASSERT_C(Runtime.FindActor(rpc), "Config query sender has already stopped: " << rpc);

        auto config = std::make_shared<NKikimrBlobStorage::TStorageConfig>();
        config->MutableSelfManagementConfig()->SetEnabled(true);
        auto bridge = std::make_shared<TBridgeInfo>();
        if (!generation) {
            // A prepared cluster can already have implicit pile information before it has a cluster state.
            bridge->IsImplicit = true;
            Runtime.Send(new IEventHandle(rpc, NodeWarden, new TEvNodeWardenStorageConfig(config, true, bridge)), 0, true);
            return rpc;
        }
        auto *state = config->MutableClusterState();
        state->SetGeneration(*generation);
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPrimaryPile);
        TBridgePileId::FromPileIndex(0).CopyToProto(state, &NKikimrBridge::TClusterState::SetPromotedPile);
        state->AddPerPileState(NKikimrBridge::TClusterState::SYNCHRONIZED);
        state->AddPerPileState(secondaryState);
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
        setup.Runtime.GetAppData().AdministrationAllowedSIDs = {"admin"};
        auto [rpc, future] = setup.StartGet({.UnbridgedStatus = true});
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
        UNIT_ASSERT(!result.has_unbridged_status());
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
        auto [rpc, future] = setup.StartGet({.Connectivity = false});
        const auto reply = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Bridge::GetClusterStateResult result;
        UNIT_ASSERT(reply.operation().result().UnpackTo(&result));
        UNIT_ASSERT(!result.has_pile_connectivity());
        UNIT_ASSERT(!result.has_unbridged_status());
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

Y_UNIT_TEST_SUITE(BridgeGetClusterStateUnbridgedStatus) {
    Y_UNIT_TEST(OnlyQueriesUnbridgedStatusWithoutLocalBridgeState) {
        TSetup setup;
        setup.Runtime.GetAppData().AdministrationAllowedSIDs = {"admin"};
        auto [rpc, future] = setup.StartGet({.Connectivity = false, .UnbridgedStatus = true});
        const auto reply = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), Ydb::StatusIds::SUCCESS);
        Ydb::Bridge::GetClusterStateResult result;
        UNIT_ASSERT(reply.operation().result().UnpackTo(&result));
        UNIT_ASSERT_VALUES_EQUAL(result.pile_states_size(), 2);
        UNIT_ASSERT(!result.has_unbridged_status());
        UNIT_ASSERT(!setup.GrabInvoke(rpc, TDuration::MilliSeconds(10)));
    }

    Y_UNIT_TEST(AbsentBridgeStateRequiresOptInAndAdministrator) {
        for (const bool include : {false, true}) {
            TSetup setup;
            setup.Runtime.GetAppData().AdministrationAllowedSIDs = {"admin"};
            auto [rpc, future] = setup.StartGet({.UnbridgedStatus = include, .Bridged = false});
            const auto reply = setup.Runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), include ? Ydb::StatusIds::UNAUTHORIZED : Ydb::StatusIds::NOT_FOUND);
            UNIT_ASSERT(!reply.operation().has_result());
            UNIT_ASSERT(!setup.GrabInvoke(rpc, TDuration::MilliSeconds(10)));
        }
    }

    Y_UNIT_TEST(ReturnsOnlyUnbridgedNodeReport) {
        for (const bool withNodes : {false, true}) {
            TSetup setup;
            auto [rpc, future] = setup.StartGet({.UnbridgedStatus = true, .Bridged = false});
            const auto invoke = setup.GrabInvoke(rpc);
            UNIT_ASSERT(invoke && invoke->HasQueryBridgeRollbackStatus());
            UNIT_ASSERT(!future.HasValue());
            auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
            response->Record.SetStatus(NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult::OK);
            auto* report = response->Record.MutableBridgeRollbackStatus();
            if (withNodes) {
                report->AddBridgeConfigNodeIds(3);
                report->AddUnreportedStaticNodeIds(5);
            }
            setup.Runtime.Send(new IEventHandle(rpc, setup.NodeWarden, response.release()), 0, true);

            const auto reply = setup.Runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL(reply.operation().status(), Ydb::StatusIds::SUCCESS);
            Ydb::Bridge::GetClusterStateResult result;
            UNIT_ASSERT(reply.operation().result().UnpackTo(&result));
            UNIT_ASSERT(result.has_unbridged_status());
            const auto& status = result.unbridged_status();
            UNIT_ASSERT_VALUES_EQUAL(status.bridge_config_node_ids_size(), withNodes ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(status.unreported_static_node_ids_size(), withNodes ? 1 : 0);
            if (withNodes) {
                UNIT_ASSERT_VALUES_EQUAL(status.bridge_config_node_ids(0), 3);
                UNIT_ASSERT_VALUES_EQUAL(status.unreported_static_node_ids(0), 5);
            }
            UNIT_ASSERT_VALUES_EQUAL(result.pile_states_size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(result.generation(), 0);
            UNIT_ASSERT(!result.has_pending_pile_operation());
            UNIT_ASSERT(!result.has_pile_connectivity());
        }
    }

    Y_UNIT_TEST(RejectsRacesAndUnavailableReports) {
        using TResult = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;
        struct TCase {
            TResult::EStatus RootStatus;
            bool RootHasBridgeState;
            Ydb::StatusIds::StatusCode Expected;
        };
        for (const TCase& test : {
            TCase{TResult::OK, true, Ydb::StatusIds::ABORTED},
            TCase{TResult::OK, false, Ydb::StatusIds::INTERNAL_ERROR},
            TCase{TResult::NO_QUORUM, false, Ydb::StatusIds::UNAVAILABLE},
        }) {
            TSetup setup;
            auto [rpc, future] = setup.StartGet({.UnbridgedStatus = true, .Bridged = false});
            const auto invoke = setup.GrabInvoke(rpc);
            UNIT_ASSERT(invoke && invoke->HasQueryBridgeRollbackStatus());
            auto response = std::make_unique<NStorage::TEvNodeConfigInvokeOnRootResult>();
            response->Record.SetStatus(test.RootStatus);
            if (test.RootHasBridgeState) {
                response->Record.MutableBridgeRollbackStatus()->SetHasClusterState(true);
            }
            if (test.RootStatus == TResult::NO_QUORUM) {
                response->Record.SetErrorReason("node reports unavailable");
            }
            setup.Runtime.Send(new IEventHandle(rpc, setup.NodeWarden, response.release()), 0, true);
            const auto reply = setup.Runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), test.Expected, reply.operation().DebugString());
            UNIT_ASSERT(!reply.operation().has_result());
            if (test.RootStatus == TResult::NO_QUORUM) {
                UNIT_ASSERT_STRING_CONTAINS(reply.operation().DebugString(), "node reports unavailable");
            }
        }
    }
}

namespace {

struct TStorageSetup : TSetup {
    TActorId Bsc = Runtime.AllocateEdgeActor();
    ui32 BscRequests = 0;
    bool ReverseMetadataResults = false;

    TStorageSetup() {
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->Type == TEvBlobStorage::TEvControllerQueryBridgeStorage::EventType &&
                    event->GetRecipientRewrite() != Bsc) {
                // Count the logical request before redirection, not its subsequent edge-mailbox observation.
                ++BscRequests;
                event->Rewrite(event->Type, Bsc);
            } else if (ReverseMetadataResults && event->Type == TEvTxProxySchemeCache::TEvNavigateKeySetResult::EventType) {
                auto& entries = event->Get<TEvTxProxySchemeCache::TEvNavigateKeySetResult>()->Request->ResultSet;
                std::reverse(entries.begin(), entries.end());
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
    }

    template<class TRequest, class TResponse>
    auto StartRpc(TRequest request, void (*handler)(std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&)) {
        using TCall = TGrpcRequestOperationCall<TRequest, TResponse>;
        NRpcService::SetRequestSyncOperationMode(request);
        auto promise = NThreading::NewPromise<typename TCall::TResponse>();
        using TCb = NRpcService::TPromiseWrapper<typename TCall::TResponse>;
        auto ctx = std::make_unique<NRpcService::TLocalRpcCtx<TCall, TCb>>(
            std::move(request), TCb(promise), "/Root", Nothing(), Nothing(), false);
        Runtime.Register(new TStartRequest(std::move(ctx), handler));
        return promise.GetFuture();
    }

    auto Start(const Ydb::Bridge::ListPileSynchronizationRequest& request) {
        return StartRpc<Ydb::Bridge::ListPileSynchronizationRequest, Ydb::Bridge::ListPileSynchronizationResponse>(request, DoListPileSynchronization);
    }

    auto Start(const Ydb::Bridge::ListDatabaseSynchronizationRequest& request) {
        return StartRpc<Ydb::Bridge::ListDatabaseSynchronizationRequest, Ydb::Bridge::ListDatabaseSynchronizationResponse>(request, DoListDatabaseSynchronization);
    }

    auto Start(const Ydb::Bridge::ListGroupSynchronizationRequest& request) {
        return StartRpc<Ydb::Bridge::ListGroupSynchronizationRequest, Ydb::Bridge::ListGroupSynchronizationResponse>(request, DoListGroupSynchronization);
    }

    auto GrabStorageQuery() {
        return Runtime.GrabEdgeEvent<TEvBlobStorage::TEvControllerQueryBridgeStorage>(Bsc);
    }

    void ReplyWithStorage(TActorId rpc, const NKikimrBridge::TStorageState& state) {
        auto response = std::make_unique<TEvBlobStorage::TEvControllerQueryBridgeStorageResult>();
        response->Record.CopyFrom(state);
        Runtime.Send(new IEventHandle(rpc, Bsc, response.release()), 0, true);
    }
};

struct TStorageMetadataSetup : TStorageSetup {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    static constexpr ui64 RootSchemeShardId = (1ull << 56) + 100;
    static constexpr ui64 TenantSchemeShardId = (1ull << 56) + 101;
    const TPathId RootId{RootSchemeShardId, 1};
    TActorId SchemeCache;

    TStorageMetadataSetup() {
        const TVector<ui64> tablets;
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            "Root", 0, RootSchemeShardId, 500, tablets, tablets, tablets);
        Runtime.GetAppData().DomainsInfo->AddDomain(domain.Get());
        auto config = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>();
        config->Roots.emplace_back(0, RootSchemeShardId, "Root");
        config->Counters = new ::NMonitoring::TDynamicCounters;
        SchemeCache = Runtime.Register(CreateSchemeBoardSchemeCache(config.Get()));
        Runtime.RegisterService(MakeSchemeCacheID(), SchemeCache);
        CacheDescription("/Root", RootId, NKikimrSchemeOp::EPathTypeDir, RootId, RootId);
    }

    // Feed actual SchemeCache notifications: TSS roots are SubDomain by path, while global GSS
    // domain IDs retain their ExtSubDomain descriptions, even after the TSS starts publishing.
    void CacheDescription(const TString& path, const TPathId& selfId, NKikimrSchemeOp::EPathType type,
            const TPathId& domainId, const TPathId& resourcesId, const TVector<TString>& pools = {}, bool byId = false) {
        auto request = MakeHolder<TNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = TNavigate::OpPath;
        entry.RedirectRequired = false;
        if (byId) {
            entry.TableId = selfId;
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
        } else {
            entry.Path = NKikimr::SplitPath(path);
        }
        const auto edge = Runtime.AllocateEdgeActor();
        Runtime.Send(new IEventHandle(SchemeCache, edge,
            new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);

        NKikimrScheme::TEvDescribeSchemeResult description;
        description.SetStatus(NKikimrScheme::StatusSuccess);
        description.SetPath(path);
        auto* self = description.MutablePathDescription()->MutableSelf();
        self->SetSchemeshardId(selfId.OwnerId);
        self->SetPathId(selfId.LocalPathId);
        const bool parentIsRoot = type == NKikimrSchemeOp::EPathTypeExtSubDomain ||
            (type == NKikimrSchemeOp::EPathTypeDir && path != "/Root");
        self->SetParentPathId(parentIsRoot ? 1 : selfId.LocalPathId);
        self->SetName(NKikimr::SplitPath(path).back());
        self->SetPathType(type);
        self->SetCreateFinished(true);
        auto* domain = description.MutablePathDescription()->MutableDomainDescription();
        domain->MutableDomainKey()->SetSchemeShard(domainId.OwnerId);
        domain->MutableDomainKey()->SetPathId(domainId.LocalPathId);
        domain->MutableResourcesDomainKey()->SetSchemeShard(resourcesId.OwnerId);
        domain->MutableResourcesDomainKey()->SetPathId(resourcesId.LocalPathId);
        for (const auto& pool : pools) {
            domain->AddStoragePools()->SetName(pool);
        }
        auto notification = byId
            ? std::make_unique<TSchemeBoardEvents::TEvNotifyUpdate>(selfId, std::move(description))
            : std::make_unique<TSchemeBoardEvents::TEvNotifyUpdate>(path, selfId, std::move(description));
        Runtime.Send(new IEventHandle(SchemeCache, edge, notification.release()), 0, true);
        const auto result = Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Request->ResultSet.front().Status, TNavigate::EStatus::Ok);
    }

    NKikimrBridge::TStorageState DatabaseSnapshot(const TPathId& owner, const TVector<TString>& pools) {
        NKikimrBridge::TStorageState state;
        state.SetStatus(Ydb::StatusIds::SUCCESS);
        auto* result = state.MutableDatabases();
        result->set_cluster_state_generation(7);
        result->mutable_observed_at()->set_seconds(123);
        result->mutable_observed_at()->set_nanos(456789000);
        auto* row = result->add_databases();
        for (size_t i = 0; i < pools.size(); ++i) {
            auto* pool = row->add_storage_pools();
            pool->mutable_id()->set_box_id(1);
            pool->mutable_id()->set_pool_id(i + 1);
            pool->set_name(pools[i]);
        }
        row->set_pile_name("secondary");
        row->set_storage_mode(NKikimrBridge::MIXED);
        row->mutable_groups()->set_required(8);
        row->mutable_groups()->set_synced(3);
        row->mutable_groups()->set_syncing(5);
        row->mutable_last_sync_error()->set_message("retry");
        row->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(99000);
        if (owner.OwnerId && owner.LocalPathId) {
            auto* identity = state.AddOwners();
            identity->SetRowIndex(0);
            identity->SetOwnerSchemeShardId(owner.OwnerId);
            identity->SetOwnerPathId(owner.LocalPathId);
        }
        return state;
    }
};

template<class TResult, class TResponse>
TResult OperationResult(const TResponse& response) {
    UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response.operation().DebugString());
    TResult result;
    UNIT_ASSERT(response.operation().result().UnpackTo(&result));
    return result;
}

Ydb::Bridge::ListDatabaseSynchronizationResult GetStorageResult(const Ydb::Bridge::ListDatabaseSynchronizationResponse& response) {
    return OperationResult<Ydb::Bridge::ListDatabaseSynchronizationResult>(response);
}

} // namespace

Y_UNIT_TEST_SUITE(BridgeStorageDiagnostics) {
    Y_UNIT_TEST(SelectedExternalDatabaseUsesActiveTenantRoot) {
        TStorageMetadataSetup setup;
        const TPathId owner(setup.RootSchemeShardId, 10);
        const TVector<TString> pools{"pool,one", "pool-two"};
        setup.CacheDescription("/Root/app", {setup.TenantSchemeShardId, 1},
            NKikimrSchemeOp::EPathTypeSubDomain, owner, owner, pools);

        Ydb::Bridge::ListDatabaseSynchronizationRequest query;
        query.set_database_path("/Root/app/");
        query.set_storage_mode(Ydb::Bridge::MIXED);
        auto future = setup.Start(query);
        const auto request = setup.GrabStorageQuery();
        const auto rpc = request->Sender;
        UNIT_ASSERT(request->Get()->Record.GetView() == NKikimrBridge::TStorageQuery::DATABASES);
        UNIT_ASSERT(request->Get()->Record.GetFilterPools());
        UNIT_ASSERT(request->Get()->Record.HasStorageMode());
        UNIT_ASSERT(request->Get()->Record.GetStorageMode() == NKikimrBridge::MIXED);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.PoolNamesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetPoolNames(0), pools[0]);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetPoolNames(1), pools[1]);
        setup.ReplyWithStorage(rpc, setup.DatabaseSnapshot({0, 0}, pools));

        const auto result = GetStorageResult(setup.Runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL(result.cluster_state_generation(), 7);
        const auto& storage = result;
        UNIT_ASSERT(storage.has_storage_owner_database_path());
        UNIT_ASSERT_VALUES_EQUAL(storage.storage_owner_database_path(), "/Root/app");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases_size(), 1);
        const auto& row = storage.databases(0);
        UNIT_ASSERT_VALUES_EQUAL(row.database_path(), "/Root/app");
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools(0).name(), pools[0]);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools(1).name(), pools[1]);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools(0).id().box_id(), 1);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools(0).id().pool_id(), 1);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pools(1).id().pool_id(), 2);
        UNIT_ASSERT(row.storage_mode() == Ydb::Bridge::MIXED);
        UNIT_ASSERT(row.has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(row.pile_name(), "secondary");
        UNIT_ASSERT_VALUES_EQUAL(row.groups().required(), 8);
        UNIT_ASSERT_VALUES_EQUAL(row.groups().synced(), 3);
        UNIT_ASSERT_VALUES_EQUAL(row.groups().syncing(), 5);
        UNIT_ASSERT_VALUES_EQUAL(row.last_sync_error().message(), "retry");
        UNIT_ASSERT_VALUES_EQUAL(row.last_sync_error().occurred_at().nanos(), 99000);
    }

    Y_UNIT_TEST(ServerlessDatabaseResolvesExternalSharedOwnerByGlobalId) {
        TStorageMetadataSetup setup;
        const TPathId serverless(setup.RootSchemeShardId, 10);
        const TPathId shared(setup.RootSchemeShardId, 20);
        const TVector<TString> pools{"shared,ssd", "shared-hdd"};
        setup.CacheDescription("/Root/serverless", {setup.TenantSchemeShardId, 1},
            NKikimrSchemeOp::EPathTypeSubDomain, serverless, shared);
        setup.CacheDescription("/Root/shared", {setup.TenantSchemeShardId + 1, 1},
            NKikimrSchemeOp::EPathTypeSubDomain, shared, shared, pools);
        setup.CacheDescription("/Root/shared", shared, NKikimrSchemeOp::EPathTypeExtSubDomain,
            shared, shared, pools, true);

        Ydb::Bridge::ListDatabaseSynchronizationRequest query;
        query.set_database_path("/Root/serverless");
        auto future = setup.Start(query);
        const auto request = setup.GrabStorageQuery();
        const auto rpc = request->Sender;
        UNIT_ASSERT(request->Get()->Record.GetFilterPools());
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.PoolNamesSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetPoolNames(0), pools[0]);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetPoolNames(1), pools[1]);
        setup.ReplyWithStorage(rpc, setup.DatabaseSnapshot({0, 0}, pools));

        const auto result = GetStorageResult(setup.Runtime.WaitFuture(future));
        const auto& storage = result;
        UNIT_ASSERT_VALUES_EQUAL(storage.storage_owner_database_path(), "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).database_path(), "/Root/shared");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).groups().required(), 8);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).groups().synced(), 3);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools(0).name(), pools[0]);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools(1).name(), pools[1]);
        UNIT_ASSERT_VALUES_EQUAL(setup.BscRequests, 1);
    }

    Y_UNIT_TEST(DatabaseStorageResolvesExternalOwnersByIdAndKeepsUnresolvedPools) {
        TStorageMetadataSetup setup;
        const TPathId first(setup.RootSchemeShardId, 10);
        const TPathId second(setup.RootSchemeShardId, 20);
        const TPathId directory(setup.RootSchemeShardId, 30);
        setup.CacheDescription("/Root/first", {setup.TenantSchemeShardId, 1},
            NKikimrSchemeOp::EPathTypeSubDomain, first, first, {"first,ssd", "first-hdd"});
        setup.CacheDescription("/Root/first", first, NKikimrSchemeOp::EPathTypeExtSubDomain,
            first, first, {"first,ssd", "first-hdd"}, true);
        setup.CacheDescription("/Root/second", {setup.TenantSchemeShardId + 1, 1},
            NKikimrSchemeOp::EPathTypeSubDomain, second, second, {"second-ssd"});
        setup.CacheDescription("/Root/second", second, NKikimrSchemeOp::EPathTypeExtSubDomain,
            second, second, {"second-ssd"}, true);
        setup.CacheDescription("/Root/dir", directory, NKikimrSchemeOp::EPathTypeDir,
            setup.RootId, setup.RootId, {}, true);
        setup.ReverseMetadataResults = true; // correlate owners by identity, independent of result order

        Ydb::Bridge::ListDatabaseSynchronizationRequest query;
        auto future = setup.Start(query);
        const auto request = setup.GrabStorageQuery();
        const auto rpc = request->Sender;
        auto snapshot = setup.DatabaseSnapshot(first, {"first,ssd", "first-hdd"});
        auto* rows = snapshot.MutableDatabases();
        const auto firstRow = rows->databases(0);
        rows->add_databases()->CopyFrom(firstRow);
        rows->mutable_databases(1)->set_pile_name("primary");
        auto* duplicate = snapshot.AddOwners();
        duplicate->SetRowIndex(1);
        duplicate->SetOwnerSchemeShardId(first.OwnerId);
        duplicate->SetOwnerPathId(first.LocalPathId);
        auto addOwner = [&](const TPathId& owner, const TString& pool) {
            const auto rowIndex = rows->databases_size();
            auto* row = rows->add_databases();
            row->CopyFrom(firstRow);
            row->clear_storage_pools();
            auto* storagePool = row->add_storage_pools();
            storagePool->mutable_id()->set_box_id(1);
            storagePool->mutable_id()->set_pool_id(rowIndex + 1);
            storagePool->set_name(pool);
            if (owner.OwnerId && owner.LocalPathId) {
                auto* identity = snapshot.AddOwners();
                identity->SetRowIndex(rowIndex);
                identity->SetOwnerSchemeShardId(owner.OwnerId);
                identity->SetOwnerPathId(owner.LocalPathId);
            }
            return row;
        };
        addOwner(second, "second-ssd");
        addOwner(directory, "directory-pool");
        addOwner({0, 0}, "unknown,scope")->clear_pile_name();
        setup.ReplyWithStorage(rpc, snapshot);

        const auto result = GetStorageResult(setup.Runtime.WaitFuture(future));
        const auto& storage = result;
        UNIT_ASSERT_VALUES_EQUAL(storage.cluster_state_generation(), 7);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases_size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).database_path(), "/Root/first");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(1).database_path(), "/Root/first");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(2).database_path(), "/Root/second");
        UNIT_ASSERT(!storage.databases(3).has_database_path());
        UNIT_ASSERT(!storage.databases(4).has_database_path());
        UNIT_ASSERT(!storage.databases(4).has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools(0).name(), "first,ssd");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(3).storage_pools(0).name(), "directory-pool");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(4).storage_pools(0).name(), "unknown,scope");
        for (const auto& row : storage.databases()) {
            UNIT_ASSERT_VALUES_EQUAL(row.groups().required(), 8);
            UNIT_ASSERT_VALUES_EQUAL(row.groups().synced(), 3);
        }
        UNIT_ASSERT_VALUES_EQUAL(setup.BscRequests, 1);
    }

    Y_UNIT_TEST(ControllerDisconnectDuringOwnerLookupKeepsAcceptedSnapshot) {
        TStorageMetadataSetup setup;
        const TPathId owner(setup.RootSchemeShardId, 10);
        const TVector<TString> pools{"pool,one", "pool-two"};
        setup.CacheDescription("/Root/app", {setup.TenantSchemeShardId, 1},
            NKikimrSchemeOp::EPathTypeSubDomain, owner, owner, pools);
        setup.CacheDescription("/Root/app", owner, NKikimrSchemeOp::EPathTypeExtSubDomain,
            owner, owner, pools, true);

        Ydb::Bridge::ListDatabaseSynchronizationRequest query;
        auto future = setup.Start(query);
        const auto request = setup.GrabStorageQuery();
        const auto rpc = request->Sender;
        const auto pipe = request->Recipient; // the fixture rewrites delivery while retaining the pipe address
        UNIT_ASSERT(pipe);

        TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr ownerReply;
        auto observer = setup.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == rpc) {
                ownerReply = ev.Release();
            }
        });
        setup.ReplyWithStorage(rpc, setup.DatabaseSnapshot(owner, pools));
        setup.Runtime.WaitFor("owner metadata reply after the accepted controller snapshot",
            [&] { return bool(ownerReply); }, TDuration::Seconds(1));
        observer.Remove();

        // The controller is no longer needed; owner metadata must still finish the accepted result.
        setup.Runtime.Send(new IEventHandle(rpc, pipe,
            new TEvTabletPipe::TEvClientDestroyed(MakeBSControllerID(), pipe, {})), 0, true);
        setup.Runtime.Send(ownerReply.Release(), 0, true);

        const auto result = GetStorageResult(setup.Runtime.WaitFuture(future));
        const auto& storage = result;
        UNIT_ASSERT_VALUES_EQUAL(storage.cluster_state_generation(), 7);
        UNIT_ASSERT_VALUES_EQUAL(storage.observed_at().seconds(), 123);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).database_path(), "/Root/app");
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).groups().required(), 8);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).groups().synced(), 3);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).groups().syncing(), 5);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools(0).name(), pools[0]);
        UNIT_ASSERT_VALUES_EQUAL(storage.databases(0).storage_pools(1).name(), pools[1]);
    }

    Y_UNIT_TEST(DefaultClusterStateDoesNotContactController) {
        TStorageSetup setup;
        auto [rpc, future] = setup.StartGet({.Connectivity = false});
        const auto result = OperationResult<Ydb::Bridge::GetClusterStateResult>(setup.Runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL(result.generation(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.pile_states_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(setup.BscRequests, 0);
    }

    Y_UNIT_TEST(InvalidStorageQueriesAreRejectedBeforeStorageLookup) {
        TStorageSetup setup;
        auto check = [&](const auto& request) {
            auto future = setup.Start(request);
            const auto response = setup.Runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::BAD_REQUEST, response.operation().DebugString());
        };
        Ydb::Bridge::ListDatabaseSynchronizationRequest emptyDatabase;
        emptyDatabase.set_database_path("");
        check(emptyDatabase);
        Ydb::Bridge::ListDatabaseSynchronizationRequest relativeDatabase;
        relativeDatabase.set_database_path("Root/app");
        check(relativeDatabase);
        Ydb::Bridge::ListGroupSynchronizationRequest emptyGroupDatabase;
        emptyGroupDatabase.set_database_path("");
        check(emptyGroupDatabase);
        Ydb::Bridge::ListGroupSynchronizationRequest emptyPool;
        emptyPool.set_storage_pool_name("");
        check(emptyPool);
        Ydb::Bridge::ListGroupSynchronizationRequest emptyPoolId;
        emptyPoolId.mutable_storage_pool_id()->set_box_id(1);
        check(emptyPoolId);
        for (const auto value : {0, 1000}) {
            Ydb::Bridge::ListGroupSynchronizationRequest unsupportedStatus;
            unsupportedStatus.set_storage_pool_name("pool");
            unsupportedStatus.add_statuses(static_cast<Ydb::Bridge::SynchronizationStatus>(value));
            check(unsupportedStatus);
        }
        UNIT_ASSERT_VALUES_EQUAL(setup.BscRequests, 0);
    }

    Y_UNIT_TEST(ControllerErrorsBecomeOrdinaryOperationStatuses) {
        for (const auto status : {Ydb::StatusIds::BAD_REQUEST, Ydb::StatusIds::NOT_FOUND,
                Ydb::StatusIds::PRECONDITION_FAILED, Ydb::StatusIds::UNAVAILABLE}) {
            TStorageSetup setup;
            auto future = setup.Start(Ydb::Bridge::ListPileSynchronizationRequest{});
            const auto request = setup.GrabStorageQuery();
            UNIT_ASSERT(request->Get()->Record.GetView() == NKikimrBridge::TStorageQuery::PILES);
            NKikimrBridge::TStorageState state;
            state.SetStatus(status);
            state.SetErrorMessage("controller diagnostic error");
            setup.ReplyWithStorage(request->Sender, state);
            const auto response = setup.Runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), status, response.operation().DebugString());
            UNIT_ASSERT_VALUES_EQUAL(response.operation().issues_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response.operation().issues(0).message(), "controller diagnostic error");
            UNIT_ASSERT(!response.operation().has_result());
        }
    }

    Y_UNIT_TEST(PileDatabaseCountsKeepObservationIdentityAndPlacementPresence) {
        TStorageSetup setup;
        auto future = setup.Start(Ydb::Bridge::ListPileSynchronizationRequest{});
        const auto request = setup.GrabStorageQuery();
        NKikimrBridge::TStorageState state;
        state.SetStatus(Ydb::StatusIds::SUCCESS);
        auto* summary = state.MutablePiles();
        summary->set_cluster_state_generation(7);
        summary->mutable_observed_at()->set_seconds(123);
        summary->mutable_observed_at()->set_nanos(456789000);
        summary->set_unattributed_storage_pools(4);
        auto* pile = summary->add_piles();
        pile->set_pile_name("");
        auto* counts = pile->mutable_databases();
        counts->set_total(120);
        counts->set_synced(80);
        counts->set_syncing(10);
        counts->set_pending(8);
        counts->set_unknown(2);
        counts->set_not_required(20);
        auto* unplaced = summary->add_piles();
        unplaced->mutable_databases()->set_total(1);
        unplaced->mutable_databases()->set_not_required(1);
        setup.ReplyWithStorage(request->Sender, state);
        const auto result = OperationResult<Ydb::Bridge::ListPileSynchronizationResult>(setup.Runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL(result.cluster_state_generation(), 7);
        UNIT_ASSERT_VALUES_EQUAL(result.observed_at().seconds(), 123);
        UNIT_ASSERT_VALUES_EQUAL(result.observed_at().nanos(), 456789000);
        UNIT_ASSERT_VALUES_EQUAL(result.piles_size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result.unattributed_storage_pools(), 4);
        const auto& row = result.piles(0);
        UNIT_ASSERT(row.has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(row.pile_name(), "");
        UNIT_ASSERT_VALUES_EQUAL(row.databases().total(), 120);
        UNIT_ASSERT_VALUES_EQUAL(row.databases().synced(), 80);
        UNIT_ASSERT_VALUES_EQUAL(row.databases().syncing(), 10);
        UNIT_ASSERT_VALUES_EQUAL(row.databases().pending(), 8);
        UNIT_ASSERT_VALUES_EQUAL(row.databases().unknown(), 2);
        UNIT_ASSERT_VALUES_EQUAL(row.databases().not_required(), 20);
        UNIT_ASSERT(!result.piles(1).has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(result.piles(1).databases().total(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.piles(1).databases().not_required(), 1);
    }

    Y_UNIT_TEST(AllGroupScopePreservesMissingPoolAndZeroGroupId) {
        TStorageSetup setup;
        Ydb::Bridge::ListGroupSynchronizationRequest query;
        query.set_pile_name("");
        query.add_statuses(Ydb::Bridge::SYNCED);
        auto future = setup.Start(query);
        const auto request = setup.GrabStorageQuery();
        const auto& forwarded = request->Get()->Record;
        UNIT_ASSERT(forwarded.GetView() == NKikimrBridge::TStorageQuery::GROUPS);
        UNIT_ASSERT(!forwarded.HasStoragePool());
        UNIT_ASSERT(!forwarded.HasStoragePoolId());
        UNIT_ASSERT(!forwarded.GetFilterPools());
        UNIT_ASSERT(forwarded.HasPileName());
        UNIT_ASSERT(forwarded.GetPileName().empty());
        UNIT_ASSERT_VALUES_EQUAL(forwarded.StatusesSize(), 1);
        UNIT_ASSERT(forwarded.GetStatuses(0) == NKikimrBridge::SYNCED);
        NKikimrBridge::TStorageState state;
        state.SetStatus(Ydb::StatusIds::SUCCESS);
        auto* group = state.MutableGroups()->add_groups();
        group->set_group_id(0);
        group->set_physical_group_id(1);
        group->set_pile_name("");
        group->set_sync_status(NKikimrBridge::SYNCED);
        setup.ReplyWithStorage(request->Sender, state);
        const auto result = OperationResult<Ydb::Bridge::ListGroupSynchronizationResult>(setup.Runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL(result.groups_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.groups(0).group_id(), 0);
        UNIT_ASSERT(result.groups(0).has_physical_group_id());
        UNIT_ASSERT_VALUES_EQUAL(result.groups(0).physical_group_id(), 1);
        UNIT_ASSERT(result.groups(0).has_pile_name());
        UNIT_ASSERT(result.groups(0).pile_name().empty());
        UNIT_ASSERT(!result.groups(0).has_storage_pool());
        UNIT_ASSERT(result.groups(0).sync_status() == Ydb::Bridge::SYNCED);
    }

    Y_UNIT_TEST(ControllerObservationDoesNotRequireLocalClusterState) {
        TStorageSetup setup;
        auto future = setup.Start(Ydb::Bridge::ListPileSynchronizationRequest{});
        const auto request = setup.GrabStorageQuery();
        NKikimrBridge::TStorageState state;
        state.SetStatus(Ydb::StatusIds::SUCCESS);
        state.MutablePiles()->set_cluster_state_generation(42);
        setup.ReplyWithStorage(request->Sender, state);
        const auto result = OperationResult<Ydb::Bridge::ListPileSynchronizationResult>(setup.Runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL(result.cluster_state_generation(), 42);
        UNIT_ASSERT_VALUES_EQUAL(setup.BscRequests, 1);
    }

    Y_UNIT_TEST(GroupScopeAndStatusFiltersReachController) {
        TStorageSetup setup;
        Ydb::Bridge::ListGroupSynchronizationRequest query;
        query.mutable_storage_pool_id()->set_box_id(0);
        query.mutable_storage_pool_id()->set_pool_id(25);
        query.set_pile_name("");
        query.add_statuses(Ydb::Bridge::SYNCING);
        query.add_statuses(Ydb::Bridge::PENDING);
        query.add_statuses(Ydb::Bridge::NOT_REQUIRED);
        auto future = setup.Start(query);
        const auto request = setup.GrabStorageQuery();
        const auto& forwarded = request->Get()->Record;
        UNIT_ASSERT(forwarded.GetView() == NKikimrBridge::TStorageQuery::GROUPS);
        UNIT_ASSERT(forwarded.HasStoragePoolId());
        UNIT_ASSERT_VALUES_EQUAL(forwarded.GetStoragePoolId().box_id(), 0);
        UNIT_ASSERT_VALUES_EQUAL(forwarded.GetStoragePoolId().pool_id(), 25);
        UNIT_ASSERT(forwarded.HasPileName());
        UNIT_ASSERT_VALUES_EQUAL(forwarded.GetPileName(), "");
        UNIT_ASSERT_VALUES_EQUAL(forwarded.StatusesSize(), 3);
        UNIT_ASSERT(forwarded.GetStatuses(0) == NKikimrBridge::SYNCING);
        UNIT_ASSERT(forwarded.GetStatuses(1) == NKikimrBridge::PENDING);
        UNIT_ASSERT(forwarded.GetStatuses(2) == NKikimrBridge::NOT_REQUIRED);
        NKikimrBridge::TStorageState state;
        state.SetStatus(Ydb::StatusIds::SUCCESS);
        auto* groups = state.MutableGroups();
        groups->set_cluster_state_generation(7);
        groups->mutable_observed_at()->set_seconds(1);
        auto* group = groups->add_groups();
        group->set_group_id(10);
        group->set_physical_group_id(20);
        group->mutable_storage_pool()->mutable_id()->set_box_id(0);
        group->mutable_storage_pool()->mutable_id()->set_pool_id(25);
        group->mutable_storage_pool()->set_name("renamed-pool");
        group->set_pile_name("");
        group->set_sync_status(NKikimrBridge::SYNCING);
        group->set_sync_phase("WRITE_KEEP");
        group->mutable_last_sync_error()->set_message("previous attempt");
        group->mutable_last_sync_error()->mutable_occurred_at()->set_nanos(99000);
        auto* pending = groups->add_groups();
        pending->set_group_id(11);
        pending->set_sync_status(NKikimrBridge::PENDING);
        pending->mutable_last_sync_error()->set_message("error without a timestamp");
        auto* ordinary = groups->add_groups();
        ordinary->set_group_id(12);
        ordinary->set_sync_status(NKikimrBridge::NOT_REQUIRED);
        setup.ReplyWithStorage(request->Sender, state);
        const auto result = OperationResult<Ydb::Bridge::ListGroupSynchronizationResult>(setup.Runtime.WaitFuture(future));
        UNIT_ASSERT_VALUES_EQUAL(result.groups_size(), 3);
        const auto& row = result.groups(0);
        UNIT_ASSERT_VALUES_EQUAL(row.group_id(), 10);
        UNIT_ASSERT(row.has_physical_group_id());
        UNIT_ASSERT_VALUES_EQUAL(row.physical_group_id(), 20);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pool().id().box_id(), 0);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pool().id().pool_id(), 25);
        UNIT_ASSERT_VALUES_EQUAL(row.storage_pool().name(), "renamed-pool");
        UNIT_ASSERT(row.has_pile_name());
        UNIT_ASSERT_VALUES_EQUAL(row.pile_name(), "");
        UNIT_ASSERT(row.sync_status() == Ydb::Bridge::SYNCING);
        UNIT_ASSERT_VALUES_EQUAL(row.sync_phase(), "WRITE_KEEP");
        UNIT_ASSERT(row.has_last_sync_error());
        UNIT_ASSERT_VALUES_EQUAL(row.last_sync_error().message(), "previous attempt");
        UNIT_ASSERT_VALUES_EQUAL(row.last_sync_error().occurred_at().nanos(), 99000);
        UNIT_ASSERT(!result.groups(1).has_physical_group_id());
        UNIT_ASSERT(!result.groups(1).has_pile_name());
        UNIT_ASSERT(result.groups(1).sync_status() == Ydb::Bridge::PENDING);
        UNIT_ASSERT(result.groups(1).has_last_sync_error());
        UNIT_ASSERT_VALUES_EQUAL(result.groups(1).last_sync_error().message(), "error without a timestamp");
        UNIT_ASSERT(!result.groups(1).last_sync_error().has_occurred_at());
        UNIT_ASSERT(result.groups(2).sync_status() == Ydb::Bridge::NOT_REQUIRED);
        UNIT_ASSERT(!result.groups(2).has_last_sync_error());
    }

    Y_UNIT_TEST(ResponseBudgetIsCheckedAfterOwnerMetadataEnrichment) {
        TStorageMetadataSetup setup;
        const TPathId owner(setup.RootSchemeShardId, 10);
        const TString path = "/Root/" + TString(200, 'a');
        setup.CacheDescription(path, owner, NKikimrSchemeOp::EPathTypeExtSubDomain,
            owner, owner, {"pool"}, true);
        auto state = setup.DatabaseSnapshot(owner, {"pool"});
        auto* data = state.MutableDatabases();
        auto* message = data->mutable_databases(0)->mutable_last_sync_error()->mutable_message();
        constexpr auto budget = TEvBlobStorage::TEvControllerQueryBridgeStorage::MaxResultBytes;
        message->assign(budget - 1024, 'x');
        const auto initial = data->ByteSizeLong();
        UNIT_ASSERT(initial < budget - 128);
        message->append(budget - 128 - initial, 'x');
        UNIT_ASSERT(data->ByteSizeLong() <= budget);
        UNIT_ASSERT(state.ByteSizeLong() <= budget);

        auto future = setup.Start(Ydb::Bridge::ListDatabaseSynchronizationRequest{});
        const auto request = setup.GrabStorageQuery();
        setup.ReplyWithStorage(request->Sender, state);
        const auto response = setup.Runtime.WaitFuture(future);
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::PRECONDITION_FAILED,
            response.operation().DebugString());
        UNIT_ASSERT(!response.operation().has_result());
        UNIT_ASSERT_STRING_CONTAINS(response.operation().issues(0).message(), "narrow");
    }

    Y_UNIT_TEST(StorageQueriesRequireAdministratorPrivileges) {
        TStorageSetup setup;
        setup.Runtime.GetAppData().AdministrationAllowedSIDs = {"admin"};
        auto check = [&](const auto& query) {
            auto future = setup.Start(query);
            const auto response = setup.Runtime.WaitFuture(future);
            UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::UNAUTHORIZED, response.operation().DebugString());
            UNIT_ASSERT(!response.operation().has_result());
        };
        check(Ydb::Bridge::ListPileSynchronizationRequest{});
        check(Ydb::Bridge::ListDatabaseSynchronizationRequest{});
        Ydb::Bridge::ListGroupSynchronizationRequest groups;
        groups.set_storage_pool_name("pool");
        check(groups);
        UNIT_ASSERT_VALUES_EQUAL(setup.BscRequests, 0);
    }
}

Y_UNIT_TEST_SUITE(BridgeUpdateClusterState) {
    using EInvokeStatus = NKikimrBlobStorage::TEvNodeConfigInvokeOnRootResult;

    // The request is rejected by the receiving node: nothing is sent to distconf and no server is held.
    void TestRejectedLocally(TUpdateOptions options, Ydb::StatusIds::StatusCode expected) {
        TSetup setup;
        auto [rpc, future] = setup.Start(options);
        const auto invoke = setup.GrabInvoke(rpc, TDuration::MilliSeconds(10));
        UNIT_ASSERT_C(!invoke, "rejected request must not reach distconf: "
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
            .SecondaryState = NKikimrBridge::TClusterState::SUSPENDED,
            .RequestedState = Ydb::Bridge::PileState::SYNCHRONIZED}, Ydb::StatusIds::ABORTED);
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
        const bool guarded = options.RequestedState == Ydb::Bridge::PileState::DISCONNECTED ||
            options.RequestedState == Ydb::Bridge::PileState::NOT_SYNCHRONIZED ||
            options.RequestedState == Ydb::Bridge::PileState::SUSPENDED;
        if (options.PublishConfigBeforeReply) {
            UNIT_ASSERT(guarded);
            setup.Runtime.WaitFor("disconnect request start", [&] { return setup.Started == 1; },
                TDuration::Seconds(1));
            setup.PublishConfig(invoke->GetSwitchBridgeClusterState().GetNewClusterState().GetPerPileState(1));
            UNIT_ASSERT(!future.HasValue());
        }
        setup.ReplyToInvoke(rpc, status, errorReason);
        auto reply = setup.Runtime.WaitFuture(future);
        if (guarded) {
            setup.Runtime.WaitFor("disconnect request completion", [&] { return setup.Finished == 1; },
                TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(setup.Started, guarded ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(setup.Finished, guarded ? 1 : 0);
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

    Y_UNIT_TEST(SuspendedPileCanBeDisconnected) {
        const auto reply = TestForwarded({.SecondaryState = NKikimrBridge::TClusterState::SUSPENDED},
            EInvokeStatus::OK, {}, [](const auto& cmd) {
                UNIT_ASSERT(cmd.GetNewClusterState().GetPerPileState(1) ==
                    NKikimrBridge::TClusterState::DISCONNECTED);
            });
        UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), Ydb::StatusIds::SUCCESS,
            reply.operation().DebugString());
    }

    Y_UNIT_TEST(SuspendedPileRejoinIsGuardedDuringConfigPublication) {
        const auto reply = TestForwarded({.SecondaryState = NKikimrBridge::TClusterState::SUSPENDED,
                .RequestedState = Ydb::Bridge::PileState::NOT_SYNCHRONIZED,
                .PublishConfigBeforeReply = true}, EInvokeStatus::OK, {},
            [](const auto& cmd) {
                UNIT_ASSERT(cmd.GetNewClusterState().GetPerPileState(1) ==
                    NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1);
            });
        UNIT_ASSERT_VALUES_EQUAL_C(reply.operation().status(), Ydb::StatusIds::SUCCESS,
            reply.operation().DebugString());
    }

    Y_UNIT_TEST(SuspendedPileRejectsOtherUpdates) {
        for (const auto state : {Ydb::Bridge::PileState::PRIMARY, Ydb::Bridge::PileState::PROMOTED,
                Ydb::Bridge::PileState::SYNCHRONIZED, Ydb::Bridge::PileState::SUSPENDED,
                Ydb::Bridge::PileState::INACTIVE}) {
            TestRejectedLocally({.SecondaryState = NKikimrBridge::TClusterState::SUSPENDED,
                .RequestedState = state}, Ydb::StatusIds::BAD_REQUEST);
        }
    }
}

}
