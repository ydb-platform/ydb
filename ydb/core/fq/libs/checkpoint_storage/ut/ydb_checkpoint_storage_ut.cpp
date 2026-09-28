#include <ydb/core/fq/libs/checkpoint_storage/ydb_checkpoint_storage.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <deque>
#include <functional>
#include <memory>

namespace NFq {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TCheckpointId CheckpointId1(11, 3);
const TCheckpointId CheckpointId2(12, 1);
const TCheckpointId CheckpointId3(12, 4);
const TCheckpointId CheckpointId4(13, 2);

////////////////////////////////////////////////////////////////////////////////

class TTestCheckpointProviderIntegration : public NFq::ICheckpointProviderIntegration {
public:
    explicit TTestCheckpointProviderIntegration(TString sinkName)
        : SinkName(std::move(sinkName))
    {}

    const TString SinkName;
    TVector<ui64> CleanedTasks;
    TVector<std::optional<ui64>> CleanupGenerationBounds;
    NYql::TIssues CleanupIssues;
    size_t CleanupCalls = 0;
    std::function<NThreading::TFuture<NYql::TIssues>(const TVector<TCleanupGraphSink>&, std::optional<ui64>)> CleanupHandler;

    TStringBuf GetSinkName() const override {
        return SinkName;
    }

    NThreading::TFuture<NYql::TIssues> CleanupGraphSinks(TVector<TCleanupGraphSink>&& sinks, std::optional<ui64> generationUpperBound) override {
        ++CleanupCalls;
        CleanupGenerationBounds.push_back(generationUpperBound);
        for (const auto& sink : sinks) {
            UNIT_ASSERT_VALUES_EQUAL(sink.Sink.GetType(), GetSinkName());
            CleanedTasks.insert(CleanedTasks.end(), sink.Args.TaskIds.begin(), sink.Args.TaskIds.end());
        }
        if (CleanupHandler) {
            return CleanupHandler(sinks, generationUpperBound);
        }
        return NThreading::MakeFuture(CleanupIssues);
    }
};

template<bool UseYdbSdk>
class TFixture : public NUnitTest::TBaseFixture/*, public NActors::TTestActorRuntime*/ {
public:
    TCheckpointStoragePtr Storage;
    IYdbConnection::TPtr Connection;
    const TIntrusivePtr<TTestCheckpointProviderIntegration> Integration = MakeIntrusive<TTestCheckpointProviderIntegration>("test");
    const TIntrusivePtr<TTestCheckpointProviderIntegration> OtherIntegration = MakeIntrusive<TTestCheckpointProviderIntegration>("other");

public:
    void SetUp(NUnitTest::TTestContext& /* context */) override {
        InitCheckpointStorage();
    }

    void InitCheckpointStorage(IEntityIdGenerator::TPtr entityIdGenerator = CreateEntityIdGenerator("id")) {
        NConfig::TYdbStorageConfig checkpointStorageConfig;
        checkpointStorageConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
        checkpointStorageConfig.SetDatabase(GetEnv("YDB_DATABASE"));
        checkpointStorageConfig.SetToken("");
        checkpointStorageConfig.SetTablePrefix(CreateGuidAsString());
        checkpointStorageConfig.SetTableClientMaxActiveSessions(20);

        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        NYdb::TDriver driver(NYdb::TDriverConfig{});
        Connection = CreateSdkYdbConnection(checkpointStorageConfig, credFactory, driver);
        Storage = NewYdbCheckpointStorage(checkpointStorageConfig, entityIdGenerator, Connection,
            {{"test-provider", Integration}, {"other-provider", OtherIntegration}});

        auto issues = Storage->Init({}).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
    }

    ui64 CountGraphDescriptions() {
        ui64 count = 0;
        auto status = Connection->GetTableClient()->RetryOperation([&](ISession::TPtr session) {
            return session->ExecuteDataQuery(
                TStringBuilder() << "PRAGMA TablePathPrefix(\"" << Connection->GetTablePathPrefix() << "\"); SELECT COUNT(*) FROM checkpoints_graphs_description;",
                ISession::TTxControl::BeginAndCommitTx(), {}).Apply([&](const auto& future) {
                    const auto& result = future.GetValue();
                    if (result.IsSuccess()) {
                        auto parser = result.GetResultSetParser(0);
                        UNIT_ASSERT(parser.TryNextRow());
                        count = parser.ColumnParser(0).GetUint64();
                    }
                    return NYdb::TStatus(result);
                });
        }).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        return count;
    }

    void CheckInvalidGraphDescription(bool nullDescription, bool gc) {
        CreateSome();
        auto status = Connection->GetTableClient()->RetryOperation([&](ISession::TPtr session) {
            const TString query = TStringBuilder()
                << "PRAGMA TablePathPrefix(\"" << Connection->GetTablePathPrefix() << "\");"
                << "UPDATE checkpoints_graphs_description SET graph_description = "
                << (nullDescription ? "NULL" : "\"invalid protobuf\"") << ';';
            return session->ExecuteDataQuery(query, ISession::TTxControl::BeginAndCommitTx(), {}).Apply([](const auto& future) {
                return NYdb::TStatus(future.GetValue());
            });
        }).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());

        if (gc) {
            const auto issues = Storage->MarkCheckpointsGC("graph1", CheckpointId4).GetValueSync();
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        }
        const auto issues = gc ? Storage->DeleteMarkedCheckpoints("graph1", CheckpointId4).GetValueSync()
                               : Storage->DeleteGraph("graph1").GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), gc ? 2 : 1);
        const auto [checkpoints, getIssues] = Storage->GetCheckpoints("graph1", {}, 10, false).GetValueSync();
        UNIT_ASSERT_C(getIssues.Empty(), getIssues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), gc ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(Integration->CleanupCalls, 0);
        UNIT_ASSERT_VALUES_EQUAL(OtherIntegration->CleanupCalls, 0);
    }

    void CheckCleanupFailure(bool gc, bool synchronousException) {
        CreateSome();
        if (gc) {
            const auto issues = Storage->MarkCheckpointsGC("graph1", CheckpointId4).GetValueSync();
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        }
        if (synchronousException) {
            Integration->CleanupHandler = [](const auto&, auto) -> NThreading::TFuture<NYql::TIssues> {
                ythrow yexception() << "Test synchronous cleanup exception";
            };
        } else {
            Integration->CleanupIssues.AddIssue("Test cleanup failure");
        }

        const auto deleteGraphs = [&] {
            return gc ? Storage->DeleteMarkedCheckpoints("graph1", CheckpointId4).GetValueSync()
                      : Storage->DeleteGraph("graph1").GetValueSync();
        };
        const auto issues = deleteGraphs();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(Integration->CleanupCalls);
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), gc ? 2 : 1);
        const auto [checkpoints, getIssues] = Storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_C(getIssues.Empty(), getIssues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(checkpoints.size(), gc ? 1 : 0);
        const auto [coordinators, coordinatorIssues] = Storage->GetCoordinators().GetValueSync();
        UNIT_ASSERT_C(coordinatorIssues.Empty(), coordinatorIssues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(coordinators.size(), gc ? 2 : 1);
        if (!gc) {
            UNIT_ASSERT_VALUES_EQUAL(coordinators.front().GraphId, "graph2");
        }
        UNIT_ASSERT_VALUES_EQUAL(Storage->GetCheckpoints("graph2").GetValueSync().first.size(), 1);

        const auto retryIssues = deleteGraphs();
        UNIT_ASSERT_C(retryIssues.Empty(), retryIssues.ToString());
    }

    void CheckParallelSinkCleanup(bool gc, bool fail, bool throwException = true) {
        const TCoordinatorId coordinator("parallel", 3);
        auto issues = Storage->RegisterGraphCoordinator(coordinator).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        NProto::TCheckpointGraphDescription desc;
        desc.MutableGraph()->SetGraphId(coordinator.GraphId);
        for (ui64 taskId : {1, 2, 3, 4}) {
            auto* task = desc.MutableGraph()->AddTasks();
            task->SetId(taskId);
            const auto stageId = (taskId - 1) / 2 + 1;
            task->SetStageId(stageId);
            (*task->MutableSecureParams())["auth-ref"] = "secret-reference";
            (*task->MutableRequestContext())["Database"] = "database";
            task->AddOutputs(); // A non-sink output must not create a cleanup request.
            for (const auto& type : {"test", "other"}) {
                auto* sink = task->AddOutputs()->MutableSink();
                sink->SetType(type);
                sink->MutableSettings()->set_type_url("test-settings");
                sink->MutableSettings()->set_value(ToString(stageId));
            }
            task->AddOutputs()->MutableSink()->SetType("unsupported");
        }
        auto result = Storage->CreateCheckpoint(coordinator, TCheckpointId(3, 1), desc, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(result.second.Empty(), result.second.ToString());

        const TCheckpointId bound(3, 2);
        desc.MutableGraph()->ClearTasks();
        result = Storage->CreateCheckpoint(coordinator, bound, desc, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(result.second.Empty(), result.second.ToString());
        if (gc) {
            issues = Storage->MarkCheckpointsGC(coordinator.GraphId, bound).GetValueSync();
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        }

        struct TCleanupState {
            TVector<NThreading::TPromise<NYql::TIssues>> Requests;
            NThreading::TPromise<void> AllStarted = NThreading::NewPromise();
        };
        auto state = std::make_shared<TCleanupState>();
        auto cleanup = [state, gc](const TVector<ICheckpointProviderIntegration::TCleanupGraphSink>& sinks, std::optional<ui64> generationUpperBound) {
            UNIT_ASSERT_VALUES_EQUAL(sinks.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(generationUpperBound.has_value(), gc);
            if (gc) {
                UNIT_ASSERT_VALUES_EQUAL(*generationUpperBound, 3);
            }
            THashSet<TString> stages;
            for (const auto& [sink, args] : sinks) {
                const auto stageId = FromString<ui64>(sink.GetSettings().value());
                UNIT_ASSERT(stageId == 1 || stageId == 2);
                UNIT_ASSERT(stages.insert(sink.GetSettings().value()).second);
                UNIT_ASSERT_VALUES_EQUAL(args.TaskIds, (TVector<ui64>{2 * stageId - 1, 2 * stageId}));
                UNIT_ASSERT(args.OutputIndex == 1 || args.OutputIndex == 2);
                UNIT_ASSERT_VALUES_EQUAL(sink.GetType(), args.OutputIndex == 1 ? "test" : "other");
                UNIT_ASSERT_VALUES_EQUAL(sink.GetSettings().type_url(), "test-settings");
                UNIT_ASSERT_VALUES_EQUAL(args.SecureParams.at("auth-ref"), "secret-reference");
                UNIT_ASSERT_VALUES_EQUAL(args.RequestContext.at("Database"), "database");
            }
            auto promise = NThreading::NewPromise<NYql::TIssues>();
            state->Requests.push_back(promise);
            if (state->Requests.size() == 2) {
                state->AllStarted.SetValue();
            }
            return promise.GetFuture();
        };
        Integration->CleanupHandler = cleanup;
        OtherIntegration->CleanupHandler = cleanup;

        auto deletion = gc ? Storage->DeleteMarkedCheckpoints(coordinator.GraphId, bound) : Storage->DeleteGraph(coordinator.GraphId);
        UNIT_ASSERT_C(state->AllStarted.GetFuture().Wait(TDuration::Seconds(10)), "All sinks must start before any cleanup finishes");
        for (const auto& integration : {Integration, OtherIntegration}) {
            auto tasks = integration->CleanedTasks;
            Sort(tasks);
            UNIT_ASSERT_VALUES_EQUAL(tasks, (TVector<ui64>{1, 2, 3, 4}));
            UNIT_ASSERT_VALUES_EQUAL(integration->CleanupCalls, 1);
        }
        UNIT_ASSERT(!deletion.HasValue() && !deletion.HasException());
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 2);

        for (size_t i = 0; i + 1 < state->Requests.size(); ++i) {
            if (fail && i == 0) {
                if (throwException) {
                    state->Requests[i].SetException("Test async sink cleanup failure");
                } else {
                    state->Requests[i].SetValue({NYql::TIssue("Test first sink cleanup failure")});
                }
            } else {
                state->Requests[i].SetValue({});
            }
        }
        UNIT_ASSERT_C(!deletion.Wait(TDuration::MilliSeconds(100)), "Deletion must wait for the last sink even after another sink fails");
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 2);
        state->Requests.back().SetValue(fail ? NYql::TIssues{NYql::TIssue("Test second sink cleanup failure")} : NYql::TIssues{});
        issues = deletion.GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), gc ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(Storage->GetCheckpoints(coordinator.GraphId).GetValueSync().first.size(), gc ? 1 : 0);
    }

    void CreateSome() {
        // coordinator1 registers and performs some work

        TCoordinatorId coordinator1("graph1", 11);
        auto issues = Storage->RegisterGraphCoordinator(coordinator1).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        NProto::TCheckpointGraphDescription desc;
        desc.MutableGraph()->SetGraphId("graph1");
        desc.MutableGraph()->ClearTasks();
        auto* task = desc.MutableGraph()->AddTasks();
        task->SetId(1);
        task->AddOutputs()->MutableSink()->SetType("test");
        task->AddOutputs()->MutableSink()->SetType("other");
        auto createCheckpointResult = Storage->CreateCheckpoint(coordinator1, CheckpointId1, desc, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint1GraphDescId = createCheckpointResult.first;

        createCheckpointResult = Storage->CreateCheckpoint(coordinator1, CheckpointId2, createCheckpointResult.first, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, createCheckpointResult.first);

        createCheckpointResult = Storage->CreateCheckpoint(coordinator1, CheckpointId3, createCheckpointResult.first, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, createCheckpointResult.first);

        // coordinator2

        TCoordinatorId coordinator2("graph2", 17);
        issues = Storage->RegisterGraphCoordinator(coordinator2).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        desc.MutableGraph()->SetGraphId("graph2");
        desc.MutableGraph()->MutableTasks(0)->SetId(2);
        createCheckpointResult = Storage->CreateCheckpoint(coordinator2, CheckpointId1, desc, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint2GraphDescId = createCheckpointResult.first;
        UNIT_ASSERT_UNEQUAL(checkpoint1GraphDescId, checkpoint2GraphDescId);

        // new coordinator for graph1

        TCoordinatorId coordinator1v2("graph1", 18);
        issues = Storage->RegisterGraphCoordinator(coordinator1v2).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        desc.MutableGraph()->SetGraphId("graph1");
        desc.MutableGraph()->ClearTasks();
        task = desc.MutableGraph()->AddTasks();
        task->SetId(1);
        task->AddOutputs()->MutableSink()->SetType("test");
        task->AddOutputs()->MutableSink()->SetType("other");
        createCheckpointResult = Storage->CreateCheckpoint(coordinator1v2, CheckpointId4, desc, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint3GraphDescId = createCheckpointResult.first;
        UNIT_ASSERT_UNEQUAL(checkpoint1GraphDescId, checkpoint3GraphDescId);
        UNIT_ASSERT_UNEQUAL(checkpoint2GraphDescId, checkpoint3GraphDescId);
    }
};

using TSdkCheckpoints = TFixture<true>;

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Note that many scenarious are tested in storage_service_ydb_ut.cpp

Y_UNIT_TEST_SUITE(TCheckpointStorageTest) {
    Y_UNIT_TEST_F(ShouldRegisterCoordinator, TSdkCheckpoints)
    {
        TCoordinatorId coordinator("graph1", 11);
        auto issues = Storage->RegisterGraphCoordinator(coordinator).GetValueSync();
        UNIT_ASSERT(issues.Empty());
    }

    Y_UNIT_TEST_F(ShouldGetCoordinators, TSdkCheckpoints)
    {
        TCoordinatorId coordinator1("graph1", 11);
        auto issues = Storage->RegisterGraphCoordinator(coordinator1).GetValueSync();

        TCoordinatorId coordinator2("graph2", 17);
        issues = Storage->RegisterGraphCoordinator(coordinator2).GetValueSync();

        auto getResult = Storage->GetCoordinators().GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 2UL);

        for (const auto& coordinator: getResult.first) {
            if (coordinator.GraphId == "graph1") {
                UNIT_ASSERT_VALUES_EQUAL(coordinator.Generation, 11);
            } else if (coordinator.GraphId == "graph2") {
                UNIT_ASSERT_VALUES_EQUAL(coordinator.Generation, 17);
            } else {
                UNIT_ASSERT(false);
            }
        }
    }

    // TODO: add various tests on graph registration

    Y_UNIT_TEST_F(ShouldCreateCheckpoint, TSdkCheckpoints)
    {
        TCoordinatorId coordinator("graph1", 11);
        auto issues = Storage->RegisterGraphCoordinator(coordinator).GetValueSync();

        auto createCheckpointResult = Storage->CreateCheckpoint(coordinator, CheckpointId1, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        issues = createCheckpointResult.second;
        UNIT_ASSERT(issues.Empty());
    }

    // TODO: add more tests on checkpoints manipulations

    Y_UNIT_TEST_F(ShouldCreateGetCheckpoints, TSdkCheckpoints)
    {
        CreateSome();

        auto getResult = Storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 4UL);
        for (const auto& metadata : getResult.first) {
            UNIT_ASSERT(metadata.Graph);
            UNIT_ASSERT_VALUES_EQUAL_C(metadata.Graph->GetGraphId(), "graph1", *metadata.Graph);
        }

        getResult = Storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);
        UNIT_ASSERT(getResult.first[0].Graph);
        UNIT_ASSERT_VALUES_EQUAL_C(getResult.first[0].Graph->GetGraphId(), "graph2", *getResult.first[0].Graph);

        // Get checkpoints without graph description
        getResult = Storage->GetCheckpoints("graph2", {}, 1, false).GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);
        UNIT_ASSERT(!getResult.first[0].Graph);
    }

    Y_UNIT_TEST_F(ShouldGetCheckpointsEmpty, TSdkCheckpoints)
    {
        auto getResult = Storage->GetCheckpoints("no-such-graph").GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST_F(ShouldDeleteGraph, TSdkCheckpoints)
    {
        CreateSome();

        // now delete graph1

        auto issues = Storage->DeleteGraph("graph1").GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        // check that the only left graph is "graph2"

        auto getCoordinatorsResult = Storage->GetCoordinators().GetValueSync();
        UNIT_ASSERT_C(getCoordinatorsResult.second.Empty(), getCoordinatorsResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getCoordinatorsResult.first.size(), 1UL);

        const auto& survivedCoordinator = getCoordinatorsResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(survivedCoordinator.GraphId, "graph2");
        UNIT_ASSERT_VALUES_EQUAL(survivedCoordinator.Generation, 17);

        // check no checkpoints left for graph1

        auto getCheckpointsResult = Storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_C(getCheckpointsResult.second.Empty(), getCheckpointsResult.second.ToString());
        UNIT_ASSERT(getCheckpointsResult.first.empty());

        // check graph2 checkpoints intact
        getCheckpointsResult = Storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_C(getCheckpointsResult.second.Empty(), getCheckpointsResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getCheckpointsResult.first.size(), 1UL);
    }

    Y_UNIT_TEST_F(ShouldMarkCheckpointsGc, TSdkCheckpoints)
    {
        CreateSome();

        auto issues = Storage->MarkCheckpointsGC("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = Storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 4UL);

        for (const auto& meta: getResult.first) {
            if (meta.CheckpointId == CheckpointId3 || meta.CheckpointId == CheckpointId4) {
                UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::Pending);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::GC);
            }
        }

        // check graph2 checkpoints intact
        getResult = Storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);

        const auto& graph2Checkpoint1 = getResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(graph2Checkpoint1.Status, ECheckpointStatus::Pending);
    }

    Y_UNIT_TEST_F(ShouldDeleteMarkedCheckpoints, TSdkCheckpoints)
    {
        CreateSome();

        auto issues = Storage->MarkCheckpointsGC("graph1", CheckpointId3).GetValueSync();
        issues = Storage->DeleteMarkedCheckpoints("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = Storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 2UL);

        for (const auto& meta: getResult.first) {
            UNIT_ASSERT(meta.CheckpointId == CheckpointId3 || meta.CheckpointId == CheckpointId4);
            UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::Pending);
        }

        // check graph2 checkpoints intact
        getResult = Storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);

        const auto& graph2Checkpoint1 = getResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(graph2Checkpoint1.Status, ECheckpointStatus::Pending);
    }

    Y_UNIT_TEST_F(ShouldCleanupGraphsOnExplicitDeletion, TSdkCheckpoints) {
        CreateSome();
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 3);
        auto issues = Storage->DeleteGraph("graph1").GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(Integration->CleanedTasks, (TVector<ui64>{1, 1}));
        UNIT_ASSERT_VALUES_EQUAL(OtherIntegration->CleanedTasks, (TVector<ui64>{1, 1}));
        for (const auto& integration : {Integration, OtherIntegration}) {
            UNIT_ASSERT_VALUES_EQUAL(integration->CleanupGenerationBounds.size(), 2);
            for (const auto& bound : integration->CleanupGenerationBounds) {
                UNIT_ASSERT(!bound);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 1);

        issues = Storage->DeleteGraph("graph1").GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(Integration->CleanedTasks.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(Storage->GetCheckpoints("graph2").GetValueSync().first.size(), 1);
    }

    Y_UNIT_TEST_F(ShouldCleanupGraphsOnlyAfterLastCheckpointDeletion, TSdkCheckpoints) {
        CreateSome();
        auto issues = Storage->MarkCheckpointsGC("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        issues = Storage->DeleteMarkedCheckpoints("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(Integration->CleanedTasks.empty());
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 3);

        issues = Storage->MarkCheckpointsGC("graph1", CheckpointId4).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        issues = Storage->DeleteMarkedCheckpoints("graph1", CheckpointId4).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(Integration->CleanedTasks, (TVector<ui64>{1}));
        for (const auto& integration : {Integration, OtherIntegration}) {
            UNIT_ASSERT_VALUES_EQUAL(integration->CleanupGenerationBounds.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(integration->CleanupGenerationBounds[0].value(), CheckpointId4.CoordinatorGeneration);
        }
        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 2);
    }

    Y_UNIT_TEST_F(ShouldDeleteGraphWithInvalidDescription, TSdkCheckpoints) {
        CheckInvalidGraphDescription(false, false);
    }

    Y_UNIT_TEST_F(ShouldDeleteGraphWithNullDescription, TSdkCheckpoints) {
        CheckInvalidGraphDescription(true, false);
    }

    Y_UNIT_TEST_F(ShouldGcCheckpointsWithInvalidGraphDescription, TSdkCheckpoints) {
        CheckInvalidGraphDescription(false, true);
    }

    Y_UNIT_TEST_F(ShouldGcCheckpointsWithNullGraphDescription, TSdkCheckpoints) {
        CheckInvalidGraphDescription(true, true);
    }

    Y_UNIT_TEST_F(ShouldDeleteGraphAfterCleanupFailure, TSdkCheckpoints) {
        CheckCleanupFailure(false, false);
    }

    Y_UNIT_TEST_F(ShouldDeleteGraphAfterCleanupException, TSdkCheckpoints) {
        CheckCleanupFailure(false, true);
    }

    Y_UNIT_TEST_F(ShouldGcAfterCleanupFailure, TSdkCheckpoints) {
        CheckCleanupFailure(true, false);
    }

    Y_UNIT_TEST_F(ShouldGcAfterCleanupException, TSdkCheckpoints) {
        CheckCleanupFailure(true, true);
    }

    Y_UNIT_TEST_F(ShouldWaitForParallelSinkCleanupOnDeletion, TSdkCheckpoints) {
        CheckParallelSinkCleanup(false, false);
    }

    Y_UNIT_TEST_F(ShouldWaitForParallelSinkCleanupOnGc, TSdkCheckpoints) {
        CheckParallelSinkCleanup(true, false);
    }

    Y_UNIT_TEST_F(ShouldWaitForFailedParallelSinkCleanupOnDeletion, TSdkCheckpoints) {
        CheckParallelSinkCleanup(false, true);
    }

    Y_UNIT_TEST_F(ShouldWaitForFailedParallelSinkCleanupOnGc, TSdkCheckpoints) {
        CheckParallelSinkCleanup(true, true);
    }

    Y_UNIT_TEST_F(ShouldGcAfterParallelSinkCleanupIssues, TSdkCheckpoints) {
        CheckParallelSinkCleanup(true, true, false);
    }

    Y_UNIT_TEST_F(ShouldDeleteGraphAfterParallelSinkCleanupIssues, TSdkCheckpoints) {
        CheckParallelSinkCleanup(false, true, false);
    }

    Y_UNIT_TEST_F(ShouldNotDeleteUnmarkedCheckpoints, TSdkCheckpoints)
    {
        CreateSome();

        auto issues = Storage->DeleteMarkedCheckpoints("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = Storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 4UL);

        for (const auto& meta: getResult.first) {
            UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::Pending);
        }

        // check graph2 checkpoints intact
        getResult = Storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);

        const auto& graph2Checkpoint1 = getResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(graph2Checkpoint1.Status, ECheckpointStatus::Pending);
    }

    Y_UNIT_TEST_F(ShouldUpdateCheckpointStatusForCheckpointsWithTheSameGenAndNo, TSdkCheckpoints)
    {
        TCoordinatorId coordinator1("graph1", 42);
        UNIT_ASSERT(Storage->RegisterGraphCoordinator(coordinator1).GetValueSync().Empty());
        auto createCheckpointResult = Storage->CreateCheckpoint(coordinator1, CheckpointId1, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT(createCheckpointResult.second.Empty());

        TCoordinatorId coordinator2("graph2", coordinator1.Generation);
        UNIT_ASSERT(Storage->RegisterGraphCoordinator(coordinator2).GetValueSync().Empty());
        UNIT_ASSERT(Storage->CreateCheckpoint(coordinator2, CheckpointId1, createCheckpointResult.first, ECheckpointStatus::Pending).GetValueSync().second.Empty());

        UNIT_ASSERT(Storage->UpdateCheckpointStatus(coordinator1, CheckpointId1, ECheckpointStatus::PendingCommit, ECheckpointStatus::Pending, 100).GetValueSync().Empty());
        UNIT_ASSERT(Storage->UpdateCheckpointStatus(coordinator2, CheckpointId1, ECheckpointStatus::PendingCommit, ECheckpointStatus::Pending, 100).GetValueSync().Empty());
    }

    struct TTestEntityIdGenerator : IEntityIdGenerator {
        TTestEntityIdGenerator(std::initializer_list<TString> list)
            : Ids(std::move(list))
        {
        }

        TString Generate(EEntityType) override {
            ++CallsCount;
            UNIT_ASSERT(!Ids.empty());
            TString result = Ids.front();
            Ids.pop_front();
            return result;
        }

        std::deque<TString> Ids;
        size_t CallsCount = 0;
    };

    Y_UNIT_TEST_F(ShouldRetryOnExistingGraphDescId, TSdkCheckpoints)
    {
        auto idGenerator = new TTestEntityIdGenerator({"id1", "id1", "id1", "id2"});
        InitCheckpointStorage(idGenerator);

        TCoordinatorId coordinator1("graph1", 11);
        auto issues = Storage->RegisterGraphCoordinator(coordinator1).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto createCheckpointResult = Storage->CreateCheckpoint(coordinator1, CheckpointId1, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint1GraphDescId = createCheckpointResult.first;
        UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, "id1");
        UNIT_ASSERT_VALUES_EQUAL(idGenerator->CallsCount, 1);

        createCheckpointResult = Storage->CreateCheckpoint(coordinator1, CheckpointId2, checkpoint1GraphDescId, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, createCheckpointResult.first);
        UNIT_ASSERT_VALUES_EQUAL(idGenerator->CallsCount, 1);

        TCoordinatorId coordinator1v2("graph1", 18);
        issues = Storage->RegisterGraphCoordinator(coordinator1v2).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        createCheckpointResult = Storage->CreateCheckpoint(coordinator1v2, CheckpointId4, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint2GraphDescId = createCheckpointResult.first;
        UNIT_ASSERT_VALUES_EQUAL(checkpoint2GraphDescId, "id2");
        UNIT_ASSERT_VALUES_EQUAL(idGenerator->CallsCount, 4);
    }
};

} // namespace NFq
