#include <ydb/core/fq/libs/checkpoint_storage/ydb_checkpoint_storage.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

#include <deque>

namespace NFq {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TCheckpointId CheckpointId1(11, 3);
const TCheckpointId CheckpointId2(12, 1);
const TCheckpointId CheckpointId3(12, 4);
const TCheckpointId CheckpointId4(13, 2);

////////////////////////////////////////////////////////////////////////////////

TCheckpointStoragePtr GetCheckpointStorage(const char* tablePrefix, IEntityIdGenerator::TPtr entityIdGenerator = CreateEntityIdGenerator("id")) {
    NConfig::TYdbStorageConfig checkpointStorageConfig;
    checkpointStorageConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    checkpointStorageConfig.SetDatabase(GetEnv("YDB_DATABASE"));
    checkpointStorageConfig.SetToken("");
    checkpointStorageConfig.SetTablePrefix(tablePrefix);

    auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
    auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    auto storage = NewYdbCheckpointStorage(checkpointStorageConfig, credFactory, entityIdGenerator, yqSharedResources);
    auto issues = storage->Init().GetValueSync();
    UNIT_ASSERT_C(issues.Empty(), issues.ToString());
    return storage;
}

void CreateSome(const TCheckpointStoragePtr& storage) {
    // coordinator1 registers and performs some work

    TCoordinatorId coordinator1("graph1", 11);
    auto issues = storage->RegisterGraphCoordinator(coordinator1).GetValueSync();
    UNIT_ASSERT_C(issues.Empty(), issues.ToString());

    NProto::TCheckpointGraphDescription desc;
    desc.MutableGraph()->SetGraphId("graph1");
    auto createCheckpointResult = storage->CreateCheckpoint(coordinator1, CheckpointId1, desc, ECheckpointStatus::Pending).GetValueSync();
    UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
    const TString checkpoint1GraphDescId = createCheckpointResult.first;

    createCheckpointResult = storage->CreateCheckpoint(coordinator1, CheckpointId2, createCheckpointResult.first, ECheckpointStatus::Pending).GetValueSync();
    UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
    UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, createCheckpointResult.first);

    createCheckpointResult = storage->CreateCheckpoint(coordinator1, CheckpointId3, createCheckpointResult.first, ECheckpointStatus::Pending).GetValueSync();
    UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
    UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, createCheckpointResult.first);

    // coordinator2

    TCoordinatorId coordinator2("graph2", 17);
    issues = storage->RegisterGraphCoordinator(coordinator2).GetValueSync();
    UNIT_ASSERT_C(issues.Empty(), issues.ToString());

    desc.MutableGraph()->SetGraphId("graph2");
    createCheckpointResult = storage->CreateCheckpoint(coordinator2, CheckpointId1, desc, ECheckpointStatus::Pending).GetValueSync();
    UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
    const TString checkpoint2GraphDescId = createCheckpointResult.first;
    UNIT_ASSERT_UNEQUAL(checkpoint1GraphDescId, checkpoint2GraphDescId);

    // new coordinator for graph1

    TCoordinatorId coordinator1v2("graph1", 18);
    issues = storage->RegisterGraphCoordinator(coordinator1v2).GetValueSync();
    UNIT_ASSERT_C(issues.Empty(), issues.ToString());

    desc.MutableGraph()->SetGraphId("graph1");
    createCheckpointResult = storage->CreateCheckpoint(coordinator1v2, CheckpointId4, desc, ECheckpointStatus::Pending).GetValueSync();
    UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
    const TString checkpoint3GraphDescId = createCheckpointResult.first;
    UNIT_ASSERT_UNEQUAL(checkpoint1GraphDescId, checkpoint3GraphDescId);
    UNIT_ASSERT_UNEQUAL(checkpoint2GraphDescId, checkpoint3GraphDescId);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Note that many scenarious are tested in storage_service_ydb_ut.cpp

Y_UNIT_TEST_SUITE(TCheckpointStorageTest) {
    Y_UNIT_TEST(ShouldRegisterCoordinator)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldRegisterCoordinator");

        TCoordinatorId coordinator("graph1", 11);
        auto issues = storage->RegisterGraphCoordinator(coordinator).GetValueSync();
        UNIT_ASSERT(issues.Empty());
    }

    Y_UNIT_TEST(ShouldGetCoordinators)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldRegisterGraph");

        TCoordinatorId coordinator1("graph1", 11);
        auto issues = storage->RegisterGraphCoordinator(coordinator1).GetValueSync();

        TCoordinatorId coordinator2("graph2", 17);
        issues = storage->RegisterGraphCoordinator(coordinator2).GetValueSync();

        auto getResult = storage->GetCoordinators().GetValueSync();
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

    Y_UNIT_TEST(ShouldCreateCheckpoint)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldCreateCheckpoint");

        TCoordinatorId coordinator("graph1", 11);
        auto issues = storage->RegisterGraphCoordinator(coordinator).GetValueSync();

        auto createCheckpointResult = storage->CreateCheckpoint(coordinator, CheckpointId1, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        issues = createCheckpointResult.second;
        UNIT_ASSERT(issues.Empty());
    }

    // TODO: add more tests on checkpoints manipulations

    Y_UNIT_TEST(ShouldCreateGetCheckpoints)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldCreateGetCheckpoints");
        CreateSome(storage);

        auto getResult = storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 4UL);
        for (const auto& metadata : getResult.first) {
            UNIT_ASSERT(metadata.Graph);
            UNIT_ASSERT_VALUES_EQUAL_C(metadata.Graph->GetGraphId(), "graph1", *metadata.Graph);
        }

        getResult = storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);
        UNIT_ASSERT(getResult.first[0].Graph);
        UNIT_ASSERT_VALUES_EQUAL_C(getResult.first[0].Graph->GetGraphId(), "graph2", *getResult.first[0].Graph);

        // Get checkpoints without graph description
        getResult = storage->GetCheckpoints("graph2", {}, 1, false).GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);
        UNIT_ASSERT(!getResult.first[0].Graph);
    }

    Y_UNIT_TEST(ShouldGetCheckpointsEmpty)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldGetCheckpointsEmpty");
        auto getResult = storage->GetCheckpoints("no-such-graph").GetValueSync();
        UNIT_ASSERT_C(getResult.second.Empty(), getResult.second.ToString());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST(ShouldDeleteGraph)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldDeleteGraph");
        CreateSome(storage);

        // now delete graph1

        auto issues = storage->DeleteGraph("graph1").GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        // check that the only left graph is "graph2"

        auto getCoordinatorsResult = storage->GetCoordinators().GetValueSync();
        UNIT_ASSERT_C(getCoordinatorsResult.second.Empty(), getCoordinatorsResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getCoordinatorsResult.first.size(), 1UL);

        const auto& survivedCoordinator = getCoordinatorsResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(survivedCoordinator.GraphId, "graph2");
        UNIT_ASSERT_VALUES_EQUAL(survivedCoordinator.Generation, 17);

        // check no checkpoints left for graph1

        auto getCheckpointsResult = storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_C(getCheckpointsResult.second.Empty(), getCheckpointsResult.second.ToString());
        UNIT_ASSERT(getCheckpointsResult.first.empty());

        // check graph2 checkpoints intact
        getCheckpointsResult = storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_C(getCheckpointsResult.second.Empty(), getCheckpointsResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(getCheckpointsResult.first.size(), 1UL);
    }

    Y_UNIT_TEST(ShouldMarkCheckpointsGc)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldMarkCheckpointsGc");
        CreateSome(storage);

        auto issues = storage->MarkCheckpointsGC("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 4UL);

        for (const auto& meta: getResult.first) {
            if (meta.CheckpointId == CheckpointId3 || meta.CheckpointId == CheckpointId4) {
                UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::Pending);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::GC);
            }
        }

        // check graph2 checkpoints intact
        getResult = storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);

        const auto& graph2Checkpoint1 = getResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(graph2Checkpoint1.Status, ECheckpointStatus::Pending);
    }

    Y_UNIT_TEST(ShouldDeleteMarkedCheckpoints)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldDeleteMarkedCheckpoints");
        CreateSome(storage);

        auto issues = storage->MarkCheckpointsGC("graph1", CheckpointId3).GetValueSync();
        issues = storage->DeleteMarkedCheckpoints("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 2UL);

        for (const auto& meta: getResult.first) {
            UNIT_ASSERT(meta.CheckpointId == CheckpointId3 || meta.CheckpointId == CheckpointId4);
            UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::Pending);
        }

        // check graph2 checkpoints intact
        getResult = storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);

        const auto& graph2Checkpoint1 = getResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(graph2Checkpoint1.Status, ECheckpointStatus::Pending);
    }

    Y_UNIT_TEST(ShouldNotDeleteUnmarkedCheckpoints)
    {
        auto storage = GetCheckpointStorage("TCheckpointStorageTestShouldDeleteCheckpoints");
        CreateSome(storage);

        auto issues = storage->DeleteMarkedCheckpoints("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = storage->GetCheckpoints("graph1").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 4UL);

        for (const auto& meta: getResult.first) {
            UNIT_ASSERT_VALUES_EQUAL(meta.Status, ECheckpointStatus::Pending);
        }

        // check graph2 checkpoints intact
        getResult = storage->GetCheckpoints("graph2").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);

        const auto& graph2Checkpoint1 = getResult.first.front();
        UNIT_ASSERT_VALUES_EQUAL(graph2Checkpoint1.Status, ECheckpointStatus::Pending);
    }

    Y_UNIT_TEST(ShouldUpdateCheckpointStatusForCheckpointsWithTheSameGenAndNo)
    {
        auto storage = GetCheckpointStorage("ShouldUpdateCheckpointStatus");
        TCoordinatorId coordinator1("graph1", 42);
        UNIT_ASSERT(storage->RegisterGraphCoordinator(coordinator1).GetValueSync().Empty());
        auto createCheckpointResult = storage->CreateCheckpoint(coordinator1, CheckpointId1, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT(createCheckpointResult.second.Empty());

        TCoordinatorId coordinator2("graph2", coordinator1.Generation);
        UNIT_ASSERT(storage->RegisterGraphCoordinator(coordinator2).GetValueSync().Empty());
        UNIT_ASSERT(storage->CreateCheckpoint(coordinator2, CheckpointId1, createCheckpointResult.first, ECheckpointStatus::Pending).GetValueSync().second.Empty());

        UNIT_ASSERT(storage->UpdateCheckpointStatus(coordinator1, CheckpointId1, ECheckpointStatus::PendingCommit, ECheckpointStatus::Pending, 100).GetValueSync().Empty());
        UNIT_ASSERT(storage->UpdateCheckpointStatus(coordinator2, CheckpointId1, ECheckpointStatus::PendingCommit, ECheckpointStatus::Pending, 100).GetValueSync().Empty());
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

    Y_UNIT_TEST(ShouldRetryOnExistingGraphDescId)
    {
        auto idGenerator = new TTestEntityIdGenerator({"id1", "id1", "id1", "id2"});
        auto storage = GetCheckpointStorage("ShouldRetryOnExistingGraphDescId", idGenerator);

        TCoordinatorId coordinator1("graph1", 11);
        auto issues = storage->RegisterGraphCoordinator(coordinator1).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto createCheckpointResult = storage->CreateCheckpoint(coordinator1, CheckpointId1, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint1GraphDescId = createCheckpointResult.first;
        UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, "id1");
        UNIT_ASSERT_VALUES_EQUAL(idGenerator->CallsCount, 1);

        createCheckpointResult = storage->CreateCheckpoint(coordinator1, CheckpointId2, checkpoint1GraphDescId, ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        UNIT_ASSERT_VALUES_EQUAL(checkpoint1GraphDescId, createCheckpointResult.first);
        UNIT_ASSERT_VALUES_EQUAL(idGenerator->CallsCount, 1);

        TCoordinatorId coordinator1v2("graph1", 18);
        issues = storage->RegisterGraphCoordinator(coordinator1v2).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        createCheckpointResult = storage->CreateCheckpoint(coordinator1v2, CheckpointId4, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());
        const TString checkpoint2GraphDescId = createCheckpointResult.first;
        UNIT_ASSERT_VALUES_EQUAL(checkpoint2GraphDescId, "id2");
        UNIT_ASSERT_VALUES_EQUAL(idGenerator->CallsCount, 4);
    }
};

} // namespace NFq
