#include <ydb/core/fq/libs/checkpoint_storage/ydb_state_storage.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

#include <google/protobuf/util/message_differencer.h>

namespace NFq {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TCheckpointId CheckpointId1(11, 3);
const TCheckpointId CheckpointId2(12, 1);
const TCheckpointId CheckpointId3(12, 4);
const TCheckpointId CheckpointId4(13, 2);

const size_t YdbRowSizeLimit = 500;

////////////////////////////////////////////////////////////////////////////////

TYqSharedResources::TPtr YqSharedResources;

TStateStoragePtr GetStateStorage(const char* tablePrefix) {

    NConfig::TCheckpointCoordinatorConfig config;
    auto& stateStorageConfig = *config.MutableStorage();
    stateStorageConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    stateStorageConfig.SetDatabase(GetEnv("YDB_DATABASE"));
    stateStorageConfig.SetToken("");
    stateStorageConfig.SetTablePrefix(tablePrefix);
    auto& stateStorageLimits = *config.MutableStateStorageLimits();
    stateStorageLimits.SetMaxRowSizeBytes(YdbRowSizeLimit);

    YqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, NKikimr::CreateYdbCredentialsProviderFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    auto storage = NewYdbStateStorage(config, NKikimr::CreateYdbCredentialsProviderFactory, YqSharedResources);
    storage->Init().GetValueSync();
    return storage;
}

NYdb::NTable::TSession GetYdbSession()
{
    NYdb::TDriverConfig cfg;
    cfg.SetEndpoint(GetEnv("YDB_ENDPOINT")).SetDatabase(GetEnv("YDB_DATABASE"));
    NYdb::TDriver driver(cfg);
    NYdb::NTable::TTableClient tableClient(driver);
    auto createSessionResult = tableClient.CreateSession().ExtractValueSync();
    UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
    return createSessionResult.GetSession();
}

NYql::NDqProto::TComputeActorState MakeStateFromBlob(size_t blobSize) {

    TString blob;
    for (size_t i = 0; i < blobSize; ++i) {
        blob += static_cast<TString::value_type>(std::rand() % 100);
    }
    NYql::NDqProto::TComputeActorState state;
    state.MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetBlob(blob);
    return state;
}

NYql::NDqProto::TComputeActorState MakeIncrementState(size_t miniKqlPStateSize) {
    NYql::NDqProto::TComputeActorState state;
    size_t itemCount = 4;
    for (size_t i = 0; i < itemCount; ++i) {
        auto* item = state.MutableMiniKqlProgram()->AddNodeState()->MutableSnapshot()->AddItems();
        item->SetKey(ToString((777 + i)));
        TString blob(miniKqlPStateSize / itemCount, 'a');
        item->SetBlob(blob);
    }
    return state;
}

NYql::NDqProto::TComputeActorState MakeIncrementState(
    const std::map<TString, TString>& snapshot,
    const std::map<TString, TString>& increment,
    const std::set<TString>& deleted)
{
    NYql::NDqProto::TComputeActorState state;
    auto* nodeState = state.MutableMiniKqlProgram()->AddNodeState();
    if (!snapshot.empty()) {
        auto* resultSnapshot = nodeState->MutableSnapshot();
        for (const auto& [key, value] : snapshot) {
            auto* item = resultSnapshot->AddItems();
            item->SetKey(key);
            item->SetBlob(value);
        }
    }

    if (!increment.empty() || !deleted.empty()) {
        auto* resultIncrement = nodeState->MutableIncrement();
        for (const auto& [key, value] : increment) {
            auto* item = resultIncrement->AddNewOrChanged();
            item->SetKey(key);
            item->SetBlob(value);
        }

        for (const auto& key : deleted) {
            auto* item = resultIncrement->AddDeletedKeys();
            *item =key;
        }
    }
    return state;
}

void SaveState(
    TStateStoragePtr storage,
    ui64 taskId,
    const TString& graphId,
    const TCheckpointId& checkpointId,
    const NYql::NDqProto::TComputeActorState& state)
{
    auto issues = storage->SaveState(taskId, graphId, checkpointId, state).GetValueSync();
    UNIT_ASSERT_C(issues.Empty(), issues.ToString());
}

NYql::NDqProto::TComputeActorState GetState(
    TStateStoragePtr storage,
    const ui64 taskId,
    const TString& graphId,
    const TCheckpointId& checkpointId)
{
    auto [states, issues] = storage->GetState({taskId}, graphId, checkpointId).GetValueSync();
    UNIT_ASSERT_C(issues.Empty(), issues.ToString());
    UNIT_ASSERT(!states.empty());
    return states[0];
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateStorageTest) {

    void ShouldSaveGetStateImpl(const char* tablePrefix, const NYql::NDqProto::TComputeActorState& state)
    {
        auto storage = GetStateStorage(tablePrefix);
        auto issues = storage->SaveState(1, "graph1", CheckpointId1, state).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto [states, getIssues] = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_C(getIssues.Empty(), getIssues.ToString());
        UNIT_ASSERT(!states.empty());
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state, states[0]));
    }

    Y_UNIT_TEST(ShouldSaveGetOldSmallState)
    {
        ShouldSaveGetStateImpl("TStateStorageTestShouldSaveGetState", MakeStateFromBlob(4));
    }

    Y_UNIT_TEST(ShouldSaveGetOldBigState)
    {
        ShouldSaveGetStateImpl("TStateStorageTestShouldSaveGetState", MakeStateFromBlob(YdbRowSizeLimit * 4));
    }

    Y_UNIT_TEST(ShouldSaveGetIncrementSmallState)
    {
        ShouldSaveGetStateImpl("ShouldSaveGetIncrementState", MakeIncrementState(10));
    }

    Y_UNIT_TEST(ShouldSaveGetIncrementBigState)
    {
        ShouldSaveGetStateImpl("ShouldSaveGetIncrementState", MakeIncrementState(YdbRowSizeLimit * 5));
    }

    Y_UNIT_TEST(ShouldNotGetNonExistendState)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldNotGetNonExistendState");
        auto getResult = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST(ShouldCountStates)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldCountStates");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeIncrementState(YdbRowSizeLimit * 3)).GetValueSync();

        auto [count, issues] = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(count, 3);
    }

    Y_UNIT_TEST(ShouldCountStatesNonExistentCheckpoint)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldCountStatesNonExistentCheckpoint");

        auto [count, issues] = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(count, 0);
    }

    Y_UNIT_TEST(ShouldDeleteNoCheckpoints)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteNoCheckpoints");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();

        auto issues = storage->DeleteCheckpoints("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 3);
    }

    Y_UNIT_TEST(ShouldDeleteNoCheckpoints2)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteNoCheckpoints2");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();

        auto issues = storage->DeleteCheckpoints("graph2", CheckpointId2).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 3);
    }

    Y_UNIT_TEST(ShouldDeleteCheckpoints)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteCheckpoints");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeIncrementState(YdbRowSizeLimit * 4)).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId2, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId2, MakeStateFromBlob(4)).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId3, MakeStateFromBlob(4)).GetValueSync();

        // delete
        auto issues = storage->DeleteCheckpoints("graph1", CheckpointId2).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);

        countResult = storage->CountStates("graph1", CheckpointId2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 2);

        // next delete
        issues = storage->DeleteCheckpoints("graph1", CheckpointId4).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        countResult = storage->CountStates("graph1", CheckpointId2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);
        countResult = storage->CountStates("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);
    }

    Y_UNIT_TEST(ShouldDeleteGraph)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteCheckpoints");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeIncrementState(YdbRowSizeLimit * 3)).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId2, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId2, MakeStateFromBlob(4)).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId3, MakeStateFromBlob(4)).GetValueSync();

        auto issues = storage->DeleteGraph("graph1").GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);
        countResult = storage->CountStates("graph1", CheckpointId2).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);
        countResult = storage->CountStates("graph1", CheckpointId3).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);
    }

    Y_UNIT_TEST(ShouldIssueErrorOnWrongGetStateParams)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldIssueErrorOnWrongGetStateParams");

        auto getResult = storage->GetState({}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());

        getResult = storage->GetState({1, 1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST(ShouldIssueErrorOnNonExistentState) {
        auto storage = GetStateStorage("TStateStorageTestShouldIssueErrorOnNonExistentState");

        auto issues = storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto getResult = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT(!getResult.first.empty());

        getResult = storage->GetState({1, 2}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST(ShouldGetMultipleStates)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldGetMultipleStates");
    
        auto state1 = MakeStateFromBlob(10);
        auto state2 = MakeIncrementState(10);
        auto state3 = MakeStateFromBlob(YdbRowSizeLimit * 6);
        auto state4 = MakeIncrementState(YdbRowSizeLimit * 3);

        auto issues = storage->SaveState(1, "graph1", CheckpointId1, state1).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        issues = storage->SaveState(42, "graph1", CheckpointId1, state2).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        issues = storage->SaveState(7, "graph1", CheckpointId1, state3).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        issues = storage->SaveState(13, "graph1", CheckpointId1, state4).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto [states, getIssues] = storage->GetState({1, 42, 7, 13}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_C(getIssues.Empty(), getIssues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(states.size(), 4);

        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state1, states[0]));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state2, states[1]));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state3, states[2]));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state4, states[3]));

        // in different order
        auto [states2, getIssues2] = storage->GetState({42, 1, 13, 7}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getIssues2.Empty());
        UNIT_ASSERT_VALUES_EQUAL(states2.size(), 4);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state2, states2[0]));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state1, states2[1]));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state4, states2[2]));
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state3, states2[3]));
    }

    Y_UNIT_TEST(ShouldLoadLastSnapshot)
    {
        auto storage = GetStateStorage("ShouldLoadLastSnapshot");

        auto state1 = MakeIncrementState({{"key1", "value1"}, {"key2", "value2"}}, {}, {});
        auto state2 = MakeIncrementState({{"key1", "value1"}, {"key2", "value2"}}, {}, {});

        SaveState(storage, 1, "graph1", CheckpointId1, state1);
        SaveState(storage, 1, "graph1", CheckpointId2, state2);

        auto state = GetState(storage, 1, "graph1", CheckpointId2);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(state, state2));
    }

    Y_UNIT_TEST(ShouldNotGetNonExistendSnaphotState)
    {
        auto storage = GetStateStorage("ShouldNotGetNonExistendSnaphotState");
        auto state = MakeIncrementState({}, {{"key1", "value1-new"}, {"key3", "value3"}}, {"key2"});
        SaveState(storage, 1, "graph1", CheckpointId4, state);

        auto [states, issues] = storage->GetState({1}, "graph1", CheckpointId4).GetValueSync();
        UNIT_ASSERT(!issues.Empty());
        UNIT_ASSERT(states.empty());
    }

    Y_UNIT_TEST(ShouldLoadIncrementSnapshot)
    {
        auto storage = GetStateStorage("ShouldLoadIncrementSnapshot");

        auto state1 = MakeIncrementState({{"key1", "value1"}, {"key2", "value2"}}, {}, {});
        auto state2 = MakeIncrementState({}, {{"key1", "value1-new"}, {"key3", "value3"}}, {"key2"});
        auto value4 = TString(YdbRowSizeLimit*3, 'x');
        auto state3 = MakeIncrementState({}, {{"key4", value4}}, {});
        auto state4 = MakeIncrementState({}, {{"key5", "value5"}}, {});

        SaveState(storage, 1, "graph1", CheckpointId1, state1);
        SaveState(storage, 1, "graph1", CheckpointId2, state2);
        SaveState(storage, 1, "graph1", CheckpointId3, state3);
        SaveState(storage, 1, "graph1", CheckpointId4, state4);

        auto expected = MakeIncrementState({{"key1", "value1-new"}, {"key3", "value3"}, {"key4", value4}}, {}, {});

        auto actual = GetState(storage, 1, "graph1", CheckpointId3);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(expected, actual));
    }

    Y_UNIT_TEST(ShouldLoadStateWithNull)
    {
        TString prefix("ShouldLoadStateWithNull");
        auto storage = GetStateStorage(prefix.c_str());

        auto actorState = MakeStateFromBlob(4);
        TString serializedState;
        UNIT_ASSERT(actorState.SerializeToString(&serializedState));

        NYdb::NTable::TSession session = GetYdbSession();
        prefix = "/local/" + prefix;
        TString path = prefix + "/states";
        auto result = session.DescribeTable(path).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& createdColumns = result.GetTableDescription().GetColumns();
        UNIT_ASSERT_EQUAL_C(createdColumns.size(), 7, "expected 7 columns");
        auto query = Sprintf(R"(
            --!syntax_v1
            PRAGMA TablePathPrefix("%s");

            UPSERT INTO states (graph_id, task_id, coordinator_generation, seq_no, blob, blob_seq_num, type) VALUES
                ("graph1", 1, %d, %d, "%s", Null, Null);
            )", prefix.c_str(), 
                static_cast<int>(CheckpointId1.CoordinatorGeneration), 
                static_cast<int>(CheckpointId1.SeqNo),
                serializedState.c_str());

        auto execResult = session.ExecuteDataQuery(query,
             NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).GetValueSync();
        UNIT_ASSERT_C(execResult.IsSuccess(), execResult.GetIssues().ToString());
        auto actual = GetState(storage, 1, "graph1", CheckpointId1);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(actorState, actual));
    }
};

} // namespace NFq
