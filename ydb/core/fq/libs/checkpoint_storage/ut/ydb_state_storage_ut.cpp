#include <ydb/core/fq/libs/checkpoint_storage/ydb_state_storage.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

namespace NFq {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TCheckpointId CheckpointId1(11, 3);
const TCheckpointId CheckpointId2(12, 1);
const TCheckpointId CheckpointId3(12, 4);
const TCheckpointId CheckpointId4(13, 2);

////////////////////////////////////////////////////////////////////////////////

TStateStoragePtr GetStateStorage(const char* tablePrefix) {
    NConfig::TYdbStorageConfig stateStorageConfig;
    stateStorageConfig.SetEndpoint(GetEnv("YDB_ENDPOINT"));
    stateStorageConfig.SetDatabase(GetEnv("YDB_DATABASE"));
    stateStorageConfig.SetToken("");
    stateStorageConfig.SetTablePrefix(tablePrefix);

    auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, NKikimr::CreateYdbCredentialsProviderFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
    auto storage = NewYdbStateStorage(stateStorageConfig, NKikimr::CreateYdbCredentialsProviderFactory, yqSharedResources);
    storage->Init().GetValueSync();
    return storage;
}

NYql::NDqProto::TComputeActorState MakeStateFromBlob(const TString& blob) {
    NYql::NDqProto::TComputeActorState state;
    state.MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetBlob(blob);
    return state;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateStorageTest) {
    Y_UNIT_TEST(ShouldSaveGetState)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldSaveGetState");

        auto issues = storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        auto getResult = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT(!getResult.first.empty());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first[0].GetMiniKqlProgram().GetData().GetStateData().GetBlob(), "blob");

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

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(countResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 3);
    }

    Y_UNIT_TEST(ShouldCountStatesNonExistentCheckpoint)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldCountStatesNonExistentCheckpoint");

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(countResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0);
    }

    Y_UNIT_TEST(ShouldDeleteNoCheckpoints)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteNoCheckpoints");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();

        auto issues = storage->DeleteCheckpoints("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 3);
    }

    Y_UNIT_TEST(ShouldDeleteNoCheckpoints2)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteNoCheckpoints2");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();

        auto issues = storage->DeleteCheckpoints("graph2", CheckpointId2).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto countResult = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 3);
    }

    Y_UNIT_TEST(ShouldDeleteCheckpoints)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldDeleteCheckpoints");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId2, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId2, MakeStateFromBlob("blob")).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId3, MakeStateFromBlob("blob")).GetValueSync();

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

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId2, MakeStateFromBlob("blob")).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId2, MakeStateFromBlob("blob")).GetValueSync();

        storage->SaveState(1, "graph1", CheckpointId3, MakeStateFromBlob("blob")).GetValueSync();

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

        auto issues = storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("blob")).GetValueSync();
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

        auto issues = storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob("1")).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        issues = storage->SaveState(42, "graph1", CheckpointId1, MakeStateFromBlob("42")).GetValueSync();
        UNIT_ASSERT(issues.Empty());

        auto getResult = storage->GetState({1, 42}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(getResult.first[0].GetMiniKqlProgram().GetData().GetStateData().GetBlob(), "1");
        UNIT_ASSERT_VALUES_EQUAL(getResult.first[1].GetMiniKqlProgram().GetData().GetStateData().GetBlob(), "42");

        // in different order
        getResult = storage->GetState({42, 1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(getResult.first[0].GetMiniKqlProgram().GetData().GetStateData().GetBlob(), "42");
        UNIT_ASSERT_VALUES_EQUAL(getResult.first[1].GetMiniKqlProgram().GetData().GetStateData().GetBlob(), "1");
    }
};

} // namespace NFq
