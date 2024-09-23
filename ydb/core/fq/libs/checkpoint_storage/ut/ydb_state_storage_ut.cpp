#include <ydb/core/fq/libs/checkpoint_storage/ydb_state_storage.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/env.h>

#include <google/protobuf/util/message_differencer.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

namespace NFq {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TCheckpointId CheckpointId1(11, 3);
const TCheckpointId CheckpointId2(12, 1);
const TCheckpointId CheckpointId3(12, 4);
const TCheckpointId CheckpointId4(13, 2);

const size_t YdbRowSizeLimit = 500;

////////////////////////////////////////////////////////////////////////////////

class TFixture : public NUnitTest::TBaseFixture {
public:
    TFixture()
        : Alloc(__LOCATION__)
    {}

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

    NYql::NDq::TComputeActorState MakeState(NYql::NUdf::TUnboxedValuePod&& value) {
        TString result;
        NKikimr::NMiniKQL::TNodeStateHelper::AddNodeState(result, value.AsStringRef());
        NYql::NDq::TComputeActorState state;
        state.MiniKqlProgram.ConstructInPlace().Data.Blob = result;
        return state;
    }

    NYql::NDq::TComputeActorState MakeStateFromBlob(size_t blobSize) {
        TString blob;
        blob.reserve(blobSize);
        for (size_t i = 0; i < blobSize; ++i) {
            blob += static_cast<TString::value_type>(std::rand() % 100);
        }
        return MakeState(NKikimr::NMiniKQL::TOutputSerializer::MakeSimpleBlobState(blob, 0));
    }

    NYql::NDq::TComputeActorState MakeIncrementState(size_t miniKqlPStateSize) {
        std::map<TString, TString> map;
        size_t itemCount = 4;
        for (size_t i = 0; i < itemCount; ++i) {
            map[ToString(777 + i)] = TString(miniKqlPStateSize / itemCount, 'a');
        }
        return MakeState(NKikimr::NMiniKQL::TOutputSerializer::MakeSnapshotState(map, 0));
    }

    NYql::NDq::TComputeActorState MakeIncrementState(
        const std::map<TString, TString>& snapshot,
        const std::map<TString, TString>& increment,
        const std::set<TString>& deleted)
    {
        if (!snapshot.empty()) {
            return MakeState(NKikimr::NMiniKQL::TOutputSerializer::MakeSnapshotState(snapshot, 0));
        }
        return MakeState(NKikimr::NMiniKQL::TOutputSerializer::MakeIncrementState(increment, deleted, 0));
    }

    void SaveState(
        TStateStoragePtr storage,
        ui64 taskId,
        const TString& graphId,
        const TCheckpointId& checkpointId,
        const NYql::NDq::TComputeActorState& state)
    {
        auto [size, issues] = storage->SaveState(taskId, graphId, checkpointId, state).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(size > 0);
    }

    NYql::NDq::TComputeActorState GetState(
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

    void ShouldSaveGetStateImpl(const char* tablePrefix, const NYql::NDq::TComputeActorState& state) {
        auto storage = GetStateStorage(tablePrefix);
        auto [size, issues] = storage->SaveState(1, "graph1", CheckpointId1, state).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(size > 0);

        auto [states, getIssues] = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_C(getIssues.Empty(), getIssues.ToString());
        UNIT_ASSERT(!states.empty());
        CheckEquals(state, states[0]);
    }

    void CheckEquals(const NYql::NDq::TComputeActorState& state1, const NYql::NDq::TComputeActorState& state2) {
        UNIT_ASSERT_VALUES_EQUAL(state1.MiniKqlProgram.Empty(), state2.MiniKqlProgram.Empty());
        if (state1.MiniKqlProgram) {
            UNIT_ASSERT_VALUES_EQUAL(state1.MiniKqlProgram->Data.Blob, state2.MiniKqlProgram->Data.Blob);
            UNIT_ASSERT_VALUES_EQUAL(state1.MiniKqlProgram->Data.Version, state2.MiniKqlProgram->Data.Version);
            UNIT_ASSERT_VALUES_EQUAL(state1.MiniKqlProgram->RuntimeVersion, state2.MiniKqlProgram->RuntimeVersion);
        }
        UNIT_ASSERT_VALUES_EQUAL(state1.Sources.size(), state2.Sources.size());
        UNIT_ASSERT(std::equal(std::begin(state1.Sources), std::end(state1.Sources), std::begin(state2.Sources), std::end(state2.Sources),
            [](const NYql::NDq::TSourceState& state1, const NYql::NDq::TSourceState& state2) {
                UNIT_ASSERT_VALUES_EQUAL(state1.InputIndex, state2.InputIndex);
                UNIT_ASSERT_VALUES_EQUAL(state1.Data.size(), state2.Data.size());
                return true;
            }));
        
        UNIT_ASSERT_VALUES_EQUAL(state1.Sinks.size(), state2.Sinks.size());
        UNIT_ASSERT(std::equal(std::begin(state1.Sinks), std::end(state1.Sinks), std::begin(state2.Sinks), std::end(state2.Sinks),
            [](const NYql::NDq::TSinkState& state1, const NYql::NDq::TSinkState& state2) {
                UNIT_ASSERT_VALUES_EQUAL(state1.OutputIndex, state2.OutputIndex);
                UNIT_ASSERT_VALUES_EQUAL(state1.Data.Blob, state2.Data.Blob);
                UNIT_ASSERT_VALUES_EQUAL(state1.Data.Version, state2.Data.Version);
                return true;
            }));
    }

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::TActorSystemStub ActorSystemStub;
    TYqSharedResources::TPtr YqSharedResources;
};


} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStateStorageTest) {

    Y_UNIT_TEST_F(ShouldSaveGetOldSmallState, TFixture)
    {
        ShouldSaveGetStateImpl("TStateStorageTestShouldSaveGetState", MakeStateFromBlob(4));
    }

    Y_UNIT_TEST_F(ShouldSaveGetOldSmallState2Tasks, TFixture)
    {
        TString result;
        auto state1 = NKikimr::NMiniKQL::TOutputSerializer::MakeSimpleBlobState(TString(20, 'a'), 0);
        auto state2 = NKikimr::NMiniKQL::TOutputSerializer::MakeSimpleBlobState(TString(20, 'b'), 0);
        NKikimr::NMiniKQL::TNodeStateHelper::AddNodeState(result, state1.AsStringRef());
        NKikimr::NMiniKQL::TNodeStateHelper::AddNodeState(result, state2.AsStringRef());
        NYql::NDq::TComputeActorState state;
        state.MiniKqlProgram.ConstructInPlace().Data.Blob = result;
        ShouldSaveGetStateImpl("TStateStorageTestShouldSaveGetState", state);
    }

    Y_UNIT_TEST_F(ShouldSaveGetOldBigState, TFixture)
    {
        ShouldSaveGetStateImpl("TStateStorageTestShouldSaveGetState", MakeStateFromBlob(YdbRowSizeLimit * 4));
    }

    Y_UNIT_TEST_F(ShouldSaveGetIncrementSmallState, TFixture)
    {
        ShouldSaveGetStateImpl("ShouldSaveGetIncrementState", MakeIncrementState(10));
    }

    Y_UNIT_TEST_F(ShouldSaveGetIncrementBigState, TFixture)
    {
        ShouldSaveGetStateImpl("ShouldSaveGetIncrementState", MakeIncrementState(YdbRowSizeLimit * 5));    
    }

    Y_UNIT_TEST_F(ShouldNotGetNonExistendState, TFixture)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldNotGetNonExistendState");
        auto getResult = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST_F(ShouldCountStates, TFixture)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldCountStates");

        storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(2, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        storage->SaveState(3, "graph1", CheckpointId1, MakeIncrementState(YdbRowSizeLimit * 3)).GetValueSync();

        auto [count, issues] = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(count, 3);
    }

    Y_UNIT_TEST_F(ShouldCountStatesNonExistentCheckpoint, TFixture)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldCountStatesNonExistentCheckpoint");

        auto [count, issues] = storage->CountStates("graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        UNIT_ASSERT_VALUES_EQUAL(count, 0);
    }

    Y_UNIT_TEST_F(ShouldDeleteNoCheckpoints, TFixture)
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

    Y_UNIT_TEST_F(ShouldDeleteNoCheckpoints2, TFixture)
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

    Y_UNIT_TEST_F(ShouldDeleteCheckpoints, TFixture)
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

    Y_UNIT_TEST_F(ShouldDeleteGraph, TFixture)
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

    Y_UNIT_TEST_F(ShouldIssueErrorOnWrongGetStateParams, TFixture)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldIssueErrorOnWrongGetStateParams");

        auto getResult = storage->GetState({}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());

        getResult = storage->GetState({1, 1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST_F(ShouldIssueErrorOnNonExistentState, TFixture) {
        auto storage = GetStateStorage("TStateStorageTestShouldIssueErrorOnNonExistentState");

        auto [size, issues] = storage->SaveState(1, "graph1", CheckpointId1, MakeStateFromBlob(4)).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        UNIT_ASSERT(size > 0);

        auto getResult = storage->GetState({1}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT(!getResult.first.empty());

        getResult = storage->GetState({1, 2}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(!getResult.second.Empty());
        UNIT_ASSERT(getResult.first.empty());
    }

    Y_UNIT_TEST_F(ShouldGetMultipleStates, TFixture)
    {
        auto storage = GetStateStorage("TStateStorageTestShouldGetMultipleStates");
    
        auto state1 = MakeStateFromBlob(10);
        auto state2 = MakeIncrementState(10);
        auto state3 = MakeStateFromBlob(YdbRowSizeLimit * 6);
        auto state4 = MakeIncrementState(YdbRowSizeLimit * 3);

        auto [size, issues] = storage->SaveState(1, "graph1", CheckpointId1, state1).GetValueSync();
        UNIT_ASSERT(issues.Empty());
        UNIT_ASSERT(size > 0);
        issues = storage->SaveState(42, "graph1", CheckpointId1, state2).GetValueSync().second;
        UNIT_ASSERT(issues.Empty());
        issues = storage->SaveState(7, "graph1", CheckpointId1, state3).GetValueSync().second;
        UNIT_ASSERT(issues.Empty());
        issues = storage->SaveState(13, "graph1", CheckpointId1, state4).GetValueSync().second;
        UNIT_ASSERT(issues.Empty());

        auto [states, getIssues] = storage->GetState({1, 42, 7, 13}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT_C(getIssues.Empty(), getIssues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(states.size(), 4);

        CheckEquals(state1, states[0]);
        CheckEquals(state2, states[1]);
        CheckEquals(state3, states[2]);
        CheckEquals(state4, states[3]);

        // in different order
        auto [states2, getIssues2] = storage->GetState({42, 1, 13, 7}, "graph1", CheckpointId1).GetValueSync();
        UNIT_ASSERT(getIssues2.Empty());
        UNIT_ASSERT_VALUES_EQUAL(states2.size(), 4);
        CheckEquals(state2, states2[0]);
        CheckEquals(state1, states2[1]);
        CheckEquals(state4, states2[2]);
        CheckEquals(state3, states2[3]);
    }

    Y_UNIT_TEST_F(ShouldLoadLastSnapshot, TFixture)
    {
        auto storage = GetStateStorage("ShouldLoadLastSnapshot");

        auto state1 = MakeIncrementState({{"key1", "value1"}, {"key2", "value2"}}, {}, {});
        auto state2 = MakeIncrementState({{"key1", "value1"}, {"key2", "value2"}}, {}, {});

        SaveState(storage, 1, "graph1", CheckpointId1, state1);
        SaveState(storage, 1, "graph1", CheckpointId2, state2);

        auto state = GetState(storage, 1, "graph1", CheckpointId2);
        CheckEquals(state, state2);
    }

    Y_UNIT_TEST_F(ShouldNotGetNonExistendSnaphotState, TFixture)
    {
        auto storage = GetStateStorage("ShouldNotGetNonExistendSnaphotState");
        auto state = MakeIncrementState({}, {{"key1", "value1-new"}, {"key3", "value3"}}, {"key2"});
        SaveState(storage, 1, "graph1", CheckpointId4, state);

        auto [states, issues] = storage->GetState({1}, "graph1", CheckpointId4).GetValueSync();
        UNIT_ASSERT(!issues.Empty());
        UNIT_ASSERT(states.empty());
    }

    Y_UNIT_TEST_F(ShouldLoadIncrementSnapshot, TFixture)
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
        CheckEquals(expected, actual);
    }

};

} // namespace NFq
