#include <ydb/core/fq/libs/checkpoint_storage/gc.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>
#include <ydb/core/fq/libs/checkpoint_storage/ydb_checkpoint_storage.h>
#include <ydb/core/fq/libs/checkpoint_storage/ydb_state_storage.h>
#include <ydb/core/fq/libs/ydb/util.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <util/system/env.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NYql::NDqProto::TComputeActorState MakeStateFromBlob(const TString& blob) {
    NYql::NDqProto::TComputeActorState state;
    state.MutableMiniKqlProgram()->MutableData()->MutableStateData()->SetBlob(blob);
    return state;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestRuntime {
    std::unique_ptr<TTestActorRuntime> Runtime;
    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;
    TActorId ActorGC;
    TString TablePrefix;
    TString YdbEndpoint;
    TString YdbDatabase;
    std::unique_ptr<NYdb::TDriver> YdbDriver;
    std::unique_ptr<NYdb::NTable::TTableClient> YdbTableClient;

    TTestRuntime(const char* tablePrefix)
        : TablePrefix(tablePrefix)
        , YdbEndpoint(GetEnv("YDB_ENDPOINT"))
        , YdbDatabase(GetEnv("YDB_DATABASE"))
    {
        Runtime.reset(new TTestBasicRuntime(1));
        Runtime->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NLog::PRI_DEBUG);
        SetupTabletServices(*Runtime);

        NConfig::TYdbStorageConfig storageConfig;
        storageConfig.SetEndpoint(YdbEndpoint);
        storageConfig.SetDatabase(YdbDatabase);
        storageConfig.SetToken("");
        storageConfig.SetTablePrefix(TablePrefix);

        auto credFactory = NKikimr::CreateYdbCredentialsProviderFactory;
        auto yqSharedResources = NFq::TYqSharedResources::Cast(NFq::CreateYqSharedResourcesImpl({}, credFactory, MakeIntrusive<NMonitoring::TDynamicCounters>()));
        CheckpointStorage = NewYdbCheckpointStorage(storageConfig, credFactory, CreateEntityIdGenerator("id"), yqSharedResources);
        auto issues = CheckpointStorage->Init().GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        StateStorage = NewYdbStateStorage(storageConfig, credFactory, yqSharedResources);
        issues = StateStorage->Init().GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        Fill();

        NConfig::TCheckpointGcConfig gcConfig;
        auto gc = NewGC(gcConfig, CheckpointStorage, StateStorage);
        ActorGC = Runtime->Register(gc.release());

        Runtime->DispatchEvents({}, TDuration::Zero());

        YdbDriver = std::make_unique<NYdb::TDriver>(NYdb::TDriverConfig().SetEndpoint(YdbEndpoint).SetDatabase(YdbDatabase));
        YdbTableClient = std::make_unique<NYdb::NTable::TTableClient>(*YdbDriver);
    }

    void SaveCheckpoint(const TCoordinatorId& coordinator, const TCheckpointId& checkpointId) {
        auto createCheckpointResult = CheckpointStorage->CreateCheckpoint(coordinator, checkpointId, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());

        StateStorage->SaveState(1, "graph", checkpointId, MakeStateFromBlob("blob")).GetValueSync();
        StateStorage->SaveState(2, "graph", checkpointId, MakeStateFromBlob("blob")).GetValueSync();

        CheckpointStorage->UpdateCheckpointStatus(
            coordinator,
            checkpointId,
            ECheckpointStatus::PendingCommit,
            ECheckpointStatus::Pending,
            100).GetValueSync();

        CheckpointStorage->UpdateCheckpointStatus(
            coordinator,
            checkpointId,
            ECheckpointStatus::Completed,
            ECheckpointStatus::PendingCommit,
            100).GetValueSync();
    }

    void Fill() {
        TCoordinatorId coordinator("graph", 11);
        auto issues = CheckpointStorage->RegisterGraphCoordinator(coordinator).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        TCheckpointId checkpointId1(11, 1);
        SaveCheckpoint(coordinator, checkpointId1);

        TCheckpointId checkpointId2(11, 2);
        SaveCheckpoint(coordinator, checkpointId2);

        TCheckpointId checkpointId3(11, 3);
        SaveCheckpoint(coordinator, checkpointId3);
    }

    void CheckpointSucceeded(const TCheckpointId& checkpointUpperBound) {
        TActorId sender = Runtime->AllocateEdgeActor();

        TCoordinatorId coordinator("graph", 11);

        auto request = std::make_unique<TEvCheckpointStorage::TEvNewCheckpointSucceeded>(
            coordinator,
            checkpointUpperBound);

        auto handle = MakeHolder<IEventHandle>(ActorGC, sender, request.release());
        Runtime->Send(handle.Release());
    }

    size_t CountGraphDescriptions() {
        auto session = YdbTableClient->CreateSession().GetValueSync();
        UNIT_ASSERT(session.IsSuccess());
        TStringBuilder query;
        query << "--!syntax_v1" << Endl;
        query << "PRAGMA TablePathPrefix(\"" << NFq::JoinPath(YdbDatabase, TablePrefix) << "\");" << Endl;
        query << "SELECT * FROM checkpoints_graphs_description;" << Endl;
        Cerr << "Count graph descriptions query:\n" << query << Endl;
        auto queryResult = session.GetSession().ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(queryResult.IsSuccess(), queryResult.GetIssues().ToString());
        return queryResult.GetResultSet(0).RowsCount();
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TGcTest) {
    Y_UNIT_TEST(ShouldRemovePreviousCheckpoints)
    {
        TTestRuntime runtime("TGcTestShouldRemovePreviousCheckpoints");

        TCheckpointId checkpointId1(11, 1);
        TCheckpointId checkpointId2(11, 2);
        TCheckpointId checkpointId3(11, 3);

        UNIT_ASSERT_VALUES_EQUAL(runtime.CountGraphDescriptions(), 3);

        runtime.CheckpointSucceeded(checkpointId3);

        ICheckpointStorage::TGetCheckpointsResult getResult;
        DoWithRetry<yexception>([&]() {
            getResult = runtime.CheckpointStorage->GetCheckpoints("graph").GetValueSync();
            UNIT_ASSERT(getResult.second.Empty());
            if (getResult.first.size() == 3) {
                throw yexception() << "gc not finished yet";
            }
        }, TRetryOptions(100, TDuration::MilliSeconds(100)), true);

        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.front().CheckpointId, checkpointId3);

        UNIT_ASSERT_VALUES_EQUAL(runtime.CountGraphDescriptions(), 1);

        IStateStorage::TCountStatesResult countResult;
        DoWithRetry<yexception>([&]() {
            countResult = runtime.StateStorage->CountStates("graph", checkpointId1).GetValueSync();
            UNIT_ASSERT(countResult.second.Empty());
            if (countResult.first != 0) {
                throw yexception() << "gc not finished yet";
            }
        }, TRetryOptions(100, TDuration::MilliSeconds(100)), true);

        countResult = runtime.StateStorage->CountStates("graph", checkpointId2).GetValueSync();
        UNIT_ASSERT(countResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0UL);

        countResult = runtime.StateStorage->CountStates("graph", checkpointId3).GetValueSync();
        UNIT_ASSERT(countResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 2UL);
    }
};

} // namespace NFq
