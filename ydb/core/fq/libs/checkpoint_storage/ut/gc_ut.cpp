#include <ydb/core/fq/libs/checkpoint_storage/gc.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>
#include <ydb/core/fq/libs/checkpoint_storage/ydb_checkpoint_storage.h>
#include <ydb/core/fq/libs/checkpoint_storage/ydb_state_storage.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>

#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>

#include <util/system/env.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

namespace {

template<typename TValue>
class TProxyActor : public TActorBootstrapped<TProxyActor<TValue>> {
public:
    TProxyActor(NThreading::TPromise<TValue> p, std::function<TValue()> operation)
        : Promise(p)
        , Operation(operation) { }

    void Bootstrap() { Promise.SetValue(Operation()); }
    NThreading::TPromise<TValue> Promise;
    std::function<TValue()> Operation;
};

////////////////////////////////////////////////////////////////////////////////

NYql::NDq::TComputeActorState MakeStateFromBlob(size_t blobSize, bool isIncrement = false) {
    TString blob;
    blob.reserve(blobSize);
    for (size_t i = 0; i < blobSize; ++i) {
        blob += static_cast<TString::value_type>(std::rand() % 100);
    }

    NUdf::TUnboxedValue value;
    if (isIncrement) {
        std::map<TString, TString> increment{{"1", blob}};
        std::set<TString> deleted;
        NKikimr::NMiniKQL::TOutputSerializer::MakeIncrementState(increment, deleted, 0);
    } else {
        value = NKikimr::NMiniKQL::TOutputSerializer::MakeSimpleBlobState(blob, 0);
    }

    const TStringBuf savedBuf = value.AsStringRef();
    TString result;
    NKikimr::NMiniKQL::TNodeStateHelper::AddNodeState(result, savedBuf);
    NYql::NDq::TComputeActorState state;
    state.MiniKqlProgram.ConstructInPlace().Data.Blob = result;
    return state;
}

////////////////////////////////////////////////////////////////////////////////

template<bool UseYdbSdk>
class TGcTestBase: public NUnitTest::TTestBase {
    using TSelf = TGcTestBase<UseYdbSdk>;
    
    IYdbConnection::TPtr Connection;

    void SetUp() override {
        
        TablePrefix = CreateGuidAsString();
        if constexpr (!UseYdbSdk) {
            InitTestServer();
        }
        Connection = MakeConnection();
        Init();
    }

    IYdbConnection::TPtr MakeConnection() {
        YdbEndpoint = GetEnv("YDB_ENDPOINT");
        YdbDatabase = UseYdbSdk ? GetEnv("YDB_DATABASE") : Server->GetRuntime()->GetAppData().TenantName;

        Runtime = std::make_unique<TTestBasicRuntime>(1, true);
        Runtime->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NLog::PRI_DEBUG);
        SetupTabletServices(*Runtime);

        NConfig::TCheckpointCoordinatorConfig config;
        auto& storageConfig = *Config.MutableStorage();
        storageConfig.SetEndpoint(YdbEndpoint);
        storageConfig.SetDatabase(YdbDatabase);
        storageConfig.SetToken("");
        storageConfig.SetTablePrefix(TablePrefix);

        NYdb::TDriver driver({});
        if (UseYdbSdk) {
            return CreateSdkYdbConnection(storageConfig, NKikimr::CreateYdbCredentialsProviderFactory, driver);
        } else {
            return CreateLocalYdbConnection(YdbDatabase, TablePrefix);
        }
    }

    TTestActorRuntime* GetRuntime() {
        if constexpr (UseYdbSdk) {
            return Runtime.get();
        } else {
            return Server->GetRuntime();
        }
    }

    void Init() {
        CheckpointStorage = NewYdbCheckpointStorage(Config.GetStorage(), CreateEntityIdGenerator("id"), Connection);
        auto issues = Call<NThreading::TFuture<NYql::TIssues>>([&](){ return CheckpointStorage->Init({}); }).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        StateStorage = NewYdbStateStorage(Config, Connection);
        issues = Call<NThreading::TFuture<NYql::TIssues>>([&](){ return StateStorage->Init({});}).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        Fill();

        TCheckpointStorageSettings::TGcSettings gcConfig;
        auto gc = NewGC(gcConfig, CheckpointStorage, StateStorage);
        ActorGC = GetRuntime()->Register(gc.release());
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void InitTestServer() {
        MsgBusPort = PortManager.GetPort(2134);
        NKikimrProto::TAuthConfig authConfig;
        ServerSettings = MakeHolder<Tests::TServerSettings>(MsgBusPort, authConfig);
        ServerSettings->NodeCount = 1;
        Server = MakeHolder<Tests::TServer>(*ServerSettings);
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
        Server->GetRuntime()->SetLogPriority(NKikimrServices::STREAMS_STORAGE_SERVICE, NActors::NLog::PRI_DEBUG);
        Client->InitRootScheme();
    }

    void SaveCheckpoint(const TCoordinatorId& coordinator, const TCheckpointId& checkpointId, bool isIncrement) {
        auto createCheckpointResult = Call<NThreading::TFuture<ICheckpointStorage::TCreateCheckpointResult>>([&](){ return CheckpointStorage->CreateCheckpoint(coordinator, checkpointId, NProto::TCheckpointGraphDescription(), ECheckpointStatus::Pending); }).GetValueSync();
        UNIT_ASSERT_C(createCheckpointResult.second.Empty(), createCheckpointResult.second.ToString());

        Call<NThreading::TFuture<IStateStorage::TSaveStateResult>>([&](){ return StateStorage->SaveState(1, "graph", checkpointId, MakeStateFromBlob(4, isIncrement)); }).GetValueSync();
        Call<NThreading::TFuture<IStateStorage::TSaveStateResult>>([&](){ return StateStorage->SaveState(2, "graph", checkpointId, MakeStateFromBlob(4, isIncrement)); }).GetValueSync();

        Call<NThreading::TFuture<NYql::TIssues>>([&]() { return CheckpointStorage->UpdateCheckpointStatus(
            coordinator,
            checkpointId,
            ECheckpointStatus::PendingCommit,
            ECheckpointStatus::Pending,
            100); }).GetValueSync();

        Call<NThreading::TFuture<NYql::TIssues>>([&]() { return CheckpointStorage->UpdateCheckpointStatus(
            coordinator,
            checkpointId,
            ECheckpointStatus::Completed,
            ECheckpointStatus::PendingCommit,
            100); }).GetValueSync();
    }

    void Fill() {
        TCoordinatorId coordinator("graph", 11);
        auto issues = Call<NThreading::TFuture<NYql::TIssues>>([&](){ return CheckpointStorage->RegisterGraphCoordinator(coordinator); }).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());

        TCheckpointId checkpointId1(11, 1);
        SaveCheckpoint(coordinator, checkpointId1, false);

        TCheckpointId checkpointId2(11, 2);
        SaveCheckpoint(coordinator, checkpointId2, false);

        TCheckpointId checkpointId3(11, 3);
        SaveCheckpoint(coordinator, checkpointId3, false);
    }

    void CheckpointSucceeded(const TCheckpointId& checkpointUpperBound, NYql::NDqProto::ECheckpointType type = NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT) {
        TActorId sender = GetRuntime()->AllocateEdgeActor();

        TCoordinatorId coordinator("graph", 11);

        auto request = std::make_unique<TEvCheckpointStorage::TEvNewCheckpointSucceeded>(
            coordinator,
            checkpointUpperBound,
            type);

        auto handle = MakeHolder<IEventHandle>(ActorGC, sender, request.release());
        GetRuntime()->Send(handle.Release());
    }

    size_t CountGraphDescriptions() {
        TStringBuilder query;
        query << "--!syntax_v1" << Endl;
        query << "PRAGMA TablePathPrefix(\"" << NFq::JoinPath(YdbDatabase, TablePrefix) << "\");" << Endl;
        query << "SELECT * FROM checkpoints_graphs_description;" << Endl;
        Cerr << "Count graph descriptions query:\n" << query << Endl;
        std::optional<NYdb::NTable::TDataQueryResult> result;
        auto status = Call<NYdb::TAsyncStatus>([&]() { return Connection->GetTableClient()->RetryOperation([&](ISession::TPtr session) {
            return session->ExecuteDataQuery(query, ISession::TTxControl::BeginAndCommitTx(), nullptr)
                .Apply([&](const NThreading::TFuture<NYdb::NTable::TDataQueryResult>& selectResult) {
                    NYdb::TStatus status = selectResult.GetValue();
                    result = selectResult.GetValue();
                    return NThreading::MakeFuture<NYdb::TStatus>(status);
                });
            }); }).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        return result->GetResultSet(0).RowsCount();
    }

    template<typename TValue>
    auto Call(std::function<TValue()> operation) {
        if (UseYdbSdk) {
            return operation();     
        }
        auto promise = NThreading::NewPromise<TValue>();
        GetRuntime()->Register(new TProxyActor<TValue>(promise, operation));
        return promise.GetFuture().GetValueSync();
    }

    IStateStorage::TCountStatesResult CountStates(
        const TString& graphId,
        const TCheckpointId& checkpointId) {
        return Call<NThreading::TFuture<IStateStorage::TCountStatesResult>>([&](){ 
            return StateStorage->CountStates(graphId, checkpointId); }).GetValueSync();
    }

private:
    TPortManager PortManager;
    ui16 MsgBusPort = 0;
    ui16 GrpcPort = 0;
    THolder<Tests::TServerSettings> ServerSettings;
    THolder<Tests::TServer> Server;
    THolder<Tests::TClient> Client;
    NConfig::TCheckpointCoordinatorConfig Config;

    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;
    TString TablePrefix;
    TActorId ActorGC;
    TString YdbEndpoint;
    TString YdbDatabase;
    NKikimr::TActorSystemStub ActorSystemStub;
    std::unique_ptr<TTestActorRuntime> Runtime;

    UNIT_TEST_SUITE_DEMANGLE(TSelf);
    UNIT_TEST(ShouldRemovePreviousCheckpoints);
    UNIT_TEST(ShouldIgnoreIncrementCheckpoint);
    UNIT_TEST_SUITE_END();

    void ShouldRemovePreviousCheckpoints()
    {
        TCheckpointId checkpointId1(11, 1);
        TCheckpointId checkpointId2(11, 2);
        TCheckpointId checkpointId3(11, 3);

        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 3);

        CheckpointSucceeded(checkpointId3);

        ICheckpointStorage::TGetCheckpointsResult getResult;
        DoWithRetry<yexception>([&]() {
            getResult = Call<NThreading::TFuture<ICheckpointStorage::TGetCheckpointsResult>>([&](){ return CheckpointStorage->GetCheckpoints("graph"); }).GetValueSync();
            UNIT_ASSERT(getResult.second.Empty());
            if (getResult.first.size() == 3) {
                throw yexception() << "gc not finished yet";
            }
        }, TRetryOptions(100, TDuration::MilliSeconds(100)), true);

        UNIT_ASSERT_VALUES_EQUAL(getResult.first.size(), 1UL);
        UNIT_ASSERT_VALUES_EQUAL(getResult.first.front().CheckpointId, checkpointId3);

        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 1);

        IStateStorage::TCountStatesResult countResult;
        DoWithRetry<yexception>([&]() {
            countResult = CountStates("graph", checkpointId1);
            UNIT_ASSERT(countResult.second.Empty());
            if (countResult.first != 0) {
                throw yexception() << "gc not finished yet";
            }
        }, TRetryOptions(100, TDuration::MilliSeconds(100)), true);

        countResult = CountStates("graph", checkpointId2);
        UNIT_ASSERT(countResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 0UL);

        countResult = CountStates("graph", checkpointId3);
        UNIT_ASSERT(countResult.second.Empty());
        UNIT_ASSERT_VALUES_EQUAL(countResult.first, 2UL);
    }

    void ShouldIgnoreIncrementCheckpoint()
    {
        TCheckpointId checkpointId1(11, 1);
        TCheckpointId checkpointId2(11, 2);
        TCheckpointId checkpointId3(11, 3);

        UNIT_ASSERT_VALUES_EQUAL(CountGraphDescriptions(), 3);

        CheckpointSucceeded(checkpointId3,  NYql::NDqProto::CHECKPOINT_TYPE_INCREMENT_OR_SNAPSHOT);

        Sleep(TDuration::MilliSeconds(2000));
        ICheckpointStorage::TGetCheckpointsResult getResult = Call<NThreading::TFuture<ICheckpointStorage::TGetCheckpointsResult>>([&](){ return CheckpointStorage->GetCheckpoints("graph"); }).GetValueSync();
        UNIT_ASSERT(getResult.second.Empty());
        UNIT_ASSERT(getResult.first.size() == 3);
    }
};

using TGcTest = TGcTestBase<true>;
using TGcLocaTest = TGcTestBase<false>;

UNIT_TEST_SUITE_REGISTRATION(TGcTest);
UNIT_TEST_SUITE_REGISTRATION(TGcLocaTest);

} // namespace

} // namespace NFq
