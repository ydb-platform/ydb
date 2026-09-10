#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/gateway/behaviour/streaming_query/common/utils.h>
#include <ydb/core/kqp/gateway/behaviour/streaming_query/object.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/scheme_board/events_internal.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/services/metadata/abstract/events.h>
#include <ydb/services/metadata/abstract/service.h>

#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/json/json_reader.h>
#include <util/system/env.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYdb;
using namespace NYdb::NQuery;
using NMetadata::NProvider::TEvTrackOperationCompletion;
using NMetadata::NProvider::TEvTrackOperationFinished;

THolder<TEvTrackOperationCompletion> CopyTracking(const TEvTrackOperationCompletion& request) {
    auto result = MakeHolder<TEvTrackOperationCompletion>();
    result->SetDatabase(request.GetDatabase());
    result->SetDatabaseId(request.GetDatabaseId());
    result->SetTypeId(request.GetTypeId());
    result->SetObjectId(request.GetObjectId());
    result->SetPathId(request.GetPathId());
    result->SetRequestGeneration(request.GetRequestGeneration());
    result->SetObjectGeneration(request.GetObjectGeneration());
    result->SetOperationOwner(request.GetOperationOwner());
    result->SetUserToken(request.GetUserToken());
    return result;
}

struct TOwnerCrash final : TActorRunnableItem::TImpl<TOwnerCrash> {
    void DoRun(IActor* actor) noexcept {
        if (actor) {
            auto& context = *TlsActivationContext;
            context.ExecutorThread.UnregisterActor(&context.Mailbox, actor->SelfId());
        }
        delete this;
    }
};

struct TContinuationTest {
    static constexpr TStringBuf QueryName = "ContinuedQuery";
    static constexpr TStringBuf QueryPath = "/Root/ContinuedQuery";

    static std::shared_ptr<TKikimrRunner> CreateRunner(const TIntrusivePtr<NTestUtils::IMockPqGateway>& gateway, ui32 nodeCount) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableStreamingQueries(true);
        config.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(true);
        return NFederatedQueryTest::MakeKikimrRunner(false, nullptr, nullptr, config, NYql::NDq::CreateS3ActorsFactory(), {
            .NodeCount = nodeCount,
            .PqGateway = gateway,
            .UseLocalCheckpointsInStreamingQueries = true,
            .UseRealThreads = false,
        });
    }

    TIntrusivePtr<NTestUtils::IMockPqGateway> Gateway = NTestUtils::CreateMockPqGateway();
    std::shared_ptr<TKikimrRunner> Runner;
    TTestActorRuntime& Runtime = *Runner->GetTestServer().GetRuntime();
    TQueryClient Client = Runner->GetQueryClient(TClientSettings().AuthToken(BUILTIN_ACL_ROOT));
    TQueryClient MetadataClient = Runner->GetQueryClient(TClientSettings().AuthToken(BUILTIN_ACL_METADATA));
    TVector<THolder<TEvTrackOperationCompletion>> Tracking;
    ui64 Finished = 0;
    TTestActorRuntime::TEventObserverHolder TrackingObserver;
    TTestActorRuntime::TEventObserverHolder FinishedObserver;

    explicit TContinuationTest(bool initializeMetadata = true, ui32 nodeCount = 1)
        : Runner(CreateRunner(Gateway, nodeCount))
    {
        Runtime.SetRegistrationObserverFunc([](auto& runtime, const TActorId&, const TActorId& actor) {
            runtime.EnableScheduleForActor(actor);
        });
        for (ui32 node = 0; node < nodeCount; ++node) {
            Runtime.GetAppData(node).FeatureFlags.SetEnableStreamingQueries(true);
            Runtime.EnableScheduleForActor(Runtime.GetActorSystem(node)->LookupLocalService(
                NMetadata::NProvider::MakeServiceId(Runtime.GetNodeId(node))));
        }
        TrackingObserver = Runtime.AddObserver<TEvTrackOperationCompletion>([this](auto& ev) {
            if (ev->Get()->GetObjectId() == QueryName) {
                Tracking.push_back(CopyTracking(*ev->Get()));
            }
        });
        FinishedObserver = Runtime.AddObserver<TEvTrackOperationFinished>([this](auto& ev) {
            if (ev->Get()->GetObjectId() == QueryName) {
                ++Finished;
            }
        });

        const auto endpoint = GetEnv("YDB_ENDPOINT");
        const auto database = GetEnv("YDB_DATABASE");
        TDriver externalDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase(database));
        NTopic::TTopicClient topics(externalDriver);
        for (const auto* name : {"input", "output"}) {
            auto status = topics.CreateTopic(name, NTopic::TCreateTopicSettings().PartitioningSettings(1, 1)).GetValueSync();
            UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        }
        Exec(TStringBuilder() << "CREATE EXTERNAL DATA SOURCE Source WITH (SOURCE_TYPE = 'Ydb', LOCATION = '"
            << endpoint << "', DATABASE_NAME = '" << database << "', AUTH_METHOD = 'NONE');");
        if (initializeMetadata) {
            // Initialize metadata independently of the operation being interrupted.
            Exec("CREATE STREAMING QUERY Warmup WITH (RUN = FALSE) AS DO BEGIN "
                "INSERT INTO Source.output SELECT value FROM Source.input WITH (FORMAT = 'raw', SCHEMA (value String NOT NULL)); END DO;");
            Exec("DROP STREAMING QUERY Warmup;");
        }
    }

    auto Start(const TString& query) {
        return Client.ExecuteQuery(query, TTxControl::NoTx(),
            TExecuteQuerySettings().RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0)));
    }

    ui32 RemoteOwnerNode() {
        const auto schemeShard = ResolveTablet(Runtime, Tests::SchemeRoot);
        return schemeShard.NodeId() == Runtime.GetNodeId(0) ? 1 : 0;
    }

    TActorId StartOnNode(const TString& query, ui32 node) {
        const auto edge = Runtime.AllocateEdgeActor(node);
        auto request = MakeHolder<TEvKqp::TEvQueryRequest>();
        ActorIdToProto(edge, request->Record.MutableRequestActorId());
        request->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
        request->Record.MutableRequest()->SetDatabase("/Root");
        request->Record.MutableRequest()->SetQuery(query);
        NACLib::TUserToken token(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{});
        token.SaveSerializationInfo();
        request->Record.SetUserToken(token.GetSerializedToken());
        Runtime.Send(new IEventHandle(MakeKqpProxyID(Runtime.GetNodeId(node)), edge, request.Release()), node);
        return edge;
    }

    TExecuteQueryResult Exec(const TString& query, EStatus status = EStatus::SUCCESS) {
        auto result = Runtime.WaitFuture(Start(query), TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
        return result;
    }

    TExecuteQueryResult ExecMetadata(const TString& query) {
        auto result = Runtime.WaitFuture(MetadataClient.ExecuteQuery(query, TTxControl::NoTx()), TDuration::Seconds(60));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    }

    static TString CreateQuery() {
        return "CREATE STREAMING QUERY ContinuedQuery WITH (RUN = FALSE) AS DO BEGIN "
            "INSERT INTO Source.output SELECT value FROM Source.input WITH (FORMAT = 'raw', SCHEMA (value String NOT NULL)); END DO;";
    }

    void Replay(const TEvTrackOperationCompletion& request, ui32 node = 0) {
        Runtime.Send(new IEventHandle(NMetadata::NProvider::MakeServiceId(Runtime.GetNodeId(node)),
            Runtime.AllocateEdgeActor(node), CopyTracking(request).Release()), node);
    }

    auto Describe() {
        return Navigate(Runtime, Runtime.AllocateEdgeActor(), TString(QueryPath), NSchemeCache::TSchemeCacheNavigate::OpPath);
    }

    NKikimrKqp::TStreamingQueryState CheckRow(bool provisional = false) {
        auto result = ExecMetadata("SELECT state, expire_at FROM `.metadata/streaming/queries` WHERE query_path = '/Root/ContinuedQuery';");
        TResultSetParser rows(result.GetResultSet(0));
        UNIT_ASSERT_VALUES_EQUAL(rows.RowsCount(), 1);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("expire_at").GetOptionalTimestamp().has_value(), provisional);
        const auto json = rows.ColumnParser("state").GetOptionalJson();
        UNIT_ASSERT(json);
        NJson::TJsonValue value;
        UNIT_ASSERT(NJson::ReadJsonTree(*json, &value));
        NKikimrKqp::TStreamingQueryState state;
        NProtobufJson::Json2Proto(value, state);
        return state;
    }

    void CheckQueriesTableTtl() {
        auto client = Runner->GetTableClient(NYdb::NTable::TClientSettings().AuthToken(BUILTIN_ACL_METADATA));
        auto session = Runtime.WaitFuture(client.CreateSession());
        UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
        const auto description = Runtime.WaitFuture(session.GetSession().DescribeTable("/Root/.metadata/streaming/queries"));
        UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
        const auto& ttl = description.GetTableDescription().GetTtlSettings();
        UNIT_ASSERT(ttl);
        UNIT_ASSERT(ttl->GetMode() == NYdb::NTable::TTtlSettings::EMode::DateTypeColumn);
        UNIT_ASSERT_VALUES_EQUAL(ttl->GetDateTypeColumn().GetColumnName(), "expire_at");
    }

    void CheckSchemeSettled() {
        auto description = Describe();
        const auto& entry = description->ResultSet.at(0);
        UNIT_ASSERT_VALUES_EQUAL(entry.Status, NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        UNIT_ASSERT(entry.StreamingQueryInfo);
        const auto& info = entry.StreamingQueryInfo->Description;
        UNIT_ASSERT(!ActorIdFromProto(info.GetOperationOwnerActorId()));
        UNIT_ASSERT(!info.GetProperties().GetProperties().contains(TStreamingQueryMeta::TProperties::InflightOperation));
    }

    void CheckSettled() {
        CheckSchemeSettled();
        const auto state = CheckRow();
        UNIT_ASSERT(!state.HasOperationActorId());
        UNIT_ASSERT(!state.HasOperationOwnerGeneration());
    }

    void CheckDropped() {
        const auto description = Describe();
        UNIT_ASSERT_VALUES_EQUAL(description->ResultSet.at(0).Status, NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown);
        auto result = ExecMetadata("SELECT * FROM `.metadata/streaming/queries` WHERE query_path = '/Root/ContinuedQuery';");
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 0);
    }

    template <typename TCondition>
    void WaitFor(const TString& description, const TCondition& condition) {
        Runtime.WaitFor(description, condition, TDuration::Seconds(60));
    }

    void WaitFinished(ui64 count) {
        Runtime.WaitFor("operation continuation finished", [&] { return Finished >= count; }, TDuration::Seconds(60));
    }

    void CrashOwner(TActorId owner) {
        // Simulate owner loss without running operation cleanup or acknowledging the client.
        UNIT_ASSERT(Runtime.FindActor(owner));
        const auto node = owner.NodeId() - Runtime.GetFirstNodeId();
        Runtime.Send(new IEventHandle(owner, Runtime.AllocateEdgeActor(node),
            new TEvents::TEvResumeRunnable(new TOwnerCrash()), TEvents::TEvResumeRunnable::EventFlags), node);
        WaitFor("operation owner unregistered", [&] { return !Runtime.FindActor(owner); });
    }

    void DropInSchemeShard() {
        const auto edge = Runtime.AllocateEdgeActor();
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        request->Record.SetDatabaseName("/Root");
        auto& tx = *request->Record.MutableTransaction()->MutableModifyScheme();
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropStreamingQuery);
        tx.MutableDrop()->SetName(TString(QueryName));
        Runtime.Send(new IEventHandle(MakeTxProxyID(), edge, request.Release()));
        auto response = Runtime.GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransactionStatus>(edge);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Status(), NTxProxy::TResultStatus::ExecInProgress);
        const auto& record = response->Get()->Record;
        Runtime.SendToPipe(record.GetSchemeShardTabletId(), edge,
            new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion(record.GetTxId()));
        UNIT_ASSERT(Runtime.GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult>(edge));
    }
};

bool IsLockRequest(const TEvKqp::TEvQueryRequest::TPtr& ev) {
    return ev->Get()->GetQuery().Contains("-- TLockStreamingQueryRequestActor::ReadQueryInfo");
}

struct TMainCheckAliveRequest : TEventPB<TMainCheckAliveRequest, google::protobuf::Empty,
    EventSpaceBegin(TEvents::ES_PRIVATE) + 8> {};
struct TMainCheckAliveResponse : TEventPB<TMainCheckAliveResponse, google::protobuf::Empty,
    EventSpaceBegin(TEvents::ES_PRIVATE) + 9> {};

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpStreamingOperationContinuation) {
    Y_UNIT_TEST(MixedVersionLivenessPreservesWireIds) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        auto result = f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation awaiting its row lock", [&] { return !tracking.empty() && !locking.empty(); });
        const auto owner = tracking.front()->Get()->GetOperationOwner();
        TActorId checker;
        bool replied = false;
        auto wire = f.Runtime.AddObserver([&](auto& ev) {
            if (ev->Recipient == owner) {
                UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TMainCheckAliveRequest::EventType);
                checker = ev->Sender;
            } else if (checker && ev->Recipient == checker && ev->Sender == owner) {
                UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TMainCheckAliveResponse::EventType);
                replied = true;
            } else {
                return;
            }
            // Deserialize at the receiver, as Interconnect does between different versions.
            ev.Reset(new IEventHandle(ev->GetTypeRewrite(), ev->Flags, ev->Recipient, ev->Sender,
                ev->ReleaseChainBuffer(), ev->Cookie));
        });
        tracking.Unblock().Stop();
        f.WaitFor("owner answered the compatible liveness probe", [&] { return replied; });
        UNIT_ASSERT(!result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
        wire.Remove();
        locking.Unblock().Stop();
        UNIT_ASSERT_VALUES_EQUAL(f.Runtime.WaitFuture(result).GetStatus(), EStatus::SUCCESS);
        f.WaitFinished(1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(MixedVersionSchemeShardWithoutOperationOwner) {
        TContinuationTest f;
        ui64 registrations = 0;
        auto legacySchemeShard = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            auto* tx = ev->Get()->Record.MutableTransaction()->MutableModifyScheme();
            if (!tx->HasCreateStreamingQuery()) {
                return;
            }
            auto* query = tx->MutableCreateStreamingQuery();
            if (query->GetName() == TContinuationTest::QueryName && query->HasOperationOwnerActorId()) {
                query->ClearOperationOwnerActorId();
                ++registrations;
            }
        });
        f.Exec(TContinuationTest::CreateQuery());
        f.CheckSettled();
        f.Exec("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.CheckSettled();
        f.Exec("CREATE OR REPLACE STREAMING QUERY ContinuedQuery WITH (RUN = FALSE) AS DO BEGIN "
            "INSERT INTO Source.output SELECT value FROM Source.input WITH (FORMAT = 'raw', SCHEMA (value String NOT NULL)); END DO;");
        f.CheckSettled();
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
        UNIT_ASSERT_VALUES_EQUAL(registrations, 4);
        UNIT_ASSERT(f.Tracking.empty());
    }

    Y_UNIT_TEST_TWIN(MixedVersionLegacyMetadataStates, Drop) {
        TContinuationTest f;
        using TState = NKikimrKqp::TStreamingQueryState;
        for (const auto status : {TState::STATUS_UNSPECIFIED, TState::STATUS_CREATING, TState::STATUS_CREATED,
            TState::STATUS_STARTING, TState::STATUS_RUNNING, TState::STATUS_STOPPING, TState::STATUS_STOPPED,
            TState::STATUS_DELETING}) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(f.Tracking.size());
            const auto deadOwner = f.Tracking.back()->GetOperationOwner();
            UNIT_ASSERT(!f.Runtime.FindActor(deadOwner));
            auto state = f.CheckRow();
            state.SetStatus(status);
            state.SetOperationActorId(ScriptExecutionRunnerActorIdString(deadOwner));
            state.ClearOperationOwnerGeneration();
            state.MutableSchemeInfo()->SetAlterVersion(
                f.Describe()->ResultSet.at(0).Self->Info.GetVersion().GetStreamingQueryVersion());
            auto json = NProtobufJson::Proto2Json(state);
            UNIT_ASSERT(json.EndsWith('}'));
            json.pop_back();
            // main writes these fields and does not know OperationOwnerGeneration.
            json += R"(,"OperationName":"ALTER STREAMING QUERY","OperationStartedAt":{"seconds":1},"QueryText":"legacy text","Run":false,"ResourcePool":""})";
            TParamsBuilder params;
            params.AddParam("$state").Json(json).Build();
            const auto updated = f.Runtime.WaitFuture(f.MetadataClient.ExecuteQuery(
                "DECLARE $state AS Json; UPDATE `.metadata/streaming/queries` SET state = $state "
                "WHERE query_path = '/Root/ContinuedQuery';", TTxControl::NoTx(), params.Build()));
            UNIT_ASSERT_C(updated.IsSuccess(), updated.GetIssues().ToString());

            if constexpr (!Drop) {
                f.Exec("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
                f.CheckSettled();
                const auto actual = f.CheckRow().GetStatus();
                UNIT_ASSERT_C(actual == TState::STATUS_CREATED || actual == TState::STATUS_STOPPED,
                    "Legacy state " << TState::EStatus_Name(status) << " was not synchronized: " << TState::EStatus_Name(actual));
            }
            f.Exec("DROP STREAMING QUERY ContinuedQuery;");
            f.CheckDropped();
        }
    }

    Y_UNIT_TEST_TWIN(MixedVersionOwnerlessDescriptionRejectsChangedObject, PathChanged) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);
        const auto initialState = f.CheckRow().SerializeAsString();
        bool registered = false;
        auto legacySchemeShard = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            auto* tx = ev->Get()->Record.MutableTransaction()->MutableModifyScheme();
            if (tx->HasCreateStreamingQuery() && tx->GetCreateStreamingQuery().GetName() == TContinuationTest::QueryName
                && tx->GetCreateStreamingQuery().HasOperationOwnerActorId()) {
                tx->MutableCreateStreamingQuery()->ClearOperationOwnerActorId();
                registered = true;
            }
        });
        bool changed = false;
        auto descriptions = f.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            for (auto& entry : ev->Get()->Request->ResultSet) {
                if (registered && entry.SyncVersion && entry.Path == SplitPath(TString(TContinuationTest::QueryPath)) && entry.Self) {
                    auto self = MakeIntrusive<NSchemeCache::TSchemeCacheNavigate::TDirEntryInfo>(*entry.Self);
                    if constexpr (PathChanged) {
                        self->Info.SetPathId(self->Info.GetPathId() + 1);
                    } else {
                        auto* version = self->Info.MutableVersion();
                        version->SetStreamingQueryVersion(version->GetStreamingQueryVersion() + 1);
                    }
                    entry.Self = std::move(self);
                    changed = true;
                }
            }
        });
        f.Exec("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);", EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT(changed);
        UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().SerializeAsString(), initialState);
        descriptions.Remove();
        legacySchemeShard.Remove();
        f.Exec("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.CheckSettled();
    }

    Y_UNIT_TEST(MixedVersionRecreatesQueryWithLegacyOrphanRow) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);
        const auto oldPathId = f.CheckRow().GetSchemeInfo().GetLocalPathId();
        // main can finish the scheme drop and lose its owner before removing the row.
        f.DropInSchemeShard();
        UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().GetSchemeInfo().GetLocalPathId(), oldPathId);

        f.Exec(TContinuationTest::CreateQuery());
        f.CheckSettled();
        const auto state = f.CheckRow();
        UNIT_ASSERT(state.GetSchemeInfo().GetLocalPathId() > oldPathId);
        UNIT_ASSERT_VALUES_EQUAL(state.GetSchemeInfo().GetLocalPathId(),
            f.Describe()->ResultSet.at(0).Self->Info.GetPathId());
        f.Exec("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.CheckSettled();
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST(MixedVersionDelayedLockDoesNotChangeRecreatedQuery) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        auto result = f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("old operation awaiting its row lock", [&] { return !tracking.empty() && !locking.empty(); });
        locking.Stop();
        f.DropInSchemeShard();
        f.Exec(TContinuationTest::CreateQuery());
        f.CheckSettled();
        const auto recreated = f.CheckRow().SerializeAsString();

        locking.Unblock();
        UNIT_ASSERT_VALUES_EQUAL(f.Runtime.WaitFuture(result).GetStatus(), EStatus::PRECONDITION_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().SerializeAsString(), recreated);
        tracking.Unblock().Stop();
        f.WaitFinished(2);
        f.CheckSettled();
    }

    Y_UNIT_TEST(MixedVersionPreservesDisabledMetadataTtl) {
        TContinuationTest f(false);
        // Pre-stage the added column before rolling out new nodes, keeping TTL disabled.
        TVector<NKikimrSchemeOp::TColumnDescription> columns;
        for (const auto& [name, type] : TVector<std::pair<TString, TString>>{
            {"database_id", "Utf8"}, {"query_path", "Utf8"}, {"state", "Json"}, {"expire_at", "Timestamp"}}) {
            auto& column = columns.emplace_back();
            column.SetName(name);
            column.SetType(type);
        }
        const auto edge = f.Runtime.AllocateEdgeActor();
        f.Runtime.Register(CreateTableCreator({".metadata", "streaming", "queries"},
            std::move(columns), {"database_id", "query_path"}, NKikimrServices::KQP_PROXY),
            0, 0, TMailboxType::Simple, 0, edge);
        const auto created = f.Runtime.GrabEdgeEvent<TEvTableCreator::TEvCreateTableResponse>(edge);
        UNIT_ASSERT_C(created && created->Get()->Success, "Could not pre-stage queries table");
        UNIT_ASSERT_VALUES_EQUAL(f.ExecMetadata("SELECT * FROM `.metadata/streaming/queries`;").GetResultSet(0).ColumnsCount(), 4);

        f.Exec(TContinuationTest::CreateQuery());
        f.CheckSettled();
        auto client = f.Runner->GetTableClient(NYdb::NTable::TClientSettings().AuthToken(BUILTIN_ACL_METADATA));
        auto session = f.Runtime.WaitFuture(client.CreateSession());
        UNIT_ASSERT_C(session.IsSuccess(), session.GetIssues().ToString());
        const auto description = f.Runtime.WaitFuture(session.GetSession().DescribeTable("/Root/.metadata/streaming/queries"));
        UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
        UNIT_ASSERT(!description.GetTableDescription().GetTtlSettings());
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST(CreateAlterDropAndValidationErrorsFinish) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.CheckSettled();
        f.Exec("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.CheckSettled();
        f.Exec(TContinuationTest::CreateQuery(), EStatus::SCHEME_ERROR);
        f.CheckSettled();
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
        f.Exec("DROP STREAMING QUERY IF EXISTS ContinuedQuery;");
    }

    Y_UNIT_TEST_TWIN(SchemeShardContinuesAfterOwnerStops, Reboot) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("durable operation waiting for row lock", [&] { return !tracking.empty() && !locking.empty(); });
        const auto& request = *tracking.front()->Get();
        const auto schemeShard = request.GetPathId().OwnerId;
        f.CrashOwner(request.GetOperationOwner());
        locking.Stop().clear();

        if constexpr (Reboot) {
            RebootTablet(f.Runtime, schemeShard, f.Runtime.AllocateEdgeActor());
            f.WaitFor("SchemeShard resumed persisted operation", [&] { return tracking.size() >= 2; });
            UNIT_ASSERT(tracking.back()->Get()->GetRequestGeneration() > tracking.front()->Get()->GetRequestGeneration());
        }
        const auto requests = tracking.size();
        tracking.Unblock().Stop();
        f.WaitFinished(requests);
        f.CheckSettled();
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST_TWIN(MultiNodeSchemeShardContinuesAfterOwnerStops, Reboot) {
        TContinuationTest f(true, 2);
        for (const auto& query : {TContinuationTest::CreateQuery(), TString("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);"),
                TString("DROP STREAMING QUERY ContinuedQuery;")}) {
            const auto finished = f.Finished;
            const auto node = f.RemoteOwnerNode();
            TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
            TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
            f.StartOnNode(query, node);
            f.WaitFor("remote operation registered before locking", [&] { return !tracking.empty() && !locking.empty(); });
            const auto request = CopyTracking(*tracking.front()->Get());
            const auto owner = request->GetOperationOwner();
            UNIT_ASSERT_VALUES_EQUAL(owner.NodeId(), f.Runtime.GetNodeId(node));
            UNIT_ASSERT_VALUES_UNEQUAL(tracking.front()->Sender.NodeId(), owner.NodeId());
            // Tracking is local to SchemeShard; only the owner checks cross Interconnect.
            UNIT_ASSERT_VALUES_EQUAL(tracking.front()->Sender.NodeId(), tracking.front()->Recipient.NodeId());

            f.CrashOwner(owner);
            locking.Stop().clear();
            if constexpr (Reboot) {
                RebootTablet(f.Runtime, request->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
                f.WaitFor("SchemeShard restored the remote owner's operation", [&] { return tracking.size() >= 2; });
                UNIT_ASSERT(tracking.back()->Get()->GetRequestGeneration() > request->GetRequestGeneration());
            }

            ui64 remoteNondeliveries = 0;
            auto undelivered = f.Runtime.AddObserver<TEvents::TEvUndelivered>([&](auto& ev) {
                if (ev->Sender == owner && ev->Get()->SourceType == TMainCheckAliveRequest::EventType) {
                    UNIT_ASSERT_VALUES_UNEQUAL(ev->Sender.NodeId(), ev->Recipient.NodeId());
                    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
                    ++remoteNondeliveries;
                }
            });
            const auto requests = tracking.size();
            tracking.Unblock().Stop();
            f.WaitFinished(finished + requests);
            UNIT_ASSERT(remoteNondeliveries);
            if (query.StartsWith("DROP")) {
                f.CheckDropped();
            } else {
                f.CheckSettled();
            }
        }
    }

    Y_UNIT_TEST_TWIN(MultiNodeTrackerRetriesAfterDisconnect, StopOwner) {
        TContinuationTest f(true, 2);
        const auto node = f.RemoteOwnerNode();
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        const auto edge = f.StartOnNode(TContinuationTest::CreateQuery(), node);
        f.WaitFor("remote owner waiting for its lock", [&] { return !tracking.empty() && !locking.empty(); });
        const auto owner = tracking.front()->Get()->GetOperationOwner();
        UNIT_ASSERT_VALUES_EQUAL(owner.NodeId(), f.Runtime.GetNodeId(node));
        UNIT_ASSERT_VALUES_UNEQUAL(tracking.front()->Sender.NodeId(), owner.NodeId());

        TActorId checker;
        ui64 replies = 0;
        bool disconnected = false;
        auto checks = f.Runtime.AddObserver([&](auto& ev) {
            if (ev->Sender == owner && ev->GetTypeRewrite() == TMainCheckAliveResponse::EventType) {
                UNIT_ASSERT_VALUES_UNEQUAL(ev->Sender.NodeId(), ev->Recipient.NodeId());
                UNIT_ASSERT(ev->InterconnectSession);
                if (++replies == 1) {
                    checker = ev->Recipient;
                    // Lose the first reply with the session so the same checker must retry.
                    ev.Reset();
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, checker);
                }
            } else if (ev->Recipient == checker && ev->GetTypeRewrite() == TEvInterconnect::TEvNodeDisconnected::EventType) {
                disconnected = true;
            }
        });
        tracking.Unblock().Stop();
        f.WaitFor("remote owner answered the first probe", [&] { return replies; });
        f.Runtime.DisconnectNodes(checker.NodeId() - f.Runtime.GetFirstNodeId(), node);
        f.WaitFor("owner checker observed the disconnected session", [&] { return disconnected; });
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);

        if constexpr (StopOwner) {
            f.CrashOwner(owner);
            locking.Stop().clear();
        } else {
            f.WaitFor("owner checker retried after reconnection", [&] { return replies >= 2; });
            UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
            checks.Remove();
            locking.Unblock().Stop();
            const auto response = f.Runtime.GrabEdgeEvent<TEvKqp::TEvQueryResponse>(edge, TDuration::Seconds(60));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS,
                response->Get()->Record.DebugString());
        }
        f.WaitFinished(1);
        f.CheckSettled();
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST_TWIN(UserTransactionLostReplyIsCompletedByTracker, Begin) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        bool injected = false;
        TActorId transactionActor;
        auto proposes = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (!injected && query.GetName() == TContinuationTest::QueryName && query.HasOperationOwnerActorId() == Begin) {
                if constexpr (!Begin) {
                    const NACLib::TUserToken token(ev->Get()->Record.GetUserToken());
                    UNIT_ASSERT_VALUES_EQUAL(token.GetUserSID(), BUILTIN_ACL_METADATA);
                }
                transactionActor = ev->Sender;
            }
        });
        auto responses = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransactionStatus>([&](auto& ev) {
            if (!injected && ev->Recipient == transactionActor && ev->Get()->Status() == NTxProxy::TResultStatus::ExecInProgress) {
                injected = true;
                ev->Get()->Record.SetStatus(NTxProxy::TResultStatus::ProxyShardNotAvailable);
                ev->Get()->Record.SetSchemeShardStatus(NKikimrScheme::StatusNotAvailable);
            }
        });
        f.Exec(TContinuationTest::CreateQuery(), EStatus::UNAVAILABLE);
        UNIT_ASSERT(injected);
        f.WaitFor("committed operation registered for continuation", [&] { return !tracking.empty(); });
        tracking.Unblock().Stop();
        f.WaitFinished(1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(OwnerAndTrackerKeepWaitingForSlowOperation) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetObjectId() == TContinuationTest::QueryName;
        });
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        auto result = f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation waiting for an unresponsive row request", [&] { return !tracking.empty() && !locking.empty(); });
        const auto owner = tracking.front()->Get()->GetOperationOwner();
        ui64 ownerChecks = 0;
        auto ownerObserver = f.Runtime.AddObserver([&](auto& ev) {
            if (ev->Recipient == owner) {
                ++ownerChecks;
            }
        });
        tracking.Unblock().Stop();
        f.Runtime.AdvanceCurrentTime(TDuration::Minutes(6));
        f.WaitFor("tracker still monitors the original owner", [&] { return ownerChecks >= 2 || result.HasValue() || f.Finished; });
        UNIT_ASSERT(!result.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
        locking.Unblock().Stop();
        UNIT_ASSERT_VALUES_EQUAL(f.Runtime.WaitFuture(result).GetStatus(), EStatus::SUCCESS);
        f.WaitFinished(1);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(UserBeginTransactionErrorReturnsWithoutRetry, Alter) {
        TContinuationTest f;
        if constexpr (Alter) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(1);
        }
        ui64 requests = 0;
        auto failures = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() != TContinuationTest::QueryName || !query.HasOperationOwnerActorId()) {
                return;
            }
            if (++requests == 1) {
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
                response->Record.SetStatus(NTxProxy::TResultStatus::ProxyShardNotAvailable);
                response->Record.SetSchemeShardStatus(NKikimrScheme::StatusNotAvailable);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        const auto query = Alter ? TString("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);") : TContinuationTest::CreateQuery();
        f.Exec(query, EStatus::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(requests, 1);
        if constexpr (Alter) {
            f.CheckSettled();
        } else {
            f.CheckDropped();
        }
        failures.Remove();
        f.Exec(query);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(UserStateErrorAttemptsSchemeFinalization, FailFinalization) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        bool stateError = false;
        auto stateFailure = f.Runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (stateError || !ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::PersistQueryInfo")) {
                return;
            }
            const auto& params = ev->Get()->GetYdbParameters();
            if (params.at("$query_path").value().text_value() != TContinuationTest::QueryPath) {
                return;
            }
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(params.at("$state").value().text_value(), &json));
            NKikimrKqp::TStreamingQueryState state;
            NProtobufJson::Json2Proto(json, state);
            if (state.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_CREATED) {
                stateError = true;
                auto response = MakeHolder<TEvKqp::TEvQueryResponse>();
                response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        ui64 finalizations = 0;
        auto finalizationFailure = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() != TContinuationTest::QueryName || query.HasOperationOwnerActorId()) {
                return;
            }
            ++finalizations;
            if constexpr (FailFinalization) {
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
                response->Record.SetStatus(NTxProxy::TResultStatus::ProxyShardNotAvailable);
                response->Record.SetSchemeShardStatus(NKikimrScheme::StatusNotAvailable);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        f.Exec(TContinuationTest::CreateQuery(), FailFinalization ? EStatus::UNAVAILABLE : EStatus::BAD_REQUEST);
        UNIT_ASSERT(stateError);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        const auto state = f.CheckRow();
        UNIT_ASSERT(!state.HasOperationActorId());
        const auto description = f.Describe();
        UNIT_ASSERT_VALUES_EQUAL(bool(ActorIdFromProto(description->ResultSet.at(0).StreamingQueryInfo->Description.GetOperationOwnerActorId())), FailFinalization);

        finalizationFailure.Remove();
        tracking.Unblock().Stop();
        f.WaitFinished(1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(UserFindsCompletedRowReturnsPreconditionFailed) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        auto result = f.Start("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.WaitFor("operation awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });

        // Another owner committed the row unlock before this request acquired the lock.
        auto state = f.CheckRow();
        UNIT_ASSERT(!state.HasOperationActorId());
        state.MutableSchemeInfo()->SetAlterVersion(tracking.front()->Get()->GetObjectGeneration());
        TParamsBuilder params;
        params.AddParam("$state").Json(NProtobufJson::Proto2Json(state)).Build();
        const auto updated = f.Runtime.WaitFuture(f.MetadataClient.ExecuteQuery(
            "DECLARE $state AS Json; UPDATE `.metadata/streaming/queries` SET state = $state "
            "WHERE query_path = '/Root/ContinuedQuery';", TTxControl::NoTx(), params.Build()), TDuration::Seconds(60));
        UNIT_ASSERT_C(updated.IsSuccess(), updated.GetIssues().ToString());

        ui64 finalizations = 0;
        auto observer = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++finalizations;
            }
        });
        locking.Unblock().Stop();
        const auto response = f.Runtime.WaitFuture(result, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), EStatus::PRECONDITION_FAILED, response.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        f.CheckSettled();
        tracking.Unblock().Stop();
        f.WaitFinished(2);
    }

    Y_UNIT_TEST_TWIN(UnlockErrorRetriesOnlyInTracker, Tracker) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        auto result = f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });
        ui64 unlocks = 0;
        auto failure = f.Runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Get()->GetQuery().Contains("-- TUnlockStreamingQueryRequestActor::ReadQueryInfo")) {
                ++unlocks;
                auto response = MakeHolder<TEvKqp::TEvQueryResponse>();
                response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        ui64 finalizations = 0;
        auto observer = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++finalizations;
            }
        });
        if constexpr (Tracker) {
            f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
            locking.Stop().clear();
            tracking.Unblock().Stop();
            f.WaitFor("tracker retries failed unlock", [&] { return unlocks >= 2; });
            UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);
            UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
            UNIT_ASSERT(f.CheckRow().HasOperationActorId());
            failure.Remove();
        } else {
            locking.Unblock().Stop();
            const auto response = f.Runtime.WaitFuture(result, TDuration::Seconds(60));
            UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), EStatus::BAD_REQUEST, response.GetIssues().ToString());
            tracking.Unblock().Stop();
        }
        f.WaitFinished(1);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        if constexpr (Tracker) {
            f.CheckSettled();
        } else {
            UNIT_ASSERT_VALUES_EQUAL(unlocks, 1);
            f.CheckSchemeSettled();
            UNIT_ASSERT(f.CheckRow().HasOperationActorId());
            failure.Remove();
        }
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST_TWIN(ProxyNotReadyRetriesInTransactionActor, TryLater) {
        TContinuationTest f;
        ui64 requests = 0;
        TActorId transactionActor;
        auto failures = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() != TContinuationTest::QueryName || !query.HasOperationOwnerActorId()) {
                return;
            }
            if (++requests == 1) {
                transactionActor = ev->Sender;
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>(TryLater
                    ? NTxProxy::TResultStatus::ProxyShardTryLater : NTxProxy::TResultStatus::ProxyNotReady);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            } else {
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender, transactionActor);
            }
        });
        f.Exec(TContinuationTest::CreateQuery());
        UNIT_ASSERT_VALUES_EQUAL(requests, 2);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(SchemeTransactionForwardsSerializedUserToken, Serialized) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);

        NMetadata::NModifications::IOperationsManager::TExternalModificationContext context;
        context.SetDatabase("/Root");
        context.SetDatabaseId(f.Tracking.back()->GetDatabaseId());
        context.SetActorSystem(f.Runtime.GetActorSystem(0));
        NACLib::TUserToken token(BUILTIN_ACL_ROOT, TVector<NACLib::TSID>{});
        if constexpr (Serialized) {
            token.SaveSerializationInfo();
        }
        UNIT_ASSERT_VALUES_EQUAL(!token.GetSerializedToken().empty(), Serialized);
        context.SetUserToken(token);

        ui64 begins = 0;
        ui64 finalizations = 0;
        auto requests = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() != TContinuationTest::QueryName) {
                return;
            }
            if (query.HasOperationOwnerActorId()) {
                ++begins;
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetUserToken(), token.GetSerializedToken());
            } else {
                ++finalizations;
                UNIT_ASSERT_VALUES_EQUAL(NACLib::TUserToken(ev->Get()->Record.GetUserToken()).GetUserSID(), BUILTIN_ACL_METADATA);
            }
        });
        NKqpProto::TKqpSchemeOperation operation;
        auto& tx = *operation.MutableAlterStreamingQuery();
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
        tx.MutableCreateStreamingQuery()->SetName(TString(TContinuationTest::QueryName));
        (*tx.MutableCreateStreamingQuery()->MutableProperties()->MutableProperties())[TStreamingQueryMeta::TProperties::Run] = "false";
        const auto behaviour = TStreamingQueryConfig::GetBehaviour();
        auto result = f.Runtime.WaitFuture(behaviour->GetOperationsManager()->ExecutePrepared(
            operation, f.Runtime.GetNodeId(), behaviour, context), TDuration::Seconds(60));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetErrorMessage());
        UNIT_ASSERT_VALUES_EQUAL(begins, 1);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(TrackerRetriesSchemeTransactionErrorsFromDescribe) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        const TVector<NKikimrScheme::EStatus> errors = {
            NKikimrScheme::StatusNotAvailable,
            NKikimrScheme::StatusPreconditionFailed,
            NKikimrScheme::StatusAccessDenied,
            NKikimrScheme::StatusInvalidParameter,
        };
        ui64 requests = 0;
        bool described = false;
        auto descriptions = f.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySet>([&](auto& ev) {
            for (const auto& entry : ev->Get()->Request->ResultSet) {
                if (entry.Path == SplitPath(TString(TContinuationTest::QueryPath))) {
                    described = true;
                }
            }
        });
        auto failures = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() != TContinuationTest::QueryName || query.HasOperationOwnerActorId()) {
                return;
            }
            UNIT_ASSERT_C(described, "Every scheme retry must start with Describe");
            described = false;
            if (requests++ < errors.size()) {
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
                response->Record.SetStatus(NTxProxy::TResultStatus::ExecError);
                response->Record.SetSchemeShardStatus(errors[requests - 1]);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        tracking.Unblock().Stop();
        f.WaitFinished(1);
        UNIT_ASSERT_VALUES_EQUAL(requests, errors.size() + 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(TrackerDescribeFailurePreservesQueryDefinition, RepeatDescribe) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        auto expectedProperties = f.Describe()->ResultSet.at(0).StreamingQueryInfo->Description.GetProperties().GetProperties();
        expectedProperties.erase(TStreamingQueryMeta::TProperties::InflightOperation);
        bool failDescribe = !RepeatDescribe;
        ui64 describeFailures = 0;
        auto descriptions = f.Runtime.AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySetResult>([&](auto& ev) {
            for (auto& entry : ev->Get()->Request->ResultSet) {
                if (failDescribe && entry.Path == SplitPath(TString(TContinuationTest::QueryPath)) && entry.SyncVersion) {
                    entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied;
                    ++ev->Get()->Request->ErrorCount;
                    ++describeFailures;
                    if constexpr (!RepeatDescribe) {
                        failDescribe = false;
                    }
                }
            }
        });
        ui64 finalizations = 0;
        auto transactions = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() != TContinuationTest::QueryName || query.HasOperationOwnerActorId()) {
                return;
            }
            ++finalizations;
            if (finalizations > 1) {
                failDescribe = false;
            }
            if (RepeatDescribe && finalizations == 1) {
                // Fail subsequent describes after the tracker has loaded properties and unlocked the row.
                failDescribe = true;
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
                response->Record.SetStatus(NTxProxy::TResultStatus::ProxyShardNotAvailable);
                response->Record.SetSchemeShardStatus(NKikimrScheme::StatusNotAvailable);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        tracking.Unblock().Stop();
        f.WaitFinished(1);
        UNIT_ASSERT(describeFailures);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, RepeatDescribe ? 2 : 1);
        descriptions.Remove();
        f.CheckSettled();
        const auto description = f.Describe();
        const auto& actualProperties = description->ResultSet.at(0).StreamingQueryInfo->Description.GetProperties().GetProperties();
        UNIT_ASSERT_VALUES_EQUAL(actualProperties.size(), expectedProperties.size());
        for (const auto& [name, value] : expectedProperties) {
            UNIT_ASSERT(actualProperties.contains(name));
            UNIT_ASSERT_VALUES_EQUAL(actualProperties.at(name), value);
        }
    }

    Y_UNIT_TEST_QUAD(SchemeShardTracksAfterPublication, Alter, Reboot) {
        TContinuationTest f;
        if constexpr (Alter) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(1);
        }
        const auto finished = f.Finished;
        TActorId owner;
        auto requests = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && query.HasOperationOwnerActorId()) {
                owner = ActorIdFromProto(query.GetOperationOwnerActorId());
            }
        });
        TBlockEvents<NSchemeBoard::NInternalEvents::TEvUpdate> publications(f.Runtime, [&](const auto& ev) {
            return ev->Get()->GetPath() == TContinuationTest::QueryPath;
        });
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(Alter ? TString("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);") : TContinuationTest::CreateQuery());
        f.WaitFor("operation waiting for publication", [&] { return owner && !publications.empty(); });
        f.CrashOwner(owner);
        locking.Stop().clear();
        if constexpr (Reboot) {
            RebootTablet(f.Runtime, Tests::SchemeRoot, f.Runtime.AllocateEdgeActor());
        }
        f.Runtime.SimulateSleep(TDuration::Seconds(2));
        const bool trackedBeforePublication = !tracking.empty();
        {
            // CREATE is still absent; ALTER still exposes the previous published version.
            const auto description = f.Describe();
            const auto& entry = description->ResultSet.at(0);
            if constexpr (Alter) {
                UNIT_ASSERT(entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
                UNIT_ASSERT_VALUES_EQUAL(entry.Self->Info.GetVersion().GetStreamingQueryVersion(), 2);
            } else {
                UNIT_ASSERT(entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown);
            }
        }
        publications.Unblock().Stop();
        f.WaitFor("tracker announced after publication", [&] { return !tracking.empty(); });
        const auto description = f.Describe();
        const auto& entry = description->ResultSet.at(0);
        UNIT_ASSERT(entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(entry.Self->Info.GetVersion().GetStreamingQueryVersion(), tracking.front()->Get()->GetObjectGeneration());
        tracking.Unblock().Stop();
        f.WaitFinished(finished + 1);
        f.CheckSettled();
        UNIT_ASSERT_C(!trackedBeforePublication, "SchemeShard announced continuation before publishing the query metadata");
    }

    Y_UNIT_TEST_TWIN(PublishedOperationResumesWithoutRepublicationWait, Alter) {
        TContinuationTest f;
        if constexpr (Alter) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(1);
        }
        const auto finished = f.Finished;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetObjectId() == TContinuationTest::QueryName;
        });
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(Alter ? TString("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);") : TContinuationTest::CreateQuery());
        f.WaitFor("operation published and awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });
        const auto original = CopyTracking(*tracking.front()->Get());
        f.CrashOwner(original->GetOperationOwner());
        locking.Stop().clear();
        tracking.clear();

        // Existing replicas retain the acknowledged version across a SchemeShard-only restart.
        TBlockEvents<NSchemeBoard::NInternalEvents::TEvUpdate> publications(f.Runtime, [&](const auto& ev) {
            return ev->Get()->GetPath() == TContinuationTest::QueryPath;
        });
        RebootTablet(f.Runtime, original->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
        f.WaitFor("tracking resumed while republication is delayed", [&] { return !tracking.empty() && !publications.empty(); });
        UNIT_ASSERT(tracking.front()->Get()->GetRequestGeneration() > original->GetRequestGeneration());
        const auto description = f.Describe();
        const auto& entry = description->ResultSet.at(0);
        UNIT_ASSERT(entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(entry.Self->Info.GetVersion().GetStreamingQueryVersion(), original->GetObjectGeneration());

        publications.Unblock().Stop();
        tracking.Unblock().Stop();
        f.WaitFinished(finished + 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(TrackerReconcilesLostFinalizationReply, Drop) {
        TContinuationTest f;
        if constexpr (Drop) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(1);
        }
        const auto finished = f.Finished;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(Drop ? TString("DROP STREAMING QUERY ContinuedQuery;") : TContinuationTest::CreateQuery());
        f.WaitFor("operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        bool injected = false;
        TActorId transactionActor;
        auto proposes = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction().GetModifyScheme();
            if ((tx.GetCreateStreamingQuery().GetName() == TContinuationTest::QueryName && !tx.GetCreateStreamingQuery().HasOperationOwnerActorId())
                || tx.GetDrop().GetName() == TContinuationTest::QueryName) {
                transactionActor = ev->Sender;
            }
        });
        auto responses = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransactionStatus>([&](auto& ev) {
            if (!injected && ev->Recipient == transactionActor && ev->Get()->Status() == NTxProxy::TResultStatus::ExecInProgress) {
                injected = true;
                ev->Get()->Record.SetStatus(NTxProxy::TResultStatus::ExecError);
                ev->Get()->Record.SetSchemeShardStatus(NKikimrScheme::StatusInvalidParameter);
            }
        });
        tracking.Unblock().Stop();
        f.WaitFinished(finished + 1);
        UNIT_ASSERT(injected);
        if constexpr (Drop) {
            f.CheckDropped();
        } else {
            f.CheckSettled();
        }
    }

    Y_UNIT_TEST(TrackerKeepsRetryingSchemeErrors) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        ui64 requests = 0;
        auto failure = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++requests;
                auto response = MakeHolder<TEvTxUserProxy::TEvProposeTransactionStatus>();
                response->Record.SetStatus(NTxProxy::TResultStatus::ExecError);
                response->Record.SetSchemeShardStatus(NKikimrScheme::StatusPreconditionFailed);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        tracking.Unblock().Stop();
        f.WaitFor("repeated scheme errors", [&] { return requests >= 20; });
        const auto trackingRequests = f.Tracking.size();
        const auto previousRequests = requests;
        f.Runtime.AdvanceCurrentTime(TDuration::Minutes(1));
        f.WaitFor("tracker still retries the same operation", [&] { return requests >= previousRequests + 2; });
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Tracking.size(), trackingRequests);
        const auto description = f.Describe();
        UNIT_ASSERT(ActorIdFromProto(description->ResultSet.at(0).StreamingQueryInfo->Description.GetOperationOwnerActorId()));
        UNIT_ASSERT(!f.CheckRow().HasOperationActorId());

        failure.Remove();
        f.WaitFinished(1);
        UNIT_ASSERT_VALUES_EQUAL(f.Tracking.size(), trackingRequests);
        f.CheckSettled();
    }

    Y_UNIT_TEST(SuccessiveOperationsHaveIndependentTrackers) {
        TContinuationTest f;
        TVector<TEvTrackOperationFinished::TPtr> finishing;
        auto finishingObserver = f.Runtime.AddObserver<TEvTrackOperationFinished>([&](auto& ev) {
            if (ev->Get()->GetObjectId() == TContinuationTest::QueryName) {
                finishing.emplace_back(std::move(ev));
            }
        });
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFor("first tracker awaiting deregistration", [&] { return !finishing.empty(); });

        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.WaitFor("second operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(f.Tracking.front()->GetRequestGeneration(), tracking.front()->Get()->GetRequestGeneration());
        UNIT_ASSERT_VALUES_UNEQUAL(f.Tracking.front()->GetObjectGeneration(), tracking.front()->Get()->GetObjectGeneration());
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();
        tracking.Unblock().Stop();
        f.WaitFor("second operation completed while first tracker still registered", [&] { return finishing.size() >= 2; });
        f.CheckSettled();
        finishingObserver.Remove();
        for (auto& ev : finishing) {
            f.Runtime.Send(ev.Release());
        }
    }

    Y_UNIT_TEST_TWIN(SchemeShardContinuesOperationRecoveredBeforePlan, Alter) {
        TContinuationTest f;
        if constexpr (Alter) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(1);
        }
        const auto finished = f.Finished;
        TActorId owner;
        auto observer = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && query.HasOperationOwnerActorId()) {
                owner = ActorIdFromProto(query.GetOperationOwnerActorId());
            }
        });
        TBlockEvents<TEvTxProcessing::TEvPlanStep> planning(f.Runtime, [](const auto& ev) {
            return ev->Get()->Record.GetTabletID() == Tests::SchemeRoot;
        });
        f.Start(Alter ? TString("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);") : TContinuationTest::CreateQuery());
        f.WaitFor("operation durable before plan", [&] { return owner && !planning.empty(); });
        f.CrashOwner(owner);
        RebootTablet(f.Runtime, Tests::SchemeRoot, f.Runtime.AllocateEdgeActor());
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, finished);
        planning.Unblock().Stop();
        f.WaitFinished(finished + 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(StaleTrackerDoesNotCompleteNewOperation) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);
        auto stale = CopyTracking(*f.Tracking.front());

        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.WaitFor("new operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();
        tracking.Stop();

        f.Replay(*stale);
        f.WaitFinished(2);
        const auto description = f.Describe();
        UNIT_ASSERT(ActorIdFromProto(description->ResultSet.at(0).StreamingQueryInfo->Description.GetOperationOwnerActorId()));

        tracking.Unblock();
        f.WaitFinished(3);
        f.CheckSettled();
    }

    Y_UNIT_TEST(TrackerRetriesProvisionalLockWithSameOwner) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        TBlockEvents<TEvKqp::TEvQueryRequest> validating(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo");
        });
        tracking.Unblock().Stop();
        f.WaitFor("tracker acquired provisional row", [&] { return !validating.empty(); });
        const auto trackingRequests = f.Tracking.size();
        const auto state = f.CheckRow(true);
        UNIT_ASSERT(state.HasOperationActorId());
        auto request = std::move(validating.front());
        validating.pop_front();
        auto response = MakeHolder<TEvKqp::TEvQueryResponse>();
        response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient, response.Release(), 0, request->Cookie));
        f.WaitFor("same tracker retries provisional validation", [&] { return !validating.empty(); });
        const auto retriedState = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(retriedState.GetOperationActorId(), state.GetOperationActorId());
        UNIT_ASSERT_VALUES_EQUAL(retriedState.GetOperationOwnerGeneration(), state.GetOperationOwnerGeneration());
        UNIT_ASSERT_VALUES_EQUAL(f.Tracking.size(), trackingRequests);
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
        validating.Unblock().Stop();
        f.WaitFinished(1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(TrackingRequestsKeepOnlyNewestPendingGeneration) {
        TContinuationTest f;
        std::deque<TEvTrackOperationFinished::TPtr> finishing;
        IEventHandle* unblocked = nullptr;
        auto finishingObserver = f.Runtime.AddObserver<TEvTrackOperationFinished>([&](auto& ev) {
            // This event has no sender, so TBlockEvents cannot print its actor name.
            if (ev.Get() == unblocked) {
                unblocked = nullptr;
            } else if (ev->Get()->GetObjectId() == TContinuationTest::QueryName) {
                finishing.emplace_back(std::move(ev));
            }
        });
        const auto finishOne = [&] {
            unblocked = finishing.front().Get();
            f.Runtime.Send(finishing.front().Release());
            finishing.pop_front();
        };
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFor("initial tracker awaiting deregistration", [&] { return finishing.size() == 1; });
        auto request = CopyTracking(*f.Tracking.front());
        const auto generation = request->GetRequestGeneration();

        // Arrival order must not downgrade the pending request or start concurrent trackers.
        for (ui64 offset : {0, 1, 3, 2, 3, 0}) {
            request->SetRequestGeneration(generation + offset);
            f.Replay(*request);
        }
        f.Runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(finishing.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(finishing.front()->Get()->GetRequestGeneration(), generation);

        finishOne();
        f.WaitFor("newest pending tracker finished", [&] { return finishing.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(finishing.front()->Get()->GetRequestGeneration(), generation + 3);
        f.Runtime.SimulateSleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(finishing.size(), 1);
        finishOne();

        // Once the newest tracker finishes, the key must be available again.
        request->SetRequestGeneration(generation);
        f.Replay(*request);
        f.WaitFor("completed operation can be tracked again", [&] { return finishing.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(finishing.front()->Get()->GetRequestGeneration(), generation);
        finishOne();
        f.CheckSettled();
    }

    Y_UNIT_TEST(QueuedGenerationContinuesAfterRemoteTrackerStops) {
        TContinuationTest f(true, 2);
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);

        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start("ALTER STREAMING QUERY ContinuedQuery SET (RUN = FALSE);");
        f.WaitFor("alter operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        auto older = CopyTracking(*tracking.front()->Get());
        f.CrashOwner(older->GetOperationOwner());
        locking.Stop().clear();

        RebootTablet(f.Runtime, older->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
        f.WaitFor("second generation announced", [&] { return tracking.size() >= 2; });
        auto remote = CopyTracking(*tracking.back()->Get());
        RebootTablet(f.Runtime, older->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
        f.WaitFor("third generation announced", [&] { return tracking.size() >= 3; });
        auto newest = CopyTracking(*tracking.back()->Get());
        UNIT_ASSERT(older->GetRequestGeneration() < remote->GetRequestGeneration());
        UNIT_ASSERT(remote->GetRequestGeneration() < newest->GetRequestGeneration());
        tracking.Stop().clear();

        TBlockEvents<TEvKqp::TEvQueryRequest> localLocking(f.Runtime, [&](const auto& ev) {
            return IsLockRequest(ev) && ev->Recipient == MakeKqpProxyID(f.Runtime.GetNodeId(0));
        });
        f.Replay(*older, 0);
        f.WaitFor("old local tracker awaiting lock", [&] { return localLocking.size() == 1; });

        TBlockEvents<TEvKqp::TEvQueryRequest> unlocking(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetQuery().Contains("-- TUnlockStreamingQueryRequestActor::ReadQueryInfo");
        });
        f.Replay(*remote, 1);
        f.WaitFor("remote tracker acquired the lock", [&] { return !unlocking.empty(); });
        const auto remoteState = f.CheckRow();
        UNIT_ASSERT_VALUES_EQUAL(remoteState.GetOperationOwnerGeneration(), remote->GetRequestGeneration());
        TActorId remoteOwner;
        UNIT_ASSERT(ScriptExecutionRunnerActorIdFromString(remoteState.GetOperationActorId(), remoteOwner));
        UNIT_ASSERT_VALUES_EQUAL(remoteOwner.NodeId(), f.Runtime.GetNodeId(1));

        f.Replay(*newest, 0);
        f.Replay(*remote, 0);
        f.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(localLocking.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 1);
        f.CrashOwner(remoteOwner);
        unlocking.clear();

        ui64 finalizations = 0;
        auto schemeRequests = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++finalizations;
            }
        });
        localLocking.Unblock(1);
        f.WaitFor("old tracker retired and queued tracker started", [&] {
            return f.Finished == 2 && localLocking.size() == 1;
        });
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);
        UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().GetOperationActorId(), remoteState.GetOperationActorId());

        localLocking.Unblock().Stop();
        f.WaitFor("queued tracker acquired the abandoned remote lock", [&] { return !unlocking.empty(); });
        UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().GetOperationOwnerGeneration(), newest->GetRequestGeneration());
        unlocking.Unblock().Stop();
        f.WaitFinished(3);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(OlderSchemeShardGenerationFinishesWithoutFinalization, StopNewOwner) {
        TContinuationTest f(true, 2);
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation registered", [&] { return !tracking.empty() && !locking.empty(); });
        auto older = CopyTracking(*tracking.front()->Get());
        f.CrashOwner(older->GetOperationOwner());
        locking.Stop().clear();

        RebootTablet(f.Runtime, older->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
        f.WaitFor("operation resumed in a newer SchemeShard generation", [&] { return tracking.size() >= 2; });
        auto newer = CopyTracking(*tracking.back()->Get());
        UNIT_ASSERT(newer->GetRequestGeneration() > older->GetRequestGeneration());
        UNIT_ASSERT_VALUES_EQUAL(newer->GetObjectGeneration(), older->GetObjectGeneration());
        tracking.Stop().clear();

        TBlockEvents<TEvKqp::TEvQueryRequest> validating(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo");
        });
        f.Replay(*newer, 1);
        f.WaitFor("newer tracker acquired the provisional row", [&] { return !validating.empty(); });
        const auto state = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(state.GetOperationOwnerGeneration(), newer->GetRequestGeneration());
        TActorId newerOwner;
        UNIT_ASSERT(ScriptExecutionRunnerActorIdFromString(state.GetOperationActorId(), newerOwner));

        if constexpr (StopNewOwner) {
            f.CrashOwner(newerOwner);
            validating.Stop().clear();
        }

        ui64 finalizations = 0;
        auto schemeRequests = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++finalizations;
            }
        });
        f.Replay(*older);
        f.WaitFinished(1);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);
        const auto preserved = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(preserved.GetOperationOwnerGeneration(), newer->GetRequestGeneration());
        UNIT_ASSERT_VALUES_EQUAL(preserved.GetOperationActorId(), state.GetOperationActorId());
        const auto description = f.Describe();
        UNIT_ASSERT(ActorIdFromProto(description->ResultSet.at(0).StreamingQueryInfo->Description.GetOperationOwnerActorId()));

        // The old tracker retires even if the newer lock owner is already dead.
        if constexpr (StopNewOwner) {
            TBlockEvents<TEvTrackOperationCompletion> recovery(f.Runtime);
            RebootTablet(f.Runtime, newer->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
            f.WaitFor("SchemeShard requests another generation", [&] { return !recovery.empty(); });
            auto request = CopyTracking(*recovery.back()->Get());
            recovery.Stop().clear();
            f.Replay(*request);
        } else {
            validating.Unblock().Stop();
        }
        f.WaitFinished(2);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(TrackerRechecksCompletionAfterLosingLockDuringSync, Completed) {
        TContinuationTest f(true, 2);
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        ui64 updates = 0;
        TBlockEvents<TEvKqp::TEvQueryRequest> syncing(f.Runtime, [&](const auto& ev) {
            // Let provisional-row validation finish, then pause sync before its first state update.
            return ev->Recipient == MakeKqpProxyID(ev->Recipient.NodeId())
                && ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo")
                && ++updates > 1;
        });
        auto older = CopyTracking(*tracking.front()->Get());
        tracking.Stop().clear();
        f.Replay(*older);
        f.WaitFor("older tracker syncing the query", [&] { return !syncing.empty(); });
        auto oldSync = std::move(syncing.front());
        syncing.pop_front();
        const auto oldState = f.CheckRow();
        TActorId oldOwner;
        UNIT_ASSERT(ScriptExecutionRunnerActorIdFromString(oldState.GetOperationActorId(), oldOwner));

        // Simulate an unresponsive tracker long enough for the next generation to take its lock.
        auto unreachable = f.Runtime.AddObserver([&](auto& ev) {
            if (ev->Recipient == oldOwner) {
                ev.Reset();
            }
        });
        ui64 unlocks = 0;
        auto unlockRequests = f.Runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Recipient == MakeKqpProxyID(ev->Recipient.NodeId())
                && ev->Get()->GetQuery().Contains("-- TUnlockStreamingQueryRequestActor::ReadQueryInfo")) {
                ++unlocks;
            }
        });
        ui64 finalizations = 0;
        auto schemeRequests = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++finalizations;
            }
        });
        TBlockEvents<TEvTrackOperationCompletion> recovery(f.Runtime);
        RebootTablet(f.Runtime, Tests::SchemeRoot, f.Runtime.AllocateEdgeActor());
        f.WaitFor("newer tracker announced", [&] { return !recovery.empty(); });
        auto newer = CopyTracking(*recovery.back()->Get());
        recovery.Stop().clear();
        f.Replay(*newer, 1);
        f.Runtime.WaitFor("newer tracker took over and started sync", [&] { return !syncing.empty(); }, TDuration::Minutes(3));
        unreachable.Remove();
        const auto newState = f.CheckRow();
        UNIT_ASSERT(newState.GetOperationOwnerGeneration() > oldState.GetOperationOwnerGeneration());
        UNIT_ASSERT_VALUES_UNEQUAL(newState.GetOperationActorId(), oldState.GetOperationActorId());
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);

        if constexpr (Completed) {
            syncing.Unblock().Stop();
            f.WaitFinished(1);
            f.CheckSettled();
        }

        syncing.push_front(std::move(oldSync));
        syncing.Unblock(1);
        if constexpr (!Completed) {
            f.WaitFinished(1);
            UNIT_ASSERT_VALUES_EQUAL(unlocks, 1);
            UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);
            UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().GetOperationActorId(), newState.GetOperationActorId());
            syncing.Unblock().Stop();
        }

        f.WaitFinished(2);
        UNIT_ASSERT_VALUES_EQUAL(unlocks, 2);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(NewerTrackerChecksOwnerChangedDuringTakeover) {
        TContinuationTest f(true, 2);
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> initialValidation(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo");
        });
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("original owner holds the provisional row", [&] { return !tracking.empty() && !initialValidation.empty(); });
        auto older = CopyTracking(*tracking.front()->Get());
        UNIT_ASSERT_VALUES_EQUAL(f.CheckRow(true).GetOperationOwnerGeneration(), 0);
        f.CrashOwner(older->GetOperationOwner());
        initialValidation.Stop().clear();

        RebootTablet(f.Runtime, older->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
        f.WaitFor("second tracker announced after reboot", [&] { return tracking.size() >= 2; });
        auto newer = CopyTracking(*tracking.back()->Get());
        UNIT_ASSERT(newer->GetRequestGeneration() > older->GetRequestGeneration());
        tracking.Stop().clear();

        ui64 lockReads = 0;
        TBlockEvents<TEvKqp::TEvQueryRequest> takeover(f.Runtime, [&](const auto& ev) {
            // Pause G2 after checking the original owner, before re-reading and claiming its lock.
            return IsLockRequest(ev) && ++lockReads == 2;
        });
        f.Replay(*newer, 1);
        f.WaitFor("newer tracker checked the dead original owner", [&] { return !takeover.empty(); });

        TBlockEvents<TEvKqp::TEvQueryRequest> validating(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo");
        });
        f.Replay(*older);
        f.WaitFor("older tracker acquired the lock", [&] { return !validating.empty(); });
        const auto state = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(state.GetOperationOwnerGeneration(), older->GetRequestGeneration());
        TActorId currentOwner;
        UNIT_ASSERT(ScriptExecutionRunnerActorIdFromString(state.GetOperationActorId(), currentOwner));

        bool ownerChecked = false;
        auto ownerObserver = f.Runtime.AddObserver([&](auto& ev) {
            // Its lock result is blocked above, so the next message is G2's liveness probe.
            if (ev->Recipient == currentOwner) {
                ownerChecked = true;
            }
        });
        takeover.Unblock().Stop();
        f.WaitFor("newer tracker noticed the changed owner", [&] { return ownerChecked || validating.size() > 1; });
        UNIT_ASSERT_C(ownerChecked, "Generation increased, but it is still below G2: the new owner must be checked");
        UNIT_ASSERT_VALUES_EQUAL(validating.size(), 1);
        const auto preserved = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(preserved.GetOperationActorId(), state.GetOperationActorId());
        UNIT_ASSERT_VALUES_EQUAL(preserved.GetOperationOwnerGeneration(), older->GetRequestGeneration());

        validating.Unblock().Stop();
        f.WaitFinished(2);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(ContinuesAfterRowUnlockBeforeSchemeFinalization, Drop) {
        TContinuationTest f;
        if constexpr (Drop) {
            f.Exec(TContinuationTest::CreateQuery());
            f.WaitFinished(1);
        }
        const auto finished = f.Finished;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvTxUserProxy::TEvProposeTransaction> finalizing(f.Runtime, [](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction().GetModifyScheme();
            return (tx.HasCreateStreamingQuery() && tx.GetCreateStreamingQuery().GetName() == TContinuationTest::QueryName
                    && !tx.GetCreateStreamingQuery().HasOperationOwnerActorId())
                || (tx.HasDrop() && tx.GetDrop().GetName() == TContinuationTest::QueryName);
        });
        f.Start(Drop ? TString("DROP STREAMING QUERY ContinuedQuery;") : TContinuationTest::CreateQuery());
        f.WaitFor("row unlocked, scheme operation still pending", [&] { return !tracking.empty() && !finalizing.empty(); });
        if constexpr (!Drop) {
            UNIT_ASSERT(!f.CheckRow().HasOperationActorId());
        }
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        finalizing.Stop().clear();
        tracking.Unblock().Stop();
        f.WaitFinished(finished + 1);
        if constexpr (Drop) {
            f.CheckDropped();
        } else {
            f.CheckSettled();
        }
    }

    Y_UNIT_TEST_TWIN(FinalizationDoesNotRecreateDroppedQuery, Tracker) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        TBlockEvents<TEvTxUserProxy::TEvProposeTransaction> finalizing(f.Runtime, [](const auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            return query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId();
        });
        auto result = f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });
        if constexpr (Tracker) {
            f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
            locking.Stop().clear();
            tracking.Unblock().Stop();
        } else {
            locking.Unblock().Stop();
        }
        f.WaitFor("finalization ready", [&] { return !finalizing.empty(); });
        const auto& tx = finalizing.front()->Get()->Record.GetTransaction().GetModifyScheme();
        UNIT_ASSERT(tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
        UNIT_ASSERT(tx.GetReplaceIfExists());

        // Simulate a concurrent drop committing before the delayed finalization.
        f.ExecMetadata("DELETE FROM `.metadata/streaming/queries` WHERE query_path = '/Root/ContinuedQuery';");
        f.DropInSchemeShard();
        finalizing.Unblock().Stop();
        if constexpr (!Tracker) {
            const auto response = f.Runtime.WaitFuture(result, TDuration::Seconds(60));
            UNIT_ASSERT_C(!response.IsSuccess(), response.GetIssues().ToString());
            tracking.Unblock().Stop();
        }
        f.WaitFinished(1);
        f.CheckDropped();
    }

    Y_UNIT_TEST(FailedDropOutcomeSurvivesOwnerLoss) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvTxUserProxy::TEvProposeTransaction> finalizing(f.Runtime, [](const auto& ev) {
            const auto& tx = ev->Get()->Record.GetTransaction().GetModifyScheme();
            return tx.HasCreateStreamingQuery() && tx.GetCreateStreamingQuery().GetName() == TContinuationTest::QueryName
                && !tx.GetCreateStreamingQuery().HasOperationOwnerActorId();
        });
        bool failed = false;
        auto failure = f.Runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (!failed && ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo")) {
                failed = true;
                auto response = MakeHolder<TEvKqp::TEvQueryResponse>();
                response->Record.SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient, response.Release(), 0, ev->Cookie));
                ev.Reset();
            }
        });
        f.Start("DROP STREAMING QUERY ContinuedQuery;");
        f.WaitFor("failed drop unlocked its row", [&] { return failed && !tracking.empty() && !finalizing.empty(); });
        UNIT_ASSERT(!f.CheckRow().HasOperationActorId());
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        finalizing.Stop().clear();
        tracking.Unblock().Stop();
        f.WaitFinished(2);
        f.CheckSettled();
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST(OldTrackerDoesNotChangeRecreatedQuery) {
        TContinuationTest f;
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(1);
        auto stale = CopyTracking(*f.Tracking.front());
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.WaitFinished(2);
        f.Exec(TContinuationTest::CreateQuery());
        f.WaitFinished(3);
        UNIT_ASSERT_VALUES_UNEQUAL(stale->GetPathId(), f.Tracking.back()->GetPathId());
        f.Replay(*stale);
        f.WaitFinished(4);
        f.CheckSettled();
    }

    Y_UNIT_TEST(ProvisionalRowRetainsTtlAfterSchemaDisappears) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> validating(f.Runtime, [](const auto& ev) {
            return ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo");
        });
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("provisional row awaiting validation commit", [&] { return !tracking.empty() && !validating.empty(); });
        auto request = CopyTracking(*tracking.front()->Get());
        f.CrashOwner(request->GetOperationOwner());
        validating.Stop().clear();
        tracking.Stop().clear();
        f.CheckRow(true);
        f.CheckQueriesTableTtl();
        f.DropInSchemeShard();
        f.Replay(*request);
        f.WaitFinished(1);
        f.CheckRow(true);
    }

    Y_UNIT_TEST(ExistingQueriesTableGetsTtlColumn) {
        TContinuationTest f(false);
        TVector<NKikimrSchemeOp::TColumnDescription> columns;
        for (const auto& [name, type] : {std::pair{"database_id", "Utf8"}, {"query_path", "Utf8"}, {"state", "Json"}}) {
            auto& column = columns.emplace_back();
            column.SetName(name);
            column.SetType(type);
        }
        const auto edge = f.Runtime.AllocateEdgeActor();
        f.Runtime.Register(CreateTableCreator({".metadata", "streaming", "queries"},
            std::move(columns), {"database_id", "query_path"}, NKikimrServices::KQP_PROXY),
            0, 0, TMailboxType::Simple, 0, edge);
        const auto created = f.Runtime.GrabEdgeEvent<TEvTableCreator::TEvCreateTableResponse>(edge);
        UNIT_ASSERT_C(created && created->Get()->Success, "Could not create legacy queries table");
        UNIT_ASSERT_VALUES_EQUAL(f.ExecMetadata("SELECT * FROM `.metadata/streaming/queries`;").GetResultSet(0).ColumnsCount(), 3);

        f.Exec(TContinuationTest::CreateQuery());
        f.CheckSettled();
        f.CheckQueriesTableTtl();
        auto migrations = f.ExecMetadata("SELECT * FROM `.metadata/initialization/migrations` "
            "WHERE componentId = 'STREAMING_QUERY' AND modificationId = 'create-generic';");
        UNIT_ASSERT_VALUES_EQUAL(migrations.GetResultSet(0).RowsCount(), 1);
    }

    Y_UNIT_TEST(TrackerFinalizesSyncFailureWithOwnedLock) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        TBlockEvents<TEvKqp::TEvScriptRequest> starting(f.Runtime);
        f.Start("CREATE STREAMING QUERY ContinuedQuery AS DO BEGIN INSERT INTO Source.output SELECT value "
            "FROM Source.input WITH (FORMAT = 'raw', SCHEMA (value String NOT NULL)); END DO;");
        f.WaitFor("operation awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();
        tracking.Unblock().Stop();
        f.WaitFor("tracker starting execution", [&] { return !starting.empty(); });
        const auto trackingRequests = f.Tracking.size();
        const auto state = f.CheckRow();
        UNIT_ASSERT(state.HasOperationActorId());
        ui64 unlocks = 0;
        auto unlockRequests = f.Runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Recipient == MakeKqpProxyID(f.Runtime.GetNodeId())
                && ev->Get()->GetQuery().Contains("-- TUnlockStreamingQueryRequestActor::ReadQueryInfo")) {
                ++unlocks;
            }
        });
        ui64 finalizations = 0;
        auto schemeRequests = f.Runtime.AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            const auto& query = ev->Get()->Record.GetTransaction().GetModifyScheme().GetCreateStreamingQuery();
            if (query.GetName() == TContinuationTest::QueryName && !query.HasOperationOwnerActorId()) {
                ++finalizations;
            }
        });
        auto request = std::move(starting.front());
        starting.pop_front();
        f.Runtime.Send(new IEventHandle(request->Sender, request->Recipient,
            new TEvKqp::TEvScriptResponse(Ydb::StatusIds::UNAVAILABLE, {NYql::TIssue("Injected transient script creation failure")}),
            0, request->Cookie));
        f.WaitFinished(1);
        UNIT_ASSERT(starting.empty());
        starting.Stop();
        UNIT_ASSERT_VALUES_EQUAL(f.Tracking.size(), trackingRequests);
        UNIT_ASSERT_VALUES_EQUAL(unlocks, 1);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        f.CheckSettled();
        const auto failedState = f.CheckRow();
        UNIT_ASSERT(failedState.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_STOPPED);
        UNIT_ASSERT(!failedState.HasCurrentExecutionId());
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }

    Y_UNIT_TEST_QUAD(ContinuesPartiallyStartedStreamingExecution, ScriptError, RemoteOwner) {
        TContinuationTest f(true, RemoteOwner ? 2 : 1);
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvScriptRequest> starting(f.Runtime);
        const TString query = "CREATE STREAMING QUERY ContinuedQuery AS DO BEGIN INSERT INTO Source.output SELECT value "
            "FROM Source.input WITH (FORMAT = 'raw', SCHEMA (value String NOT NULL)); END DO;";
        if constexpr (RemoteOwner) {
            f.StartOnNode(query, f.RemoteOwnerNode());
        } else {
            f.Start(query);
        }
        f.WaitFor("execution allocated before script creation", [&] { return !tracking.empty() && !starting.empty(); });
        const auto abandonedExecution = f.CheckRow().GetCurrentExecutionId();
        UNIT_ASSERT(abandonedExecution);
        if constexpr (RemoteOwner) {
            UNIT_ASSERT_VALUES_UNEQUAL(tracking.front()->Sender.NodeId(), tracking.front()->Get()->GetOperationOwner().NodeId());
        }
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        starting.Stop().clear();
        TTestActorRuntime::TEventObserverHolder failure;
        if constexpr (ScriptError) {
            failure = f.Runtime.AddObserver<TEvKqp::TEvScriptRequest>([&](auto& ev) {
                f.Runtime.Send(new IEventHandle(ev->Sender, ev->Recipient,
                    new TEvKqp::TEvScriptResponse(Ydb::StatusIds::BAD_REQUEST, {NYql::TIssue("Injected script creation failure")}),
                    0, ev->Cookie));
                ev.Reset();
            });
        }
        tracking.Unblock().Stop();
        f.WaitFinished(1);
        f.CheckSettled();
        const auto state = f.CheckRow();
        if constexpr (ScriptError) {
            UNIT_ASSERT_C(state.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_STOPPED, state.DebugString());
            UNIT_ASSERT(!state.GetCurrentExecutionId());
        } else {
            UNIT_ASSERT_C(state.GetStatus() == NKikimrKqp::TStreamingQueryState::STATUS_RUNNING, state.DebugString());
            UNIT_ASSERT(state.GetCurrentExecutionId());
            UNIT_ASSERT_VALUES_UNEQUAL(state.GetCurrentExecutionId(), abandonedExecution);
        }
        f.Exec("DROP STREAMING QUERY ContinuedQuery;");
        f.CheckDropped();
    }
}

} // namespace NKikimr::NKqp
