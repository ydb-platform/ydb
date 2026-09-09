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

    static std::shared_ptr<TKikimrRunner> CreateRunner(const TIntrusivePtr<NTestUtils::IMockPqGateway>& gateway) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableStreamingQueries(true);
        config.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(true);
        return NFederatedQueryTest::MakeKikimrRunner(false, nullptr, nullptr, config, NYql::NDq::CreateS3ActorsFactory(), {
            .PqGateway = gateway,
            .UseLocalCheckpointsInStreamingQueries = true,
            .UseRealThreads = false,
        });
    }

    TIntrusivePtr<NTestUtils::IMockPqGateway> Gateway = NTestUtils::CreateMockPqGateway();
    std::shared_ptr<TKikimrRunner> Runner = CreateRunner(Gateway);
    TTestActorRuntime& Runtime = *Runner->GetTestServer().GetRuntime();
    TQueryClient Client = Runner->GetQueryClient(TClientSettings().AuthToken(BUILTIN_ACL_ROOT));
    TQueryClient MetadataClient = Runner->GetQueryClient(TClientSettings().AuthToken(BUILTIN_ACL_METADATA));
    TVector<THolder<TEvTrackOperationCompletion>> Tracking;
    ui64 Finished = 0;
    TTestActorRuntime::TEventObserverHolder TrackingObserver;
    TTestActorRuntime::TEventObserverHolder FinishedObserver;

    explicit TContinuationTest(bool initializeMetadata = true) {
        Runtime.GetAppData().FeatureFlags.SetEnableStreamingQueries(true);
        Runtime.SetRegistrationObserverFunc([](auto& runtime, const TActorId&, const TActorId& actor) {
            runtime.EnableScheduleForActor(actor);
        });
        Runtime.EnableScheduleForActor(Runtime.GetActorSystem(0)->LookupLocalService(
            NMetadata::NProvider::MakeServiceId(Runtime.GetNodeId())));
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

    void Replay(const TEvTrackOperationCompletion& request) {
        Runtime.Send(new IEventHandle(NMetadata::NProvider::MakeServiceId(Runtime.GetNodeId()),
            Runtime.AllocateEdgeActor(), CopyTracking(request).Release()));
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
        Runtime.Send(new IEventHandle(owner, Runtime.AllocateEdgeActor(),
            new TEvents::TEvResumeRunnable(new TOwnerCrash()), TEvents::TEvResumeRunnable::EventFlags));
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

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpStreamingOperationContinuation) {
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

    Y_UNIT_TEST_TWIN(OlderSchemeShardGenerationMonitorsNewOwner, StopNewOwner) {
        TContinuationTest f;
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
        f.Replay(*newer);
        f.WaitFor("newer tracker acquired the provisional row", [&] { return !validating.empty(); });
        const auto state = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(state.GetOperationOwnerGeneration(), newer->GetRequestGeneration());
        TActorId newerOwner;
        UNIT_ASSERT(ScriptExecutionRunnerActorIdFromString(state.GetOperationActorId(), newerOwner));

        if constexpr (StopNewOwner) {
            f.CrashOwner(newerOwner);
            validating.Stop().clear();
        }

        ui64 ownerChecks = 0;
        auto ownerObserver = f.Runtime.AddObserver([&](auto& ev) {
            // The newer owner's lock result is held, so only liveness probes reach it.
            if (ev->Recipient == newerOwner) {
                ++ownerChecks;
            }
        });
        f.Replay(*older);
        f.WaitFor("older tracker keeps monitoring the newer owner", [&] {
            return ownerChecks >= 2 || f.Finished;
        });
        UNIT_ASSERT_C(ownerChecks >= 2, "An older generation must monitor the owner without restarting the tracker");
        UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
        const auto preserved = f.CheckRow(true);
        UNIT_ASSERT_VALUES_EQUAL(preserved.GetOperationOwnerGeneration(), newer->GetRequestGeneration());
        UNIT_ASSERT_VALUES_EQUAL(preserved.GetOperationActorId(), state.GetOperationActorId());
        const auto description = f.Describe();
        UNIT_ASSERT(ActorIdFromProto(description->ResultSet.at(0).StreamingQueryInfo->Description.GetOperationOwnerActorId()));

        // Only the newer generation may continue; the older tracker observes completion.
        if constexpr (StopNewOwner) {
            RebootTablet(f.Runtime, newer->GetPathId().OwnerId, f.Runtime.AllocateEdgeActor());
        } else {
            validating.Unblock().Stop();
        }
        f.WaitFinished(2);
        f.CheckSettled();
    }

    Y_UNIT_TEST_TWIN(TrackerRechecksCompletionAfterLosingLockDuringSync, Completed) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvQueryRequest> locking(f.Runtime, IsLockRequest);
        f.Start(TContinuationTest::CreateQuery());
        f.WaitFor("operation awaiting row lock", [&] { return !tracking.empty() && !locking.empty(); });
        f.CrashOwner(tracking.front()->Get()->GetOperationOwner());
        locking.Stop().clear();

        ui64 updates = 0;
        TBlockEvents<TEvKqp::TEvQueryRequest> syncing(f.Runtime, [&](const auto& ev) {
            // Let provisional-row validation finish, then pause sync before its first state update.
            return ev->Recipient == MakeKqpProxyID(f.Runtime.GetNodeId())
                && ev->Get()->GetQuery().Contains("-- TUpdateStreamingQueryStateRequestActor::ReadQueryInfo")
                && ++updates > 1;
        });
        tracking.Unblock().Stop();
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
        RebootTablet(f.Runtime, Tests::SchemeRoot, f.Runtime.AllocateEdgeActor());
        f.Runtime.WaitFor("newer tracker took over and started sync", [&] { return !syncing.empty(); }, TDuration::Minutes(3));
        unreachable.Remove();
        const auto newState = f.CheckRow();
        UNIT_ASSERT(newState.GetOperationOwnerGeneration() > oldState.GetOperationOwnerGeneration());
        UNIT_ASSERT_VALUES_UNEQUAL(newState.GetOperationActorId(), oldState.GetOperationActorId());
        TActorId newOwner;
        UNIT_ASSERT(ScriptExecutionRunnerActorIdFromString(newState.GetOperationActorId(), newOwner));
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);

        if constexpr (Completed) {
            syncing.Unblock().Stop();
            f.WaitFinished(1);
            f.CheckSettled();
        }

        ui64 ownerChecks = 0;
        auto ownerObserver = f.Runtime.AddObserver([&](auto& ev) {
            // While the new owner's state update is blocked, only liveness probes reach it.
            if (ev->Recipient == newOwner) {
                ++ownerChecks;
            }
        });
        syncing.push_front(std::move(oldSync));
        syncing.Unblock(1);
        if constexpr (!Completed) {
            f.WaitFor("older tracker monitors the new lock owner after failed unlock", [&] { return ownerChecks > 0 || f.Finished; });
            UNIT_ASSERT(ownerChecks > 0);
            UNIT_ASSERT_VALUES_EQUAL(unlocks, 1);
            UNIT_ASSERT_VALUES_EQUAL(finalizations, 0);
            UNIT_ASSERT_VALUES_EQUAL(f.Finished, 0);
            UNIT_ASSERT_VALUES_EQUAL(f.CheckRow().GetOperationActorId(), newState.GetOperationActorId());
            syncing.Unblock().Stop();
        }

        f.WaitFinished(2);
        UNIT_ASSERT_VALUES_EQUAL(unlocks, 2);
        UNIT_ASSERT_VALUES_EQUAL(finalizations, 1);
        f.CheckSettled();
    }

    Y_UNIT_TEST(NewerTrackerChecksOwnerChangedDuringTakeover) {
        TContinuationTest f;
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
        f.Replay(*newer);
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
        f.Runtime.Register(CreateTableCreator({"Root", ".metadata", "streaming", "queries"},
            std::move(columns), {"database_id", "query_path"}, NKikimrServices::KQP_PROXY),
            0, 0, TMailboxType::Simple, 0, edge);
        const auto created = f.Runtime.GrabEdgeEvent<TEvTableCreator::TEvCreateTableResponse>(edge);
        UNIT_ASSERT_C(created && created->Get()->Success, "Could not create legacy queries table");

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

    Y_UNIT_TEST_TWIN(ContinuesPartiallyStartedStreamingExecution, ScriptError) {
        TContinuationTest f;
        TBlockEvents<TEvTrackOperationCompletion> tracking(f.Runtime);
        TBlockEvents<TEvKqp::TEvScriptRequest> starting(f.Runtime);
        f.Start("CREATE STREAMING QUERY ContinuedQuery AS DO BEGIN INSERT INTO Source.output SELECT value "
            "FROM Source.input WITH (FORMAT = 'raw', SCHEMA (value String NOT NULL)); END DO;");
        f.WaitFor("execution allocated before script creation", [&] { return !tracking.empty() && !starting.empty(); });
        const auto abandonedExecution = f.CheckRow().GetCurrentExecutionId();
        UNIT_ASSERT(abandonedExecution);
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
