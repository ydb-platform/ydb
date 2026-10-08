#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/fq/libs/ydb/ydb.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NFederatedQueryTest;

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreamsQueriesRestart) {

    Y_UNIT_TEST(LocalConnectionOperationTimeout) {
        TKikimrSettings settings;
        settings.SetUseRealThreads(false);
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetDispatchTimeout(TDuration::Seconds(10));

        const auto connection = NFq::CreateLocalYdbConnection(
            runtime.GetAppData().TenantName, ".metadata/streaming/checkpoints", 1);
        const auto timeout = TDuration::MilliSeconds(100);
        const auto proxyId = runtime.GetLocalServiceId(MakeKqpProxyID(runtime.GetNodeId(0)));
        TVector<TAutoPtr<NActors::IEventHandle>> heldRequests;
        auto observer = runtime.AddObserver<TEvKqp::TEvQueryRequest>([&](auto& ev) {
            if (ev->Sender == proxyId && ev->Get()->GetAction() == NKikimrKqp::QUERY_ACTION_EXECUTE) {
                UNIT_ASSERT_VALUES_EQUAL(ev->Get()->GetOperationTimeout(), timeout);
                // The proxy has installed its timeout timer before forwarding this request.
                heldRequests.push_back(ev.Release());
            }
        });
        // The session has not received the held query. Let the proxy's second timeout
        // round reply instead of delivering an abort to an idle session.
        auto abortObserver = runtime.AddObserver<TEvKqp::TEvAbortExecution>([](auto& ev) {
            ev.Reset();
        });

        struct TRunOperationActor : NActors::TActorBootstrapped<TRunOperationActor> {
            TRunOperationActor(NFq::IYdbTableClient::TPtr client, NFq::TOperationFunc operation,
                NThreading::TPromise<TStatus> promise)
                : Client(std::move(client))
                , Operation(std::move(operation))
                , Promise(promise)
            {}

            void Bootstrap() {
                Client->RetryOperation(std::move(Operation),
                    NYdb::NRetry::TRetryOperationSettings().MaxRetries(1))
                    .Subscribe([promise = Promise](const NYdb::TAsyncStatus& result) mutable {
                        promise.SetValue(result.GetValue());
                    });
                PassAway();
            }

            NFq::IYdbTableClient::TPtr Client;
            NFq::TOperationFunc Operation;
            NThreading::TPromise<TStatus> Promise;
        };

        TVector<EStatus> attemptStatuses;
        const auto runQuery = [&](TDuration operationTimeout) {
            auto promise = NThreading::NewPromise<TStatus>();
            runtime.Register(new TRunOperationActor(connection->GetTableClient(),
                [&, operationTimeout](NFq::ISession::TPtr session) {
                    return session->ExecuteDataQuery("SELECT 1;", NFq::ISession::TTxControl::BeginAndCommitTx(),
                        nullptr, NYdb::NTable::TExecDataQuerySettings().OperationTimeout(operationTimeout))
                        .Apply([&](const NThreading::TFuture<NYdb::NTable::TDataQueryResult>& result) {
                            const auto& status = result.GetValue();
                            attemptStatuses.push_back(status.GetStatus());
                            return TStatus(status);
                        });
                }, promise));
            return runtime.WaitFuture(promise.GetFuture());
        };

        const auto timedOut = runQuery(timeout);
        UNIT_ASSERT_VALUES_EQUAL(timedOut.GetStatus(), EStatus::INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(timedOut.GetIssues().ToString(), "MaxRetries is reached");
        UNIT_ASSERT_VALUES_EQUAL(heldRequests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(attemptStatuses.size(), 2);
        for (const auto status : attemptStatuses) {
            UNIT_ASSERT_VALUES_EQUAL(status, EStatus::TIMEOUT);
        }

        observer.Remove();
        abortObserver.Remove();
        heldRequests.clear();
        attemptStatuses.clear();
        const auto success = runQuery(TDuration::Seconds(30));
        UNIT_ASSERT_C(success.IsSuccess(), success.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(attemptStatuses.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(attemptStatuses.front(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST_F(RestartQueryAfterPartitionIncrease, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableUpdatingPartitionsOnStreamingQueryRestart(true);

        const auto runTest = [&](bool local) {
            const std::string suffix = local ? "_local" : "_nonlocal";
            const std::string inputTopicName  = std::string("restartAfterPartIncInputTopic")  + suffix;
            const TString outputTopicName = std::string("restartAfterPartIncOutputTopic") + suffix;
            const std::string sourceName      = std::string("restartAfterPartIncSource")      + suffix;
            const std::string queryName       = std::string("restartAfterPartIncQuery")       + suffix;

            CreateScopedTopicExt(inputTopicName, NYdb::NTopic::TCreateTopicSettings()
                .PartitioningSettings(/* minActivePartitions */ 1, /* maxActivePartitions */ 1), local);
            CreateScopedTopicExt(outputTopicName, std::nullopt, local);

            std::string inputRef, outputRef;
            if (local) {
                inputRef  = fmt::format("`{}`", inputTopicName);
                outputRef = fmt::format("`{}`", outputTopicName);
            } else {
                CreatePqSource(sourceName);
                inputRef  = fmt::format("`{}`.`{}`", sourceName, inputTopicName);
                outputRef = fmt::format("`{}`.`{}`", sourceName, outputTopicName);
            }

            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    $in = SELECT value FROM {input_ref} WITH (
                        FORMAT = "json_each_row",
                        SCHEMA = (value String NOT NULL)
                    )
                    WHERE value LIKE "%data%";
                    INSERT INTO {output_ref} SELECT value FROM $in;
                END DO;)",
                "query_name"_a = queryName,
                "input_ref"_a  = inputRef,
                "output_ref"_a = outputRef
            ));

            WriteTopicMessage(inputTopicName, R"({"value": "my_data_0"})", 0, local);
            ReadTopicMessages(outputTopicName, {"my_data_0"},
                TInstant::Now() - TDuration::Seconds(100),
                /* sort */ true, local);

            const auto storageCounters = GetCounters()->FindSubgroup("subsystem", "checkpoints_storage_service");
            UNIT_ASSERT_C(storageCounters, "Checkpoint storage counters are missing");
            const auto tableClientCounters = storageCounters->FindSubgroup("component", "local_table_client");
            UNIT_ASSERT_C(tableClientCounters, "Local table client counters are missing");
            UNIT_ASSERT(tableClientCounters->FindCounter("ActiveSessions"));
            UNIT_ASSERT(tableClientCounters->FindCounter("SessionLimitExceeded"));
            UNIT_ASSERT(tableClientCounters->FindHistogram("SessionHoldDurationMs"));

            Sleep(TDuration::Seconds(2));

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
                "query_name"_a = queryName
            ));
            Sleep(TDuration::MilliSeconds(500));

            {
                auto alterSettings = NYdb::NTopic::TAlterTopicSettings();
                alterSettings
                    .BeginAlterPartitioningSettings()
                        .MinActivePartitions(20)
                        .MaxActivePartitions(20)
                    .EndAlterTopicPartitioningSettings();
                const auto result = GetTopicClient(local)->AlterTopic(inputTopicName, alterSettings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            }

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
                "query_name"_a = queryName
            ));

            Sleep(TDuration::Seconds(2));

            constexpr ui32 messageCount = 20;
            for (ui32 i = 1; i < messageCount; ++i) {
                WriteTopicMessage(inputTopicName, fmt::format(R"({{"value": "my_data_{}"}})", i), i, local);
            }
            WriteTopicMessage(inputTopicName, R"({"value": "my_data_0"})", 0, local);

            std::vector<std::string> expectedMessages = {"my_data_0"}; // initial message written before restart
            for (ui32 i = 0; i < messageCount; ++i) {
                expectedMessages.push_back(fmt::format("my_data_{}", i));
            }
            ReadTopicMessages(outputTopicName, expectedMessages,
                TInstant::Now() - TDuration::Seconds(100),
                /* sort */ true, local);

            ExecQuery(fmt::format(R"(
                DROP STREAMING QUERY `{query_name}`;)",
                "query_name"_a = queryName
            ));
        };

        runTest(/* local */ false);
        runTest(/* local */ true);
    }

    Y_UNIT_TEST_F(PartitionPredicatePreservedAfterPartitionIncrease, TStreamingTestFixture) {
        constexpr char inputTopicName[]  = "partPredicateAfterIncInputTopic";
        constexpr char outputTopicName[] = "partPredicateAfterIncOutputTopic";
        constexpr char sourceName[]      = "partPredicateAfterIncSource";
        constexpr char queryName[]       = "partPredicateAfterIncQuery";

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(true);
        config.MutableFeatureFlags()->SetEnableUpdatingPartitionsOnStreamingQueryRestart(true);

        const ui32 initialPartitionCount = 4;
        CreateScopedTopicExt(inputTopicName, NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(initialPartitionCount, initialPartitionCount));
        CreateScopedTopic(outputTopicName);
        CreatePqSource(sourceName);

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT value FROM `{source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA = (value String NOT NULL)
                )
                WHERE __ydb_partition_id < 2;
                INSERT INTO `{source}`.`{output_topic}` SELECT value FROM $in;
            END DO;)",
            "query_name"_a = queryName,
            "source"_a    = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        for (ui32 i = 2; i < initialPartitionCount; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format("not_valid_json_p{}", i), i);
        }
        // Write valid JSON to partitions inside the predicate (0, 1).
        for (ui32 i = 0; i < 2; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format(R"({{"value": "before_data_p{}"}})", i), i);
        }

        ReadTopicMessages(outputTopicName,
            {"before_data_p0", "before_data_p1"},
            TInstant::Now() - TDuration::Seconds(100),
            /* sort */ true);

        Sleep(TDuration::Seconds(2));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));
        Sleep(TDuration::MilliSeconds(500));

        {
            NYdb::NTopic::TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .MinActivePartitions(20)
                    .MaxActivePartitions(20)
                .EndAlterTopicPartitioningSettings();
            const auto alterResult = GetTopicClient()
                ->AlterTopic(inputTopicName, alterSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                alterResult.GetStatus(), NYdb::EStatus::SUCCESS,
                alterResult.GetIssues().ToOneLineString());
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
            "query_name"_a = queryName
        ));

        Sleep(TDuration::Seconds(2));

        const TInstant afterRestart = TInstant::Now();

        const ui32 newPartitionCount = 20;
        for (ui32 i = 4; i < newPartitionCount; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format("not_valid_json_p{}", i), i);
        }

        WriteTopicMessage(inputTopicName, R"({"value": "after_data_p0"})", 0);
        WriteTopicMessage(inputTopicName, R"({"value": "after_data_p1"})", 1);

        ReadTopicMessages(outputTopicName,
            {"after_data_p0", "after_data_p1"},
            afterRestart,
            /* sort */ true);

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));
    }
}

} // namespace NKikimr::NKqp
