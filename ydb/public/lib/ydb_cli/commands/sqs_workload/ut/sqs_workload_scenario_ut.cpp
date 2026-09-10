#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_run_read.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_run_write.h>
#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_topic.h>

#include <library/cpp/testing/unittest/registar.h>

TVector<NYdb::NTopic::ECodec> NYdb::NConsoleClient::InitAllowedCodecs() {
    return {
        NYdb::NTopic::ECodec::RAW,
        NYdb::NTopic::ECodec::ZSTD,
        NYdb::NTopic::ECodec::GZIP,
    };
}

namespace NYdb::NConsoleClient {

    namespace {

        class TAwsSdkGuard {
        public:
            TAwsSdkGuard() {
                Aws::InitAPI(Options);
            }

            ~TAwsSdkGuard() {
                Aws::ShutdownAPI(Options);
            }

        private:
            Aws::SDKOptions Options;
        };

        template <typename TCommand>
        void AssertAwsSdkLogOptionIsRegistered() {
            char arg0[] = "command";
            char* argv[] = {arg0};
            TClientCommand::TConfig config(Y_ARRAY_SIZE(argv), argv);
            TCommand command;

            command.PrepareOptions(config, false);

            const auto* option = command.Opts.GetOpts().FindLongOption("aws-sdk-log");
            UNIT_ASSERT(option);
            UNIT_ASSERT(option->GetHasArg() == NLastGetopt::NO_ARGUMENT);
        }

        TClientCommand::TConfig MakeEmptyConfig() {
            static char arg0[] = "command";
            static char* argv[] = {arg0};
            return TClientCommand::TConfig(Y_ARRAY_SIZE(argv), argv);
        }

    } // namespace

    Y_UNIT_TEST_SUITE(SqsWorkloadScenario) {
        Y_UNIT_TEST(AwsSdkLogIsOffByDefaultAndDebugWhenEnabled) {
            TSqsWorkloadScenario scenario;

            UNIT_ASSERT_EQUAL(
                scenario.GetAwsSdkLogLevel(),
                Aws::Utils::Logging::LogLevel::Off);

            scenario.AwsSdkLog = true;

            UNIT_ASSERT_EQUAL(
                scenario.GetAwsSdkLogLevel(),
                Aws::Utils::Logging::LogLevel::Debug);
        }

        Y_UNIT_TEST(InitAwsSdkAppliesLogLevel) {
            {
                TSqsWorkloadScenario scenario;
                scenario.AwsSdkLog = false;
                scenario.InitAwsSdk();
                scenario.DestroyAwsSdk();
            }

            {
                TSqsWorkloadScenario scenario;
                scenario.AwsSdkLog = true;
                scenario.InitAwsSdk();
                scenario.DestroyAwsSdk();
            }
        }

        Y_UNIT_TEST(SqsClientConfigurationUsesScenarioSettings) {
            TAwsSdkGuard awsSdk;
            TSqsWorkloadScenario scenario;
            scenario.Endpoint = "localhost:8771";
            scenario.RequestTimeoutMs = 2500;
            scenario.WorkersCount = 3;
            scenario.AwsRegion = "ru-central1";

            const auto clientConfiguration = scenario.CreateSqsClientConfiguration();

            UNIT_ASSERT_VALUES_EQUAL(clientConfiguration.endpointOverride, "localhost:8771");
            UNIT_ASSERT(clientConfiguration.scheme == Aws::Http::Scheme::HTTP);
            UNIT_ASSERT_VALUES_EQUAL(clientConfiguration.httpRequestTimeoutMs, 2500);
            UNIT_ASSERT(clientConfiguration.disableExpectHeader);
            UNIT_ASSERT_VALUES_EQUAL(clientConfiguration.maxConnections, 12);
            UNIT_ASSERT(clientConfiguration.executor);
            UNIT_ASSERT_VALUES_EQUAL(clientConfiguration.region, "ru-central1");
        }

        Y_UNIT_TEST(SqsClientConfigurationKeepsDefaultRegionWhenUnset) {
            TAwsSdkGuard awsSdk;
            TSqsWorkloadScenario scenario;
            scenario.Endpoint = "localhost:8771";
            scenario.RequestTimeoutMs = 2000;
            scenario.WorkersCount = 1;

            const auto defaultRegion = Aws::Client::ClientConfiguration().region;
            const auto clientConfiguration = scenario.CreateSqsClientConfiguration();

            UNIT_ASSERT(clientConfiguration.disableExpectHeader);
            UNIT_ASSERT_VALUES_EQUAL(clientConfiguration.region, defaultRegion);
        }

        Y_UNIT_TEST(InitStatsCollectorCreatesCollector) {
            TSqsWorkloadScenario scenario;
            scenario.Quiet = true;
            scenario.PrintTimestamp = false;
            scenario.WindowSec = TDuration::Seconds(1);
            scenario.TotalSec = TDuration::Seconds(1);
            scenario.Percentile = 80.0;

            scenario.InitStatsCollector(/*writerCount=*/1, /*readerCount=*/1);

            UNIT_ASSERT(scenario.StatsCollector);
        }

        Y_UNIT_TEST(InitSqsClientUsesCreatedConfiguration) {
            TAwsSdkGuard awsSdk;
            TSqsWorkloadScenario scenario;
            scenario.Endpoint = "localhost:8771";
            scenario.RequestTimeoutMs = 2000;
            scenario.WorkersCount = 1;
            scenario.UseXmlAPI = true;
            scenario.Quiet = true;
            scenario.WindowSec = TDuration::Seconds(1);
            scenario.TotalSec = TDuration::Seconds(1);
            scenario.Percentile = 80.0;
            scenario.InitStatsCollector(/*writerCount=*/0, /*readerCount=*/0);

            auto config = MakeEmptyConfig();
            scenario.InitSqsClient(config);

            UNIT_ASSERT(scenario.SqsClient);
            scenario.DestroySqsClient();
            UNIT_ASSERT(!scenario.SqsClient);
        }

        Y_UNIT_TEST(InitSqsClientJsonApiPath) {
            TAwsSdkGuard awsSdk;
            TSqsWorkloadScenario scenario;
            scenario.Endpoint = "localhost:8771";
            scenario.RequestTimeoutMs = 2000;
            scenario.WorkersCount = 1;
            scenario.UseXmlAPI = false;
            scenario.Quiet = true;
            scenario.WindowSec = TDuration::Seconds(1);
            scenario.TotalSec = TDuration::Seconds(1);
            scenario.Percentile = 80.0;
            scenario.AwsAccessKeyId = "AKIA_TEST";
            scenario.AwsSecretKey = "secret";
            scenario.AwsRegion = "ru-central1";
            scenario.InitStatsCollector(/*writerCount=*/0, /*readerCount=*/0);

            auto config = MakeEmptyConfig();
            scenario.InitSqsClient(config);

            UNIT_ASSERT(scenario.SqsClient);
            scenario.DestroySqsClient();
        }

        Y_UNIT_TEST(ReadCommandRegistersAwsSdkLogOption) {
            AssertAwsSdkLogOptionIsRegistered<TCommandWorkloadSqsRunRead>();
        }

        Y_UNIT_TEST(WriteCommandRegistersAwsSdkLogOption) {
            AssertAwsSdkLogOptionIsRegistered<TCommandWorkloadSqsRunWrite>();
        }
    }

} // namespace NYdb::NConsoleClient
