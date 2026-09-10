#include <ydb/public/lib/ydb_cli/commands/sqs_workload/sqs_workload_scenario.h>

#include <library/cpp/testing/unittest/registar.h>

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

        Y_UNIT_TEST(SqsClientConfigurationDisablesExpectHeader) {
            TAwsSdkGuard awsSdk;
            TSqsWorkloadScenario scenario;
            scenario.Endpoint = "localhost:8771";
            scenario.RequestTimeoutMs = 2000;
            scenario.WorkersCount = 1;

            const auto clientConfiguration = scenario.CreateSqsClientConfiguration();

            UNIT_ASSERT(clientConfiguration.disableExpectHeader);
        }
    }

} // namespace NYdb::NConsoleClient
