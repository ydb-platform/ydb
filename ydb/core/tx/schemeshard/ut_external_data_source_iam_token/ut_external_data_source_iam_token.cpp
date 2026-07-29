// Verification test for the IAM-delegation RFC
// (rfc/external_data_source_cloud_authentication_for_ydb.md).
//
// Goal: prove that when a user authenticated with a *cloud IAM token* runs
// CREATE EXTERNAL DATA SOURCE, the schemeshard operation actually sees that
// raw IAM token — i.e. NACLib::TUserToken::GetOriginalUserToken() is non-empty
// and equals the token the user presented. This is the fact the RFC relies on
// for sourcing SetupDelegation's on_behalf_of at CREATE time.
//
// It exercises the *real* path (no hand-built token):
//   NYdb driver (auth token) -> gRPC -> TicketParser -> fake AccessService
//   -> KQP -> TxProxy -> SchemeShard (TEvModifySchemeTransaction).
//
// A runtime observer captures the serialized user token off the inbound
// TEvModifySchemeTransaction (exactly what schemeshard__operation.cpp's
// ParseUserToken reads) and re-parses it to inspect OriginalUserToken/UserSID.

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

using namespace NKikimr;
using namespace NKikimr::Tests;

Y_UNIT_TEST_SUITE(ExternalDataSourceIamToken) {

    Y_UNIT_TEST(UserIamTokenReachesSchemeShardOnCreate) {
        // ---- ports ----
        TPortManager tp;
        const ui16 port = tp.GetPort(2134);
        const ui16 grpcPort = tp.GetPort(2135);
        const ui16 accessServicePort = tp.GetPort(4284);
        const TString accessServiceEndpoint = "localhost:" + ToString(accessServicePort);

        // ---- fake IAM: AccessService that authenticates one token to a subject ----
        const TString iamToken = "user1-iam-token";
        TAccessServiceMock accessServiceMock;
        accessServiceMock.AuthenticateData[iamToken]
            .Response.mutable_subject()->mutable_user_account()->set_id("user1");

        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials())
               .RegisterService(&accessServiceMock);
        std::unique_ptr<grpc::Server> accessServer(builder.BuildAndStart());

        // ---- point the ticket parser at the mock (cloud AccessService path) ----
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseLoginProvider(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetAccessServiceType("Yandex_v2");
        authConfig.SetAccessServiceEndpoint(accessServiceEndpoint);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetUseStaff(false);

        // ---- server with the real ticket parser + external data sources enabled ----
        auto settings = TServerSettings(port, authConfig);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        settings.AppConfig->MutableFeatureFlags()->SetEnableExternalDataSources(true);
        settings.AppConfig->MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        settings.AppConfig->MutableQueryServiceConfig()->AddHostnamePatterns("my-bucket");

        TServer server(settings);
        server.EnableGRpc(grpcPort);
        auto* runtime = server.GetRuntime();
        for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
            runtime->GetAppData(i).FeatureFlags.SetEnableExternalDataSources(true);
        }

        TClient client(settings);
        client.InitRootScheme();

        // ---- observer: capture the serialized user token off the CREATE EDS
        //      modify-scheme event as it reaches schemeshard ----
        TString capturedSerializedToken;
        bool sawCreateEds = false;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NSchemeShard::TEvSchemeShard::EvModifySchemeTransaction) {
                const auto* msg = ev->Get<NSchemeShard::TEvSchemeShard::TEvModifySchemeTransaction>();
                for (const auto& tx : msg->Record.GetTransaction()) {
                    if (tx.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateExternalDataSource) {
                        sawCreateEds = true;
                        capturedSerializedToken = msg->Record.GetUserToken();
                    }
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });

        // ---- run CREATE EXTERNAL DATA SOURCE as the IAM-authenticated user ----
        auto driver = NYdb::TDriver(NYdb::TDriverConfig()
            .SetEndpoint("localhost:" + ToString(grpcPort))
            .SetDatabase("/Root")
            .SetAuthToken(iamToken));            // <- the raw IAM token the user presents

        NYdb::NQuery::TQueryClient queryClient(driver);
        const TString ddl = R"(
            CREATE EXTERNAL DATA SOURCE `/Root/MyExternalDataSource` WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION    = "my-bucket",
                AUTH_METHOD = "NONE"
            );)";
        auto res = queryClient.ExecuteQuery(ddl, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), NYdb::EStatus::SUCCESS, res.GetIssues().ToString());

        // ---- assertions: the token reached schemeshard, intact ----
        UNIT_ASSERT_C(sawCreateEds, "schemeshard did not receive CreateExternalDataSource");
        UNIT_ASSERT_C(!capturedSerializedToken.empty(),
            "no user token on the modify-scheme event at schemeshard");

        NACLib::TUserToken parsed(capturedSerializedToken);   // same as schemeshard ParseUserToken
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetUserSID(), "user1@as");
        UNIT_ASSERT_VALUES_EQUAL_C(parsed.GetOriginalUserToken(), iamToken,
            "the raw IAM token must be present in the TUserToken at the schemeshard operation");

        driver.Stop(true);
        accessServer->Shutdown();
    }
}
