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
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

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
        // TAccessServiceMock implements the *v1* servicecontrol interface, but
        // EnableAccessServiceV2Interface defaults to true -- which would make the
        // ticket parser call accessservice.v2/Authenticate, get UNIMPLEMENTED, treat
        // it as a *retryable* error, and retry forever (request hangs, never fails).
        // ticket_parser_ut.cpp couples the two the same way via
        // SetEnableAccessServiceV2Interface(IsAccessServiceV2Interface<TMock>()).
        settings.SetEnableAccessServiceV2Interface(false);
        // A runtime observer is only consulted by the test runtime's own mailbox
        // dispatch loop; with real executor threads it is never invoked. Observers
        // therefore require the simulated runtime, which in turn means every
        // blocking client call must run off-thread while this thread pumps the
        // runtime -- see runCall below (same pattern as TKikimrRunner::RunCall).
        settings.SetUseRealThreads(false);
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

        // Run a blocking call on a worker thread while this thread advances the
        // simulated runtime, so the actor system keeps making progress.
        TAdaptiveThreadPool threadPool;
        threadPool.Start();
        auto runCall = [&](auto&& func) {
            return runtime->WaitFuture(NThreading::Async(std::move(func), threadPool));
        };

        TClient client(settings);
        runCall([&] { client.InitRootScheme(); return true; });

        // ---- authorize the IAM user on /Root ----
        // Authentication alone is not enough: TxProxy checks the ACL *before*
        // forwarding to schemeshard (schemereq.cpp: "Access denied for user1@as on
        // path /Root, with access CreateTable"), so without this the CREATE never
        // produces a TEvModifySchemeTransaction and there is no token to capture.
        {
            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, "user1@as");
            const TString serializedAcl = acl.SerializeAsString();
            runCall([&] { return client.ModifyACL("/", "Root", serializedAcl); });
        }

        // ---- diagnostics: surface auth + grpc activity so a hang is explainable ----
        runtime->SetLogPriority(NKikimrServices::TICKET_PARSER, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);

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
            // Async discovery is required under the simulated runtime: the default
            // Sync mode blocks on ListEndpoints with a ~10s *real* deadline, which
            // the simulated clock fast-forwards past while the actor system idles
            // (the server then sees the deadline already expired). TKikimrRunner
            // sets this for the same reason.
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Async)
            .SetAuthToken(iamToken));            // <- the raw IAM token the user presents

        NYdb::NQuery::TQueryClient queryClient(driver);

        // Bound every request: ClientTimeout defaults to TDuration::Max(), so a
        // stuck request would otherwise hang until the suite timeout with no info.
        auto execSettings = NYdb::NQuery::TExecuteQuerySettings()
            .ClientTimeout(TDuration::Seconds(30));

        // Probe first: does *any* query work? Distinguishes "query service never
        // became ready / auth never happened" from "the EDS DDL was rejected".
        {
            auto probe = runCall([&] {
                return queryClient.ExecuteQuery(
                    "SELECT 1;", NYdb::NQuery::TTxControl::NoTx(), execSettings).GetValueSync();
            });
            Cerr << "PROBE(SELECT 1) status=" << probe.GetStatus()
                 << " issues=" << probe.GetIssues().ToString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL_C(probe.GetStatus(), NYdb::EStatus::SUCCESS,
                "trivial query failed -- server/auth wiring is broken, not the EDS DDL: "
                    + probe.GetIssues().ToString());
        }

        const TString ddl = R"(
            CREATE EXTERNAL DATA SOURCE `/Root/MyExternalDataSource` WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION    = "my-bucket",
                AUTH_METHOD = "NONE"
            );)";
        auto res = runCall([&] {
            return queryClient.ExecuteQuery(
                ddl, NYdb::NQuery::TTxControl::NoTx(), execSettings).GetValueSync();
        });
        Cerr << "DDL(CREATE EDS) status=" << res.GetStatus()
             << " issues=" << res.GetIssues().ToString() << Endl;
        Cerr << "observer: sawCreateEds=" << sawCreateEds
             << " capturedTokenBytes=" << capturedSerializedToken.size() << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), NYdb::EStatus::SUCCESS, res.GetIssues().ToString());

        // ---- assertions: the token reached schemeshard, intact ----
        UNIT_ASSERT_C(sawCreateEds, "schemeshard did not receive CreateExternalDataSource");
        UNIT_ASSERT_C(!capturedSerializedToken.empty(),
            "no user token on the modify-scheme event at schemeshard");

        NACLib::TUserToken parsed(capturedSerializedToken);   // same as schemeshard ParseUserToken
        UNIT_ASSERT_VALUES_EQUAL(parsed.GetUserSID(), "user1@as");
        UNIT_ASSERT_VALUES_EQUAL_C(parsed.GetOriginalUserToken(), iamToken,
            "the raw IAM token must be present in the TUserToken at the schemeshard operation");

        runCall([&] { driver.Stop(true); return true; });
        threadPool.Stop();
        accessServer->Shutdown();
    }
}
