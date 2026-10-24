#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NKikimr::NGRpcService {
    namespace {

        std::shared_ptr<const NPathAliasing::TPathNormalizer> MakeNormalizer() {
            NKikimrConfig::TPathRewriteConfig config;
            auto* rule = config.AddRules();
            rule->SetSrc("/raw");
            rule->SetDst("/rewritten");
            return std::make_shared<const NPathAliasing::TPathNormalizer>(config);
        }

        std::unique_ptr<TEvHttpRequestAuthAndCheck> MakeRequest() {
            return std::make_unique<TEvHttpRequestAuthAndCheck>(
                "/raw", TMaybe<TString>{}, TActorId{}, TAuditMode::NonModifying(), "peer", "request-id");
        }

        class TNamedRequest final : public TEvHttpRequestAuthAndCheck {
        public:
            explicit TNamedRequest(TString method, const TString& database = "/raw")
                : TEvHttpRequestAuthAndCheck(
                    database, TMaybe<TString>{}, TActorId{}, TAuditMode::NonModifying(), "peer", "request-id")
                , Method_(std::move(method))
            {}

            TString GetRpcMethodName() const override {
                return Method_;
            }

        private:
            TString Method_;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(PathAliasingRequestContext) {
        Y_UNIT_TEST(InternalRequestKeepsLiveDatabaseAndIdentityPaths) {
            auto request = MakeRequest();
            request->InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request->NormalizePath("/raw"), "/raw");

            request->UseDatabase("/resolved");
            UNIT_ASSERT_VALUES_EQUAL(request->GetDatabaseName().GetOrElse(""), "/resolved");
        }

        Y_UNIT_TEST(ExplicitlyDisabledRequestKeepsLiveDatabase) {
            auto request = MakeRequest();
            request->EnablePathNormalization();
            request->DisablePathNormalization();
            request->InitializePathNormalization(MakeNormalizer());

            request->UseDatabase("/resolved");
            UNIT_ASSERT_VALUES_EQUAL(request->GetDatabaseName().GetOrElse(""), "/resolved");
        }

        Y_UNIT_TEST(EnabledRequestCachesRewrittenDatabase) {
            TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic");
            request.EnablePathNormalization();
            request.InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw"), "/rewritten");

            request.UseDatabase("/resolved");
            request.InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request.GetDatabaseName().GetOrElse(""), "/rewritten");
        }

        Y_UNIT_TEST(EmptyMappingKeepsCachedDatabase) {
            TNamedRequest request("Ydb.Query.V1.QueryService/ExecuteQuery");
            request.EnablePathNormalization();
            request.InitializePathNormalization(nullptr);
            UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw"), "/raw");
            request.UseDatabase("/resolved");
            UNIT_ASSERT_VALUES_EQUAL(request.GetDatabaseName().GetOrElse(""), "/raw");
        }

        Y_UNIT_TEST_TWIN(LegacyServicesKeepRawDatabaseAndIdentityPaths, relativePathsEnabled) {
            TActorSystemStub actorSystem;
            actorSystem.AppData.DomainsInfo = new TDomainsInfo;
            actorSystem.AppData.DomainsInfo->AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("Root").Release());
            actorSystem.AppData.FeatureFlags.SetEnableRelativePaths(relativePathsEnabled);
            const TString methods[] = {
                "Ydb.PersQueue.V1.PersQueueService/CreateTopic",
                "Ydb.PersQueue.V1.ClusterDiscoveryService/DiscoverClusters",
                "Ydb.Cms.V1.CmsService/CreateDatabase",
            };
            for (const auto& method : methods) {
                TNamedRequest request(method);
                request.EnablePathNormalization();
                request.InitializePathNormalization(MakeNormalizer());
                UNIT_ASSERT_VALUES_EQUAL_C(request.NormalizePath("/raw"), "/raw", method);
                UNIT_ASSERT_VALUES_EQUAL_C(request.GetDatabaseName().GetOrElse(""), "/raw", method);

                request.UseDatabase("/resolved");
                UNIT_ASSERT_VALUES_EQUAL_C(request.GetDatabaseName().GetOrElse(""), "/resolved", method);

                TNamedRequest relativeRequest(method, "mydb");
                relativeRequest.EnablePathNormalization();
                relativeRequest.InitializePathNormalization(MakeNormalizer());
                UNIT_ASSERT_VALUES_EQUAL_C(relativeRequest.GetDatabaseName().GetOrElse(""),
                    relativePathsEnabled ? "/Root/mydb" : "mydb", method);
                UNIT_ASSERT_VALUES_EQUAL_C(relativeRequest.NormalizePath("/raw"), "/raw", method);
            }
        }
    }

} // namespace NKikimr::NGRpcService
