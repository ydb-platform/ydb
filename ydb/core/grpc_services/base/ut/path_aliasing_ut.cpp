#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>
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
            explicit TNamedRequest(TString method, TString database = "/raw")
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

        ui64 GetPathAliasCount(const NMonitoring::TDynamicCounterPtr& root, const char* service, const char* method) {
            auto counter = root->GetSubgroup("counters", "ydb")
                ->GetSubgroup("api_service", service)
                ->GetSubgroup("method", method)
                ->FindNamedCounter("name", "api.grpc.request.path_alias_count");
            return counter ? counter->Val() : 0;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(PathAliasingRequestContext) {
        Y_UNIT_TEST(DatabaseAndResourceAliasesCountOncePerRequest) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            for (ui64 count = 1; count <= 2; ++count) {
                TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic");
                request.EnablePathNormalization();
                request.InitializePathNormalization(MakeNormalizer(), root);
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "Ydb.Topic.V1.TopicService", "CreateTopic"), count);

                UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw/a"), "/rewritten/a");
                UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw/b"), "/rewritten/b");
                request.InitializePathNormalization(MakeNormalizer(), root);
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "Ydb.Topic.V1.TopicService", "CreateTopic"), count);
            }
        }

        Y_UNIT_TEST(ResourceAliasCountsWithServiceAndMethodLabels) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            const std::pair<const char*, const char*> labels[] = {
                {"Ydb.Topic.V1.TopicService", "CreateTopic"},
                {"Ydb.Topic.V1.TopicService", "DescribeTopic"},
                {"Ydb.Scheme.V1.SchemeService", "DescribePath"},
            };
            for (const auto& [service, method] : labels) {
                TNamedRequest request(TString(service) + "/" + method, "/physical");
                request.EnablePathNormalization();
                request.InitializePathNormalization(MakeNormalizer(), root);
                UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/physical/a"), "/physical/a");
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, service, method), 0);
                UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw/a"), "/rewritten/a");
            }
            for (const auto& [service, method] : labels) {
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, service, method), 1);
            }
        }

        Y_UNIT_TEST(UnchangedAndDisabledPathsDoNotCount) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            TNamedRequest disabled("Ydb.Topic.V1.TopicService/CreateTopic");
            disabled.EnablePathNormalization();
            disabled.DisablePathNormalization();
            disabled.InitializePathNormalization(MakeNormalizer(), root);
            UNIT_ASSERT_VALUES_EQUAL(disabled.NormalizePath("/raw/a"), "/raw/a");

            TNamedRequest empty("Ydb.Topic.V1.TopicService/CreateTopic");
            empty.EnablePathNormalization();
            empty.InitializePathNormalization(nullptr, root);
            UNIT_ASSERT_VALUES_EQUAL(empty.NormalizePath("/raw/a"), "/raw/a");

            TNamedRequest legacy("Ydb.PersQueue.V1.PersQueueService/CreateTopic");
            legacy.EnablePathNormalization();
            legacy.InitializePathNormalization(MakeNormalizer(), root);
            UNIT_ASSERT_VALUES_EQUAL(legacy.NormalizePath("/raw/a"), "/raw/a");
            UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "Ydb.Topic.V1.TopicService", "CreateTopic"), 0);
            UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "Ydb.PersQueue.V1.PersQueueService", "CreateTopic"), 0);
        }

        Y_UNIT_TEST(ConcurrentNormalizationCountsOnce) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic", "/physical");
            request.EnablePathNormalization();
            request.InitializePathNormalization(MakeNormalizer(), root);
            std::thread threads[8];
            for (auto& thread : threads) {
                thread = std::thread([&request] {
                    for (size_t i = 0; i < 100; ++i) {
                        request.NormalizePath("/raw/a");
                    }
                });
            }
            for (auto& thread : threads) {
                thread.join();
            }
            UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "Ydb.Topic.V1.TopicService", "CreateTopic"), 1);
        }

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

        Y_UNIT_TEST(LegacyServicesKeepRawDatabaseAndIdentityPaths) {
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
            }
        }
    }

} // namespace NKikimr::NGRpcService
