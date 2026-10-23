#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
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
            explicit TNamedRequest(TString method, TString database = "/raw", NYdbGrpc::ICounterBlockPtr counters = {})
                : TEvHttpRequestAuthAndCheck(
                    database, TMaybe<TString>{}, TActorId{}, TAuditMode::NonModifying(), "peer", "request-id")
                , Method_(std::move(method))
                , Counters_(std::move(counters))
            {}

            TString GetRpcMethodName() const override {
                return Method_;
            }

        protected:
            NYdbGrpc::ICounterBlock* GetRequestCounters() const override {
                return Counters_.Get();
            }

        private:
            TString Method_;
            NYdbGrpc::ICounterBlockPtr Counters_;
        };

        ui64 GetPathAliasCount(const NMonitoring::TDynamicCounterPtr& root, const char* service, const char* method) {
            auto counter = root->GetSubgroup("counters", "ydb")
                ->GetSubgroup("api_service", service)
                ->GetSubgroup("method", method)
                ->FindNamedCounter("name", "api.grpc.request.path_alias_count");
            UNIT_ASSERT(counter);
            return counter->Val();
        }

    } // namespace

    Y_UNIT_TEST_SUITE(PathAliasingRequestContext) {
        Y_UNIT_TEST(DatabaseAndResourceAliasesCountOncePerRequest) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            auto counters = CreateCounterCb(root, nullptr)("topic", "CreateTopic");
            for (ui64 count = 1; count <= 2; ++count) {
                TNamedRequest request("Ydb.Topic.V1.TopicService/CreateTopic", "/raw", counters);
                request.EnablePathNormalization();
                request.InitializePathNormalization(MakeNormalizer());
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "topic", "CreateTopic"), count);

                UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw/a"), "/rewritten/a");
                UNIT_ASSERT_VALUES_EQUAL(request.NormalizePath("/raw/b"), "/rewritten/b");
                request.InitializePathNormalization(MakeNormalizer());
                UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "topic", "CreateTopic"), count);
            }
        }

        Y_UNIT_TEST(ResourceAliasCountsWithServiceAndMethodLabels) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            auto makeCounters = CreateCounterCb(root, nullptr);
            const std::pair<const char*, const char*> labels[] = {
                {"topic", "CreateTopic"}, {"topic", "DescribeTopic"}, {"scheme", "DescribePath"},
            };
            for (const auto& [service, method] : labels) {
                TNamedRequest request("request", "/physical", makeCounters(service, method));
                request.EnablePathNormalization();
                request.InitializePathNormalization(MakeNormalizer());
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
            auto counters = CreateCounterCb(root, nullptr)("topic", "CreateTopic");
            TNamedRequest disabled("request", "/raw", counters);
            disabled.EnablePathNormalization();
            disabled.DisablePathNormalization();
            disabled.InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(disabled.NormalizePath("/raw/a"), "/raw/a");

            TNamedRequest empty("request", "/raw", counters);
            empty.EnablePathNormalization();
            empty.InitializePathNormalization(nullptr);
            UNIT_ASSERT_VALUES_EQUAL(empty.NormalizePath("/raw/a"), "/raw/a");

            TNamedRequest legacy("Ydb.PersQueue.V1.PersQueueService/CreateTopic", "/raw", counters);
            legacy.EnablePathNormalization();
            legacy.InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(legacy.NormalizePath("/raw/a"), "/raw/a");
            UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "topic", "CreateTopic"), 0);
        }

        Y_UNIT_TEST(ConcurrentNormalizationCountsOnce) {
            NMonitoring::TDynamicCounterPtr root = new NMonitoring::TDynamicCounters;
            TNamedRequest request("request", "/physical", CreateCounterCb(root, nullptr)("topic", "CreateTopic"));
            request.EnablePathNormalization();
            request.InitializePathNormalization(MakeNormalizer());
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
            UNIT_ASSERT_VALUES_EQUAL(GetPathAliasCount(root, "topic", "CreateTopic"), 1);
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
