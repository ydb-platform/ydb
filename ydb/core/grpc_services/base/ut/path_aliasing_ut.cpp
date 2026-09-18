#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {
    namespace {

        std::shared_ptr<const NPathAliasing::TPathNormalizer> MakeNormalizer() {
            NKikimrConfig::TPathRewriteConfig config;
            auto* rule = config.AddRules();
            rule->SetPattern("^/raw$");
            rule->SetReplacement("/rewritten");
            return std::make_shared<const NPathAliasing::TPathNormalizer>(config);
        }

        std::unique_ptr<TEvRequestAuthAndCheck> MakeRequest() {
            return std::make_unique<TEvRequestAuthAndCheck>(
                "/raw", TMaybe<TString>{}, TActorId{}, TAuditMode::NonModifying(), "peer", "request-id");
        }

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
            auto request = MakeRequest();
            request->EnablePathNormalization();
            request->InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request->NormalizePath("/raw"), "/rewritten");

            request->UseDatabase("/resolved");
            request->InitializePathNormalization(MakeNormalizer());
            UNIT_ASSERT_VALUES_EQUAL(request->GetDatabaseName().GetOrElse(""), "/rewritten");
        }
    }

} // namespace NKikimr::NGRpcService
