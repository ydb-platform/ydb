#include "kqp_federated_query_actors.h"

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/federated_query/ut_service/common/helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using TDescriptionPromise = NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>;

Y_UNIT_TEST_SUITE(DescribeSchemaSecretsServiceSlow) {
    Y_UNIT_TEST(SchemeCacheRecoverAfterLookupErrorFails) {
        TKikimrSettings settings;
        // SchemeCache will return only retry errors, so secrets retrieval will be slow due to retries and never succeed
        auto schemeCacheStatusGetter = MakeHolder<TTestSchemeCacheStatusGetter>(
            TTestSchemeCacheStatusGetter::EFailProbability::Always);
        auto factory = std::make_shared<TTestDescribeSchemaSecretsServiceFactory>(
            /* secretUpdateListener */ nullptr,
            schemeCacheStatusGetter.Get());
        settings.SetDescribeSchemaSecretsServiceFactory(factory);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        static const auto SECRETS_CNT = 20;
        std::vector<std::pair<TString, TString>> secrets;
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets.push_back({"/Root/secret-name-" + ToString(i), "secret-value-" + ToString(i)});
            CreateSchemaSecret(secrets.back().first, secrets.back().second, session);
        }
        std::vector<TDescriptionPromise> promises;
        for (const auto& [secretName, secretValue] : secrets) {
            promises.push_back(ResolveSecret(secretName, kikimr));
        }

        for (int i = 0; i < SECRETS_CNT; ++i) {
            AssertBadRequest(
                promises[i],
                Sprintf(
                    "<main>: Error: Retry limit exceeded for secret `%s`\n",
                    secrets[i].first.c_str()),
                Ydb::StatusIds::UNAVAILABLE);
        }

        // SchemeCache will return OK responses, so secrets retrieval should be fast and succeeded
        schemeCacheStatusGetter->SetFailProbability(TTestSchemeCacheStatusGetter::EFailProbability::None);
        promises.clear();
        for (const auto& [secretName, secretValue] : secrets) {
            promises.push_back(ResolveSecret(secretName, kikimr));
        }

        for (int i = 0; i < SECRETS_CNT; ++i) {
            AssertSecretValue(secrets[i].second, promises[i]);
        }
    }
}

} // NKikimr::NKqp
