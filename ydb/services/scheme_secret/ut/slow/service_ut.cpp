#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/services/scheme_secret/service.h>

#include <ydb/services/scheme_secret/ut/common/helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NKikimr::NSecret {

using NKqp::TKikimrRunner;
using NKqp::TKikimrSettings;
using TDescriptionPromise = NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription>;

Y_UNIT_TEST_SUITE(DescribeSchemaSecretsServiceSlow) {
    Y_UNIT_TEST(SchemeCacheRecoverAfterLookupErrorFails) {
        TKikimrSettings settings;
        // SchemeCache will return only retry errors, so secrets retrieval will be slow due to retries and never succeed
        auto schemeCacheStatusGetter = MakeHolder<TTestSchemeCacheStatusGetter>(
            TTestSchemeCacheStatusGetter::EFailProbability::Always);
        auto factory = std::make_shared<TTestDescribeSchemaSecretsServiceFactory>(
            /* secretUpdateListener */ nullptr,
            schemeCacheStatusGetter.Get(),
            /* schemeShardStatusGetter */ nullptr);
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

    Y_UNIT_TEST(SchemeShardRecoverAfterNotAvailableFails) {
        TKikimrSettings settings;
        auto schemeShardStatusGetter = MakeHolder<TTestSchemeShardStatusGetter>(
            /* statusOverwriteRemainingCount */ std::numeric_limits<ui32>::max(),
            NKikimrScheme::EStatus::StatusNotAvailable);
        auto factory = std::make_shared<TTestDescribeSchemaSecretsServiceFactory>(
            /* secretUpdateListener */ nullptr,
            /* schemeCacheStatusGetter */ nullptr,
            schemeShardStatusGetter.Get());
        settings.SetDescribeSchemaSecretsServiceFactory(factory);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString secretName = "/Root/secret-name";
        CreateSchemaSecret(secretName, "secret-value", session);

        TDescribeSecretSettings describeSettings;
        describeSettings.RetryPolicy = MakeShortRetryPolicy();
        auto promise = ResolveSecret(secretName, kikimr, /* userToken */ nullptr, describeSettings);
        AssertBadRequest(
            promise,
            "<main>: Error: Retry limit exceeded for secret `/Root/secret-name`\n",
            Ydb::StatusIds::UNAVAILABLE);
    }
}

} // namespace NKikimr::NSecret
