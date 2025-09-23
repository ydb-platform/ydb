#include "kqp_federated_query_actors.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>

namespace NYql {

namespace {
    void CreateSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
        auto createSecretQuery = "CREATE SECRET `" + secretName + "` WITH (value = \"" + secretValue + "\");";
        auto createSecretQueryResult = session.ExecuteSchemeQuery(createSecretQuery).GetValueSync();
        UNIT_ASSERT_C(createSecretQueryResult.GetStatus() == NYdb::EStatus::SUCCESS, createSecretQueryResult.GetIssues().ToString());
    }

    void AlterSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
        auto createSecretQuery = "ALTER  SECRET `" + secretName + "` WITH (value = \"" + secretValue + "\");";
        auto createSecretQueryResult = session.ExecuteSchemeQuery(createSecretQuery).GetValueSync();
        UNIT_ASSERT_C(createSecretQueryResult.GetStatus() == NYdb::EStatus::SUCCESS, createSecretQueryResult.GetIssues().ToString());
    }

    void DropSchemaSecret(const TString& secretName, NYdb::NTable::TSession& session) {
        auto createSecretQuery = "DROP  SECRET `" + secretName + "`;";
        auto createSecretQueryResult = session.ExecuteSchemeQuery(createSecretQuery).GetValueSync();
        UNIT_ASSERT_C(createSecretQueryResult.GetStatus() == NYdb::EStatus::SUCCESS, createSecretQueryResult.GetIssues().ToString());
    }

    NThreading::TPromise<NKikimr::NKqp::TEvDescribeSecretsResponse::TDescription>
    ResolveSecret(const TString& userId, const TString& secretName, NKikimr::NKqp::TKikimrRunner& kikimr) {
        auto promise = NThreading::NewPromise<NKikimr::NKqp::TEvDescribeSecretsResponse::TDescription>();
        const auto evResolveSecret = new NKikimr::NKqp::TDescribeSchemaSecretsService::TEvResolveSecret(userId,secretName, promise);
        auto actorSystem = kikimr.GetTestServer().GetRuntime()->GetActorSystem(0);
        actorSystem->Send(NKikimr::NKqp::MakeKqpDescribeSchemaSecretServiceId(actorSystem->NodeId), evResolveSecret);
        return promise;
    }
}

Y_UNIT_TEST_SUITE(DescribeSchemaSecretsService) {
    Y_UNIT_TEST(GetNewValue) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        NKikimr::NKqp::TKikimrRunner kikimr{ NKikimr::NKqp::TKikimrSettings(appCfg) };
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        for (int i = 0; i < 3; ++i) {
            auto promise = ResolveSecret("ownerId", "/Root/secret-name", kikimr);
            UNIT_ASSERT_VALUES_EQUAL(secretValue, promise.GetFuture().GetValueSync().SecretValues[0]);
        }
    }

    Y_UNIT_TEST(GetUpdatedValue) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        NKikimr::NKqp::TKikimrRunner kikimr{ NKikimr::NKqp::TKikimrSettings(appCfg) };
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        auto promise = ResolveSecret("ownerId", "/Root/secret-name", kikimr);
        UNIT_ASSERT_VALUES_EQUAL(secretValue, promise.GetFuture().GetValueSync().SecretValues[0]);

        for (int i = 0; i < 3; ++i) {
            TString newSecretValue = secretName + "-" + ToString(i);
            AlterSchemaSecret(secretName, newSecretValue, session);

            auto promise = ResolveSecret("ownerId", "/Root/secret-name", kikimr);
            UNIT_ASSERT_VALUES_EQUAL(newSecretValue, promise.GetFuture().GetValueSync().SecretValues[0]);
        }
    }

    Y_UNIT_TEST(GetUnexistingValue) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        NKikimr::NKqp::TKikimrRunner kikimr{ NKikimr::NKqp::TKikimrSettings(appCfg) };
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto promise = ResolveSecret("ownerId", "/Root/secret-not-exist", kikimr);

        UNIT_ASSERT_VALUES_EQUAL(Ydb::StatusIds::BAD_REQUEST, promise.GetFuture().GetValueSync().Status);
    }

    Y_UNIT_TEST(GetDroppedValue) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        NKikimr::NKqp::TKikimrRunner kikimr{ NKikimr::NKqp::TKikimrSettings(appCfg) };
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        auto promise = ResolveSecret("ownerId", "/Root/secret-name", kikimr);
        UNIT_ASSERT_VALUES_EQUAL(secretValue, promise.GetFuture().GetValueSync().SecretValues[0]);

        DropSchemaSecret(secretName, session);

        promise = ResolveSecret("ownerId", "/Root/secret-name", kikimr);
        UNIT_ASSERT_VALUES_EQUAL(Ydb::StatusIds::BAD_REQUEST, promise.GetFuture().GetValueSync().Status);

        secretValue += "-updated";
        CreateSchemaSecret(secretName, secretValue, session);

        promise = ResolveSecret("ownerId", "/Root/secret-name", kikimr);
        UNIT_ASSERT_VALUES_EQUAL(secretValue, promise.GetFuture().GetValueSync().SecretValues[0]);
    }

    Y_UNIT_TEST(GetInParallel) {
        static const int SECRETS_CNT = 5;
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
        NKikimr::NKqp::TKikimrRunner kikimr{ NKikimr::NKqp::TKikimrSettings(appCfg) };
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // new values
        std::vector<std::pair<TString, TString>> secrets;
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets.push_back({"/Root/secret-name-" + ToString(i), "secret-value-" + ToString(i)});
            CreateSchemaSecret(secrets.back().first, secrets.back().second, session);
        }
        std::vector<NThreading::TPromise<NKikimr::NKqp::TEvDescribeSecretsResponse::TDescription>> promises;
        for (const auto& [secretName, secretValue] : secrets) {
            promises.push_back(ResolveSecret("ownerId", secretName, kikimr));
        }

        for (int i = 0; i < SECRETS_CNT; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(secrets[i].second, promises[i].GetFuture().GetValueSync().SecretValues[0]);
        }

        // altered values
        promises.clear();
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets[i].second += "-new";
            AlterSchemaSecret(secrets[i].first, secrets[i].second, session);
        }
        for (const auto& [secretName, secretValue] : secrets) {
            promises.push_back(ResolveSecret("ownerId", secretName, kikimr));
        }

        for (int i = 0; i < SECRETS_CNT; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(secrets[i].second, promises[i].GetFuture().GetValueSync().SecretValues[0]);
        }
    }
}

}
