#include "kqp_federated_query_actors.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/common/simple/services.h>

namespace NKikimr::NKqp {
using TDescriptionPromise = NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>;

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

    TDescriptionPromise
    ResolveSecrets(const TVector<TString>& secretNames, TKikimrRunner& kikimr, const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr) {
        auto promise = NThreading::NewPromise<TEvDescribeSecretsResponse::TDescription>();
        const auto evResolveSecret = new TDescribeSchemaSecretsService::TEvResolveSecret(userToken, "/Root", secretNames, promise);
        auto actorSystem = kikimr.GetTestServer().GetRuntime()->GetActorSystem(0);
        actorSystem->Send(MakeKqpDescribeSchemaSecretServiceId(actorSystem->NodeId), evResolveSecret);
        return promise;
    }

    TDescriptionPromise
    ResolveSecret(const TString& secretName, TKikimrRunner& kikimr, const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr) {
        return ResolveSecrets(TVector<TString>{secretName}, kikimr, userToken);
    }

    void AssertBadRequest(TDescriptionPromise promise, const TString& err) {
        UNIT_ASSERT_VALUES_EQUAL(Ydb::StatusIds::BAD_REQUEST, promise.GetFuture().GetValueSync().Status);
        UNIT_ASSERT_VALUES_EQUAL(err, promise.GetFuture().GetValueSync().Issues.ToString());
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken(const TString& userSid = "", const TVector<TString>& groupSids = {}) {
        if (userSid.empty() && groupSids.empty()) {
            return nullptr;
        }
        return new NACLib::TUserToken(userSid, groupSids);
    }

    void AssertSecretValues(const TVector<TString>& secretValues, TDescriptionPromise promise) {
        UNIT_ASSERT_VALUES_EQUAL_C(secretValues.size(), promise.GetFuture().GetValueSync().SecretValues.size(), promise.GetFuture().GetValueSync().Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(secretValues, promise.GetFuture().GetValueSync().SecretValues);
    }

    void AssertSecretValue(const TString& secretValue, TDescriptionPromise promise) {
        AssertSecretValues(TVector<TString>{secretValue}, promise);
    }
}

Y_UNIT_TEST_SUITE(DescribeSchemaSecretsService) {
    Y_UNIT_TEST(GetNewValue) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        for (int i = 0; i < 3; ++i) {
            auto promise = ResolveSecret("/Root/secret-name", kikimr);
            AssertSecretValue(secretValue, promise);
        }
    }

    Y_UNIT_TEST(GetUpdatedValue) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        auto promise = ResolveSecret("/Root/secret-name", kikimr);
        AssertSecretValue(secretValue, promise);

        for (int i = 0; i < 3; ++i) {
            TString newSecretValue = secretValue + "-" + ToString(i);
            AlterSchemaSecret(secretName, newSecretValue, session);

            auto promise = ResolveSecret("/Root/secret-name", kikimr);
            AssertSecretValue(newSecretValue, promise);
        }
    }

    Y_UNIT_TEST(GetUnexistingValue) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto promise = ResolveSecret("/Root/secret-not-exist", kikimr);

        AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-not-exist` not found\n");
    }

    Y_UNIT_TEST(GetDroppedValue) {
        class TTestSecretUpdateListener : public TDescribeSchemaSecretsService::ISecretUpdateListener {
        public:
            NThreading::TPromise<TString> DeletionPromise = NThreading::NewPromise<TString>();

        public:
            void HandleNotifyDelete(const TString& secretName) override {
                Y_ENSURE(!DeletionPromise.HasValue()); // only one call of HandleNotifyDelete is expected
                DeletionPromise.SetValue(secretName);
            }
        };

        class TTestDescribeSchemaSecretsServiceFactory : public IDescribeSchemaSecretsServiceFactory {
        public:
            TTestDescribeSchemaSecretsServiceFactory(TDescribeSchemaSecretsService::ISecretUpdateListener* secretUpdateListener)
                : SecretUpdateListener(secretUpdateListener)
            {
            }

            NActors::IActor* CreateService() override {
                auto* service = new TDescribeSchemaSecretsService();
                service->SetSecretUpdateListener(SecretUpdateListener);
                return service;
            }

        private:
            TDescribeSchemaSecretsService::ISecretUpdateListener* SecretUpdateListener;
        };

        TKikimrSettings settings;
        auto secretUpdateListener = MakeHolder<TTestSecretUpdateListener>();
        auto factory = std::make_shared<TTestDescribeSchemaSecretsServiceFactory>(secretUpdateListener.Get());
        settings.SetDescribeSchemaSecretsServiceFactory(factory);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString secretName = "/Root/secret-name";
        TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        auto promise = ResolveSecret("/Root/secret-name", kikimr);
        AssertSecretValue(secretValue, promise);

        DropSchemaSecret(secretName, session);
        UNIT_ASSERT_VALUES_EQUAL("/Root/secret-name", secretUpdateListener->DeletionPromise.GetFuture().GetValueSync());

        promise = ResolveSecret("/Root/secret-name", kikimr);
        AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-name` not found\n");

        secretValue += "-updated";
        CreateSchemaSecret(secretName, secretValue, session);

        promise = ResolveSecret("/Root/secret-name", kikimr);
        AssertSecretValue(secretValue, promise);
    }

    Y_UNIT_TEST(GetInParallel) {
        static const int SECRETS_CNT = 5;
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // new values
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
            AssertSecretValue(secrets[i].second, promises[i]);
        }

        // altered values
        promises.clear();
        for (int i = 0; i < SECRETS_CNT; ++i) {
            secrets[i].second += "-new";
            AlterSchemaSecret(secrets[i].first, secrets[i].second, session);
        }
        for (const auto& [secretName, secretValue] : secrets) {
            promises.push_back(ResolveSecret(secretName, kikimr));
        }

        for (int i = 0; i < SECRETS_CNT; ++i) {
            AssertSecretValue(secrets[i].second, promises[i]);
        }
    }

    Y_UNIT_TEST(GetSameValueMultipleTimes) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        CreateSchemaSecret(secretName, secretValue, session);

        auto promise = ResolveSecrets({secretName, secretName}, kikimr);
        AssertSecretValues({secretValue, secretValue}, promise);
    }

    Y_UNIT_TEST(FailWithoutGrants) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        auto adminSession = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin"))
            .CreateSession().GetValueSync().GetSession();

        CreateSchemaSecret(secretName, secretValue, adminSession);

        auto promise = ResolveSecret(secretName, kikimr, GetUserToken("root@builtin"));
        AssertSecretValue(secretValue, promise);

        const auto userToken = GetUserToken("user@builtin");
        { // assert no grants by default
            auto promise = ResolveSecret("/Root/secret-name", kikimr, userToken);
            AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-name` not found\n");
        }

        // provide grants
        const auto grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", secretName.data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        { // assert grants are ok
            auto promise = ResolveSecret("/Root/secret-name", kikimr, userToken);
            AssertSecretValue(secretValue, promise);
        }

        // revoke grants
        const auto revokeResult = adminSession.ExecuteSchemeQuery(
            Sprintf("REVOKE 'ydb.granular.select_row' ON `%s` FROM `%s`;", secretName.data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(revokeResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        { // assert no grants after revoking
            auto promise = ResolveSecret("/Root/secret-name", kikimr, userToken);
            AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-name` not found\n");
        }
    }

    Y_UNIT_TEST(GroupGrants) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        const TString secretName = "/Root/secret-name";
        const TString secretValue = "secret-value";
        auto adminSession = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin"))
            .CreateSession().GetValueSync().GetSession();

        CreateSchemaSecret(secretName, secretValue, adminSession);

        auto promise = ResolveSecret(secretName, kikimr, GetUserToken("root@builtin"));
        AssertSecretValue(secretValue, promise);

        const auto userToken = GetUserToken("user@builtin", {"group"});
        { // assert no grants by default
            auto promise = ResolveSecret("/Root/secret-name", kikimr, userToken);
            AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-name` not found\n");
        }

        const auto createGroupResult = adminSession.ExecuteSchemeQuery(
            Sprintf("CREATE GROUP `group` WITH USER `user@builtin`;")
        ).GetValueSync();
        UNIT_ASSERT_C(createGroupResult.GetStatus() == NYdb::EStatus::SUCCESS, createGroupResult.GetIssues().ToString());

        const auto grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", secretName.data(), "group")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        { // assert group grants are ok
            auto promise = ResolveSecret("/Root/secret-name", kikimr, userToken);
            AssertSecretValue(secretValue, promise);
        }

        // revoke grants
        const auto revokeResult = adminSession.ExecuteSchemeQuery(
            Sprintf("REVOKE 'ydb.granular.select_row' ON `%s` FROM `%s`;", secretName.data(), "group")
        ).GetValueSync();
        UNIT_ASSERT_C(revokeResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        { // assert no grants after revoking
            auto promise = ResolveSecret("/Root/secret-name", kikimr, userToken);
            AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-name` not found\n");
        }
    }

    Y_UNIT_TEST(BatchRequest) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        const TString secretName1 = "/Root/secret-name-1";
        const TString secretValue1 = "secret-value-1";
        const TString secretName2 = "/Root/secret-name-2";
        const TString secretValue2 = "secret-value-2";
        const TString secretName3 = "/Root/secret-name-3";
        const TString secretValue3 = "secret-value-3";

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        CreateSchemaSecret(secretName1, secretValue1, session);
        CreateSchemaSecret(secretName2, secretValue2, session);
        CreateSchemaSecret(secretName3, secretValue3, session);

        { // nothing from cache
            auto promise = ResolveSecrets({secretName1, secretName2}, kikimr);
            AssertSecretValues({secretValue1, secretValue2}, promise);
        }

        { // something from cache
            auto promise = ResolveSecrets({secretName2, secretName3}, kikimr);
            AssertSecretValues({secretValue2, secretValue3}, promise);
        }

        { // all from cache
            auto promise = ResolveSecrets({secretName1, secretName2, secretName3}, kikimr);
            AssertSecretValues({secretValue1, secretValue2, secretValue3}, promise);
        }
    }

    Y_UNIT_TEST(BigBatchRequest) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        TVector<TString> names;
        TVector<TString> values;
        for (int i = 0; i < 10; ++i) {
            names.push_back("/Root/secret-name-" + ToString(i));
            values.push_back("secret-value-" + ToString(i));
        }

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        for (size_t i = 0; i < names.size(); ++i) {
            CreateSchemaSecret(names[i], values[i], session);
        }

        { // nothing from cache
            const auto SecretsToResolveCnt = names.size() / 2;
            auto promise = ResolveSecrets({names.begin(), names.begin() + SecretsToResolveCnt}, kikimr);
            AssertSecretValues({values.begin(), values.begin() + SecretsToResolveCnt}, promise);
        }

        { // something from cache
            auto promise = ResolveSecrets(names, kikimr);
            AssertSecretValues(values, promise);
        }
    }

    Y_UNIT_TEST(EmptyBatch) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        auto promise = ResolveSecrets(TVector<TString>{}, kikimr);
        AssertBadRequest(promise, "<main>: Error: empty secret names list\n");
    }

    Y_UNIT_TEST(MixedGrantsInBatch) {
        TKikimrRunner kikimr;
        kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);

        auto adminSession = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken("root@builtin"))
            .CreateSession().GetValueSync().GetSession();

        TVector<TString> names;
        TVector<TString> values;
        for (int i = 0; i < 2; ++i) {
            names.push_back("/Root/secret-name-" + ToString(i));
            values.push_back("secret-value-" + ToString(i));
            CreateSchemaSecret(names.back(), values.back(), adminSession);
        }

        auto grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", names[0].data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        auto userToken = GetUserToken("user@builtin");
        { // user has grants for names[0], has no grants for names[1]
            auto promise = ResolveSecrets({names[0], names[1]}, kikimr, userToken);
            AssertBadRequest(promise, "<main>: Error: secret `/Root/secret-name-1` not found\n");
        }

        grantResult = adminSession.ExecuteSchemeQuery(
            Sprintf("GRANT 'ydb.granular.select_row' ON `%s` TO `%s`;", names[1].data(), "user@builtin")
        ).GetValueSync();
        UNIT_ASSERT_C(grantResult.GetStatus() == NYdb::EStatus::SUCCESS, grantResult.GetIssues().ToString());

        { // user has grants for all names[0]
            auto promise = ResolveSecrets({names[0], names[1]}, kikimr, userToken);
            AssertSecretValues(values, promise);
        }
    }

}

}
