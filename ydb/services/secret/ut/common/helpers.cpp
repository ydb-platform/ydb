#include "helpers.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/services/secret/describe_schema_secrets_service.h>

namespace NKikimr::NSecret {
    using TDescriptionPromise = NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription>;

    void CreateSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
        auto query = "CREATE SECRET `" + secretName + "` WITH (value = \"" + secretValue + "\");";
        auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(queryResult.GetStatus() == NYdb::EStatus::SUCCESS, queryResult.GetIssues().ToString());
    }

    void AlterSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session) {
        auto query = "ALTER SECRET `" + secretName + "` WITH (value = \"" + secretValue + "\");";
        auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(queryResult.GetStatus() == NYdb::EStatus::SUCCESS, queryResult.GetIssues().ToString());
    }

    void DropSchemaSecret(const TString& secretName, NYdb::NTable::TSession& session) {
        auto query = "DROP SECRET `" + secretName + "`;";
        auto queryResult = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(queryResult.GetStatus() == NYdb::EStatus::SUCCESS, queryResult.GetIssues().ToString());
    }

    TDescriptionPromise
    ResolveSecrets(
        const TVector<TString>& secretNames,
        NKqp::TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        TDescribeSecretSettings settings)
    {
        auto promise = NThreading::NewPromise<NKqp::TEvDescribeSecretsResponse::TDescription>();
        const auto evResolveSecret = new TDescribeSchemaSecretsService::TEvResolveSecret(
            userToken, "/Root", secretNames, promise, std::move(settings));
        auto actorSystem = kikimr.GetTestServer().GetRuntime()->GetActorSystem(0);
        actorSystem->Send(MakeKqpDescribeSchemaSecretServiceId(actorSystem->NodeId), evResolveSecret);
        return promise;
    }

    TDescriptionPromise
    ResolveSecret(
        const TString& secretName,
        NKqp::TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        TDescribeSecretSettings settings)
    {
        return ResolveSecrets(TVector<TString>{secretName}, kikimr, userToken, std::move(settings));
    }

    void AssertBadRequest(TDescriptionPromise promise, const TString& err, Ydb::StatusIds::StatusCode status) {
        const auto& result = promise.GetFuture().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(status, result.Status);
        UNIT_ASSERT_VALUES_EQUAL(err, result.Issues.ToString());
    }

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken(const TString& userSid, const TVector<TString>& groupSids) {
        if (userSid.empty() && groupSids.empty()) {
            return nullptr;
        }
        return new NACLib::TUserToken(userSid, groupSids);
    }

    void AssertSecretValues(const TVector<TString>& secretValues, TDescriptionPromise promise) {
        const auto& result = promise.GetFuture().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(secretValues.size(), result.SecretValues.size(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(secretValues, result.SecretValues);
    }

    void AssertSecretValue(const TString& secretValue, TDescriptionPromise promise) {
        AssertSecretValues(TVector<TString>{secretValue}, promise);
    }

    TTestDescribeSchemaSecretsServiceFactory::TTestDescribeSchemaSecretsServiceFactory(
        TDescribeSchemaSecretsService::ISecretUpdateListener* secretUpdateListener,
        TDescribeSchemaSecretsService::ISchemeCacheStatusGetter* schemeCacheStatusGetter,
        TDescribeSchemaSecretsService::ISchemeShardStatusGetter* schemeShardStatusGetter
    )
        : SecretUpdateListener(secretUpdateListener)
        , SchemeCacheStatusGetter(schemeCacheStatusGetter)
        , SchemeShardStatusGetter(schemeShardStatusGetter)
    {
    }

    NActors::IActor* TTestDescribeSchemaSecretsServiceFactory::CreateService() {
        auto* service = new TDescribeSchemaSecretsService();
        service->SetSecretUpdateListener(SecretUpdateListener);
        service->SetSchemeCacheStatusGetter(SchemeCacheStatusGetter);
        service->SetSchemeShardStatusGetter(SchemeShardStatusGetter);
        return service;
    }

    TTestSchemeCacheStatusGetter::TTestSchemeCacheStatusGetter(EFailProbability failProbability)
        : FailProbability(failProbability)
    {
    }

    NSchemeCache::TSchemeCacheNavigate::EStatus TTestSchemeCacheStatusGetter::GetStatus(
        NSchemeCache::TSchemeCacheNavigate::TEntry& entry) const
    {
        switch (FailProbability) {
            case EFailProbability::None:
                return entry.Status;
            case EFailProbability::OneTenth: {
                static const int MOD = 10;
                if ((std::uniform_int_distribution<int>(0, MOD - 1))(RandomGen) == 0) {
                    return NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError;
                }
                return entry.Status;
            }
            case EFailProbability::Always:
                return NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError;
            default:
                Y_ENSURE(false, "Unexpected value");
        }
    }

    void TTestSchemeCacheStatusGetter::SetFailProbability(EFailProbability failProbability) {
        FailProbability = failProbability;
    }

    TTestSchemeShardStatusGetter::TTestSchemeShardStatusGetter(
        const ui32 statusOverwriteRemainingCount,
        const NKikimrScheme::EStatus overwrittenStatus
    )
        : OverwrittenStatus(overwrittenStatus)
        , StatusOverwriteRemainingCount(statusOverwriteRemainingCount)
    {
    }

    NKikimrScheme::EStatus TTestSchemeShardStatusGetter::GetStatus(
        const NKikimrScheme::TEvDescribeSchemeResult& record) const
    {
        if (StatusOverwriteRemainingCount > 0) {
            --StatusOverwriteRemainingCount;
            return OverwrittenStatus;
        }
        return record.GetStatus();
    }

} // NKikimr::NSecret
