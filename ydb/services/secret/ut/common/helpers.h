#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/services/secret/describe_schema_secrets_service.h>
#include <ydb/services/secret/resolver.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSecret {
    using TDescriptionPromise = NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription>;

    void CreateSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session);
    void AlterSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session);
    void DropSchemaSecret(const TString& secretName, NYdb::NTable::TSession& session);

    TDescriptionPromise
    ResolveSecrets(
        const TVector<TString>& secretNames,
        NKqp::TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr,
        TDescribeSecretSettings settings = {}
    );
    TDescriptionPromise
    ResolveSecret(
        const TString& secretName,
        NKqp::TKikimrRunner& kikimr,
        const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr,
        TDescribeSecretSettings settings = {}
    );

    void AssertBadRequest(TDescriptionPromise promise, const TString& err, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::BAD_REQUEST);

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken(const TString& userSid = "", const TVector<TString>& groupSids = {});

    void AssertSecretValues(const TVector<TString>& secretValues, TDescriptionPromise promise);
    void AssertSecretValue(const TString& secretValue, TDescriptionPromise promise);

    class TTestDescribeSchemaSecretsServiceFactory : public IDescribeSchemaSecretsServiceFactory {
    public:
        TTestDescribeSchemaSecretsServiceFactory(
            TDescribeSchemaSecretsService::ISecretUpdateListener* secretUpdateListener,
            TDescribeSchemaSecretsService::ISchemeCacheStatusGetter* schemeCacheStatusGetter,
            TDescribeSchemaSecretsService::ISchemeShardStatusGetter* schemeShardStatusGetter = nullptr
        );

        NActors::IActor* CreateService() override;

    private:
        TDescribeSchemaSecretsService::ISecretUpdateListener* SecretUpdateListener;
        TDescribeSchemaSecretsService::ISchemeCacheStatusGetter* SchemeCacheStatusGetter;
        TDescribeSchemaSecretsService::ISchemeShardStatusGetter* SchemeShardStatusGetter;
    };

    class TTestSchemeCacheStatusGetter : public TDescribeSchemaSecretsService::ISchemeCacheStatusGetter {
    public:
        enum class EFailProbability {
            None = 0,
            OneTenth = 1,
            Always = 2,
        };

        TTestSchemeCacheStatusGetter(EFailProbability failProbability);

        NSchemeCache::TSchemeCacheNavigate::EStatus GetStatus(
            NSchemeCache::TSchemeCacheNavigate::TEntry& entry) const override;

        void SetFailProbability(EFailProbability failProbability);

    private:
        EFailProbability FailProbability;
        mutable std::mt19937 RandomGen;
    };

    class TTestSchemeShardStatusGetter : public TDescribeSchemaSecretsService::ISchemeShardStatusGetter {
    public:
        TTestSchemeShardStatusGetter(
            const ui32 statusOverwriteRemainingCount,
            const NKikimrScheme::EStatus overwrittenStatus);

        NKikimrScheme::EStatus GetStatus(
            const NKikimrScheme::TEvDescribeSchemeResult& record) const override;

    private:
        const NKikimrScheme::EStatus OverwrittenStatus;
        mutable ui32 StatusOverwriteRemainingCount = 0;
    };

} // NKikimr::NSecret
