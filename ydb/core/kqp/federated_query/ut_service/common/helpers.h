#pragma once

#include <ydb/core/kqp/common/events/script_executions.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>

namespace NKikimr::NKqp {
    using TDescriptionPromise = NThreading::TPromise<TEvDescribeSecretsResponse::TDescription>;

    void CreateSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session);
    void AlterSchemaSecret(const TString& secretName, const TString& secretValue, NYdb::NTable::TSession& session);
    void DropSchemaSecret(const TString& secretName, NYdb::NTable::TSession& session);

    TDescriptionPromise
    ResolveSecrets(const TVector<TString>& secretNames, TKikimrRunner& kikimr, const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr);
    TDescriptionPromise
    ResolveSecret(const TString& secretName, TKikimrRunner& kikimr, const TIntrusiveConstPtr<NACLib::TUserToken> userToken = nullptr);

    void AssertBadRequest(TDescriptionPromise promise, const TString& err, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::BAD_REQUEST);

    TIntrusiveConstPtr<NACLib::TUserToken> GetUserToken(const TString& userSid = "", const TVector<TString>& groupSids = {});

    void AssertSecretValues(const TVector<TString>& secretValues, TDescriptionPromise promise);
    void AssertSecretValue(const TString& secretValue, TDescriptionPromise promise);

    class TTestDescribeSchemaSecretsServiceFactory : public IDescribeSchemaSecretsServiceFactory {
    public:
        TTestDescribeSchemaSecretsServiceFactory(
            TDescribeSchemaSecretsService::ISecretUpdateListener* secretUpdateListener,
            TDescribeSchemaSecretsService::ISchemeCacheStatusGetter* schemeCacheStatusGetter
        );

        NActors::IActor* CreateService() override;

    private:
        TDescribeSchemaSecretsService::ISecretUpdateListener* SecretUpdateListener;
        TDescribeSchemaSecretsService::ISchemeCacheStatusGetter* SchemeCacheStatusGetter;
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

} // NKikimr::NKqp
