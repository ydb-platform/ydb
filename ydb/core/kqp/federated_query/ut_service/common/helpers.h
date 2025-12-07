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
            TDescribeSchemaSecretsService::ISchemeCacheResponseModifier* schemeCacheResponseModifier
        );

        NActors::IActor* CreateService() override;

    private:
        TDescribeSchemaSecretsService::ISecretUpdateListener* SecretUpdateListener;
        TDescribeSchemaSecretsService::ISchemeCacheResponseModifier* SchemeCacheResponseModifier;
    };

    class TTestSchemeCacheResponseModifier : public TDescribeSchemaSecretsService::ISchemeCacheResponseModifier {
    public:
        enum class EFailProbablity {
            FailProbabilityNever = 0,
            FailProbabilityQuoter = 1,
            FailProbabilityHalf = 2,
            FailProbabilityAlways = 3,
        };

        TTestSchemeCacheResponseModifier(EFailProbablity failProbablitity);

        void Modify(NSchemeCache::TSchemeCacheNavigate& result) override;

        void SetFailProbablitity(EFailProbablity failProbablitity);

    private:
        EFailProbablity FailProbablitity;
    };

} // NKikimr::NKqp
