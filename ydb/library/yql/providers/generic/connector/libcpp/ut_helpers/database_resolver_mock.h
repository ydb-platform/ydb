#pragma once
#include <ydb/library/yql/providers/generic/connector/libcpp/ut_helpers/defaults.h>

#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NConnector::NTest {
    using namespace testing;

    MATCHER_P(DatabaseAuthMapMatcher, expected, "database auth map matcher") {
        return arg == expected;
    }

    class TDatabaseAsyncResolverMock: public NYql::IDatabaseAsyncResolver {
    public:
        MOCK_METHOD(NThreading::TFuture<NYql::TDatabaseResolverResponse>, ResolveIds, (const TDatabaseAuthMap& ids), (const override));

        void AddClickHouseCluster(
            const TString& clusterId = DEFAULT_CH_CLUSTER_ID,
            const TString& hostFqdn = DEFAULT_CH_HOST,
            const ui32 port = DEFAULT_CH_PORT,
            const TString& serviceAccountId = DEFAULT_CH_SERVICE_ACCOUNT_ID,
            const TString& serviceAccountIdSignature = DEFAULT_CH_SERVICE_ACCOUNT_ID_SIGNATURE) {
            NYql::IDatabaseAsyncResolver::TDatabaseAuthMap dbResolverReq;
            dbResolverReq[std::make_pair(clusterId, NYql::EDatabaseType::ClickHouse)] =
                NYql::TDatabaseAuth{
                    NYql::TStructuredTokenBuilder().SetServiceAccountIdAuth(serviceAccountId, serviceAccountIdSignature).ToJson(),
                    true,
                    true};

            NYql::TDatabaseResolverResponse::TDatabaseDescriptionMap databaseDescriptions;
            databaseDescriptions[std::make_pair(clusterId, NYql::EDatabaseType::ClickHouse)] =
                NYql::TDatabaseResolverResponse::TDatabaseDescription{"", hostFqdn, port, clusterId};
            auto dbResolverPromise = NThreading::NewPromise<NYql::TDatabaseResolverResponse>();
            dbResolverPromise.SetValue(NYql::TDatabaseResolverResponse(std::move(databaseDescriptions), true));

            auto result = dbResolverPromise.GetFuture();
            EXPECT_CALL(*this, ResolveIds(DatabaseAuthMapMatcher(dbResolverReq)))
                .WillOnce(Return(result));
        }
    };
} // namespace NYql::NConnector::NTest
