#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>

namespace NYql::NDq {
    // When accessing external data sources using authentication via tokens,
    // there are two options:
    // 1. Use static IAM-token provided by user (especially usefull during debugging);
    // 2. Use service account credentials in order to get (and renew) token by demand.
    class TGenericTokenProvider {
    public:
        using TPtr = std::unique_ptr<TGenericTokenProvider>;

        TGenericTokenProvider() = delete;

        TGenericTokenProvider(const NYql::NGeneric::TSource& source,
                              const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);

        void MaybeFillToken(NConnector::NApi::TDataSourceInstance* dsi) const;

    private:
        NYql::NGeneric::TSource Source_;
        std::string StaticIAMToken_;
        NYdb::TCredentialsProviderPtr CredentialsProvider_;
    };

    TGenericTokenProvider::TPtr
    CreateGenericTokenProvider(const NYql::NGeneric::TSource& source,
                               const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);
} //namespace NYql::NDq
