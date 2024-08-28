#pragma once

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>
#include <ydb/library/yql/providers/generic/proto/source.pb.h>

namespace NYql::NDq {
    // When accessing external data sources using authentication via tokens,
    // there are two options:
    // 1. Use static IAM-token provided by user (especially useful during debugging);
    // 2. Use service account credentials in order to get (and refresh) IAM-token by demand.
    class TGenericTokenProvider {
    public:
        using TPtr = std::unique_ptr<TGenericTokenProvider>;
        TGenericTokenProvider() = default; // No auth required
        TGenericTokenProvider(const TString& staticIamToken);
        TGenericTokenProvider(
            const TString& serviceAccountId,
            const TString& ServiceAccountIdSignature,
            const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);

        void MaybeFillToken(NConnector::NApi::TDataSourceInstance& dsi) const;

    private:
        TString StaticIAMToken_;
        NYdb::TCredentialsProviderPtr CredentialsProvider_;
    };

    TGenericTokenProvider::TPtr
    CreateGenericTokenProvider(
        const TString& staticIamToken,
        const TString& serviceAccountId, const TString& ServiceAccountIdSignature,
        const ISecuredServiceAccountCredentialsFactory::TPtr& credentialsFactory);
} // namespace NYql::NDq
