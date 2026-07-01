#include "structured_token_credentials.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>
#include <yql/essentials/providers/common/structured_token/yql_structured_token.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

namespace NFq {

namespace {

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateKikimrIamAuthCredentialsProviderFactory(
    const NYql::TStructuredTokenParser& parser)
{
    NKikimr::TAppData* appData = NKikimr::AppData();
    Y_ENSURE(appData, "AppData is not available");
    if (!appData->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
        throw yexception() << "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it";
    }
    const auto& serviceControl = appData->ReplicationConfig.GetIamServiceControl();

    TString serviceAccountId;
    TString resourceId;
    parser.GetIamAuth(serviceAccountId, resourceId);

    NYdb::TIamServiceParams iamParams;
    iamParams.SystemServiceAccountCredentials = NYdb::CreateIamCredentialsProviderFactory();
    iamParams.Endpoint = serviceControl.GetEndpoint();
    iamParams.ServiceId = serviceControl.GetServiceId();
    iamParams.MicroserviceId = serviceControl.GetMicroserviceId();
    iamParams.ResourceType = serviceControl.GetResourceType();
    iamParams.ResourceId = resourceId;
    iamParams.TargetServiceAccountId = serviceAccountId;

    return NYdb::CreateIamServiceCredentialsProviderFactory(iamParams);
}

class TKikimrStructuredTokenCredentialsFactoryOverFactory : public NYql::IStructuredTokenCredentialsFactory {
public:
    explicit TKikimrStructuredTokenCredentialsFactoryOverFactory(
        NYql::IStructuredTokenCredentialsFactory::TPtr factory)
        : InnerFactory_(std::move(factory))
    {}

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(
        const TString& structuredTokenJson, bool addBearerToToken) override
    {
        if (NYql::IsStructuredTokenJson(structuredTokenJson)) {
            NYql::TStructuredTokenParser parser = NYql::CreateStructuredTokenParser(structuredTokenJson);
            if (parser.HasIamAuth()) {
                return CreateKikimrIamAuthCredentialsProviderFactory(parser);
            }
        }
        return InnerFactory_->Create(structuredTokenJson, addBearerToToken);
    }

private:
    const NYql::IStructuredTokenCredentialsFactory::TPtr InnerFactory_;
};

} // namespace

NYql::IStructuredTokenCredentialsFactory::TPtr CreateKikimrStructuredTokenCredentialsFactoryOverFactory(
    NYql::IStructuredTokenCredentialsFactory::TPtr factory)
{
    return std::make_shared<TKikimrStructuredTokenCredentialsFactoryOverFactory>(std::move(factory));
}

NYql::IStructuredTokenCredentialsFactory::TPtr CreateKikimrStructuredTokenCredentialsFactory(
    NYql::ISecuredServiceAccountCredentialsFactory::TPtr saFactory)
{
    return NYql::CreateStructuredTokenCredentialsFactory(
        std::move(saFactory),
        [](const NYql::TStructuredTokenParser& parser) {
            return CreateKikimrIamAuthCredentialsProviderFactory(parser);
        });
}

} // namespace NFq
