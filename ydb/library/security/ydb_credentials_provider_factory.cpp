#include "ydb_credentials_provider_factory.h"

#include <ydb/public/sdk/cpp/client/iam/common/iam.h>

namespace NKikimr {

std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateYdbCredentialsProviderFactory(const TYdbCredentialsSettings& settings)
{
    if (settings.UseLocalMetadata) {
        return NYdb::CreateIamCredentialsProviderFactory();
    } else if (settings.SaKeyFile) {
        NYdb::TIamJwtFilename params = {.JwtFilename = settings.SaKeyFile};

        if (settings.IamEndpoint)
            params.Endpoint = settings.IamEndpoint;

        return NYdb::CreateIamJwtFileCredentialsProviderFactory(std::move(params));
    }

    return settings.OAuthToken
        ? NYdb::CreateOAuthCredentialsProviderFactory(settings.OAuthToken)
        : NYdb::CreateInsecureCredentialsProviderFactory();
}

}
