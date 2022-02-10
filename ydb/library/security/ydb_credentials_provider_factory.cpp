#include "ydb_credentials_provider_factory.h" 
 
 
namespace NKikimr { 
 
std::shared_ptr<NYdb::ICredentialsProviderFactory> CreateYdbCredentialsProviderFactory(const TYdbCredentialsSettings& settings) 
{ 
    return settings.OAuthToken 
        ? NYdb::CreateOAuthCredentialsProviderFactory(settings.OAuthToken) 
        : NYdb::CreateInsecureCredentialsProviderFactory(); 
} 
 
} 
