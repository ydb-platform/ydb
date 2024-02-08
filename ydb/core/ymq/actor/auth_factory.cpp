#include "auth_factory.h"

#include <util/stream/file.h>

namespace NKikimr::NSQS {

void TAuthFactory::RegisterAuthActor(TActorSystem& system, TAuthActorData&& data)
{
    IActor* const actor = CreateProxyActionActor(
        *data.SQSRequest,
        std::move(data.HTTPCallback),
        data.EnableQueueLeader);

    system.Register(actor, NActors::TMailboxType::HTSwap, data.ExecutorPoolID);
}

TAuthFactory::TCredentialsFactoryPtr
TAuthFactory::CreateCredentialsProviderFactory(const NKikimrConfig::TSqsConfig& config)
{
    if (!config.HasAuthConfig())
        return NYdb::CreateInsecureCredentialsProviderFactory();

    const auto& authCfg = config.GetAuthConfig();

    Y_ABORT_UNLESS(authCfg.LocalAuthConfig_case() == TSqsConfig::TYdbAuthConfig::kOauthToken);

    const TString token = TFileInput(authCfg.GetOauthToken().GetTokenFile()).ReadAll();

    Y_ABORT_UNLESS(!token.empty());

    return NYdb::CreateOAuthCredentialsProviderFactory(token);
}
}
