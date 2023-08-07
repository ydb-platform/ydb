#pragma once

#include <ydb/core/ymq/actor/auth_factory.h>

namespace NKikimr::NSQS {

class TMultiAuthFactory : public IAuthFactory {
public:
    void Initialize(
        NActors::TActorSystemSetup::TLocalServices& services,
        const TAppData& appData,
        const TSqsConfig& config) final;

    void RegisterAuthActor(NActors::TActorSystem& system, TAuthActorData&& data) final;

    TCredentialsFactoryPtr CreateCredentialsProviderFactory(const TSqsConfig& config);

private:
    bool IsYandexCloudMode_ {false};
    TAuthFactory AuthFactory_ {};
    NYdb::TCredentialsProviderPtr CredentialsProvider_;
    bool UseResourceManagerFolderService_ {false};
};
}
