#include "service_initializer.h"

namespace NKikimr {

void TServiceInitializersList::AddServiceInitializer(TIntrusivePtr<IServiceInitializer> serviceInitializer) {
    ServiceInitializersList.push_back(serviceInitializer);
}

void TServiceInitializersList::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    for (auto& serviceInitializer : ServiceInitializersList) {
        serviceInitializer->InitializeServices(setup, appData);
    }
}

void TAppDataInitializersList::AddAppDataInitializer(TIntrusivePtr<IAppDataInitializer> appDataInitializer) {
    AppDataInitializersList.push_back(appDataInitializer);
}

void TAppDataInitializersList::Initialize(NKikimr::TAppData* appData) {
    for (auto& appDataInitializer : AppDataInitializersList) {
        appDataInitializer->Initialize(appData);
    }
}

} // namespace NKikimr
