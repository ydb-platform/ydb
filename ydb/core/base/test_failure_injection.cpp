#include "test_failure_injection.h"
#include "appdata.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {

void TTestFailureInjector::InjectFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType) {
    if (actorSystem) {
        TAppData* appData = actorSystem->AppData<TAppData>();
        if (appData) {
            appData->InjectFailure(static_cast<ui64>(failureType));
        }
    }
}

void TTestFailureInjector::RemoveFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType) {
    if (actorSystem) {
        TAppData* appData = actorSystem->AppData<TAppData>();
        if (appData) {
            appData->RemoveFailure(static_cast<ui64>(failureType));
        }
    }
}

void TTestFailureInjector::ClearAllFailures(NActors::TActorSystem* actorSystem) {
    if (actorSystem) {
        TAppData* appData = actorSystem->AppData<TAppData>();
        if (appData) {
            appData->ClearAllFailures();
        }
    }
}

bool TTestFailureInjector::HasInjectedFailure(NActors::TActorSystem* actorSystem, EInjectedFailureType failureType) {
    if (actorSystem) {
        TAppData* appData = actorSystem->AppData<TAppData>();
        if (appData) {
            return appData->HasInjectedFailure(static_cast<ui64>(failureType));
        }
    }
    return false;
}

} // namespace NKikimr
