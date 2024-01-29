#pragma once
#include <util/system/types.h>
#include <util/generic/maybe.h>

namespace NInfra {
// InfraBuzzer subscription, for more info see https://github.yandex-team.ru/data-ui/infra-buzzer
struct TSubscription {
    ui32 ServiceId;
    // Do not set if you want to subscribe to all environments
    TMaybe<ui32> EnvironmentId;

    TSubscription(ui32 serviceId)
        : ServiceId(serviceId)
        , EnvironmentId()
    {}

    TSubscription(ui32 serviceId, ui32 environmentId)
        : ServiceId(serviceId)
        , EnvironmentId(environmentId)
    {}
};
}
