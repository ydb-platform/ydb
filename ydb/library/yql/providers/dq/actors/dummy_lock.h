#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NYql {

NActors::IActor* CreateDummyLock(
    const TString& lockName,
    const TString& lockAttributesYson);

} // namespace NYql

