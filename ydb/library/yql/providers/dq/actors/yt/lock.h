#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYql {

NActors::IActor* CreateYtLock(
    NActors::TActorId ytWrapper,
    const TString& prefix,
    const TString& lockName,
    const TString& lockAttributesYson,
    bool temporary);

} // namespace NYql
