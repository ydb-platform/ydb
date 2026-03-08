#pragma once

#include "events.h"

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NMVP {

struct TResolveOutput {
    TString Name;
    bool Ready = false;
    TVector<NSupportLinks::TResolvedLink> Links;
    TVector<NSupportLinks::TSupportError> Errors;
    TMaybe<NActors::TActorId> Actor;
};

} // namespace NMVP
