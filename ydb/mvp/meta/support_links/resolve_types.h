#pragma once

#include "events.h"

#include <ydb/mvp/meta/mvp.h>

#include <util/generic/maybe.h>

namespace NMVP {

struct TResolveOutput {
    TString Name;
    bool Ready = false;
    TVector<NSupportLinks::TResolvedLink> Links;
    TVector<NSupportLinks::TSupportError> Errors;
    TMaybe<NActors::TActorId> Actor;
};

} // namespace NMVP
