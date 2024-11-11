#pragma once

#include "percentile_counter.h"

#include <ydb/library/actors/core/actor.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NKikimr::NPQ {

TPercentileCounter CreateSLIDurationCounter(const NActors::TActorContext& ctx,
                                            const TString& name,
                                            const TVector<ui32>& durations,
                                            const TString& account,
                                            ui32 border);

inline
ui64 ToMilliSeconds(TDuration d) {
    return d.MilliSeconds();
}

}
