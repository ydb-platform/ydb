#pragma once

#include "abstract.h"

#include <ydb/core/tx/columnshard/private_events/events.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap {

class TEvNormalizerResult: public NActors::TEventLocal<TEvNormalizerResult, NColumnShard::TEvPrivate::EEv::EvNormalizerResult> {
    INormalizerChanges::TPtr Changes;

public:
    TEvNormalizerResult(INormalizerChanges::TPtr changes)
        : Changes(changes)
    {
    }

    INormalizerChanges::TPtr GetChanges() const {
        Y_ABORT_UNLESS(!!Changes);
        return Changes;
    }
};

}   // namespace NKikimr::NOlap
