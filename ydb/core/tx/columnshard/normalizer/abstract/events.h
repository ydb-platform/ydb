#pragma once

#include "abstract.h"

#include <ydb/core/tx/columnshard/private_events/events.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NColumnShard {

class TEvPrivate::TEvNormalizerResult: public NActors::TEventLocal<TEvPrivate::TEvNormalizerResult, TEvPrivate::EEv::EvNormalizerResult> {
    NOlap::INormalizerChanges::TPtr Changes;

public:
    TEvNormalizerResult(NOlap::INormalizerChanges::TPtr changes)
        : Changes(changes)
    {
    }

    NOlap::INormalizerChanges::TPtr GetChanges() const {
        Y_ABORT_UNLESS(!!Changes);
        return Changes;
    }
};

}   // namespace NKikimr::NColumnShard
