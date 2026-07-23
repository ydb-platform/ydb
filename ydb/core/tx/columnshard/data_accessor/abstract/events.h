#pragma once

#include "collector.h"

#include <ydb/core/tx/columnshard/private_events/events.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NColumnShard {

class TEvPrivate::TEvAskTabletDataAccessors
    : public NActors::TEventLocal<TEvPrivate::TEvAskTabletDataAccessors, TEvPrivate::EEv::EvAskTabletDataAccessors> {
private:
    using TPortions = THashMap<TInternalPathId, NOlap::NDataAccessorControl::TPortionsByConsumer>;
    YDB_ACCESSOR_DEF(TPortions, Portions);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>, Callback);

public:
    explicit TEvAskTabletDataAccessors(TPortions&& portions, const std::shared_ptr<NOlap::NDataAccessorControl::IAccessorCallback>& callback)
        : Portions(std::move(portions))
        , Callback(callback)
    {
    }
};

}   // namespace NKikimr::NColumnShard
