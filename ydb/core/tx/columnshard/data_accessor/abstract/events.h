#pragma once

#include "collector.h"

#include <ydb/core/tx/columnshard/private_events/events.h>

#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap::NDataAccessorControl {

class TEvAskTabletDataAccessors
    : public NActors::TEventLocal<TEvAskTabletDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskTabletDataAccessors> {
private:
    using TPortions = THashMap<TInternalPathId, TPortionsByConsumer>;
    YDB_ACCESSOR_DEF(TPortions, Portions);
    YDB_READONLY_DEF(std::shared_ptr<IAccessorCallback>, Callback);

public:
    explicit TEvAskTabletDataAccessors(TPortions&& portions, const std::shared_ptr<IAccessorCallback>& callback)
        : Portions(std::move(portions))
        , Callback(callback)
    {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
