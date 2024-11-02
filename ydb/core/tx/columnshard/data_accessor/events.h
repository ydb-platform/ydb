#pragma once

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap {
class IGranuleDataAccessor;
class TDataAccessorsRequest;
}

namespace NKikimr::NOlap::NDataAccessorControl {

class TEvRegisterController: public NActors::TEventLocal<TEvRegisterController, NColumnShard::TEvPrivate::EEv::EvRegisterGranuleDataAccessor> {
private:
    YDB_READONLY_DEF(std::shared_ptr<IGranuleDataAccessor>, Accessor);

public:
    explicit TEvRegisterController(const std::shared_ptr<IGranuleDataAccessor>& accessor)
        : Accessor(accessor) {
    }
};

class TEvUnregisterController
    : public NActors::TEventLocal<TEvUnregisterController, NColumnShard::TEvPrivate::EEv::EvUnregisterGranuleDataAccessor> {
private:
    YDB_READONLY(ui64, PathId, 0);

public:
    explicit TEvUnregisterController(const ui64 pathId)
        : PathId(pathId) {
    }
};

class TEvAskDataAccessors: public NActors::TEventLocal<TEvAskDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskDataAccessors> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TDataAccessorsRequest>, Request);

public:
    explicit TEvAskDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request)
        : Request(request) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
