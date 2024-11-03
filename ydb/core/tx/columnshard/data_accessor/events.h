#pragma once

#include "controller.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NOlap {
class IGranuleDataAccessor;
class TDataAccessorsRequest;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NDataAccessorControl {

class TEvAddPortion: public NActors::TEventLocal<TEvAddPortion, NColumnShard::TEvPrivate::EEv::EvAddPortionDataAccessor> {
private:
    TPortionDataAccessor Accessor;

public:
    TPortionDataAccessor ExtractAccessor() {
        return std::move(Accessor);
    }

    explicit TEvAddPortion(const TPortionDataAccessor& accessor)
        : Accessor(accessor) {
    }
};

class TEvRemovePortion: public NActors::TEventLocal<TEvRemovePortion, NColumnShard::TEvPrivate::EEv::EvRemovePortionDataAccessor> {
private:
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);

public:
    explicit TEvRemovePortion(const TPortionInfo::TConstPtr& portion)
        : Portion(portion) {
    }
};

class TEvRegisterController: public NActors::TEventLocal<TEvRegisterController, NColumnShard::TEvPrivate::EEv::EvRegisterGranuleDataAccessor> {
private:
    std::unique_ptr<IGranuleDataAccessor> Controller;

public:
    std::unique_ptr<IGranuleDataAccessor> ExtractController() {
        return std::move(Controller);
    }

    explicit TEvRegisterController(std::unique_ptr<IGranuleDataAccessor>&& accessor)
        : Controller(std::move(accessor)) {
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
