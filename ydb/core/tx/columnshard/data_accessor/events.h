#pragma once

#include "abstract/collector.h"

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
    std::vector<TPortionDataAccessor> Accessors;

public:
    std::vector<TPortionDataAccessor> ExtractAccessors() {
        return std::move(Accessors);
    }

    explicit TEvAddPortion(const TPortionDataAccessor& accessor) {
        Accessors.emplace_back(accessor);
    }

    explicit TEvAddPortion(const std::vector<TPortionDataAccessor>& accessors) {
        Accessors = accessors;
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
    bool IsUpdateFlag = false;

public:
    bool IsUpdate() const {
        return IsUpdateFlag;
    }

    std::unique_ptr<IGranuleDataAccessor> ExtractController() {
        return std::move(Controller);
    }

    explicit TEvRegisterController(std::unique_ptr<IGranuleDataAccessor>&& accessor, const bool isUpdate)
        : Controller(std::move(accessor))
        , IsUpdateFlag(isUpdate)
    {
    }
};

class TEvUnregisterController
    : public NActors::TEventLocal<TEvUnregisterController, NColumnShard::TEvPrivate::EEv::EvUnregisterGranuleDataAccessor> {
private:
    YDB_READONLY(NColumnShard::TInternalPathId, PathId, NColumnShard::TInternalPathId{});


public:
    explicit TEvUnregisterController(const NColumnShard::TInternalPathId pathId)
        : PathId(pathId) {
    }
};

class TEvAskTabletDataAccessors: public NActors::TEventLocal<TEvAskTabletDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskTabletDataAccessors> {
private:
    YDB_ACCESSOR_DEF(std::vector<TPortionInfo::TConstPtr>, Portions);
    YDB_READONLY_DEF(std::shared_ptr<NDataAccessorControl::IAccessorCallback>, Callback);
    YDB_READONLY_DEF(TString, Consumer);

public:
    explicit TEvAskTabletDataAccessors(const std::vector<TPortionInfo::TConstPtr>& portions,
        const std::shared_ptr<NDataAccessorControl::IAccessorCallback>& callback, const TString& consumer)
        : Portions(portions)
        , Callback(callback)
        , Consumer(consumer) {
    }
};

class TEvAskServiceDataAccessors
    : public NActors::TEventLocal<TEvAskServiceDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskServiceDataAccessors> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TDataAccessorsRequest>, Request);

public:
    explicit TEvAskServiceDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request)
        : Request(request) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
