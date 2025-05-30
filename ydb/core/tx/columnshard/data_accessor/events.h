#pragma once

#include "abstract/collector.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
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
    YDB_READONLY_DEF(TTabletId, TabletId);

public:
    std::vector<TPortionDataAccessor> ExtractAccessors() {
        return std::move(Accessors);
    }

    TEvAddPortion(const TTabletId tabletId, const TPortionDataAccessor& accessor)
        : TabletId(tabletId) {
        Accessors.emplace_back(accessor);
    }

    TEvAddPortion(const TTabletId tabletId, const std::vector<TPortionDataAccessor>& accessors)
        : TabletId(tabletId) {
        Accessors = accessors;
    }
};

class TEvRemovePortion: public NActors::TEventLocal<TEvRemovePortion, NColumnShard::TEvPrivate::EEv::EvRemovePortionDataAccessor> {
private:
    YDB_READONLY_DEF(TPortionInfo::TConstPtr, Portion);
    YDB_READONLY_DEF(TTabletId, TabletId);

public:
    TEvRemovePortion(const TTabletId tabletId, const TPortionInfo::TConstPtr& portion)
        : Portion(portion)
        , TabletId(tabletId) {
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

    TEvRegisterController(std::unique_ptr<IGranuleDataAccessor>&& accessor, const bool isUpdate)
        : Controller(std::move(accessor))
        , IsUpdateFlag(isUpdate)
    {
    }
};

class TEvUnregisterController
    : public NActors::TEventLocal<TEvUnregisterController, NColumnShard::TEvPrivate::EEv::EvUnregisterGranuleDataAccessor> {
private:
    YDB_READONLY_DEF(TInternalPathId, PathId);
    YDB_READONLY_DEF(TTabletId, TabletId);

public:
    TEvUnregisterController(const TTabletId tabletId, const TInternalPathId pathId)
        : PathId(pathId)
        , TabletId(tabletId){
    }
};

class TEvClearCache
    : public NActors::TEventLocal<TEvClearCache, NColumnShard::TEvPrivate::EEv::EvClearCacheDataAccessor> {
private:
    YDB_READONLY_DEF(TTabletId, TabletId);

public:
    explicit TEvClearCache(const TTabletId tabletId)
        : TabletId(tabletId) {
    }
};


class TEvAskTabletDataAccessors
    : public NActors::TEventLocal<TEvAskTabletDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskTabletDataAccessors> {
private:
    using TPortions = THashMap<TInternalPathId, TPortionsByConsumer>;
    YDB_ACCESSOR_DEF(TPortions, Portions);
    YDB_READONLY_DEF(std::shared_ptr<NDataAccessorControl::IAccessorCallback>, Callback);
    YDB_READONLY_DEF(TTabletId, TabletId);

public:
    explicit TEvAskTabletDataAccessors(TPortions&& portions, const std::shared_ptr<NDataAccessorControl::IAccessorCallback>& callback, const TTabletId tabletId)
        : Portions(std::move(portions))
        , Callback(callback)
        , TabletId(tabletId) {
    }
};

class TEvAskServiceDataAccessors
    : public NActors::TEventLocal<TEvAskServiceDataAccessors, NColumnShard::TEvPrivate::EEv::EvAskServiceDataAccessors> {
private:
    YDB_READONLY_DEF(std::shared_ptr<TDataAccessorsRequest>, Request);
    YDB_READONLY_DEF(TTabletId, TabletId);

public:
    explicit TEvAskServiceDataAccessors(const TTabletId tabletId, const std::shared_ptr<TDataAccessorsRequest>& request)
        : Request(request)
        , TabletId(tabletId) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
