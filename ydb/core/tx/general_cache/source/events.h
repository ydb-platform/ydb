#pragma once
#include <ydb/core/base/events.h>

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NGeneralCache::NSource {

template <class TPolicy>
struct TEvents {
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using TSourceId = typename TPolicy::TSourceId;
    using EConsumer = typename TPolicy::EConsumer;

    enum EEv {
        EvObjectsInfo = EventSpaceBegin(TKikimrEvents::ES_GENERAL_CACHE_SOURCE),
        EvAdditionalObjectsInfo,
        EvEnd
    };

    class TEvObjectsInfo: public NActors::TEventLocal<TEvObjectsInfo, EvObjectsInfo> {
    private:
        bool RemovedExtracted = false;
        THashSet<TAddress> Removed;

        bool ObjectsExtracted = false;
        THashMap<TAddress, TObject> Objects;

        bool ErrorsExtracted = false;
        THashMap<TAddress, TString> Errors;

        const TSourceId SourceId;

    public:
        TSourceId GetSourceId() const {
            return SourceId;
        }

        THashMap<TAddress, TObject> ExtractObjects() {
            AFL_VERIFY(!ObjectsExtracted);
            ObjectsExtracted = true;
            return std::move(Objects);
        }

        THashSet<TAddress> ExtractRemoved() {
            AFL_VERIFY(!RemovedExtracted);
            RemovedExtracted = true;
            return std::move(Removed);
        }

        THashMap<TAddress, TString> ExtractErrors() {
            AFL_VERIFY(!ErrorsExtracted);
            ErrorsExtracted = true;
            return std::move(Errors);
        }

        TEvObjectsInfo(
            const TSourceId sourceId, THashMap<TAddress, TObject>&& objects, THashSet<TAddress>&& removed, THashMap<TAddress, TString>&& errors)
            : Removed(std::move(removed))
            , Objects(std::move(objects))
            , Errors(std::move(errors))
            , SourceId(sourceId) {
        }
    };

    class TEvAdditionalObjectsInfo: public NActors::TEventLocal<TEvAdditionalObjectsInfo, EvAdditionalObjectsInfo> {
    private:
        bool AddObjectsExtracted = false;
        THashMap<TAddress, TObject> AddObjects;

        bool RemoveObjectsExtracted = false;
        THashSet<TAddress> RemoveObjects;

        const TSourceId SourceId;

    public:
        TSourceId GetSourceId() const {
            return SourceId;
        }

        THashMap<TAddress, TObject> ExtractAddObjects() {
            AFL_VERIFY(!AddObjectsExtracted);
            AddObjectsExtracted = true;
            return std::move(AddObjects);
        }

        THashSet<TAddress> ExtractRemoveObjects() {
            AFL_VERIFY(!RemoveObjectsExtracted);
            RemoveObjectsExtracted = true;
            return std::move(RemoveObjects);
        }

        TEvAdditionalObjectsInfo(const TSourceId sourceId, THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& remove)
            : AddObjects(std::move(add))
            , RemoveObjects(std::move(remove))
            , SourceId(sourceId) {
        }
    };
};

}   // namespace NKikimr::NGeneralCache::NSource
