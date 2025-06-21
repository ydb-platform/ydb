#pragma once

namespace NKikimr::NGeneralCache::NSource {

template <class TPolicy>
struct TEvents {
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using EConsumer = typename TPolicy::EConsumer;

    enum EEv {
        EvObjectsInfo = EventSpaceBegin(NActors::TEvents::ES_GENERAL_CACHE_SOURCE),
        EvEnd
    };

    class TEvObjectsInfo: public NActors::TEventLocal<TEvObjectsInfo, EvObjectsInfo> {
    private:
        bool RemovedExtracted = false;
        THashSet<TAddress> Removed;

        bool ObjectsExtracted = false;
        THashMap<TAddress, TObject> Objects;

    public:
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

        TEvObjectsInfo(THashMap<TAddress, TObject>&& objects, THashSet<TAddress>&& removed)
            : Removed(std::move(removed))
            , Objects(std::move(objects)) {
        }
    };
};

}   // namespace NKikimr::NGeneralCache::NSource
