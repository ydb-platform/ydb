#pragma once
#include "abstract.h"

#include <ydb/core/base/events.h>
#include <ydb/core/tx/general_cache/source/abstract.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NGeneralCache::NPublic {

template <class TPolicy>
struct TEvents {
    using TAddress = typename TPolicy::TAddress;
    using EConsumer = typename TPolicy::EConsumer;
    using ICallback = ICallback<TPolicy>;

    enum EEv {
        EvAskData = EventSpaceBegin(TKikimrEvents::ES_GENERAL_CACHE_PUBLIC),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_GENERAL_CACHE_PUBLIC), "expected EvEnd < EventSpaceEnd");

    class TEvAskData: public NActors::TEventLocal<TEvAskData, EvAskData> {
    private:
        YDB_READONLY(EConsumer, Consumer, EConsumer::Undefined);

        bool AddressesExtracted = false;
        THashSet<TAddress> Addresses;

        bool CallbackExtracted = false;
        std::shared_ptr<ICallback> Callback;

    public:
        TEvAskData(const EConsumer consumer, THashSet<TAddress>&& addresses, std::shared_ptr<ICallback>&& callback)
            : Consumer(consumer)
            , Addresses(std::move(addresses))
            , Callback(std::move(callback)) {
        }

        THashSet<TAddress> ExtractAddresses() {
            AFL_VERIFY(!AddressesExtracted);
            AddressesExtracted = true;
            return std::move(Addresses);
        }
        std::shared_ptr<ICallback> ExtractCallback() {
            AFL_VERIFY(!CallbackExtracted);
            CallbackExtracted = true;
            return std::move(Callback);
        }
    };
};

}   // namespace NKikimr::NGeneralCache::NPublic
