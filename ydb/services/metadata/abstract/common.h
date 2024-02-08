#pragma once
#include "fetcher.h"
#include "events.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor_virtual.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/object_factory/object_factory.h>
#include <ydb/core/base/events.h>
#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NProvider {

class TEvRefreshSubscriberData: public NActors::TEventLocal<TEvRefreshSubscriberData, EvRefreshSubscriberData> {
private:
    YDB_READONLY_DEF(NFetcher::ISnapshot::TPtr, Snapshot);
public:
    TEvRefreshSubscriberData(NFetcher::ISnapshot::TPtr snapshot)
        : Snapshot(snapshot) {

    }

    template <class TSnapshot>
    const TSnapshot* GetSnapshotAs() const {
        return dynamic_cast<const TSnapshot*>(Snapshot.get());
    }

    template <class TSnapshot>
    std::shared_ptr<TSnapshot> GetSnapshotPtrAs() const {
        return std::dynamic_pointer_cast<TSnapshot>(Snapshot);
    }

    template <class TSnapshot>
    std::shared_ptr<TSnapshot> GetValidatedSnapshotAs() const {
        auto result = dynamic_pointer_cast<TSnapshot>(Snapshot);
        Y_ABORT_UNLESS(result);
        return result;
    }
};

}
