#pragma once

#include "events.h"
#include "private_events.h"
#include "source_cache.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {
class TSpecialReadContext;
}

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TDuplicateManager: public NActors::TActor<TDuplicateManager> {
private:
    TSourceCache Fetcher;

private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NPrivate::TEvFilterConstructionResult, Handle);
            hFunc(NPrivate::TEvDuplicateFilterDataFetched, Handle);
            hFunc(NPrivate::TEvDuplicateSourceCacheResult, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NPrivate::TEvFilterConstructionResult::TPtr&);
    void Handle(const NPrivate::TEvDuplicateFilterDataFetched::TPtr&);
    void Handle(const NPrivate::TEvDuplicateSourceCacheResult::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

public:
    TDuplicateManager(const TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
