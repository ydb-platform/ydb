#pragma once

#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NOlap::NReader::NSimple {
class TSpecialReadContext;
}

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

class TDuplicateManager: public NActors::TActor<TDuplicateManager> {
private:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestFilter, Handle);
            hFunc(NActors::TEvents::TEvPoison, Handle);
            default:
                AFL_VERIFY(false)("unexpected_event", ev->GetTypeName());
        }
    }

    void Handle(const TEvRequestFilter::TPtr&);
    void Handle(const NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

public:
    TDuplicateManager(const TSpecialReadContext& context);
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
