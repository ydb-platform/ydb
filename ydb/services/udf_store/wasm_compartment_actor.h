#pragma once

#include "events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NUdfStore {

class TWasmCompartmentActor : public NActors::TActorBootstrapped<TWasmCompartmentActor> {
private:
    using TBase = NActors::TActorBootstrapped<TWasmCompartmentActor>;

    TString Md5_;
    NWasm::TWasmCompartmentStatePtr State_;

    void HandleLoad(TEvWasmCompartmentLoad::TPtr& ev);
    void HandleUnload(TEvWasmCompartmentUnload::TPtr& ev);

public:
    explicit TWasmCompartmentActor(const TString& md5)
        : Md5_(md5)
    {}

    void Bootstrap();

    NWasm::TWasmCompartmentStatePtr GetState() const {
        return State_;
    }

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvWasmCompartmentLoad, HandleLoad);
            hFunc(TEvWasmCompartmentUnload, HandleUnload);
            default:
                break;
        }
    }
};

} // namespace NKikimr::NUdfStore
