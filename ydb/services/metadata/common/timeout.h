#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/services/metadata/abstract/events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr::NMetadata::NInternal {

class TEvTimeout: public NActors::TEventLocal<TEvTimeout, NProvider::EEvents::EvTimeout> {

};

template <class TDerived>
class TTimeoutActor: public NActors::TActorBootstrapped<TDerived> {
private:
    using TBase = NActors::TActorBootstrapped<TDerived>;
    const TInstant Deadline = TInstant::Max();
    void HandleTimeout() {
        OnTimeout();
    }
protected:
    virtual void OnBootstrap() = 0;
    virtual void OnTimeout() = 0;
public:
    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            sFunc(TEvTimeout, HandleTimeout);
            default:
                break;
        }
    }

    TTimeoutActor(const TInstant deadline)
        : Deadline(deadline) {

    }

    TTimeoutActor(const TDuration livetime)
        : Deadline(TAppData::TimeProvider->Now() + livetime) {

    }

    void Bootstrap() {
        OnBootstrap();
        if (Deadline != TInstant::Max()) {
            TBase::Schedule(Deadline, new TEvTimeout);
        }
    }
};

}
