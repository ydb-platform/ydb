#pragma once

#include <ydb/library/actors/testlib/test_runtime.h>

#include <functional>

namespace NKikimr::NHullComp::NTesting {

    inline void RunInActorContext(const std::function<void(NActors::TTestActorRuntimeBase&)>& callback) {
        class TRunner : public NActors::TActor<TRunner> {
            std::function<void()> Callback;

            STFUNC(Execute) {
                Y_UNUSED(ev);
                Callback();
                PassAway();
            }

        public:
            explicit TRunner(std::function<void()> callback)
                : TActor(&TRunner::Execute)
                , Callback(std::move(callback))
            {}
        };

        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto actor = runtime.Register(new TRunner([&] { callback(runtime); }));
        runtime.Send(new NActors::IEventHandle(actor, {}, new NActors::TEvents::TEvWakeup));
    }

} // NKikimr::NHullComp::NTesting
