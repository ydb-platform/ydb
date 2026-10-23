#pragma once

#include "actor.h"
#include "events.h"
#include <util/generic/noncopyable.h>

namespace NActors {
    template<typename T> struct dependent_false : std::false_type {};

    namespace NDetail {
        TAutoPtr<IEventHandle> MakeBootstrapEventHandle(const TActorId& self, const TActorId& parentId);
        [[noreturn]] void AbortUnexpectedBootstrapMessage(const IEventHandle& ev);
    }

    template<typename TDerived>
    class TActorBootstrapped: public TActor<TDerived> {
    protected:
        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
            return NDetail::MakeBootstrapEventHandle(self, parentId);
        }

        STFUNC(StateBootstrap) {
            if (Y_UNLIKELY(ev->GetTypeRewrite() != TEvents::TSystem::Bootstrap)) {
                NDetail::AbortUnexpectedBootstrapMessage(*ev);
            }
            using T = decltype(&TDerived::Bootstrap);
            TDerived& self = static_cast<TDerived&>(*this);
            if constexpr (std::is_invocable_v<T, TDerived, const TActorContext&>) {
                self.Bootstrap(TActivationContext::ActorContextFor(TActor<TDerived>::SelfId()));
            } else if constexpr (std::is_invocable_v<T, TDerived, const TActorId&, const TActorContext&>) {
                self.Bootstrap(ev->Sender, TActivationContext::ActorContextFor(TActor<TDerived>::SelfId()));
            } else if constexpr (std::is_invocable_v<T, TDerived>) {
                self.Bootstrap();
            } else if constexpr (std::is_invocable_v<T, TDerived, const TActorId&>) {
                self.Bootstrap(ev->Sender);
            } else {
                static_assert(dependent_false<TDerived>::value, "No correct Bootstrap() signature");
            }
        }

        TActorBootstrapped()
            : TActor<TDerived>(&TDerived::StateBootstrap) {
        }

        template <typename T>
        TActorBootstrapped(T&& activityType)
            : TActor<TDerived>(&TDerived::StateBootstrap, std::forward<T>(activityType)) {
        }
    };
}
