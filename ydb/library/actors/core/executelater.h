#pragma once

#include "actor_bootstrapped.h"

#include <utility>

namespace NActors {
    template <typename TCallback>
    class TExecuteLater: public TActorBootstrapped<TExecuteLater<TCallback>> {
    public:

        static constexpr char ActorName[] = "AS_EXECUTE_LATER";

        TExecuteLater(
            TCallback&& callback,
            IActor::EActivityType activityType,
            ui32 channel = 0,
            ui64 cookie = 0,
            const TActorId& reportCompletionTo = TActorId(),
            const TActorId& reportExceptionTo = TActorId()) noexcept
            : Callback(std::move(callback))
            , Channel(channel)
            , Cookie(cookie)
            , ReportCompletionTo(reportCompletionTo)
            , ReportExceptionTo(reportExceptionTo)
        {
            this->SetActivityType(activityType);
        }

        void Bootstrap(const TActorContext& ctx) noexcept {
            try {
                {
                    /* RAII, Callback should be destroyed right before sending
                   TEvCallbackCompletion */

                    auto local = std::move(Callback);
                    using T = decltype(local);

                    if constexpr (std::is_invocable_v<T, const TActorContext&>) {
                        local(ctx);
                    } else {
                        local();
                    }
                }

                if (ReportCompletionTo) {
                    ctx.Send(ReportCompletionTo,
                             new TEvents::TEvCallbackCompletion(ctx.SelfID),
                             Channel, Cookie);
                }
            } catch (...) {
                if (ReportExceptionTo) {
                    const TString msg = CurrentExceptionMessage();
                    ctx.Send(ReportExceptionTo,
                             new TEvents::TEvCallbackException(ctx.SelfID, msg),
                             Channel, Cookie);
                }
            }

            this->Die(ctx);
        }

    private:
        TCallback Callback;
        const ui32 Channel;
        const ui64 Cookie;
        const TActorId ReportCompletionTo;
        const TActorId ReportExceptionTo;
    };

    template <typename T>
    IActor* CreateExecuteLaterActor(
        T&& func,
        IActor::EActivityType activityType,
        ui32 channel = 0,
        ui64 cookie = 0,
        const TActorId& reportCompletionTo = TActorId(),
        const TActorId& reportExceptionTo = TActorId()) noexcept {
        return new TExecuteLater<T>(std::forward<T>(func),
                                    activityType,
                                    channel,
                                    cookie,
                                    reportCompletionTo,
                                    reportExceptionTo);
    }
}
