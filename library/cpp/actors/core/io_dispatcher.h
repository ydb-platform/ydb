#pragma once

#include "actor.h"
#include "event_local.h"
#include "events.h"
#include "actorsystem.h"
#include "executor_thread.h"
#include "executelater.h"

namespace NActors {

    struct TEvInvokeQuery : TEventLocal<TEvInvokeQuery, TEvents::TSystem::InvokeQuery> {
        std::function<void()> Callback;

        TEvInvokeQuery(std::function<void()>&& callback)
            : Callback(std::move(callback))
        {}
    };

    inline TActorId MakeIoDispatcherActorId() {
        return TActorId(0, TStringBuf("IoDispatcher", 12));
    }

    extern IActor *CreateIoDispatcherActor(const NMonitoring::TDynamicCounterPtr& counters);

    /* InvokeIoCallback enqueues callback() to be executed in IO thread pool and then return result in TEvInvokeResult
     * message to parentId actor.
     */
    template<typename TCallback>
    static void InvokeIoCallback(TCallback&& callback, ui32 poolId, IActor::EActivityType activityType) {
        if (!TActivationContext::Send(new IEventHandle(MakeIoDispatcherActorId(), TActorId(),
                new TEvInvokeQuery(callback)))) {
            TActivationContext::Register(CreateExecuteLaterActor(std::move(callback), activityType), TActorId(),
                TMailboxType::HTSwap, poolId);
        }
    }

} // NActors
