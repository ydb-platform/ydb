#pragma once

#include "public.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

void HandleUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev,
                           int component, const TString& location);

void HandleUnexpectedEvent(const NActors::IEventHandlePtr& ev, int component,
                           const TString& location);

void LogUnexpectedEvent(const TAutoPtr<NActors::IEventHandle>& ev,
                        int component, const TString& location);

////////////////////////////////////////////////////////////////////////////////

inline NActors::TActorId Register(const NActors::TActorContext& ctx,
                                  NActors::IActorPtr actor)
{
    return ctx.Register(actor.release());
}

inline NActors::TActorId RegisterLocal(const NActors::TActorContext& ctx,
                                       NActors::IActorPtr actor)
{
    return ctx.RegisterWithSameMailbox(actor.release());
}

template <typename T, typename... TArgs>
inline NActors::TActorId Register(const NActors::TActorContext& ctx,
                                  TArgs&&... args)
{
    auto actor = std::make_unique<T>(std::forward<TArgs>(args)...);
    return Register(ctx, std::move(actor));
}

template <typename T, typename... TArgs>
inline NActors::TActorId RegisterLocal(const NActors::TActorContext& ctx,
                                       TArgs&&... args)
{
    auto actor = std::make_unique<T>(std::forward<TArgs>(args)...);
    return RegisterLocal(ctx, std::move(actor));
}

inline void Send(const NActors::TActorContext& ctx,
                 const NActors::TActorId& recipient,
                 NActors::IEventBasePtr event, ui64 cookie = 0)
{
    ctx.Send(
        recipient,
        event.release(),
        0,   // flags
        cookie);
}

inline void SendWithUndeliveryTracking(
    const NActors::TActorContext& ctx, const NActors::TActorId& recipient,
    NActors::IEventBasePtr event, ui64 cookie = 0)
{
    auto ev = std::make_unique<NActors::IEventHandle>(
        recipient,
        ctx.SelfID,
        event.release(),
        NActors::IEventHandle::FlagForwardOnNondelivery,   // flags
        cookie,                                            // cookie
        &ctx.SelfID   // forwardOnNondelivery
    );

    ctx.Send(ev.release());
}

template <typename T>
inline void Send(const NActors::TActorContext& ctx,
                 const NActors::TActorId& recipient, ui64 cookie = 0)
{
    Send(ctx, recipient, std::make_unique<T>(), cookie);
}

template <typename T, typename... TArgs>
inline void Send(const NActors::TActorContext& ctx,
                 const NActors::TActorId& recipient, ui64 cookie,
                 TArgs&&... args)
{
    auto event = std::make_unique<T>(std::forward<TArgs>(args)...);
    Send(ctx, recipient, std::move(event), cookie);
}

template <typename T>
inline void Reply(const NActors::TActorContext& ctx, const T& request,
                  NActors::IEventBasePtr response)
{
    ctx.Send(
        request.Sender,
        response.release(),
        0,   // flags
        request.Cookie);
}

template <typename T, typename... TArgs>
inline void Schedule(const NActors::TActorContext& ctx, TDuration delta,
                     TArgs&&... args)
{
    auto event = std::make_unique<T>(std::forward<TArgs>(args)...);
    ctx.Schedule(delta, event.release());
}

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_IMPLEMENT_REQUEST(name, ns)                        \
    void Handle##name(const ns::TEv##name##Request::TPtr& ev,      \
                      const NActors::TActorContext& ctx);          \
                                                                   \
    void Reject##name(const ns::TEv##name##Request::TPtr& ev,      \
                      const NActors::TActorContext& ctx)           \
    {                                                              \
        auto response = std::make_unique<ns::TEv##name##Response>( \
            MakeError(E_REJECTED, #name " request rejected"));     \
        NYdb::NBS::Reply(ctx, *ev, std::move(response));           \
    }                                                              \
    // STORAGE_IMPLEMENT_REQUEST

#define STORAGE_HANDLE_REQUEST(name, ns)         \
    HFunc(ns::TEv##name##Request, Handle##name); \
    // STORAGE_HANDLE_REQUEST

#define STORAGE_REJECT_REQUEST(name, ns)         \
    HFunc(ns::TEv##name##Request, Reject##name); \
    // STORAGE_REJECT_REQUEST

}   // namespace NYdb::NBS
