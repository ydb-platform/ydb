#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/event_pb.h>

#include <library/cpp/lwtrace/shuttle.h>

#include <util/generic/string.h>
#include <util/generic/typetraits.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TEmpty
{
    static TString Name()
    {
        return "Empty";
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TRequestEvent
    : public NActors::TEventLocal<TRequestEvent<TArgs, EventId>, EventId>
    , public TArgs
{
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>();

    TRequestEvent() = default;

    template <typename ...Args>
    TRequestEvent(TCallContextPtr callContext, Args&& ...args)
        : TArgs(std::forward<Args>(args)...)
        , CallContext(std::move(callContext))
    {
    }

    template <typename ...Args>
    TRequestEvent(Args&& ...args)
        : TArgs(std::forward<Args>(args)...)
    {
    }

    static TString Name()
    {
        return TypeName<TArgs>();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TResponseEvent
    : public NActors::TEventLocal<TResponseEvent<TArgs, EventId>, EventId>
    , public TArgs
{
    const NProto::TError Error;

    TResponseEvent() = default;

    template <
        typename T1,
        typename = std::enable_if_t<
            !std::is_convertible<T1, NProto::TError>::value &&
            !std::is_same<std::decay_t<T1>, TResponseEvent<TArgs, EventId>>::
                value>,
        typename... Args>
    explicit TResponseEvent(T1&& a1, Args&&... args)
        : TArgs(std::forward<T1>(a1), std::forward<Args>(args)...)
    {}

    template <typename... Args>
    explicit TResponseEvent(NProto::TError error, Args&&... args)
        : TArgs(std::forward<Args>(args)...)
        , Error(std::move(error))
    {}

    const NProto::TError& GetError() const
    {
        return Error;
    }

    ui32 GetStatus() const
    {
        return GetError().GetCode();
    }

    const TString& GetErrorReason() const
    {
        return GetError().GetMessage();
    }

    static TString Name()
    {
        return TypeName<TArgs>();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TProtoRequestEvent
    : public NActors::TEventPB<TProtoRequestEvent<TArgs, EventId>, TArgs, EventId>
{
    TCallContextPtr CallContext = MakeIntrusive<TCallContext>();

    TProtoRequestEvent() = default;

    TProtoRequestEvent(TCallContextPtr callContext)
        : CallContext(std::move(callContext))
    {
    }

    template <typename T>
    TProtoRequestEvent(TCallContextPtr callContext, T&& proto)
        : CallContext(std::move(callContext))
    {
        TProtoRequestEvent::Record = std::forward<T>(proto);
    }

    static TString Name()
    {
        return TypeName<TArgs>();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
struct TProtoResponseEvent
    : public NActors::TEventPB<TProtoResponseEvent<TArgs, EventId>, TArgs, EventId>
{
    TProtoResponseEvent() = default;

    template <typename T, typename = std::enable_if_t<!std::is_convertible<T, NProto::TError>::value>>
    TProtoResponseEvent(T&& proto)
    {
        TProtoResponseEvent::Record = std::forward<T>(proto);
    }

    TProtoResponseEvent(const NProto::TError& error)
    {
        if (error.GetCode() != 0 || !error.GetMessage().empty()) {
            *TProtoResponseEvent::Record.MutableError() = error;
        }
    }

    const NProto::TError& GetError() const
    {
        return TProtoResponseEvent::Record.GetError();
    }

    ui32 GetStatus() const
    {
        return GetError().GetCode();
    }

    const TString& GetErrorReason() const
    {
        return GetError().GetMessage();
    }

    static TString Name()
    {
        return TypeName<TArgs>();
    }
};

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_EVENT_IDS(name, ...)                                \
        Ev##name##Request,                                                     \
        Ev##name##Response,                                                    \
// BLOCKSTORE_DECLARE_EVENT_IDS

#define BLOCKSTORE_DECLARE_REQUEST(name, ...)                                  \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr const char* Name = #name;                             \
                                                                               \
        using TRequest = TEv##name##Request;                                   \
        using TResponse = TEv##name##Response;                                 \
    };                                                                         \
// BLOCKSTORE_DECLARE_REQUEST

#define BLOCKSTORE_DECLARE_EVENTS(name, ...)                                   \
    using TEv##name##Request = NBlockStore::TRequestEvent<                                  \
        T##name##Request,                                                      \
        Ev##name##Request                                                      \
    >;                                                                         \
                                                                               \
    using TEv##name##Response = NBlockStore::TResponseEvent<                                \
        T##name##Response,                                                     \
        Ev##name##Response                                                     \
    >;                                                                         \
                                                                               \
    BLOCKSTORE_DECLARE_REQUEST(name, __VA_ARGS__)                              \
// BLOCKSTORE_DECLARE_EVENTS

#define BLOCKSTORE_DECLARE_PROTO_EVENTS(name, ...)                             \
    using TEv##name##Request = TProtoRequestEvent<                             \
        NProto::T##name##Request,                                              \
        Ev##name##Request                                                      \
    >;                                                                         \
                                                                               \
    using TEv##name##Response = TProtoResponseEvent<                           \
        NProto::T##name##Response,                                             \
        Ev##name##Response                                                     \
    >;                                                                         \
                                                                               \
    BLOCKSTORE_DECLARE_REQUEST(name, __VA_ARGS__)                              \
// BLOCKSTORE_DECLARE_PROTO_EVENTS

}   // namespace NYdb::NBS::NBlockStore
