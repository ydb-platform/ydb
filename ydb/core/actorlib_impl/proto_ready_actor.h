#pragma once

#include <ydb/library/actors/core/actor.h>
#include <util/system/type_name.h>

template <typename TType, TType val>
struct TName4Ptr2Member {
};

namespace NActors {

class TProtocolNest {
public:
    template <typename T>
    inline void AddProtocol(T stateFunc, ui32 msgType) noexcept {
        AddProtocol(static_cast<IActor::TReceiveFunc>(stateFunc), msgType);
    }

    template <typename TMsg, typename TStateFunc>
    inline void AddMsgProtocol(TStateFunc stateFunc) noexcept {
        AddProtocol(static_cast<IActor::TReceiveFunc>(stateFunc),
                    TMsg::EventType);
    }

    void AddProtocol(IActor::TReceiveFunc stateFunc, ui32 msgType) noexcept {
        ProtocolFunctions.erase(msgType);
        ProtocolFunctions.emplace(msgType, stateFunc);
    }

    void RemoveProtocol(ui32 msgType) noexcept {
        ProtocolFunctions.erase(msgType);
    }

protected:
    TMap<ui32, IActor::TReceiveFunc> ProtocolFunctions;
};


template <template <typename ...> class TBaseActor, typename TDerived>
class TProtoReadyActor: public TProtocolNest, public TBaseActor<TDerived> {
public:

    template <
        typename TOrigActor,
        typename TProtocol,
        void (TProtocol::*Member)(
            TAutoPtr<IEventHandle>&, const TActorContext&)>
    void CallProtocolStateFunc(
        TAutoPtr<IEventHandle>& ev, const TActorContext& ctx)
    {
        TOrigActor* orig = static_cast<TOrigActor*>(this);
        TProtocol* protoThis = static_cast<TProtocol*>(orig);
        (protoThis->*Member)(ev, ctx);
    }

    template <
        typename TOrigActor,
        typename TProtocol,
        void (TProtocol::*Member)(
            TAutoPtr<IEventHandle>&)>
    void CallProtocolStateFunc(
        TAutoPtr<IEventHandle>& ev)
    {
        TOrigActor* orig = static_cast<TOrigActor*>(this);
        TProtocol* protoThis = static_cast<TProtocol*>(orig);
        (protoThis->*Member)(ev);
    }

    TProtoReadyActor() {
        DerivedActorFunc = this->CurrentStateFunc();
        this->TBaseActor<TDerived>::Become(
            &TProtoReadyActor::ProtocolDispatcher);
    }

    template <typename ... TArgs>
    TProtoReadyActor(TArgs&&...args)
        : TBaseActor<TDerived>(std::move(args)...)
    {
        DerivedActorFunc = this->CurrentStateFunc();
        this->TBaseActor<TDerived>::Become(
            &TProtoReadyActor::ProtocolDispatcher);
    }

    template <typename T>
    void Become(T stateFunc, const char* hint = "undef") noexcept {
        Y_UNUSED(hint);
        DerivedActorFunc = static_cast<IActor::TReceiveFunc>(stateFunc);
    }

private:
    IActor::TReceiveFunc DerivedActorFunc;

    STFUNC(ProtocolDispatcher) {
        Y_ABORT_UNLESS(ev.Get() != nullptr);

        auto funcIter = ProtocolFunctions.find(ev->Type);

        if (funcIter == ProtocolFunctions.end()) {
            return (this->*DerivedActorFunc)(ev);
        }

        auto func = funcIter->second;
        return (this->*func)(ev);
    }
};

}
